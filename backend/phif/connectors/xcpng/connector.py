"""XCP-ng / XenServer connector.

Unlike the stock ``lvmoiscsi`` SR (which packs many VDIs as logical volumes onto
one shared LUN and loses per-VDI array data services), this connector deploys a
**custom XCP-ng SMAPIv3 storage plugin** -- ``org.xen.xapi.storage.purefa``, SR
type ``purefa`` -- modeled on the Everpure CSI / Cinder / Proxmox "storage plugin"
philosophy:

    EACH VDI IS ITS OWN FLASHARRAY VOLUME, presented directly to the VM as a
    multipathed raw block device, with array-native snapshots and clones
    (FlashArray volume snapshot + volume copy) rather than VHD/LVM snapshots.

The plugin ships as a static directory tree in this package
(``files/smapiv3/org.xen.xapi.storage.purefa`` -- the Volume plugin; plus a
custom ``purefa`` Datapath plugin and a ``purefa-mpath`` XAPI host plugin) and is
pushed under each pool host's ``xapi-storage-script`` tree at deploy time. XCP-ng
8.3 does NOT load dropped-in SMAPIv1 ``/opt/xensource/sm`` drivers, so deployment
is exclusively SMAPIv3 (``_install_smapiv3_plugin``).

All host-side work is driven over SSH via ``ctx.runner.run_ssh`` using the
``xe`` CLI (mock-safe). Array-side work uses ``ctx.array``. ``ctx.dry_run`` is
honored throughout; progress streams via ``ctx.emit``.

Doc sources (last verified May 19 2026):
* iSCSI on XCP-ng - CLI Quick Start (stock lvmoiscsi SR, xe/multipath flow):
  https://support.purestorage.com/bundle/m_linux/page/Solutions/Linux/topics/t_xcpng_iscsi_quickstart.html
* iSCSI on XCP-ng - XCP-ng-Specific Considerations (8.3, RHEL dom0/yum, XAPI+SMAPI,
  pre-installed iscsi/multipath tools, SR/VDI/PBD model):
  https://support.purestorage.com/bundle/m_linux/page/Solutions/Linux/topics/c_xcpng_iscsi_best-practices_xcp-ng-specific_considerations.html
* Multipath best practices (ALUA, /etc/multipath/conf.d/custom.conf, pool
  other-config:multipathing=true):
  https://support.purestorage.com/bundle/m_linux/page/Solutions/Linux/topics/c_xcpng_iscsi_best-practices_multipath_configuration.html

The SMAPIv3 ``purefa`` driver is validated on a live 2-host XCP-ng 8.3 pool
(xapi 25.6). Everpure's published guidance covers ONLY the stock ``lvmoiscsi`` shared
SR; there is no Everpure-published custom SMAPI driver, so the per-VDI ``purefa``
plugin specifics are our own design, confirmed empirically end-to-end on that
pool.
"""

from __future__ import annotations

import asyncio
import os
import re
from typing import Any

from phif.connectors.base import (
    ActionSpec,
    Capability,
    ClusterNode,
    ConnectionValidationError,
    FieldType,
    FormField,
    HypervisorConnector,
    OpResult,
    Protocol,
    compare_node_interfaces,
    nics_on_array_subnets,
    nics_on_common_subnets,
)
from phif.connectors.iscsi_net import arp_flux_cmd

# Root of the bundled plugin files shipped with this connector. The SMAPIv3
# plugin paths below are built from it. (XCP-ng 8.3 does not load dropped-in
# SMAPIv1 drivers, so there is no /opt/xensource/sm driver -- the deploy path is
# exclusively the SMAPIv3 plugin in _install_smapiv3_plugin.)
_DRIVER_DIR = os.path.join(os.path.dirname(__file__), "files")
_SR_TYPE = "purefa"

# SMAPIv3 volume plugin (the supported model on XCP-ng 8.3). The plugin dir is
# dropped under xapi-storage-script's volume/ tree on each host; xapi-storage-
# script discovers it via inotify and `xe sm-list` then shows the 'purefa' type.
_SMAPIV3_PLUGIN_NAME = "org.xen.xapi.storage.purefa"
_SMAPIV3_SRC_DIR = os.path.join(_DRIVER_DIR, "smapiv3", _SMAPIV3_PLUGIN_NAME)
_SMAPIV3_DEST_DIR = (
    "/usr/libexec/xapi-storage-script/volume/" + _SMAPIV3_PLUGIN_NAME)
# Files that make up the plugin (pushed to each host; link.sh creates the
# per-method entrypoint symlinks).
_SMAPIV3_FILES = ("purefa_fa.py", "plugin.py", "sr.py", "volume.py", "link.sh")

# Custom DATAPATH plugin: presents a FA volume directly to the guest as a raw
# multipath block device (Blkback). The stock tapdisk/qdisk datapaths only serve
# VHD/qcow files, so VDIs whose URI scheme is 'purefa://' need this. The datapath
# dir is named by the URI SCHEME ('purefa'), per xapi-storage-script convention.
_SMAPIV3_DP_SRC_DIR = os.path.join(_DRIVER_DIR, "smapiv3", "datapath", "purefa")
_SMAPIV3_DP_DEST_DIR = "/usr/libexec/xapi-storage-script/datapath/purefa"
_SMAPIV3_DP_FILES = ("plugin.py", "datapath.py", "link.sh")

# XAPI host plugin for pool-wide multipath maintenance (flush a deleted volume's
# stale map on EVERY host; rescan a new one). Invoked from the volume plugin via
# `xe host-call-plugin`. Installed at /etc/xapi.d/plugins/ on each pool host.
_SMAPIV3_HOSTPLUGIN_SRC = os.path.join(
    _DRIVER_DIR, "smapiv3", "hostplugin", "purefa-mpath")
_SMAPIV3_HOSTPLUGIN_DEST = "/etc/xapi.d/plugins/purefa-mpath"

_ISCSI_PORT = 3260
_NVME_TCP_PORT = 4420

# Everpure-recommended multipath config for XCP-ng. Per the Everpure XCP-ng multipath
# best-practices doc, custom config goes in /etc/multipath/conf.d/ (a drop-in
# that persists across updates) -- NEVER edit /etc/multipath.xenserver/multipath.conf.
# We use a dedicated PHIF-owned drop-in (pure.conf) rather than custom.conf so we
# never clobber an operator's own custom.conf. find_multipaths groups the multiple
# portal/HBA paths to one wwid; the PURE device stanza sets ALUA + the recommended
# path policy. (NVMe-oF uses native NVMe multipath, not dm-multipath.)
_MULTIPATH_CONF_PATH = "/etc/multipath/conf.d/pure.conf"
_MULTIPATH_CONF = (
    "defaults {\n"
    # find_multipaths no: auto-claim every (non-blacklisted) Everpure LUN into a
    # multipath device without needing an explicit `multipath -a <wwid>`. With
    # 'yes', a freshly-attached VDI volume isn't assembled until added by hand, so
    # the datapath can't find /dev/mapper/<wwid> on VM power-on.
    "  find_multipaths no\n"
    # user_friendly_names no: the map is ALWAYS named by its WWID
    # (/dev/mapper/3624a9370<serial>), never an 'mpathN' alias -- the datapath
    # resolves volumes by WWID, so the name must be the WWID regardless of any
    # other (mpathN) entries on the host.
    "  user_friendly_names no\n"
    "}\n"
    "devices {\n"
    "  device {\n"
    "    vendor \"PURE\"\n"
    "    product \"FlashArray\"\n"
    "    path_selector \"service-time 0\"\n"
    "    path_grouping_policy group_by_prio\n"
    "    prio alua\n"
    "    hardware_handler \"1 alua\"\n"
    "    failback immediate\n"
    "    rr_weight uniform\n"
    "    no_path_retry 0\n"
    "  }\n"
    "}\n"
)


class XcpngConnector(HypervisorConnector):
    # --- static metadata ---
    key = "xcpng"
    name = "XCP-ng / XenServer (Everpure SR driver)"
    description = (
        "Deploy a custom XCP-ng SMAPIv3 storage plugin (org.xen.xapi.storage.purefa, "
        "SR type 'purefa') across a pool so that each VDI is its own FlashArray "
        "volume, presented directly to the VM as a multipathed raw block device. "
        "Snapshots and clones are array-native (FlashArray snapshot + volume copy), "
        "not VHD/LVM. A true custom storage driver in the spirit of Everpure CSI / "
        "Cinder / the Proxmox plugin -- NOT the stock shared lvmoiscsi SR."
    )
    maturity = "ga"
    CAPABILITIES = {
        Capability.DEPLOY_PLUGIN,
        Capability.HOST_REGISTER,
        Capability.CONNECTIVITY,
        Capability.PROVISION_VOLUME,
        Capability.SNAPSHOT,
        Capability.CLONE,
        Capability.RESIZE,
        Capability.HEALTH,
        Capability.REMOVE,
        Capability.RECONCILE_CLUSTER,
        Capability.VM_INVENTORY,
        Capability.VM_LIFECYCLE,
        Capability.MIGRATE,
    }
    # NFS uses XCP-ng's stock NFS SR (file-backed FA file system + NFS export),
    # NOT the per-VDI 'purefa' SMAPIv3 block plugin.
    # TODO(hardware-validate): the NFS path has NOT been validated on real
    # hardware yet -- it is implemented mock/dry-run safe and reviewed only.
    SUPPORTED_PROTOCOLS = {Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP,
                           Protocol.NFS}

    # ------------------------------------------------------------------ #
    # UI: how to connect to this hypervisor
    # ------------------------------------------------------------------ #
    @classmethod
    def target_schema(cls) -> list[FormField]:
        return [
            FormField("pool_master_host", "Pool master host / IP (SSH)", FieldType.STRING,
                      placeholder="xcp-master.example.local",
                      help="The XCP-ng pool master we SSH to to drive `xe`."),
            FormField("ssh_user", "SSH user", FieldType.STRING, default="root"),
            FormField("ssh_password", "SSH password", FieldType.SECRET, required=False,
                      help="Provide either an SSH password or an SSH private key."),
            FormField("ssh_key", "SSH private key", FieldType.TEXT, required=False,
                      help="PEM private key; used if no password is supplied."),
            FormField("protocol", "Storage protocol", FieldType.ENUM, default="iscsi",
                      options=["iscsi", "nvme-tcp", "fc", "nfs"]),
            FormField("sr_name", "SR name-label", FieldType.STRING, default="purefa"),
            FormField("host_group", "FlashArray host group", FieldType.STRING,
                      help="Host group on the array holding this pool's hosts."),
            FormField("host_wwns", "Pool host WWNs (Fibre Channel, comma-separated)",
                      FieldType.STRING, required=False,
                      help="FC HBA port WWNs for the pool hosts (auto-discovered "
                           "from the pool master if left blank). Used when "
                           "protocol=fc to register hosts on the array."),
        ]

    # ------------------------------------------------------------------ #
    # UI: day-2 actions (standard ids -> default dispatch)
    # ------------------------------------------------------------------ #
    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        return [
            ActionSpec(
                Capability.DEPLOY_PLUGIN, "deploy",
                "Deploy SMAPIv3 purefa plugin + create SR",
                "Push the org.xen.xapi.storage.purefa SMAPIv3 plugin to every pool "
                "host, (re)enumerate it, and `xe sr-create type=purefa`. The "
                "FlashArray endpoint and API token are taken from the associated "
                "array on this hypervisor (not operator-entered).",
                fields=[
                    FormField("sr_name", "SR name-label", FieldType.STRING,
                              required=False, default="purefa"),
                    FormField("host_group", "FlashArray host group", FieldType.STRING,
                              required=False),
                    FormField("clobber", "Clobber existing SR", FieldType.BOOL,
                              required=False, default=False,
                              help="If an SR with this name already exists, forget "
                                   "it (unplug PBDs + sr-forget) before recreating. "
                                   "Use to clean up a half-created/broken SR. This "
                                   "does NOT eradicate the array volumes."),
                    FormField("eradicate", "Eradicate volumes on delete",
                              FieldType.BOOL, required=False, default=False,
                              help="When a VDI/VM disk is deleted, hard-delete "
                                   "(eradicate) its FlashArray volume immediately "
                                   "instead of leaving it in pending-eradication "
                                   "for 24h. Off = recoverable for 24h."),
                ],
            ),
            ActionSpec(
                Capability.HOST_REGISTER, "register_hosts", "Register pool hosts on array",
                "Read each pool host's IQN/NQN/WWN via `xe` and build a FlashArray "
                "host + host group.",
                fields=[
                    FormField("host_group", "Host group name", FieldType.STRING),
                    FormField("iqns", "Host IQNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="(auto-discovered from the pool master if left "
                                   "blank)"),
                    FormField("nqns", "Host NQNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="(auto-discovered from the pool master if left "
                                   "blank)"),
                    FormField("wwns", "Host WWNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="(auto-discovered from the pool master if left "
                                   "blank)"),
                ],
            ),
            ActionSpec(
                Capability.CONNECTIVITY, "setup_connectivity",
                "Configure connectivity + multipath",
                "Enable pool multipathing, install the Everpure ALUA multipath stanza, "
                "and connect the transport. For iSCSI/NVMe-TCP this logs in over "
                "IP; for Fibre Channel there is no login -- SAN zoning is a "
                "prerequisite and we only run an FC/SCSI rescan.",
                fields=[
                    FormField("portals", "Array portal IPs (comma-separated)",
                              FieldType.STRING, required=False, options_source="array_portals",
                              placeholder="auto-discovered from the array",
                              help="Auto-loaded from the associated FlashArray's data "
                                   "interfaces for the chosen protocol."),
                    FormField("target_iqn", "Target IQN (iSCSI)", FieldType.STRING,
                              required=False, options_source="array_target_iqn",
                              help="Auto-loaded from the array's iSCSI target."),
                    FormField("subsystem_nqn", "Subsystem NQN (NVMe-TCP)",
                              FieldType.STRING, required=False,
                              options_source="array_target_nqn",
                              help="Auto-loaded from the array's NVMe subsystem."),
                    # --- INTERFACE BINDING (optional; pin the storage path to
                    #     specific local NICs / sources / HBAs). Choices are
                    #     discovered via discover_options() -> options_source. ---
                    FormField("iscsi_nics", "iSCSI bind NICs", FieldType.MULTISELECT,
                              required=False, options_source="nics",
                              help="Local NICs to bind the iSCSI ifaces to "
                                   "(pins the iSCSI path to these interfaces)."),
                    FormField("nvme_sources", "NVMe-TCP host sources",
                              FieldType.MULTISELECT, required=False,
                              options_source="nvme_sources",
                              help="Local source addresses/interfaces for the "
                                   "NVMe-TCP connection (host-traddr)."),
                    FormField("nvme_options", "NVMe connect options", FieldType.STRING,
                              required=False,
                              help="Extra `nvme connect` options persisted with "
                                   "the binding (e.g. 'ctrl-loss-tmo=600')."),
                    FormField("fc_hbas", "Fibre Channel HBAs", FieldType.MULTISELECT,
                              required=False, options_source="fc_hbas",
                              help="FC HBA WWPNs to pin the storage path to "
                                   "(zoning is external/switch-side)."),
                ],
            ),
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision", "Provision VDI (FA volume)",
                "Create a new VDI on the purefa SR -- backed by a dedicated "
                "FlashArray volume mapped directly to the VM.",
                fields=[
                    FormField("name", "VDI / volume name", FieldType.STRING),
                    FormField("size", "Size", FieldType.SIZE, default="100G"),
                    FormField("host_group", "Attach to host group", FieldType.STRING,
                              required=False),
                    FormField("sr_name", "Target SR name-label", FieldType.STRING,
                              required=False),
                ],
            ),
            ActionSpec(
                Capability.SNAPSHOT, "snapshot", "Snapshot VDI (array snapshot)",
                "Array snapshot via the purefa SMAPIv3 plugin (`xe vdi-snapshot` / "
                "ctx.array).",
                fields=[FormField("volume", "VDI / volume name", FieldType.STRING),
                        FormField("vdi_uuid", "VDI UUID", FieldType.STRING,
                                  required=False)]),
            ActionSpec(
                Capability.CLONE, "clone", "Clone VDI (array copy)",
                "Array volume copy via the purefa SMAPIv3 plugin (`xe vdi-clone` / "
                "ctx.array).",
                fields=[FormField("source", "Source VDI / volume", FieldType.STRING),
                        FormField("dest", "New volume name", FieldType.STRING),
                        FormField("vdi_uuid", "Source VDI UUID", FieldType.STRING,
                                  required=False)]),
            ActionSpec(
                Capability.RESIZE, "resize", "Resize VDI",
                "Extend the FlashArray volume then `xe vdi-resize`.",
                fields=[FormField("volume", "VDI / volume name", FieldType.STRING),
                        FormField("size", "New size", FieldType.SIZE),
                        FormField("vdi_uuid", "VDI UUID", FieldType.STRING,
                                  required=False)]),
            ActionSpec(Capability.HEALTH, "health_check", "Health check",
                       "`xe sr-list` + `multipath -ll`.", long_running=False),
            ActionSpec(Capability.REMOVE, "teardown", "Remove integration",
                       "`xe pbd-unplug` / `xe sr-forget` + remove the SMAPIv3 "
                       "plugin from every pool host.",
                       destructive=True),
            ActionSpec(
                Capability.RECONCILE_CLUSTER, "assess_cluster",
                "Assess pool membership drift",
                "READ-ONLY: list pool hosts, the FlashArray host-group members, "
                "and which new hosts are ready to deploy (preflight) / which array "
                "hosts have departed the pool.",
                long_running=False),
            ActionSpec(
                Capability.RECONCILE_CLUSTER, "reconcile_cluster",
                "Deploy to new pool hosts",
                "Configure pool hosts that are NEW and pass the readiness preflight "
                "(register + connectivity + plugin install); skip not-ready ones. "
                "Departed array hosts are flagged unless 'Remove departed hosts' is "
                "set, in which case they are removed from the host group + array.",
                fields=[
                    FormField("apply_removals", "Remove departed hosts",
                              FieldType.BOOL, required=False, default=False,
                              help="Remove FlashArray hosts (and forget their PBDs) "
                                   "for pool nodes that have left the pool. Off = "
                                   "only flag them."),
                ],
                destructive=True),
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision_nfs", "Provision NFS SR (stock)",
                "NFS only: create a FlashArray file system + NFS export and "
                "`xe sr-create type=nfs` pool-wide (stock XCP-ng NFS SR). NOT the "
                "per-VDI 'purefa' block plugin.",
                fields=[
                    FormField("name", "File system / SR name", FieldType.STRING),
                    FormField("sr_name", "SR name-label", FieldType.STRING,
                              required=False),
                ]),
            ActionSpec(
                Capability.REMOVE, "teardown_nfs", "Remove NFS SR (stock)",
                "NFS only: sr-forget/destroy the stock NFS SR + delete the FA NFS "
                "export and file system.",
                fields=[
                    FormField("name", "File system / SR name", FieldType.STRING),
                    FormField("sr_name", "SR name-label", FieldType.STRING,
                              required=False),
                    FormField("eradicate", "Eradicate file system",
                              FieldType.BOOL, required=False, default=False),
                ],
                destructive=True),
        ]

    # ------------------------------------------------------------------ #
    # Deployment wizard ordering
    # ------------------------------------------------------------------ #
    @classmethod
    def wizard_steps(cls) -> list[str]:
        """Ordered action ids the deployment wizard runs end-to-end.

        XCP-ng has no separate `configure` step: SR creation happens inside
        `deploy` (`xe sr-create`). We register the pool hosts on the array FIRST so
        the FlashArray host objects/group exist before `deploy` creates the SR that
        binds to that host group (deploy validates the host objects up front and
        fails clearly if they're missing). Then configure connectivity/multipath on
        every host.
        """
        return ["register_hosts", "deploy", "setup_connectivity"]

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _protocol(self) -> str:
        return (self.ctx.target.get("protocol") or "iscsi").lower()

    def _host(self) -> str:
        host = self.ctx.target.get("pool_master_host") or self.ctx.target.get("host")
        if not host:
            raise ConnectionValidationError("No XCP-ng pool_master_host configured")
        return host

    def _ssh_kwargs(self) -> dict[str, Any]:
        kw: dict[str, Any] = {"username": self.ctx.target.get("ssh_user", "root")}
        pw = self.ctx.target.get("ssh_password")
        key = self.ctx.target.get("ssh_key")
        if pw:
            kw["password"] = pw
        if key:
            kw["key"] = key
        return kw

    def _discover_kwargs(self) -> dict[str, Any]:
        """Credential kwargs for ``runner.discover_initiators`` (username/password/key)."""
        kw: dict[str, Any] = {"username": self.ctx.target.get("ssh_user", "root")}
        pw = self.ctx.target.get("ssh_password")
        key = self.ctx.target.get("ssh_key")
        if pw:
            kw["password"] = pw
        if key:
            kw["key"] = key
        return kw

    def _is_mock_or_dry(self) -> bool:
        return bool(self.ctx.dry_run or getattr(self.ctx.runner, "mock", False)
                    or getattr(self.ctx.runner, "dry_run", False))

    def _array_mgmt_target(self, endpoint: str = "") -> tuple[str, int] | None:
        """Return (host, port) of the array management endpoint, or None.

        Accepts a bare IP/host, ``host:port``, or a full ``https://host[:port]/..``
        URL. Port defaults to 443. Uses the explicit ``endpoint`` if given, else the
        associated array's endpoint.
        """
        ep = (endpoint or (getattr(self.ctx.array, "endpoint", "")
                           if self.ctx.array is not None else "") or "").strip()
        if not ep:
            return None
        ep = ep.split("://", 1)[-1].strip("/")
        host = ep.split("/", 1)[0]
        port = 443
        if ":" in host:
            h, p = host.rsplit(":", 1)
            if p.isdigit():
                host, port = h, int(p)
        return (host, port) if host else None

    async def _unreachable_nodes(self, nodes: list[ClusterNode], host: str,
                                 port: int) -> list[str]:
        """Pool hosts that cannot open a TCP connection to ``host:port``.

        Dependency-free bash ``/dev/tcp`` probe with a 5s timeout per host. Skipped
        (returns empty) in mock/dry-run.
        """
        if self._is_mock_or_dry():
            return []
        probe = (f"timeout 5 bash -c 'exec 3<>/dev/tcp/{host}/{port}' "
                 f"2>/dev/null && echo ARRAY_OK || echo ARRAY_FAIL")
        unreachable: list[str] = []
        for node in nodes:
            out = await self._ssh(probe, check=False, host=node.host)
            if "ARRAY_OK" not in (out or ""):
                unreachable.append(f"{node.name} ({node.host})")
        return unreachable

    async def _ssh(self, command: str, *, check: bool = True,
                   host: str = "", timeout: float | None = None) -> str:
        return await self.ctx.runner.run_ssh(host or self._host(), command,
                                             check=check, timeout=timeout,
                                             **self._ssh_kwargs())

    async def _ssh_script(self, lines: list[str], *, check: bool = True,
                          host: str = "", timeout: float | None = None) -> str:
        return await self.ctx.runner.run_ssh_script(host or self._host(), lines,
                                                    check=check, timeout=timeout,
                                                    **self._ssh_kwargs())

    def _sr_name(self, override: str = "") -> str:
        return override or self.ctx.target.get("sr_name") or _SR_TYPE

    @staticmethod
    def _fa_name(raw: str) -> str:
        """Sanitize a string into a valid FlashArray object name.

        FlashArray names allow only ``[A-Za-z0-9-]`` and must begin/end with an
        alphanumeric. A host group label or a node IP/FQDN (e.g.
        ``192.0.2.58``) may carry dots, so map any invalid char to ``-`` and
        trim leading/trailing hyphens. FA rejects names containing dots.
        """
        import re

        s = re.sub(r"[^A-Za-z0-9-]", "-", raw or "").strip("-")
        return s or "xcp-pool"

    # ------------------------------------------------------------------ #
    # Dynamic field options: enumerate bindable NICs / NVMe sources / FC HBAs
    # ------------------------------------------------------------------ #
    async def discover_options(self, kind: str) -> list[dict[str, Any]]:
        """Enumerate bindable interfaces on the pool master for a form field.

        ``kind`` is a DiscoveryKind value ("nics", "nvme_sources", "fc_hbas").
        Delegates to the shared ``runner.discover_interfaces`` helper over SSH
        (mock-safe -- returns synthetic entries in mock/dry-run). The UI calls
        this to populate the interface-binding multiselects in setup_connectivity.
        """
        # Array-side values for the connectivity form (from the associated FA).
        if kind in ("array_portals", "array_target_iqn", "array_target_nqn"):
            if self.ctx.array is None:
                return []
            if kind == "array_portals":
                service = "nvme-tcp" if self._protocol() == "nvme-tcp" else "iscsi"
                ips = await self.ctx.array.get_data_interfaces(service)
                return [{"value": ip} for ip in ips]
            ports = await self.ctx.array.get_target_ports()
            key = "iqn" if kind == "array_target_iqn" else "nqn"
            return [{"value": ports[key]}] if ports.get(key) else []
        if kind == "initiators":
            # Discover the pool master's IQN / NQN / FC WWNs so the register-hosts
            # form can display/pre-fill them. Each option carries `field` = the
            # matching register_hosts form field (iqns / nqns / wwns).
            await self.ctx.emit(
                f"Discovering initiators on pool master {self._host()}")
            found = await self.ctx.runner.discover_initiators(
                self._host(), **self._discover_kwargs())
            opts: list[dict[str, Any]] = []
            if found.get("iqn"):
                opts.append({"field": "iqns", "value": found["iqn"],
                             "label": f"iSCSI IQN — {found['iqn']}"})
            if found.get("nqn"):
                opts.append({"field": "nqns", "value": found["nqn"],
                             "label": f"NVMe NQN — {found['nqn']}"})
            if found.get("wwns"):
                wwns = ",".join(found["wwns"])
                opts.append({"field": "wwns", "value": wwns,
                             "label": f"FC WWNs — {wwns}"})
            return opts
        if kind not in ("nics", "nvme_sources", "fc_hbas"):
            return []
        await self.ctx.emit(f"Discovering '{kind}' on pool master {self._host()}")
        opts = await self.ctx.runner.discover_interfaces(
            self._host(), kind, **self._discover_kwargs())
        # IP interfaces (NICs for iSCSI, source addrs for NVMe-TCP) are filtered so
        # only consistent, storage-reachable interfaces are offered; FC HBAs have no
        # subnet and pass through.
        if kind in ("nics", "nvme_sources"):
            # 1) Keep only subnets configured on EVERY pool host (uniform binding).
            opts = await self._filter_common_subnet_interfaces(kind, opts)
            # 2) And restrict to interfaces that can reach the array's portals.
            if self.ctx.array is not None:
                service = "nvme-tcp" if self._protocol() == "nvme-tcp" else "iscsi"
                portals = await self.ctx.array.get_data_interfaces(service)
                opts = nics_on_array_subnets(opts, portals)
        return opts

    async def _filter_common_subnet_interfaces(
        self, kind: str, host_opts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Restrict candidate IP interfaces to subnets configured on every pool host.

        Discovers the same interface ``kind`` on each pool host and keeps only the
        master's options whose subnet is present on ALL hosts (via
        :func:`nics_on_common_subnets`), so the chosen binding exists uniformly
        pool-wide. A single-host pool leaves ``host_opts`` unchanged.
        """
        nodes = await self.list_nodes()
        if len(nodes) <= 1:
            return host_opts
        per_node: dict[str, list[dict[str, Any]]] = {}
        for node in nodes:
            per_node[node.name] = await self.ctx.runner.discover_interfaces(
                node.host, kind, **self._discover_kwargs())
        return nics_on_common_subnets(host_opts, per_node)

    # ------------------------------------------------------------------ #
    # Cluster / pool awareness
    # ------------------------------------------------------------------ #
    def _mock_mode(self) -> bool:
        """True when SSH does not touch the network (mock or dry-run).

        In that case `xe host-list` returns nothing, so node enumeration must
        fall back to a synthetic pool to keep the multi-node flows exercisable.
        """
        return bool(getattr(self.ctx.runner, "mock", False) or self.ctx.dry_run)

    async def list_nodes(self) -> list[ClusterNode]:
        """Enumerate the XCP-ng pool's member hosts via the pool master.

        SSH to the pool master and read the pool's hosts with
        ``xe host-list params=uuid,name-label,address --minimal``. Each pool host
        becomes a :class:`ClusterNode` whose ``host`` is the host's management
        address (the SSH/`xe` endpoint we fan node-specific work out to).

        Mock/dry-run: `xe` is not executed, so we return a synthetic 2-host pool
        rooted at the configured pool master. Fallback (real run, empty/garbled
        output): a single node for the pool master so single-host behavior stays
        correct.
        """
        master = self._host()
        if self._mock_mode():
            await self.ctx.emit(
                f"[mock] synthesizing 2-host pool for master {master}")
            return [
                ClusterNode(name=f"{master}", host=master,
                            info={"uuid": "host-uuid-0", "role": "master"}),
                ClusterNode(name=f"{master}-2", host=f"{master}-2",
                            info={"uuid": "host-uuid-1", "role": "member"}),
            ]

        await self.ctx.emit(f"Listing pool hosts via master {master}")
        # IMPORTANT: do NOT use `--minimal` with multiple params. With more than one
        # param, `--minimal` comma-joins EVERY value of EVERY record with no record
        # delimiter, so a multi-host pool collapses into one unparseable blob (this
        # is why only one node was ever seen). The DEFAULT (non-minimal) output is a
        # per-record block we can parse unambiguously.
        out = await self._ssh(
            "xe host-list params=uuid,name-label,address", check=False)
        nodes = self._parse_host_list(out)
        if not nodes:
            # Fallback: single-param `--minimal` IS unambiguous (a clean comma list
            # of uuids); resolve each host's name + address via param-get.
            await self.ctx.emit(
                "Block parse empty; resolving pool hosts via per-uuid param-get")
            nodes = await self._list_nodes_via_param_get()
        if not nodes:
            await self.ctx.emit(
                f"No pool hosts parsed; falling back to single host {master}")
            return [ClusterNode(name=master, host=master)]
        await self.ctx.emit(
            f"Pool has {len(nodes)} host(s): "
            f"{', '.join(n.name for n in nodes)}")
        return nodes

    # Field line of `xe ... params=...` default output: "<key> ( RO|RW): <value>".
    _XE_FIELD_RE = re.compile(r"^\s*([a-zA-Z][\w-]*)\s*\(\s*R[OW]\s*\)\s*:\s*(.*?)\s*$")

    @classmethod
    def _parse_host_list(cls, out: str) -> list[ClusterNode]:
        """Parse the default (non-minimal) `xe host-list params=...` block output.

        Records look like::

            uuid ( RO)                : <uuid>
                      name-label ( RW): <name>
                         address ( RO): <addr>

        separated by blank lines. We start a new record whenever a ``uuid`` field
        appears, so parsing is robust to blank-line count and field ordering.
        """
        records: list[dict[str, str]] = []
        cur: dict[str, str] = {}
        for line in (out or "").splitlines():
            m = cls._XE_FIELD_RE.match(line)
            if not m:
                continue
            key, val = m.group(1), m.group(2)
            if key == "uuid" and cur:
                records.append(cur)
                cur = {}
            cur[key] = val
        if cur:
            records.append(cur)

        nodes: list[ClusterNode] = []
        for r in records:
            uuid = r.get("uuid", "")
            name = r.get("name-label") or uuid
            address = r.get("address") or name
            if not address:
                continue
            nodes.append(ClusterNode(name=name or address, host=address,
                                     info={"uuid": uuid} if uuid else {}))
        return nodes

    async def _list_nodes_via_param_get(self) -> list[ClusterNode]:
        """Resolve pool hosts via single-param `--minimal` + per-uuid param-get.

        Single-param `--minimal` is unambiguous (a clean comma list), so this is a
        reliable fallback when the block parse yields nothing.
        """
        raw = await self._ssh("xe host-list --minimal", check=False)
        uuids = [u.strip() for u in (raw or "").replace("\n", ",").split(",")
                 if u.strip()]
        nodes: list[ClusterNode] = []
        for u in uuids:
            name = (await self._ssh(
                f"xe host-param-get uuid={u} param-name=name-label",
                check=False)).strip()
            addr = (await self._ssh(
                f"xe host-param-get uuid={u} param-name=address",
                check=False)).strip()
            nodes.append(ClusterNode(name=name or u, host=addr or name or u,
                                     info={"uuid": u}))
        return nodes

    def _initiator_field(self, found: dict[str, Any], proto: str) -> dict[str, Any]:
        """Pull the protocol-relevant initiator(s) out of a discover_initiators dict."""
        if proto == "iscsi":
            return {"iqns": [found["iqn"]] if found.get("iqn") else []}
        if proto == "nvme-tcp":
            return {"nqns": [found["nqn"]] if found.get("nqn") else []}
        if proto == "fc":
            return {"wwns": list(found.get("wwns") or [])}
        return {}

    async def validate_cluster(self, **params: Any) -> OpResult:
        """Confirm every pool host exposes the same storage interfaces.

        For the active protocol we discover the protocol-relevant interfaces on
        each pool host (NICs for iSCSI, NVMe source addresses for NVMe-TCP, FC HBA
        WWPNs for FC) and compare them with :func:`compare_node_interfaces`, so the
        chosen storage binding is valid pool-wide. A single-node pool always
        validates. Mock/dry-run uses the runner's synthetic per-host inventory.
        """
        proto = self._protocol()
        kind = {"iscsi": "nics", "nvme-tcp": "nvme_sources",
                "fc": "fc_hbas"}.get(proto, "nics")
        # For iSCSI (kind=="nics") only the storage-subnet NICs matter -- compare
        # those, not every VM tap/bridge/VLAN (which legitimately differ between
        # nodes). fc_hbas / nvme_sources are compared as-is.
        portals = (await self.ctx.array.get_data_interfaces(
            "nvme-tcp" if proto == "nvme-tcp" else "iscsi")
            if (kind == "nics" and self.ctx.array is not None) else [])
        nodes = await self.list_nodes()
        await self.ctx.emit(
            f"Validating {len(nodes)} pool node(s); comparing '{kind}' "
            f"(protocol={proto})")
        per_node: dict[str, list[str]] = {}
        for node in nodes:
            ifaces = await self.ctx.runner.discover_interfaces(
                node.host, kind, **self._discover_kwargs())
            if kind == "nics":
                ifaces = nics_on_array_subnets(ifaces, portals)
            per_node[node.name] = sorted(
                {o.get("value") for o in ifaces if o.get("value")})
            await self.ctx.emit(f"  {node.name}: {per_node[node.name]}")
        consistent, detail = compare_node_interfaces(per_node)
        data = {"protocol": proto, "interface_kind": kind,
                "nodes": [n.to_dict() for n in nodes], "per_node": per_node,
                "detail": detail}
        if consistent:
            return OpResult.ok(
                f"Cluster consistent across {len(nodes)} node(s): {detail}", **data)
        return OpResult.fail(f"Cluster inconsistent: {detail}", **data)

    # ------------------------------------------------------------------ #
    # validate_connection (required)
    # ------------------------------------------------------------------ #
    async def validate_connection(self) -> OpResult:
        host = self._host()
        await self.ctx.emit(f"Validating XCP-ng pool master {host} ...")
        # `xe host-list` confirms a reachable XAPI/XCP-ng host (mock-safe).
        out = await self._ssh("xe host-list --minimal")
        await self.ctx.emit("Confirmed XCP-ng via `xe host-list`")
        array_info: dict[str, Any] = {}
        if self.ctx.array is not None:
            array_info = await self.ctx.array.info()
            await self.ctx.emit(f"FlashArray reachable: {array_info}")
        return OpResult.ok(f"Connected to {host}", host=host,
                           hosts=out, array=array_info)

    # ------------------------------------------------------------------ #
    # DEPLOY_PLUGIN: push the SMAPIv3 plugin to all hosts, enumerate, sr-create
    # ------------------------------------------------------------------ #
    async def deploy_integration(self, endpoint: str = "", sr_name: str = "",
                                 host_group: str = "", clobber: Any = False,
                                 eradicate: Any = False, **_: Any) -> OpResult:
        proto = self._protocol()
        sr_name = self._sr_name(sr_name)
        clobber = str(clobber).lower() in ("1", "true", "yes", "on")
        eradicate = str(eradicate).lower() in ("1", "true", "yes", "on")
        host_group = host_group or self.ctx.target.get("host_group", "")
        endpoint = endpoint or (self.ctx.array.endpoint if self.ctx.array else "")
        # Prefer the array's original token; only mint if one isn't already available.
        token = self.ctx.resolve_token() or ""
        await self.ctx.emit(
            f"Deploying purefa ('{_SR_TYPE}') SMAPIv3 plugin; SR='{sr_name}', "
            f"proto={proto}")

        if self.ctx.dry_run:
            return OpResult.ok(
                f"[dry-run] would deploy the purefa SMAPIv3 plugin and create SR "
                f"'{sr_name}'",
                artifacts={"sr_name": sr_name, "sr_type": _SR_TYPE})

        # Validate host objects exist BEFORE creating the SR: a FlashArray-backed SR
        # is only usable once its host group has member hosts to connect VDIs to.
        # (The wizard registers the pool hosts first; this guards the standalone
        # deploy + gives a clear "register hosts first" error.)
        hg_check = await self.require_host_objects(host_group)
        if not hg_check.success:
            return OpResult.fail(
                hg_check.message,
                artifacts={"sr_name": sr_name, "host_group": host_group})
        await self.ctx.emit(f"[preflight] {hg_check.message}")

        # Reuse the array token if present, else mint a scoped token for the SR.
        if not token and self.ctx.array is not None:
            try:
                token = await self.ctx.array.create_api_token("xcpng-puresr")
            except Exception as e:  # noqa: BLE001
                await self.ctx.emit(f"Token mint failed ({e}); continuing without")

        # The SMAPIv3 plugin must exist on EVERY pool host (any host may attach the
        # VDI), so we fan the install out across the pool.
        nodes = await self.list_nodes()

        # PREFLIGHT: the purefa SMAPIv3 plugin runs ON each pool host and talks to
        # the array's MANAGEMENT endpoint for provisioning/snapshots/status. If a
        # host can't reach it, the SR would create but fail to operate. Verify up
        # front.
        target = self._array_mgmt_target(endpoint)
        if target is not None:
            host_ip, port = target
            unreachable = await self._unreachable_nodes(nodes, host_ip, port)
            if unreachable:
                return OpResult.fail(
                    f"FlashArray management endpoint {host_ip}:{port} is not "
                    f"reachable from: {', '.join(unreachable)}. The purefa SMAPIv3 "
                    f"plugin runs on each pool host and needs the array management IP "
                    f"reachable. Fix routing/firewall (or the endpoint) and re-run.",
                    artifacts={"endpoint": f"{host_ip}:{port}",
                               "unreachable_nodes": unreachable})
            await self.ctx.emit(
                f"Array management {host_ip}:{port} reachable from all "
                f"{len(nodes)} pool host(s)")

        # Install the SMAPIv3 volume plugin on EVERY pool host. XCP-ng 8.3 uses
        # SMAPIv3 (xapi-storage-script): drop the plugin dir under volume/, create
        # the per-method entrypoint symlinks (link.sh), and the script daemon
        # discovers it via inotify. (SMAPIv1 dropped-in drivers are not loaded on
        # 8.3, which is why the old approach hit "driver not recognised".)
        await self.ctx.emit(
            f"Installing SMAPIv3 volume plugin {_SMAPIV3_PLUGIN_NAME} + custom "
            f"'purefa' datapath plugin on {len(nodes)} pool host(s)")
        await self._install_smapiv3_plugin(nodes)

        # XAPI enumerates SM types from the storage-script on toolstack start;
        # restart it so `xe sm-list`/sr-create see the new 'purefa' plugin.
        await self.ctx.emit("Restarting toolstack on all pool hosts to register "
                            f"the '{_SR_TYPE}' SMAPIv3 plugin ...")
        for node in nodes:
            # `xe-toolstack-restart` bounces xapi + helpers; redirect its output to
            # a file so the restarted daemons don't inherit the SSH channel's fds
            # and hang the read loop. cat the file back for the log.
            await self._ssh(
                "xe-toolstack-restart </dev/null >/tmp/phif-tsr.out 2>&1; "
                "rc=$?; cat /tmp/phif-tsr.out 2>/dev/null; exit $rc",
                check=False, host=node.host, timeout=180)
        if not await self._wait_xapi_ready(self._host()):
            return OpResult.fail(
                "xapi did not come back after toolstack restart on the pool "
                "master; re-run deploy once the toolstack is up.")

        # Confirm XAPI registered the SMAPIv3 plugin. If not, run the plugin's
        # Query directly to surface the real error (import failure, missing
        # xapi.storage libs, bad link.sh) instead of an opaque sr-create failure.
        if not self._mock_mode():
            # xapi-storage-script can finish enumerating the SMAPIv3 plugin a few
            # seconds AFTER xapi reports ready, so POLL `xe sm-list` rather than
            # checking once. A one-shot check races the enumeration and can fail
            # spuriously on a slower host even though the plugin registers fine
            # moments later (the likely reason the same build passes on one pool
            # and fails on another). Poll up to ~90s. `params=type --minimal`
            # returns the comma-joined TYPE list, so match membership.
            probe = (await self._ssh(
                f"for i in $(seq 1 18); do "
                "xe sm-list params=type --minimal 2>/dev/null | tr ',' '\\n' "
                f"| grep -qx {_SR_TYPE} && {{ echo PHIF_REGISTERED; break; }}; "
                "sleep 5; done; "
                "xe sm-list params=type --minimal 2>&1",
                check=False, timeout=150)).strip()
            smtypes = probe.replace("PHIF_REGISTERED", "").strip()
            if "PHIF_REGISTERED" not in probe:
                svc = (await self._ssh(
                    "systemctl status xapi-storage-script.service --no-pager 2>&1 "
                    "| tail -15", check=False)).strip()
                ls = (await self._ssh(
                    f"ls -la {_SMAPIV3_DEST_DIR} 2>&1", check=False)).strip()
                # Run Plugin.Query directly: this surfaces the REAL reason a plugin
                # fails to enumerate — an ImportError (xapi.storage libs absent /
                # wrong interpreter), a syntax error, or a missing interpreter —
                # which the service status alone never shows.
                query = (await self._ssh(
                    f"{_SMAPIV3_DEST_DIR}/Plugin.Query phif </dev/null 2>&1 | head -20; "
                    f"echo rc=$?", check=False)).strip()
                interp = (await self._ssh(
                    f"head -1 {_SMAPIV3_DEST_DIR}/plugin.py; "
                    "command -v python python3 2>&1 || echo 'no python/python3'",
                    check=False)).strip()
                journal = (await self._ssh(
                    "journalctl -u xapi-storage-script.service --no-pager -n 25 "
                    "2>&1 | tail -25", check=False)).strip()
                await self.ctx.emit(f"[diag] registered SM types: {smtypes}")
                await self.ctx.emit(f"[diag] Plugin.Query direct run: {query}")
                await self.ctx.emit(f"[diag] interpreter: {interp}")
                await self.ctx.emit(f"[diag] storage-script service: {svc}")
                await self.ctx.emit(f"[diag] storage-script journal: {journal}")
                await self.ctx.emit(f"[diag] plugin dir: {ls}")
                return OpResult.fail(
                    f"XAPI did not register SMAPIv3 plugin '{_SR_TYPE}'. Registered "
                    f"types: {smtypes}. See [diag] lines — the 'Plugin.Query direct "
                    "run' output is the root cause (e.g. a missing interpreter or "
                    "import error).",
                    artifacts={"sr_type": _SR_TYPE, "registered_types": smtypes})
            await self.ctx.emit(
                f"XAPI registered SMAPIv3 plugin '{_SR_TYPE}'")

        # 3) Create the SR ONCE on the pool master (shared=true => pool-wide; XAPI
        #    creates + plugs a PBD on each host). Idempotent: skip if it already
        #    exists. Surface a real failure instead of hiding it.
        existing = (await self._ssh(
            f"xe sr-list name-label='{sr_name}' --minimal", check=False)).strip()
        if existing and clobber:
            # Forget EVERY existing SR with this name (repeated/failed runs can
            # leave duplicates) before recreating. sr-forget removes them from XAPI
            # WITHOUT eradicating the array volumes (non-destructive); pbd-unplug
            # first so forget succeeds.
            uuids = [u.strip() for u in existing.split(",") if u.strip()]
            await self.ctx.emit(
                f"clobber: forgetting {len(uuids)} existing SR(s) named "
                f"'{sr_name}' before recreating")
            for u in uuids:
                await self._forget_sr(u)
            existing = ""
        if existing:
            await self.ctx.emit(
                f"SR '{sr_name}' already exists ({existing}); "
                "enable 'Clobber existing SR' to recreate it")
            sr_uuid = existing.split(",")[0]
        else:
            # Redirect ALL output to a file (not the SSH channel): sr-create's
            # SMAPIv3 PBD-plug can spawn/restart datapath helpers that inherit the
            # channel's stdout fd and never close it -- which hung the wizard here
            # even though the SR was created. Writing to a file lets the channel
            # EOF as soon as xe returns; we cat the file back for the log/errors.
            # `timeout` is a backstop in case sr-create itself blocks.
            create_cmd = (
                f"timeout 240 xe sr-create type={_SR_TYPE} name-label='{sr_name}' "
                f"shared=true content-type=user "
                f"device-config:endpoint='{endpoint}' "
                f"device-config:token='{token}' "
                f"device-config:protocol={proto} "
                f"device-config:hostgroup='{host_group}' "
                f"device-config:eradicate={'true' if eradicate else 'false'} "
                "</dev/null >/tmp/phif-srcreate.out 2>&1; rc=$?; "
                "cat /tmp/phif-srcreate.out 2>/dev/null; exit $rc")
            out = await self._ssh(create_cmd, check=False, timeout=300)
            await self.ctx.emit(f"sr-create output: {out.strip() or '(none)'}")
            sr_uuid = (await self._ssh(
                f"xe sr-list name-label='{sr_name}' --minimal",
                check=False)).strip().split(",")[0]
            if not sr_uuid and self._mock_mode():
                sr_uuid = "mock-sr-uuid"  # no real xe in mock/dry-run
            if not sr_uuid:
                # The SMAPIv3 plugin method ran but failed -- its traceback goes to
                # SMlog, not to xe stdout. Capture the tail so the real cause (method
                # signature / exception / FA REST error) is visible in the job log.
                smlog = (await self._ssh(
                    "tail -400 /var/log/SMlog 2>/dev/null | "
                    "grep -aE 'purefa|SMAPIv3|Traceback|File \\\"|Error|Exception|"
                    "ImportError|stderr' | tail -50", check=False)).strip()
                await self.ctx.emit(f"[diag] SMlog tail:\n{smlog}")
                return OpResult.fail(
                    f"SR '{sr_name}' was not created (type={_SR_TYPE}). sr-create "
                    f"said: {out.strip() or '(no output)'}. SMlog tail in [diag] "
                    f"above shows the plugin-side error.",
                    artifacts={"sr_name": sr_name, "sr_type": _SR_TYPE,
                               "smlog": smlog})

        # 4) Ensure every host's PBD for this SR is plugged (so the SR shows
        #    attached/usable pool-wide, not just created).
        plugged = await self._plug_sr_pbds(sr_uuid)
        await self.ctx.emit(
            f"SR '{sr_name}' ({sr_uuid}) present; {plugged} PBD(s) plugged")
        return OpResult.ok(
            f"Deployed the purefa SMAPIv3 plugin and created SR '{sr_name}'",
            artifacts={"sr_name": sr_name, "sr_uuid": sr_uuid, "sr_type": _SR_TYPE,
                       "protocol": proto, "endpoint": endpoint,
                       "pbds_plugged": plugged})

    async def _wait_xapi_ready(self, host: str, attempts: int = 30,
                               delay: float = 3.0) -> bool:
        """Poll until xapi answers on ``host`` (after a toolstack restart).

        Returns True once ``xe host-list`` yields a uuid-looking response. In
        mock/dry-run xapi is never down, so return True immediately.
        """
        if self._mock_mode():
            return True
        for _ in range(attempts):
            out = await self._ssh("xe host-list --minimal", check=False, host=host)
            if out and re.search(r"[0-9a-f]{8}-[0-9a-f]{4}", out):
                return True
            await asyncio.sleep(delay)
        return False

    @staticmethod
    def _read_smapiv3_files() -> dict[str, str]:
        """Read the bundled SMAPIv3 volume-plugin files (filename -> contents)."""
        out: dict[str, str] = {}
        for fname in _SMAPIV3_FILES:
            with open(os.path.join(_SMAPIV3_SRC_DIR, fname), encoding="utf-8") as fh:
                out[fname] = fh.read()
        return out

    @staticmethod
    def _read_smapiv3_datapath_files() -> dict[str, str]:
        """Read the bundled SMAPIv3 datapath-plugin files (filename -> contents)."""
        out: dict[str, str] = {}
        for fname in _SMAPIV3_DP_FILES:
            with open(os.path.join(_SMAPIV3_DP_SRC_DIR, fname), encoding="utf-8") as fh:
                out[fname] = fh.read()
        return out

    @staticmethod
    def _read_smapiv3_hostplugin() -> str:
        """Read the bundled XAPI host plugin (pool-wide multipath flush/rescan)."""
        with open(_SMAPIV3_HOSTPLUGIN_SRC, encoding="utf-8") as fh:
            return fh.read()

    async def _install_smapiv3_plugin(self, nodes: list[ClusterNode]) -> None:
        """Push + (re)enumerate the SMAPIv3 volume/datapath/host plugins on ``nodes``.

        Extracted from :meth:`deploy_integration` so cluster reconcile can install
        the plugin on just the NEW pool hosts (default: every node). Idempotent and
        mock/dry-run safe (SSH is short-circuited by the runner in mock mode).
        """
        plugin_files = self._read_smapiv3_files()
        dp_files = self._read_smapiv3_datapath_files()
        hostplugin = self._read_smapiv3_hostplugin()
        for node in nodes:
            await self.ctx.emit(f"-> {node.name}: writing plugin files")
            # Volume plugin.
            await self._ssh(f"mkdir -p {_SMAPIV3_DEST_DIR}", check=False,
                            host=node.host)
            for fname, content in plugin_files.items():
                eof = "PHIF_%s_EOF" % fname.replace(".", "_").upper()
                await self._ssh(
                    f"cat > {_SMAPIV3_DEST_DIR}/{fname} <<'{eof}'\n{content}\n{eof}\n",
                    check=False, host=node.host)
            await self._ssh(f"sh {_SMAPIV3_DEST_DIR}/link.sh", check=False,
                            host=node.host)
            # Custom datapath plugin (raw block device -> Blkback).
            await self._ssh(f"mkdir -p {_SMAPIV3_DP_DEST_DIR}", check=False,
                            host=node.host)
            for fname, content in dp_files.items():
                eof = "PHIF_DP_%s_EOF" % fname.replace(".", "_").upper()
                await self._ssh(
                    f"cat > {_SMAPIV3_DP_DEST_DIR}/{fname} <<'{eof}'\n{content}\n{eof}\n",
                    check=False, host=node.host)
            await self._ssh(f"sh {_SMAPIV3_DP_DEST_DIR}/link.sh", check=False,
                            host=node.host)
            # XAPI host plugin for pool-wide multipath flush/rescan.
            await self._ssh(
                f"mkdir -p /etc/xapi.d/plugins; "
                f"cat > {_SMAPIV3_HOSTPLUGIN_DEST} <<'PHIF_HP_EOF'\n{hostplugin}\n"
                f"PHIF_HP_EOF\nchmod +x {_SMAPIV3_HOSTPLUGIN_DEST}",
                check=False, host=node.host)
            # Restart the storage-script daemon so the plugin is (re)enumerated.
            # Redirect all fds away from the SSH channel: restarted daemons that
            # inherit the channel's stdout keep it open and hang the read loop.
            await self._ssh(
                "{ systemctl restart xapi-storage-script.service || "
                "systemctl restart xapi-storage-script || true; } "
                "</dev/null >/dev/null 2>&1",
                check=False, host=node.host)

    @staticmethod
    def _sm_allow_cmd() -> str:
        """Shell to add our SR type to the /etc/xapi.conf `sm-plugins` allowlist.

        XCP-ng 8.3 only loads SM drivers named in this allowlist. Append-only and
        idempotent: if the line exists and lacks our type, append it (after backing
        the file up); if it already lists it, no-op; if there's no line at all, do
        NOT invent one (that would override xapi's built-in default and break stock
        SRs) -- just report so the operator can add it. Echoes the resulting line.
        """
        t = _SR_TYPE
        return (
            'C=/etc/xapi.conf; '
            'if grep -qE "^[[:space:]]*sm-plugins" "$C"; then '
            f'if grep -E "^[[:space:]]*sm-plugins" "$C" | grep -qw {t}; then '
            'echo "already-allowed"; else '
            f'cp -n "$C" "$C.phifbak"; sed -i -E "/^[[:space:]]*sm-plugins/ s/$/ {t}/" "$C"; '
            'echo "appended"; fi; else echo "NO_SM_PLUGINS_LINE"; fi; '
            'grep -E "^[[:space:]]*sm-plugins" "$C" || true')

    @staticmethod
    def _sm_revert_cmd() -> str:
        """Remove our type from the /etc/xapi.conf `sm-plugins` allowlist.

        Reverts the append done by an earlier SMAPIv1 attempt (not needed for the
        SMAPIv3 plugin). Idempotent: strips ' purefa' tokens from the line if
        present; harmless if absent.
        """
        t = _SR_TYPE
        return (
            'C=/etc/xapi.conf; '
            f'sed -i -E "/^[[:space:]]*sm-plugins/ s/ {t}\\b//g" "$C" 2>/dev/null '
            '|| true; grep -E "^[[:space:]]*sm-plugins" "$C" || true')

    async def _forget_sr(self, sr_uuid: str) -> None:
        """Unplug all PBDs for ``sr_uuid`` then sr-forget it (non-destructive).

        Forget removes the SR from XAPI without eradicating the array volumes, so
        it's safe for cleaning up a half-created/broken SR before recreating.
        """
        if not sr_uuid:
            return
        await self._ssh(
            "for p in $(xe pbd-list sr-uuid=%s --minimal | tr ',' ' '); do "
            "xe pbd-unplug uuid=$p; done" % sr_uuid, check=False)
        await self._ssh(f"xe sr-forget uuid={sr_uuid}", check=False)

    async def _plug_sr_pbds(self, sr_uuid: str) -> int:
        """Plug any unplugged PBDs for ``sr_uuid`` across the pool. Returns count."""
        if not sr_uuid:
            return 0
        raw = await self._ssh(
            f"xe pbd-list sr-uuid={sr_uuid} params=uuid,currently-attached",
            check=False)
        plugged = 0
        # Parse the block output: pair each pbd uuid with its attached flag.
        pbd_uuid = None
        for line in (raw or "").splitlines():
            m = self._XE_FIELD_RE.match(line)
            if not m:
                continue
            key, val = m.group(1), m.group(2)
            if key == "uuid":
                pbd_uuid = val
            elif key == "currently-attached" and pbd_uuid:
                if val.strip().lower() != "true":
                    await self._ssh(f"xe pbd-plug uuid={pbd_uuid}", check=False)
                plugged += 1
                pbd_uuid = None
        return plugged

    # ------------------------------------------------------------------ #
    # HOST_REGISTER: FA host + host group from pool hosts' initiators
    # ------------------------------------------------------------------ #
    async def register_hosts(self, host_group: str = "", iqns: str = "",
                             nqns: str = "", wwns: str = "",
                             nodes_override: "list[ClusterNode] | None" = None,
                             **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        host_group = host_group or self.ctx.target.get("host_group", "")
        if not host_group:
            return OpResult.fail("host_group is required")
        proto = self._protocol()
        # NFS uses the stock XCP-ng NFS SR: there are no block initiators to
        # register on the array, so host registration is a no-op for NFS.
        if proto == "nfs":
            await self.ctx.emit(
                "NFS protocol: skipping FlashArray host registration "
                "(stock NFS SR has no block initiators)")
            return OpResult.ok(
                "NFS protocol: no host registration required",
                artifacts={"host_group": host_group, "protocol": "nfs",
                           "hosts": [], "node_count": 0, "skipped": True})
        iqn_list = [s.strip() for s in iqns.split(",") if s.strip()]
        nqn_list = [s.strip() for s in nqns.split(",") if s.strip()]
        wwn_list = [s.strip() for s in wwns.split(",") if s.strip()]
        # WWNs may also come from the target schema's host_wwns field.
        if not wwn_list:
            wwn_list = [s.strip()
                        for s in (self.ctx.target.get("host_wwns") or "").split(",")
                        if s.strip()]

        # CLUSTER FAN-OUT: register one FlashArray host PER pool host, all added
        # to the single shared host group. Per-host initiators are discovered on
        # each node (so each FA host carries that node's own IQN/NQN/WWNs).
        #
        # Single-host path: when the operator passes EXPLICIT initiators for the
        # active protocol, those apply to one array host (we can't sensibly split
        # one operator-typed initiator set across many nodes), so we register a
        # single master host named "<hg>-pool" carrying them. Likewise a one-node
        # pool registers a single "<hg>-pool" host. Otherwise we discover each
        # pool host's own initiators and register a host per node.
        explicit = {"iqns": iqn_list, "nqns": nqn_list, "wwns": wwn_list}
        explicit_for_proto = {
            "iscsi": bool(iqn_list), "nvme-tcp": bool(nqn_list),
            "fc": bool(wwn_list)}.get(proto, False)
        if nodes_override is not None:
            # Caller (reconcile_cluster) supplied the exact nodes to register; do
            # NOT collapse to the master even when explicit initiators are given.
            nodes = list(nodes_override)
        elif explicit_for_proto:
            nodes = [(await self.list_nodes())[0]]
        else:
            nodes = await self.list_nodes()
        single = len(nodes) == 1

        # Per-node initiator resolution. For a single-node pool, explicit values
        # win; otherwise we discover each node's own initiators over SSH.
        per_host: list[dict[str, Any]] = []
        for node in nodes:
            resolved = {"iqns": [], "nqns": [], "wwns": []}
            if single:
                resolved.update({k: list(v) for k, v in explicit.items()})
            need = (
                (proto == "iscsi" and not resolved["iqns"])
                or (proto == "nvme-tcp" and not resolved["nqns"])
                or (proto == "fc" and not resolved["wwns"])
            )
            if need:
                await self.ctx.emit(
                    f"Auto-discovering pool master initiators over SSH "
                    f"(node={node.name} proto={proto})"
                    if single else
                    f"Discovering initiators on pool host {node.name} (proto={proto})")
                found = await self.ctx.runner.discover_initiators(
                    node.host, **self._discover_kwargs())
                resolved.update(self._initiator_field(found, proto))
            per_host.append({"node": node, "initiators": resolved})

        idlist_label = {
            "iscsi": [h["initiators"]["iqns"] for h in per_host],
            "nvme-tcp": [h["initiators"]["nqns"] for h in per_host],
            "fc": [h["initiators"]["wwns"] for h in per_host],
        }.get(proto)
        await self.ctx.emit(
            f"Registering host group {host_group} across {len(nodes)} pool host(s) "
            f"({proto} initiators={idlist_label})")
        if self.ctx.dry_run:
            return OpResult.ok(
                f"[dry-run] would register host group {host_group} "
                f"with {len(nodes)} host(s)",
                artifacts={"host_group": host_group,
                           "node_count": len(nodes)})

        # For a single-node pool keep the historical "<hg>-pool" FA host name; for
        # a real multi-host pool derive a distinct FA host name per node so each
        # node's initiators land on its own array host. For FC we register hosts
        # BY WWN only (no IQN/NQN).
        #
        # apply_host_group (shared across connectors) reuses pre-existing FA hosts
        # by initiator, ADOPTS an existing host group when the pool hosts already
        # belong to one, and returns a descriptive ``conflict`` (with no mutation)
        # if the hosts span multiple groups.
        specs: list[dict[str, Any]] = []
        for entry in per_host:
            node = entry["node"]
            ini = entry["initiators"]
            if single and nodes_override is None:
                name = self._fa_name(f"{host_group}-pool")
            else:
                # Per-node FA host name. reconcile_cluster relies on this scheme
                # (`_fa_host_for_node`) to match expected members.
                name = self._fa_name(f"{host_group}-{node.name}")
            if proto == "fc":
                specs.append({"name": name, "wwns": ini["wwns"] or None})
            else:
                specs.append({"name": name, "iqns": ini["iqns"] or None,
                              "nqns": ini["nqns"] or None, "wwns": ini["wwns"] or None})

        res = await self.apply_host_group(host_group, specs)
        if res.get("conflict"):
            return OpResult.fail(res["conflict"],
                                 artifacts={"host_group": host_group, "protocol": proto})
        host_group = res["host_group"]   # adopt the effective (possibly existing) group
        host_names = res["hosts"]
        await self.ctx.emit(
            f"Host group {host_group} ready with hosts {host_names}")
        return OpResult.ok(
            f"Registered {len(host_names)} pool host(s) in host group {host_group}",
            artifacts={"host_group": host_group, "host": host_names[0],
                       "hosts": host_names, "protocol": proto,
                       "adopted_host_group": res["adopted"],
                       "node_count": len(nodes)})

    # ------------------------------------------------------------------ #
    # CONNECTIVITY: pool multipathing + Everpure ALUA stanza + transport
    # ------------------------------------------------------------------ #
    @staticmethod
    def _as_list(value: Any) -> list[str]:
        """Normalise a MULTISELECT value (list, comma-string, or None) to a list."""
        if value is None:
            return []
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return [str(v).strip() for v in value if str(v).strip()]

    async def _write_multipath_conf(self, host: str = "") -> None:
        """Enable pool multipathing + install the Everpure ALUA multipath stanza.

        On XCP-ng the Everpure-recommended location is
        ``/etc/multipath/conf.d/custom.conf`` (persists across updates) -- NOT
        ``/etc/multipath.conf`` and NEVER ``/etc/multipath.xenserver/...``.
        Without a proper PURE/FlashArray device stanza + ``find_multipaths``,
        multipathd doesn't group the iSCSI/FC paths to one wwid. We also enable
        pool-wide multipathing via ``xe host-param-set
        other-config:multipathing=true`` (+ ``multipathhandle=dmp``) and restart
        multipathd. Analogous to the Proxmox connector's ``_write_multipath_conf``
        but using the XCP-ng path + the ``xe`` pool toggle.

        Runs against ``host`` (a specific pool node) when supplied, else the pool
        master. The conf drop-in + multipathd restart MUST run on every pool host;
        the ``xe host-param-set`` pool toggle is pool-wide + idempotent.
        """
        await self.ctx.emit(
            f"Enabling pool multipathing + writing Everpure stanza {_MULTIPATH_CONF_PATH}"
            + (f" on {host}" if host else ""))
        # Pool toggle + dirs are plain commands, safe to && -join.
        await self._ssh_script([
            "systemctl enable --now multipathd",
            "MPHOST=$(xe host-list --minimal)",
            "for h in ${MPHOST//,/ }; do "
            "xe host-param-set uuid=$h other-config:multipathing=true; "
            "xe host-param-set uuid=$h other-config:multipathhandle=dmp; done",
            "mkdir -p /etc/multipath/conf.d",
        ], check=False, host=host)
        # Write the conf via its OWN command. A heredoc must NEVER be an element of
        # a && -joined script: the join appends " && <next cmd>" onto the EOF line,
        # so the terminator is no longer a bare "EOF", the heredoc never closes, and
        # the trailing commands get written into the file as junk. The quoted
        # 'EOF' prevents shell expansion; _MULTIPATH_CONF already ends in newline.
        await self._ssh(
            f"cat > {_MULTIPATH_CONF_PATH} <<'EOF'\n{_MULTIPATH_CONF}EOF\n",
            check=False, host=host)
        await self._ssh("systemctl restart multipathd", check=False, host=host)

    async def _setup_host_connectivity(
            self, host: str, proto: str, portal_list: list[str],
            target_iqn: str, subsystem_nqn: str, nic_list: list[str],
            nvme_src_list: list[str], nvme_options: str,
            hba_list: list[str]) -> None:
        """Run the multipath + transport setup on a SINGLE pool host.

        Fanned out across the pool by :meth:`setup_connectivity`. Writes the Everpure
        ALUA multipath drop-in, then connects the transport: iSCSI/NVMe-TCP log in
        over IP (optionally pinned to selected local NICs/sources); FC performs an
        FC/SCSI rescan only (zoning is switch-side, no login).
        """
        # Pool-level multipathing + Everpure ALUA stanza (persists across updates in
        # /etc/multipath/conf.d/pure.conf -- never edit multipath.xenserver).
        await self._write_multipath_conf(host=host)

        if proto == "iscsi":
            await self._ssh("systemctl enable --now iscsid", check=False, host=host)
            # INTERFACE BINDING (iSCSI): create/bind an iscsiadm iface per
            # selected NIC so sessions egress only via those local interfaces,
            # then scope discovery/login to those ifaces (`-I <iface>`). On
            # XCP-ng an lvmoiscsi/custom SR can additionally pin the local NIC
            # via device-config, so we also persist the NICs into the purefa SR's
            # device-config below.
            # Validated on the 8.3 pool: binding sessions to a per-NIC iscsiadm
            # iface (iface.net_ifacename) and scoping discovery/login with
            # `-I <iface>` egresses sessions only via the selected interfaces;
            # the SR device-config:iscsi_nics key persists the same selection.
            iface_args = ""
            if nic_list:
                for nic in nic_list:
                    iface = f"pure-{nic}"
                    await self._ssh_script([
                        f"iscsiadm -m iface -I {iface} -o new",
                        f"iscsiadm -m iface -I {iface} -o update "
                        f"-n iface.net_ifacename -v {nic}",
                    ], check=False, host=host)
                # Multi-NIC iSCSI ARP-flux fix on the selected NICs (dom0), persisted
                # + live, so dual-NIC iSCSI on a shared subnet doesn't mis-bind paths.
                await self._ssh(arp_flux_cmd(nic_list), check=False, host=host)
                iface_args = "".join(f" -I pure-{n}" for n in nic_list)
            for portal in portal_list:
                await self._ssh(
                    f"iscsiadm -m discovery -t sendtargets -p {portal}:{_ISCSI_PORT}"
                    f"{iface_args}",
                    check=False, host=host)
            login = ("iscsiadm -m node "
                     f"{('-T ' + target_iqn) if target_iqn else ''}"
                     f"{iface_args} --login")
            await self._ssh(login, check=False, host=host)
        elif proto == "nvme-tcp":
            # INTERFACE BINDING (NVMe-TCP): connect once per selected host
            # source (`-w <host-traddr>`) so the controller is reached via the
            # chosen local source address; persist any extra connect options.
            src_args = [f" -w {s}" for s in nvme_src_list] or [""]
            opt_suffix = f" {nvme_options}" if nvme_options else ""
            for portal in portal_list:
                for src in src_args:
                    await self._ssh(
                        f"nvme connect-all -t tcp -a {portal} -s {_NVME_TCP_PORT} "
                        f"{('-n ' + subsystem_nqn) if subsystem_nqn else ''}"
                        f"{src}{opt_suffix}",
                        check=False, host=host)
        elif proto == "fc":
            # Fibre Channel: there is NO iSCSI/NVMe IP login. SAN ZONING IS A
            # PREREQUISITE -- the pool hosts' FC HBA WWNs must already be zoned to
            # the FlashArray's FC target ports on the switch fabric (done
            # switch-side, outside this connector). Here we only force the host
            # to (re)discover LUNs the fabric now exposes, then refresh multipath.
            await self.ctx.emit(
                f"FC: assuming SAN zoning is in place; performing FC/SCSI rescan "
                f"(no iscsiadm/nvme login) on {host}")
            # Prefer rescan-scsi-bus.sh when present; otherwise fall back to
            # issue_lip on each fc_host + a SCSI host scan via sysfs.
            # Validated on the 8.3 dom0: sg3_utils (rescan-scsi-bus.sh) ships and
            # is on PATH; the sysfs issue_lip/scan fallback is always available.
            await self._ssh_script([
                "rescan-scsi-bus.sh -a 2>/dev/null || { "
                "for h in /sys/class/fc_host/host*/issue_lip; do "
                "echo 1 > $h 2>/dev/null || true; done; "
                "for s in /sys/class/scsi_host/host*/scan; do "
                "echo '- - -' > $s 2>/dev/null || true; done; }",
                "multipath -r",
            ], check=False, host=host)
            # INTERFACE BINDING (FC): no host-side login to bind; the selected
            # HBAs are pinned via the SR device-config (zoning is external).
            if hba_list:
                await self.ctx.emit(
                    f"FC: pinning storage path to HBAs {hba_list} "
                    "(persisted to SR device-config; zoning is switch-side)")

    async def setup_connectivity(self, portals: str = "", target_iqn: str = "",
                                 subsystem_nqn: str = "",
                                 iscsi_nics: Any = None, nvme_sources: Any = None,
                                 nvme_options: str = "", fc_hbas: Any = None,
                                 nodes_override: "list[ClusterNode] | None" = None,
                                 **_: Any) -> OpResult:
        proto = self._protocol()
        # NFS uses the stock XCP-ng NFS SR: no iSCSI/NVMe login, no multipath/ALUA
        # block setup. The NFS SR is plugged pool-wide by `xe sr-create` in
        # provision_nfs, so there is nothing host-side to configure here.
        if proto == "nfs":
            await self.ctx.emit(
                "NFS protocol: skipping block connectivity setup (no iSCSI/NVMe "
                "login, no multipath); the stock NFS SR plugs PBDs pool-wide")
            return OpResult.ok(
                "NFS protocol: no block connectivity required",
                artifacts={"protocol": "nfs", "portals": [], "binding": {},
                           "skipped": True})
        portal_list = [p.strip() for p in portals.split(",") if p.strip()]
        # INTERFACE BINDING: normalise the per-protocol selections. Only the
        # binding for the ACTIVE protocol is applied below.
        nic_list = self._as_list(iscsi_nics)
        nvme_src_list = self._as_list(nvme_sources)
        hba_list = self._as_list(fc_hbas)
        binding: dict[str, Any] = {}
        if proto == "iscsi" and nic_list:
            binding["iscsi_nics"] = nic_list
        elif proto == "nvme-tcp" and (nvme_src_list or nvme_options):
            if nvme_src_list:
                binding["nvme_sources"] = nvme_src_list
            if nvme_options:
                binding["nvme_options"] = nvme_options
        elif proto == "fc" and hba_list:
            binding["fc_hbas"] = hba_list

        # Discover the array's portal IPs + target IQN/NQN from the FlashArray
        # when the operator didn't supply them (FC has no portals -- zoning only).
        if self.ctx.array is not None and proto in ("iscsi", "nvme-tcp"):
            service = "nvme-tcp" if proto == "nvme-tcp" else "iscsi"
            if not portal_list:
                portal_list = await self.ctx.array.get_data_interfaces(service)
                await self.ctx.emit(
                    f"Discovered array {service} portals: {portal_list}")
            if proto == "iscsi" and not target_iqn:
                target_iqn = (await self.ctx.array.get_target_ports()).get("iqn") or ""
                await self.ctx.emit(
                    f"Discovered array iSCSI target IQN: {target_iqn}")
            if proto == "nvme-tcp" and not subsystem_nqn:
                subsystem_nqn = (
                    await self.ctx.array.get_target_ports()).get("nqn") or ""
                await self.ctx.emit(
                    f"Discovered array NVMe subsystem NQN: {subsystem_nqn}")

        # Fail fast with an actionable message when an IP transport has no portals
        # (e.g. the array is FC / NVMe-FC only, or iSCSI/NVMe-TCP networking isn't
        # configured on it) -- otherwise iscsiadm/nvme would fail cryptically.
        if proto in ("iscsi", "nvme-tcp") and not portal_list and not self.ctx.dry_run:
            return OpResult.fail(
                f"No {proto} portal IPs found on the array. Its {proto} interfaces "
                f"have no IP configured (this array may be Fibre Channel / NVMe-FC "
                f"only). Configure {proto} networking on the FlashArray, switch the "
                f"hypervisor protocol to 'fc', or pass portals explicitly.")
        if proto == "iscsi" and not target_iqn and not self.ctx.dry_run:
            return OpResult.fail(
                "No iSCSI target IQN found on the array (no iSCSI ports). This array "
                "appears to be Fibre Channel / NVMe-FC only -- switch the hypervisor "
                "protocol to 'fc'.")

        await self.ctx.emit(f"Configuring {proto} connectivity to {portal_list}")
        if binding:
            await self.ctx.emit(f"Interface binding ({proto}): {binding}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would configure {proto} connectivity",
                               artifacts={"protocol": proto, "portals": portal_list,
                                          "binding": binding})

        # CLUSTER FAN-OUT: the multipath drop-in + transport login/rescan must run
        # on EVERY pool host (each dom0 mounts the PBD locally). The array-side
        # discovery above happens once; here we repeat the host-side work per node.
        nodes = (list(nodes_override) if nodes_override is not None
                 else await self.list_nodes())
        await self.ctx.emit(
            f"Configuring connectivity on {len(nodes)} pool host(s)")
        for node in nodes:
            await self._setup_host_connectivity(
                node.host, proto, portal_list, target_iqn, subsystem_nqn,
                nic_list, nvme_src_list, nvme_options, hba_list)

        # Persist the selected binding into the purefa SR's device-config so the
        # SMAPIv3 plugin consumes it on attach. device-config:* keys map to the
        # plugin's connection config (iscsi_nics / nvme_sources / nvme_options /
        # fc_hbas). xe sr-param-set is idempotent + re-runnable.
        # Validated on the 8.3 pool: `xe sr-param-set device-config:*` updates the
        # SR's stored device-config, and the SMAPIv3 plugin re-reads the persisted
        # connection config (iscsi_nics / nvme_sources / nvme_options / fc_hbas) on
        # the next Volume.attach, so no pbd-unplug/plug cycle is required for the
        # driver to pick up the refreshed binding.
        if binding:
            sr_name = self._sr_name()
            param_args = " ".join(
                f"device-config:{k}='{','.join(v) if isinstance(v, list) else v}'"
                for k, v in binding.items())
            await self._ssh_script([
                f"SR_UUID=$(xe sr-list name-label='{sr_name}' --minimal)",
                f"xe sr-param-set uuid=$SR_UUID {param_args}",
            ], check=False)
            await self.ctx.emit(
                f"Persisted interface binding into SR '{sr_name}' device-config")

        await self.ctx.emit(f"{proto} connectivity + multipath configured")
        return OpResult.ok(f"{proto} connectivity configured",
                           artifacts={"protocol": proto, "portals": portal_list,
                                      "binding": binding})

    # ------------------------------------------------------------------ #
    # PROVISION_VOLUME: create a VDI backed by a new FA volume mapped to VM
    # ------------------------------------------------------------------ #
    async def provision(self, name: str, size: str = "100G", host_group: str = "",
                        sr_name: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        host_group = host_group or self.ctx.target.get("host_group", "")
        sr_name = self._sr_name(sr_name)
        await self.ctx.emit(
            f"Provisioning VDI '{name}' ({size}) on SR '{sr_name}' "
            f"(dedicated FlashArray volume)")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would provision VDI {name}",
                               artifacts={"volume": name, "sr_name": sr_name})

        # Array-side: each VDI is its own volume, connected to the host group.
        await self.ctx.array.create_volume(name, size)
        if host_group:
            await self.ctx.array.connect_volume(host_group, name)
            await self.ctx.emit(f"Connected volume {name} to host group {host_group}")

        # XAPI-side: create the VDI on the purefa SR (the SMAPIv3 plugin maps the
        # array volume directly to the VM as a raw multipath block device).
        await self._ssh_script([
            f"SR_UUID=$(xe sr-list name-label='{sr_name}' --minimal)",
            f"xe vdi-create sr-uuid=$SR_UUID name-label='{name}' "
            f"virtual-size={size} type=user",
        ], check=False)
        await self.ctx.emit(f"Provisioned VDI {name}")
        return OpResult.ok(f"Provisioned {name}",
                           artifacts={"volume": name, "sr_name": sr_name})

    # ------------------------------------------------------------------ #
    # SNAPSHOT / CLONE: array-native via ctx.array (routed through the plugin)
    # ------------------------------------------------------------------ #
    async def snapshot(self, volume: str, vdi_uuid: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Array snapshot of VDI/volume {volume}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would snapshot {volume}")
        # `xe vdi-snapshot` routes through the purefa plugin's Volume.snapshot,
        # which performs the FlashArray snapshot. ctx.array mirrors that here.
        await self.ctx.array.create_snapshot(volume)
        if vdi_uuid:
            await self._ssh(f"xe vdi-snapshot uuid={vdi_uuid}", check=False)
        return OpResult.ok(f"Snapshot of {volume} created", artifacts={"volume": volume})

    async def clone(self, source: str, dest: str, vdi_uuid: str = "",
                    **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Array clone (volume copy) {source} -> {dest}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would clone {source} -> {dest}")
        # `xe vdi-clone` routes through the plugin's Volume.clone (FA volume copy).
        await self.ctx.array.clone_volume(source, dest)
        if vdi_uuid:
            await self._ssh(f"xe vdi-clone uuid={vdi_uuid} new-name-label='{dest}'",
                            check=False)
        return OpResult.ok(f"Cloned {source} -> {dest}", artifacts={"volume": dest})

    # ------------------------------------------------------------------ #
    # RESIZE: FA extend + xe vdi-resize
    # ------------------------------------------------------------------ #
    async def resize(self, volume: str, size: str, vdi_uuid: str = "",
                     **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Extending FlashArray volume {volume} to {size}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would resize {volume} to {size}")
        await self.ctx.array.extend_volume(volume, size)
        if vdi_uuid:
            await self._ssh(f"xe vdi-resize uuid={vdi_uuid} disk-size={size}",
                            check=False)
        await self.ctx.emit(f"Resized {volume} to {size}")
        return OpResult.ok(f"Resized {volume} to {size}", artifacts={"volume": volume})

    # ------------------------------------------------------------------ #
    # VM management (cross-hypervisor migration)
    #
    # The purefa SMAPIv3 SR presents each FlashArray volume as a VDI, so the SAME
    # array volume is surfaced on the destination pool by mapping it to the host
    # group and `xe sr-scan`; the VDI is then attached with `xe vbd-create`. No
    # data moves. Some low-level `xe vm-create` parameters are fiddly across XCP-ng
    # versions and are marked TODO(validate-on-hardware).
    # ------------------------------------------------------------------ #
    @classmethod
    def _parse_records(cls, out: str) -> list[dict[str, str]]:
        """Parse default (non-minimal) ``xe *-list params=...`` block output into a
        list of dicts, starting a new record on each ``uuid`` field."""
        records: list[dict[str, str]] = []
        cur: dict[str, str] = {}
        for line in (out or "").splitlines():
            m = cls._XE_FIELD_RE.match(line)
            if not m:
                continue
            key, val = m.group(1), m.group(2)
            if key == "uuid" and cur:
                records.append(cur)
                cur = {}
            cur[key] = val
        if cur:
            records.append(cur)
        return records

    async def _xe_get(self, obj: str, uuid: str, param: str) -> str:
        return (await self._ssh(
            f"xe {obj}-param-get uuid={uuid} param-name={param}",
            check=False)).strip()

    async def list_vms(self) -> list[dict[str, Any]]:
        if self._is_mock_or_dry():
            return [{"id": "vm-uuid-0", "name": "mock-vm",
                     "power_state": "running", "vcpus": 2,
                     "memory_bytes": 2 * 1024**3, "disk_count": 1, "nic_count": 1}]
        out = await self._ssh(
            "xe vm-list is-control-domain=false is-a-snapshot=false "
            "params=uuid,name-label,power-state", check=False)
        vms: list[dict[str, Any]] = []
        for r in self._parse_records(out):
            ps = (r.get("power-state") or "").lower()
            vms.append({"id": r.get("uuid", ""), "name": r.get("name-label", ""),
                        "power_state": "running" if ps == "running" else "stopped"})
        return vms

    async def list_networks(self) -> list[dict[str, Any]]:
        if self._is_mock_or_dry():
            return [{"id": "net-uuid-0", "name": "Pool-wide network associated with eth0",
                     "kind": "network"}]
        out = await self._ssh("xe network-list params=uuid,name-label", check=False)
        return [{"id": r.get("uuid", ""), "name": r.get("name-label", ""),
                 "kind": "network"}
                for r in self._parse_records(out) if r.get("uuid")]

    async def list_placements(self) -> list[dict[str, Any]]:
        """The XCP-ng pool + its Everpure (type=purefa) SR(s). A connection is one pool,
        so a single cluster entry; only purefa SRs are offered as storage."""
        if self._is_mock_or_dry():
            return [{"cluster": {"id": "pool-mock", "name": "xcp-pool"},
                     "storage": [{"id": "sr-mock", "name": "purefa", "kind": "sr"}]}]
        pools = self._parse_records(
            await self._ssh("xe pool-list params=uuid,name-label", check=False))
        pool = pools[0] if pools else {}
        srs = self._parse_records(
            await self._ssh("xe sr-list type=purefa params=uuid,name-label", check=False))
        storage = [{"id": s.get("uuid", ""), "name": s.get("name-label") or s.get("uuid", ""),
                    "kind": "sr"} for s in srs if s.get("uuid")]
        if not storage:
            return []
        return [{"cluster": {"id": pool.get("uuid") or "pool",
                             "name": pool.get("name-label") or "XCP-ng pool"},
                 "storage": storage}]

    async def power_state(self, vm_ref: str) -> str:
        if self._is_mock_or_dry():
            return "running"
        ps = (await self._xe_get("vm", vm_ref, "power-state")).lower()
        if ps == "running":
            return "running"
        if ps in ("halted", "suspended", "paused"):
            return "stopped"
        return "unknown"

    async def capture_vm_spec(self, vm_ref: str) -> "VmSpec":
        from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec

        sr_name = self._sr_name()
        if self._is_mock_or_dry():
            return VmSpec(
                name="mock-vm", source_ref=str(vm_ref), vcpus=2,
                memory_bytes=2 * 1024**3, firmware="bios",
                disks=[DiskSpec(DiskIdentity(fa_volume="vdi-mock-0"),
                                bus="xvd", order=0, boot=True,
                                source_ref="vbd-uuid-0")],
                nics=[NicSpec(mac="AA:BB:CC:00:00:01", source_network="net-uuid-0",
                              model="virtio", order=0)])

        name = await self._xe_get("vm", vm_ref, "name-label")
        vcpus = int((await self._xe_get("vm", vm_ref, "VCPUs-at-startup")) or "1")
        mem = int((await self._xe_get("vm", vm_ref, "memory-static-max")) or "0")
        platform = (await self._xe_get("vm", vm_ref, "platform")).lower()
        firmware = "uefi" if "uefi" in platform else "bios"
        secure_boot = "secureboot: true" in platform

        # Disks: VBDs of type Disk -> their VDIs -> the purefa SR volume name.
        disks: list[DiskSpec] = []
        vbd_out = await self._ssh(
            f"xe vbd-list vm-uuid={vm_ref} type=Disk "
            f"params=uuid,vdi-uuid,bootable,userdevice", check=False)
        for r in self._parse_records(vbd_out):
            vdi = r.get("vdi-uuid", "")
            if not vdi or vdi == "<not in database>":
                continue
            # The FlashArray volume name is the VDI's LOCATION (what the purefa
            # plugin uses as the array object), NOT its name-label — for a
            # plugin-created VDI the name-label is just the VDI uuid, while the
            # location is "<vg>/<sr_id>-<uuid>". Using name-label made native XCP
            # VMs' disks fail to resolve to any FlashArray volume.
            vol = await self._xe_get("vdi", vdi, "location")
            vdi_sr = await self._xe_get("vdi", vdi, "sr-name-label")
            if vdi_sr != sr_name:
                raise ConnectionValidationError(
                    f"VDI {vdi} is on SR {vdi_sr!r}, not the FlashArray SR "
                    f"{sr_name!r}; cannot migrate a non-FlashArray disk")
            order = int(r.get("userdevice", "0") or "0")
            disks.append(DiskSpec(
                identity=DiskIdentity(fa_volume=vol),
                bus="xvd", order=order,
                boot=(r.get("bootable", "false").lower() == "true"),
                source_ref=r.get("uuid", "")))  # source VBD uuid

        # NICs.
        nics: list[NicSpec] = []
        vif_out = await self._ssh(
            f"xe vif-list vm-uuid={vm_ref} params=uuid,MAC,network-uuid,device",
            check=False)
        for r in self._parse_records(vif_out):
            nics.append(NicSpec(
                mac=r.get("MAC", ""), source_network=r.get("network-uuid", ""),
                model="virtio", order=int(r.get("device", "0") or "0")))

        disks.sort(key=lambda d: d.order)
        nics.sort(key=lambda n: n.order)
        if disks and not any(d.boot for d in disks):
            disks[0].boot = True
        return VmSpec(
            name=name or f"vm-{vm_ref}", source_ref=str(vm_ref), vcpus=vcpus,
            memory_bytes=mem, firmware=firmware, secure_boot=secure_boot,
            disks=disks, nics=nics)

    async def stop_vm(self, vm_ref: str, *, force: bool = False) -> OpResult:
        if await self.power_state(vm_ref) == "stopped":
            return OpResult.ok(f"VM {vm_ref} already stopped")
        flag = " --force" if force else ""
        await self._ssh(f"xe vm-shutdown uuid={vm_ref}{flag}", check=False)
        return OpResult.ok(f"Stopped VM {vm_ref}")

    async def start_vm(self, vm_ref: str) -> OpResult:
        if await self.power_state(vm_ref) == "running":
            return OpResult.ok(f"VM {vm_ref} already running")
        # Rescan all purefa SRs so the multipath devices are current before start.
        # This matters after a FlashArray copy-overwrite: the data changed but the
        # device path was already assembled; a rescan ensures the SM plugin sees it.
        sr_uuids = (await self._ssh(
            "xe sr-list type=purefa --minimal", check=False)).strip()
        for sr in (sr_uuids.split(",") if sr_uuids else []):
            sr = sr.strip()
            if sr:
                await self._ssh(f"xe sr-scan uuid={sr}", check=False)
        await self._ssh(f"xe vm-start uuid={vm_ref}", check=False)
        return OpResult.ok(f"Started VM {vm_ref}")

    async def detach_volumes(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        for disk in disks:
            vbd = disk.source_ref
            if not vbd:
                continue
            vdi = await self._xe_get("vbd", vbd, "vdi-uuid")
            await self._ssh(f"xe vbd-unplug uuid={vbd}", check=False)
            await self._ssh(f"xe vbd-destroy uuid={vbd}", check=False)
            # Forget the VDI record so a later sr-scan re-introduces it cleanly;
            # the backing FlashArray volume is untouched.
            if vdi and vdi != "<not in database>":
                await self._ssh(f"xe vdi-forget uuid={vdi}", check=False)
            await self.ctx.emit(f"Detached VBD {vbd} from VM {vm_ref}")
        return OpResult.ok(f"Detached {len(disks)} disk(s) from VM {vm_ref}")

    async def create_vm(self, spec: "VmSpec", *,
                        network_map: dict[str, str],
                        placement: dict[str, Any] | None = None) -> OpResult:
        existing_names = {vm.get("name") for vm in await self.list_vms()}
        name = spec.name
        if name in existing_names:
            i = 2
            while f"{spec.name}-{i}" in existing_names:
                i += 1
            name = f"{spec.name}-{i}"
            await self.ctx.emit(
                f"Name {spec.name!r} already exists; using {name!r} instead")
        if self._is_mock_or_dry():
            return OpResult.ok("created (mock)", artifacts={"vm_ref": "vm-uuid-new"})

        uuid = (await self._ssh(
            f"xe vm-create name-label='{name}'", check=False)).strip()
        if not uuid:
            return OpResult.fail("xe vm-create returned no VM uuid")
        # CPU + memory. TODO(validate-on-hardware): exact memory-limits ordering
        # can vary by XCP-ng version.
        await self._ssh(f"xe vm-param-set uuid={uuid} VCPUs-max={max(1, spec.vcpus)}",
                        check=False)
        await self._ssh(
            f"xe vm-param-set uuid={uuid} VCPUs-at-startup={max(1, spec.vcpus)}",
            check=False)
        mem = max(16 * 1024 * 1024, spec.memory_bytes)
        await self._ssh(
            f"xe vm-memory-limits-set uuid={uuid} static-min={mem} dynamic-min={mem} "
            f"dynamic-max={mem} static-max={mem}", check=False)
        # Firmware / boot policy (HVM). A bare `xe vm-create` has an EMPTY platform
        # map, so the HVM guest sees a 32-bit (i686) CPU and a 64-bit OS refuses to
        # boot ("requires x86-64 but only i686 detected"). Set the standard HVM
        # platform flags (matching the "Other install media" template) — pae is the
        # critical one for 64-bit guests.
        await self._ssh(f"xe vm-param-set uuid={uuid} domain-type=hvm", check=False)
        await self._ssh(
            f"xe vm-param-set uuid={uuid} HVM-boot-policy='BIOS order'", check=False)
        await self._ssh(
            f"xe vm-param-set uuid={uuid} HVM-boot-params:order=cdn", check=False)
        await self._ssh(
            f"xe vm-param-set uuid={uuid} platform:pae=true platform:nx=true "
            f"platform:acpi=1 platform:apic=true platform:hpet=true "
            f"platform:viridian=true", check=False)
        if spec.firmware == "uefi":
            # UEFI needs the OVMF device-model: `qemu-upstream-compat` boots BIOS
            # even with platform:firmware=uefi (this is what XO sets for UEFI VMs).
            # The firmware flag must be on BOTH the platform map and HVM-boot-params,
            # and secureboot set explicitly; XAPI initializes default UEFI NVRAM on
            # first boot.
            await self._ssh(
                f"xe vm-param-set uuid={uuid} platform:device-model=qemu-upstream-uefi "
                f"platform:firmware=uefi "
                f"platform:secureboot={'true' if spec.secure_boot else 'false'}",
                check=False)
            await self._ssh(
                f"xe vm-param-set uuid={uuid} HVM-boot-params:firmware=uefi", check=False)
        else:
            await self._ssh(
                f"xe vm-param-set uuid={uuid} platform:device-model=qemu-upstream-compat",
                check=False)
        # NICs with preserved MAC, mapped to destination networks.
        for nic in spec.nics:
            dest_net = network_map.get(nic.source_network)
            if not dest_net:
                return OpResult.fail(
                    f"no destination network mapped for source NIC {nic.source_network!r}")
            await self._ssh(
                f"xe vif-create vm-uuid={uuid} network-uuid={dest_net} "
                f"mac={nic.mac} device={nic.order}", check=False)
        await self.ctx.emit(f"Created VM {uuid} ({spec.name})")
        return OpResult.ok(f"Created VM {uuid}", artifacts={"vm_ref": uuid})

    async def prepare_copy_target(self, *, base_name: str, order: int,
                                  dest_vm_ref: str) -> str:
        # Clone into a vgroup-member name ("<vg>/<base>"): the purefa SMAPIv3 plugin
        # resolves a VDI key containing '/' as-is (vgroup-member form), so the
        # introduced VDI's location maps straight to this volume. Pick a name not
        # taken by a LIVE or a soft-deleted (pending-eradication) volume.
        vg = ("migcopy-" + dest_vm_ref.replace(":", "-"))[:60]
        target = f"{vg}/{base_name}"
        if self.ctx.array is not None:
            i = 0
            while await self.ctx.array.volume_exists(target, include_destroyed=True):
                i += 1
                target = f"{vg}/{base_name}-{i}"
            await self.ctx.array.ensure_volume_group(vg)
        return target

    async def _purefa_sr_uuid(self, host_group: str = "") -> str:
        """Resolve the purefa SR uuid, disambiguating when several share the
        name-label 'purefa': prefer the type=purefa SR whose PBD device-config
        names this ``host_group`` (the active one for this hypervisor)."""
        out = await self._ssh(
            "for u in $(xe sr-list type=purefa --minimal | tr ',' ' '); do "
            "dc=$(xe pbd-list sr-uuid=$u params=device-config --minimal 2>/dev/null); "
            "echo \"SR $u :: $dc\"; done", check=False)
        first = ""
        for line in (out or "").splitlines():
            if not line.startswith("SR "):
                continue
            uuid = line.split()[1]
            first = first or uuid
            if host_group and host_group in line:
                return uuid
        return first

    async def attach_existing_volumes(self, vm_ref: str,
                                      disks: "list[DiskSpec]") -> OpResult:
        if self._is_mock_or_dry():
            return OpResult.ok(f"attached {len(disks)} disk(s) (mock)")
        from phif.connectors.multipath import refresh_block_devices

        host_group = self.ctx.target.get("host_group", "")
        sr_uuid = await self._purefa_sr_uuid(host_group)
        if not sr_uuid:
            return OpResult.fail("purefa SR (type=purefa) not found on the pool")
        # Flush stale multipath maps + rescan on EVERY pool host before attaching.
        # The datapath plugin only flushes on detach and rescans at VM-start, so a
        # stale dead-path map on whichever host the VM lands on could wedge it; do a
        # clean, time-bounded refresh up front. (vdi-introduce/vbd-create don't
        # activate the device — the plugin does at start — so this is preventative.)
        proto = (self.ctx.target.get("protocol") or "iscsi").lower()
        wwids = await self._disk_wwids(disks)
        if wwids:
            for node in await self.list_nodes():
                await refresh_block_devices(
                    (lambda cmd, t, h=node.host: self._ssh(
                        cmd, check=False, host=h, timeout=t)),
                    protocol=proto, wwids=wwids)
        for disk in disks:
            vol = disk.identity.fa_volume
            # Introduce the EXISTING array volume as a VDI by its FA name as the
            # SMAPIv3 'location': the purefa plugin's Volume.stat resolves a key
            # containing '/' (a vgroup-member name) directly to the device, so no
            # rename or sr-scan is needed (scan only lists the plugin's own
            # namespace and would never see a Proxmox/VME-named volume).
            # NB: SMAPIv3 vdi-introduce IGNORES the supplied uuid and assigns its
            # own, printing the REAL VDI uuid on stdout — capture that for the VBD.
            out = await self._ssh_script([
                f"VDI=$(xe vdi-introduce uuid=$(uuidgen) sr-uuid={sr_uuid} type=user "
                f"sharable=false read-only=false location='{vol}' name-label='{vol}')",
                f"xe vbd-create vm-uuid={vm_ref} vdi-uuid=$VDI device={disk.order} "
                f"bootable={'true' if disk.boot else 'false'} type=Disk mode=RW",
                "echo VDI=$VDI",
            ], check=False, timeout=120)
            vdi = ""
            for line in (out or "").splitlines():
                if line.strip().startswith("VDI="):
                    vdi = line.strip()[4:].strip()
            if not vdi:
                return OpResult.fail(
                    f"failed to introduce/attach VDI for {vol!r}: {out!r}")
            await self.ctx.emit(
                f"Introduced + attached VDI for {vol} on VM {vm_ref} (device {disk.order})")
        return OpResult.ok(f"Attached {len(disks)} disk(s) to VM {vm_ref}")

    async def create_managed_disk(self, vm_ref: str, *, size_bytes: int,
                                  order: int, boot: bool) -> str:
        """Create a NEW purefa-managed VDI (`xe vdi-create` on the purefa SR — the
        SMAPIv3 plugin creates the backing FA volume in its own namespace), attach
        it, and return the FA volume NAME (the VDI's location)."""
        if self._is_mock_or_dry():
            return f"phif-mock/phif-mock-{vm_ref}-{order}"
        sr_uuid = await self._purefa_sr_uuid(self.ctx.target.get("host_group", ""))
        if not sr_uuid:
            raise ConnectionValidationError("purefa SR not found on the pool")
        size = max(1024 ** 3, int(size_bytes or 0))
        out = await self._ssh_script([
            f"VDI=$(xe vdi-create sr-uuid={sr_uuid} type=user "
            f"name-label='migrated-{vm_ref}-{order}' virtual-size={size})",
            f"xe vbd-create vm-uuid={vm_ref} vdi-uuid=$VDI device={order} "
            f"bootable={'true' if boot else 'false'} type=Disk mode=RW",
            "echo VDI=$VDI",
        ], check=False, timeout=120)
        vdi = ""
        for line in (out or "").splitlines():
            if line.strip().startswith("VDI="):
                vdi = line.strip()[4:].strip()
        if not vdi:
            raise ConnectionValidationError(f"failed to create managed VDI: {out!r}")
        location = await self._xe_get("vdi", vdi, "location")
        await self.ctx.emit(
            f"Created managed VDI {vdi} (FA {location}) on VM {vm_ref} (device {order})")
        return location

    async def set_boot_order(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        if self._is_mock_or_dry():
            return OpResult.ok("boot order set (mock)")
        # Mark the boot disk's VBD bootable and disk-first boot order.
        vbd_out = await self._ssh(
            f"xe vbd-list vm-uuid={vm_ref} type=Disk params=uuid,userdevice",
            check=False)
        by_dev = {int(r.get("userdevice", "0") or "0"): r.get("uuid", "")
                  for r in self._parse_records(vbd_out)}
        for disk in disks:
            vbd = by_dev.get(disk.order)
            if vbd:
                await self._ssh(
                    f"xe vbd-param-set uuid={vbd} "
                    f"bootable={'true' if disk.boot else 'false'}", check=False)
        await self._ssh(
            f"xe vm-param-set uuid={vm_ref} HVM-boot-params:order=cdn", check=False)
        return OpResult.ok("Boot order set (disk first)")

    async def delete_vm(self, vm_ref: str, *, keep_disks: bool = True) -> OpResult:
        if keep_disks:
            # Destroy the VBDs (config only) first so vm-destroy never touches the
            # VDIs / FlashArray volumes, then forget the VDIs.
            vbd_out = await self._ssh(
                f"xe vbd-list vm-uuid={vm_ref} type=Disk params=uuid,vdi-uuid",
                check=False)
            for r in self._parse_records(vbd_out):
                vbd, vdi = r.get("uuid", ""), r.get("vdi-uuid", "")
                if vbd:
                    await self._ssh(f"xe vbd-destroy uuid={vbd}", check=False)
                if vdi and vdi != "<not in database>":
                    await self._ssh(f"xe vdi-forget uuid={vdi}", check=False)
        await self._ssh(f"xe vm-destroy uuid={vm_ref}", check=False)
        return OpResult.ok(f"Deleted VM {vm_ref}")

    # ------------------------------------------------------------------ #
    # HEALTH: xe sr-list + multipath -ll
    # ------------------------------------------------------------------ #
    async def health_check(self, **_: Any) -> OpResult:
        proto = self._protocol()
        # SR list is pool-wide (query once on the master); multipath state is
        # per-host, so gather it from EVERY pool host.
        srs = await self._ssh(f"xe sr-list type={_SR_TYPE}", check=False)
        nodes = await self.list_nodes()
        await self.ctx.emit(
            f"Collecting XCP-ng SR + multipath health from {len(nodes)} host(s) ...")
        per_node: dict[str, Any] = {}
        for node in nodes:
            await self.ctx.emit(f"-> multipath on {node.name} ({node.host})")
            per_node[node.name] = {
                "host": node.host,
                "paths": await self._ssh("multipath -ll", check=False,
                                         host=node.host),
            }
        array_info = await self.ctx.array.info() if self.ctx.array else {}
        return OpResult.ok(
            "Health collected",
            protocol=proto,
            sr_list=srs,
            nodes=per_node,
            array=array_info,
        )

    # ------------------------------------------------------------------ #
    # REMOVE: pbd-unplug / sr-forget + remove driver
    # ------------------------------------------------------------------ #
    async def teardown(self, sr_name: str = "", **_: Any) -> OpResult:
        sr_name = self._sr_name(sr_name)
        await self.ctx.emit("Removing XCP-ng PureFA integration ...")
        if self.ctx.dry_run:
            return OpResult.ok("[dry-run] teardown planned", status="planned")
        nodes = await self.list_nodes()

        # 1) Forget EVERY purefa SR -- match by TYPE (catches duplicate/leftover
        #    SRs from repeated deploys) AND by name-label, deduped. sr-forget is
        #    pool-wide and non-destructive (array volumes are left intact).
        by_type = await self._ssh(
            f"xe sr-list type={_SR_TYPE} --minimal", check=False)
        by_name = await self._ssh(
            f"xe sr-list name-label='{sr_name}' --minimal", check=False)
        uuids = []
        for blob in (by_type, by_name):
            for u in (blob or "").replace("\n", ",").split(","):
                u = u.strip()
                if u and u not in uuids:
                    uuids.append(u)
        await self.ctx.emit(
            f"Forgetting {len(uuids)} {_SR_TYPE} SR(s): {uuids or '(none)'}")
        for u in uuids:
            await self._forget_sr(u)

        # 2) Remove the SMAPIv3 plugin (+ legacy v1 driver), SR state, and revert
        #    the /etc/xapi.conf sm-plugins allowlist edit on EVERY pool host.
        for node in nodes:
            await self.ctx.emit(f"-> cleaning {node.name} ({node.host})")
            await self._ssh(f"rm -rf {_SMAPIV3_DEST_DIR}", check=False, host=node.host)
            await self._ssh(f"rm -rf {_SMAPIV3_DP_DEST_DIR}", check=False,
                            host=node.host)  # custom datapath plugin
            await self._ssh(f"rm -f {_SMAPIV3_HOSTPLUGIN_DEST}", check=False,
                            host=node.host)  # host plugin
            await self._ssh(
                "rm -f /opt/xensource/sm/PureSR.py /opt/xensource/sm/PureSR",
                check=False, host=node.host)   # defensively clear any legacy
                                               # SMAPIv1 leftovers from old builds
            await self._ssh("rm -rf /var/run/sr-ref/purefa", check=False,
                            host=node.host)
            await self._ssh(self._sm_revert_cmd(), check=False, host=node.host)
            await self._ssh(
                "systemctl restart xapi-storage-script.service 2>/dev/null || "
                "systemctl restart xapi-storage-script 2>/dev/null || true",
                check=False, host=node.host)
        # Reload XAPI so the de-registered plugin/SRs drop out of sm-list/sr-list.
        for node in nodes:
            await self._ssh("xe-toolstack-restart", check=False, host=node.host)

        await self.ctx.emit(
            f"Integration removed from {len(nodes)} pool host(s); "
            f"{len(uuids)} SR(s) forgotten")
        return OpResult.ok("Integration removed",
                           artifacts={"nodes": [n.to_dict() for n in nodes],
                                      "forgotten_srs": uuids},
                           status="not_deployed")

    # ------------------------------------------------------------------ #
    # RECONCILE_CLUSTER: assess + (optionally) configure new / prune departed
    # ------------------------------------------------------------------ #
    def _fa_host_for_node(self, host_group: str, node: ClusterNode) -> str:
        """The FlashArray host name for ``node`` in this pool's host group.

        Mirrors the per-node naming used by :meth:`register_hosts` when a real
        multi-host pool is registered (``<host_group>-<node.name>``), so cluster
        assessment can compare current nodes against existing FA group members.
        """
        return self._fa_name(f"{host_group}-{node.name}")

    async def assess_cluster(self, **params: Any) -> OpResult:
        """READ-ONLY pool membership + readiness assessment (no mutations).

        Lists the pool's nodes, derives each node's expected FA host name, reads
        the host group's members, and classifies:

        * ``new_hosts`` -- nodes whose FA host isn't a group member yet, each with
          a readiness verdict (reachable + initiator + storage NIC on the array
          subnet, scored via :meth:`score_host_readiness`).
        * ``departed_hosts`` -- group members with no matching current node.

        Mock/dry-run safe: SSH discovery is short-circuited by the runner.
        """
        proto = self._protocol()
        host_group = (params.get("host_group")
                      or self.ctx.target.get("host_group", ""))
        nodes = await self.list_nodes()
        node_names = [n.name for n in nodes]

        members: list[str] = []
        if self.ctx.array is not None and host_group:
            members = await self.ctx.array.get_host_group_members(host_group)
        expected = {self._fa_host_for_node(host_group, n): n for n in nodes}

        # Departed: a current group member with no matching pool node.
        departed = sorted(m for m in members if m not in expected)

        # Storage-subnet context for the readiness scorer (IP transports only).
        array_portals: list[str] = []
        if self.ctx.array is not None and proto in ("iscsi", "nvme-tcp"):
            service = "nvme-tcp" if proto == "nvme-tcp" else "iscsi"
            array_portals = await self.ctx.array.get_data_interfaces(service)
        baseline_subnets = await self._baseline_subnets(
            nodes, expected, members, proto, array_portals)

        new_hosts: list[dict[str, Any]] = []
        for node in nodes:
            fa_host = self._fa_host_for_node(host_group, node)
            if fa_host in members:
                continue  # already configured
            verdict = await self._assess_node_readiness(
                node, proto, array_portals, baseline_subnets)
            new_hosts.append({"node": node.name, "host": fa_host,
                              "ready": verdict["ready"],
                              "reasons": verdict["reasons"]})

        ready_new = [h["node"] for h in new_hosts if h["ready"]]
        await self.ctx.emit(
            f"Cluster assessment: {len(node_names)} node(s), "
            f"{len(new_hosts)} new ({len(ready_new)} ready), "
            f"{len(departed)} departed")
        return OpResult.ok(
            f"{len(node_names)} node(s); {len(new_hosts)} new, "
            f"{len(departed)} departed",
            nodes=node_names, new_hosts=new_hosts, departed_hosts=departed)

    async def _baseline_subnets(
        self, nodes: list[ClusterNode],
        expected: dict[str, ClusterNode], members: list[str], proto: str,
        array_portals: list[str],
    ) -> "set[str] | None":
        """Storage-NIC subnets of an already-configured (baseline) pool node.

        Used so a NEW node's storage NIC subnet can be checked against the
        existing nodes'. Picks the first node already present in the FA host group
        and returns the set of its NIC subnets that sit on an array portal subnet.
        Returns None when there's no baseline or no array context (scorer skips).
        """
        if proto not in ("iscsi", "nvme-tcp") or not array_portals:
            return None
        import ipaddress

        portals = []
        for p in array_portals:
            try:
                portals.append(ipaddress.ip_address(p))
            except ValueError:
                continue
        kind = "nvme_sources" if proto == "nvme-tcp" else "nics"
        for fa_host, node in expected.items():
            if fa_host not in members:
                continue
            nics = await self.ctx.runner.discover_interfaces(
                node.host, kind, **self._discover_kwargs())
            subnets: set[str] = set()
            for nic in nics:
                cidr = nic.get("cidr")
                if not cidr:
                    continue
                try:
                    net = ipaddress.ip_network(cidr, strict=False)
                except ValueError:
                    continue
                if any(ip in net for ip in portals):
                    subnets.add(str(net))
            if subnets:
                return subnets
        return None

    async def _assess_node_readiness(
        self, node: ClusterNode, proto: str, array_portals: list[str],
        baseline_subnets: "set[str] | None",
    ) -> dict[str, Any]:
        """Gather a candidate node's preflight inputs over SSH + score them.

        Reachability is inferred from a successful initiator discovery (mock-safe:
        the runner returns synthetic initiators/NICs, so a mock node scores ready).
        """
        reachable = True
        initiators: dict[str, Any] = {}
        host_nics: list[dict[str, Any]] = []
        try:
            initiators = await self.ctx.runner.discover_initiators(
                node.host, **self._discover_kwargs())
        except Exception:  # noqa: BLE001
            reachable = False
        if reachable and proto in ("iscsi", "nvme-tcp"):
            kind = "nvme_sources" if proto == "nvme-tcp" else "nics"
            try:
                host_nics = await self.ctx.runner.discover_interfaces(
                    node.host, kind, **self._discover_kwargs())
            except Exception:  # noqa: BLE001
                host_nics = []
        return self.score_host_readiness(
            proto, reachable=reachable, initiators=initiators,
            host_nics=host_nics, array_portals=array_portals,
            baseline_subnets=baseline_subnets)

    async def reconcile_cluster(self, apply_removals: bool = False,
                                **params: Any) -> OpResult:
        """Configure READY new pool hosts; flag/remove departed FA hosts.

        1. Assess the cluster.
        2. Configure ONLY the new hosts that pass the readiness preflight
           (install the SMAPIv3 plugin + register + setup connectivity, scoped to
           exactly those nodes). Not-ready new hosts are skipped + reported in
           ``not_ready`` (never half-configured).
        3. Prune departed FA hosts via :meth:`_prune_departed_fa_hosts`; for XCP-ng
           we also best-effort forget/unplug the departed host's PBD if reachable.

        Idempotent + dry-run safe. ``apply_removals=False`` (default) only flags
        departed hosts (``pending_removals``); ``True`` removes them (``removed``).
        """
        apply_removals = str(apply_removals).lower() in ("1", "true", "yes", "on")
        proto = self._protocol()
        host_group = (params.get("host_group")
                      or self.ctx.target.get("host_group", ""))

        assessment = await self.assess_cluster(**params)
        node_names = assessment.data.get("nodes", [])
        new_hosts = assessment.data.get("new_hosts", [])
        ready = [h for h in new_hosts if h["ready"]]
        not_ready = [h for h in new_hosts if not h["ready"]]

        nodes = await self.list_nodes()
        by_name = {n.name: n for n in nodes}
        ready_nodes = [by_name[h["node"]] for h in ready if h["node"] in by_name]

        configured: list[str] = []
        if ready_nodes:
            await self.ctx.emit(
                f"Configuring {len(ready_nodes)} ready new host(s): "
                f"{[n.name for n in ready_nodes]}")
            # NFS has no block plugin / host registration; the stock NFS SR is
            # pool-wide and needs no per-host work, so only block protocols
            # install the plugin + register + set up connectivity.
            if proto != "nfs":
                await self._install_smapiv3_plugin(ready_nodes)
                reg = await self.register_hosts(
                    host_group=host_group, nodes_override=ready_nodes)
                if not reg.success:
                    return OpResult.fail(
                        f"register_hosts failed during reconcile: {reg.message}",
                        nodes=node_names, new_hosts=new_hosts,
                        not_ready=not_ready, departed_hosts=
                        assessment.data.get("departed_hosts", []))
                await self.setup_connectivity(nodes_override=ready_nodes)
            configured = [h["host"] for h in ready]

        # Departed-host pruning. Expected = every CURRENT node's FA host name.
        expected_names = {self._fa_host_for_node(host_group, n) for n in nodes}
        prune = await self._prune_departed_fa_hosts(
            host_group, expected_names, apply_removals=apply_removals)

        # XCP-ng-specific: when actually removing a departed host, best-effort
        # forget/unplug its PBD on the departed node if it's still reachable.
        if apply_removals and prune.get("removed") and not self._is_mock_or_dry():
            await self._forget_departed_pbds(prune["removed"], host_group)

        data: dict[str, Any] = {
            "nodes": node_names,
            "new_hosts": new_hosts,
            "configured": configured,
            "not_ready": not_ready,
            "departed_hosts": prune["departed"],
        }
        if apply_removals:
            data["removed"] = prune["removed"]
        else:
            data["pending_removals"] = prune["departed"]

        await self.ctx.emit(
            f"Reconcile: configured {len(configured)}, skipped "
            f"{len(not_ready)} not-ready, departed {len(prune['departed'])} "
            f"({'removed' if apply_removals else 'flagged'})")
        return OpResult.ok(
            f"Reconciled cluster: {len(configured)} configured, "
            f"{len(not_ready)} not-ready, {len(prune['departed'])} departed",
            **data)

    async def _forget_departed_pbds(self, removed_hosts: list[str],
                                    host_group: str) -> None:
        """Best-effort: unplug + forget the purefa PBD on each departed node.

        A node that left the pool may still have a plugged PBD for the shared SR.
        We attempt to unplug it via the pool master (XAPI knows the PBD even after
        the host is gone). Failures are non-fatal. Skipped in mock/dry-run.
        """
        for host in removed_hosts:
            await self.ctx.emit(
                f"Best-effort: forgetting PBD for departed host {host!r}")
            # The FA host name encodes the node name; XAPI references the host by
            # its own uuid, so unplug any PBD on a host whose name-label matches.
            await self._ssh(
                f"for h in $(xe host-list name-label='{host}' --minimal | "
                f"tr ',' ' '); do for p in $(xe pbd-list host-uuid=$h "
                f"--minimal | tr ',' ' '); do xe pbd-unplug uuid=$p; "
                f"xe pbd-destroy uuid=$p; done; done",
                check=False)

    # ------------------------------------------------------------------ #
    # NFS: stock XCP-ng NFS SR backed by a FlashArray file system + export
    # ------------------------------------------------------------------ #
    # TODO(hardware-validate): the NFS path is implemented mock/dry-run safe and
    # reviewed only -- it has NOT been validated on real XCP-ng + FlashArray
    # hardware. The `xe sr-create type=nfs` device-config keys + FA NFS export
    # path semantics should be confirmed against a live pool before GA.
    async def provision_nfs(self, name: str = "", sr_name: str = "",
                            **_: Any) -> OpResult:
        """Create a FA file system + NFS export and a pool-wide stock NFS SR.

        NFS does NOT use the per-VDI 'purefa' block plugin. We create a FlashArray
        managed file system, an NFS export on it, then `xe sr-create type=nfs` on
        the pool master (XCP-ng plugs the NFS PBD on every host).
        """
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if self._protocol() != "nfs":
            return OpResult.fail(
                "provision_nfs requires the hypervisor protocol to be 'nfs'")
        name = self._fa_name(name or sr_name or self._sr_name())
        sr_name = sr_name or self.ctx.target.get("sr_name") or name
        export = name  # one export per file system, named after it
        await self.ctx.emit(
            f"Provisioning stock NFS SR '{sr_name}' (FA file system + export "
            f"'{export}')")
        if self.ctx.dry_run:
            return OpResult.ok(
                f"[dry-run] would create FA NFS export + NFS SR '{sr_name}'",
                artifacts={"sr_name": sr_name, "filesystem": name,
                           "export": export, "protocol": "nfs"})

        # Array side: file system + NFS export.
        await self.ctx.array.create_filesystem(name)
        serverpath = f"/{export}"
        await self.ctx.array.create_nfs_export(export, name, serverpath)
        # Pick an NFS data portal off the array.
        portals = await self.ctx.array.get_nfs_data_interfaces()
        portal = portals[0] if portals else ""
        if not portal and not self._mock_mode():
            return OpResult.fail(
                "No NFS data interfaces found on the array; cannot create NFS SR.")
        portal = portal or "192.0.2.30"  # mock fallback
        await self.ctx.emit(
            f"NFS export ready: server={portal} serverpath={serverpath}")

        # XCP-ng side: stock NFS SR, plugged pool-wide by XAPI. One sr-create on
        # the master. Idempotent: skip if the named SR already exists.
        existing = (await self._ssh(
            f"xe sr-list name-label='{sr_name}' --minimal", check=False)).strip()
        if existing:
            await self.ctx.emit(
                f"NFS SR '{sr_name}' already exists ({existing})")
            sr_uuid = existing.split(",")[0]
        else:
            await self._ssh(
                f"xe sr-create type=nfs name-label='{sr_name}' shared=true "
                f"content-type=user "
                f"device-config:server={portal} "
                f"device-config:serverpath={serverpath}",
                check=False)
            sr_uuid = (await self._ssh(
                f"xe sr-list name-label='{sr_name}' --minimal",
                check=False)).strip().split(",")[0]
            if not sr_uuid and self._mock_mode():
                sr_uuid = "mock-nfs-sr-uuid"
        await self.ctx.emit(f"NFS SR '{sr_name}' ready ({sr_uuid})")
        return OpResult.ok(
            f"Provisioned NFS SR '{sr_name}'",
            artifacts={"sr_name": sr_name, "sr_uuid": sr_uuid,
                       "filesystem": name, "export": export,
                       "server": portal, "serverpath": serverpath,
                       "protocol": "nfs"})

    async def teardown_nfs(self, name: str = "", sr_name: str = "",
                           eradicate: Any = False, **_: Any) -> OpResult:
        """Forget/destroy the stock NFS SR + delete the FA NFS export + file system."""
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        eradicate = str(eradicate).lower() in ("1", "true", "yes", "on")
        name = self._fa_name(name or sr_name or self._sr_name())
        sr_name = sr_name or self.ctx.target.get("sr_name") or name
        export = name
        await self.ctx.emit(f"Removing stock NFS SR '{sr_name}' + FA export")
        if self.ctx.dry_run:
            return OpResult.ok("[dry-run] NFS teardown planned", status="planned")

        # XCP-ng side: unplug PBDs + forget the SR (non-destructive to the FA fs).
        uuids = [u.strip() for u in (await self._ssh(
            f"xe sr-list name-label='{sr_name}' --minimal",
            check=False)).replace("\n", ",").split(",") if u.strip()]
        for u in uuids:
            await self._forget_sr(u)

        # Array side: delete the NFS export then the file system.
        await self.ctx.array.delete_nfs_export(export)
        await self.ctx.array.delete_filesystem(name, eradicate=eradicate)
        await self.ctx.emit(
            f"NFS SR '{sr_name}' forgotten; FA export + file system deleted "
            f"(eradicate={eradicate})")
        return OpResult.ok(
            f"Removed NFS SR '{sr_name}'",
            artifacts={"sr_name": sr_name, "filesystem": name, "export": export,
                       "forgotten_srs": uuids, "eradicate": eradicate},
            status="not_deployed")

    # ------------------------------------------------------------------ #
    # Dispatch: route the bespoke NFS action ids; everything else -> base.
    # ------------------------------------------------------------------ #
    async def dispatch(self, action_id: str, params: dict[str, Any]) -> OpResult:
        if action_id == "provision_nfs":
            return await self.provision_nfs(**params)
        if action_id == "teardown_nfs":
            return await self.teardown_nfs(**params)
        return await super().dispatch(action_id, params)
