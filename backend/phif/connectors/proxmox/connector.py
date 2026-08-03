"""Proxmox VE connector (Everpure FlashArray storage plugin).

This connector wraps a **true Proxmox VE custom storage plugin** for Everpure
FlashArray, modelled on the Everpure CSI driver and the OpenStack Cinder Everpure
driver rather than on Proxmox LVM-thick or a shared LVM pool:

* The storage type is ``purefa`` and is implemented by a Perl plugin,
  ``PureFAPlugin.pm`` (subclass of ``PVE::Storage::Plugin``), shipped as a
  static file alongside this connector and installed on each node at
  ``/usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm``.
* **Each VM disk is its own FlashArray volume** (``vm-<vmid>-disk-N``), created
  on the array and presented DIRECTLY to the VM as a raw multipath block device
  (virtio-scsi) over iSCSI, FC, or NVMe-TCP. There is no LVM layer.
* **Per-VM FlashArray volume groups** (vgroups): by default each newly
  provisioned VM gets its own FA volume group ``vm-<vmid>`` and every disk is
  created as a member volume ``vm-<vmid>/vm-<vmid>-disk-N``, so a multi-disk VM
  can be snapshotted crash-consistently as a group. The ``<vg>/`` prefix lives
  ONLY on the array; PVE still uses the plain ``vm-<vmid>-disk-N`` volume name.
  The grouping is additive (controlled by the plugin's ``volume_groups`` config,
  default on): pre-existing standalone volumes keep their flat names unchanged.
* **Snapshots and clones happen on the array** (FlashArray volume snapshots and
  volume copy), not via Proxmox/LVM/qcow2.

All node-side work runs over SSH via ``ctx.runner.run_ssh`` (mock-safe);
array-side work uses ``ctx.array``.

Doc sources:
* Everpure "Proxmox with FlashArray" solution bundle (m_proxmox):
  https://support.purestorage.com/bundle/m_proxmox/page/Solutions/Proxmox/topics/c_proxmox_with_flasharray_quick_start_guide.html
* Connecting Proxmox to FlashArray (iSCSI / FC / NVMe-TCP) topics under m_proxmox.
* Community per-volume PVE storage plugin reference (model for the per-disk
  volume design): https://github.com/kolesa-team/pve-purestorage-plugin

This connector is GA and has been validated on a live 2-node PVE cluster over
both iSCSI and FC. The Everpure-published m_proxmox topic pages render as a JS-only
shell to non-browser fetchers, so the CLI/REST specifics below were confirmed
empirically against the live cluster rather than scraped from the doc pages.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
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
from phif.connectors.base import _nic_network
from phif.connectors.iscsi_net import arp_flux_cmd

# Proxmox serializes mutations of a VM behind a per-VM lock file
# (/var/lock/qemu-server/lock-<vmid>.conf); concurrent `qm` commands (e.g. several
# migrations at once) collide with "can't lock file ... got timeout". We serialize
# all node-side commands per host so PHIF only ever issues one at a time, and retry
# on a lock-timeout to ride out any lock still held by an external/lingering task.
# Locks are keyed by (event-loop, host) so they're never reused across loops (which
# asyncio forbids) — important for the test suite where each test has its own loop.
_HOST_CMD_LOCKS: dict[tuple[int, str], asyncio.Lock] = {}
# Serializes vmid allocation (`pvesh get /cluster/nextid`) + `qm create` per
# CLUSTER, so concurrent/batched migrations never grab the same vmid. nextid only
# reflects a consumed id once the VM's config exists, so the lock must span the
# whole allocate->create — held cluster-wide (keyed by connection host), NOT by the
# round-robin target node, or creates on different nodes would still collide.
_CLUSTER_CREATE_LOCKS: dict[tuple[int, str], asyncio.Lock] = {}
_LOCK_TIMEOUT_RE = re.compile(r"can't lock file|got timeout|lock-\d+\.conf", re.I)

# Round-robin cursor per cluster (keyed by the connection host), so concurrent /
# batched migrations spread their new VMs across the cluster's online nodes.
_NODE_RR: dict[str, int] = {}
# Cache of SSH-reachable round-robin candidate hosts per (event-loop, cluster).
_RR_REACHABLE: dict[tuple[int, str], list[str]] = {}


def _keyed_lock(registry: dict[tuple[int, str], asyncio.Lock], key: str) -> asyncio.Lock:
    """A per-(event-loop, key) asyncio.Lock — never reused across loops (which
    asyncio forbids), important for the test suite where each test has its own."""
    k = (id(asyncio.get_running_loop()), key)
    lk = registry.get(k)
    if lk is None:
        lk = asyncio.Lock()
        registry[k] = lk
    return lk


def _host_cmd_lock(host: str) -> asyncio.Lock:
    return _keyed_lock(_HOST_CMD_LOCKS, host)


# Static plugin file shipped with this connector package.
PLUGIN_FILE = Path(__file__).resolve().parent / "files" / "PureFAPlugin.pm"
PLUGIN_DEST = "/usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm"
PLUGIN_STORAGE_TYPE = "purefa"

# PVE cluster membership file (pmxcfs). JSON of the form:
#   {"nodename": "...", "nodelist": {"<name>": {"id": N, "ip": "x.x.x.x",
#                                                "online": 1, ...}, ...}}
_PVE_MEMBERS_PATH = "/etc/pve/.members"

# NVMe-TCP / iSCSI transport defaults per the Everpure connectivity guides.
_NVME_DISCOVERY_PORT = 8009
_NVME_CONNECT_PORT = 4420
_NVME_CTRL_LOSS_TMO = 1800
_NVME_RECONNECT_DELAY = 10
_ISCSI_PORT = 3260

# Everpure FlashArray multipath config, written as a DROP-IN that multipathd
# auto-includes from /etc/multipath/conf.d/*.conf — so we never overwrite the
# operator's /etc/multipath.conf (which may blacklist local disks or configure
# other vendors). find_multipaths groups the multiple portal/HBA paths to one
# wwid; the PURE device stanza sets ALUA + the recommended path policy. (NVMe-oF
# uses native NVMe multipath, configured separately in the NVMe-TCP path.)
_MULTIPATH_DROPIN = "/etc/multipath/conf.d/pure.conf"
_MULTIPATH_CONF = """\
# Managed by PHIF — Everpure FlashArray multipath settings.
defaults {
    polling_interval 10
    # 'no' so every (non-blacklisted) Everpure LUN is auto-claimed into a multipath
    # device without an explicit `multipath -a <wwid>` -- otherwise a freshly
    # attached per-disk volume isn't assembled and /dev/mapper/<wwid> is missing.
    find_multipaths no
    # Always name the map by its WWID (/dev/mapper/3624a9370<serial>), never an
    # 'mpathN' alias -- volumes are resolved by WWID.
    user_friendly_names no
}

devices {
    device {
        vendor "PURE"
        product "FlashArray"
        path_selector "service-time 0"
        path_grouping_policy group_by_prio
        prio alua
        hardware_handler "1 alua"
        path_checker tur
        failback immediate
        fast_io_fail_tmo 10
        dev_loss_tmo 60
        no_path_retry 0
    }
}
"""


class ProxmoxConnector(HypervisorConnector):
    # --- static metadata ---
    key = "proxmox"
    name = "Proxmox VE (Everpure storage plugin)"
    description = (
        "Connect Proxmox VE to an Everpure FlashArray via a native 'purefa' storage "
        "plugin. Each VM disk is its own FlashArray volume presented directly to "
        "the VM as a raw multipath block device over NVMe-TCP (recommended), "
        "iSCSI, or FC. Snapshots and clones are performed on the array. Models "
        "the Everpure CSI / OpenStack Cinder block driver, not LVM."
    )
    maturity = "ga"
    CAPABILITIES = {
        Capability.HOST_REGISTER,
        Capability.CONNECTIVITY,
        Capability.DEPLOY_PLUGIN,
        Capability.CONFIGURE,
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
    # NFS (Protocol.NFS) is a DIFFERENT storage model from the per-disk block
    # plugin: a FlashArray File NFS export mounted as a PVE-native 'nfs' storage.
    # NOTE: the NFS path is NOT yet hardware-validated against FlashArray File.
    SUPPORTED_PROTOCOLS = {Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP,
                           Protocol.NFS}

    # ------------------------------------------------------------------ #
    # UI: how to connect to this hypervisor
    # ------------------------------------------------------------------ #
    @classmethod
    def target_schema(cls) -> list[FormField]:
        return [
            FormField("node_host", "Node host / IP (SSH)", FieldType.STRING,
                      placeholder="pve01.example.local",
                      help="A Proxmox cluster member we can SSH to."),
            FormField("ssh_user", "SSH user", FieldType.STRING, default="root"),
            FormField("ssh_password", "SSH password", FieldType.SECRET, required=False,
                      help="Provide either an SSH password or an SSH private key."),
            FormField("ssh_key", "SSH private key", FieldType.TEXT, required=False,
                      help="PEM private key; used if no password is supplied."),
            FormField("protocol", "Storage protocol", FieldType.ENUM, default="nvme-tcp",
                      options=["nvme-tcp", "iscsi", "fc", "nfs"]),
            FormField("storage_id", "Proxmox storage ID", FieldType.STRING,
                      default="purefa",
                      help="Name of the purefa storage in /etc/pve/storage.cfg."),
            FormField("host_group", "FlashArray host group", FieldType.STRING,
                      required=False,
                      help="FA host group the Proxmox cluster nodes belong to."),
            FormField("node_iqn", "Node IQN (iSCSI)", FieldType.STRING, required=False,
                      placeholder="iqn.1993-08.org.debian:01:abc",
                      help="(auto-discovered from the node if left blank)"),
            FormField("node_nqn", "Node NQN (NVMe-TCP)", FieldType.STRING, required=False,
                      placeholder="nqn.2014-08.org.nvmexpress:uuid:...",
                      help="(auto-discovered from the node if left blank)"),
            FormField("node_wwns", "Node HBA WWPNs (FC, comma-separated)",
                      FieldType.STRING, required=False,
                      placeholder="21:00:00:24:ff:00:00:01,21:00:00:24:ff:00:00:02",
                      help="FC HBA port WWNs for this node. "
                           "(auto-discovered from the node if left blank)"),
        ]

    # ------------------------------------------------------------------ #
    # UI: day-2 actions
    # ------------------------------------------------------------------ #
    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        return [
            ActionSpec(
                Capability.DEPLOY_PLUGIN, "deploy", "Deploy purefa storage plugin",
                "Push PureFAPlugin.pm to the node(s), install perl REST deps, and "
                "reload the PVE daemons so the 'purefa' storage type is available.",
                fields=[
                    FormField("storage_id", "Proxmox storage ID", FieldType.STRING,
                              default="purefa"),
                ],
            ),
            ActionSpec(
                Capability.HOST_REGISTER, "register_hosts", "Register node on array",
                "Create a FlashArray host (from the node IQN/NQN/WWN) and host group.",
                fields=[
                    FormField("host_name", "Array host name", FieldType.STRING,
                              required=False, options_source="fa_host_name",
                              help="Auto-loaded from the node host (sanitized for the array)."),
                    FormField("host_group", "Host group name", FieldType.STRING,
                              help="Proxmox cluster host group on the array."),
                    FormField("node_iqn", "Node IQN (iSCSI)", FieldType.STRING,
                              required=False,
                              help="(auto-discovered from the node if left blank)"),
                    FormField("node_nqn", "Node NQN (NVMe-TCP)", FieldType.STRING,
                              required=False,
                              help="(auto-discovered from the node if left blank)"),
                    FormField("node_wwns", "Node HBA WWPNs (FC, comma-separated)",
                              FieldType.STRING, required=False,
                              help="(auto-discovered from the node if left blank)"),
                ],
            ),
            ActionSpec(
                Capability.CONNECTIVITY, "setup_connectivity",
                "Configure connectivity + multipath",
                "Install transport tooling, connect persistently to the array "
                "portals, and set the recommended multipath / IO policy on the node.",
                fields=[
                    FormField("portals", "Array portal IPs (comma-separated)",
                              FieldType.STRING, required=False, options_source="array_portals",
                              placeholder="auto-discovered from the array",
                              help="Auto-loaded from the associated FlashArray's data "
                                   "interfaces for the chosen protocol."),
                    FormField("subsystem_nqn", "Subsystem NQN (NVMe-TCP)",
                              FieldType.STRING, required=False,
                              options_source="array_target_nqn",
                              help="Auto-loaded from the array's NVMe subsystem."),
                    FormField("iqn_target", "Target IQN (iSCSI)", FieldType.STRING,
                              required=False, options_source="array_target_iqn",
                              help="Auto-loaded from the array's iSCSI target."),
                    # --- interface binding (transport-specific) -----------------
                    # Which host interfaces/HBAs each transport binds its sessions
                    # to. Choices are discovered live from the node via
                    # discover_options(kind) -> runner.discover_interfaces(...).
                    FormField("iscsi_nics", "iSCSI NICs", FieldType.MULTISELECT,
                              required=False, options_source="nics",
                              help="NICs to bind iSCSI sessions to (iSCSI only)"),
                    FormField("nvme_sources", "NVMe-TCP source interfaces",
                              FieldType.MULTISELECT, required=False,
                              options_source="nvme_sources",
                              help="Host source interfaces/addresses for NVMe-TCP "
                                   "connections (host-traddr; NVMe-TCP only)"),
                    FormField("nvme_options", "Extra nvme connect options",
                              FieldType.STRING, required=False,
                              help="Extra flags appended to `nvme connect` "
                                   "(NVMe-TCP only)"),
                    FormField("fc_hbas", "Fibre Channel HBAs", FieldType.MULTISELECT,
                              required=False, options_source="fc_hbas",
                              help="HBAs to use (FC only)"),
                    FormField("rebind", "Re-bind iSCSI ifaces", FieldType.BOOL,
                              required=False, default=False,
                              help="Delete and recreate the iSCSI NIC ifaces so all "
                                   "cluster nodes end up with the same binding "
                                   "(use when nodes' existing bindings differ)."),
                ],
            ),
            ActionSpec(
                Capability.CONFIGURE, "enable", "Enable storage",
                "Enable the purefa storage (created disabled by Configure) once "
                "host registration and connectivity are complete.",
                fields=[
                    FormField("storage_id", "Proxmox storage ID", FieldType.STRING,
                              required=False, default="purefa"),
                ],
            ),
            ActionSpec(
                Capability.CONFIGURE, "configure", "Define purefa storage",
                "Write the 'purefa' stanza into /etc/pve/storage.cfg and reload the "
                "PVE daemons. The FlashArray endpoint and API token are taken from "
                "the associated array on this hypervisor.",
                fields=[
                    FormField("storage_id", "Proxmox storage ID", FieldType.STRING,
                              default="purefa"),
                    FormField("host_group", "FlashArray host group", FieldType.STRING,
                              required=False),
                    FormField("content", "Content types", FieldType.STRING,
                              default="images,rootdir"),
                ],
            ),
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision", "Provision VM disk",
                "Create a FlashArray volume (one LUN per disk), connect it to the "
                "host group, then attach it directly to the VM as a raw multipath "
                "block device via the purefa storage.",
                fields=[
                    FormField("name", "Volume name (vm-<vmid>-disk-N)", FieldType.STRING),
                    FormField("size", "Size", FieldType.SIZE, default="1T"),
                    FormField("host_group", "Attach to host group", FieldType.STRING,
                              required=False),
                    FormField("vmid", "VM ID", FieldType.STRING, required=False),
                    FormField("disk", "Disk slot (e.g. scsi1)", FieldType.STRING,
                              required=False, default="scsi1"),
                    FormField("storage_id", "Proxmox storage ID", FieldType.STRING,
                              required=False),
                ],
            ),
            ActionSpec(
                Capability.SNAPSHOT, "snapshot", "Snapshot disk (array)",
                "Create a FlashArray snapshot of the disk's volume.",
                fields=[FormField("volume", "Volume name", FieldType.STRING),
                        FormField("suffix", "Snapshot suffix", FieldType.STRING,
                                  required=False)]),
            ActionSpec(
                Capability.CLONE, "clone", "Clone disk (array copy)",
                "FlashArray volume copy producing a new directly-attached volume.",
                fields=[FormField("source", "Source volume", FieldType.STRING),
                        FormField("dest", "New volume name", FieldType.STRING),
                        FormField("host_group", "Attach clone to host group",
                                  FieldType.STRING, required=False)]),
            ActionSpec(
                Capability.RESIZE, "resize", "Resize disk",
                "Extend the FlashArray volume, then rescan + qm resize on the node.",
                fields=[FormField("volume", "Volume name", FieldType.STRING),
                        FormField("size", "New size", FieldType.SIZE),
                        FormField("vmid", "VM ID", FieldType.STRING, required=False),
                        FormField("disk", "Disk slot (e.g. scsi1)", FieldType.STRING,
                                  required=False)]),
            ActionSpec(Capability.HEALTH, "health_check", "Health check",
                       "pvesm status + multipath/nvme path status.",
                       long_running=False),
            ActionSpec(Capability.REMOVE, "teardown", "Remove integration",
                       "Remove the purefa storage entry and the plugin file.",
                       destructive=True),
            # --- cluster reconcile (membership drift) ----------------------- #
            ActionSpec(
                Capability.RECONCILE_CLUSTER, "assess_cluster",
                "Check for cluster changes",
                "READ-ONLY: compare current PVE cluster membership against the "
                "FlashArray host group, and score new nodes for deploy-readiness.",
                long_running=False),
            ActionSpec(
                Capability.RECONCILE_CLUSTER, "reconcile_cluster",
                "Deploy to new hosts",
                "Configure storage on newly-added (ready) cluster nodes; optionally "
                "remove FlashArray hosts for nodes that have left the cluster.",
                fields=[
                    FormField("apply_removals", "Also remove departed hosts",
                              FieldType.BOOL, default=False, required=False,
                              help="Remove FA hosts/group members for nodes that "
                                   "have left the cluster (default: flag only)."),
                ],
                destructive=True),
            # --- NFS datastore (FlashArray File; PVE-native 'nfs' storage) --- #
            # NOTE: the NFS path is NOT yet hardware-validated against FlashArray
            # File. It uses a different storage model from the per-disk block
            # plugin (no host registration / multipath; a mounted NFS export).
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision_nfs_datastore",
                "Provision NFS datastore",
                "Create a FlashArray File system + NFS export and define a "
                "PVE-native 'nfs' storage pointing at it (protocol=nfs).",
                fields=[
                    FormField("name", "Datastore / file system name",
                              FieldType.STRING, placeholder="pve-nfs"),
                    FormField("export_path", "NFS export path", FieldType.STRING,
                              required=False, default="/",
                              help="Export path on the file system (default '/')."),
                    FormField("storage_id", "Proxmox storage ID", FieldType.STRING,
                              required=False, default="purefa-nfs"),
                    FormField("content", "Content types", FieldType.STRING,
                              required=False, default="images,iso"),
                ]),
            ActionSpec(
                Capability.REMOVE, "teardown_nfs_datastore",
                "Remove NFS datastore",
                "Remove the PVE 'nfs' storage, then delete the NFS export and "
                "FlashArray file system.",
                fields=[
                    FormField("name", "File system name", FieldType.STRING),
                    FormField("storage_id", "Proxmox storage ID", FieldType.STRING,
                              required=False, default="purefa-nfs"),
                    FormField("eradicate", "Eradicate file system", FieldType.BOOL,
                              required=False, default=False),
                ],
                destructive=True),
        ]

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _protocol(self) -> str:
        return (self.ctx.target.get("protocol") or "nvme-tcp").lower()

    def _storage_id(self, override: str = "") -> str:
        return override or self.ctx.target.get("storage_id") or PLUGIN_STORAGE_TYPE

    def _ssh_kwargs(self) -> dict[str, Any]:
        kw: dict[str, Any] = {"username": self.ctx.target.get("ssh_user", "root")}
        pw = self.ctx.target.get("ssh_password")
        key = self.ctx.target.get("ssh_key")
        if pw:
            kw["password"] = pw
        if key:
            kw["key"] = key
        return kw

    def _host(self) -> str:
        # A migration may pin this connector to a specific cluster node (round-robin
        # spread); all qm/pvesm commands then target the node that owns the new VM.
        override = getattr(self, "_host_override", None)
        if override:
            return override
        host = self.ctx.target.get("node_host") or self.ctx.target.get("host")
        if not host:
            raise ConnectionValidationError("No Proxmox node_host configured")
        return host

    # purefa plugin error signatures (login/lock/timeout) seen in pvesm / qm output.
    _PUREFA_ERR_RE = re.compile(
        r"purefa:.*(login failed|failed|timed out|aborting|can't connect|locked)",
        re.I)

    async def _reachable_rr_hosts(self, conn_host: str) -> list[str]:
        """Online cluster nodes PHIF can actually SSH to (round-robin candidates).

        PVE shares root SSH keys among nodes internally, but PHIF authenticates with
        its own credential, which may only be authorized on the connection node — so
        we probe each peer and skip the unreachable ones (logged) rather than fail a
        migration by placing a VM on a node we can't drive. Cached per cluster."""
        ck = (id(asyncio.get_running_loop()), conn_host)
        cached = _RR_REACHABLE.get(ck)
        if cached is not None:
            return cached
        try:
            nodes = [n for n in await self.list_nodes()
                     if (n.info or {}).get("online", True)]
        except Exception:  # noqa: BLE001
            nodes = []
        hosts: list[str] = []
        for n in nodes:
            if n.host == conn_host:
                hosts.append(n.host)
                continue
            try:
                await self._ssh_on(n.host, "true", check=True, timeout=10)
                hosts.append(n.host)
            except Exception as exc:  # noqa: BLE001 — unreachable peer; skip it
                await self.ctx.emit(
                    f"[placement] skipping cluster node {n.host} for VM spread: "
                    f"SSH not usable ({type(exc).__name__})")
        if conn_host not in hosts:
            hosts.insert(0, conn_host)
        _RR_REACHABLE[ck] = hosts
        return hosts

    async def _pick_rr_node(self) -> str | None:
        """Round-robin an SSH-reachable online cluster node for VM spread. Returns
        the node's host/IP, or None to keep the default connection host (mock/dry-
        run, a single reachable node, or discovery failure)."""
        if self._is_mock_or_dry():
            return None
        conn_host = self._host()
        hosts = await self._reachable_rr_hosts(conn_host)
        if len(hosts) <= 1:
            return None
        key = self.ctx.target.get("node_host") or self.ctx.target.get("host") or "_"
        idx = _NODE_RR.get(key, 0)
        _NODE_RR[key] = idx + 1
        return hosts[idx % len(hosts)]

    async def _locked_retry(self, host: str, thunk, *,
                            attempts: int = 6, base_delay: float = 2.0) -> str:
        """Run ``thunk`` holding the per-host command lock (one command at a time
        per Proxmox host), retrying on a Proxmox lock-file timeout. Detects the
        failure both as a raised error (check=True) and in returned output
        (check=False)."""
        async with _host_cmd_lock(host):
            last_exc: Exception | None = None
            for i in range(attempts):
                try:
                    out = await thunk()
                except Exception as exc:  # noqa: BLE001
                    if _LOCK_TIMEOUT_RE.search(str(exc)) and i < attempts - 1:
                        last_exc = exc
                        await asyncio.sleep(base_delay * (i + 1))
                        continue
                    raise
                if _LOCK_TIMEOUT_RE.search(out or "") and i < attempts - 1:
                    await asyncio.sleep(base_delay * (i + 1))
                    continue
                return out
            if last_exc is not None:
                raise last_exc
            return out  # type: ignore[return-value]

    async def _ssh_on(self, host: str, command: str, *, check: bool = True,
                      timeout: float | None = None) -> str:
        return await self._locked_retry(host, lambda: self.ctx.runner.run_ssh(
            host, command, check=check, timeout=timeout, **self._ssh_kwargs()))

    async def _ssh_script_on(self, host: str, lines: list[str], *,
                             check: bool = True, timeout: float | None = None) -> str:
        return await self._locked_retry(host, lambda: self.ctx.runner.run_ssh_script(
            host, lines, check=check, timeout=timeout, **self._ssh_kwargs()))

    async def _ssh(self, command: str, *, check: bool = True,
                   timeout: float | None = None) -> str:
        return await self._ssh_on(self._host(), command, check=check, timeout=timeout)

    async def _ssh_script(self, lines: list[str], *, check: bool = True,
                          timeout: float | None = None) -> str:
        return await self._ssh_script_on(self._host(), lines, check=check, timeout=timeout)

    def _array_mgmt_target(self) -> tuple[str, int] | None:
        """Return (host, port) of the associated array's management endpoint.

        Parses ``ctx.array.endpoint`` (which may be a bare IP/host, ``host:port``,
        or a full ``https://host[:port]/...`` URL). Port defaults to 443. Returns
        None when no array is associated or the endpoint is unparseable.
        """
        if self.ctx.array is None:
            return None
        ep = (getattr(self.ctx.array, "endpoint", "") or "").strip()
        if not ep:
            return None
        ep = ep.split("://", 1)[-1].strip("/")  # drop scheme + trailing slashes
        host = ep.split("/", 1)[0]              # drop any path
        port = 443
        if ":" in host:
            h, p = host.rsplit(":", 1)
            if p.isdigit():
                host, port = h, int(p)
        return (host, port) if host else None

    async def _unreachable_nodes(self, nodes: list[ClusterNode], host: str,
                                 port: int) -> list[str]:
        """Return the nodes that cannot open a TCP connection to ``host:port``.

        Uses a dependency-free bash ``/dev/tcp`` probe with a hard 5s timeout, run
        on each node over SSH. Skipped (returns empty) in mock/dry-run.
        """
        if self._is_mock_or_dry():
            return []
        probe = (f"timeout 5 bash -c 'exec 3<>/dev/tcp/{host}/{port}' "
                 f"2>/dev/null && echo ARRAY_OK || echo ARRAY_FAIL")
        unreachable: list[str] = []
        for node in nodes:
            out = await self._ssh_on(node.host, probe, check=False)
            if "ARRAY_OK" not in (out or ""):
                unreachable.append(f"{node.name} ({node.host})")
        return unreachable

    def _is_mock_or_dry(self) -> bool:
        """True when no real node I/O happens (runner short-circuits).

        ``list_nodes`` cannot read ``/etc/pve/.members`` in mock/dry-run (SSH is
        skipped and returns ""), so it returns a synthetic cluster instead.
        """
        return bool(self.ctx.dry_run or getattr(self.ctx.runner, "mock", False)
                    or getattr(self.ctx.runner, "dry_run", False))

    @staticmethod
    def _split_csv(value: str) -> list[str]:
        return [s.strip() for s in (value or "").split(",") if s.strip()]

    @staticmethod
    def _fa_name(raw: str) -> str:
        """Sanitize a string into a valid FlashArray object name.

        FlashArray names allow only ``[A-Za-z0-9-]`` and must begin/end with an
        alphanumeric. A node IP/FQDN (e.g. ``192.0.2.58``) has dots, so map any
        invalid char to ``-`` and trim leading/trailing hyphens.
        """
        import re

        s = re.sub(r"[^A-Za-z0-9-]", "-", raw or "").strip("-")
        return s or "pve-node"

    @staticmethod
    def _as_list(value: Any) -> list[str]:
        """Normalise a MULTISELECT value into a list of strings.

        The UI sends a MULTISELECT as a JSON list, but action params may also
        arrive as a comma-separated string (or a single value). Accept all forms.
        """
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return [str(v).strip() for v in value if str(v).strip()]
        return [s.strip() for s in str(value).split(",") if s.strip()]

    # ------------------------------------------------------------------ #
    # Dynamic field options: enumerate bindable NICs / NVMe sources / HBAs
    # ------------------------------------------------------------------ #
    async def discover_options(self, kind: str) -> list[dict[str, Any]]:
        """Enumerate bindable interfaces/HBAs on the node for the UI dropdowns.

        Delegates to ``runner.discover_interfaces`` (mock-safe) for the known
        DiscoveryKind values used by the interface-binding fields in
        ``setup_connectivity``. Unknown kinds yield no options.
        """
        if kind in ("nics", "nvme_sources", "fc_hbas"):
            opts = await self.ctx.runner.discover_interfaces(
                self._host(), kind, **self._ssh_kwargs())
            # IP interfaces (NICs for iSCSI, source addrs for NVMe-TCP) are filtered
            # so only consistent, storage-reachable interfaces are offered. FC HBAs
            # have no subnet, so they pass through.
            if kind in ("nics", "nvme_sources"):
                # 1) Keep only interfaces whose subnet is configured on EVERY
                #    cluster node, so the chosen binding exists uniformly cluster-
                #    wide (the operator's selection is applied to all nodes).
                opts = await self._filter_common_subnet_interfaces(kind, opts)
                # 2) And, when an array is associated, restrict to interfaces that
                #    can actually reach the array's storage portals.
                if self.ctx.array is not None:
                    portals = await self.ctx.array.get_data_interfaces(
                        "nvme-tcp" if self._protocol() == "nvme-tcp" else "iscsi")
                    opts = nics_on_array_subnets(opts, portals)
            return opts
        # Array-side values for the connectivity/register forms (from the FA).
        if kind == "fa_host_name":
            node = self.ctx.target.get("node_host") or ""
            return [{"value": self._fa_name(node)}] if node else []
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
            # Discover the node's IQN / NQN / FC WWNs so the register-hosts form
            # can display them. Each option carries `field` = the form field to
            # populate so the UI can pre-fill the matching input.
            found = await self.ctx.runner.discover_initiators(
                self._host(), **self._ssh_kwargs())
            opts: list[dict[str, Any]] = []
            if found.get("iqn"):
                opts.append({"field": "node_iqn", "value": found["iqn"],
                             "label": f"iSCSI IQN — {found['iqn']}"})
            if found.get("nqn"):
                opts.append({"field": "node_nqn", "value": found["nqn"],
                             "label": f"NVMe NQN — {found['nqn']}"})
            if found.get("wwns"):
                wwns = ", ".join(self._normalize_wwn(w) for w in found["wwns"])
                opts.append({"field": "node_wwns", "value": wwns,
                             "label": f"FC WWNs — {wwns}"})
            return opts
        return []

    async def _filter_common_subnet_interfaces(
        self, kind: str, host_opts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Restrict candidate IP interfaces to subnets configured on every node.

        Discovers the same interface ``kind`` on each cluster node and keeps only
        the connection host's options whose subnet is present on ALL nodes (via
        :func:`nics_on_common_subnets`), so the binding the operator picks exists
        uniformly across the cluster. A single-node cluster (or missing per-node
        data) leaves ``host_opts`` unchanged.
        """
        nodes = await self.list_nodes()
        if len(nodes) <= 1:
            return host_opts
        per_node: dict[str, list[dict[str, Any]]] = {}
        for node in nodes:
            per_node[node.name] = await self.ctx.runner.discover_interfaces(
                node.host, kind, **self._ssh_kwargs())
        return nics_on_common_subnets(host_opts, per_node)

    @staticmethod
    def _normalize_wwn(raw: str) -> str:
        """Normalise an FC WWPN to the colon-delimited form the array expects.

        ``runner.discover_initiators`` returns bare 0x-stripped hex
        (e.g. ``21000024ff000001``). FlashArray ``create_host(wwns=...)`` and the
        rest of this connector use the colon-delimited WWPN form
        (``21:00:00:24:ff:00:00:01``). Already-delimited values pass through.

        Live FC validation confirmed FlashArray ``create_host(wwns=...)`` expects
        the colon-delimited WWPN form, which is what this normalisation produces.
        """
        tok = raw.strip().lower()
        if tok.startswith("0x"):
            tok = tok[2:]
        if ":" in tok:
            return tok
        if len(tok) == 16 and all(ch in "0123456789abcdef" for ch in tok):
            return ":".join(tok[i:i + 2] for i in range(0, 16, 2))
        return tok

    # ------------------------------------------------------------------ #
    # Cluster awareness: enumerate PVE nodes, validate uniform interfaces
    # ------------------------------------------------------------------ #
    async def list_nodes(self) -> list[ClusterNode]:
        """Enumerate the Proxmox cluster member nodes.

        Reads the pmxcfs cluster membership file (``/etc/pve/.members``) on the
        configured node over SSH and returns one :class:`ClusterNode` per member
        (using each member's cluster IP as its SSH/management host). Node-specific
        operations (host registration, connectivity) fan out across these; cluster-
        wide ones (storage definition) run once on the connection host.

        In mock/dry-run the runner short-circuits SSH (returns ""), so a synthetic
        2-node cluster is returned to keep the cluster flow exercisable. A
        standalone node (no ``.members`` / parse failure / single member) falls
        back to the single connection host.
        """
        conn_host = self._host()
        if self._is_mock_or_dry():
            await self.ctx.emit(
                f"[mock/dry-run] synthetic 2-node cluster seeded from {conn_host}")
            return [
                ClusterNode(name="pve-node1", host=conn_host,
                            info={"online": True, "mock": True}),
                ClusterNode(name="pve-node2", host="192.0.2.2",
                            info={"online": True, "mock": True}),
            ]

        try:
            raw = await self._ssh(f"cat {_PVE_MEMBERS_PATH}", check=False)
            members = (json.loads(raw) or {}).get("nodelist") or {}
            nodes: list[ClusterNode] = []
            for name, meta in members.items():
                meta = meta or {}
                ip = meta.get("ip") or name
                nodes.append(ClusterNode(
                    name=str(name), host=str(ip),
                    info={"online": bool(meta.get("online", 1))}))
            if nodes:
                await self.ctx.emit(
                    f"PVE cluster: {len(nodes)} node(s) -> "
                    f"{', '.join(n.name for n in nodes)}")
                return nodes
        except (ValueError, TypeError) as exc:  # not JSON / not a cluster
            await self.ctx.emit(
                f"Not a PVE cluster (or .members unreadable: {exc}); "
                f"treating {conn_host} as a single node")

        return [ClusterNode(name=str(conn_host), host=str(conn_host))]

    async def validate_cluster(self, **params: Any) -> OpResult:
        """Ensure every cluster node exposes the same storage interfaces.

        For the active protocol, discovers the relevant interface kind on each
        node (iSCSI->nics, FC->fc_hbas, NVMe-TCP->nvme_sources) via
        ``runner.discover_interfaces`` and compares the sets across nodes with
        :func:`compare_node_interfaces`. A uniform set is required so the chosen
        storage binding is valid cluster-wide.
        """
        proto = self._protocol()
        kind = {"iscsi": "nics", "fc": "fc_hbas",
                "nvme-tcp": "nvme_sources"}.get(proto, "nics")
        portals = (await self.ctx.array.get_data_interfaces(
            "nvme-tcp" if proto == "nvme-tcp" else "iscsi")
            if self.ctx.array is not None else [])
        array_iqn = ((await self.ctx.array.get_target_ports()).get("iqn")
                     if self.ctx.array is not None and proto == "iscsi" else None)
        nodes = await self.list_nodes()
        data: dict[str, Any] = {"protocol": proto, "kind": kind,
                                "nodes": [n.to_dict() for n in nodes]}

        # For iSCSI, first look at EXISTING iface bindings + targets on each node.
        # If hosts are already bound, use those bound NICs as the consistency basis
        # (and their subnets), and report prior connectivity to this/other arrays.
        if proto == "iscsi":
            bound: dict[str, list[str]] = {}
            existing_conn: dict[str, dict[str, Any]] = {}
            for node in nodes:
                st = await self.ctx.runner.discover_iscsi_state(
                    node.host, **self._ssh_kwargs())
                bound[node.name] = st.get("bound_nics", [])
                this_arr = [t for t in st.get("targets", [])
                            if array_iqn and t.get("target") == array_iqn]
                other = [t for t in st.get("targets", [])
                         if not array_iqn or t.get("target") != array_iqn]
                existing_conn[node.name] = {"this_array": this_arr, "other_arrays": other}
                await self.ctx.emit(
                    f"  {node.name}: bound NICs={bound[node.name]}; "
                    f"sessions to this array={len(this_arr)}, other arrays={len(other)}")
            data["existing_bound_nics"] = bound
            data["existing_targets"] = existing_conn

            any_bound = any(bound.values())
            if any_bound:
                consistent, detail = compare_node_interfaces(bound)
                data["rebind_recommended"] = not consistent
                if consistent:
                    return OpResult.ok(
                        f"Existing iSCSI NIC binding consistent across cluster: {detail}",
                        **data)
                return OpResult.fail(
                    "Existing iSCSI NIC binding differs across nodes (some unbound or "
                    f"bound differently): {detail}. Re-run connectivity with rebind=true "
                    "to make all nodes match.", **data)

        # No existing binding (or non-iSCSI): compare the storage-subnet interfaces.
        per_node: dict[str, list[str]] = {}
        for node in nodes:
            ifaces = await self.ctx.runner.discover_interfaces(
                node.host, kind, **self._ssh_kwargs())
            if kind == "nics":
                ifaces = nics_on_array_subnets(ifaces, portals)
            values = [str(o.get("value")) for o in ifaces if o.get("value")]
            per_node[node.name] = values
            await self.ctx.emit(f"  {node.name} ({node.host}): {values}")
        consistent, detail = compare_node_interfaces(per_node)
        data["per_node"] = per_node
        if consistent:
            return OpResult.ok(f"Cluster storage interfaces consistent: {detail}", **data)
        return OpResult.fail(f"Cluster storage interfaces inconsistent: {detail}", **data)

    @classmethod
    def wizard_steps(cls) -> list[str]:
        """Ordered wizard actions for Proxmox.

        Deploy the plugin and define the (cluster-wide) storage first, then
        register each node on the array and set up per-node connectivity.
        """
        # configure creates the storage DISABLED; enable it only after host
        # registration + connectivity are in place (last step).
        return ["deploy", "configure", "register_hosts", "setup_connectivity", "enable"]

    # ------------------------------------------------------------------ #
    # validate_connection (required)
    # ------------------------------------------------------------------ #
    async def validate_connection(self) -> OpResult:
        host = self.ctx.target.get("node_host") or self.ctx.target.get("host")
        await self.ctx.emit(f"Validating Proxmox node {host} ...")
        if not host:
            raise ConnectionValidationError("No Proxmox node_host configured")
        # pveversion confirms this is a reachable Proxmox node (mock-safe).
        out = await self._ssh("pveversion")
        await self.ctx.emit("Confirmed Proxmox VE via `pveversion`")
        array_info: dict[str, Any] = {}
        if self.ctx.array is not None:
            array_info = await self.ctx.array.info()
            await self.ctx.emit(f"FlashArray reachable: {array_info}")
        return OpResult.ok(f"Connected to {host}", host=host,
                           pveversion=out, array=array_info)

    # ------------------------------------------------------------------ #
    # DEPLOY_PLUGIN: push PureFAPlugin.pm + deps + reload daemons
    # ------------------------------------------------------------------ #
    async def deploy_integration(self, storage_id: str = "",
                                 nodes: "list[ClusterNode] | None" = None,
                                 **_: Any) -> OpResult:
        storage_id = self._storage_id(storage_id)
        await self.ctx.emit(
            f"Deploying purefa storage plugin ({PLUGIN_FILE.name} -> {PLUGIN_DEST})")
        if not PLUGIN_FILE.exists():
            return OpResult.fail(f"Plugin file missing: {PLUGIN_FILE}")
        plugin_src = PLUGIN_FILE.read_text(encoding="utf-8")
        await self.ctx.emit(
            f"Plugin {PLUGIN_FILE.name} ({len(plugin_src)} bytes) declares "
            f"storage type {PLUGIN_STORAGE_TYPE!r}")
        if self.ctx.dry_run:
            return OpResult.ok("[dry-run] plugin deploy planned",
                               artifacts={"storage_id": storage_id},
                               status="planned")

        # The plugin file must exist on EVERY cluster node (each runs the Perl
        # plugin locally), so push + reload on each member. (Storage *definition*
        # is cluster-wide via pmxcfs and is handled once in configure().) Callers
        # (reconcile_cluster) may pass an explicit node subset.
        if nodes is None:
            nodes = await self.list_nodes()

        # PREFLIGHT: the purefa plugin runs ON each node and talks to the array's
        # MANAGEMENT endpoint (provision/snapshot/clone/resize/status). If a node
        # can't reach it, the storage would install but hang `pvesm status` and
        # show inactive. Verify reachability up front and fail with a clear message
        # rather than deploying a broken integration.
        target = self._array_mgmt_target()
        if target is not None:
            host_ip, port = target
            unreachable = await self._unreachable_nodes(nodes, host_ip, port)
            if unreachable:
                return OpResult.fail(
                    f"FlashArray management endpoint {host_ip}:{port} is not "
                    f"reachable from: {', '.join(unreachable)}. The purefa plugin "
                    f"runs on each node and needs the array management IP reachable "
                    f"for provisioning, snapshots, clones and status. Fix routing/"
                    f"firewall (or the configured endpoint) and re-run.",
                    artifacts={"endpoint": f"{host_ip}:{port}",
                               "unreachable_nodes": unreachable})
            await self.ctx.emit(
                f"Array management {host_ip}:{port} reachable from all "
                f"{len(nodes)} node(s)")

        heredoc = f"cat > {PLUGIN_DEST} <<'PUREFA_PM_EOF'\n{plugin_src}\nPUREFA_PM_EOF"
        for node in nodes:
            await self.ctx.emit(f"-> deploying plugin on {node.name} ({node.host})")
            # Ensure the Custom plugin dir exists, install perl REST deps, then
            # copy the plugin onto the node via an SSH heredoc (mock-safe).
            # Install perl REST deps + multipath-tools, but tolerate nodes that
            # can't reach package repos (offline / no-subscription): skip apt
            # entirely when the deps are already present (common on PVE), and
            # only hard-fail if the JSON module is genuinely missing afterward.
            await self._ssh_script_on(node.host, [
                "if perl -MJSON -MLWP::UserAgent -e1 2>/dev/null && "
                "command -v multipath >/dev/null 2>&1; then "
                "echo 'perl deps + multipath already present; skipping apt'; "
                "else apt-get update || true; "
                "apt-get install -y libjson-perl liblwp-protocol-https-perl "
                "libwww-perl multipath-tools || true; fi",
                "perl -MJSON -e1 || { echo 'ERROR: perl JSON missing and apt "
                "could not install it'; exit 1; }",
                f"mkdir -p {Path(PLUGIN_DEST).parent.as_posix()}",
            ])
            await self._ssh_on(node.host, heredoc)
            # Reload the PVE daemons so the new storage type registers.
            await self._ssh_on(node.host, "systemctl reload pvedaemon pveproxy pvestatd")
            # Verify the storage subsystem is happy.
            await self._ssh_on(node.host, "timeout 30 pvesm status", check=False)
        await self.ctx.emit(
            f"purefa storage plugin installed at {PLUGIN_DEST} on "
            f"{len(nodes)} node(s)")
        return OpResult.ok("purefa storage plugin deployed",
                           artifacts={"storage_id": storage_id,
                                      "plugin": PLUGIN_DEST,
                                      "storage_type": PLUGIN_STORAGE_TYPE,
                                      "nodes": [n.to_dict() for n in nodes]},
                           status="plugin_installed")

    # ------------------------------------------------------------------ #
    # CONFIGURE: define the purefa storage in /etc/pve/storage.cfg
    # ------------------------------------------------------------------ #
    async def configure(self, storage_id: str = "", pure_endpoint: str = "",
                        pure_api_token: str = "", host_group: str = "",
                        content: str = "images,rootdir", **_: Any) -> OpResult:
        storage_id = self._storage_id(storage_id)
        proto = self._protocol()
        host_group = host_group or self.ctx.target.get("host_group", "")
        # Prefer an explicit token, else reuse the array's original token.
        pure_api_token = self.ctx.resolve_token(pure_api_token) or ""
        pure_endpoint = pure_endpoint or (self.ctx.array.endpoint if self.ctx.array else "")
        await self.ctx.emit(
            f"Defining purefa storage {storage_id!r} (proto={proto}, "
            f"host_group={host_group!r}) in /etc/pve/storage.cfg")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would define storage {storage_id}",
                               artifacts={"storage_id": storage_id})

        # Register the storage via `pvesm` rather than hand-editing storage.cfg.
        # pvesm validates options against the plugin and writes a correctly
        # separated section (hand-appending merged the stanza into the previous
        # section and PVE rejected it). Idempotent: `set` if it already exists.
        import shlex

        sid = shlex.quote(storage_id)
        # All options for create; only the MUTABLE ones for an update — endpoint,
        # token and protocol are `fixed` plugin properties and pvesm set rejects
        # them, so a re-run that tried to set them failed (rc 255). Idempotent:
        # create if absent, else update host_group/content on the existing entry.
        # Create the storage DISABLED — it must not be enabled/activated until the
        # whole configuration (plugin install, host registration, connectivity) is
        # done, so PVE/VMs never target a storage with no working path. The wizard
        # runs the `enable` step last; standalone, run "Enable storage" when ready.
        create_opts = " ".join([
            f"--pure_endpoint {shlex.quote(pure_endpoint)}",
            f"--pure_api_token {shlex.quote(pure_api_token)}",
            f"--protocol {shlex.quote(proto)}",
            f"--host_group {shlex.quote(host_group)}",
            f"--content {shlex.quote(content)}",
            "--shared 1",
            "--disable 1",
        ])
        update_opts = " ".join([
            f"--host_group {shlex.quote(host_group)}",
            f"--content {shlex.quote(content)}",
        ])
        await self._ssh(
            f"pvesm add purefa {sid} {create_opts} 2>/dev/null || "
            f"pvesm set {sid} {update_opts}")
        await self._ssh("timeout 30 pvesm status", check=False)
        await self.ctx.emit(
            f"Storage {storage_id} (type purefa) defined (disabled until 'enable')")
        return OpResult.ok(f"Configured purefa storage {storage_id} (disabled)",
                           artifacts={"storage_id": storage_id,
                                      "storage_type": PLUGIN_STORAGE_TYPE,
                                      "host_group": host_group, "enabled": False})

    async def enable_storage(self, storage_id: str = "", **_: Any) -> OpResult:
        """Enable the purefa storage once the full configuration is in place.

        Final step: the storage was created disabled by ``configure`` so PVE/VMs
        never target it before host registration + connectivity exist. This flips
        ``disable`` off (cluster-wide; storage.cfg is shared) and verifies it.
        """
        import shlex

        storage_id = self._storage_id(storage_id)
        sid = shlex.quote(storage_id)
        await self.ctx.emit(f"Enabling purefa storage {storage_id}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would enable storage {storage_id}",
                               artifacts={"storage_id": storage_id})
        await self._ssh(f"pvesm set {sid} --disable 0")
        await self._ssh("timeout 30 pvesm status", check=False)
        return OpResult.ok(f"Enabled purefa storage {storage_id}",
                           artifacts={"storage_id": storage_id, "enabled": True})

    async def dispatch(self, action_id: str, params: dict[str, Any]) -> OpResult:
        # Route the custom "enable" action; everything else uses default routing.
        if action_id == "enable":
            return await self.enable_storage(**params)
        if action_id == "provision_nfs_datastore":
            return await self.provision_nfs_datastore(**params)
        if action_id == "teardown_nfs_datastore":
            return await self.teardown_nfs_datastore(**params)
        # assess_cluster / reconcile_cluster are routed by the base dispatch.
        return await super().dispatch(action_id, params)

    # ------------------------------------------------------------------ #
    # HOST_REGISTER: one FA host per cluster node, all in one host group
    # ------------------------------------------------------------------ #
    def _explicit_initiators(self, node_iqn: str, node_nqn: str,
                             node_wwns: str) -> tuple[str, str, list[str]]:
        """Resolve operator-supplied initiators (action params over connection).

        These apply only to the connection node; other cluster members always
        auto-discover their own initiators over SSH.
        """
        node_iqn = node_iqn or self.ctx.target.get("node_iqn", "")
        node_nqn = node_nqn or self.ctx.target.get("node_nqn", "")
        wwn_list = self._split_csv(node_wwns) or self._split_csv(
            self.ctx.target.get("node_wwns", ""))
        return node_iqn, node_nqn, wwn_list

    async def _resolve_node_initiators(
        self, node_host: str, proto: str, *, node_iqn: str = "",
        node_nqn: str = "", wwn_list: list[str] | None = None,
    ) -> tuple[list[str] | None, list[str] | None, list[str] | None]:
        """Return (iqns, nqns, wwns) for ``node_host`` and the active protocol.

        Explicit values win; otherwise the node's initiators are auto-discovered
        over SSH (mock/dry-run yields synthetic values). Only the initiator
        matching the protocol is registered.
        """
        wwn_list = list(wwn_list or [])
        explicit = {"iscsi": bool(node_iqn), "nvme-tcp": bool(node_nqn),
                    "fc": bool(wwn_list)}.get(proto, False)
        if not explicit:
            discovered = await self.ctx.runner.discover_initiators(
                node_host, **self._ssh_kwargs())
            if proto == "iscsi" and discovered.get("iqn"):
                node_iqn = discovered["iqn"]
            elif proto == "nvme-tcp" and discovered.get("nqn"):
                node_nqn = discovered["nqn"]
            elif proto == "fc" and discovered.get("wwns"):
                wwn_list = [self._normalize_wwn(w) for w in discovered["wwns"]]
            await self.ctx.emit(
                f"Auto-discovered initiators for {node_host} ({proto}): "
                f"iqn={'yes' if discovered.get('iqn') else 'no'} "
                f"nqn={'yes' if discovered.get('nqn') else 'no'} "
                f"wwns={len(discovered.get('wwns') or [])}")
        iqns = [node_iqn] if (proto == "iscsi" and node_iqn) else None
        nqns = [node_nqn] if (proto == "nvme-tcp" and node_nqn) else None
        wwns = wwn_list if (proto == "fc" and wwn_list) else None
        return iqns, nqns, wwns

    async def register_hosts(self, host_group: str = "", host_name: str = "",
                             node_iqn: str = "", node_nqn: str = "",
                             node_wwns: str = "",
                             nodes: "list[ClusterNode] | None" = None,
                             **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        proto = self._protocol()
        # NFS uses a mounted FlashArray File export, not block LUNs: there are no
        # host initiators to register. Skip host registration entirely.
        if proto == "nfs":
            await self.ctx.emit("protocol=nfs: skipping FlashArray host registration "
                                "(NFS export is mounted, no initiators)")
            return OpResult.ok("NFS: no host registration required",
                               artifacts={"protocol": "nfs", "skipped": True})
        # Carry over the host group from the hypervisor's connection when the
        # action field is left blank.
        host_group = host_group or self.ctx.target.get("host_group", "")
        if not host_group:
            return OpResult.fail("No host_group set (configure it on the hypervisor)")

        # Allow callers (reconcile_cluster) to scope to an explicit node subset;
        # default to the full cluster.
        if nodes is None:
            nodes = await self.list_nodes()
        conn_host = self._host()
        # Operator-supplied initiators / host name apply to the connection node
        # only (the rest of the cluster auto-discovers).
        x_iqn, x_nqn, x_wwns = self._explicit_initiators(node_iqn, node_nqn, node_wwns)

        registered: list[str] = []
        host_specs: list[dict[str, Any]] = []
        for node in nodes:
            is_conn = node.host == conn_host
            # FlashArray host names allow only [A-Za-z0-9-]; an IP/FQDN has dots,
            # so sanitize (e.g. 192.0.2.58 -> 192-0-2-58). Prefer the node's
            # cluster name; fall back to its host. host_name overrides only for the
            # connection node when single-node (no cluster name to clash with).
            if is_conn and host_name:
                fa_name = self._fa_name(host_name)
            else:
                fa_name = self._fa_name(node.name or node.host or "pve-node")

            iqns, nqns, wwns = await self._resolve_node_initiators(
                node.host, proto,
                node_iqn=x_iqn if is_conn else "",
                node_nqn=x_nqn if is_conn else "",
                wwn_list=x_wwns if is_conn else [])

            # Fail only if discovery yielded nothing AND nothing supplied — never
            # in dry-run (which performs no SSH/discovery against a real node).
            if not self.ctx.dry_run and not (iqns or nqns or wwns):
                return OpResult.fail(
                    f"No {proto} initiator provided or discoverable for "
                    f"{fa_name!r} ({node.host}) (supply node_iqn/node_nqn/"
                    "node_wwns or ensure the node exposes one)")

            await self.ctx.emit(
                f"Registering host {fa_name!r} ({node.host}, proto={proto}) "
                f"iqns={iqns} nqns={nqns} wwns={wwns}")
            registered.append(fa_name)
            host_specs.append({"name": fa_name, "iqns": iqns, "wwns": wwns,
                               "nqns": nqns})

        if self.ctx.dry_run:
            return OpResult.ok(
                f"[dry-run] would register {len(registered)} host(s) "
                f"{registered} in host group {host_group}",
                artifacts={"hosts": registered, "host_group": host_group})

        # Reuse pre-existing FA hosts (by initiator), adopt an existing host group
        # if the nodes already belong to one, and fail descriptively on a multi-group
        # conflict. (apply_host_group is shared across all connectors.)
        res = await self.apply_host_group(host_group, host_specs)
        if res.get("conflict"):
            return OpResult.fail(res["conflict"],
                                 artifacts={"host_group": host_group, "protocol": proto})
        host_group = res["host_group"]
        registered = res["hosts"]
        await self.ctx.emit(
            f"Host group {host_group} ready with {len(registered)} host(s): "
            f"{registered}")
        return OpResult.ok(
            f"Registered {len(registered)} host(s) in host group {host_group}",
            artifacts={"hosts": registered, "host": registered[0] if registered else "",
                       "host_group": host_group, "protocol": proto,
                       "adopted_host_group": res["adopted"],
                       "nodes": [n.to_dict() for n in nodes]})

    # ------------------------------------------------------------------ #
    # CONNECTIVITY: transport + multipath + persistent connections
    # ------------------------------------------------------------------ #
    async def setup_connectivity(self, portals: str = "", subsystem_nqn: str = "",
                                 iqn_target: str = "", iscsi_nics: Any = None,
                                 nvme_sources: Any = None, nvme_options: str = "",
                                 fc_hbas: Any = None, rebind: Any = False,
                                 nodes: "list[ClusterNode] | None" = None,
                                 **_: Any) -> OpResult:
        proto = self._protocol()
        # NFS has no block transport: no iSCSI/NVMe login, no multipath. The NFS
        # export is mounted by the PVE-native 'nfs' storage (provision_nfs_datastore).
        if proto == "nfs":
            await self.ctx.emit("protocol=nfs: skipping block connectivity "
                                "(no iSCSI/NVMe login or multipath for NFS)")
            return OpResult.ok("NFS: no block connectivity required",
                               artifacts={"protocol": "nfs", "skipped": True})
        rebind = str(rebind).lower() in ("1", "true", "yes", "on")
        portal_list = [p.strip() for p in portals.split(",") if p.strip()]
        # Interface binding selections (only the active protocol's is applied).
        iscsi_nics_l = self._as_list(iscsi_nics)
        nvme_sources_l = self._as_list(nvme_sources)
        fc_hbas_l = self._as_list(fc_hbas)
        binding = {"iscsi_nics": iscsi_nics_l, "nvme_sources": nvme_sources_l,
                   "fc_hbas": fc_hbas_l}
        if iscsi_nics_l or nvme_sources_l or fc_hbas_l:
            await self.ctx.emit(
                f"Interface binding: iscsi_nics={iscsi_nics_l} "
                f"nvme_sources={nvme_sources_l} fc_hbas={fc_hbas_l}")

        # Discover the array's portal IPs + target IQN/NQN from the FlashArray
        # when the operator didn't supply them (FC has no portals — zoning only).
        if self.ctx.array is not None and proto in ("iscsi", "nvme-tcp"):
            service = "nvme-tcp" if proto == "nvme-tcp" else "iscsi"
            if not portal_list:
                portal_list = await self.ctx.array.get_data_interfaces(service)
                await self.ctx.emit(f"Discovered array {service} portals: {portal_list}")
            if proto == "iscsi" and not iqn_target:
                iqn_target = (await self.ctx.array.get_target_ports()).get("iqn") or ""
                await self.ctx.emit(f"Discovered array iSCSI target IQN: {iqn_target}")
            if proto == "nvme-tcp" and not subsystem_nqn:
                subsystem_nqn = (await self.ctx.array.get_target_ports()).get("nqn") or ""
                await self.ctx.emit(f"Discovered array NVMe subsystem NQN: {subsystem_nqn}")

        # Fail fast with an actionable message when an IP transport has no portals
        # (e.g. the array is FC/NVMe-FC only, or iSCSI/NVMe-TCP networking isn't
        # configured on it) — otherwise iscsiadm/nvme would fail cryptically.
        if proto in ("iscsi", "nvme-tcp") and not portal_list and not self.ctx.dry_run:
            return OpResult.fail(
                f"No {proto} portal IPs found on the array. Its {proto} interfaces "
                f"have no IP configured (this array may be Fibre Channel / NVMe-FC "
                f"only). Configure {proto} networking on the FlashArray, switch the "
                f"hypervisor protocol to 'fc', or pass portals explicitly.")
        if proto == "iscsi" and not iqn_target and not self.ctx.dry_run:
            return OpResult.fail(
                "No iSCSI target IQN found on the array (no iSCSI ports). This array "
                "appears to be Fibre Channel / NVMe-FC only — switch the hypervisor "
                "protocol to 'fc'.")

        if nodes is None:
            nodes = await self.list_nodes()
        await self.ctx.emit(
            f"Configuring {proto} connectivity on {len(nodes)} node(s) "
            f"to portals {portal_list}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would configure {proto} connectivity",
                               artifacts={"protocol": proto, "portals": portal_list,
                                          "nodes": [n.to_dict() for n in nodes],
                                          **binding})

        if proto not in ("nvme-tcp", "iscsi", "fc"):
            return OpResult.fail(f"Unsupported protocol {proto!r}")

        # Fan out the transport + multipath setup to EVERY node. The per-node body
        # is identical, parameterized by node.host.
        for node in nodes:
            await self.ctx.emit(f"-> connectivity on {node.name} ({node.host})")
            if proto == "nvme-tcp":
                await self._connect_nvme_tcp(node.host, portal_list, subsystem_nqn,
                                             nvme_sources_l, nvme_options)
            elif proto == "iscsi":
                await self._connect_iscsi(node.host, portal_list, iqn_target,
                                          iscsi_nics_l, rebind=rebind)
            else:  # fc
                await self._connect_fc(node.host, fc_hbas_l)

        # Persist the interface binding ONCE — storage.cfg is cluster-wide (pmxcfs),
        # so pvesm set runs on the connection host only.
        await self._persist_binding(**{k: v for k, v in binding.items() if v})

        artifacts: dict[str, Any] = {"protocol": proto, "portals": portal_list,
                                     "nodes": [n.to_dict() for n in nodes], **binding}
        if proto == "nvme-tcp":
            artifacts["subsystem_nqn"] = subsystem_nqn
        await self.ctx.emit(
            f"{proto} connectivity configured on {len(nodes)} node(s)")
        return OpResult.ok(f"{proto} connectivity configured", artifacts=artifacts)

    async def _persist_binding(self, **keys: Any) -> None:
        """Persist the chosen interface binding onto the purefa storage entry.

        Sets ``iscsi_nics`` / ``nvme_sources`` / ``fc_hbas`` (and any extra
        binding keys) as options on the storage via ``pvesm set`` so the purefa
        Perl plugin (PureFAPlugin.pm) can honor the operator's binding. Using
        pvesm (rather than editing storage.cfg by hand) validates the options
        against the plugin and keeps the section well-formed.

        PureFAPlugin.pm declares these binding keys in its properties()/options()
        so pvesm accepts them, and its activate_storage/activate_volume paths
        honor them (open-iscsi ifaces limited to the named NICs, NVMe-TCP
        host-traddr per source, FC restricted to the named HBA WWPNs). When a key
        is unset the plugin's default transport path is unchanged.
        """
        import shlex

        opts = " ".join(
            f"--{k} {shlex.quote(','.join(v) if isinstance(v, list) else str(v))}"
            for k, v in keys.items() if v
        )
        if not opts:
            return
        storage_id = self._storage_id()
        await self.ctx.emit(
            f"Setting interface binding on storage {storage_id!r}: "
            f"{ {k: v for k, v in keys.items() if v} }")
        await self._ssh(f"pvesm set {shlex.quote(storage_id)} {opts}", check=False)

    async def _connect_nvme_tcp(self, host: str, portals: list[str],
                                subsystem_nqn: str,
                                nvme_sources: list[str] | None = None,
                                nvme_options: str = "") -> None:
        # Per the Everpure NVMe-TCP guide: install nvme-cli, load modules persistently,
        # enable native multipath, generate host NQN, set queue-depth IO policy,
        # then connect persistently via nvmf-autoconnect. Runs on a single node;
        # setup_connectivity loops this over every cluster member.
        setup = [
            "apt-get update || true",
            "apt-get install -y nvme-cli",
            "modprobe nvme && modprobe nvme-tcp && modprobe nvme-core",
            "printf 'nvme\\nnvme-tcp\\nnvme-core\\n' > /etc/modules-load.d/nvme-tcp.conf",
            "echo 'options nvme_core multipath=Y' > /etc/modprobe.d/nvme-tcp.conf",
            "mkdir -p /etc/nvme",
            "[ -f /etc/nvme/hostnqn ] || nvme gen-hostnqn > /etc/nvme/hostnqn",
            # queue-depth IO policy via udev (Everpure recommendation)
            "printf 'ACTION==\"add\", SUBSYSTEM==\"nvme-subsystem\", "
            "ATTR{iopolicy}=\"queue-depth\"\\n' "
            "> /etc/udev/rules.d/99-nvme-iopolicy.rules",
            "udevadm control --reload-rules && udevadm trigger",
        ]
        await self._ssh_script_on(host, setup)

        sources = nvme_sources or []
        opts = (" " + nvme_options.strip()) if nvme_options.strip() else ""
        for portal in portals:
            # When source interfaces are selected, bind one connection per source
            # via host-traddr (-w). Otherwise connect once with no explicit source.
            # `portal` is an array NVMe-TCP data IP discovered from the FlashArray
            # (get_data_interfaces), or an operator override.
            if sources:
                for src in sources:
                    cmd = (
                        f"nvme connect -t tcp -a {portal} -s {_NVME_CONNECT_PORT} "
                        f"-n {subsystem_nqn} -w {src} "
                        f"--ctrl-loss-tmo={_NVME_CTRL_LOSS_TMO} "
                        f"--reconnect-delay={_NVME_RECONNECT_DELAY}{opts}")
                    await self._ssh_on(host, cmd)
            else:
                cmd = (
                    f"nvme connect -t tcp -a {portal} -s {_NVME_CONNECT_PORT} "
                    f"-n {subsystem_nqn} "
                    f"--ctrl-loss-tmo={_NVME_CTRL_LOSS_TMO} "
                    f"--reconnect-delay={_NVME_RECONNECT_DELAY}{opts}")
                await self._ssh_on(host, cmd)

        # Persistent connections on boot.
        await self._ssh_script_on(host, [
            "systemctl enable nvmf-autoconnect.service",
            "nvme connect-all",
        ])
        await self.ctx.emit(f"NVMe-TCP persistent connections configured on {host}")

    async def _write_multipath_conf(self, host: str) -> None:
        """Install the Everpure FlashArray multipath config as a drop-in and (re)start
        multipathd.

        Writes a dedicated /etc/multipath/conf.d/pure.conf that multipathd
        auto-includes, rather than overwriting the operator's /etc/multipath.conf
        (which may blacklist local disks or configure other arrays). The main
        file is only created (empty) if it doesn't exist yet, since multipathd
        needs it present to enable multipathing. Re-running just rewrites our own
        drop-in (idempotent). Without the PURE device stanza + find_multipaths,
        multipathd won't group the iSCSI/FC paths to one wwid.
        """
        await self.ctx.emit(
            f"Writing Everpure multipath drop-in {_MULTIPATH_DROPIN} on {host}")
        await self._ssh_on(host, "mkdir -p /etc/multipath/conf.d")
        # Never clobber an existing main config — only create it if absent.
        await self._ssh_on(
            host, "test -f /etc/multipath.conf || touch /etc/multipath.conf")
        await self._ssh_on(
            host,
            f"cat > {_MULTIPATH_DROPIN} <<'PUREFA_MPATH_EOF'\n"
            f"{_MULTIPATH_CONF}PUREFA_MPATH_EOF")
        await self._ssh_script_on(host, [
            "systemctl enable --now multipathd",
            "systemctl reload multipathd || systemctl restart multipathd",
            "sleep 1",
            "multipath -r || true",
        ], check=False)

    async def _connect_iscsi(self, host: str, portals: list[str], iqn_target: str,
                             iscsi_nics: list[str] | None = None,
                             rebind: bool = False) -> None:
        # iSCSI connectivity (open-iscsi + multipath-tools) was validated on the
        # live cluster: the commands below and the multipath.conf device stanza
        # (vendor PURE, find_multipaths no) are confirmed against the iSCSI block
        # path. (The m_proxmox iSCSI topic renders JS-only, so this was verified
        # empirically rather than scraped.)
        setup = [
            "if command -v iscsiadm >/dev/null 2>&1 && command -v multipath "
            ">/dev/null 2>&1; then echo 'iscsi/multipath present'; else "
            "apt-get update || true; apt-get install -y open-iscsi multipath-tools "
            "|| true; fi",
            "systemctl enable --now iscsid multipathd",
        ]
        await self._ssh_script_on(host, setup)

        nics = iscsi_nics or []
        # Interface binding: create one open-iscsi iface per selected NIC and bind
        # it to that NIC's net_ifacename, then discover + login bound to each
        # iface. Idempotent (--op=new tolerated if it exists); when rebind=true we
        # delete any existing phif_ iface first so all nodes end up uniform.
        iface_names: list[str] = []
        for nic in nics:
            iface = f"phif_{nic}"
            iface_names.append(iface)
            if rebind:
                await self._ssh_on(host, f"iscsiadm -m iface -I {iface} --op=delete",
                                   check=False)
            await self._ssh_on(host, f"iscsiadm -m iface -I {iface} --op=new",
                               check=False)
            await self._ssh_on(
                host,
                f"iscsiadm -m iface -I {iface} --op=update "
                f"-n iface.net_ifacename -v {nic}")

        # Multi-NIC iSCSI ARP-flux fix on the selected storage NICs (persisted +
        # live). Without it, dual-NIC iSCSI on a shared subnet binds sessions to
        # the wrong path or logs in intermittently.
        if nics:
            await self.ctx.emit(f"[{host}] applying iSCSI ARP-flux sysctls for {nics}")
            await self._ssh_on(host, arp_flux_cmd(nics), check=False)

        # Discovery + login are idempotent-by-intent: a node that already has the
        # target discovered / is already logged in returns non-zero (e.g. rc=11/15
        # "already exists"). Tolerate that (check=False) so connectivity converges
        # uniformly across every cluster node instead of aborting on the first
        # already-configured host.
        for portal in portals:
            if iface_names:
                for iface in iface_names:
                    await self._ssh_on(
                        host,
                        f"iscsiadm -m discovery -t st -p {portal}:{_ISCSI_PORT} "
                        f"-I {iface}", check=False)
            else:
                await self._ssh_on(
                    host,
                    f"iscsiadm -m discovery -t sendtargets -p {portal}:{_ISCSI_PORT}",
                    check=False)

        target = ("-T " + iqn_target) if iqn_target else ""
        if iface_names:
            for iface in iface_names:
                await self._ssh_on(
                    host,
                    f"iscsiadm -m node {target} -I {iface} --login".replace("  ", " "),
                    check=False)
        else:
            await self._ssh_on(
                host, f"iscsiadm -m node {target} --login".replace("  ", " "),
                check=False)
        await self._write_multipath_conf(host)
        await self.ctx.emit(f"iSCSI connectivity + multipath configured on {host}")

    async def _connect_fc(self, host: str,
                          fc_hbas: list[str] | None = None) -> None:
        # Fibre Channel has NO host-side login/discovery step (unlike iSCSI's
        # sendtargets+login and NVMe-TCP's `nvme connect`). Connectivity is
        # established by SAN ZONING on the fabric switch (zone the node HBA WWPNs
        # to the FlashArray FC target ports) plus registering those WWNs on the
        # array (see register_hosts). Once the volume is connected to the host
        # group on the array, the node only needs to RESCAN the FC/SCSI bus and
        # let multipath assemble the device.
        #
        # PREREQUISITE (operator action, off-host): create FC zones on the switch
        # pairing each node HBA WWPN with the array's FC target WWPNs, and ensure
        # the host group is registered on the array by WWN.
        #
        # FC connectivity was validated on the live cluster: issuing an FC LIP per
        # HBA, rescanning every SCSI host, then preferring rescan-scsi-bus.sh
        # (sg3-utils) when present, plus the multipath.conf device stanza (vendor
        # PURE, find_multipaths no), are confirmed for the FC block path. (The
        # m_proxmox FC topic renders JS-only, so this was verified empirically.)
        await self._ssh_script_on(host, [
            "apt-get update || true",
            # sg3-utils provides rescan-scsi-bus.sh; multipath-tools for /dev/mapper.
            "apt-get install -y multipath-tools sg3-utils",
            "systemctl enable --now multipathd",
        ])
        # Trigger an FC LIP per HBA so the fabric re-presents LUNs, then rescan
        # every SCSI host, then prefer rescan-scsi-bus.sh if present. No login.
        await self._ssh_script_on(host, [
            "for f in /sys/class/fc_host/host*/issue_lip; do "
            "[ -w \"$f\" ] && echo 1 > \"$f\" || true; done",
            "for h in /sys/class/scsi_host/host*/scan; do echo '- - -' > $h; done",
            "command -v rescan-scsi-bus.sh >/dev/null 2>&1 && "
            "rescan-scsi-bus.sh -a || true",
        ], check=False)
        await self._write_multipath_conf(host)
        # FC interface binding is a SELECTION, not a host-side login: zoning is
        # external (fabric switch). The binding is persisted once (cluster-wide)
        # by setup_connectivity after the per-node fan-out.
        hbas = fc_hbas or []
        if hbas:
            await self.ctx.emit(f"Limiting FC to selected HBAs: {hbas} "
                                "(zoning is external to PHIF)")
        await self.ctx.emit(
            f"FC rescan complete on {host}; multipath assembled (SAN zoning assumed "
            "on fabric, no IP login step performed)")

    # ------------------------------------------------------------------ #
    # RECONCILE_CLUSTER: detect membership drift; deploy to new (ready) nodes
    # ------------------------------------------------------------------ #
    def _expected_fa_host(self, node: ClusterNode) -> str:
        """The FlashArray host name register_hosts() would create for ``node``.

        Mirrors register_hosts: the sanitized node cluster name (falling back to
        its host). This is the per-node identity used to match cluster nodes to FA
        host-group members.
        """
        return self._fa_name(node.name or node.host or "pve-node")

    async def _baseline_storage_subnets(
        self, configured_nodes: list[ClusterNode], array_portals: list[str],
    ) -> set[str]:
        """Storage NIC subnets of an already-configured node (readiness baseline).

        Picks the first already-configured node, discovers its NICs, keeps those on
        the array's portal subnets, and returns their networks as strings. Empty in
        mock or when nothing matches (score_host_readiness then skips the baseline
        comparison).
        """
        if self._is_mock_or_dry() or not array_portals or not configured_nodes:
            return set()
        import ipaddress

        portals = []
        for p in array_portals:
            try:
                portals.append(ipaddress.ip_address(p))
            except ValueError:
                continue
        subnets: set[str] = set()
        ref = configured_nodes[0]
        nics = await self.ctx.runner.discover_interfaces(
            ref.host, "nics", **self._ssh_kwargs())
        for nic in nics:
            net = _nic_network(nic)
            if net and any(ip in net for ip in portals):
                subnets.add(str(net))
        return subnets

    async def assess_cluster(self, **params: Any) -> OpResult:
        """READ-ONLY cluster drift report (no array/node changes).

        Compares current PVE cluster membership to the FlashArray host group:
        nodes whose expected FA host is not yet a group member are "new" (each
        scored for deploy-readiness via :meth:`score_host_readiness`); group
        members with no matching node are "departed".
        """
        proto = self._protocol()
        host_group = (params.get("host_group")
                      or self.ctx.target.get("host_group", ""))
        nodes = await self.list_nodes()
        node_names = [n.name for n in nodes]

        # NFS has no host group / initiators -> no block-host drift to assess.
        if proto == "nfs" or self.ctx.array is None or not host_group:
            return OpResult.ok(
                f"{len(nodes)} node(s); no host-group assessment "
                f"({'nfs' if proto == 'nfs' else 'no array/host_group'})",
                nodes=node_names, new_hosts=[], departed_hosts=[])

        members = await self.ctx.array.get_host_group_members(host_group)
        member_set = set(members)
        expected = {self._expected_fa_host(n): n for n in nodes}

        # NEW: nodes whose FA host isn't a group member yet.
        new_nodes = [(fa, node) for fa, node in expected.items()
                     if fa not in member_set]
        # DEPARTED: members not matching any current node.
        departed = sorted(m for m in members if m not in expected)

        array_portals: list[str] = []
        if proto in ("iscsi", "nvme-tcp"):
            service = "nvme-tcp" if proto == "nvme-tcp" else "iscsi"
            array_portals = await self.ctx.array.get_data_interfaces(service)
        # Baseline = storage subnets of an already-configured node, so new nodes
        # are required to sit on the same storage network as the existing cluster.
        configured = [n for n in nodes if self._expected_fa_host(n) in member_set]
        baseline = await self._baseline_storage_subnets(configured, array_portals)

        new_hosts: list[dict[str, Any]] = []
        for fa, node in new_nodes:
            if self._is_mock_or_dry():
                # Mock: no real SSH; report a sensible "ready" verdict.
                inits: dict[str, Any] = {}
                host_nics: list[dict[str, Any]] = []
                reachable = True
                verdict = {"ready": True, "reasons": []}
            else:
                inits = await self.ctx.runner.discover_initiators(
                    node.host, **self._ssh_kwargs())
                host_nics = await self.ctx.runner.discover_interfaces(
                    node.host, "nics", **self._ssh_kwargs())
                reachable = bool(inits) or bool(host_nics)
                verdict = self.score_host_readiness(
                    proto, reachable=reachable, initiators=inits,
                    host_nics=host_nics, array_portals=array_portals,
                    baseline_subnets=baseline or None)
            new_hosts.append({"node": node.name, "host": fa,
                              "ready": verdict["ready"],
                              "reasons": verdict["reasons"]})

        await self.ctx.emit(
            f"Cluster assessment: {len(node_names)} node(s); "
            f"{len(new_hosts)} new, {len(departed)} departed")
        return OpResult.ok(
            f"{len(new_hosts)} new host(s), {len(departed)} departed",
            nodes=node_names, new_hosts=new_hosts, departed_hosts=departed)

    async def reconcile_cluster(self, apply_removals: bool = False,
                                **params: Any) -> OpResult:
        """Configure READY new nodes; flag (or remove) departed FA hosts.

        Idempotent + dry-run safe. Only readiness-passing new nodes are configured
        (plugin install + register_hosts + setup_connectivity, scoped to
        {already-configured nodes + ready new nodes}); not-ready new nodes are
        skipped and reported in ``not_ready``.
        """
        apply_removals = str(apply_removals).lower() in ("1", "true", "yes", "on")
        proto = self._protocol()
        host_group = (params.get("host_group")
                      or self.ctx.target.get("host_group", ""))

        assessment = await self.assess_cluster(**params)
        nodes = await self.list_nodes()
        by_name = {n.name: n for n in nodes}
        new_hosts = assessment.data.get("new_hosts", [])
        departed = assessment.data.get("departed_hosts", [])

        ready = [h for h in new_hosts if h.get("ready")]
        not_ready = [{"node": h["node"], "reasons": h["reasons"]}
                     for h in new_hosts if not h.get("ready")]

        # Scope = already-configured nodes + ready new nodes (so we never touch
        # not-ready new nodes, but re-running over configured nodes is idempotent).
        member_set: set[str] = set()
        if self.ctx.array is not None and host_group:
            member_set = set(await self.ctx.array.get_host_group_members(host_group))
        ready_names = {h["node"] for h in ready}
        scope_nodes = [n for n in nodes
                       if self._expected_fa_host(n) in member_set
                       or n.name in ready_names]

        configured: list[str] = []
        if ready and proto != "nfs":
            await self.ctx.emit(
                f"Reconcile: configuring {len(ready)} ready new node(s) "
                f"(scope={len(scope_nodes)} node(s))")
            # Install the plugin on the ready new nodes, then register + connect
            # over the full scope (idempotent on already-configured nodes).
            new_node_objs = [by_name[n] for n in ready_names if n in by_name]
            await self.deploy_integration(nodes=new_node_objs)
            await self.register_hosts(host_group=host_group, nodes=scope_nodes)
            await self.setup_connectivity(nodes=scope_nodes)
            configured = [h["node"] for h in ready]
        elif ready and proto == "nfs":
            await self.ctx.emit(
                "Reconcile: protocol=nfs has no per-host config; "
                "new nodes mount the shared NFS storage automatically")

        # Departed FA hosts: prune (or flag) via the shared helper.
        expected_hosts = {self._expected_fa_host(n) for n in nodes}
        prune = await self._prune_departed_fa_hosts(
            host_group, expected_hosts, apply_removals=apply_removals)

        data: dict[str, Any] = {
            "nodes": [n.name for n in nodes],
            "new_hosts": new_hosts,
            "configured": configured,
            "not_ready": not_ready,
            "departed_hosts": departed,
        }
        if apply_removals:
            data["removed"] = prune["removed"]
        else:
            data["pending_removals"] = prune["departed"]

        msg = (f"Reconcile: configured {len(configured)} node(s), "
               f"skipped {len(not_ready)} not-ready, "
               f"{'removed' if apply_removals else 'flagged'} "
               f"{len(prune['removed'] if apply_removals else prune['departed'])} "
               f"departed")
        await self.ctx.emit(msg)
        return OpResult.ok(msg, **data)

    # ------------------------------------------------------------------ #
    # NFS datastore (FlashArray File + PVE-native 'nfs' storage)
    # NOTE: NOT yet hardware-validated against FlashArray File.
    # ------------------------------------------------------------------ #
    async def provision_nfs_datastore(self, name: str, export_path: str = "/",
                                      storage_id: str = "",
                                      content: str = "images,iso",
                                      **_: Any) -> OpResult:
        """Create an FA file system + NFS export and define a PVE 'nfs' storage.

        Unlike the per-disk block plugin, this uses FlashArray File: one managed
        file system with an NFS export, mounted via PVE's native 'nfs' storage
        type (no host registration, multipath, or per-disk LUNs).
        """
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        export_path = export_path or "/"
        storage_id = storage_id or "purefa-nfs"
        await self.ctx.emit(
            f"Provisioning NFS datastore: FA file system {name!r}, export "
            f"path {export_path!r}, PVE storage {storage_id!r}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would provision NFS datastore {name}",
                               artifacts={"name": name, "storage_id": storage_id})

        # Array side: create the file system + NFS export.
        await self.ctx.array.create_filesystem(name)
        export = await self.ctx.array.create_nfs_export(name, name, export_path)
        # Discover an NFS data (VIP) interface to mount from.
        portals = await self.ctx.array.get_nfs_data_interfaces()
        if not portals:
            return OpResult.fail(
                "No NFS data interface found on the array (FlashArray File / NFS "
                "VIP not configured).")
        portal = portals[0]

        # Node side: define a PVE-native 'nfs' storage pointing at the export. The
        # NFS server presents the export at <portal>:<export path>; PVE mounts it.
        import shlex

        sid = shlex.quote(storage_id)
        await self._ssh(
            f"pvesm add nfs {sid} --server {shlex.quote(portal)} "
            f"--export {shlex.quote(export_path)} "
            f"--content {shlex.quote(content)} 2>/dev/null || "
            f"pvesm set {sid} --content {shlex.quote(content)}")
        await self._ssh("timeout 30 pvesm status", check=False)
        await self.ctx.emit(
            f"NFS datastore {storage_id} mounted from {portal}:{export_path}")
        return OpResult.ok(
            f"Provisioned NFS datastore {storage_id} ({portal}:{export_path})",
            artifacts={"name": name, "storage_id": storage_id,
                       "server": portal, "export": export_path,
                       "nfs_export": export.get("name"), "protocol": "nfs"})

    async def teardown_nfs_datastore(self, name: str, storage_id: str = "",
                                     eradicate: Any = False, **_: Any) -> OpResult:
        """Remove the PVE 'nfs' storage, NFS export, and FA file system."""
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        storage_id = storage_id or "purefa-nfs"
        eradicate = str(eradicate).lower() in ("1", "true", "yes", "on")
        await self.ctx.emit(
            f"Removing NFS datastore {storage_id!r} (file system {name!r})")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would remove NFS datastore {name}",
                               artifacts={"name": name, "storage_id": storage_id})

        import shlex

        await self._ssh(f"pvesm remove {shlex.quote(storage_id)}", check=False)
        # Delete the export(s) on this file system, then the file system itself.
        for exp in await self.ctx.array.get_nfs_exports(name):
            await self.ctx.array.delete_nfs_export(exp["name"])
        await self.ctx.array.delete_filesystem(name, eradicate=eradicate)
        await self.ctx.emit(f"NFS datastore {storage_id} removed")
        return OpResult.ok(f"Removed NFS datastore {storage_id}",
                           artifacts={"name": name, "storage_id": storage_id,
                                      "eradicated": eradicate})

    # ------------------------------------------------------------------ #
    # PROVISION_VOLUME: FA volume (one LUN per disk) -> connect -> attach to VM
    # ------------------------------------------------------------------ #
    async def provision(self, name: str, size: str = "1T", host_group: str = "",
                        vmid: str = "", disk: str = "scsi1", storage_id: str = "",
                        **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        proto = self._protocol()
        storage_id = self._storage_id(storage_id)
        host_group = host_group or self.ctx.target.get("host_group", "")
        await self.ctx.emit(f"Creating FlashArray volume {name} ({size}) [one LUN per disk]")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would provision {name}",
                               artifacts={"volume": name, "storage_id": storage_id})

        await self.ctx.array.create_volume(name, size)
        if host_group:
            await self.ctx.array.connect_volume(host_group, name)
            await self.ctx.emit(f"Connected {name} to host group {host_group}")

        # Node-side: rescan transport + assemble the multipath device.
        if proto == "nvme-tcp":
            await self._ssh("nvme connect-all", check=False)
        elif proto == "iscsi":
            await self._ssh("iscsiadm -m session --rescan", check=False)
        elif proto == "fc":
            await self._ssh(
                "for h in /sys/class/scsi_host/host*/scan; do echo '- - -' > $h; done",
                check=False)
        await self._ssh("multipath -r || true", check=False)

        artifacts: dict[str, Any] = {"volume": name, "protocol": proto,
                                     "storage_id": storage_id}
        # Attach the volume directly to the VM as a raw block disk (no LVM) using
        # the purefa storage. `pvesm alloc` registers the disk on the storage and
        # `qm set` attaches it; the plugin presents /dev/mapper/<wwid> to the guest.
        if vmid:
            await self._ssh_script([
                f"qm set {vmid} -{disk} {storage_id}:{name}",
            ], check=False)
            artifacts["vmid"] = vmid
            artifacts["disk"] = disk
            await self.ctx.emit(f"Attached {name} to VM {vmid} as {disk} (raw block)")
        await self.ctx.emit(f"Provisioned {name}")
        return OpResult.ok(f"Provisioned {name}", artifacts=artifacts)

    # ------------------------------------------------------------------ #
    # VM management (cross-hypervisor migration)
    #
    # Proxmox keeps each VM disk as its own purefa-storage volume, so migration
    # attaches the SAME FlashArray volume by its purefa volume name (`qm set
    # <vmid> --<slot> purefa:<volume>`) — no data movement. Disk identity flows
    # through the FlashArray serial (resolved by the orchestrator).
    # ------------------------------------------------------------------ #
    _DISK_BUSES = ("scsi", "virtio", "sata", "ide")

    @classmethod
    def _pve_bus(cls, bus: str) -> str:
        b = (bus or "scsi").lower()
        return b if b in cls._DISK_BUSES else "scsi"

    @classmethod
    def _pve_slot(cls, disk: "DiskSpec") -> str:
        """Proxmox device slot for a disk (e.g. ``scsi0``), derived from its bus +
        order so it is valid even when the source platform isn't Proxmox."""
        return f"{cls._pve_bus(disk.bus)}{disk.order}"

    async def plan_volume_adoption(self, disks, dest_vm_ref):
        """No volume renaming on Move-to-Proxmox.

        FlashArray refuses to MOVE a volume between volume groups via rename
        ("Cannot move volume through rename"), so a volume in a foreign vgroup
        (e.g. an XCP-ng ``phif-<srid>/…`` volume) can't be re-homed into
        ``vm-<vmid>/…``. Instead, :meth:`attach_existing_volumes` attaches such a
        volume by its raw multipath device, while a volume already in the
        destination VM's group (or flat) is attached purefa-managed. So no rename
        plan is produced.
        """
        return []

    async def prepare_copy_target(self, *, base_name: str, order: int,
                                  dest_vm_ref: str) -> str:
        # Clone into the destination VM's per-VM vgroup as a standard
        # vm-<vmid>-disk-<n> member, so `qm set purefa:vm-<vmid>-disk-<n>` resolves
        # it via the plugin's volume-group lookup (see _resolve_fa_volume). Pick a
        # name not taken by a LIVE or a soft-deleted (pending-eradication) volume —
        # a leftover destroyed disk-<n> from a previous VM would otherwise make the
        # clone fail with "Volume has been destroyed".
        vg = f"vm-{dest_vm_ref}"
        idx = order
        target = f"{vg}/vm-{dest_vm_ref}-disk-{idx}"
        if self.ctx.array is not None:
            while await self.ctx.array.volume_exists(target, include_destroyed=True):
                idx += 1
                target = f"{vg}/vm-{dest_vm_ref}-disk-{idx}"
            await self.ctx.array.ensure_volume_group(vg)
        return target

    async def _resolve_fa_volume(self, vmid: str, pve_volname: str) -> str:
        """Map a PVE ``purefa:`` volname to the real FlashArray volume name.

        The Everpure plugin stores each disk in a per-VM volume GROUP, so the array
        volume is ``vm-<vmid>/<volname>`` (default). Probe that first; fall back to
        the flat name when volume groups are disabled.
        """
        grouped = f"vm-{vmid}/{pve_volname}"
        try:
            if await self.ctx.array.get_volume(grouped):
                return grouped
        except Exception:  # noqa: BLE001 — fall back to the flat name
            pass
        return pve_volname

    async def _node_name(self) -> str:
        if self._is_mock_or_dry():
            return "pve-node1"
        out = await self._ssh("hostname -s", check=False)
        return (out or "").strip() or self._host()

    async def list_vms(self) -> list[dict[str, Any]]:
        if self._is_mock_or_dry():
            return [{"id": "100", "name": "mock-vm", "power_state": "running",
                     "vcpus": 2, "memory_bytes": 2 * 1024**3,
                     "disk_count": 1, "nic_count": 1}]
        # Use structured JSON, NOT `qm list` — that output is column-positional and
        # a VM NAME containing spaces (e.g. "VM 100") shifts the columns so the
        # status is misread (the VM looked "stopped" while actually running).
        raw = await self._ssh(
            "pvesh get /cluster/resources --type vm --output-format json", check=False)
        vms: list[dict[str, Any]] = []
        try:
            for r in json.loads(raw or "[]"):
                if r.get("type") != "qemu" or r.get("vmid") is None:
                    continue  # skip LXC / non-VM resources
                status = (r.get("status") or "").lower()
                vms.append({
                    "id": str(r["vmid"]), "name": r.get("name", ""),
                    "power_state": "running" if status == "running" else "stopped",
                    "vcpus": r.get("maxcpu"), "memory_bytes": r.get("maxmem")})
        except (ValueError, TypeError):
            pass
        return vms

    async def list_networks(self) -> list[dict[str, Any]]:
        if self._is_mock_or_dry():
            return [{"id": "vmbr0", "name": "vmbr0", "kind": "bridge"}]
        node = await self._node_name()
        raw = await self._ssh(
            f"pvesh get /nodes/{node}/network --type any_bridge --output-format json",
            check=False)
        nets: list[dict[str, Any]] = []
        try:
            for entry in json.loads(raw or "[]"):
                iface = entry.get("iface")
                if iface:
                    nets.append({"id": iface, "name": iface,
                                 "kind": entry.get("type", "bridge")})
        except (ValueError, TypeError):
            pass
        return nets

    async def power_state(self, vm_ref: str) -> str:
        if self._is_mock_or_dry():
            return "running"
        out = await self._ssh(f"qm status {vm_ref}", check=False)
        # "status: running"
        token = (out or "").strip().split(":")[-1].strip().lower()
        if token in ("running", "stopped"):
            return token
        return "unknown"

    async def capture_vm_spec(self, vm_ref: str) -> "VmSpec":
        from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec

        storage_id = self._storage_id()
        if self._is_mock_or_dry():
            return VmSpec(
                name="mock-vm", source_ref=str(vm_ref), vcpus=2,
                memory_bytes=2 * 1024**3, firmware="bios",
                disks=[DiskSpec(DiskIdentity(fa_volume=f"vm-{vm_ref}-disk-0"),
                                bus="scsi", order=0, boot=True,
                                source_ref="scsi0")],
                nics=[NicSpec(mac="AA:BB:CC:00:00:01", source_network="vmbr0",
                              model="virtio", order=0)])

        raw = await self._ssh(f"qm config {vm_ref}", check=False)
        cfg: dict[str, str] = {}
        for line in (raw or "").splitlines():
            if ":" in line:
                k, _, v = line.partition(":")
                cfg[k.strip()] = v.strip()

        cores = int(cfg.get("cores", "1") or 1)
        sockets = int(cfg.get("sockets", "1") or 1)
        memory_mib = int(cfg.get("memory", "512") or 512)
        firmware = "uefi" if cfg.get("bios", "seabios").lower() == "ovmf" else "bios"

        # Boot order: "boot: order=scsi0;ide2;net0" or legacy "bootdisk: scsi0".
        # Stash the EXACT original boot setting so a rollback re-attach can restore
        # it — `qm set --delete scsiN` (detach) drops the disk from the boot order,
        # so re-adding the disk alone would leave the VM unbootable.
        boot_slots: list[str] = []
        boot_line = cfg.get("boot", "")
        self._pve_boot_order = boot_line  # e.g. "order=scsi0;ide2;net0"
        if "order=" in boot_line:
            boot_slots = boot_line.split("order=", 1)[1].split(",")[0].split(";")
        elif cfg.get("bootdisk"):
            boot_slots = [cfg["bootdisk"]]

        disks: list[DiskSpec] = []
        nics: list[NicSpec] = []
        for key, val in cfg.items():
            bus = next((b for b in self._DISK_BUSES if key.startswith(b)
                        and key[len(b):].isdigit()), None)
            if bus and f"{storage_id}:" in val:
                # "purefa:vm-100-disk-0,size=100G,..." — the PVE volname is flat, but
                # the array volume lives in a per-VM volume group (<vm-vmid>/<vol>).
                volref = val.split(",", 1)[0]
                volname = volref.split(":", 1)[1] if ":" in volref else volref
                fa_volume = await self._resolve_fa_volume(str(vm_ref), volname)
                order = int(key[len(bus):])
                disks.append(DiskSpec(
                    identity=DiskIdentity(fa_volume=fa_volume),
                    bus=bus, order=order,
                    boot=(key in boot_slots) or (not boot_slots and not disks),
                    source_ref=key))
            elif key.startswith("net") and key[3:].isdigit():
                # "virtio=AA:BB:..,bridge=vmbr0,..."
                model = "virtio"
                mac = ""
                bridge = ""
                for part in val.split(","):
                    if "=" in part:
                        pk, pv = part.split("=", 1)
                        if pk in ("virtio", "e1000", "vmxnet3", "rtl8139"):
                            model, mac = pk, pv
                        elif pk == "bridge":
                            bridge = pv
                nics.append(NicSpec(mac=mac, source_network=bridge, model=model,
                                    order=int(key[3:])))

        disks.sort(key=lambda d: d.order)
        nics.sort(key=lambda n: n.order)
        if disks and not any(d.boot for d in disks):
            disks[0].boot = True
        return VmSpec(
            name=cfg.get("name", f"vm-{vm_ref}"), source_ref=str(vm_ref),
            vcpus=cores * sockets, memory_bytes=memory_mib * 1024 * 1024,
            firmware=firmware, disks=disks, nics=nics,
            raw={"scsihw": cfg.get("scsihw", "virtio-scsi-single")})

    async def stop_vm(self, vm_ref: str, *, force: bool = False) -> OpResult:
        if await self.power_state(vm_ref) == "stopped":
            return OpResult.ok(f"VM {vm_ref} already stopped")
        cmd = f"qm stop {vm_ref}" if force else f"qm shutdown {vm_ref}"
        await self._ssh(cmd, check=False)
        return OpResult.ok(f"Stopped VM {vm_ref}")

    async def start_vm(self, vm_ref: str) -> OpResult:
        if await self.power_state(vm_ref) == "running":
            return OpResult.ok(f"VM {vm_ref} already running")
        await self._ssh(f"qm start {vm_ref}", check=False)
        return OpResult.ok(f"Started VM {vm_ref}")

    async def detach_volumes(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        for disk in disks:
            slot = disk.source_ref or self._pve_slot(disk)
            # --delete removes the disk from the VM config WITHOUT destroying the
            # backing purefa volume (that lives on the array, untouched).
            await self._ssh(f"qm set {vm_ref} --delete {slot}", check=False)
            await self.ctx.emit(f"Detached {slot} from VM {vm_ref}")
        return OpResult.ok(f"Detached {len(disks)} disk(s) from VM {vm_ref}")

    @staticmethod
    def _pve_vm_name(name: str) -> str:
        """Sanitize a VM name to a Proxmox-valid name (DNS-like: letters, digits,
        hyphens, dots). Proxmox rejects spaces/most punctuation — e.g. an XCP-ng
        VM named "AlmaLinux 8" must become "AlmaLinux-8" or `qm create` errors."""
        import re

        s = re.sub(r"[^A-Za-z0-9.-]", "-", (name or "").strip())
        s = re.sub(r"-{2,}", "-", s).strip("-.")
        return s or "migrated-vm"

    async def create_vm(self, spec: "VmSpec", *,
                        network_map: dict[str, str],
                        placement: dict[str, Any] | None = None) -> OpResult:
        # Spread VMs across the cluster: pin this migration to a round-robin node
        # so the new VM (and all its subsequent qm commands) land there.
        rr_host = await self._pick_rr_node()
        if rr_host:
            self._host_override = rr_host
            await self.ctx.emit(f"Placing VM on cluster node {rr_host} (round-robin spread)")
        storage_id = self._storage_id()
        base_name = self._pve_vm_name(spec.name)
        existing_names = {vm.get("name") for vm in await self.list_vms()}
        vm_name = base_name
        if vm_name in existing_names:
            i = 2
            while f"{base_name}-{i}" in existing_names:
                i += 1
            vm_name = f"{base_name}-{i}"
            await self.ctx.emit(
                f"Name {base_name!r} already exists; using {vm_name!r} instead")

        mem_mib = max(16, spec.memory_bytes // (1024 * 1024))
        scsihw = spec.raw.get("scsihw", "virtio-scsi-single")
        create_args = [f"--name {vm_name}",
                       f"--cores {max(1, spec.vcpus)}", "--sockets 1",
                       f"--memory {mem_mib}", f"--scsihw {scsihw}"]
        if spec.firmware == "uefi":
            create_args.append("--bios ovmf")
            # Fresh EFI vars disk (NVRAM is NOT migrated; guest is pre-prepared).
            create_args.append(f"--efidisk0 {storage_id}:1,efitype=4m")
        # Map the source's logical NIC model to a Proxmox-accepted one; the guest
        # is pre-prepared with virtio drivers, so unknown models fall back to virtio.
        pve_models = {"virtio", "e1000", "e1000e", "vmxnet3", "rtl8139"}
        for nic in spec.nics:
            dest_net = network_map.get(nic.source_network)
            if not dest_net:
                return OpResult.fail(
                    f"no destination network mapped for source NIC {nic.source_network!r}")
            model = (nic.model or "").lower()
            if model not in pve_models:
                model = "virtio"
            create_args.append(f"--net{nic.order} {model}={nic.mac},bridge={dest_net}")

        cluster_key = (self.ctx.target.get("node_host")
                       or self.ctx.target.get("host") or "_")
        # Allocate the vmid and create the VM ATOMICALLY per cluster: nextid only
        # advances once a config exists, so without this lock concurrent migrations
        # grab the same id and clobber each other's VM. `qm create` provisions
        # efidisk0 via the purefa plugin, which can transiently fail — retry (a
        # failed create leaves no config, so the vmid is reusable; purge first).
        async with _keyed_lock(_CLUSTER_CREATE_LOCKS, cluster_key):
            if self._is_mock_or_dry():
                vmid = "900"
            else:
                vmid = (await self._ssh("pvesh get /cluster/nextid",
                                        check=False)).strip() or "900"
            create_cmd = " ".join([f"qm create {vmid}", *create_args])
            last_exc: Exception | None = None
            for attempt in range(3):
                try:
                    await self._ssh_script([create_cmd], check=True, timeout=180)
                    last_exc = None
                    break
                except Exception as exc:
                    last_exc = exc
                    if attempt < 2:
                        await self.ctx.emit(
                            f"qm create {vmid} attempt {attempt + 1} failed ({exc}); retrying")
                        await self._ssh(
                            f"qm destroy {vmid} --purge 1 >/dev/null 2>&1 || true",
                            check=False, timeout=60)
                        await asyncio.sleep(3)
            if last_exc is not None:
                return OpResult.fail(f"qm create {vmid} failed after retries: {last_exc}")
        # Confirm the config really exists before declaring success — guards against
        # a silent clobber and makes a missing VM a hard failure, not a false OK.
        if not self._is_mock_or_dry():
            cfg = await self._ssh(f"qm config {vmid}", check=False)
            if "does not exist" in (cfg or "") or _LOCK_TIMEOUT_RE.search(cfg or ""):
                return OpResult.fail(
                    f"VM {vmid} not present after create: {(cfg or '').strip()[:200]}")
        await self.ctx.emit(f"Created VM {vmid} ({vm_name})")
        return OpResult.ok(f"Created VM {vmid}", artifacts={"vm_ref": vmid})

    async def attach_existing_volumes(self, vm_ref: str,
                                      disks: "list[DiskSpec]") -> OpResult:
        from phif.connectors.multipath import refresh_block_devices
        from phif.migrate.spec import scsi_wwid

        proto = self._protocol()
        storage_id = self._storage_id()
        # Flush any STALE multipath map, rescan, and wait for the device (all
        # time-bounded). Activating a dead-path map can wedge the node, and an
        # unbounded `qm`/rescan would hang the migration for the 10-min SSH timeout.
        await refresh_block_devices(
            (lambda cmd, t: self._ssh(cmd, check=False, timeout=t)),
            protocol=proto, wwids=await self._disk_wwids(disks))

        own_vg = f"vm-{vm_ref}"
        for disk in disks:
            slot = self._pve_slot(disk)
            fa = disk.identity.fa_volume
            vg = fa.split("/", 1)[0] if "/" in fa else ""
            if not vg or vg == own_vg:
                # purefa-managed: the plugin resolves a flat name or this VM's own
                # vgroup member (`qm set purefa:<basename>`).
                pve_vol = fa.split("/")[-1]
                await self._ssh_script(
                    [f"qm set {vm_ref} --{slot} {storage_id}:{pve_vol}"],
                    check=False, timeout=120)
                await self.ctx.emit(f"Attached {pve_vol} to VM {vm_ref} as {slot} (purefa)")
            else:
                # Volume lives in a FOREIGN vgroup (e.g. an XCP-ng `phif-…` volume).
                # FlashArray can't move it into this VM's group via rename, so attach
                # the raw multipath device directly — boots all the same (the disk is
                # an unmanaged passthrough rather than a purefa-managed volume).
                serial = ""
                if self.ctx.array is not None:
                    serial = ((await self.ctx.array.get_volume(fa)) or {}).get("serial") or ""
                if not serial:
                    return OpResult.fail(
                        f"could not resolve a device for {fa!r} (no array serial)")
                dev = f"/dev/mapper/{scsi_wwid(serial)}"
                await self._ssh_script(
                    [f"qm set {vm_ref} --{slot} {dev}"], check=False, timeout=120)
                await self.ctx.emit(
                    f"Attached {fa} to VM {vm_ref} as {slot} (raw device {dev})")
        # Restore the boot order (detach via `qm set --delete` dropped the disk
        # from it). Prefer the EXACT original captured on this connector (rollback
        # to the source); otherwise put the boot disk first (fresh destination VM).
        boot = getattr(self, "_pve_boot_order", "")
        if not boot:
            ordered = sorted(disks, key=lambda d: (not d.boot, d.order))
            slots = [self._pve_slot(d) for d in ordered]
            boot = f"order={';'.join(slots)}" if slots else ""
        if boot:
            # Quote: the boot value contains ';' (e.g. order=scsi0;ide2;net0) which
            # the shell would otherwise treat as command separators.
            await self._ssh(f"qm set {vm_ref} --boot '{boot}'", check=False, timeout=60)
            await self.ctx.emit(f"Restored boot order on VM {vm_ref}: {boot}")
        return OpResult.ok(f"Attached {len(disks)} disk(s) to VM {vm_ref}")

    async def create_managed_disk(self, vm_ref: str, *, size_bytes: int,
                                  order: int, boot: bool) -> str:
        """Allocate a NEW purefa-managed disk on the VM (`qm set --scsiN purefa:<GiB>`
        — the plugin creates vm-<vmid>/vm-<vmid>-disk-N) and return its FA volume
        name. Migration then copy-overwrites the source onto it."""
        storage_id = self._storage_id()
        slot = f"{self._pve_bus('scsi')}{order}"
        if self._is_mock_or_dry():
            return f"vm-{vm_ref}/vm-{vm_ref}-disk-{order}"
        gib = max(1, -(-int(size_bytes or 0) // (1024 ** 3)))  # ceil to GiB
        out = await self._ssh_script(
            [f"qm set {vm_ref} --{slot} {storage_id}:{gib}"], check=False, timeout=120)
        # Read back the volname PVE allocated for that slot.
        cfg = await self._ssh(f"qm config {vm_ref}", check=False)
        volname = ""
        for line in (cfg or "").splitlines():
            if line.startswith(f"{slot}:") and f"{storage_id}:" in line:
                ref = line.split(":", 1)[1].strip().split(",", 1)[0]
                volname = ref.split(":", 1)[1] if ":" in ref else ref
                break
        if not volname:
            # The disk was NOT created. Surface the actual purefa plugin error
            # (e.g. "purefa: login failed: 500 'storage-purefa'-locked command
            # timed out - aborting") instead of guessing a volume name and letting
            # a misleading downstream FlashArray error be reported.
            blob = f"{out or ''}\n{cfg or ''}"
            detail = next((ln.strip() for ln in blob.splitlines()
                           if "purefa" in ln.lower()
                           and self._PUREFA_ERR_RE.search(ln)), "")
            if not detail:
                detail = (out or "").strip().splitlines()[-1:] or ["no disk created"]
                detail = detail[0][:200]
            raise RuntimeError(
                f"failed to create managed disk on VM {vm_ref} ({slot}): {detail}")
        fa = await self._resolve_fa_volume(str(vm_ref), volname)
        await self.ctx.emit(
            f"Created managed disk {fa} ({gib}G) on VM {vm_ref} as {slot}")
        return fa

    async def set_boot_order(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        ordered = sorted(disks, key=lambda d: (not d.boot, d.order))
        slots = [self._pve_slot(d) for d in ordered]
        if not slots:
            return OpResult.ok("no disks; boot order unchanged")
        # Quote: ';' between slots would otherwise be shell command separators.
        out = await self._ssh(f"qm set {vm_ref} --boot 'order={';'.join(slots)}'", check=False)
        # A missing config here means the dest VM vanished (e.g. clobbered) — fail
        # loudly rather than report a phantom success.
        if "does not exist" in (out or ""):
            return OpResult.fail(f"VM {vm_ref} config missing when setting boot order: "
                                 f"{(out or '').strip()[:200]}")
        return OpResult.ok(f"Set boot order: {';'.join(slots)}")

    async def delete_vm(self, vm_ref: str, *, keep_disks: bool = True) -> OpResult:
        if keep_disks:
            # A detached purefa disk lingers as an `unusedN:` config entry, and
            # `qm destroy` would try to FREE it (the array then refuses because the
            # volume is now connected to the destination — a scary-but-benign
            # warning, and a real hazard if it were NOT yet connected). Strip those
            # references from the VM config FIRST so destroy leaves the volume
            # completely alone. (qm set --delete / qm unlink only move the disk back
            # to `unused`, they don't drop the reference, so we edit the config.)
            storage_id = self._storage_id()
            await self._ssh_script([
                f"CONF=/etc/pve/qemu-server/{vm_ref}.conf",
                f"if [ -f \"$CONF\" ]; then "
                f"sed -i -E '/^unused[0-9]+: *{storage_id}:/d' \"$CONF\"; fi",
            ], check=False)
            await self._ssh(
                f"qm destroy {vm_ref} --destroy-unreferenced-disks 0 --purge 0",
                check=False)
        else:
            await self._ssh(f"qm destroy {vm_ref}", check=False)
        return OpResult.ok(f"Deleted VM {vm_ref}")

    # ------------------------------------------------------------------ #
    # SNAPSHOT / CLONE  (on the array)
    # ------------------------------------------------------------------ #
    async def snapshot(self, volume: str, suffix: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Creating FlashArray snapshot of volume {volume}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would snapshot {volume}")
        await self.ctx.array.create_snapshot(volume, suffix or None)
        return OpResult.ok(f"Snapshot of {volume} created", artifacts={"volume": volume})

    async def clone(self, source: str, dest: str, host_group: str = "",
                    **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        host_group = host_group or self.ctx.target.get("host_group", "")
        await self.ctx.emit(f"FlashArray volume copy {source} -> {dest}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would clone {source} -> {dest}")
        await self.ctx.array.clone_volume(source, dest)
        if host_group:
            await self.ctx.array.connect_volume(host_group, dest)
            await self.ctx.emit(f"Connected clone {dest} to host group {host_group}")
        return OpResult.ok(f"Cloned {source} -> {dest}", artifacts={"volume": dest})

    # ------------------------------------------------------------------ #
    # RESIZE: FA extend + node rescan + qm resize
    # ------------------------------------------------------------------ #
    async def resize(self, volume: str, size: str, vmid: str = "", disk: str = "",
                     **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        proto = self._protocol()
        await self.ctx.emit(f"Extending FlashArray volume {volume} to {size}")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would resize {volume} to {size}")
        await self.ctx.array.extend_volume(volume, size)
        # Rescan so the node sees the new capacity.
        if proto == "nvme-tcp":
            await self._ssh("nvme connect-all", check=False)
        elif proto == "iscsi":
            await self._ssh("iscsiadm -m session --rescan", check=False)
        elif proto == "fc":
            # FC: no login; rescan the SCSI bus so the new capacity is seen.
            await self._ssh(
                "for h in /sys/class/scsi_host/host*/scan; do echo '- - -' > $h; done",
                check=False)
        await self._ssh("multipath -r || true", check=False)
        # Grow the disk inside Proxmox so the guest sees it (raw block device).
        if vmid and disk:
            await self._ssh(f"qm resize {vmid} {disk} {size}", check=False)
        await self.ctx.emit(f"Resized {volume} to {size}")
        return OpResult.ok(f"Resized {volume} to {size}", artifacts={"volume": volume})

    # ------------------------------------------------------------------ #
    # HEALTH: pvesm status + multipath/nvme paths
    # ------------------------------------------------------------------ #
    async def health_check(self, **_: Any) -> OpResult:
        proto = self._protocol()
        path_cmd = "nvme list-subsys" if proto == "nvme-tcp" else "multipath -ll"
        # Path/multipath state is per-node, so collect it from EVERY cluster node
        # rather than just the connection host (pvesm status is cluster-wide but is
        # gathered per node too so an unreachable member shows up here).
        nodes = await self.list_nodes()
        await self.ctx.emit(
            f"Collecting Proxmox + path health from {len(nodes)} node(s) ...")
        per_node: dict[str, Any] = {}
        for node in nodes:
            await self.ctx.emit(f"-> health on {node.name} ({node.host})")
            per_node[node.name] = {
                "host": node.host,
                "pvesm_status": await self._ssh_on(node.host, "timeout 30 pvesm status",
                                                   check=False),
                "paths": await self._ssh_on(node.host, path_cmd, check=False),
            }
        array_info = await self.ctx.array.info() if self.ctx.array else {}
        return OpResult.ok(
            "Health collected",
            protocol=proto,
            nodes=per_node,
            array=array_info,
        )

    # ------------------------------------------------------------------ #
    # REMOVE: remove storage.cfg entry + plugin file
    # ------------------------------------------------------------------ #
    async def teardown(self, storage_id: str = "", **_: Any) -> OpResult:
        storage_id = self._storage_id(storage_id)
        await self.ctx.emit(f"Removing purefa storage {storage_id!r} and plugin ...")
        if self.ctx.dry_run:
            return OpResult.ok("[dry-run] teardown planned", status="planned")

        # The storage *definition* lives in pmxcfs (/etc/pve/storage.cfg) and is
        # cluster-wide, so remove it ONCE on the connection host. The plugin file
        # and daemon reload are per-node (each member runs the Perl plugin from its
        # own filesystem), so fan those out across every cluster node — mirroring
        # how deploy_integration installs them on every node.
        await self._ssh(f"pvesm remove {storage_id}", check=False)

        nodes = await self.list_nodes()
        for node in nodes:
            await self.ctx.emit(f"-> removing plugin on {node.name} ({node.host})")
            await self._ssh_on(node.host, f"rm -f {PLUGIN_DEST}", check=False)
            await self._ssh_on(node.host,
                               "systemctl reload pvedaemon pveproxy pvestatd",
                               check=False)
        await self.ctx.emit(
            f"purefa storage and plugin removed from {len(nodes)} node(s)")
        return OpResult.ok("Integration removed",
                           artifacts={"nodes": [n.to_dict() for n in nodes]},
                           status="not_deployed")
