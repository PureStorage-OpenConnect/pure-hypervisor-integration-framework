"""HPE VM Essentials (VME) connector implementation.

HPE VM Essentials (VME) is a new KVM/libvirt-based hypervisor platform managed by
the **VME Manager**, which is built on the Morpheus platform and exposes a
Morpheus-lineage REST API.

DESIGN GOAL (native Morpheus plugin model)
-------------------------------------------
HPE VME is rebranded Morpheus and exposes the ``morpheus-plugin-core`` SDK, so the
*proper* per-VM-disk integration is a **native storage provider plugin** (built
under ``files/morpheus-plugin/``): one FlashArray volume per VM disk, attached to
the KVM VM as a raw multipathed block device, with array-offloaded
snapshot/clone/resize. VME orchestrates the host presentation + the libvirt disk
definition itself (via the plugin's ``MvmProvisionFacet``) -- no SSH/virsh back
door behind VME's control plane.

This connector's job is therefore to **deploy + configure** that plugin and the
array-side prerequisites, then let VME drive per-disk provisioning:

  1. DEPLOY  -- upload the Everpure plugin JAR to the VME Manager (Plugins API), so a
     "Everpure FlashArray" StorageServerType becomes available.
  2. CONFIGURE -- register the FlashArray as a VME **storage server** of that type
     (endpoint / token / host group / protocol).
  3. HOST_REGISTER -- ``create_host`` / ``create_host_group``: register the KVM
     host initiators on the array as a host group (the plugin connects per-disk
     volumes to it).
  4. CONNECTIVITY -- iSCSI/FC/NVMe transport + Everpure multipath on the KVM hosts.

After that, disks are provisioned **in VME** on the Everpure datastore (the plugin
calls ``createVolume`` -> ``prepareHostForVolume`` -> ``buildDiskConfig``). The
connector also keeps **direct array** day-2 ops (PROVISION_VOLUME create+connect,
SNAPSHOT, CLONE, RESIZE) for array-side management outside VME's flow.

SUPPORT STATUS (researched 2026-06, see docs/connectors/hpevme.md)
-----------------------------------------------------------------
Researched against Everpure's HPE VME solution bundle, HPE VME (``hpevm-docs``)
docs, and the Morpheus OpenAPI spec (VME Manager *is* rebranded Morpheus; the
hypervisor is "HVM" in the API):

  * **VME has NO native Everpure storage-provider plugin.** The documented Morpheus
    ``storage-server`` types are 3Par / Dell ECS / Dell Isilon / HPE Alletra --
    no Everpure FlashArray. So there is no array-aware control-plane integration to
    "deploy"; Everpure is consumed at the **array + host + libvirt** level.
  * **Everpure's own officially-supported VME path is a shared datastore** (a
    FlashArray volume presented to all hosts and formatted as a **GFS2 Pool**
    over iSCSI, or an **NFS** export holding QCOW2 images) -- NOT one volume per
    VM disk. The :meth:`register_datastore_fallback` helper covers that model.
  * The **native plugin** (``files/morpheus-plugin/``) provides the per-VM-disk
    raw-block model the supported way: VME's plugin SDK exposes ``StorageProvider``
    / ``StorageServerType`` (register the array), ``DatastoreTypeProvider`` (volume
    CRUD + ``SnapshotFacet``), and ``MvmProvisionFacet`` (``prepareHostForVolume`` /
    ``buildDiskConfig`` -> libvirt disk / ``releaseVolumeFromHost``) so VME itself
    attaches ``/dev/mapper/<wwid>`` -- no SSH/virsh behind the control plane.

Confirmed Morpheus/VME API facts now baked in (no longer guessed): OAuth is
``POST /oauth/token`` **form-encoded**; hosts are ``GET /api/servers`` (mgmt IP
in ``sshHost``); liveness is ``GET /api/ping``, identity ``GET /api/whoami``;
plugins upload via ``POST /api/plugins`` (multipart); the FlashArray registers as
a storage server via ``POST /api/storage-servers`` with the plugin's type code.
``maturity = "ga"``: the plugin is compiled against the real VME SDK and
validated end-to-end on a live VME appliance (provision, image-based deploy,
clone-from-running-VM, and snapshot create/revert/delete against the array).

SDKs are imported lazily; the VME Manager is driven over REST via
``ctx.runner.run_http`` (mock-safe); array-side via ``ctx.array``; host-side
(libvirt / multipath) via ``ctx.runner.run_ssh``. ``ctx.dry_run`` is respected.
"""

from __future__ import annotations

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
    _nic_network,
    compare_node_interfaces,
    nics_on_array_subnets,
    nics_on_common_subnets,
)
from phif.connectors.iscsi_net import arp_flux_cmd

# Morpheus-lineage API paths (CONFIRMED against the Morpheus OpenAPI spec /
# apidocs.morpheusdata.com -- VME Manager is rebranded Morpheus 8.x).
#   * /oauth/token   -- OAuth token endpoint (no /api prefix), form-encoded body.
#   * /api/servers   -- compute hosts ("Hosts" in the UI); mgmt IP in sshHost.
#   * /api/ping      -- unauthenticated liveness; /api/whoami -- identity+perms.
#   * /api/data-stores (hyphenated) -- datastores; /api/instances -- VMs.
_API_BASE = "/api"
_OAUTH_PATH = "/oauth/token"

# The native Everpure storage plugin (built under files/morpheus-plugin/). Its
# Plugin getCode() and the StorageServerType code the plugin registers.
_PLUGIN_CODE = "pure-flasharray-vme"
_STORAGE_SERVER_TYPE = "pure-flasharray-vme.storage"
# Built shadow JAR, relative to files/morpheus-plugin/ (./gradlew shadowJar).
_PLUGIN_JAR_DEFAULT = "build/libs/pure-flasharray-vme-plugin-0.1.0-all.jar"

# Everpure FlashArray multipath config for SCSI (iSCSI/FC) on the VME KVM host,
# written as a DROP-IN that multipathd auto-includes from
# /etc/multipath/conf.d/*.conf — so we never overwrite the operator's
# /etc/multipath.conf. (NVMe-oF uses native NVMe multipath.) Mirrors Proxmox.
_MULTIPATH_DROPIN = "/etc/multipath/conf.d/pure.conf"
_MULTIPATH_CONF = """\
# Managed by PHIF — Everpure FlashArray multipath settings.
defaults {
    polling_interval 10
    find_multipaths no
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


class HpeVmeConnector(HypervisorConnector):
    # --- static metadata ---
    key = "hpevme"
    name = "HPE VM Essentials"
    description = (
        "HPE VM Essentials (VME) -- KVM/libvirt hypervisor managed by the VME "
        "Manager (Morpheus-lineage REST API). Primary model: one FlashArray "
        "volume per VM disk, presented directly to the VM as a raw multipathed "
        "block device, with array-based snapshots/clones/resize (Everpure "
        "CSI/Cinder/Proxmox-style). Per-disk direct raw-block attach is "
        "implemented and validated end-to-end (provision, image deploy, "
        "clone-from-VM, snapshot create/revert/delete) via the compiled "
        "morpheus-plugin, with a datastore fallback."
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
    # plugin: a FlashArray File NFS export holding QCOW2 VM images, mounted on the
    # KVM hosts and registered as a VME datastore. NOTE: the NFS paths are NOT yet
    # hardware-validated against FlashArray File.
    SUPPORTED_PROTOCOLS = {
        Protocol.ISCSI,
        Protocol.FC,
        Protocol.NVME_TCP,
        Protocol.NVME_FC,   # NVMe-oF over Fibre Channel
        Protocol.NFS,       # FlashArray File NFS datastore (VM images / ISO)
    }

    # ------------------------------------------------------------------ UI ---
    @classmethod
    def target_schema(cls) -> list[FormField]:
        return [
            FormField("vme_manager_url", "VME Manager URL", FieldType.STRING,
                      placeholder="https://vme-mgr.example.local",
                      help="Base URL of the HPE VME Manager (Morpheus) appliance."),
            FormField("username", "Username", FieldType.STRING, default="admin"),
            FormField("password", "Password", FieldType.SECRET),
            FormField("protocol", "Storage protocol", FieldType.ENUM, default="iscsi",
                      options=["iscsi", "fc", "nvme-tcp", "nvme-fc", "nfs"],
                      help="Transport used to present FlashArray volumes to VME KVM "
                           "hosts. iSCSI and FC are SCSI (dm-multipath); nvme-tcp / "
                           "nvme-fc use native NVMe multipath; nfs presents a "
                           "FlashArray File NFS datastore for QCOW2 VM images "
                           "(no block host registration / multipath)."),
            FormField("host_group", "Default host group", FieldType.STRING,
                      required=False,
                      help="FlashArray host group for the VME KVM nodes (optional)."),
            FormField("host_wwns", "KVM host WWNs (comma-separated)", FieldType.STRING,
                      required=False,
                      help="Fibre Channel HBA port WWNs of the VME KVM host(s). Only "
                           "used when protocol=fc. Auto-discovered from the KVM host "
                           "if left blank."),
            FormField("kvm_ssh_username", "KVM host SSH username", FieldType.STRING,
                      required=False,
                      help="SSH user for the VME KVM hosts (used for NIC/initiator "
                           "discovery + host-side setup). The VME Manager login is "
                           "NOT the KVM-host SSH login. Defaults to the Manager "
                           "username if blank. IMPORTANT: if this user is not root, "
                           "it must have passwordless sudo (NOPASSWD) configured on "
                           "the KVM hosts — host storage commands (iscsiadm, "
                           "multipath, nvme) require root."),
            FormField("kvm_ssh_password", "KVM host SSH password", FieldType.SECRET,
                      required=False,
                      help="SSH password for the VME KVM hosts. Defaults to the VME "
                           "Manager password if blank."),
            FormField("kvm_ssh_key", "KVM host SSH private key", FieldType.TEXT,
                      required=False,
                      help="SSH private key for the VME KVM hosts (alternative to a "
                           "password)."),
            FormField("plugin_jar", "Everpure plugin JAR path", FieldType.STRING,
                      required=False,
                      help="Override path to the Everpure storage plugin JAR. Normally "
                           "built into the backend image automatically and uploaded "
                           "to the VME Manager by the deploy action; set this only to "
                           "use a custom-built JAR."),
        ]

    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        return [
            ActionSpec(
                Capability.HOST_REGISTER, "register_hosts",
                "Register VME hosts on array",
                "Create a FlashArray host group from the VME KVM host initiators "
                "so per-disk volumes can be presented to the nodes.",
                fields=[
                    FormField("host_group", "Host group name", FieldType.STRING),
                    FormField("iqns", "Host IQNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="For iscsi. Auto-discovered from the KVM host if "
                                   "left blank."),
                    FormField("wwns", "Host WWNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="For fc. Auto-discovered from the KVM host if "
                                   "left blank."),
                    FormField("nqns", "Host NQNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="For nvme-tcp. Auto-discovered from the KVM host "
                                   "if left blank."),
                ],
            ),
            ActionSpec(
                Capability.CONNECTIVITY, "setup_connectivity",
                "Set up connectivity",
                "Confirm/prepare iSCSI/FC/NVMe-TCP transport + multipath between "
                "VME KVM hosts and the FlashArray.",
                fields=[
                    FormField("host_group", "Host group name", FieldType.STRING,
                              required=False),
                    # INTERFACE BINDING (per active protocol). Since per-disk
                    # volumes are attached directly to VMs, the binding governs
                    # which host interfaces carry the storage sessions. Choices are
                    # discovered from the KVM host via discover_options(kind).
                    FormField("iscsi_nics", "iSCSI NICs", FieldType.MULTISELECT,
                              required=False, options_source="nics",
                              help="NICs to bind iSCSI ifaces to (protocol=iscsi). "
                                   "Discovered from the KVM host."),
                    FormField("nvme_sources", "NVMe-TCP source interfaces",
                              FieldType.MULTISELECT, required=False,
                              options_source="nvme_sources",
                              help="Host source interfaces/addresses for NVMe-TCP "
                                   "(host-traddr; protocol=nvme-tcp)."),
                    FormField("nvme_options", "NVMe connect options", FieldType.STRING,
                              required=False,
                              help="Extra options appended to `nvme connect` "
                                   "(e.g. '-l 600'). Used when protocol=nvme-tcp."),
                    FormField("fc_hbas", "FC HBAs", FieldType.MULTISELECT,
                              required=False, options_source="fc_hbas",
                              help="Fibre Channel HBA port WWPNs to use for the "
                                   "sessions (protocol=fc)."),
                ],
            ),
            ActionSpec(
                Capability.DEPLOY_PLUGIN, "deploy",
                "Upload Everpure plugin to VME",
                "Upload the native Everpure FlashArray storage plugin JAR to the VME "
                "Manager (Administration > Integrations > Plugins). Makes the "
                "'Everpure FlashArray' storage-server type available.",
                fields=[
                    FormField("plugin_jar", "Plugin JAR path", FieldType.STRING,
                              required=False,
                              help="Override the built plugin JAR path "
                                   "(files/morpheus-plugin/build/libs/...-all.jar)."),
                    FormField("force", "Force re-upload (upgrade)",
                              FieldType.BOOL, default=False, required=False,
                              help="Re-upload the JAR even if the plugin is already "
                                   "installed, replacing the running plugin with this "
                                   "build. Use after rebuilding the plugin."),
                ],
            ),
            ActionSpec(
                Capability.CONFIGURE, "configure",
                "Register FlashArray storage server",
                "Register the FlashArray as a VME storage server of the Everpure "
                "plugin type (endpoint / token / host group / protocol), so VME "
                "can provision per-VM-disk volumes on it.",
                fields=[
                    FormField("host_group", "Host group name", FieldType.STRING,
                              required=False),
                ],
            ),
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision",
                "Provision FA volume (direct array)",
                "Create a dedicated FlashArray volume and connect it to the VME "
                "host group (array-side). The guest attach is performed by VME via "
                "the Everpure plugin when the disk is added to a VM; this action is for "
                "array-side management outside VME's provisioning flow.",
                fields=[
                    FormField("name", "Volume name", FieldType.STRING),
                    FormField("size", "Size", FieldType.SIZE, default="1T"),
                    FormField("host_group", "Host group", FieldType.STRING,
                              required=False,
                              help="Defaults to the connection's host_group."),
                ],
            ),
            ActionSpec(
                Capability.SNAPSHOT, "snapshot", "Snapshot VM disk",
                "Array-based snapshot of the FlashArray volume backing a VM disk.",
                fields=[FormField("volume", "Volume name", FieldType.STRING),
                        FormField("suffix", "Snapshot suffix", FieldType.STRING,
                                  required=False)],
            ),
            ActionSpec(
                Capability.CLONE, "clone", "Clone VM disk",
                "Array-based clone of a FlashArray volume into a new VM disk.",
                fields=[FormField("source", "Source volume", FieldType.STRING),
                        FormField("dest", "New volume name", FieldType.STRING)],
            ),
            ActionSpec(
                Capability.RESIZE, "resize", "Resize VM disk",
                "Extend the FlashArray volume backing a VM disk.",
                fields=[FormField("volume", "Volume name", FieldType.STRING),
                        FormField("size", "New size", FieldType.SIZE)],
            ),
            ActionSpec(Capability.HEALTH, "health_check", "Health check",
                       long_running=False),
            ActionSpec(Capability.REMOVE, "teardown", "Remove integration",
                       destructive=True),
            # --- cluster reconcile (membership drift) ----------------------- #
            ActionSpec(
                Capability.RECONCILE_CLUSTER, "assess_cluster",
                "Check for cluster changes",
                "READ-ONLY: compare current VME cluster membership against the "
                "FlashArray host group, and score new KVM nodes for deploy-readiness.",
                long_running=False),
            ActionSpec(
                Capability.RECONCILE_CLUSTER, "reconcile_cluster",
                "Deploy to new hosts",
                "Configure storage on newly-added (ready) VME KVM nodes; optionally "
                "remove FlashArray hosts for nodes that have left the cluster.",
                fields=[
                    FormField("apply_removals", "Also remove departed hosts",
                              FieldType.BOOL, default=False, required=False,
                              help="Remove FA hosts/group members for nodes that "
                                   "have left the cluster (default: flag only)."),
                ],
                destructive=True),
            # --- NFS datastore for QCOW2 VM images (FlashArray File) --------- #
            # NOTE: NOT yet hardware-validated against FlashArray File. Different
            # storage model from the per-disk block plugin (no host registration /
            # multipath; a FlashArray File NFS export mounted on the KVM hosts and
            # registered as a VME 'nfs' datastore).
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision_nfs_datastore",
                "Provision NFS datastore (VM images)",
                "Create a FlashArray File system + NFS export, mount it on the VME "
                "KVM hosts, and register it as a VME 'nfs' datastore for QCOW2 VM "
                "images (protocol=nfs). NOT hardware-validated.",
                fields=[
                    FormField("name", "Datastore / file system name",
                              FieldType.STRING, placeholder="vme-nfs"),
                    FormField("export_path", "NFS export path", FieldType.STRING,
                              required=False, default="/",
                              help="Export path on the file system (default '/')."),
                    FormField("mountpoint", "KVM host mount point", FieldType.STRING,
                              required=False,
                              help="OS mount point on the KVM hosts "
                                   "(default /mnt/<name>)."),
                ]),
            # --- ISO storage SPECIAL CASE (kernel NFS on the VME Manager) ---- #
            # WHY a dedicated action (NOT the normal 'nfs' datastore): HPE VME's
            # built-in NFS client for ISO storage is a JAVA client that opens its
            # sessions from HIGH (unprivileged, >1024) SOURCE PORTS. The FlashArray
            # blocks NFS traffic from high source ports, so a normal VME 'nfs'
            # datastore for ISOs FAILS to mount/serve. The workaround mounts the
            # export with the OS *kernel* NFS client on the VME MANAGER host (which
            # uses privileged <1024 source ports the array allows) and registers it
            # in VME as a local 'directory' datastore instead of an 'nfs' datastore.
            ActionSpec(
                Capability.PROVISION_VOLUME, "setup_iso_storage",
                "Set up ISO storage (kernel NFS workaround)",
                "Create a FlashArray File NFS export for ISOs, mount it with the OS "
                "kernel NFS client on the VME MANAGER host (privileged source ports "
                "the array allows -- VME's Java NFS client uses high ports the array "
                "blocks), and register it as a VME 'directory' (local) datastore.",
                fields=[
                    FormField("name", "ISO file system name", FieldType.STRING,
                              placeholder="vme-iso"),
                    FormField("mountpoint", "Manager-host mount point",
                              FieldType.STRING, required=False,
                              help="OS mount point on the VME Manager host "
                                   "(default /mnt/<name>)."),
                    FormField("export_path", "NFS export path", FieldType.STRING,
                              required=False, default="/"),
                ]),
            ActionSpec(
                Capability.REMOVE, "teardown_iso_storage",
                "Remove ISO storage",
                "Unmount the ISO export on the VME Manager host, remove its fstab "
                "entry, and delete the NFS export + FlashArray file system.",
                fields=[
                    FormField("name", "ISO file system name", FieldType.STRING),
                    FormField("mountpoint", "Manager-host mount point",
                              FieldType.STRING, required=False),
                    FormField("eradicate", "Eradicate file system", FieldType.BOOL,
                              required=False, default=False),
                ],
                destructive=True),
        ]

    # ------------------------------------------------------------- helpers ---
    def _manager_url(self) -> str:
        url = self.ctx.target.get("vme_manager_url") or self.ctx.target.get("host")
        if not url:
            raise ConnectionValidationError("No vme_manager_url configured")
        return url.rstrip("/")

    def _protocol(self) -> str:
        return (self.ctx.target.get("protocol") or "iscsi").lower()

    @staticmethod
    def _is_nvme(proto: str) -> bool:
        """True for any NVMe-oF transport (the array identifies these hosts by NQN
        and they use native NVMe multipath, not dm-multipath)."""
        return proto in ("nvme-tcp", "nvme-fc", "nvme-roce")

    @staticmethod
    def _is_fabric(proto: str) -> bool:
        """True for Fibre Channel transports (fc / nvme-fc): fabric-zoned, no
        software session login -- the host discovers LUNs/namespaces via a rescan."""
        return proto in ("fc", "nvme-fc")

    def _default_host_group(self) -> str:
        return self.ctx.target.get("host_group") or ""

    @staticmethod
    def _csv(value: str | None) -> list[str]:
        return [s.strip() for s in (value or "").split(",") if s.strip()]

    # When set (by the per-node fan-out), host-side helpers SSH to this node
    # instead of the default connection KVM host. Lets the existing single-host
    # helpers (_write_multipath_conf, _bind_*) act on each cluster node unchanged.
    _ssh_host_override: str | None = None
    # Cached real KVM-host address resolved from list_nodes() (VME /api/servers),
    # so sync callers don't hit the "localhost" fallback when the connection only
    # carries a VME manager URL.
    _resolved_kvm_host: str | None = None

    def _kvm_host(self) -> str:
        """SSH target for host-side (multipath / libvirt / FC rescan) operations."""
        return (self._ssh_host_override
                or self.ctx.target.get("kvm_host")
                or self._resolved_kvm_host
                or self.ctx.target.get("host", "localhost"))

    def _ssh_user(self) -> str:
        return (self.ctx.target.get("kvm_ssh_username")
                or self.ctx.target.get("username", "root"))

    def _needs_sudo(self) -> bool:
        """True when the KVM-host SSH user isn't root, so privileged storage
        commands (iscsiadm/multipath/nvme, /sys + /etc writes) must run via sudo."""
        return self._ssh_user() != "root"

    def _ssh_kwargs(self) -> dict[str, Any]:
        """Creds for a KVM-host SSH session, shared by the single- and per-node paths.

        The VME Manager credentials authenticate the Morpheus REST API, NOT SSH on
        the KVM hosts -- those are a different system. So prefer the dedicated
        ``kvm_ssh_*`` fields and only fall back to the Manager creds when they're
        not supplied. A non-root host user runs every command via passwordless
        ``sudo`` (the host commands need root).
        """
        kw: dict[str, Any] = {
            "username": self._ssh_user(),
            "password": (self.ctx.target.get("kvm_ssh_password")
                         or self.ctx.target.get("password")),
            "sudo": self._needs_sudo(),
        }
        key = self.ctx.target.get("kvm_ssh_key") or self.ctx.target.get("ssh_key")
        if key:
            kw["key"] = key
        return kw

    async def _check_sudo(self, host: str) -> None:
        """If the host user is non-root, verify passwordless sudo works — fail with a
        clear, actionable error instead of an opaque permission-denied mid-deploy."""
        if not self._needs_sudo() or self._is_mock_or_dry():
            return
        user = self._ssh_user()
        # Probe WITHOUT the sudo-wrapper (run `sudo -n true` directly).
        out = await self.ctx.runner.run_ssh(
            host, "sudo -n true 2>&1 || echo PHIF_SUDO_FAIL",
            check=False, timeout=20,
            **{k: v for k, v in self._ssh_kwargs().items() if k != "sudo"})
        if "PHIF_SUDO_FAIL" in (out or "") or "password" in (out or "").lower():
            raise ConnectionValidationError(
                f"KVM host {host}: user {user!r} cannot run passwordless sudo, which "
                "HPE VME host operations require (iscsiadm/multipath/nvme need root). "
                f"Configure NOPASSWD sudo for {user!r} (e.g. a sudoers drop-in: "
                f"'{user} ALL=(ALL) NOPASSWD:ALL'), or use a root SSH user.")
        await self.ctx.emit(f"[{host}] passwordless sudo OK for {user!r}")

    async def _discovery_host(self) -> str:
        """Resolve a real KVM host to SSH to for interface/initiator discovery.

        The VME hypervisor has no single ``host`` -- the KVM hosts come from the VME
        Manager (:meth:`list_nodes`). An explicit ``kvm_host`` override wins; else use
        the first discovered cluster node. (Previously this fell through to
        ``_kvm_host()`` -> ``"localhost"``, so discovery tried to SSH the PHIF
        container itself and failed with "Connection refused".)
        """
        explicit = self.ctx.target.get("kvm_host") or self.ctx.target.get("host")
        if explicit:
            return str(explicit)
        nodes = await self.list_nodes()
        return nodes[0].host if nodes else "localhost"

    async def _ssh_on(self, host: str, command: str) -> str:
        return await self.ctx.runner.run_ssh(host, command, **self._ssh_kwargs())

    async def _ssh(self, command: str) -> str:
        return await self._ssh_on(self._kvm_host(), command)

    def _is_mock_or_dry(self) -> bool:
        """True when no real host/manager I/O happens (runner short-circuits).

        ``list_nodes`` cannot query the VME Manager REST in mock/dry-run (run_http
        returns ``{}``), so it returns a synthetic cluster instead.
        """
        return bool(self.ctx.dry_run or getattr(self.ctx.runner, "mock", False)
                    or getattr(self.ctx.runner, "dry_run", False))

    async def discover_options(self, kind: str) -> list[dict[str, Any]]:
        """Enumerate bindable host interfaces/HBAs for a dynamic UI dropdown.

        Delegates to the shared :meth:`JobRunner.discover_interfaces` against the
        VME KVM host for the well-known DiscoveryKind values:

          * ``"nics"``         -> Ethernet NICs for iSCSI ``iface`` binding;
          * ``"nvme_sources"`` -> IP-bearing interfaces usable as NVMe-TCP
            ``host-traddr`` sources;
          * ``"fc_hbas"``      -> Fibre Channel HBAs.

        Returns ``[{"value","label", ...}, ...]``. Mock/dry-run yields synthetic
        entries so the UI dropdowns stay exercisable without a real host.

        ``kind == "initiators"`` is a special case: it discovers the KVM host's
        IQN / NQN / FC WWNs (via :meth:`JobRunner.discover_initiators`) and returns
        each tagged with the ``register_hosts`` form ``field`` it should pre-fill
        (``iqns`` / ``nqns`` / ``wwns``) plus a human ``label``, so the UI can show
        the operator what was found and populate the matching input.

        The KVM-host SSH session used for interface discovery reuses the
        connection's username / password / ssh_key; live iSCSI validation confirmed
        these connection credentials reach the VME KVM hosts for discovery.
        """
        if kind == "initiators":
            found = await self._discover_initiators()
            opts: list[dict[str, Any]] = []
            if found.get("iqn"):
                opts.append({"field": "iqns", "value": found["iqn"],
                             "label": f"iSCSI IQN — {found['iqn']}"})
            if found.get("nqn"):
                opts.append({"field": "nqns", "value": found["nqn"],
                             "label": f"NVMe NQN — {found['nqn']}"})
            if found.get("wwns"):
                wwns = ", ".join(self._normalize_wwn(w) for w in found["wwns"])
                opts.append({"field": "wwns", "value": wwns,
                             "label": f"FC WWNs — {wwns}"})
            return opts
        opts = await self.ctx.runner.discover_interfaces(
            await self._discovery_host(),
            kind,
            **self._ssh_kwargs(),
        )
        # IP interfaces (NICs for iSCSI, source addrs for NVMe-TCP) are filtered so
        # only consistent, storage-reachable interfaces are offered; FC HBAs have no
        # subnet and pass through.
        if kind in ("nics", "nvme_sources"):
            # 1) Keep only subnets configured on EVERY cluster host (uniform bind).
            opts = await self._filter_common_subnet_interfaces(kind, opts)
            # 2) And restrict to interfaces that can reach the array's portals.
            if self.ctx.array is not None:
                portals = await self.ctx.array.get_data_interfaces(
                    "nvme-tcp" if self._protocol() == "nvme-tcp" else "iscsi")
                opts = nics_on_array_subnets(opts, portals)
        return opts

    async def _filter_common_subnet_interfaces(
        self, kind: str, host_opts: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Restrict candidate IP interfaces to subnets configured on every host.

        Discovers the same interface ``kind`` on each cluster host and keeps only
        the primary host's options whose subnet is present on ALL hosts (via
        :func:`nics_on_common_subnets`), so the chosen binding exists uniformly
        across the cluster. A single-host cluster leaves ``host_opts`` unchanged.
        """
        nodes = await self.list_nodes()
        if len(nodes) <= 1:
            return host_opts
        per_node: dict[str, list[dict[str, Any]]] = {}
        for node in nodes:
            per_node[node.name] = await self.ctx.runner.discover_interfaces(
                node.host, kind, **self._ssh_kwargs())
        return nics_on_common_subnets(host_opts, per_node)

    async def _discover_initiators(self) -> dict[str, Any]:
        """Auto-discover the VME KVM host's storage initiators via the shared helper.

        Delegates to :meth:`JobRunner.discover_initiators`, which reads the standard
        Linux locations over SSH (``/etc/iscsi/initiatorname.iscsi`` for the IQN,
        ``/etc/nvme/hostnqn`` for the NQN, ``/sys/class/fc_host/*/port_name`` for FC
        WWNs). Returns ``{"iqn": str|None, "nqn": str|None, "wwns": [str, ...]}``.
        Mock/dry-run yields synthetic values so flows stay exercisable, so the
        operator never has to type initiator IDs.

        The KVM-host SSH session used for initiator discovery uses the connection's
        username / password; live iSCSI validation confirmed initiator discovery
        (the IQN read from ``/etc/iscsi/initiatorname.iscsi``) works with these
        connection credentials.
        """
        return await self._discover_initiators_on(await self._discovery_host())

    async def _discover_initiators_on(self, host: str) -> dict[str, Any]:
        """Auto-discover storage initiators on a specific KVM host (per-node fan-out).

        Same as :meth:`_discover_initiators` but targets ``host`` explicitly so the
        cluster register flow can discover each node's initiators.
        """
        return await self.ctx.runner.discover_initiators(host, **self._ssh_kwargs())

    async def _discover_host_wwns(self) -> list[str]:
        """Discover the KVM host FC HBA port WWNs via the shared initiator helper.

        Normalizes the helper's bare-hex WWNs (e.g. ``2100002432aabbcc``) to the
        colon-separated form the FlashArray expects. In mock/dry-run mode the helper
        returns synthetic WWNs so discovery-driven flows remain exercisable.
        """
        result = await self._discover_initiators()
        return [self._normalize_wwn(w) for w in result.get("wwns", [])]

    @staticmethod
    def _normalize_wwn(raw: str) -> str:
        """Normalize a WWN to colon-separated form (``aa:bb:...``).

        Accepts ``0x``-prefixed, bare 16-hex, or already-colon-separated input.
        Non 16-hex values are returned stripped/lowercased unchanged.
        """
        token = raw.strip().lower()
        if token.startswith("0x"):
            token = token[2:]
        if ":" in token:
            return token
        hexdigits = "".join(ch for ch in token if ch in "0123456789abcdef")
        if len(hexdigits) == 16:
            return ":".join(hexdigits[i:i + 2] for i in range(0, 16, 2))
        return token

    @staticmethod
    def _scsi_wwid(serial: str) -> str:
        """Build the Linux SCSI multipath WWID for a FlashArray volume serial.

        Everpure volumes use NAA IEEE Registered Extended (type 6) with the Everpure OUI::

            wwid = "3" + "624a9370" + lc(serial24)
            e.g. 3624a93700123456789abcdef0bb82813

        The FA REST returns the 24-hex serial WITHOUT the "624a9370" prefix, so we
        prepend it -- but tolerate a serial that already includes "624a937" (and
        strip a leading "0x"). Mirrors the Proxmox PureFAPlugin.pm ``_scsi_wwid``.
        """
        s = (serial or "").strip().lower()
        if s.startswith("0x"):
            s = s[2:]
        if s.startswith("624a937"):
            return f"3{s}"          # serial already carries the Everpure OUI
        return f"3624a9370{s}"      # prepend NAA-6 + Everpure OUI

    @staticmethod
    def _fa_name(raw: str) -> str:
        """Sanitize a string into a valid FlashArray object name.

        FlashArray names allow only ``[A-Za-z0-9-]`` and must begin/end with an
        alphanumeric. A KVM-host name/FQDN/IP (e.g. ``192.0.2.58`` or
        ``kvm01.example.local``) has dots, so map any invalid char to ``-`` and
        trim leading/trailing hyphens.
        """
        import re

        s = re.sub(r"[^A-Za-z0-9-]", "-", raw or "").strip("-")
        return s or "vme-host"

    async def _authenticate(self) -> str:
        """Obtain a VME Manager (Morpheus) bearer token via run_http (mock-safe).

        CONFIRMED (Morpheus OpenAPI): ``POST /oauth/token`` with a
        **form-encoded** (``application/x-www-form-urlencoded``) body --
        ``grant_type=password``, ``client_id=morph-api``, ``scope=write``,
        ``username``, ``password`` -- returns ``{access_token, refresh_token,
        expires_in, token_type:"Bearer", scope}``. Sub-tenant users authenticate
        as ``subdomain\\username``.
        """
        url = f"{self._manager_url()}{_OAUTH_PATH}"
        await self.ctx.emit("Authenticating to VME Manager ...")
        resp = await self.ctx.runner.run_http(
            "POST",
            url,
            # Form-encoded (NOT JSON) -- this is the documented Morpheus contract.
            data={
                "grant_type": "password",
                "client_id": "morph-api",
                "scope": "write",
                "username": self.ctx.target.get("username", "admin"),
                "password": self.ctx.target.get("password", ""),
            },
        )
        token = (resp.get("json") or {}).get("access_token", "")
        # In mock/dry-run mode run_http returns {}, so synthesize a placeholder.
        return token or "mock-vme-token"

    def _auth_headers(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

    # ------------------------------------------------------ cluster awareness ---
    async def list_nodes(self) -> list[ClusterNode]:
        """Enumerate the VME cluster's KVM hosts via the VME Manager REST API.

        Node-specific operations (host registration on the array, transport setup +
        multipath drop-in) fan out across these; cluster-level operations (the VME
        storage-integration / datastore registration) run once.

        In mock/dry-run the manager REST returns ``{}`` (run_http short-circuits),
        so a synthetic 2-host cluster is returned to keep the cluster flow
        exercisable. A standalone deployment (no hosts returned / parse failure)
        falls back to the single host derived from the connection.

        CONFIRMED (Morpheus OpenAPI): hosts are ``GET /api/servers`` (the UI
        "Hosts" == API "servers"; there is no ``/api/hosts``). ``?vmHypervisor=true``
        filters to hypervisor hosts; each server's reachable management address is
        ``sshHost`` (falling back to ``internalIp`` / ``externalIp``), and the host
        kind is in ``computeServerType.code``.
        """
        fallback = self._kvm_host()
        if self._is_mock_or_dry():
            await self.ctx.emit(
                f"[mock/dry-run] synthetic 2-host VME cluster seeded from {fallback}")
            return [
                ClusterNode(name="vme-kvm1", host=fallback,
                            info={"online": True, "mock": True}),
                ClusterNode(name="vme-kvm2", host="192.0.2.2",
                            info={"online": True, "mock": True}),
            ]

        try:
            url = self._manager_url()
            token = await self._authenticate()
            # CONFIRMED: GET /api/servers?vmHypervisor=true returns hypervisor hosts.
            resp = await self.ctx.runner.run_http(
                "GET", f"{url}{_API_BASE}/servers?vmHypervisor=true",
                headers=self._auth_headers(token),
            )
            body = resp.get("json") or {}
            raw_hosts = body.get("servers") or body.get("hosts") or []
            nodes: list[ClusterNode] = []
            for h in raw_hosts:
                h = h or {}
                name = h.get("name") or h.get("hostname") or h.get("id")
                # CONFIRMED field precedence: sshHost (the address Morpheus uses to
                # reach the host) -> internalIp -> externalIp.
                mgmt = (h.get("sshHost") or h.get("internalIp")
                        or h.get("externalIp") or h.get("ip") or name)
                if not name:
                    continue
                cst = (h.get("computeServerType") or {})
                nodes.append(ClusterNode(name=str(name), host=str(mgmt),
                                         info={"id": h.get("id"),
                                               "type": cst.get("code")}))
            if nodes:
                # Cache the first real host so sync host-side callers (e.g.
                # _check_sudo outside the per-node fan-out) don't fall back to the
                # bogus "localhost" default when the connection carries only a VME
                # manager URL and no explicit kvm_host.
                self._resolved_kvm_host = nodes[0].host
                await self.ctx.emit(
                    f"VME cluster: {len(nodes)} KVM host(s) -> "
                    f"{', '.join(n.name for n in nodes)}")
                return nodes
        except (ValueError, TypeError, KeyError) as exc:  # not the expected shape
            await self.ctx.emit(
                f"Could not enumerate VME KVM hosts ({exc}); treating "
                f"{fallback} as a single host")

        return [ClusterNode(name=str(fallback), host=str(fallback))]

    async def validate_cluster(self, **params: Any) -> OpResult:
        """Ensure every VME KVM host exposes the same storage interfaces.

        For the active protocol, discovers the relevant interface kind on each host
        (iscsi->nics, fc->fc_hbas, nvme-tcp->nvme_sources) via
        ``runner.discover_interfaces`` and compares the sets across hosts with
        :func:`compare_node_interfaces`, so the chosen storage binding is valid
        cluster-wide.
        """
        proto = self._protocol()
        kind = {"iscsi": "nics", "fc": "fc_hbas",
                "nvme-tcp": "nvme_sources"}.get(proto, "nics")
        # Only the storage-subnet NICs matter — compare those, not every VM
        # tap/bridge/VLAN (which legitimately differ between hosts).
        portals = (await self.ctx.array.get_data_interfaces(
            "nvme-tcp" if proto == "nvme-tcp" else "iscsi")
            if self.ctx.array is not None else [])
        nodes = await self.list_nodes()
        await self.ctx.emit(
            f"Validating {len(nodes)} KVM host(s) for uniform {kind} ({proto})")
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
        data = {"protocol": proto, "kind": kind, "per_node": per_node,
                "nodes": [n.to_dict() for n in nodes]}
        if consistent:
            return OpResult.ok(f"Cluster interfaces consistent: {detail}", **data)
        return OpResult.fail(f"Cluster interfaces inconsistent: {detail}", **data)

    @classmethod
    def wizard_steps(cls) -> list[str]:
        """Ordered wizard actions for HPE VME.

        Deploy + configure the (cluster-level) VME storage integration first, then
        register each KVM host on the array and set up per-host connectivity.
        """
        return ["deploy", "configure", "register_hosts", "setup_connectivity"]

    # ---------------------------------------------------------- operations ---
    async def validate_connection(self) -> OpResult:
        url = self._manager_url()
        await self.ctx.emit(f"Validating connection to VME Manager {url} ...")
        # CONFIRMED: GET /api/ping is an unauthenticated liveness probe; do it
        # first so an unreachable appliance fails fast before we try to auth.
        await self.ctx.runner.run_http("GET", f"{url}{_API_BASE}/ping")
        token = await self._authenticate()
        # CONFIRMED: GET /api/whoami returns the user + permissions, so it both
        # validates the token and surfaces what this account can do.
        await self.ctx.runner.run_http(
            "GET", f"{url}{_API_BASE}/whoami", headers=self._auth_headers(token),
        )
        if self.ctx.array is not None:
            info = await self.ctx.array.info()
            await self.ctx.emit(f"FlashArray reachable: {info}")
        return OpResult.ok(f"Connected to VME Manager {url}",
                           url=url, protocol=self._protocol())

    async def register_hosts(self, host_group: str = "", iqns: str = "", wwns: str = "",
                             nqns: str = "", nodes: "list[ClusterNode] | None" = None,
                             **_: Any) -> OpResult:
        """Register the VME KVM host(s) on the FlashArray, all in one host group.

        Fans out across the cluster's KVM hosts (:meth:`list_nodes`): each host gets
        its OWN FA host (its discovered/supplied initiators), and every FA host is
        placed in the single shared ``host_group`` so per-disk volumes can be
        presented to the group. Explicit action-field initiators (iqns/wwns/nqns)
        apply to a single-host registration; in a multi-host cluster the
        protocol-relevant initiator is auto-discovered per node so the operator
        never types them.

        Live iSCSI validation confirmed one FA host per VME KVM node (VME Manager
        exposes per-host initiators under /api/servers), all placed in the shared
        host group; a single-node deployment keeps the ``{host_group}-vme`` host name.
        """
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        proto = self._protocol()
        host_group = host_group or self._default_host_group()
        if not host_group:
            return OpResult.fail("No host_group specified")

        # NFS is a file (datastore) model with NO block host registration -- the
        # export is mounted on the hosts, there are no initiators in a host group.
        if proto == "nfs":
            return OpResult.ok(
                "Protocol 'nfs' uses a FlashArray File datastore -- no block host "
                "registration needed (use provision_nfs_datastore).",
                artifacts={"protocol": "nfs", "host_group": host_group, "hosts": [],
                           "nodes": []})

        explicit_iqns = self._csv(iqns)
        explicit_wwns = self._csv(wwns) or self._csv(self.ctx.target.get("host_wwns"))
        explicit_nqns = self._csv(nqns)

        # Cluster reconcile passes an explicit subset (only the ready new nodes) so
        # we configure ONLY those; everything else defaults to the full cluster.
        # The historical single-node ``{host_group}-vme`` name only applies to a
        # genuine single-node *deployment* (full cluster of one); a subset passed in
        # for reconcile always uses per-node names so they match the cluster.
        if nodes is None:
            nodes = await self.list_nodes()
            single = len(nodes) == 1
        else:
            single = False
        conn_host = self._kvm_host()

        registered: list[dict[str, Any]] = []
        for node in nodes:
            # Operator-supplied initiators apply to the connection node only (a
            # single-host deployment, or the matching member of a cluster); other
            # cluster members always auto-discover their own initiators.
            is_conn = single or node.host == conn_host
            iqn_list = list(explicit_iqns) if is_conn else []
            wwn_list = list(explicit_wwns) if is_conn else []
            nqn_list = list(explicit_nqns) if is_conn else []

            # The array identifies NVMe hosts by NQN for ALL NVMe transports
            # (tcp/fc/roce); SCSI FC by WWN; iSCSI by IQN.
            need_iscsi = proto == "iscsi" and not iqn_list
            need_nvme = self._is_nvme(proto) and not nqn_list
            need_fc = proto == "fc" and not wwn_list
            if need_iscsi or need_nvme or need_fc:
                await self.ctx.emit(
                    f"No explicit {proto} initiators for {node.name}; "
                    f"auto-discovering from the VME KVM host {node.host!r} ..."
                )
                discovered = await self._discover_initiators_on(node.host)
                if need_iscsi and discovered.get("iqn"):
                    iqn_list = [discovered["iqn"]]
                if need_nvme and discovered.get("nqn"):
                    nqn_list = [discovered["nqn"]]
                if need_fc:
                    wwn_list = [self._normalize_wwn(w)
                                for w in discovered.get("wwns", [])]

            # FAIL-FAST on a missing initiator for the chosen IP transport. Without
            # this, registration silently creates an FA host with NO ports, the
            # array then rejects every host login ("authorization failure", error
            # 24), and volumes never surface — a confusing failure surfacing only
            # later at VM boot. Better to stop here with an actionable message.
            if proto == "iscsi" and not iqn_list and not self.ctx.dry_run:
                return OpResult.fail(
                    f"iSCSI selected but no IQN was provided or discovered for "
                    f"{node.name} ({node.host}). The KVM host's "
                    f"/etc/iscsi/initiatorname.iscsi could not be read — ensure the "
                    f"host is reachable over SSH (kvm_ssh_* creds, passwordless "
                    f"sudo) so its initiator can be registered on the array.")
            if self._is_nvme(proto) and not nqn_list and not self.ctx.dry_run:
                return OpResult.fail(
                    f"{proto} selected but no NQN was provided or discovered for "
                    f"{node.name} ({node.host}); ensure the KVM host is reachable "
                    f"for initiator discovery (cat /etc/nvme/hostnqn).")

            if proto == "fc":
                # Fibre Channel: register by HBA port WWN only. No IP login -- the
                # host's WWNs must already be zoned to the array on the fabric.
                if not wwn_list and not self.ctx.dry_run:
                    return OpResult.fail(
                        f"Fibre Channel selected but no host WWNs provided or "
                        f"discovered for {node.name} (set host_wwns / wwns, or "
                        f"ensure the KVM host is reachable for initiator discovery)"
                    )
                iqn_list, nqn_list = [], []  # FC carries WWNs only.
                await self.ctx.emit(
                    f"Registering {node.name} on FlashArray by WWN "
                    f"(fc, wwns={wwn_list})"
                )
            else:
                await self.ctx.emit(
                    f"Registering {node.name} initiators on FlashArray "
                    f"({proto}, iqns={iqn_list}, wwns={wwn_list}, nqns={nqn_list})"
                )

            # FA host name: a single-host deployment keeps the historical
            # ``{host_group}-vme`` name; a cluster uses a per-node name so each KVM
            # host maps to its own FA host. FA names allow only [A-Za-z0-9-].
            if single:
                host_name = self._fa_name(f"{host_group}-vme")
            else:
                host_name = self._fa_name(f"{host_group}-{node.name}")
            registered.append({"node": node.name, "host": host_name,
                               "iqns": iqn_list, "wwns": wwn_list, "nqns": nqn_list})

        if self.ctx.dry_run:
            return OpResult.ok(
                f"[dry-run] would register {len(nodes)} host(s) in group "
                f"{host_group} ({proto})",
                artifacts={"host_group": host_group, "protocol": proto,
                           "wwns": explicit_wwns,
                           "hosts": [r["host"] for r in registered],
                           "nodes": [r["node"] for r in registered]})

        # Reuse pre-existing FA hosts (by initiator), adopt an existing host group
        # if the hosts already belong to one, and fail descriptively if they span
        # multiple groups. (apply_host_group is shared across all connectors.)
        specs = [{"name": r["host"], "iqns": r["iqns"], "wwns": r["wwns"],
                  "nqns": r["nqns"]} for r in registered]
        res = await self.apply_host_group(host_group, specs)
        if res.get("conflict"):
            return OpResult.fail(res["conflict"],
                                 artifacts={"host_group": host_group, "protocol": proto})
        host_group = res["host_group"]
        host_names = res["hosts"]
        return OpResult.ok(
            f"Host group {host_group} registered ({proto}, {len(host_names)} host(s))",
            artifacts={"host_group": host_group, "host": host_names[0],
                       "hosts": host_names, "nodes": [r["node"] for r in registered],
                       "protocol": proto, "wwns": registered[0]["wwns"],
                       "adopted_host_group": res["adopted"]})

    @staticmethod
    def _as_list(value: Any) -> list[str]:
        """Normalize a MULTISELECT field (list, or comma-separated string) to a list."""
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return [str(v).strip() for v in value if str(v).strip()]
        return [s.strip() for s in str(value).split(",") if s.strip()]

    async def setup_connectivity(self, host_group: str = "", iscsi_nics: Any = None,
                                 nvme_sources: Any = None, nvme_options: str = "",
                                 fc_hbas: Any = None,
                                 nodes: "list[ClusterNode] | None" = None,
                                 **_: Any) -> OpResult:
        proto = self._protocol()
        host_group = host_group or self._default_host_group()
        # NFS: a mounted FlashArray File export, NOT a block transport -- there is no
        # iSCSI/FC/NVMe session login or dm-multipath to set up here.
        if proto == "nfs":
            return OpResult.ok(
                "Protocol 'nfs' needs no block connectivity / multipath "
                "(use provision_nfs_datastore to mount the export).",
                protocol="nfs", host_group=host_group, steps=[], binding={},
                portals=[], target_iqn="", target_nqn="", nodes=[], per_node={})
        await self.ctx.emit(f"Preparing {proto} connectivity + multipath for VME hosts ...")
        # Block protocols rely on the FA host group + multipath on the KVM hosts so
        # each volume appears as /dev/mapper/<mpath> for direct attach.
        # Live iSCSI validation confirmed the Everpure-recommended multipath.conf /
        # udev settings on the VME KVM hosts yield the expected /dev/mapper/<wwid>
        # multipath device for each presented FlashArray volume.

        # ARRAY PORTAL + TARGET DISCOVERY is CLUSTER-LEVEL (the array side is shared
        # across all KVM hosts): for the IP transports, discover the array's
        # data-network portal IPs and the iSCSI target IQN / NVMe subsystem NQN from
        # the FlashArray so the operator doesn't have to supply them. FC has no
        # portals (fabric zoning only), so it is skipped. Runs ONCE.
        portals: list[str] = []
        target_iqn = ""
        target_nqn = ""
        if self.ctx.array is not None and proto in ("iscsi", "nvme-tcp"):
            service = "nvme-tcp" if proto == "nvme-tcp" else "iscsi"
            portals = await self.ctx.array.get_data_interfaces(service)
            await self.ctx.emit(f"Discovered array {service} portals: {portals}")
            ports = await self.ctx.array.get_target_ports()
            if proto == "iscsi":
                target_iqn = ports.get("iqn") or ""
                await self.ctx.emit(f"Discovered array iSCSI target IQN: {target_iqn}")
            else:
                target_nqn = ports.get("nqn") or ""
                await self.ctx.emit(f"Discovered array NVMe subsystem NQN: {target_nqn}")

            # FAIL-FAST: an IP transport with no portals means the array has no
            # iSCSI/NVMe-TCP networking configured (it may be Fibre Channel /
            # NVMe-FC only) — bail out with an actionable message rather than
            # letting the host-side session login fail cryptically later.
            if not portals and not self.ctx.dry_run:
                return OpResult.fail(
                    f"No {proto} portal IPs found on the array. Its {proto} "
                    f"interfaces have no IP configured (this array may be Fibre "
                    f"Channel / NVMe-FC only). Configure {proto} networking on the "
                    f"FlashArray, switch the hypervisor protocol to 'fc', or pass "
                    f"portals explicitly.")

        # PER-HOST FAN-OUT: the transport setup (interface binding) + the Everpure
        # multipath drop-in / rescan run on EACH KVM host in the cluster. A single
        # node keeps the original single-host behavior.
        iscsi_nics_list = self._as_list(iscsi_nics)
        nvme_sources_list = self._as_list(nvme_sources)
        fc_hbas_list = self._as_list(fc_hbas)
        # Cluster reconcile passes the ready new-node subset; default = full cluster.
        if nodes is None:
            nodes = await self.list_nodes()

        binding: dict[str, Any] = {}
        steps: list[str] = []
        per_node: dict[str, list[str]] = {}
        for node in nodes:
            self._ssh_host_override = node.host
            try:
                node_binding, node_steps = await self._setup_host_connectivity(
                    proto, iscsi_nics_list, nvme_sources_list, nvme_options,
                    fc_hbas_list, portals=portals, target_iqn=target_iqn)
            finally:
                self._ssh_host_override = None
            binding = node_binding  # uniform across nodes (same selection)
            steps = node_steps       # uniform step set
            per_node[node.name] = node_steps

        if binding and not self.ctx.dry_run:
            # Persist the binding choice in the connector state where relevant.
            self.ctx.target.connection["interface_binding"] = binding

        return OpResult.ok(
            f"Connectivity ({proto}) prepared on {len(nodes)} host(s)",
            protocol=proto, host_group=host_group, steps=steps,
            binding=binding, portals=portals, target_iqn=target_iqn,
            target_nqn=target_nqn, nodes=[n.to_dict() for n in nodes],
            per_node=per_node)

    async def _setup_host_connectivity(
        self, proto: str, iscsi_nics_list: list[str], nvme_sources_list: list[str],
        nvme_options: str, fc_hbas_list: list[str],
        *, portals: list[str] | None = None, target_iqn: str = "",
    ) -> tuple[dict[str, Any], list[str]]:
        """Transport setup + Everpure multipath drop-in for ONE KVM host.

        Acts on the host currently targeted by ``_ssh_host_override`` (set by the
        per-node fan-out in :meth:`setup_connectivity`). Returns the chosen
        ``(binding, steps)`` for that host. Mock-safe / dry-run no-op for SSH.
        """
        steps: list[str] = []
        binding: dict[str, Any] = {}
        # A non-root host user must have passwordless sudo for the storage commands;
        # fail early with a clear message rather than an opaque iscsiadm error.
        await self._check_sudo(self._kvm_host())
        # INTERFACE BINDING -- act only on the selected protocol. Since per-disk
        # volumes are attached directly to VMs, the binding governs which host
        # interfaces carry the sessions.
        if proto == "iscsi" and iscsi_nics_list:
            binding["iscsi_nics"] = iscsi_nics_list
            await self._bind_iscsi_ifaces(iscsi_nics_list)
            steps.append("iscsi_iface_binding")
        elif proto == "nvme-tcp" and nvme_sources_list:
            binding["nvme_sources"] = nvme_sources_list
            if nvme_options:
                binding["nvme_options"] = nvme_options
            await self._bind_nvme_sources(nvme_sources_list, nvme_options)
            steps.append("nvme_source_binding")
        elif proto == "fc" and fc_hbas_list:
            binding["fc_hbas"] = fc_hbas_list
            await self._bind_fc_hbas(fc_hbas_list)
            steps.append("fc_hba_binding")

        if proto == "fc":
            # FIBRE CHANNEL: no software login (no iscsiadm / no `nvme connect`).
            # PREREQUISITE: SAN zoning between the KVM host HBA WWNs and the
            # FlashArray FC target ports must already exist (done on the fabric
            # switch). Here we only trigger a host-side FC/SCSI rescan so newly
            # connected LUNs are discovered, then refresh multipath.
            await self.ctx.emit(
                "FC: assuming SAN zoning (host WWNs <-> FA FC ports) is in place; "
                "rescanning FC/SCSI bus (no iscsiadm / nvme connect)"
            )
            # Prefer rescan-scsi-bus.sh; fall back to issuing LIP on each fc_host
            # and scanning each scsi_host. Best-effort (check=False semantics noted).
            rescan = (
                "rescan-scsi-bus.sh -a 2>/dev/null || { "
                "for h in /sys/class/fc_host/*/issue_lip; do echo 1 > \"$h\"; done; "
                "for s in /sys/class/scsi_host/host*/scan; do "
                "echo '- - -' > \"$s\"; done; }"
            )
            await self._ssh(rescan)
            steps.append("fc_scsi_rescan")
            # Install the Everpure-recommended multipath drop-in (PURE/FlashArray device
            # stanza, ALUA, find_multipaths) and restart multipathd so each LUN
            # surfaces as /dev/mapper/<wwid>.
            await self._write_multipath_conf()
            steps.append("multipath_conf")
            await self._ssh("multipath -r 2>/dev/null || multipath; multipath -ll")
            steps.append("multipath_reload")
        elif proto == "nvme-fc":
            # NVMe-oF over FC: fabric-based (no nvme connect to an IP). PREREQUISITE:
            # SAN zoning (host NVMe-FC initiators <-> FA NVMe-FC target ports) must
            # already exist. We nudge the fabric autoconnect; native NVMe multipath
            # assembles the namespace (no /etc/multipath.conf device stanza).
            await self.ctx.emit(
                "NVMe-FC: assuming fabric zoning is in place; connecting fabric "
                "(no iscsiadm / no IP nvme connect)")
            await self._ssh("nvme connect-all -t fc 2>/dev/null || true; "
                            "nvme list 2>/dev/null || true")
            steps.append("nvme_fc_connect")
        elif proto == "iscsi":
            # iSCSI is a SCSI transport. The Everpure VME plugin presents each FA volume
            # to the host group, but the KVM host must already hold persistent iSCSI
            # SESSIONS to the array's portals for any LUN to surface — without them
            # the device never appears and a VM provision fails with "Error running
            # vm". So log in to every discovered portal (startup=automatic), THEN
            # install the Everpure multipath drop-in so the portal paths group to one
            # wwid, then refresh multipath.
            await self._login_iscsi(portals or [], target_iqn)
            steps.append("iscsi_login")
            await self._write_multipath_conf()
            steps.append("multipath_conf")
            await self._ssh("multipath -r 2>/dev/null || multipath; multipath -ll")
            steps.append("multipath_reload")
        else:
            # NVMe-TCP uses native NVMe multipath (no device-mapper multipath.conf);
            # just confirm + refresh.
            await self._ssh("multipath -r 2>/dev/null || multipath; multipath -ll")
            steps.append("multipath_reload")
        return binding, steps

    async def _login_iscsi(self, portals: list[str], target_iqn: str = "") -> None:
        """Establish persistent iSCSI sessions to the FlashArray portals on the KVM
        host so presented LUNs actually surface as block devices.

        Enables/starts ``iscsid``, runs sendtargets discovery against each portal,
        marks every resulting node ``startup=automatic`` (so sessions survive a
        reboot), then logs in. Idempotent: re-discovery/re-login on an existing
        session is a no-op. Best-effort per command (a single unreachable portal
        must not abort the others); the subsequent multipath build + the attach-time
        device verification surface any real failure. Mock-safe / dry-run no-op.
        """
        if self.ctx.dry_run or not portals:
            return
        await self.ctx.emit(
            f"iSCSI: enabling iscsid + logging in to {len(portals)} portal(s) "
            f"{portals}")
        # open-iscsi packages the daemon as iscsid (most distros) or open-iscsi.
        await self._ssh(
            "systemctl enable --now iscsid 2>/dev/null "
            "|| systemctl enable --now open-iscsi 2>/dev/null || true")
        for portal in portals:
            await self._ssh(
                f"iscsiadm -m discovery -t sendtargets -p {portal}:3260 2>&1 || true")
        # Persist auto-login for the discovered node records, then log in. Scope to
        # the array's target IQN when known so we never touch unrelated targets.
        scope = f"-T {target_iqn} " if target_iqn else ""
        await self._ssh(
            f"iscsiadm -m node {scope}-o update -n node.startup -v automatic 2>&1 || true")
        await self._ssh(f"iscsiadm -m node {scope}--login 2>&1 || true")
        await self._ssh("iscsiadm -m session 2>&1 || true")

    async def _write_multipath_conf(self) -> None:
        """Install the Everpure FlashArray multipath config as a drop-in and (re)start
        multipathd.

        Writes a dedicated /etc/multipath/conf.d/pure.conf that multipathd
        auto-includes, rather than overwriting the operator's /etc/multipath.conf
        (which may blacklist local disks or configure other arrays). The main file
        is only created (empty) if absent. Re-running rewrites only our drop-in.
        Without the PURE device stanza + find_multipaths, multipathd won't group
        the iSCSI/FC paths to one wwid. SCSI transports only (NVMe-oF uses native
        NVMe multipath). Mock-safe / dry-run no-op.
        """
        if self.ctx.dry_run:
            return
        await self.ctx.emit(f"Writing Everpure multipath drop-in {_MULTIPATH_DROPIN} on the KVM host")
        await self._ssh("mkdir -p /etc/multipath/conf.d")
        await self._ssh("test -f /etc/multipath.conf || touch /etc/multipath.conf")
        await self._ssh(
            f"cat > {_MULTIPATH_DROPIN} <<'PUREFA_MPATH_EOF'\n"
            f"{_MULTIPATH_CONF}PUREFA_MPATH_EOF"
        )
        # NB: never `systemctl restart multipathd` here -- on hosts with active
        # maps the stop phase blocks and the restart hangs (the SSH channel then
        # sits open until the runner's timeout abandons it). Instead ensure the
        # daemon is running and tell the *running* daemon to re-read its config,
        # which applies our drop-in and returns immediately.
        await self._ssh(
            "systemctl enable --now multipathd 2>/dev/null || true; "
            "multipathd reconfigure 2>/dev/null "
            "|| systemctl reload multipathd 2>/dev/null || true; "
            "multipath -r 2>/dev/null || true"
        )

    # ----------------------------------------------- interface binding apply ---
    async def _bind_iscsi_ifaces(self, nics: list[str]) -> None:
        """Bind iSCSI sessions to the chosen NICs via iscsiadm iface objects.

        For each NIC we create an iscsiadm iface and pin it to the NIC's netdev so
        the iSCSI sessions egress that interface. Mock-safe / dry-run no-op (the
        SSH helper skips real I/O in PHIF_MOCK_MODE / dry-run).
        """
        await self.ctx.emit(f"iSCSI: binding sessions to NICs {nics}")
        if self.ctx.dry_run:
            return
        for nic in nics:
            iface = f"iface-{nic}"
            # Creating an iface that already exists exits non-zero on some
            # open-iscsi builds, so make `new` best-effort and let the bind
            # (`update`) be the command whose rc we actually trust. With stderr
            # now merged into the stream, a failing bind reports its real reason.
            await self._ssh(
                f"iscsiadm -m iface -I {iface} -o new 2>/dev/null || true; "
                f"iscsiadm -m iface -I {iface} -o update "
                f"-n iface.net_ifacename -v {nic}"
            )
        # Multi-NIC iSCSI ARP-flux fix on the selected storage NICs (persisted +
        # live), so dual-NIC iSCSI on a shared subnet doesn't bind to the wrong path.
        if nics:
            await self.ctx.emit(f"iSCSI: applying ARP-flux sysctls for {nics}")
            await self._ssh(arp_flux_cmd(nics))

    async def _bind_nvme_sources(self, sources: list[str], options: str = "") -> None:
        """Bind NVMe-TCP sessions to the chosen source interfaces/addresses.

        Uses ``nvme connect`` with ``-w <host-traddr>`` so the session originates
        from the selected source address. Mock-safe / dry-run no-op.

        # TODO(validate-on-appliance): NVMe-TCP is NOT yet hardware-validated (only
        # the iSCSI block path was). Confirm the NVMe-TCP subsystem NQN / target
        # traddr + port the VME KVM host should connect to (VME-API specific); the
        # binding here pins only the host-side source (``-w``).
        """
        await self.ctx.emit(f"NVMe-TCP: binding sessions to sources {sources}")
        if self.ctx.dry_run:
            return
        extra = f" {options}".rstrip() if options else ""
        for src in sources:
            # TODO(validate-on-appliance, NVMe-TCP not hardware-validated):
            # -t/-a/-s/-n (transport/target traddr/port/subnqn) come from the
            # VME/array discovery; -w pins the host source address.
            await self._ssh(f"nvme connect -t tcp -w {src}{extra}")

    async def _bind_fc_hbas(self, hbas: list[str]) -> None:
        """Select which FC HBAs carry the sessions and rescan them.

        FC has no software login; selecting an HBA means we scan that specific
        ``fc_host`` so only the chosen ports surface LUNs. Mock-safe / dry-run
        no-op.

        # TODO(validate-on-appliance): FC is NOT yet hardware-validated (only the
        # iSCSI block path was). Confirm the WWPN -> /sys/class/fc_host/hostN
        # mapping on VME KVM hosts (the scaffold matches by port_name) and that SAN
        # zoning for the selected HBAs is in place.
        """
        await self.ctx.emit(f"FC: selecting HBAs {hbas} for sessions")
        if self.ctx.dry_run:
            return
        for wwpn in hbas:
            bare = self._normalize_wwn(wwpn).replace(":", "")
            await self._ssh(
                f"for h in /sys/class/fc_host/*; do "
                f"pn=$(cat \"$h/port_name\" 2>/dev/null | sed 's/^0x//'); "
                f"[ \"$pn\" = \"{bare}\" ] && "
                f"echo '- - -' > \"/sys/class/scsi_host/$(basename $h)/scan\"; done"
            )

    async def deploy_integration(self, plugin_jar: str = "", force: bool = False,
                                 **_: Any) -> OpResult:
        """Upload the native Everpure FlashArray storage plugin to the VME Manager.

        The plugin (built under ``files/morpheus-plugin/`` via ``./gradlew
        shadowJar``) registers a "Everpure FlashArray" StorageServerType so VME
        can provision per-VM-disk FA volumes natively. This uploads the shadow JAR
        via the Plugins API; the appliance loads it on every node with no restart.
        Idempotent: if the plugin code is already present it is reported as
        installed instead of re-uploaded -- UNLESS ``force`` is set, which always
        re-uploads the JAR so an updated build replaces the running plugin (an
        upgrade). Pass ``force=true`` in the deploy job params after changing the
        plugin code.

        CONFIRMED: plugins live under ``GET /api/plugins`` and upload via
        ``POST /api/plugins/upload`` (multipart ``plugin`` field). ``POST
        /api/plugins`` is GET-only -> 404.
        """
        url = self._manager_url()
        jar = plugin_jar or self.ctx.target.get("plugin_jar") or self._default_plugin_jar()
        force = force or bool(self.ctx.target.get("plugin_force_upload"))
        await self.ctx.emit(f"Deploying Everpure FlashArray VME plugin from {jar!r} "
                            f"(force={force}) ...")
        token = await self._authenticate()

        # Already installed? (idempotent) -- skipped when force-upgrading.
        if not force:
            try:
                resp = await self.ctx.runner.run_http(
                    "GET", f"{url}{_API_BASE}/plugins", headers=self._auth_headers(token))
                plugins = (resp.get("json") or {}).get("plugins") or []
                if any((p or {}).get("code") == _PLUGIN_CODE for p in plugins):
                    await self.ctx.emit(f"Plugin {_PLUGIN_CODE!r} already installed "
                                        "(pass force=true to re-upload an updated build)")
                    return OpResult.ok("Everpure VME plugin already installed",
                                       status="deployed", plugin=_PLUGIN_CODE)
            except Exception as exc:  # noqa: BLE001 - listing is best-effort
                await self.ctx.emit(f"Could not list installed plugins ({exc}); proceeding")

        if self._is_mock_or_dry():
            return OpResult.ok(f"[mock/dry-run] would upload plugin JAR {jar}",
                               status="deployed", plugin=_PLUGIN_CODE, jar=jar)

        # Upload the shadow JAR (multipart). The JAR must exist on the PHIF host.
        import os

        if not os.path.isfile(jar):
            return OpResult.fail(
                f"Plugin JAR not found at {jar!r}. It is normally built into the "
                f"backend image by the Dockerfile's gradle stage; rebuild the image "
                f"(docker compose build), or build it standalone with "
                f"`cd backend/phif/connectors/hpevme/files/morpheus-plugin && "
                f"gradle shadowJar`, or set plugin_jar.")
        # CONFIRMED (Morpheus OpenAPI + morpheus-cli): upload is POST
        # /api/plugins/upload (multipart) -- NOT POST /api/plugins (GET-only, hence
        # the 404). The shipping CLI names the file part "plugin". Async: the plugin
        # goes status "installing" -> "loaded"; the idempotent GET above catches it
        # on the next run.
        with open(jar, "rb") as fh:
            await self.ctx.runner.run_http(
                "POST", f"{url}{_API_BASE}/plugins/upload",
                headers={"Authorization": f"Bearer {token}"},
                files={"plugin": (os.path.basename(jar), fh, "application/java-archive")},
            )
        await self.ctx.emit(f"Uploaded Everpure VME plugin {_PLUGIN_CODE!r} (installing)")
        return OpResult.ok("Everpure FlashArray VME plugin uploaded",
                           status="deployed", plugin=_PLUGIN_CODE, jar=jar)

    def _default_plugin_jar(self) -> str:
        """Absolute path to the plugin's built shadow JAR next to this connector.

        Globs for the shadow JAR (``*-all.jar``) so a version bump doesn't require
        editing a hardcoded path; falls back to the pinned default name. Picks the
        newest match when several exist (e.g. after an upgrade build).
        """
        import glob
        import os

        here = os.path.dirname(os.path.abspath(__file__))
        libs = os.path.join(here, "files", "morpheus-plugin", "build", "libs")
        matches = sorted(glob.glob(os.path.join(libs, "*-all.jar")),
                         key=lambda p: os.path.getmtime(p) if os.path.exists(p) else 0)
        if matches:
            return matches[-1]
        return os.path.join(here, "files", "morpheus-plugin", *_PLUGIN_JAR_DEFAULT.split("/"))

    async def configure(self, host_group: str = "", pure_endpoint: str = "",
                        pure_api_token: str = "", **_: Any) -> OpResult:
        """Register the FlashArray as a VME storage server of the Everpure plugin type.

        Once the plugin is installed (:meth:`deploy_integration`), this creates the
        storage-server record VME provisions against. The FlashArray endpoint +
        token come from the **associated array** (``ctx.array`` / ``resolve_token``),
        not operator form fields; the host group + protocol are stored in the
        server's config so the plugin connects per-disk volumes to the right group.

        CONFIRMED: ``POST /api/storage-servers`` with a ``storageServer`` object
        whose ``type`` is the plugin's StorageServerType code.
        """
        url = self._manager_url()
        proto = self._protocol()
        host_group = host_group or self._default_host_group()
        endpoint = pure_endpoint or (
            getattr(self.ctx.array, "endpoint", "") if self.ctx.array else "")
        api_token = self.ctx.resolve_token(pure_api_token or None) or ""
        await self.ctx.emit(
            f"Registering FlashArray {endpoint!r} as a VME storage server "
            f"(type {_STORAGE_SERVER_TYPE}, protocol={proto}, host_group={host_group!r})")
        if self.ctx.dry_run:
            return OpResult.ok("[dry-run] would register storage server",
                               protocol=proto, host_group=host_group,
                               storage_server_type=_STORAGE_SERVER_TYPE)
        token = await self._authenticate()
        await self.ctx.runner.run_http(
            "POST", f"{url}{_API_BASE}/storage-servers",
            headers=self._auth_headers(token),
            json_body={
                "storageServer": {
                    "name": f"pure-{endpoint}" if endpoint else "pure-flasharray",
                    "type": _STORAGE_SERVER_TYPE,
                    "serviceUrl": endpoint,
                    "serviceToken": api_token,
                    "config": {"hostGroup": host_group, "protocol": proto},
                }
            },
        )
        return OpResult.ok(
            "FlashArray registered as a VME storage server",
            protocol=proto, host_group=host_group,
            storage_server_type=_STORAGE_SERVER_TYPE, endpoint=endpoint)

    async def provision(self, name: str, size: str = "1T",
                        host_group: str = "", **_: Any) -> OpResult:
        """Direct-array provisioning: create an FA volume + connect to the host group.

        This is array-side only. The **guest attach is performed by VME** via the
        Everpure plugin (``MvmProvisionFacet``) when the disk is added to a VM; PHIF no
        longer attaches over SSH/libvirt. Use this for array-side management (e.g.
        pre-creating volumes) outside VME's own provisioning flow.
        """
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        proto = self._protocol()
        host_group = host_group or self._default_host_group()

        await self.ctx.emit(
            f"[{proto}] provisioning FlashArray volume {name} ({size})"
        )
        if self.ctx.dry_run:
            return OpResult.ok(
                f"[dry-run] would create + connect {name}",
                artifacts={"volume": name, "protocol": proto, "host_group": host_group},
            )

        # 1. Dedicated FA volume.
        await self.ctx.array.create_volume(name, size)
        # 2. Present it to the VME host group (initiators registered earlier).
        if host_group:
            await self.ctx.array.connect_volume(host_group, name)
            await self.ctx.emit(f"Connected {name} to host group {host_group}")
        # 3. Resolve the resulting host device path (informational; VME's plugin
        #    builds the libvirt disk pointing at it).
        device = await self._resolve_mpath(name, proto)

        return OpResult.ok(
            f"Provisioned FlashArray volume {name} (attach via VME/Everpure plugin)",
            artifacts={"volume": name, "protocol": proto, "host_group": host_group,
                       "device": device, "managed_by": "vme-pure-plugin"})

    async def _resolve_mpath(self, volume: str, proto: str) -> str:
        """Resolve the host block-device path for an FA volume.

        The device is named from the array-assigned **serial**, not the volume
        name: we look the serial up via ``ctx.array.get_volume`` and build the
        identifier. For SCSI transports (iSCSI/FC) the dm-multipath node is
        ``/dev/mapper/3<624a9370><lc(serial)>``; for NVMe-oF the namespace is
        identified by its EUI (``eui.<lc(serial)>``). If the serial can't be
        fetched (e.g. dry-run, or no associated array) we fall back to deriving
        the id from the volume name so the flow stays exercisable.

        # TODO(validate-on-appliance): NVMe-oF is NOT yet hardware-validated (only
        # the iSCSI SCSI/dm-multipath path was). Confirm the exact NVMe namespace
        # device path Everpure presents on a VME KVM host (eui.* vs nguid) -- NVMe-oF
        # for VME is not covered by Everpure's docs.
        """
        serial = volume
        if self.ctx.array is not None:
            try:
                vol = await self.ctx.array.get_volume(volume)
                serial = (vol or {}).get("serial") or volume
            except Exception:   # dry-run / transient -- fall back to the name
                serial = volume

        if self._is_nvme(proto):
            # NVMe-oF: native NVMe multipath; the namespace EUI carries the serial.
            ident = "eui." + serial.lower().removeprefix("0x")
            device = f"/dev/disk/by-id/nvme-{ident}"
            if self._is_fabric(proto):  # nvme-fc: confirm the namespace appeared
                await self._ssh(f"nvme list 2>/dev/null | grep -i {serial} || true")
            return device

        wwid = self._scsi_wwid(serial)  # 3 + 624a9370 + lc(serial)
        if self._is_fabric(proto):
            # FC: confirm the multipath device resolved by WWID (no login involved).
            await self._ssh(
                f"multipath -ll {wwid} 2>/dev/null || ls -l /dev/mapper/{wwid}"
            )
        return f"/dev/mapper/{wwid}"

    async def snapshot(self, volume: str, suffix: str | None = None, **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Array snapshot of VM disk {volume}")
        await self.ctx.array.create_snapshot(volume, suffix)
        return OpResult.ok(f"Snapshot of {volume} created")

    async def clone(self, source: str, dest: str, **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Array clone of VM disk {source} -> {dest}")
        await self.ctx.array.clone_volume(source, dest)
        return OpResult.ok(f"Cloned {source} -> {dest}", artifacts={"volume": dest})

    async def resize(self, volume: str, size: str, **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Extending VM disk {volume} -> {size}")
        await self.ctx.array.extend_volume(volume, size)
        # TODO(validate-on-appliance): the array-side extend is on the validated
        # FlashArray block path, but the guest-side rescan trigger is NOT yet
        # hardware-validated -- after the extend the guest must rescan the block
        # device (e.g. virsh blockresize / multipathd reconfigure); confirm the
        # VME-side trigger.
        return OpResult.ok(f"Resized {volume} to {size}")

    # ------------------------------------------------------------------ #
    # VM management (cross-hypervisor migration)
    #
    # HPE VME == Morpheus: VMs are "instances" and the Everpure plugin owns the guest
    # attach. Migration re-points the SAME FlashArray volume by referencing the
    # existing storage on the destination instance — no data moves. The exact
    # Morpheus instance/volume/interface request shapes vary by version and are
    # NOT hardware-confirmed here, so they are marked TODO(validate-on-appliance)
    # and kept mock-safe so the flow stays exercisable.
    # ------------------------------------------------------------------ #
    async def _api(self, method: str, path: str, *,
                   json_body: Any = None) -> dict[str, Any]:
        url = self._manager_url()
        token = await self._authenticate()
        resp = await self.ctx.runner.run_http(
            method, f"{url}{_API_BASE}{path}",
            headers=self._auth_headers(token), json_body=json_body)
        return resp.get("json") or {}

    async def list_vms(self) -> list[dict[str, Any]]:
        if self._is_mock_or_dry():
            return [{"id": "101", "name": "mock-vm", "power_state": "running",
                     "vcpus": 2, "memory_bytes": 2 * 1024**3,
                     "disk_count": 1, "nic_count": 1}]
        body = await self._api("GET", "/instances")
        vms: list[dict[str, Any]] = []
        for inst in body.get("instances") or []:
            status = (inst.get("status") or "").lower()
            vms.append({"id": str(inst.get("id")), "name": inst.get("name", ""),
                        "power_state": "running" if status == "running" else "stopped"})
        return vms

    async def list_networks(self) -> list[dict[str, Any]]:
        if self._is_mock_or_dry():
            return [{"id": "1", "name": "VM Network", "kind": "network"}]
        body = await self._api("GET", "/networks")
        return [{"id": str(n.get("id")), "name": n.get("name", ""), "kind": "network"}
                for n in (body.get("networks") or [])]

    async def power_state(self, vm_ref: str) -> str:
        if self._is_mock_or_dry():
            return "running"
        body = await self._api("GET", f"/instances/{vm_ref}")
        status = ((body.get("instance") or {}).get("status") or "").lower()
        if status == "running":
            return "running"
        if status in ("stopped", "suspended", "off"):
            return "stopped"
        return "unknown"

    async def capture_vm_spec(self, vm_ref: str) -> "VmSpec":
        from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec

        if self._is_mock_or_dry():
            return VmSpec(
                name="mock-vm", source_ref=str(vm_ref), vcpus=2,
                memory_bytes=2 * 1024**3, firmware="bios",
                disks=[DiskSpec(DiskIdentity(fa_volume="vol-mock-0"),
                                bus="scsi", order=0, boot=True,
                                source_ref="vmevol-0")],
                nics=[NicSpec(mac="AA:BB:CC:00:00:01", source_network="1",
                              model="virtio", order=0)])

        body = await self._api("GET", f"/instances/{vm_ref}")
        inst = body.get("instance") or {}
        plan = inst.get("plan") or {}
        config = inst.get("config") or {}
        vcpus = int(inst.get("maxCores") or config.get("cores") or plan.get("maxCores") or 1)
        memory = int(inst.get("maxMemory") or plan.get("maxMemory") or 0)
        firmware = "uefi" if str(config.get("uefi") or "").lower() in ("true", "1") else "bios"

        # The instance volume record does NOT carry the FlashArray volume name (its
        # externalId is null); the per-server storage-volume does, as
        # /dev/mapper/3624a9370<serial>. Resolve each disk to its REAL array volume
        # (name + serial) so the destination can copy/re-map it — the migration's
        # FA-volume lookup needs the array name, not Morpheus's display name ("root").
        sv = (await self._api("GET", "/storage-volumes?max=500")).get("storageVolumes") or []
        ext_by_id = {v.get("id"): (v.get("externalId") or "") for v in sv}
        disks: list[DiskSpec] = []
        for i, vol in enumerate(inst.get("volumes") or []):
            ext = ext_by_id.get(vol.get("id")) or vol.get("externalId") or ""
            serial = self._serial_from_wwid(ext)
            fa = (await self.ctx.array.find_volume_name_by_serial(serial)
                  if serial and self.ctx.array else None)
            if not fa:
                raise ConnectionValidationError(
                    f"Could not resolve the FlashArray volume for disk "
                    f"{vol.get('name')!r} of instance {vm_ref} (externalId={ext!r}); "
                    f"the disk must be backed by an Everpure FlashArray volume to migrate.")
            size_bytes = int(vol.get("maxStorage") or 0) or (
                int(vol.get("size") or 0) * 1024 ** 3)
            disks.append(DiskSpec(
                identity=DiskIdentity(fa_volume=fa,
                                      serial=(serial.upper() if serial else None),
                                      size_bytes=size_bytes or None),
                bus="scsi", order=int(vol.get("displayOrder", i)),
                boot=bool(vol.get("rootVolume", i == 0)),
                source_ref=str(vol.get("id", ""))))

        nics: list[NicSpec] = []
        for i, iface in enumerate(inst.get("interfaces") or []):
            net = iface.get("network") or {}
            nics.append(NicSpec(
                mac=iface.get("macAddress", ""),
                source_network=str(net.get("id", "")),
                model="virtio", order=int(iface.get("displayOrder", i))))

        disks.sort(key=lambda d: d.order)
        nics.sort(key=lambda n: n.order)
        if disks and not any(d.boot for d in disks):
            disks[0].boot = True
        return VmSpec(
            name=inst.get("name", f"vm-{vm_ref}"), source_ref=str(vm_ref),
            vcpus=vcpus, memory_bytes=memory, firmware=firmware,
            disks=disks, nics=nics)

    async def stop_vm(self, vm_ref: str, *, force: bool = False) -> OpResult:
        if await self.power_state(vm_ref) == "stopped":
            return OpResult.ok(f"instance {vm_ref} already stopped")
        # TODO(validate-on-appliance): confirm stop verb (PUT vs POST) for this version.
        await self._api("PUT", f"/instances/{vm_ref}/stop")
        return OpResult.ok(f"Stopped instance {vm_ref}")

    async def start_vm(self, vm_ref: str) -> OpResult:
        if await self.power_state(vm_ref) == "running":
            return OpResult.ok(f"instance {vm_ref} already running")
        await self._api("PUT", f"/instances/{vm_ref}/start")
        return OpResult.ok(f"Started instance {vm_ref}")

    async def detach_volumes(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        if self._is_mock_or_dry():
            return OpResult.ok(f"detached {len(disks)} volume(s) (mock)")
        for disk in disks:
            vol_id = disk.source_ref
            if not vol_id:
                continue
            # keepBacking so the FlashArray volume survives the config detach.
            await self._api(
                "DELETE", f"/instances/{vm_ref}/volumes/{vol_id}",
                json_body={"keepBackingVolume": True})
            await self.ctx.emit(f"Detached volume {vol_id} from instance {vm_ref}")
        return OpResult.ok(f"Detached {len(disks)} volume(s) from instance {vm_ref}")

    @staticmethod
    def _serial_from_wwid(wwid: str) -> str:
        """Reverse of :meth:`_scsi_wwid`: extract the 24-hex FA serial from a Linux
        multipath WWID (``3624a9370<serial>`` or ``/dev/mapper/3624a9370<serial>``)."""
        w = (wwid or "").strip().lower().rsplit("/", 1)[-1]
        if w.startswith("3624a9370"):
            return w[len("3624a9370"):]
        if w.startswith("624a9370"):
            return w[len("624a9370"):]
        return w

    async def _purefa_datastore_id(self) -> str:
        """The VME datastore id backed by the Everpure FlashArray plugin (where
        provisioning lands a per-VM FA volume)."""
        body = await self._api("GET", "/data-stores?max=100")
        for d in body.get("datastores") or []:
            code = ((d.get("datastoreType") or {}).get("code") or "").lower()
            if "pure" in code and d.get("allowProvision"):
                return str(d.get("id"))
        raise ConnectionValidationError(
            "No provisionable Everpure FlashArray datastore "
            "(pure-flasharray-vme.datastore) found on the VME appliance — the Everpure "
            "storage integration must be configured on VME before migrating.")

    async def _mvm_layout_id(self) -> int:
        """The 'HVM' (mvm) instance-type's KVM layout id (the blank-VM layout we
        provision the migrated disk into). Falls back to the validated default."""
        body = await self._api("GET", "/instance-types?max=200")
        for t in body.get("instanceTypes") or []:
            if (t.get("code") or "") == "mvm":
                for lay in t.get("instanceTypeLayouts") or []:
                    if (lay.get("provisionTypeCode") or "") == "kvm":
                        return int(lay.get("id"))
        return 32  # validated: "Single HVM" (mvm-1.0-single)

    async def _resolve_plan(self, vcpus: int, memory_bytes: int) -> int:
        """Pick the smallest KVM service plan that fits the source vCPU/RAM (the
        plan sets CPU/memory; disk size is the volume size, set separately)."""
        body = await self._api("GET", "/service-plans?max=500&phrase=kvm")
        plans = [p for p in body.get("servicePlans") or []
                 if (p.get("provisionType") or {}).get("code") == "kvm"]
        key = lambda p: (p.get("maxCores") or 0, p.get("maxMemory") or 0)
        fits = [p for p in plans
                if (p.get("maxCores") or 0) >= max(1, vcpus)
                and (p.get("maxMemory") or 0) >= int(memory_bytes or 0)]
        chosen = (min(fits, key=key) if fits
                  else max(plans, key=key) if plans else None)
        if not chosen:
            raise ConnectionValidationError(
                "No KVM service plan is available on the VME appliance.")
        return int(chosen["id"])

    async def _provision_refs(self, placement: dict[str, Any] | None = None) -> dict[str, Any]:
        """Resolve the Morpheus references a VM provision needs: the cluster
        (group/site), its cloud (zone), the mvm KVM layout, the Everpure datastore, and
        the resource pool. The operator's wizard ``placement`` (cluster + storage)
        wins; otherwise a pinned connection field (``kvm_cluster`` /
        ``resource_pool_id``) or the single configured cluster + Everpure datastore."""
        placement = placement or {}
        want_cluster = (placement.get("cluster")
                        or self.ctx.target.get("kvm_cluster")
                        or self.ctx.target.get("vme_group_id"))
        groups = (await self._api("GET", "/groups")).get("groups") or []
        group = None
        for g in groups:
            if want_cluster and (str(g.get("id")) == str(want_cluster)
                                 or (g.get("name") or "") == str(want_cluster)):
                group = g
                break
        group = group or (groups[0] if groups else {})
        zones = group.get("zones") or []
        return {
            "site_id": group.get("id") or 1,
            "zone_id": (zones[0].get("id") if zones else None) or 1,
            "layout_id": await self._mvm_layout_id(),
            "datastore_id": (str(placement["storage"]) if placement.get("storage")
                             else await self._purefa_datastore_id()),
            "resource_pool_id": self.ctx.target.get("resource_pool_id") or "pool-1",
        }

    async def list_placements(self) -> list[dict[str, Any]]:
        """Everpure-connected clusters (Morpheus groups) + their Everpure datastores, for the
        migration destination wizard. Only groups whose cloud(s) expose a
        provisionable Everpure FlashArray datastore are returned."""
        if self._is_mock_or_dry():
            return [{"cluster": {"id": "1", "name": "VME"},
                     "storage": [{"id": "4", "name": "purefa", "kind": "datastore"}]}]
        ds = (await self._api("GET", "/data-stores?max=100")).get("datastores") or []
        pure = [{"id": str(d["id"]), "name": d.get("name") or str(d["id"]),
                 "kind": "datastore", "zone": (d.get("zone") or {}).get("id")}
                for d in ds
                if "pure" in ((d.get("datastoreType") or {}).get("code") or "").lower()
                and d.get("allowProvision")]
        groups = (await self._api("GET", "/groups")).get("groups") or []
        out: list[dict[str, Any]] = []
        for g in groups:
            zone_ids = {z.get("id") for z in (g.get("zones") or [])}
            store = [{"id": d["id"], "name": d["name"], "kind": d["kind"]}
                     for d in pure if d["zone"] is None or d["zone"] in zone_ids]
            if store:
                out.append({"cluster": {"id": str(g.get("id")),
                                        "name": g.get("name") or str(g.get("id"))},
                            "storage": store})
        return out

    async def create_vm(self, spec: "VmSpec", *,
                        network_map: dict[str, str],
                        placement: dict[str, Any] | None = None) -> OpResult:
        ordered = sorted(spec.disks, key=lambda d: (0 if d.boot else 1, d.order))
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
            return OpResult.ok("created (mock)", artifacts={"vm_ref": "901"})

        # Provision an HVM (mvm) instance with an Everpure-FlashArray-backed volume per
        # source disk, POWERED OFF. Validated against the live VME/Morpheus API: a
        # blank disk can't boot ("Error running vm"), so we never boot here — the
        # migration FA-copy-overwrites the source data onto these volumes, then
        # start_vm() boots the (now bootable) disk. The Everpure VME plugin creates +
        # attaches the FA volume; the KVM host must already hold iSCSI sessions
        # (see register_hosts + setup_connectivity) for the device to surface.
        refs = await self._provision_refs(placement)
        plan_id = await self._resolve_plan(spec.vcpus, spec.memory_bytes)
        volumes = []
        for idx, d in enumerate(ordered):
            gb = max(1, -(-int(d.identity.size_bytes or 0) // (1000 ** 3)))
            volumes.append({"id": -1, "rootVolume": bool(d.boot) or idx == 0,
                            "name": "root" if (d.boot or idx == 0) else f"data-{d.order}",
                            "size": gb, "storageType": 1,
                            "datastoreId": refs["datastore_id"]})
        interfaces = []
        for nic in spec.nics:
            dest_net = network_map.get(nic.source_network)
            if not dest_net:
                return OpResult.fail(
                    f"no destination network mapped for source NIC {nic.source_network!r}")
            interfaces.append({"network": {"id": dest_net}})
        cfg = {"resourcePoolId": refs["resource_pool_id"], "poolProviderType": "mvm",
               "uefi": spec.firmware == "uefi", "provisionPoweredOff": True,
               "createBackup": False}
        body = await self._api("POST", "/instances", json_body={
            "zoneId": refs["zone_id"],
            "instance": {"name": name, "site": {"id": refs["site_id"]},
                         "instanceType": {"code": "mvm"},
                         "layout": {"id": refs["layout_id"]},
                         "plan": {"id": plan_id}, "config": cfg},
            "config": cfg,
            "volumes": volumes,
            "networkInterfaces": interfaces,
        })
        new_id = str((body.get("instance") or {}).get("id") or "")
        if not new_id:
            return OpResult.fail("VME instance create returned no id")
        await self.ctx.emit(
            f"Provisioning instance {new_id} ({name}) powered-off with "
            f"{len(volumes)} Everpure-backed disk(s); plan={plan_id}")
        # Wait for the powered-off provision to create + attach the FA disks, then
        # resolve each disk's FA volume name (for the migration copy-overwrite).
        await self._await_provisioned(new_id)
        await self._cache_provisioned_disks(new_id, ordered)
        return OpResult.ok(f"Created instance {new_id}", artifacts={"vm_ref": new_id})

    async def _await_provisioned(self, vm_ref: str) -> None:
        """Poll until a powered-off provision settles to 'stopped' (disk created +
        attached). 'running' is also accepted; 'failed' raises with the reason."""
        import asyncio

        for _ in range(60):  # ~5 min
            inst = (await self._api("GET", f"/instances/{vm_ref}")).get("instance") or {}
            status = (inst.get("status") or "").lower()
            if status in ("stopped", "running"):
                return
            if status == "failed":
                raise ConnectionValidationError(
                    f"VME provision of instance {vm_ref} failed: "
                    f"{inst.get('statusMessage') or 'Error running vm'}")
            await asyncio.sleep(8)
        raise ConnectionValidationError(
            f"VME instance {vm_ref} did not finish provisioning in time")

    async def _cache_provisioned_disks(self, vm_ref: str,
                                       ordered_disks: "list[DiskSpec]") -> None:
        """Map each source disk -> the FA volume name the Everpure plugin provisioned,
        by matching displayOrder and resolving the storage volume's WWID->serial->
        array volume name. Cached for create_managed_disk()."""
        inst = (await self._api("GET", f"/instances/{vm_ref}")).get("instance") or {}
        ivols = sorted(inst.get("volumes") or [],
                       key=lambda v: v.get("displayOrder", 0))
        # storage-volume id -> externalId (/dev/mapper/<wwid>), where the FA serial lives.
        sv = (await self._api("GET", "/storage-volumes?max=500")).get("storageVolumes") or []
        ext_by_id = {v.get("id"): (v.get("externalId") or "") for v in sv}
        self._provisioned_disks: dict[int, str] = {}
        for idx, disk in enumerate(ordered_disks):
            if idx >= len(ivols):
                break
            iv = ivols[idx]
            ext = ext_by_id.get(iv.get("id")) or iv.get("externalId") or ""
            serial = self._serial_from_wwid(ext)
            fa = (await self.ctx.array.find_volume_name_by_serial(serial)
                  if serial and self.ctx.array else None)
            if not fa:
                raise ConnectionValidationError(
                    f"Could not resolve the FlashArray volume for disk {disk.order} "
                    f"of instance {vm_ref} (externalId={ext!r}, serial={serial!r}).")
            self._provisioned_disks[disk.order] = fa
            await self.ctx.emit(
                f"Resolved instance {vm_ref} disk {disk.order} -> FA volume {fa}")

    async def attach_existing_volumes(self, vm_ref: str,
                                      disks: "list[DiskSpec]") -> OpResult:
        if self._is_mock_or_dry():
            return OpResult.ok(f"attached {len(disks)} volume(s) (mock)")
        # The KVM hosts must see the freshly-mapped block device before VME wires
        # the libvirt disk. Flush any stale multipath map first (a dead-path map can
        # wedge the host on activation) then rescan — all time-bounded.
        await self._rescan_hosts(await self._disk_wwids(disks))
        for disk in disks:
            # TODO(validate-on-appliance): reference the EXISTING storage-server
            # volume (by FA volume name) rather than provisioning a new one.
            await self._api("POST", f"/instances/{vm_ref}/volumes", json_body={
                "volume": {"name": disk.identity.fa_volume,
                           "rootVolume": disk.boot,
                           "existing": True}})
            await self.ctx.emit(
                f"Attached volume {disk.identity.fa_volume} to instance {vm_ref}")
        return OpResult.ok(f"Attached {len(disks)} volume(s) to instance {vm_ref}")

    async def _rescan_hosts(self, wwids: "list[str] | None" = None) -> None:
        """Flush stale multipath maps + rescan the transport on every KVM host so a
        newly mapped FlashArray volume appears cleanly before VME wires it. Bounded
        timeouts so a stuck device can't wedge/hang the host."""
        if self._is_mock_or_dry():
            return
        from phif.connectors.multipath import refresh_block_devices

        proto = self._protocol()
        kw = self._ssh_kwargs()
        for node in await self.list_nodes():
            await refresh_block_devices(
                (lambda cmd, t, h=node.host: self.ctx.runner.run_ssh(
                    h, cmd, check=False, timeout=t, **kw)),
                protocol=proto, wwids=wwids or [])

    async def create_managed_disk(self, vm_ref: str, *, size_bytes: int,
                                  order: int, boot: bool) -> str:
        """Return the FA volume name for source disk ``order`` on the destination.

        Unlike Proxmox/XCP (where each disk is created on demand), an HPE/Morpheus
        instance must be provisioned WITH all its volumes (Morpheus owns the create
        + host attach). :meth:`create_vm` already provisioned + resolved them, so
        here we just hand back the cached FA volume name the migration copies onto.
        """
        if self._is_mock_or_dry():
            return f"vme-mock-{vm_ref}-disk-{order}"
        cache = getattr(self, "_provisioned_disks", None) or {}
        fa = cache.get(order)
        if not fa:
            raise ConnectionValidationError(
                f"No provisioned FlashArray volume cached for disk {order} on "
                f"instance {vm_ref}; create_vm must run (and succeed) first.")
        return fa

    async def set_boot_order(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        # VME marks a volume rootVolume=true at attach time; no separate boot-order
        # call is required. TODO(validate-on-appliance): confirm boot device wiring.
        return OpResult.ok("boot device set via rootVolume at attach")

    async def delete_vm(self, vm_ref: str, *, keep_disks: bool = True) -> OpResult:
        if self._is_mock_or_dry():
            return OpResult.ok(f"deleted instance {vm_ref} (mock)")
        # preserveVolumes keeps the FlashArray-backed volumes when removing the
        # half-created destination instance during rollback.
        await self._api(
            "DELETE",
            f"/instances/{vm_ref}?preserveVolumes={'true' if keep_disks else 'false'}")
        return OpResult.ok(f"Deleted instance {vm_ref}")

    async def health_check(self, **_: Any) -> OpResult:
        url = self._manager_url()
        token = await self._authenticate()
        # CONFIRMED: GET /api/health (appliance health/alarms) + GET /api/instances.
        health = await self.ctx.runner.run_http(
            "GET", f"{url}{_API_BASE}/health", headers=self._auth_headers(token),
        )
        instances = await self.ctx.runner.run_http(
            "GET", f"{url}{_API_BASE}/instances", headers=self._auth_headers(token),
        )
        array_info = await self.ctx.array.info() if self.ctx.array else {}
        return OpResult.ok(
            "Healthy",
            vme_health=health.get("json", {}),
            vme_instances=instances.get("json", {}),
            array=array_info,
        )

    async def teardown(self, volume: str = "", host_group: str = "",
                       **_: Any) -> OpResult:
        """Disconnect a per-disk volume from the host group on the array.

        The guest-side detach + multipath flush is handled by VME via the plugin's
        ``releaseVolumeFromHost`` when the disk is removed from a VM; PHIF only
        disconnects the volume on the array here. The FA volume itself is NOT
        destroyed (data-preserving); use the array/delete op for that.
        """
        host_group = host_group or self._default_host_group()
        await self.ctx.emit("Disconnecting HPE VME per-disk volume from the array ...")
        if self.ctx.dry_run:
            return OpResult.ok("[dry-run] would disconnect volume",
                               status="not_deployed")

        if volume and host_group and self.ctx.array is not None:
            try:
                await self.ctx.array.disconnect_volume(host_group, volume)
                await self.ctx.emit(f"Disconnected {volume} from host group {host_group}")
            except Exception as exc:  # noqa: BLE001 - best-effort cleanup
                await self.ctx.emit(f"Array disconnect skipped/failed: {exc}")
        return OpResult.ok("Per-disk volume disconnected (FA volume preserved)",
                           status="not_deployed")

    # ------------------------------------------------- per-node FA host name ---
    def _fa_host_name(self, host_group: str, node: "ClusterNode", *,
                      single: bool) -> str:
        """FA host name for a VME KVM node (matches :meth:`register_hosts`).

        A single-host deployment keeps the historical ``{host_group}-vme`` name; a
        cluster uses the per-node ``{host_group}-{node.name}`` name. FA names allow
        only ``[A-Za-z0-9-]``.
        """
        if single:
            return self._fa_name(f"{host_group}-vme")
        return self._fa_name(f"{host_group}-{node.name}")

    def _fa_host_name_candidates(self, host_group: str,
                                 node: "ClusterNode") -> "set[str]":
        """All FA host names a VME node might ALREADY be registered under.

        Membership matching must not assume a single naming convention. A node's
        FA host may have been created by this connector
        (``{host_group}-{node.name}``, or the legacy single-node
        ``{host_group}-vme``), by the VME morpheus-plugin, or by an operator using
        the node's own hostname / management IP. Comparing only the per-node
        convention name flags EVERY host as both "new" and "departed" when the
        array actually names hosts differently (e.g. bare hostname). Return every
        plausible form so an already-configured node is recognized however named.
        """
        cands = {
            self._fa_name(f"{host_group}-{node.name}"),  # per-node convention
            self._fa_name(f"{host_group}-vme"),          # legacy single-node
            self._fa_name(node.name),                    # bare hostname / FQDN
        }
        if node.host:
            cands.add(self._fa_name(node.host))          # bare mgmt host / IP
        return {c for c in cands if c}

    # ----------------------------------------------------- cluster reconcile ---
    async def assess_cluster(self, **params: Any) -> OpResult:
        """READ-ONLY: report VME cluster membership drift vs. the FA host group.

        Lists the VME KVM nodes, derives each one's expected FA host name, and
        compares that set to the FlashArray host group's current members:

        * ``new_hosts`` -- nodes whose FA host is NOT yet a group member, each with
          a readiness verdict (``{"node","host","ready","reasons"}``) from
          :meth:`score_host_readiness` (reachable + protocol initiator discovered +
          a storage NIC on the array's portal subnet matching the existing nodes);
        * ``departed_hosts`` -- group members with no matching current node.

        Changes nothing. Mock/dry-run safe (uses the synthetic cluster + mock
        discovery). NFS has no block host group, so drift is reported on nodes only.
        """
        proto = self._protocol()
        host_group = self._default_host_group()
        nodes = await self.list_nodes()
        node_names = [n.name for n in nodes]

        if proto == "nfs" or not host_group or self.ctx.array is None:
            return OpResult.ok(
                f"{len(nodes)} VME node(s); no block host-group assessment "
                f"(protocol={proto}, host_group={host_group!r})",
                nodes=node_names, new_hosts=[], departed_hosts=[])

        single = len(nodes) == 1
        # Canonical name a NEW host would be registered under (per-node, or the
        # legacy single-node name).
        primary = {n.name: self._fa_host_name(host_group, n, single=single)
                   for n in nodes}
        # Every name a node might ALREADY be known by on the array -- matched
        # robustly so a host named by the morpheus-plugin / operator (bare
        # hostname) is not mis-flagged as both new and departed.
        candidates = {n.name: self._fa_host_name_candidates(host_group, n)
                      for n in nodes}
        members = await self.ctx.array.get_host_group_members(host_group)
        member_set = set(members)

        # Nodes whose FA host is already a member (under ANY of its known names).
        claimed: set[str] = set()
        configured_nodes: list[ClusterNode] = []
        for node in nodes:
            match = candidates[node.name] & member_set
            if match:
                claimed |= match
                configured_nodes.append(node)
        departed = sorted(m for m in member_set if m not in claimed)

        # Array portals + a baseline subnet (from an already-configured node) feed
        # the readiness scorer for IP transports.
        array_portals: list[str] = []
        if proto in ("iscsi", "nvme-tcp"):
            service = "nvme-tcp" if proto == "nvme-tcp" else "iscsi"
            array_portals = await self.ctx.array.get_data_interfaces(service)
        baseline_subnets = await self._baseline_subnets(
            configured_nodes, proto, array_portals)

        new_hosts: list[dict[str, Any]] = []
        for node in nodes:
            if node in configured_nodes:
                continue  # already configured under one of its known names
            verdict = await self._score_node(
                node, proto, array_portals, baseline_subnets)
            new_hosts.append({"node": node.name, "host": primary[node.name],
                              "ready": verdict["ready"],
                              "reasons": verdict["reasons"]})

        not_ready = [h["node"] for h in new_hosts if not h["ready"]]
        msg = (f"VME cluster: {len(nodes)} node(s), {len(new_hosts)} new, "
               f"{len(departed)} departed")
        if not_ready:
            msg += f"; {len(not_ready)} new node(s) not ready"
        return OpResult.ok(msg, nodes=node_names, new_hosts=new_hosts,
                           departed_hosts=departed, not_ready=not_ready)

    async def _baseline_subnets(
        self, configured_nodes: "list[ClusterNode]",
        proto: str, array_portals: list[str],
    ) -> "set[str]":
        """Storage-NIC subnets of an ALREADY-configured node (readiness baseline).

        For IP transports, takes the first already-configured (group-member) node
        and returns the set of its NIC subnets that reach the array portals, so new
        nodes can be required to share a storage subnet with existing nodes. Empty
        (=> no baseline constraint) when nothing is configured yet, for FC, or when
        portals are unknown.
        """
        if proto not in ("iscsi", "nvme-tcp") or not array_portals:
            return set()
        import ipaddress

        portals = []
        for p in array_portals:
            try:
                portals.append(ipaddress.ip_address(p))
            except ValueError:
                continue
        for node in configured_nodes:
            nics = await self._host_storage_nics(node.host)
            subnets: set[str] = set()
            for nic in nics:
                net = _nic_network(nic)
                if net and any(ip in net for ip in portals):
                    subnets.add(str(net))
            if subnets:
                return subnets
        return set()

    async def _host_storage_nics(self, host: str) -> list[dict[str, Any]]:
        """Discover a node's IP NICs (for readiness scoring). Mock-safe."""
        return await self.ctx.runner.discover_interfaces(
            host, "nics", **self._ssh_kwargs())

    async def _score_node(
        self, node: "ClusterNode", proto: str, array_portals: list[str],
        baseline_subnets: "set[str]",
    ) -> dict[str, Any]:
        """Gather inputs and run :meth:`score_host_readiness` for one new node."""
        # reachable: discover_initiators succeeds (mock-safe -> synthetic values).
        reachable = True
        initiators: dict[str, Any] = {}
        try:
            initiators = await self._discover_initiators_on(node.host)
        except Exception:   # noqa: BLE001 - unreachable host
            reachable = False
        host_nics: list[dict[str, Any]] = []
        if proto in ("iscsi", "nvme-tcp"):
            try:
                host_nics = await self._host_storage_nics(node.host)
            except Exception:   # noqa: BLE001
                reachable = False
        return self.score_host_readiness(
            proto, reachable=reachable, initiators=initiators,
            host_nics=host_nics, array_portals=array_portals,
            baseline_subnets=baseline_subnets or None)

    async def reconcile_cluster(self, apply_removals: bool = False,
                                **params: Any) -> OpResult:
        """Configure READY new VME KVM nodes; optionally remove departed FA hosts.

        Runs :meth:`assess_cluster`, then:

        1. Configures ONLY the new nodes that pass the readiness preflight, by
           re-running :meth:`register_hosts` + :meth:`setup_connectivity` with the
           ready-new node subset (so existing nodes are untouched and non-ready new
           nodes are never half-configured -- they are reported in ``not_ready``).
        2. Prunes departed FA hosts via :meth:`_prune_departed_fa_hosts`: with
           ``apply_removals=False`` (default) they are only flagged
           (``pending_removals``); with ``True`` they are removed + reported in
           ``removed``.

        Returns ``data["nodes"]`` (current membership) so the monitor's baseline
        updates. Mock/dry-run safe.
        """
        proto = self._protocol()
        host_group = self._default_host_group()
        assessment = await self.assess_cluster(**params)
        data = assessment.data or {}
        nodes_now = data.get("nodes", [])
        new_hosts = data.get("new_hosts", [])
        departed = data.get("departed_hosts", [])

        if proto == "nfs" or not host_group or self.ctx.array is None:
            return OpResult.ok(
                f"No block reconcile for protocol={proto} (host_group={host_group!r})",
                nodes=nodes_now, new_hosts=new_hosts, configured=[],
                not_ready=[], departed_hosts=departed, pending_removals=departed)

        ready = [h for h in new_hosts if h.get("ready")]
        not_ready = [h["node"] for h in new_hosts if not h.get("ready")]

        configured: list[str] = []
        if ready:
            all_nodes = await self.list_nodes()
            by_name = {n.name: n for n in all_nodes}
            ready_nodes = [by_name[h["node"]] for h in ready if h["node"] in by_name]
            if ready_nodes:
                await self.ctx.emit(
                    f"Configuring {len(ready_nodes)} ready new node(s): "
                    f"{', '.join(n.name for n in ready_nodes)}")
                await self.register_hosts(host_group=host_group, nodes=ready_nodes)
                await self.setup_connectivity(host_group=host_group,
                                              nodes=ready_nodes)
                configured = [n.name for n in ready_nodes]

        # Departed FA hosts: flag (default) or remove. "Expected" must include
        # EVERY name a current node could be registered under (not just the
        # per-node convention) -- otherwise a host the array names by bare hostname
        # is wrongly flagged departed, and apply_removals would DELETE an in-use FA
        # host.
        all_nodes = await self.list_nodes()
        expected_hosts: set[str] = set()
        for n in all_nodes:
            expected_hosts |= self._fa_host_name_candidates(host_group, n)
        prune = await self._prune_departed_fa_hosts(
            host_group, expected_hosts, apply_removals=apply_removals)

        result: dict[str, Any] = {
            "nodes": nodes_now, "new_hosts": new_hosts, "configured": configured,
            "not_ready": not_ready, "departed_hosts": prune["departed"]}
        if apply_removals:
            result["removed"] = prune["removed"]
        else:
            result["pending_removals"] = prune["departed"]

        msg = (f"Reconcile: configured {len(configured)} node(s)"
               + (f", skipped {len(not_ready)} not-ready" if not_ready else "")
               + (f", removed {len(prune['removed'])} departed" if apply_removals
                  else f", {len(prune['departed'])} departed pending"))
        return OpResult.ok(msg, **result)

    # ------------------------------------------------- NFS datastore (images) ---
    # NOTE: NOT yet hardware-validated against FlashArray File. This is the file
    # (datastore) storage model -- distinct from the per-disk block plugin. A
    # FlashArray File NFS export holds QCOW2 VM images and is mounted on the KVM
    # hosts, then registered as a VME 'nfs' datastore.
    async def provision_nfs_datastore(self, name: str, export_path: str = "/",
                                      mountpoint: str = "", **_: Any) -> OpResult:
        """Create an FA File NFS export, mount it on the KVM hosts, register in VME.

        Steps (mock/dry-run safe):
          1. ``ctx.array.create_filesystem(name)`` + ``create_nfs_export(...)``;
          2. discover the NFS portal (``ctx.array.get_nfs_data_interfaces()``);
          3. mount ``<portal>:<export_path> <mountpoint>`` on each VME KVM host;
          4. register a VME 'nfs' datastore (``POST /api/data-stores``) for QCOW2
             VM images.

        # TODO(validate-on-appliance): the NFS / FlashArray File datastore path is
        # NOT hardware-validated (only the iSCSI block path was). Confirm against
        # FlashArray File and the exact VME 'nfs' datastore payload.
        """
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        export = f"vme-{self._fa_name(name)}-export"
        mountpoint = mountpoint or f"/mnt/{self._fa_name(name)}"
        await self.ctx.emit(
            f"[nfs datastore] creating FA File system {name!r} + export {export!r} "
            f"for QCOW2 VM images (NOT hardware-validated)")

        if self.ctx.dry_run:
            return OpResult.ok(
                f"[dry-run] would provision NFS datastore {name}",
                artifacts={"datastore": name, "filesystem": name, "export": export,
                           "mountpoint": mountpoint, "type": "nfs",
                           "hardware_validated": False})

        # 1. FA File system + NFS export.
        await self.ctx.array.create_filesystem(name)
        await self.ctx.array.create_nfs_export(export, name, export_path)
        # 2. NFS portal.
        portals = await self.ctx.array.get_nfs_data_interfaces()
        portal = portals[0] if portals else ""
        await self.ctx.emit(f"FA File NFS portal: {portal!r}")
        # 3. Mount the export on every VME KVM host (datastore is shared).
        nodes = await self.list_nodes()
        for node in nodes:
            self._ssh_host_override = node.host
            try:
                await self._mount_nfs(portal, export_path, mountpoint)
            finally:
                self._ssh_host_override = None
        # 4. Register a VME 'nfs' datastore for QCOW2 VM images.
        url = self._manager_url()
        token = await self._authenticate()
        resp = await self.ctx.runner.run_http(
            "POST", f"{url}{_API_BASE}/data-stores",
            headers=self._auth_headers(token),
            json_body={
                "datastore": {
                    "name": name,
                    "type": "nfs",
                    "nfsServer": portal,
                    "nfsPath": export_path,
                    "content": "images",
                }
            },
        )
        ds_id = (resp.get("json") or {}).get("datastore", {}).get("id")
        return OpResult.ok(
            f"NFS datastore {name} provisioned for QCOW2 VM images "
            f"(NOT hardware-validated)",
            artifacts={"datastore": name, "filesystem": name, "export": export,
                       "mountpoint": mountpoint, "portal": portal, "type": "nfs",
                       "vme_datastore_id": ds_id, "hardware_validated": False,
                       "nodes": [n.name for n in nodes]})

    async def _mount_nfs(self, portal: str, export_path: str, mountpoint: str) -> None:
        """Mount an NFS export on the host currently targeted by ``_ssh_host_override``.

        Adds an /etc/fstab entry (idempotent) and mounts it. Mock-safe / dry-run
        no-op (the SSH helper skips real I/O).
        """
        if self.ctx.dry_run:
            return
        src = f"{portal}:{export_path}"
        fstab = f"{src} {mountpoint} nfs _netdev,vers=3 0 0"
        await self._ssh(f"mkdir -p {mountpoint}")
        await self._ssh(
            f"grep -qsF {mountpoint!r} /etc/fstab || "
            f"echo {fstab!r} >> /etc/fstab")
        await self._ssh(
            f"mountpoint -q {mountpoint} || mount -t nfs {src} {mountpoint}")

    # ----------------------------------------- ISO storage SPECIAL CASE (NFS) ---
    async def setup_iso_storage(self, name: str, mountpoint: str = "",
                                export_path: str = "/", **_: Any) -> OpResult:
        """ISO datastore workaround: kernel-NFS mount on the VME MANAGER host.

        WHY THIS IS A SEPARATE ACTION (not provision_nfs_datastore):
        ------------------------------------------------------------
        HPE VME's BUILT-IN NFS CLIENT for ISO storage is a **Java** client that
        opens its NFS sessions from **HIGH (unprivileged, >1024) SOURCE PORTS**. The
        FlashArray REJECTS NFS traffic from high source ports, so a normal VME 'nfs'
        datastore for ISOs FAILS to mount/serve. The workaround:

          1. Create an FA File system + NFS export for ISOs and get the NFS portal.
          2. SSH to the **VME MANAGER host** (NOT the KVM hosts) and mount the export
             with the OS **kernel** NFS client (fstab entry + ``mount -t nfs``). The
             kernel client uses **privileged (<1024) source ports**, which the
             FlashArray ALLOWS -- sidestepping the high-source-port problem.
          3. Register that OS mount path in VME as a **'directory' / local-directory
             datastore** (NOT an 'nfs'-type datastore), so VME serves ISOs from the
             OS mount instead of going through its blocked Java NFS client.

        Mock/dry-run safe.

        # TODO(validate-on-appliance): this FlashArray File / kernel-NFS ISO path is
        # NOT hardware-validated (only the iSCSI block path was). Confirm the VME
        # Manager-host SSH credentials and the exact 'directory' datastore payload.
        """
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        export = f"vme-{self._fa_name(name)}-iso-export"
        mountpoint = mountpoint or f"/mnt/{self._fa_name(name)}"
        manager_host = self._manager_ssh_host()
        await self.ctx.emit(
            f"[iso storage] FA File export {export!r} -> kernel-NFS mount on the "
            f"VME MANAGER host {manager_host!r} (VME's Java NFS client uses high "
            f"source ports the array blocks; the OS kernel mount uses privileged "
            f"ports the array allows)")

        if self.ctx.dry_run:
            return OpResult.ok(
                f"[dry-run] would set up ISO storage {name} (kernel-NFS workaround)",
                artifacts={"datastore": name, "filesystem": name, "export": export,
                           "mountpoint": mountpoint, "manager_host": manager_host,
                           "datastore_type": "directory", "hardware_validated": False})

        # 1. FA File system + NFS export for ISOs.
        await self.ctx.array.create_filesystem(name)
        await self.ctx.array.create_nfs_export(export, name, export_path)
        portals = await self.ctx.array.get_nfs_data_interfaces()
        portal = portals[0] if portals else ""
        # 2. Kernel NFS mount on the VME MANAGER host (privileged source ports).
        self._ssh_host_override = manager_host
        try:
            await self._mount_nfs(portal, export_path, mountpoint)
        finally:
            self._ssh_host_override = None
        # 3. Register a VME 'directory' (local) datastore over the OS mount -- NOT
        #    an 'nfs' datastore (which would route ISO traffic through VME's blocked
        #    Java NFS client again).
        url = self._manager_url()
        token = await self._authenticate()
        resp = await self.ctx.runner.run_http(
            "POST", f"{url}{_API_BASE}/data-stores",
            headers=self._auth_headers(token),
            json_body={
                "datastore": {
                    "name": name,
                    "type": "directory",   # local-directory, NOT 'nfs'
                    "directoryPath": mountpoint,
                    "content": "iso",
                }
            },
        )
        ds_id = (resp.get("json") or {}).get("datastore", {}).get("id")
        return OpResult.ok(
            f"ISO storage {name} set up via kernel-NFS mount on the VME Manager host "
            f"+ a VME 'directory' datastore (NOT hardware-validated)",
            artifacts={"datastore": name, "filesystem": name, "export": export,
                       "mountpoint": mountpoint, "portal": portal,
                       "manager_host": manager_host, "datastore_type": "directory",
                       "vme_datastore_id": ds_id, "hardware_validated": False})

    async def teardown_iso_storage(self, name: str, mountpoint: str = "",
                                   eradicate: bool = False, **_: Any) -> OpResult:
        """Tear down ISO storage: unmount on the Manager host, remove fstab, drop FA.

        Reverses :meth:`setup_iso_storage` -- unmounts the export on the VME Manager
        host, removes its /etc/fstab entry, deletes the NFS export, and deletes (or
        eradicates) the FlashArray file system. Mock/dry-run safe.
        """
        export = f"vme-{self._fa_name(name)}-iso-export"
        mountpoint = mountpoint or f"/mnt/{self._fa_name(name)}"
        manager_host = self._manager_ssh_host()
        await self.ctx.emit(
            f"[iso storage] tearing down {name!r}: unmount {mountpoint!r} on "
            f"{manager_host!r}, remove fstab, delete export + file system")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would tear down ISO storage {name}",
                               artifacts={"datastore": name, "filesystem": name})

        self._ssh_host_override = manager_host
        try:
            await self._ssh(f"umount {mountpoint} 2>/dev/null || true")
            # Drop our fstab entry (matches the mountpoint). Use a '#' sed delimiter
            # so the mountpoint's '/' chars don't need escaping.
            sed_expr = "\\#" + mountpoint + "#d"
            await self._ssh(
                f"sed -i {sed_expr!r} /etc/fstab 2>/dev/null || true")
        finally:
            self._ssh_host_override = None

        if self.ctx.array is not None:
            await self.ctx.array.delete_nfs_export(export)
            await self.ctx.array.delete_filesystem(name, eradicate=eradicate)
        return OpResult.ok(
            f"ISO storage {name} torn down",
            artifacts={"datastore": name, "filesystem": name, "export": export,
                       "mountpoint": mountpoint, "eradicated": bool(eradicate)})

    def _manager_ssh_host(self) -> str:
        """SSH target for the VME MANAGER host (where kernel-NFS ISO mounts live).

        Distinct from :meth:`_kvm_host` (the KVM compute hosts). Defaults to the
        manager URL's hostname; an explicit ``manager_ssh_host`` overrides it.
        """
        explicit = self.ctx.target.get("manager_ssh_host")
        if explicit:
            return str(explicit)
        url = self._manager_url()
        # Strip scheme + any path/port to leave the bare hostname.
        host = url.split("://", 1)[-1].split("/", 1)[0].split(":", 1)[0]
        return host or self._kvm_host()

    # ---- dispatch: route the connector's bespoke action ids ----
    async def dispatch(self, action_id: str, params: dict[str, Any]) -> OpResult:
        if action_id == "provision_nfs_datastore":
            return await self.provision_nfs_datastore(**params)
        if action_id == "setup_iso_storage":
            return await self.setup_iso_storage(**params)
        if action_id == "teardown_iso_storage":
            return await self.teardown_iso_storage(**params)
        # assess_cluster / reconcile_cluster are routed by the base dispatch.
        return await super().dispatch(action_id, params)

    # ----------------------------------------------------- datastore fallback ---
    async def register_datastore_fallback(self, datastore: str, backing: str = "",
                                          **_: Any) -> OpResult:
        """ALTERNATE MODEL: register an Everpure-backed datastore in VME.

        This is the model Everpure **officially documents/supports** for VME (a
        FlashArray volume presented to all hosts and formatted as a GFS2 Pool over
        iSCSI, or an NFS export) -- as opposed to this connector's primary
        per-VM-disk attach. Retained as a helper; not wired into ``CAPABILITIES`` /
        ``action_schemas`` so the UI advertises only the per-disk model.

        CONFIRMED path: ``POST /api/data-stores`` (hyphenated) with a ``datastore``
        object whose ``storageServer.id`` links a storage server. NOTE: API-driven
        datastore *creation* is restricted in Morpheus, and there is no Everpure
        storage-server type -- so on a live appliance the GFS2 Pool is typically
        created through the VME console UI over the Everpure multipath device, not this
        call. # TODO(validate-on-appliance): the API-driven datastore-pool payload
        is NOT hardware-validated (only the per-VM-disk iSCSI block path was);
        confirm the exact data-store payload accepted by VME.
        """
        url = self._manager_url()
        proto = self._protocol()
        backing = backing or datastore
        await self.ctx.emit(
            f"[datastore model] registering VME datastore {datastore!r} ({proto}) "
            f"backed by {backing!r}"
        )
        if self.ctx.array is not None and not self.ctx.dry_run:
            await self.ctx.array.create_volume(backing, "1T")
        if self.ctx.dry_run:
            return OpResult.ok(f"[dry-run] would register datastore {datastore}",
                               artifacts={"datastore": datastore})
        token = await self._authenticate()
        resp = await self.ctx.runner.run_http(
            "POST", f"{url}{_API_BASE}/data-stores",
            headers=self._auth_headers(token),
            json_body={
                "datastore": {
                    "name": datastore,
                    "type": "block",
                    "externalId": backing,
                }
            },
        )
        ds_id = (resp.get("json") or {}).get("datastore", {}).get("id")
        return OpResult.ok(f"Datastore {datastore} registered in VME (datastore model)",
                           artifacts={"datastore": datastore, "volume": backing,
                                      "vme_datastore_id": ds_id})
