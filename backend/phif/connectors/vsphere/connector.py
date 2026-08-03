"""VMware vSphere connector for PHIF.

Integration wrapped
-------------------
* **Everpure Data vSphere Client Remote Plugin** — registered with vCenter so the
  VMware admin can drive FlashArray storage from the vSphere Client. As of
  vSphere 8.0 only *remote* plugins are supported; the plugin is registered via
  the vCenter Extension Manager / REST API.
* **VASA Provider** — the Everpure VASA provider (endpoint ``https://<array>:8084``)
  is registered with vCenter as a Storage Provider to enable Virtual Volumes
  (vVols). Two endpoints (CT0/CT1) are registered for HA on real arrays.
* **VMFS / NFS / vVol datastores**, array hosts/host-groups, volumes, snapshots,
  clones, resize, QoS and replication (protection groups).

Runtime surfaces
----------------
* pyVmomi (SOAP) — used for all vCenter inventory and VM management operations.
  Synchronous pyVmomi calls are wrapped with ``asyncio.to_thread`` to keep
  the connector's public interface async.  The ServiceInstance is cached on
  ``self._vsphere_si``.
* All vCenter operations use pyVmomi exclusively — no REST API.
* Array-side host registration — ``purestorage.flasharray`` Ansible playbook in
  ``ansible/vsphere/`` (``ctx.runner.run_ansible``).
* All other array ops — ``ctx.array`` (FlashArrayClient).

Heavy SDKs (pyVmomi) are imported lazily inside sync helpers so mock-mode tests
pass without them installed.
"""

from __future__ import annotations

import asyncio
import os
import time
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
)

# Everpure VASA provider control-plane port (per Everpure vVols user guide).
VASA_PORT = 8084
# Path to the array-side host-registration playbook shipped with this connector.
_ANSIBLE_DIR = os.path.normpath(
    os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "ansible", "vsphere")
)
HOST_REGISTER_PLAYBOOK = os.path.join(_ANSIBLE_DIR, "register_hosts.yml")


class VSphereConnector(HypervisorConnector):
    # --- static metadata ---
    key = "vsphere"
    name = "VMware vSphere"
    description = (
        "Everpure Data integration for VMware vSphere: registers the vSphere "
        "Client Remote Plugin and the VASA Provider (vVols), and manages VMFS/"
        "NFS/vVol datastores, FlashArray hosts, volumes, snapshots, clones, "
        "QoS and replication."
    )
    maturity = "ga"
    CAPABILITIES = {
        Capability.DEPLOY_PLUGIN,
        Capability.CONFIGURE,
        Capability.HOST_REGISTER,
        Capability.CONNECTIVITY,
        Capability.PROVISION_DATASTORE,
        Capability.PROVISION_VOLUME,
        Capability.SNAPSHOT,
        Capability.CLONE,
        Capability.RESIZE,
        Capability.QOS,
        Capability.REPLICATION,
        Capability.HEALTH,
        Capability.REMOVE,
        Capability.VM_INVENTORY,
        Capability.VM_LIFECYCLE,
        Capability.MIGRATE,
    }

    # Everpure FlashArray SCSI OUI used to derive ESXi NAA device names from serials.
    _PURE_SCSI_OUI = "624a9370"
    SUPPORTED_PROTOCOLS = {
        Protocol.ISCSI,
        Protocol.FC,
        Protocol.NVME_FC,
        Protocol.NVME_ROCE,
    }

    # ------------------------------------------------------------------ UI ---
    @classmethod
    def target_schema(cls) -> list[FormField]:
        return [
            FormField("vcenter_host", "vCenter host / IP", FieldType.STRING,
                      placeholder="vcenter.example.com"),
            FormField("vcenter_user", "vCenter username", FieldType.STRING,
                      default="administrator@vsphere.local"),
            FormField("vcenter_password", "vCenter password", FieldType.SECRET),
            FormField("datacenter", "Datacenter", FieldType.STRING, required=False,
                      help="Optional default datacenter for datastore operations."),
            FormField("cluster", "Cluster", FieldType.STRING, required=False,
                      help="Optional default cluster for datastore/host operations."),
            FormField("host_group", "FlashArray host group", FieldType.STRING,
                      required=False,
                      help="FA host group the ESXi cluster belongs to. Used as the "
                           "default for register_hosts / setup_connectivity when "
                           "their host_group field is left blank."),
            FormField("esxi_user", "ESXi host SSH username", FieldType.STRING,
                      required=False, default="root",
                      help="Used only for VM migration FROM vSphere: cloning a "
                           "VMFS disk onto a FlashArray volume requires vmkfstools "
                           "on the ESXi host over SSH."),
            FormField("esxi_password", "ESXi host SSH password", FieldType.SECRET,
                      required=False,
                      help="Root password for the ESXi hosts (typically uniform "
                           "across the cluster). Required only to migrate VMs with "
                           "VMFS-backed disks off vSphere."),
        ]

    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        return [
            ActionSpec(
                Capability.DEPLOY_PLUGIN, "deploy", "Deploy vSphere plugin + VASA",
                "Register the Everpure vSphere Client Remote Plugin and the VASA "
                "Provider (vVols) with vCenter. The plugin containers must already "
                "be running on the PHIF server (docker compose --profile vsphere up -d). "
                "Enter the PHIF server's externally-reachable hostname or IP; the "
                "plugin proxy listens on port 9443.",
                fields=[
                    FormField("phif_host", "PHIF server host / IP", FieldType.STRING,
                              required=False,
                              placeholder="phif.example.com",
                              help="Hostname or IP of the PHIF server running the "
                                   "vSphere plugin containers. Used to construct "
                                   "https://<host>:9443 as the plugin manifest URL. "
                                   "Leave blank only if supplying 'plugin_url' directly."),
                    FormField("plugin_url", "Remote plugin manifest URL (override)",
                              FieldType.STRING, required=False,
                              placeholder="https://<plugin-host>:9443/plugin-manifest.zip",
                              help="Overrides the auto-constructed URL. Use only if "
                                   "the plugin proxy is not on the default port 9443."),
                    FormField("register_vasa", "Register VASA provider", FieldType.BOOL,
                              required=False, default=True),
                ],
            ),
            ActionSpec(
                Capability.CONFIGURE, "configure", "Register / refresh VASA provider",
                "(Re)register or refresh the Everpure VASA Storage Provider on vCenter "
                "for vVols.",
                fields=[
                    FormField("provider_name", "Storage provider name", FieldType.STRING,
                              required=False, placeholder="Everpure-VASA-<array>"),
                ],
            ),
            ActionSpec(
                Capability.HOST_REGISTER, "register_hosts", "Register ESXi hosts on array",
                "Create a FlashArray host group for the ESXi cluster. Initiators are "
                "AUTO-DISCOVERED from vCenter per transport (iSCSI->IQNs, Fibre "
                "Channel->WWNs, NVMe-oF->NQNs); leave the address fields blank to use "
                "discovery, or supply them explicitly to override.",
                fields=[
                    FormField("host_group", "Host group name", FieldType.STRING,
                              required=False,
                              help="Defaults to the host group set on the hypervisor "
                                   "connection if left blank."),
                    FormField("protocol", "Transport", FieldType.ENUM, default="iscsi",
                              options=["iscsi", "fc", "nvme-fc", "nvme-roce"]),
                    FormField("hosts", "ESXi host names (comma-separated)", FieldType.STRING,
                              required=False),
                    FormField("iqns", "iSCSI IQNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="Optional (auto-discovered from vCenter if left blank)."),
                    FormField("wwns", "FC WWNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="Optional (auto-discovered from vCenter if left blank). "
                                   "ESXi HBA port WWNs must already be zoned to the array "
                                   "on the SAN."),
                    FormField("nqns", "NVMe NQNs (comma-separated)", FieldType.STRING,
                              required=False,
                              help="Optional (auto-discovered from vCenter if left blank)."),
                ],
            ),
            ActionSpec(
                Capability.CONNECTIVITY, "setup_connectivity", "Set up host connectivity",
                "Create FlashArray hosts for the chosen transport (iSCSI/FC/NVMe-oF) "
                "and group them.",
                fields=[
                    FormField("host_group", "Host group name", FieldType.STRING,
                              required=False,
                              help="Defaults to the host group set on the hypervisor "
                                   "connection if left blank."),
                    FormField("protocol", "Transport", FieldType.ENUM, default="iscsi",
                              options=["iscsi", "fc", "nvme-fc", "nvme-roce"]),
                    FormField("hosts", "ESXi host names (comma-separated)", FieldType.STRING),
                    FormField("initiators", "Initiators per host (host=addr,addr;...)",
                              FieldType.TEXT, required=False,
                              help="Optional (auto-discovered from vCenter if left blank). "
                                   "e.g. esx1=iqn.a,iqn.b;esx2=iqn.c"),
                    # --- interface binding (iSCSI port binding / HBA selection) ---
                    FormField("iscsi_vmknics", "iSCSI port-binding VMkernel NICs",
                              FieldType.MULTISELECT, required=False,
                              options_source="nics",
                              help="VMkernel NICs for iSCSI port binding. Binds the ESXi "
                                   "iSCSI software adapter to these VMkernel adapters "
                                   "(esxcli iscsi networkportal). Choices are discovered "
                                   "from vCenter."),
                    FormField("fc_hbas", "FC HBAs to use",
                              FieldType.MULTISELECT, required=False,
                              options_source="fc_hbas",
                              help="Fibre Channel HBAs on the ESXi hosts to use for this "
                                   "transport. Choices are discovered from vCenter."),
                    FormField("nvme_adapters", "NVMe-oF adapters",
                              FieldType.MULTISELECT, required=False,
                              options_source="nvme_sources",
                              help="NVMe-oF adapters / vmknics to use for NVMe-oF. "
                                   "Choices are discovered from vCenter."),
                ],
            ),
            ActionSpec(
                Capability.PROVISION_DATASTORE, "provision_datastore", "Provision datastore",
                "Create a FlashArray volume, connect it to the ESXi host group, rescan "
                "and create a VMFS datastore (or attach an NFS export).",
                fields=[
                    FormField("name", "Datastore name", FieldType.STRING),
                    FormField("size", "Size", FieldType.SIZE, default="1T"),
                    FormField("host_group", "ESXi host group", FieldType.STRING),
                    FormField("type", "Datastore type", FieldType.ENUM, default="vmfs",
                              options=["vmfs", "nfs", "vvol"]),
                    FormField("protocol", "Transport (block datastores)", FieldType.ENUM,
                              default="iscsi", required=False,
                              options=["iscsi", "fc", "nvme-fc", "nvme-roce"],
                              help="Block transport used to surface the LUN to ESXi. For "
                                   "fc the LUN appears via SAN zoning + HBA rescan (no IP "
                                   "target/iSCSI login)."),
                    FormField("cluster", "vCenter cluster", FieldType.STRING, required=False),
                    FormField("nfs_server", "NFS server (NFS only)", FieldType.STRING,
                              required=False),
                    FormField("nfs_path", "NFS export path (NFS only)", FieldType.STRING,
                              required=False),
                ],
            ),
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision", "Provision volume",
                "Create a FlashArray volume and optionally connect it to a host group.",
                fields=[
                    FormField("name", "Volume name", FieldType.STRING),
                    FormField("size", "Size", FieldType.SIZE, default="1T"),
                    FormField("host_group", "Attach to host group", FieldType.STRING,
                              required=False),
                ],
            ),
            ActionSpec(
                Capability.SNAPSHOT, "snapshot", "Snapshot volume",
                fields=[FormField("volume", "Volume name", FieldType.STRING),
                        FormField("suffix", "Snapshot suffix", FieldType.STRING,
                                  required=False)]),
            ActionSpec(
                Capability.CLONE, "clone", "Clone volume",
                fields=[FormField("source", "Source volume", FieldType.STRING),
                        FormField("dest", "New volume name", FieldType.STRING),
                        FormField("host_group", "Attach clone to host group",
                                  FieldType.STRING, required=False)]),
            ActionSpec(
                Capability.RESIZE, "resize", "Resize datastore / volume",
                "Extend the FlashArray volume and grow the backing datastore.",
                fields=[FormField("volume", "Volume name", FieldType.STRING),
                        FormField("size", "New size", FieldType.SIZE),
                        FormField("datastore", "Datastore to grow", FieldType.STRING,
                                  required=False)]),
            ActionSpec(
                Capability.QOS, "set_qos", "Set volume QoS",
                fields=[FormField("volume", "Volume name", FieldType.STRING),
                        FormField("iops_limit", "IOPS limit", FieldType.INT, required=False),
                        FormField("bw_limit", "Bandwidth limit (bytes/s)", FieldType.INT,
                                  required=False)]),
            ActionSpec(
                Capability.REPLICATION, "configure_replication", "Configure replication",
                "Create a FlashArray protection group for the listed volumes.",
                fields=[FormField("name", "Protection group name", FieldType.STRING),
                        FormField("volumes", "Volumes (comma-separated)", FieldType.STRING)]),
            ActionSpec(Capability.HEALTH, "health_check", "Health check", long_running=False),
            ActionSpec(Capability.REMOVE, "teardown", "Remove integration",
                       "Unregister the VASA provider and the remote plugin from vCenter.",
                       destructive=True),
        ]

    @classmethod
    def wizard_steps(cls) -> list[str]:
        """Ordered action ids the deployment wizard runs end-to-end for vSphere.

        Cluster-wide steps (``deploy`` = remote plugin + VASA, ``configure`` =
        VASA refresh, ``provision_datastore``) run once against vCenter; the
        per-host steps (``register_hosts``, ``setup_connectivity``) fan out across
        the cluster's ESXi hosts. Only ids that exist in ``action_schemas`` are
        listed (the orchestrator skips unknown ids regardless).
        """
        order = ["deploy", "configure", "register_hosts",
                 "setup_connectivity", "provision_datastore"]
        known = {a.id for a in cls.action_schemas()}
        return [step for step in order if step in known]

    # ------------------------------------------------ pyVmomi ServiceInstance cache ---

    def _si_sync(self):
        """Return a cached pyVmomi ServiceInstance, connecting if needed.

        Refuses to connect under mock mode or dry-run. pyVmomi talks to vCenter
        directly rather than through ``ctx.runner``, so without this guard a
        mock-mode run would open a real TLS session to whatever ``vcenter_host``
        happens to be set to. Callers that need to work offline must branch on
        ``_mock_or_dry()`` before reaching here; this raise is the backstop that
        keeps the mock-mode contract honest instead of silently doing real I/O.
        """
        if self._mock_or_dry():
            raise ConnectionValidationError(
                "pyVmomi access is disabled in mock/dry-run mode "
                "(no live vCenter connection is attempted)")
        if not hasattr(self, "_vsphere_si"):
            import ssl
            from pyVim.connect import SmartConnect
            host = self.ctx.target.get("vcenter_host") or ""
            user = self.ctx.target.get("vcenter_user") or ""
            pwd = self.ctx.target.get("vcenter_password") or ""
            if not host:
                raise ConnectionValidationError("No vcenter_host configured")
            ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            self._vsphere_si = SmartConnect(
                host=host, user=user, pwd=pwd, sslContext=ctx
            )
        return self._vsphere_si

    def _find_all_sync(self, vim_types, container=None):
        """Return all managed objects of the given vim types via a ContainerView."""
        content = self._si_sync().content
        root = container or content.rootFolder
        view = content.viewManager.CreateContainerView(root, vim_types, True)
        try:
            return list(view.view)
        finally:
            view.Destroy()

    def _find_one_sync(self, vim_types, name, container=None):
        """Return the first managed object whose .name matches, or None."""
        for obj in self._find_all_sync(vim_types, container):
            if obj.name == name:
                return obj
        return None

    def _get_vm_sync(self, vm_ref):
        """Return a VirtualMachine object by moId or name, raising if not found."""
        from pyVmomi import vim
        for vm in self._find_all_sync([vim.VirtualMachine]):
            if vm._moId == vm_ref or vm.name == vm_ref:
                return vm
        raise ValueError(f"VM {vm_ref!r} not found in vCenter")

    def _wait_task_sync(self, task, timeout=600):
        """Poll a pyVmomi Task until success/error/timeout."""
        from pyVmomi import vim
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            state = task.info.state
            if state == vim.TaskInfo.State.success:
                return task.info.result
            if state == vim.TaskInfo.State.error:
                err = task.info.error
                raise RuntimeError(f"vCenter task failed: {getattr(err, 'msg', str(err))}")
            time.sleep(3)
        raise RuntimeError(f"Task timed out after {timeout}s")

    def _default_resource_pool_sync(self):
        """Return the default resource pool: cluster's pool if cluster configured,
        else first non-root pool, else first pool found."""
        from pyVmomi import vim
        cluster_name = self.ctx.target.get("cluster") or ""
        if cluster_name:
            cluster = self._find_one_sync([vim.ClusterComputeResource], cluster_name)
            if cluster:
                return cluster.resourcePool
        pools = self._find_all_sync([vim.ResourcePool])
        # Skip root resource pools (their parent is ComputeResource, not ResourcePool)
        for p in pools:
            if hasattr(p.parent, "resourcePool"):
                return p
        return pools[0] if pools else None

    def _default_vm_folder_sync(self):
        """Return the VM folder for the configured datacenter, or rootFolder."""
        from pyVmomi import vim
        dc_name = self.ctx.target.get("datacenter") or ""
        dcs = self._find_all_sync([vim.Datacenter])
        if dc_name:
            for dc in dcs:
                if dc.name == dc_name:
                    return dc.vmFolder
        return dcs[0].vmFolder if dcs else self._si_sync().content.rootFolder

    # ------------------------------------------------------ FlashArray side ---
    def _array_host(self) -> str:
        """Resolve the FlashArray management host from the associated array."""
        endpoint = (getattr(self.ctx.array, "endpoint", "") or "") if self.ctx.array else ""
        if not endpoint:
            return self.ctx.target.get("array_host") or self.ctx.target.get("vcenter_host", "array")
        host = endpoint
        if "://" in host:
            host = host.split("://", 1)[1]
        host = host.split("/", 1)[0]
        return host

    # ----------------------------------------------- initiator discovery ---
    def _initiator_kind_for(self, protocol: str) -> str:
        if protocol in self._FC_PROTOCOLS:
            return "wwns"
        if protocol in self._NVME_PROTOCOLS:
            return "nqns"
        return "iqns"

    async def _discover_esxi_initiators(self, protocol: str) -> dict[str, list[str]]:
        """Auto-discover ESXi host storage-adapter initiators from vCenter via pyVmomi.

        Returns a dict ``{"iqns": [...], "wwns": [...], "nqns": [...]}`` collected
        across the ESXi hosts in the configured cluster/datacenter.
        """
        kind = self._initiator_kind_for(protocol)

        # Mock / dry-run: synthesize a small, realistic set so flows stay testable.
        if self.ctx.runner.mock or self.ctx.dry_run:
            await self.ctx.emit(
                f"[discovery] (mock/dry-run) synthesizing ESXi {kind} from vCenter"
            )
            synthetic = {
                "iqns": [f"iqn.1998-01.com.vmware:esxi{n:02d}" for n in (1, 2)],
                "wwns": ["21:00:00:24:ff:00:00:01", "21:00:00:24:ff:00:00:02"],
                "nqns": [f"nqn.2014-08.com.vmware:nvme:esxi{n:02d}" for n in (1, 2)],
            }
            return {kind: synthetic[kind]}

        def _collect_sync():
            from pyVmomi import vim
            cluster_name = self.ctx.target.get("cluster") or ""
            hosts = []
            if cluster_name:
                cluster = self._find_one_sync([vim.ClusterComputeResource], cluster_name)
                if cluster:
                    hosts = list(cluster.host)
            if not hosts:
                hosts = self._find_all_sync([vim.HostSystem])

            iqns: list[str] = []
            wwns: list[str] = []
            nqns: list[str] = []

            for host in hosts:
                try:
                    ss = host.configManager.storageSystem
                    hbas = (ss.storageDeviceInfo.hostBusAdapter
                            if ss and ss.storageDeviceInfo else [])
                    for hba in hbas:
                        if isinstance(hba, vim.host.InternetScsiHba):
                            if hba.iScsiName:
                                iqns.append(hba.iScsiName)
                        elif isinstance(hba, vim.host.FibreChannelHba):
                            raw = format(hba.portWorldWideName, "016x")
                            wwn = ":".join(raw[i:i+2] for i in range(0, 16, 2))
                            wwns.append(wwn)
                        elif isinstance(hba, vim.host.NvmeOverFibreChannelAdapter):
                            # NQN lives on the host config, not the HBA object
                            pass
                    # NVMe host NQN
                    try:
                        nvme_info = host.configManager.nvmeTopologySystem
                        if nvme_info and hasattr(nvme_info, "hostNqn") and nvme_info.hostNqn:
                            nqns.append(nvme_info.hostNqn)
                    except Exception:
                        pass
                except Exception:
                    continue

            # Deduplicate preserving order
            def _dedup(lst):
                seen: dict[str, None] = {}
                for v in lst:
                    seen.setdefault(v, None)
                return list(seen)

            return {"iqns": _dedup(iqns), "wwns": _dedup(wwns), "nqns": _dedup(nqns)}

        result = await asyncio.to_thread(_collect_sync)
        return {kind: result.get(kind, [])}

    @staticmethod
    def _parse_discovered_initiators(body: Any, kind: str) -> dict[str, list[str]]:
        """Best-effort extraction of initiators of ``kind`` from a vCenter reply."""
        found: list[str] = []
        aliases = {
            "iqns": ("iqns", "iqn", "iscsi_name", "iscsiName"),
            "wwns": ("wwns", "wwn", "port_wwn", "portWorldWideName"),
            "nqns": ("nqns", "nqn", "host_nqn", "hostNqn"),
        }[kind]

        def _collect(node: Any) -> None:
            if isinstance(node, dict):
                for key, val in node.items():
                    if key in aliases:
                        if isinstance(val, str):
                            found.append(val)
                        elif isinstance(val, (list, tuple)):
                            found.extend(str(v) for v in val if v)
                    else:
                        _collect(val)
            elif isinstance(node, (list, tuple)):
                for item in node:
                    _collect(item)

        _collect(body)
        seen: dict[str, None] = {}
        for v in found:
            seen.setdefault(v, None)
        return {kind: list(seen)}

    # ------------------------------------------- interface-binding discovery ---
    _SYNTHETIC_DISCOVERY: dict[str, list[dict[str, str]]] = {
        "nics": [
            {"value": "vmk1", "label": "vmk1 (iSCSI)"},
            {"value": "vmk2", "label": "vmk2 (iSCSI)"},
        ],
        "fc_hbas": [
            {"value": "vmhba1", "label": "vmhba1 (FC HBA 21:00:00:24:ff:00:00:01)"},
            {"value": "vmhba2", "label": "vmhba2 (FC HBA 21:00:00:24:ff:00:00:02)"},
        ],
        "nvme_sources": [
            {"value": "vmhba64", "label": "vmhba64 (NVMe-oF)"},
            {"value": "vmk3", "label": "vmk3 (NVMe-RoCE)"},
        ],
    }

    async def discover_options(self, kind: str) -> list[dict[str, Any]]:
        """Enumerate ESXi adapters for the interface-binding form fields via pyVmomi."""
        if kind == "initiators":
            return await self._discover_initiator_options()

        if kind not in self._SYNTHETIC_DISCOVERY:
            return []

        if self.ctx.runner.mock or self.ctx.dry_run:
            await self.ctx.emit(
                f"[discovery] (mock/dry-run) synthesizing ESXi adapters for {kind!r}"
            )
            return [dict(o) for o in self._SYNTHETIC_DISCOVERY[kind]]

        def _collect_sync():
            from pyVmomi import vim
            cluster_name = self.ctx.target.get("cluster") or ""
            hosts = []
            if cluster_name:
                cluster = self._find_one_sync([vim.ClusterComputeResource], cluster_name)
                if cluster:
                    hosts = list(cluster.host)
            if not hosts:
                hosts = self._find_all_sync([vim.HostSystem])
            if not hosts:
                return []

            host = hosts[0]
            out: list[dict[str, Any]] = []

            if kind == "nics":
                try:
                    ns = host.configManager.networkSystem
                    for vmk in (ns.networkInfo.vnic if ns and ns.networkInfo else []):
                        ip = ""
                        try:
                            ip = vmk.spec.ip.ipAddress or ""
                        except Exception:
                            pass
                        label = f"{vmk.device} ({ip})" if ip else vmk.device
                        out.append({"value": vmk.device, "label": label})
                except Exception:
                    pass

            elif kind == "fc_hbas":
                try:
                    ss = host.configManager.storageSystem
                    for hba in (ss.storageDeviceInfo.hostBusAdapter
                                if ss and ss.storageDeviceInfo else []):
                        if isinstance(hba, vim.host.FibreChannelHba):
                            out.append({"value": hba.device, "label": hba.device})
                except Exception:
                    pass

            elif kind == "nvme_sources":
                try:
                    ss = host.configManager.storageSystem
                    for hba in (ss.storageDeviceInfo.hostBusAdapter
                                if ss and ss.storageDeviceInfo else []):
                        if isinstance(hba, vim.host.NvmeOverFibreChannelAdapter):
                            out.append({"value": hba.device, "label": hba.device})
                except Exception:
                    pass
                # Also include NVMe-oF capable vmknics (best-effort)
                try:
                    ns = host.configManager.networkSystem
                    for vmk in (ns.networkInfo.vnic if ns and ns.networkInfo else []):
                        dev = vmk.device
                        # vmknics used for NVMe-RoCE are typically tagged; include all
                        # and let the operator pick
                        if not any(o["value"] == dev for o in out):
                            try:
                                ip = vmk.spec.ip.ipAddress or ""
                                out.append({"value": dev,
                                            "label": f"{dev} (NVMe-RoCE {ip})" if ip
                                            else f"{dev} (NVMe-RoCE)"})
                            except Exception:
                                pass
                except Exception:
                    pass

            return out

        try:
            return await asyncio.to_thread(_collect_sync)
        except Exception:
            return [dict(o) for o in self._SYNTHETIC_DISCOVERY.get(kind, [])]

    @staticmethod
    def _parse_discovered_adapters(body: Any) -> list[dict[str, Any]]:
        """Best-effort extraction of adapter options from a vCenter reply."""
        out: list[dict[str, Any]] = []

        def _collect(node: Any) -> None:
            if isinstance(node, dict):
                value = (node.get("value") or node.get("device")
                         or node.get("name") or node.get("vmnic") or node.get("vmk"))
                if value:
                    label = node.get("label") or node.get("description") or str(value)
                    out.append({"value": str(value), "label": str(label)})
                    return
                for val in node.values():
                    _collect(val)
            elif isinstance(node, (list, tuple)):
                for item in node:
                    _collect(item)

        _collect(body)
        return out

    async def _discover_initiator_options(self) -> list[dict[str, Any]]:
        """Discover the ESXi hosts' initiators for the register_hosts form."""
        kinds = (
            ("iqns", "iscsi", "iSCSI IQN"),
            ("wwns", "fc", "FC WWN"),
            ("nqns", "nvme-fc", "NVMe NQN"),
        )
        opts: list[dict[str, Any]] = []
        for field_name, protocol, label in kinds:
            found = (await self._discover_esxi_initiators(protocol)).get(field_name, [])
            for addr in found:
                opts.append({"field": field_name, "value": addr,
                             "label": f"{label} — {addr}"})
        return opts

    # --------------------------------------------------- cluster awareness ---
    def _mock_or_dry(self) -> bool:
        """True when no live vCenter is reachable (mock mode or dry-run)."""
        return bool(self.ctx.dry_run or getattr(self.ctx.runner, "mock", False)
                    or getattr(self.ctx.runner, "dry_run", False))

    async def list_nodes(self) -> list[ClusterNode]:
        """Return the ESXi hosts in the configured cluster/datacenter via pyVmomi."""
        cluster_name = self.ctx.target.get("cluster", "") or ""
        datacenter = self.ctx.target.get("datacenter", "") or ""

        if self._mock_or_dry():
            await self.ctx.emit(
                f"[cluster] (mock/dry-run) synthesizing 2-host ESXi cluster "
                f"{cluster_name or 'default'}"
            )
            return [
                ClusterNode(
                    name=f"esxi{n:02d}.example.com",
                    host=f"esxi{n:02d}.example.com",
                    info={"cluster": cluster_name or "default", "datacenter": datacenter},
                )
                for n in (1, 2)
            ]

        def _list_sync():
            from pyVmomi import vim
            hosts = []
            if cluster_name:
                cluster = self._find_one_sync([vim.ClusterComputeResource], cluster_name)
                if cluster:
                    hosts = [(h.name, h._moId) for h in cluster.host]
            if not hosts:
                hosts = [(h.name, h._moId)
                         for h in self._find_all_sync([vim.HostSystem])]
            return hosts

        try:
            host_pairs = await asyncio.to_thread(_list_sync)
        except Exception as exc:
            await self.ctx.emit(f"[cluster] pyVmomi list_nodes failed: {exc}; falling back")
            host_pairs = []

        if host_pairs:
            nodes = [
                ClusterNode(name=name, host=name,
                            info={"cluster": cluster_name, "datacenter": datacenter,
                                  "moid": moid})
                for name, moid in host_pairs
            ]
            await self.ctx.emit(
                f"[cluster] discovered {len(nodes)} ESXi host(s) from vCenter"
            )
            return nodes

        host = (cluster_name or self.ctx.target.get("vcenter_host") or "vcenter")
        await self.ctx.emit(
            f"[cluster] no ESXi hosts enumerated; falling back to single node {host}"
        )
        return [ClusterNode(name=str(host), host=str(host),
                            info={"cluster": cluster_name, "datacenter": datacenter})]

    @staticmethod
    def _parse_cluster_hosts(body: Any, cluster: str, datacenter: str) -> list[ClusterNode]:
        """Best-effort extraction of ESXi host names from a vCenter reply."""
        nodes: list[ClusterNode] = []
        seen: set[str] = set()

        def _collect(node: Any) -> None:
            if isinstance(node, dict):
                name = (node.get("name") or node.get("host_name")
                        or node.get("hostName") or node.get("host"))
                if name and str(name) not in seen:
                    seen.add(str(name))
                    nodes.append(ClusterNode(
                        name=str(name), host=str(name),
                        info={"cluster": cluster, "datacenter": datacenter}))
                    return
                for val in node.values():
                    _collect(val)
            elif isinstance(node, (list, tuple)):
                for item in node:
                    _collect(item)

        _collect(body)
        return nodes

    async def validate_cluster(self, **params: Any) -> OpResult:
        """Validate the ESXi cluster is uniformly configured for storage."""
        protocol = (params.get("protocol") or "iscsi").lower()
        if protocol not in {p.value for p in self.SUPPORTED_PROTOCOLS}:
            return OpResult.fail(f"Unsupported protocol {protocol!r}")
        kind = self._initiator_kind_for(protocol)
        nodes = await self.list_nodes()
        await self.ctx.emit(
            f"Validating {len(nodes)} ESXi host(s) for uniform {kind} ({protocol})"
        )

        per_node = await self._per_host_initiators(nodes, protocol, kind)
        consistent, detail = compare_node_interfaces(per_node)
        node_dicts = [n.to_dict() for n in nodes]
        if consistent:
            return OpResult.ok(
                f"Cluster uniform for {protocol}: {detail}",
                nodes=node_dicts, protocol=protocol,
                per_node={n: sorted(v) for n, v in per_node.items()},
            )
        return OpResult.fail(
            f"Cluster NOT uniform for {protocol}: {detail}",
            nodes=node_dicts, protocol=protocol,
            per_node={n: sorted(v) for n, v in per_node.items()},
        )

    async def _per_host_initiators(self, nodes: list[ClusterNode], protocol: str,
                                   kind: str) -> dict[str, list[str]]:
        """Collect the storage-adapter initiators of ``kind`` for each ESXi host."""
        per_node: dict[str, list[str]] = {}
        for node in nodes:
            found = (await self._discover_esxi_initiators(protocol)).get(kind, [])
            per_node[node.name] = list(found)
        return per_node

    # ------------------------------------------------------------ lifecycle ---
    async def validate_connection(self) -> OpResult:
        host = self.ctx.target.get("vcenter_host")
        if not host:
            raise ConnectionValidationError("No vcenter_host configured")
        await self.ctx.emit(f"Connecting to vCenter {host} via pyVmomi ...")

        def _connect():
            si = self._si_sync()
            about = si.content.about
            return f"{about.fullName} (build {about.build})"

        if self._mock_or_dry():
            desc = "VMware vCenter Server (mock)"
        else:
            try:
                desc = await asyncio.to_thread(_connect)
            except Exception as exc:
                raise ConnectionValidationError(f"vCenter connection failed: {exc}") from exc
        await self.ctx.emit(f"Connected: {desc}")
        info: dict[str, Any] = {}
        if self.ctx.array is not None:
            info = await self.ctx.array.info()
            await self.ctx.emit(f"FlashArray reachable: {info}")
        return OpResult.ok(f"Connected to vCenter {host}",
                           artifacts={"vcenter": host, "vcenter_version": desc}, array=info)

    # -------------------------------------------- pyVmomi SMS helper ---

    def _sms_storage_manager_sync(self):
        """Return the SMS StorageManager, reusing the cached SI session cookie.

        Negotiates the SMS SOAP version automatically: tries from newest to
        oldest so the call succeeds regardless of vCenter generation.
        """
        from pyVmomi import SoapStubAdapter, VmomiSupport
        import ssl
        try:
            from pyVmomi import sms as smslib
        except ImportError as exc:
            raise RuntimeError(
                "pyVmomi SMS module unavailable; upgrade pyVmomi >= 7.0"
            ) from exc
        si = self._si_sync()
        host = self.ctx.target.get("vcenter_host") or ""
        ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

        versions = sorted(
            [v for v in VmomiSupport.versionIdMap if v.startswith("sms.version.version")],
            key=lambda v: int(v.split("version")[-1]),
            reverse=True,
        )
        last_exc: Exception = RuntimeError("No SMS versions available in pyVmomi")
        for version in versions:
            try:
                stub = SoapStubAdapter(
                    host=host, port=443,
                    version=version,
                    path="/vsm/sdk",
                    sslContext=ssl_ctx,
                )
                stub.cookie = si._stub.cookie
                sms_si = smslib.ServiceInstance("ServiceInstance", stub)
                return sms_si.QueryStorageManager()
            except Exception as exc:
                last_exc = exc
                continue
        raise RuntimeError(f"No supported SMS version found: {last_exc}")

    def _get_cluster_hosts_sync(self, cluster_name: str = "") -> list:
        """Return vim.HostSystem objects for the configured cluster (or all hosts)."""
        from pyVmomi import vim
        name = cluster_name or self.ctx.target.get("cluster") or ""
        if name:
            cluster = self._find_one_sync([vim.ClusterComputeResource], name)
            if cluster:
                return list(cluster.host)
        return self._find_all_sync([vim.HostSystem])

    def _setup_iscsi_on_host_sync(
        self, host, vmknics: list[str], target_ips: list[str],
    ) -> str:
        """Enable iSCSI SW adapter, bind VMkernel NICs, add FA send targets, rescan.

        Returns the host's iSCSI IQN.
        """
        import time
        from pyVmomi import vim

        ss = host.configManager.storageSystem

        # Enable the software iSCSI adapter (idempotent).
        ss.UpdateSoftwareInternetScsiEnabled(enabled=True)

        # Wait up to ~10 s for the adapter to materialise after enabling.
        iscsi_hba = None
        for _ in range(10):
            for hba in (ss.storageDeviceInfo.hostBusAdapter or []):
                if isinstance(hba, vim.host.InternetScsiHba):
                    iscsi_hba = hba
                    break
            if iscsi_hba:
                break
            time.sleep(1)

        if iscsi_hba is None:
            raise RuntimeError(
                f"iSCSI software adapter not found on {host.name} after enabling"
            )

        vmhba = iscsi_hba.device
        iqn = iscsi_hba.iScsiName

        # Bind each VMkernel NIC to the iSCSI adapter (port binding).
        iscsi_mgr = host.configManager.iscsiManager
        if iscsi_mgr:
            for vmk in vmknics:
                try:
                    iscsi_mgr.BindVnic(iScsiHbaName=vmhba, vnicDevice=vmk)
                except Exception as exc:
                    if "already" not in str(exc).lower():
                        raise

        # Add FlashArray iSCSI portals as send targets.
        if target_ips:
            send_targets = [
                vim.host.InternetScsiHba.SendTarget(address=ip, port=3260)
                for ip in target_ips
            ]
            try:
                ss.AddInternetScsiSendTargets(iScsiHbaDevice=vmhba, targets=send_targets)
            except Exception as exc:
                if "already" not in str(exc).lower() and "duplicate" not in str(exc).lower():
                    raise

        # Rescan so the host discovers the FA targets.
        try:
            ss.RescanAllHba()
        except Exception:
            pass

        return iqn

    def _create_nfs_datastore_sync(
        self, name: str, nfs_server: str, nfs_path: str, cluster: str = ""
    ) -> None:
        """Mount an NFS export as a datastore on all cluster hosts via pyVmomi."""
        from pyVmomi import vim
        spec = vim.host.NasVolume.Specification(
            accessMode="readWrite",
            localPath=name,
            remoteHost=nfs_server,
            remotePath=nfs_path,
        )
        for host in self._get_cluster_hosts_sync(cluster):
            try:
                host.configManager.datastoreSystem.CreateNasDatastore(spec)
            except Exception as exc:
                if "already" not in str(exc).lower():
                    raise

    def _create_vmfs_datastore_sync(
        self, name: str, volume_serial: str, cluster: str = ""
    ) -> None:
        """Create a VMFS 6 datastore on the cluster hosts for the given FA volume.

        Identifies the LUN by its SCSI NAA ID (derived from the FA volume serial),
        then uses QueryVmfsDatastoreCreateOptions + CreateVmfsDatastore on the first
        host (the datastore auto-appears on other hosts after rescan).
        """
        import time
        from pyVmomi import vim

        # Everpure FlashArray SCSI NAA = "3624a9370" + last 24 chars of serial (lowercase)
        naa = "3624a9370" + volume_serial.lower()[-24:]

        hosts = self._get_cluster_hosts_sync(cluster)
        if not hosts:
            raise RuntimeError("No ESXi hosts found for VMFS datastore creation")

        # Rescan HBAs on all hosts to surface the new LUN.
        for h in hosts:
            try:
                h.configManager.storageSystem.RescanAllHba()
            except Exception:
                pass
        time.sleep(3)

        host = hosts[0]
        ds_system = host.configManager.datastoreSystem

        # Wait up to 15 s for the LUN to appear in available disks.
        device_path = None
        for attempt in range(15):
            disks = ds_system.QueryAvailableDisksForVmfs() or []
            for disk in disks:
                if naa.lower() in (disk.canonicalName or "").lower():
                    device_path = disk.devicePath
                    break
            if device_path:
                break
            time.sleep(1)
            if attempt % 3 == 2:
                try:
                    host.configManager.storageSystem.RescanAllHba()
                except Exception:
                    pass

        if not device_path:
            raise RuntimeError(
                f"LUN {naa} not found after rescan — verify the volume is "
                "connected to the host group before provisioning a datastore."
            )

        options = ds_system.QueryVmfsDatastoreCreateOptions(
            devicePath=device_path, vmfsMajorVersion=6
        )
        if not options:
            raise RuntimeError(f"No VMFS create options returned for {device_path}")

        spec = options[0].spec
        spec.vmfs.volumeName = name
        ds_system.CreateVmfsDatastore(spec)

    def _grow_vmfs_datastore_sync(self, datastore_name: str, cluster: str = "") -> None:
        """Extend a VMFS datastore to consume the full resized LUN."""
        hosts = self._get_cluster_hosts_sync(cluster)
        if not hosts:
            raise RuntimeError("No ESXi hosts found for datastore grow")

        host = hosts[0]
        ds_system = host.configManager.datastoreSystem

        datastore = next(
            (ds for ds in (host.datastore or []) if ds.name == datastore_name), None
        )
        if not datastore:
            raise RuntimeError(f"Datastore {datastore_name!r} not found on {host.name}")

        extents = getattr(getattr(datastore.info, "vmfs", None), "extent", None) or []
        if not extents:
            return

        options = ds_system.QueryVmfsDatastoreExtendOptions(
            datastore=datastore, devicePath=extents[0].diskName, suppressExpandCandidates=False
        )
        if not options:
            return
        ds_system.ExtendVmfsDatastore(datastore=datastore, addSpec=options[0].spec)

    def _register_vasa_provider_sync(self, name: str, url: str,
                                      username: str = "", password: str = "") -> None:
        """Register the VASA provider via pyVmomi SMS StorageManager."""
        from pyVmomi import sms as smslib
        mgr = self._sms_storage_manager_sync()
        for p in (mgr.QueryProvider() or []):
            if getattr(p, "name", "") == name or getattr(p, "url", "") == url:
                return  # already registered
        spec = smslib.provider.VasaProviderSpec()
        spec.name = name
        spec.url = url
        if username:
            try:
                cred = smslib.provider.StorageProviderCredential()
                cred.username = username
                cred.password = password
                spec.credential = cred
            except AttributeError:
                pass
        mgr.RegisterProvider(providerSpec=spec)

    def _unregister_vasa_provider_sync(self, name: str) -> None:
        mgr = self._sms_storage_manager_sync()
        for p in (mgr.QueryProvider() or []):
            if getattr(p, "name", "") == name:
                mgr.UnregisterProvider(providerId=p.uid)
                return

    # ------------------------------------------- integration lifecycle ---

    def _plugin_base(self, phif_host: str) -> str:
        """Return the plugin proxy base URL (https://<host>:9443)."""
        return f"https://{phif_host.strip()}:9443"

    async def _plugin_vcenter_thumbprint(self, phif_host: str, vcenter_host: str) -> str:
        """Ask the plugin server for the vCenter's TLS thumbprint.

        Uses the plugin's own management API so it computes the thumbprint exactly
        the way it will use it when connecting to vCenter.
        """
        url = f"{self._plugin_base(phif_host)}/api/management/thumbprint?url={vcenter_host}"
        resp = await self.ctx.runner.run_http("GET", url, expected=(200,))
        body = resp.get("json") or {}
        thumb = body.get("thumbprint", "")
        if not thumb:
            # run_http returns an empty body in mock/dry-run, so there is no real
            # thumbprint to report. Synthesize one rather than failing the flow.
            if self._mock_or_dry():
                return ":".join(["00"] * 20)
            raise RuntimeError(f"Plugin thumbprint endpoint returned no thumbprint: {body}")
        return thumb

    async def deploy_integration(self, phif_host: str = "", plugin_url: str = "",
                                 register_vasa: bool = True, force: bool = False,
                                 **_: Any) -> OpResult:
        host = phif_host.strip()
        if not host:
            # Allow deriving the host from an explicit plugin_url.
            if plugin_url.strip():
                from urllib.parse import urlparse
                host = urlparse(plugin_url.strip()).hostname or ""
            if not host:
                return OpResult.fail(
                    "Provide 'phif_host' (the PHIF server address; the plugin proxy "
                    "listens on port 9443)")

        vcenter_host = self.ctx.target.get("vcenter_host") or ""
        vcenter_user = self.ctx.target.get("vcenter_user") or ""
        vcenter_pwd = self.ctx.target.get("vcenter_password") or ""
        if not vcenter_host:
            return OpResult.fail("No vcenter_host configured on the hypervisor")

        resolved_url = plugin_url.strip() or f"{self._plugin_base(host)}/plugin-manifest.zip"

        await self.ctx.emit(
            f"Registering Everpure vSphere Client Remote Plugin via plugin server "
            f"(manifest: {resolved_url}) ...")

        if self.ctx.dry_run:
            await self.ctx.emit("[dry-run] would register remote plugin + VASA provider")
            return OpResult.ok("Dry-run: plugin + VASA registration planned",
                               artifacts={"plugin_url": resolved_url})

        # 1. Fetch the vCenter thumbprint from the plugin server's management API.
        await self.ctx.emit(f"Fetching vCenter thumbprint for {vcenter_host} ...")
        thumbprint = await self._plugin_vcenter_thumbprint(host, vcenter_host)
        await self.ctx.emit(f"vCenter thumbprint: {thumbprint}")

        # 2. Register the plugin via the plugin server's management API. The plugin
        #    server performs the vCenter extension registration itself (selecting
        #    the correct remote-plugin manifest and deployment behaviour).
        register_url = f"{self._plugin_base(host)}/api/management/register"
        payload = {
            "vcenterFqdn": vcenter_host,
            "vcenterThumbprint": thumbprint,
            "username": vcenter_user,
            "password": vcenter_pwd,
            "pluginUrl": resolved_url,
            "skipLocalPluginRemoval": False,
            "force": force,
        }
        await self.ctx.emit(f"Registering plugin with vCenter {vcenter_host} ...")
        resp = await self.ctx.runner.run_http(
            "POST", register_url, json_body=payload, expected=(200, 201, 202),
        )
        await self.ctx.emit("Remote plugin registered via plugin server")

        artifacts: dict[str, Any] = {
            "plugin": "com.purestorage.purestoragehtml",
            "plugin_url": resolved_url,
            "vcenter": vcenter_host,
            "register_response": resp.get("json") or {},
        }

        # The associated FlashArray is not injected into the plugin's store —
        # add it from the plugin's own UI at https://<phif-host>:9443 once the
        # plugin is registered.
        await self.ctx.emit(
            "Add the FlashArray as a managed array from the plugin UI "
            f"(https://{host}:9443)")

        if register_vasa:
            try:
                vasa = await self._register_vasa_pyvmomi()
                artifacts.update(vasa)
            except Exception as exc:
                vasa_host = self._array_host()
                vasa_url = f"https://{vasa_host}:{VASA_PORT}"
                await self.ctx.emit(
                    f"[warn] VASA registration via SMS failed ({exc}). "
                    f"Register the VASA provider manually: vCenter → Storage Providers "
                    f"→ Add Provider → URL: {vasa_url}  (use FlashArray credentials)"
                )
        return OpResult.ok("vSphere plugin registered", artifacts=artifacts)

    async def _register_vasa_pyvmomi(self, provider_name: str = "",
                                     vasa_host: str = "") -> dict[str, Any]:
        """Register the Everpure VASA provider via pyVmomi SMS."""
        array = self.ctx.target.get("vcenter_host", "array")
        if self.ctx.array is not None:
            info = await self.ctx.array.info()
            array = info.get("name", array)
        name = provider_name or f"Everpure-VASA-{array}"
        vasa_host = vasa_host or self._array_host()
        url = f"https://{vasa_host}:{VASA_PORT}"

        username = getattr(self.ctx.array, "_username", "") or "" if self.ctx.array else ""
        password = getattr(self.ctx.array, "_password", "") or "" if self.ctx.array else ""

        await self.ctx.emit(f"Registering VASA provider {name} at {url} ...")
        if self._mock_or_dry():
            await self.ctx.emit(
                "[mock/dry-run] skipping SMS registration; reporting the "
                "provider that would be registered")
            return {"vasa_provider": name, "vasa_url": url}
        await asyncio.to_thread(self._register_vasa_provider_sync, name, url, username, password)
        return {"vasa_provider": name, "vasa_url": url}

    async def configure(self, provider_name: str = "", **_: Any) -> OpResult:
        await self.ctx.emit("Refreshing VASA storage provider registration ...")
        if self.ctx.dry_run:
            return OpResult.ok("Dry-run: VASA provider refresh planned")
        vasa = await self._register_vasa_pyvmomi(provider_name=provider_name)
        return OpResult.ok("VASA provider registered/refreshed", artifacts=vasa)

    # ------------------------------------------------- host / connectivity ---
    @staticmethod
    def _csv(value: str) -> list[str]:
        return [s.strip() for s in (value or "").split(",") if s.strip()]

    @staticmethod
    def _fa_host_name(esxi_name: str) -> str:
        """Sanitize an ESXi host name to meet FlashArray naming requirements.

        FA allows only alphanumeric and '-', 1-63 chars, must start/end with
        alnum.  ESXi hosts are often named by IP (e.g. 192.0.2.59) which
        contains dots — those are replaced with hyphens.
        """
        import re
        name = re.sub(r"[^a-zA-Z0-9-]", "-", esxi_name).strip("-")[:63].rstrip("-")
        return name or "esxi-host"

    _FC_PROTOCOLS = {"fc"}
    _NVME_PROTOCOLS = {"nvme-fc", "nvme-roce", "nvme-tcp"}

    def _initiators_for(self, protocol: str, iqns: list[str], wwns: list[str],
                        nqns: list[str]) -> dict[str, list[str]]:
        if protocol in self._FC_PROTOCOLS:
            return {"wwns": wwns}
        if protocol in self._NVME_PROTOCOLS:
            return {"nqns": nqns}
        return {"iqns": iqns}

    async def register_hosts(self, host_group: str = "", protocol: str = "iscsi",
                             hosts: str = "", iqns: str = "", wwns: str = "",
                             nqns: str = "", fa_api_token: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        protocol = (protocol or "iscsi").lower()
        if protocol not in {p.value for p in self.SUPPORTED_PROTOCOLS}:
            return OpResult.fail(f"Unsupported protocol {protocol!r}")
        host_group = host_group or self.ctx.target.get("host_group", "")
        if not host_group:
            return OpResult.fail(
                "No host_group provided or configured on the hypervisor "
                "(set 'host_group' on the connection or pass it to this action)")
        host_list = self._csv(hosts)
        iqn_list, wwn_list, nqn_list = self._csv(iqns), self._csv(wwns), self._csv(nqns)
        selected = self._initiators_for(protocol, iqn_list, wwn_list, nqn_list)
        addrs = next(iter(selected.values()))
        if not addrs:
            await self.ctx.emit(
                f"No {self._initiator_kind_for(protocol)} provided; auto-discovering "
                f"from vCenter ESXi hosts"
            )
            selected = await self._discover_esxi_initiators(protocol)
            addrs = next(iter(selected.values()))
            await self.ctx.emit(f"Discovered {len(addrs)} initiator(s) from vCenter")
        if not addrs:
            return OpResult.fail(
                f"No initiators provided and none discovered from vCenter for "
                f"protocol {protocol!r}"
            )
        await self.ctx.emit(
            f"Registering ESXi host group {host_group} (protocol={protocol}, "
            f"hosts={host_list or '-'}, initiators={len(addrs)})"
        )
        if protocol in self._FC_PROTOCOLS:
            await self.ctx.emit(
                "Fibre Channel: ensure the ESXi HBA WWNs are zoned to the array on the "
                "SAN fabric (prerequisite); registering hosts by WWN"
            )
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: host group {host_group} registration planned")

        kind = self._initiator_kind_for(protocol)
        explicit_initiators = bool(iqn_list or wwn_list or nqn_list)
        per_host: dict[str, list[str]] = {}
        if host_list:
            await self.ctx.emit(
                f"Cluster fan-out: registering {len(host_list)} named ESXi host(s) "
                f"into host group {host_group}"
            )
            for h in host_list:
                h_addrs = addrs if explicit_initiators else (
                    await self._discover_esxi_initiators(protocol)).get(kind, [])
                per_host[h] = h_addrs
        elif not explicit_initiators:
            nodes = await self.list_nodes()
            if len(nodes) > 1:
                await self.ctx.emit(
                    f"Cluster fan-out: registering {len(nodes)} discovered ESXi "
                    f"host(s) into host group {host_group}"
                )
                for node in nodes:
                    per_host[node.name] = (
                        await self._discover_esxi_initiators(protocol)).get(kind, [])

        all_addrs: list[str] = []
        for v in (per_host.values() if per_host else [addrs]):
            all_addrs.extend(v)
        union = list(dict.fromkeys(all_addrs))
        playbook_selected = {kind: union}
        await self.ctx.runner.run_ansible(
            HOST_REGISTER_PLAYBOOK,
            extravars={
                "fa_url": self.ctx.array.endpoint if self.ctx.array else "",
                "fa_api_token": self.ctx.resolve_token(fa_api_token or None) or "",
                "host_group": host_group,
                "protocol": protocol,
                "esxi_hosts": list(per_host) or host_list,
                "iqns": playbook_selected.get("iqns", []),
                "wwns": playbook_selected.get("wwns", []),
                "nqns": playbook_selected.get("nqns", []),
            },
        )
        if per_host:
            specs = [{"name": self._fa_host_name(h), kind: h_addrs}
                     for h, h_addrs in per_host.items()]
        else:
            specs = [{"name": host_group, **selected}]
        res = await self.apply_host_group(host_group, specs)
        if res.get("conflict"):
            return OpResult.fail(res["conflict"],
                                 artifacts={"host_group": host_group, "protocol": protocol})
        host_group = res["host_group"]
        member_hosts = res["hosts"]
        return OpResult.ok(
            f"Host group {host_group} registered on array ({protocol})",
            artifacts={"host_group": host_group, "hosts": member_hosts,
                       "protocol": protocol, "adopted_host_group": res["adopted"]},
        )

    @staticmethod
    def _parse_initiators(spec: str) -> dict[str, list[str]]:
        """Parse 'host1=addr,addr;host2=addr' into {host: [addr, ...]}."""
        out: dict[str, list[str]] = {}
        for chunk in (spec or "").split(";"):
            chunk = chunk.strip()
            if not chunk or "=" not in chunk:
                continue
            host, addrs = chunk.split("=", 1)
            out[host.strip()] = [a.strip() for a in addrs.split(",") if a.strip()]
        return out

    async def _apply_interface_binding(
        self, *, protocol: str, vmknics: list[str], fc_hbas: list[str],
        nvme_nics: list[str],
    ) -> dict[str, list[str]] | None:
        """Record the operator's adapter selection for `protocol`, if any.

        Returns the ``interface_binding`` artifact, or None when the operator
        selected nothing — callers omit the artifact entirely in that case so
        "no selection" stays distinguishable from "selected nothing".

        Only the field matching the transport is honoured: an iSCSI run ignores
        `fc_hbas`, and so on. For iSCSI the actual BindVnic calls happen inside
        :meth:`_setup_iscsi_on_host_sync`; FC/NVMe selection is a filter on which
        adapters' initiators get registered, so there is nothing further to push
        to vCenter here.
        """
        if protocol in self._NVME_PROTOCOLS:
            selected, key = nvme_nics, "nvme_adapters"
            label, op = "NVMe adapter selection", "nvme/adapter-select"
        elif protocol in self._FC_PROTOCOLS:
            selected, key = fc_hbas, "fc_hbas"
            label, op = "FC HBA selection", "fc/hba-select"
        else:
            selected, key = vmknics, "iscsi_port_binding"
            label, op = "iSCSI port binding", "iscsi/port-binding"

        if not selected:
            return None

        await self.ctx.emit(
            f"Applying {label} via {op}: {', '.join(selected)}")
        return {key: list(selected)}

    async def _rescan_storage(self, cluster: str) -> None:
        """Trigger ESXi storage (HBA) rescan via pyVmomi for all hosts in cluster."""
        if self._mock_or_dry():
            # Name the logical operation (host storage/rescan) so the mock path is
            # observable in the job log, the same way run_http echoes its URL.
            await self.ctx.emit(
                "[mock/dry-run] would issue host storage/rescan "
                f"(RescanAllHba + RescanVmfs) for cluster {cluster or 'default'}")
            return

        def _rescan_sync():
            from pyVmomi import vim
            cluster_name = cluster or self.ctx.target.get("cluster") or ""
            hosts = []
            if cluster_name:
                cl = self._find_one_sync([vim.ClusterComputeResource], cluster_name)
                if cl:
                    hosts = list(cl.host)
            if not hosts:
                hosts = self._find_all_sync([vim.HostSystem])
            for host in hosts:
                try:
                    ss = host.configManager.storageSystem
                    if ss:
                        ss.RescanAllHba()
                        ss.RescanVmfs()
                except Exception:
                    pass

        await asyncio.to_thread(_rescan_sync)
        await self.ctx.emit("Triggered ESXi storage rescan (HBA rescan)")

    @staticmethod
    def _as_list(value: Any) -> list[str]:
        """Normalize a MULTISELECT value (list, or comma-separated string) to a list."""
        if value is None:
            return []
        if isinstance(value, (list, tuple)):
            return [str(v).strip() for v in value if str(v).strip()]
        return [s.strip() for s in str(value).split(",") if s.strip()]

    async def setup_connectivity(self, host_group: str = "", protocol: str = "iscsi",
                                 hosts: str = "", initiators: str = "",
                                 cluster: str = "",
                                 iscsi_nics: Any = None,
                                 iscsi_vmknics: Any = None,
                                 fc_hbas: Any = None,
                                 nvme_sources: Any = None,
                                 nvme_adapters: Any = None,
                                 **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        protocol = (protocol or "iscsi").lower()
        if protocol not in {p.value for p in self.SUPPORTED_PROTOCOLS}:
            return OpResult.fail(f"Unsupported protocol {protocol!r}")
        host_group = host_group or self.ctx.target.get("host_group", "")
        if not host_group:
            return OpResult.fail(
                "No host_group provided or configured on the hypervisor "
                "(set 'host_group' on the connection or pass it to this action)")

        is_fc = protocol in self._FC_PROTOCOLS
        is_nvme = protocol in self._NVME_PROTOCOLS
        is_iscsi = not is_fc and not is_nvme
        cluster = cluster or self.ctx.target.get("cluster", "") or ""
        per_host = self._parse_initiators(initiators)

        # Coalesce wizard alias names onto single lists
        vmknics = self._as_list(iscsi_nics) or self._as_list(iscsi_vmknics)
        nvme_nics = self._as_list(nvme_sources) or self._as_list(nvme_adapters)

        await self.ctx.emit(
            f"Setting up {protocol} connectivity → host group {host_group}"
        )

        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: {protocol} connectivity for {host_group} planned")

        specs: list[dict] = []

        if is_iscsi:
            target_ips: list[str] = await self.ctx.array.get_data_interfaces("iscsi")
            await self.ctx.emit(f"FlashArray iSCSI portals: {target_ips}")

            def _iscsi_all():
                result: dict[str, str] = {}
                for h in self._get_cluster_hosts_sync(cluster):
                    iqn = self._setup_iscsi_on_host_sync(h, vmknics, target_ips)
                    if iqn:
                        result[h.name] = iqn
                return result

            iqns_per_host: dict[str, str]
            if self._mock_or_dry():
                # No vCenter to enable the software adapter on. Use the operator's
                # explicit host list (or the synthetic cluster) and pair each host
                # with its supplied IQN, falling back to a synthetic one.
                await self.ctx.emit(
                    "[mock/dry-run] skipping ESXi iSCSI adapter configuration")
                names = self._csv(hosts) or [n.name for n in await self.list_nodes()]
                iqns_per_host = {
                    n: (per_host.get(n) or [f"iqn.1998-01.com.vmware:{n}"])[0]
                    for n in names
                }
            else:
                iqns_per_host = await asyncio.to_thread(_iscsi_all)
            await self.ctx.emit(
                f"iSCSI adapter enabled and configured on {len(iqns_per_host)} host(s)"
            )

            for hname, iqn in iqns_per_host.items():
                specs.append({
                    "name": self._fa_host_name(hname),
                    "iqns": per_host.get(hname, [iqn]),
                })

        elif is_fc:
            await self.ctx.emit("Fibre Channel: discovering WWNs from vCenter ...")
            discovered = (await self._discover_esxi_initiators(protocol)).get("wwns", [])
            nodes = await self.list_nodes()
            host_list = self._csv(hosts) or [n.name for n in nodes]
            for hname in host_list:
                specs.append({
                    "name": self._fa_host_name(hname),
                    "wwns": per_host.get(hname, discovered),
                })

        else:  # NVMe
            await self.ctx.emit("NVMe-oF: discovering NQNs from vCenter ...")
            discovered = (await self._discover_esxi_initiators(protocol)).get("nqns", [])
            nodes = await self.list_nodes()
            host_list = self._csv(hosts) or [n.name for n in nodes]
            for hname in host_list:
                specs.append({
                    "name": self._fa_host_name(hname),
                    "nqns": per_host.get(hname, discovered),
                })

        await self.ctx.emit(
            f"Registering {len(specs)} host(s) on FlashArray → host group {host_group} ..."
        )
        res = await self.apply_host_group(host_group, specs)
        if res.get("conflict"):
            return OpResult.fail(res["conflict"],
                                 artifacts={"host_group": host_group, "protocol": protocol})

        effective_hg = res["host_group"]
        created = res.get("created", [])
        reused = res.get("reused", [])
        if created:
            await self.ctx.emit(f"Created FA host(s): {', '.join(created)}")
        if reused:
            await self.ctx.emit(f"Reused existing FA host(s): {', '.join(reused)}")
        # Rescan only for Fibre Channel. FC has no login step, so the HBAs must be
        # rescanned before the fabric's new targets/LUNs appear. iSCSI and NVMe-oF
        # both perform an explicit login/connect, and provision_datastore rescans
        # again once a LUN actually exists — so rescanning here would be noise.
        rescanned = is_fc
        if rescanned:
            await self.ctx.emit(
                f"Host group {effective_hg!r} ready — rescanning storage on ESXi hosts ..."
            )
            await self._rescan_storage(cluster)
            await self.ctx.emit("Storage rescan complete")
        else:
            await self.ctx.emit(f"Host group {effective_hg!r} ready")

        artifacts: dict[str, Any] = {
            "host_group": effective_hg,
            "hosts": res["hosts"],
            "protocol": protocol,
            "rescanned": rescanned,
        }
        binding = await self._apply_interface_binding(
            protocol=protocol, vmknics=vmknics,
            fc_hbas=self._as_list(fc_hbas), nvme_nics=nvme_nics)
        if binding:
            artifacts["interface_binding"] = binding
        return OpResult.ok(
            f"{protocol} connectivity configured for {effective_hg}",
            artifacts=artifacts,
        )

    # ----------------------------------------------------------- provision ---
    async def provision(self, name: str, size: str = "1T", host_group: str = "",
                        **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Creating volume {name} ({size})")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: volume {name} ({size}) planned")
        await self.ctx.array.create_volume(name, size)
        if host_group:
            await self.ctx.array.connect_volume(host_group, name)
            await self.ctx.emit(f"Connected {name} to {host_group}")
        return OpResult.ok(f"Provisioned volume {name}", artifacts={"volume": name})

    async def provision_datastore(self, name: str, size: str = "1T", host_group: str = "",
                                  type: str = "vmfs", protocol: str = "iscsi",
                                  cluster: str = "", nfs_server: str = "",
                                  nfs_path: str = "", **_: Any) -> OpResult:
        ds_type = (type or "vmfs").lower()
        protocol = (protocol or "iscsi").lower()
        cluster = cluster or self.ctx.target.get("cluster", "") or ""
        await self.ctx.emit(
            f"Provisioning {ds_type.upper()} datastore {name} ({size}) over {protocol}"
        )

        if ds_type == "nfs":
            if not (nfs_server and nfs_path):
                return OpResult.fail("NFS datastore requires nfs_server and nfs_path")
            if self.ctx.dry_run:
                return OpResult.ok(f"Dry-run: NFS datastore {name} planned")
            if self._mock_or_dry():
                await self.ctx.emit(
                    f"[mock/dry-run] would mount NFS datastore {name} on the cluster")
            else:
                await asyncio.to_thread(
                    self._create_nfs_datastore_sync, name, nfs_server, nfs_path, cluster
                )
            return OpResult.ok(
                f"NFS datastore {name} created",
                artifacts={"datastore": name, "type": "nfs",
                           "nfs": f"{nfs_server}:{nfs_path}"},
            )

        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if not host_group:
            return OpResult.fail("host_group is required for VMFS/vVol datastores")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: {ds_type} datastore {name} planned")

        await self.ctx.array.create_volume(name, size)
        await self.ctx.emit(f"Created backing volume {name} ({size})")
        await self.ctx.array.connect_volume(host_group, name)
        await self.ctx.emit(f"Connected {name} to host group {host_group} ({protocol})")

        vol_info = await self.ctx.array.get_volume(name) or {}
        serial = vol_info.get("serial", "")

        # The new LUN has to be discovered before VMFS can be laid down on it.
        # This matters most for FC, which has no login step, but every transport
        # needs the host to notice the device. (_create_vmfs_datastore_sync also
        # rescans while polling for the device to appear; this makes the step an
        # explicit, logged part of the flow.)
        await self._rescan_storage(cluster)

        if self._mock_or_dry():
            await self.ctx.emit(
                f"[mock/dry-run] would create {ds_type.upper()} datastore {name} "
                f"on serial {serial or '<unknown>'}")
        else:
            await asyncio.to_thread(self._create_vmfs_datastore_sync, name, serial, cluster)
        await self.ctx.emit(f"{ds_type.upper()} datastore {name} created in vCenter")
        return OpResult.ok(
            f"{ds_type.upper()} datastore {name} provisioned over {protocol}",
            artifacts={"datastore": name, "volume": name, "type": ds_type,
                       "host_group": host_group, "protocol": protocol},
        )

    # ----------------------------------------------------------- day-2 ops ---
    async def snapshot(self, volume: str, suffix: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: snapshot of {volume} planned")
        await self.ctx.array.create_snapshot(volume, suffix=suffix or None)
        return OpResult.ok(f"Snapshot of {volume} created",
                           artifacts={"volume": volume, "suffix": suffix})

    async def clone(self, source: str, dest: str, host_group: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: clone {source} -> {dest} planned")
        await self.ctx.array.clone_volume(source, dest)
        if host_group:
            await self.ctx.array.connect_volume(host_group, dest)
            await self.ctx.emit(f"Connected clone {dest} to {host_group}")
        return OpResult.ok(f"Cloned {source} -> {dest}", artifacts={"volume": dest})

    async def resize(self, volume: str, size: str, datastore: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        await self.ctx.emit(f"Extending volume {volume} to {size}")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: resize {volume} -> {size} planned")
        await self.ctx.array.extend_volume(volume, size)
        if datastore:
            cluster = self.ctx.target.get("cluster", "") or ""
            if self._mock_or_dry():
                await self.ctx.emit(
                    f"[mock/dry-run] would grow datastore {datastore} onto the "
                    f"extended device")
            else:
                await asyncio.to_thread(self._grow_vmfs_datastore_sync, datastore, cluster)
            await self.ctx.emit(f"Grew datastore {datastore}")
        return OpResult.ok(f"Resized {volume} to {size}",
                           artifacts={"volume": volume, "datastore": datastore})

    async def set_qos(self, volume: str, iops_limit: int | None = None,
                      bw_limit: int | None = None, **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: QoS for {volume} planned")
        await self.ctx.array.set_qos(volume, iops_limit=iops_limit, bw_limit=bw_limit)
        return OpResult.ok(f"QoS set on {volume}",
                           artifacts={"volume": volume, "iops_limit": iops_limit,
                                      "bw_limit": bw_limit})

    async def configure_replication(self, name: str, volumes: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        vol_list = self._csv(volumes)
        await self.ctx.emit(f"Creating protection group {name} for {vol_list}")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: protection group {name} planned")
        await self.ctx.array.create_protection_group(name, vol_list)
        return OpResult.ok(f"Protection group {name} created",
                           artifacts={"protection_group": name, "volumes": vol_list})

    async def health_check(self, **_: Any) -> OpResult:
        info = await self.ctx.array.info() if self.ctx.array else {}
        vcenter = self.ctx.target.get("vcenter_host")
        si_ok = False
        try:
            def _check():
                return bool(self._si_sync().content.about)
            si_ok = await asyncio.to_thread(_check)
        except Exception:
            si_ok = False
        return OpResult.ok(
            "Healthy",
            array=info, vcenter=vcenter, vcenter_pyvmomi=si_ok,
        )

    async def teardown(self, phif_host: str = "", **_: Any) -> OpResult:
        await self.ctx.emit("Unregistering VASA provider and remote plugin from vCenter ...")
        if self.ctx.dry_run:
            return OpResult.ok("Dry-run: integration removal planned")
        array = self.ctx.target.get("vcenter_host", "array")
        if self.ctx.array is not None:
            info = await self.ctx.array.info()
            array = info.get("name", array)
        vasa_name = f"Everpure-VASA-{array}"
        try:
            await asyncio.to_thread(self._unregister_vasa_provider_sync, vasa_name)
            await self.ctx.emit(f"VASA provider {vasa_name!r} unregistered")
        except Exception as exc:
            await self.ctx.emit(f"[warn] VASA unregister: {exc}")

        # Unregister the remote plugin via the plugin server's management API.
        host = phif_host.strip() or self.ctx.target.get("phif_host", "")
        vcenter_host = self.ctx.target.get("vcenter_host") or ""
        if host and vcenter_host:
            try:
                thumbprint = await self._plugin_vcenter_thumbprint(host, vcenter_host)
                payload = {
                    "vcenterFqdn": vcenter_host,
                    "vcenterThumbprint": thumbprint,
                    "username": self.ctx.target.get("vcenter_user") or "",
                    "password": self.ctx.target.get("vcenter_password") or "",
                }
                await self.ctx.runner.run_http(
                    "POST", f"{self._plugin_base(host)}/api/management/unregister",
                    json_body=payload, expected=(200, 201, 202, 204),
                )
                await self.ctx.emit("Remote plugin unregistered via plugin server")
            except Exception as exc:
                await self.ctx.emit(f"[warn] Plugin unregister: {exc}")
        else:
            await self.ctx.emit(
                "[warn] Skipping plugin unregister: phif_host not available")
        return OpResult.ok("vSphere integration removed", status="not_deployed")

    # -------------------------------------------- VM management helpers ---

    @classmethod
    def _naa_from_serial(cls, serial: str) -> str:
        """ESXi NAA device name for an Everpure FlashArray volume serial."""
        return f"naa.{cls._PURE_SCSI_OUI}{serial.lower()}"

    @classmethod
    def _serial_from_naa(cls, naa: str) -> str | None:
        """Extract an Everpure FA volume serial (24-hex) from an ESXi device id, or None.

        Handles BOTH device-id forms ESXi reports for an RDM/LUN:

        * ``naa.624a9370<serial24>`` — the NAA, OUI at the start.
        * ``vml.0200<lun><...>624a9370<serial24><ascii model>`` — the VML id, where
          the Everpure OUI ``624a9370`` is embedded mid-string and trailing bytes are the
          ASCII vendor/model.

        We locate the Everpure OUI and take the 24 hex digits that follow it. Returns
        None when the OUI isn't present (a non-Everpure device) so the caller rejects a
        disk that can't map to a volume on the connected FlashArray.
        """
        s = (naa or "").lower().split("/")[-1]
        idx = s.find(cls._PURE_SCSI_OUI)
        if idx < 0:
            return None
        serial = s[idx + len(cls._PURE_SCSI_OUI): idx + len(cls._PURE_SCSI_OUI) + 24]
        if len(serial) == 24 and all(ch in "0123456789abcdef" for ch in serial):
            return serial
        return None

    @staticmethod
    def _vc_power_state(vc_state: str | None) -> str:
        s = (vc_state or "").upper()
        if s == "POWERED_ON":
            return "running"
        if s in ("POWERED_OFF", "SUSPENDED"):
            return "stopped"
        return "unknown"

    async def _find_vvol_datastore(self) -> str:
        """Return the first vVol datastore moId found in vCenter, or empty string."""
        if self._mock_or_dry():
            return ""

        def _find_sync():
            from pyVmomi import vim
            for ds in self._find_all_sync([vim.Datastore]):
                try:
                    if ds.summary.type == "VVOL":
                        return ds._moId
                except Exception:
                    continue
            return ""

        try:
            return await asyncio.to_thread(_find_sync)
        except Exception:
            return ""

    # ------------------------------------------------- VM inventory (MIGRATE) ---

    async def list_vms(self) -> list[dict[str, Any]]:
        """Enumerate VMs from vCenter via pyVmomi."""
        if self._mock_or_dry():
            return [
                {"id": "vm-42", "name": "mock-vsphere-vm", "power_state": "stopped",
                 "vcpus": 2, "memory_bytes": 4 * 1024 ** 3,
                 "disk_count": 1, "nic_count": 1},
            ]

        def _list_sync():
            from pyVmomi import vim
            out = []
            for vm in self._find_all_sync([vim.VirtualMachine]):
                if vm.config is None:
                    continue
                hw = vm.config.hardware
                ps = vm.runtime.powerState
                power_state = ("running" if ps == vim.VirtualMachine.PowerState.poweredOn
                               else "stopped")
                disk_count = sum(
                    1 for d in hw.device if isinstance(d, vim.vm.device.VirtualDisk))
                nic_count = sum(
                    1 for d in hw.device
                    if isinstance(d, vim.vm.device.VirtualEthernetCard))
                out.append({
                    "id": vm._moId,
                    "name": vm.config.name,
                    "power_state": power_state,
                    "vcpus": hw.numCPU,
                    "memory_bytes": hw.memoryMB * 1024 * 1024,
                    "disk_count": disk_count,
                    "nic_count": nic_count,
                })
            return out

        try:
            return await asyncio.to_thread(_list_sync)
        except Exception:
            return []

    async def list_networks(self) -> list[dict[str, Any]]:
        """Enumerate networks from vCenter via pyVmomi."""
        if self._mock_or_dry():
            return [{"id": "network-15", "name": "VM Network", "kind": "standard_portgroup"}]

        def _list_sync():
            from pyVmomi import vim
            out = []
            for net in self._find_all_sync(
                    [vim.Network, vim.dvs.DistributedVirtualPortgroup]):
                kind = ("dvportgroup"
                        if isinstance(net, vim.dvs.DistributedVirtualPortgroup)
                        else "standard_portgroup")
                out.append({"id": net._moId, "name": net.name, "kind": kind})
            return out

        try:
            return await asyncio.to_thread(_list_sync)
        except Exception:
            return []

    async def list_placements(self) -> list[dict[str, Any]]:
        """Return clusters with their Everpure-backed datastores for the migration wizard.

        Storage = FlashArray-backed VMFS datastores (where the VM home / VMDKs / RDM
        pointers live) plus any vVol datastore. A migration to vSphere needs an
        FA-backed VMFS, so listing only vVol datastores left the wizard's storage list
        blank (and unselectable)."""
        if self._mock_or_dry():
            return [{"cluster": {"id": "default", "name": "default"},
                     "storage": [{"id": "datastore1", "name": "datastore1", "kind": "vmfs"}]}]

        def _enum_clusters():
            from pyVmomi import vim
            return [{"id": c._moId, "name": c.name}
                    for c in self._find_all_sync([vim.ClusterComputeResource])]

        clusters = await asyncio.to_thread(_enum_clusters)
        if not clusters:
            cl = self.ctx.target.get("cluster") or "default"
            clusters = [{"id": cl, "name": cl}]

        storage: list[dict[str, Any]] = []
        for name, serial in await asyncio.to_thread(self._fa_backed_vmfs_sync):
            if self.ctx.array is not None and \
                    await self.ctx.array.find_volume_name_by_serial(serial):
                storage.append({"id": name, "name": name, "kind": "vmfs"})
        vvol_ds = await self._find_vvol_datastore()
        if vvol_ds and not any(s["id"] == vvol_ds for s in storage):
            storage.append({"id": vvol_ds, "name": vvol_ds, "kind": "vvol"})

        # Datastores on a FlashArray are typically shared cluster-wide; offer the same
        # set under each cluster.
        return [{"cluster": c, "storage": storage} for c in clusters]

    async def power_state(self, vm_ref: str) -> str:
        """``running`` | ``stopped`` | ``unknown`` for a vCenter VM via pyVmomi."""
        if self._mock_or_dry():
            return "stopped"

        def _get_sync():
            from pyVmomi import vim
            vm = self._get_vm_sync(vm_ref)
            ps = vm.runtime.powerState
            if ps == vim.VirtualMachine.PowerState.poweredOn:
                return "running"
            if ps in (vim.VirtualMachine.PowerState.poweredOff,
                      vim.VirtualMachine.PowerState.suspended):
                return "stopped"
            return "unknown"

        try:
            return await asyncio.to_thread(_get_sync)
        except Exception:
            return "unknown"

    async def capture_vm_spec(self, vm_ref: str) -> "VmSpec":
        """Read a VM's logical hardware into a normalized VmSpec via pyVmomi."""
        from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec

        if self._mock_or_dry():
            return VmSpec(
                name="mock-vsphere-vm", source_ref=str(vm_ref),
                vcpus=2, memory_bytes=4 * 1024 ** 3, firmware="bios",
                disks=[DiskSpec(
                    DiskIdentity(fa_volume="phifmig-mock-disk0",
                                 serial="aabbcc001122334455667788"),
                    bus="scsi", order=0, boot=True, source_ref="2000")],
                nics=[NicSpec(mac="00:50:56:12:34:56",
                              source_network="network-15", model="vmxnet3", order=0)],
                raw={"vsphere_disk_types": {"2000": "rdm"}})

        def _capture_sync():
            from pyVmomi import vim
            vm = self._get_vm_sync(vm_ref)
            hw = vm.config.hardware
            name = vm.config.name
            vcpus = hw.numCPU
            memory_bytes = hw.memoryMB * 1024 * 1024
            firmware = "uefi" if vm.config.firmware == "efi" else "bios"

            # Boot disk keys
            boot_keys: set[int] = set()
            try:
                for item in (vm.config.bootOptions.bootOrder or []):
                    if isinstance(item, vim.vm.BootOptions.BootableDiskDevice):
                        boot_keys.add(item.deviceKey)
            except Exception:
                pass

            disk_types: dict[str, str] = {}
            disks_raw = []
            nic_i = 0
            nics_raw = []

            for dev in hw.device:
                if isinstance(dev, vim.vm.device.VirtualDisk):
                    backing = dev.backing
                    key_str = str(dev.key)
                    order = dev.unitNumber if dev.unitNumber is not None else len(disks_raw)
                    boot = (dev.key in boot_keys) or (not boot_keys and len(disks_raw) == 0)
                    size_bytes = dev.capacityInBytes if hasattr(dev, "capacityInBytes") else 0

                    if isinstance(backing,
                                  vim.vm.device.VirtualDisk.RawDiskMappingVer1BackingInfo):
                        device_name = (backing.deviceName or "").split("/")[-1]
                        serial = self._serial_from_naa(device_name)
                        disk_types[key_str] = "rdm"
                        disks_raw.append({
                            "key_str": key_str, "order": order, "boot": boot,
                            "size_bytes": size_bytes, "disk_type": "rdm",
                            "serial": serial, "fa_volume": "",
                        })
                    elif isinstance(backing,
                                    vim.vm.device.VirtualDisk.FlatVer2BackingInfo):
                        try:
                            is_vvol = (backing.datastore is not None and
                                       backing.datastore.summary.type == "VVOL")
                        except Exception:
                            is_vvol = False
                        if is_vvol:
                            disk_types[key_str] = "vvol"
                            disks_raw.append({
                                "key_str": key_str, "order": order, "boot": boot,
                                "size_bytes": size_bytes, "disk_type": "vvol",
                                "serial": None, "fa_volume": "",
                            })
                        else:
                            disk_types[key_str] = "vmfs"
                            disks_raw.append({
                                "key_str": key_str, "order": order, "boot": boot,
                                "size_bytes": size_bytes, "disk_type": "vmfs",
                                "serial": None, "fa_volume": "",
                            })
                    else:
                        disk_types[key_str] = "vmfs"
                        disks_raw.append({
                            "key_str": key_str, "order": order, "boot": boot,
                            "size_bytes": size_bytes, "disk_type": "vmfs",
                            "serial": None, "fa_volume": "",
                        })

                elif isinstance(dev, vim.vm.device.VirtualEthernetCard):
                    backing = dev.backing
                    mac = dev.macAddress or ""
                    # Normalize the pyVmomi class name (e.g. "vim.vm.device.
                    # VirtualVmxnet3") to a clean logical model ("vmxnet3"); the
                    # destination connector maps it to its own best-fit.
                    model = type(dev).__name__.rsplit(".", 1)[-1].replace("Virtual", "").lower()
                    net_id = ""
                    if isinstance(backing, vim.vm.device.VirtualEthernetCard.NetworkBackingInfo):
                        try:
                            net_id = backing.network._moId
                        except Exception:
                            net_id = backing.deviceName or ""
                    elif isinstance(
                            backing,
                            vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo):
                        try:
                            net_id = backing.port.portgroupKey
                        except Exception:
                            pass
                    nics_raw.append({
                        "mac": mac, "source_network": net_id,
                        "model": model, "order": nic_i,
                    })
                    nic_i += 1

            return {
                "name": name, "vcpus": vcpus, "memory_bytes": memory_bytes,
                "firmware": firmware, "disks_raw": disks_raw,
                "nics_raw": nics_raw, "disk_types": disk_types,
            }

        raw = await asyncio.to_thread(_capture_sync)

        disks: list[DiskSpec] = []
        for dr in raw["disks_raw"]:
            serial = dr["serial"]
            fa_volume = dr["fa_volume"]
            if serial and not fa_volume and self.ctx.array is not None:
                fa_volume = await self.ctx.array.find_volume_name_by_serial(serial) or ""
            disks.append(DiskSpec(
                DiskIdentity(fa_volume=fa_volume, serial=serial,
                             size_bytes=dr["size_bytes"]),
                bus="scsi", order=dr["order"], boot=dr["boot"],
                source_ref=dr["key_str"]))

        nics: list[NicSpec] = [
            NicSpec(mac=n["mac"], source_network=n["source_network"],
                    model=n["model"], order=n["order"])
            for n in raw["nics_raw"]
        ]

        disks.sort(key=lambda d: d.order)
        nics.sort(key=lambda n: n.order)
        if disks and not any(d.boot for d in disks):
            disks[0].boot = True

        # A disk already bound to a specific backing volume (RDM or vVol) must map
        # to a volume on the CONNECTED FlashArray — its device serial is resolved
        # against this array (ctx.array) above, so an empty fa_volume means "not on
        # the connected FA" (whether non-Everpure, or an Everpure volume on a DIFFERENT
        # array). Reject the VM in that case; we can't migrate a disk we can't map.
        # (VMFS disks are EXEMPT: prepare_source_disks clones them onto a NEW FA
        # volume on the connected array, so they don't pre-map. An Everpure-backed RDM
        # that DID resolve is the simplest source and migrates directly.)
        unmappable = [
            d.source_ref for d in disks
            if raw["disk_types"].get(d.source_ref) in ("rdm", "vvol")
            and not d.identity.fa_volume]
        if unmappable:
            raise ConnectionValidationError(
                f"VM {raw['name']!r} has disk(s) ({', '.join(sorted(unmappable))}) that "
                "cannot be mapped to a volume on the connected FlashArray (the device "
                "serial did not resolve on this array). Only disks backed by a volume "
                "on the connected FlashArray can be migrated.")

        return VmSpec(
            name=raw["name"], source_ref=str(vm_ref),
            vcpus=raw["vcpus"], memory_bytes=raw["memory_bytes"],
            firmware=raw["firmware"], disks=disks, nics=nics,
            raw={"vsphere_disk_types": raw["disk_types"]})

    # ----------------------------------------- VMFS → RDM/vVol preparation ---

    @staticmethod
    def _datastore_path_to_vmfs(path: str) -> str:
        """'[datastore] dir/file.vmdk' -> '/vmfs/volumes/datastore/dir/file.vmdk'."""
        ds = path.split("]", 1)[0].lstrip("[").strip()
        rest = path.split("]", 1)[1].strip()
        return f"/vmfs/volumes/{ds}/{rest}"

    @staticmethod
    def _vmfs_path_to_datastore(path: str) -> str:
        """'/vmfs/volumes/datastore/dir/file.vmdk' -> '[datastore] dir/file.vmdk'."""
        rest = path[len("/vmfs/volumes/"):]
        ds, _, tail = rest.partition("/")
        return f"[{ds}] {tail}"

    async def cleanup_migration_scratch(self, spec: "VmSpec") -> None:
        """Delete the temporary phifmig-src clone volumes + RDM pointer vmdks made
        by prepare_source_disks. Idempotent; runs on completion and rollback. Never
        touches the real source vmdks/VM or the destination's volumes."""
        scratch = (spec.raw or {}).get("vsphere_scratch") or []
        if not scratch or self._mock_or_dry() or self.ctx.array is None:
            return

        def _delete_pointer_sync(ds_vmdk: str):
            from pyVmomi import vim
            si = self._si_sync()
            dc = next(iter(self._find_all_sync([vim.Datacenter])), None)
            # DeleteVirtualDisk removes the RDM descriptor + its -rdm mapping pair.
            self._wait_task_sync(
                si.content.virtualDiskManager.DeleteVirtualDisk_Task(
                    name=ds_vmdk, datacenter=dc))

        for entry in scratch:
            vol = entry.get("volume")
            hg = entry.get("host_group")
            ds_vmdk = entry.get("ds_vmdk")
            if ds_vmdk:
                try:
                    await asyncio.to_thread(_delete_pointer_sync, ds_vmdk)
                except Exception:
                    pass  # already gone / never created
            if vol:
                if hg:
                    try:
                        await self.ctx.array.disconnect_volume_from_group(hg, vol)
                    except Exception:
                        pass
                try:
                    await self.ctx.array.delete_volume(vol, eradicate=True)
                    await self.ctx.emit(f"[vsphere] cleaned up scratch volume {vol}")
                except Exception:
                    pass

    @staticmethod
    def _gen_esxi_password() -> str:
        """A policy-compliant random ESXi password (upper/lower/digit/special)."""
        import secrets, string
        base = secrets.token_urlsafe(18)
        return f"Pf1!{base}"[:30]

    def _provision_esxi_ssh_sync(self, vm_ref: str, disk_keys: list[str],
                                 override_user: str, override_pwd: str) -> dict[str, Any]:
        """Resolve the ESXi host running the VM, ensure SSH access + the disk vmdk
        paths. If no override credentials are given, create a temporary local
        Admin account on the host via the vCenter API (HostLocalAccountManager) —
        the equivalent of ``esxcli system account add`` + ``permission set`` — so
        PHIF can run vmkfstools without a pre-shared root password.

        Returns {addr, user, password, created, vmdks}.
        """
        from pyVmomi import vim
        vm = self._get_vm_sync(vm_ref)
        host = vm.runtime.host
        addr = host.name
        try:
            for vnic in host.config.network.vnic:
                if vnic.device == "vmk0" and vnic.spec.ip.ipAddress:
                    addr = vnic.spec.ip.ipAddress
                    break
        except Exception:
            pass

        # Enable the SSH service.
        try:
            ss = host.configManager.serviceSystem
            if not any(s.key == "TSM-SSH" and s.running for s in ss.serviceInfo.service):
                ss.StartService(id="TSM-SSH")
        except Exception:
            pass

        user, password, created = override_user, override_pwd, False
        if not override_pwd:
            user = "phif-migrate"
            password = self._gen_esxi_password()
            am = host.configManager.accountManager
            spec = vim.host.LocalAccountManager.AccountSpecification(
                id=user, password=password, description="PHIF migration (temporary)")
            try:
                am.CreateUser(user=spec)
            except vim.fault.AlreadyExists:
                am.UpdateUser(user=spec)  # reset the password
            # Grant Admin on the HOST (host-local users aren't known to vCenter's
            # AuthorizationManager, so use the host's HostAccessManager — the API
            # equivalent of `esxcli system permission set --role=Admin`).
            host.configManager.hostAccessManager.ChangeAccessMode(
                principal=user, isGroup=False, accessMode="accessAdmin")
            created = True

        paths: dict[str, str] = {}
        for dev in vm.config.hardware.device:
            if isinstance(dev, vim.vm.device.VirtualDisk) and str(dev.key) in disk_keys:
                paths[str(dev.key)] = self._datastore_path_to_vmfs(dev.backing.fileName)
        return {"addr": addr, "user": user, "password": password,
                "created": created, "vmdks": paths}

    def _deprovision_esxi_ssh_sync(self, vm_ref: str, user: str) -> None:
        """Remove the temporary ESXi local account created for the clone (best-effort)."""
        try:
            host = self._get_vm_sync(vm_ref).runtime.host
            host.configManager.accountManager.RemoveUser(userName=user)
        except Exception:
            pass

    _SNAPSHOT_NAME = "phif-migrate-clone"

    def _create_snapshot_sync(self, vm_ref: str) -> None:
        """Snapshot the (running) VM so its base vmdks freeze read-only for a
        consistent hot clone; the guest keeps running, writing to the delta."""
        vm = self._get_vm_sync(vm_ref)
        task = vm.CreateSnapshot_Task(
            name=self._SNAPSHOT_NAME,
            description="PHIF migration: clone base disk while VM runs",
            memory=False, quiesce=False)
        self._wait_task_sync(task)

    def _remove_snapshot_sync(self, vm_ref: str) -> None:
        """Delete the migration snapshot, consolidating the delta back into the base."""
        vm = self._get_vm_sync(vm_ref)
        if not vm.snapshot:
            return

        def _find(nodes):
            for n in nodes:
                if n.name == self._SNAPSHOT_NAME:
                    return n.snapshot
                hit = _find(n.childSnapshotList)
                if hit:
                    return hit
            return None

        snap = _find(vm.snapshot.rootSnapshotList)
        if snap:
            self._wait_task_sync(
                snap.RemoveSnapshot_Task(removeChildren=True, consolidate=True))

    async def prepare_source_disks(self, spec: "VmSpec",
                                   options: dict[str, Any]) -> "VmSpec":
        """Clone VMFS-backed disks onto dedicated FlashArray volumes before migration.

        vVol and RDM disks already map 1:1 to an FA volume and need no preparation.
        VMFS disks are just files in a shared datastore, so each is cloned onto a
        newly-provisioned FA volume presented as a raw device. The ONLY vSphere
        mechanism that actually copies a VMDK's data onto a raw LUN is
        ``vmkfstools -i <src.vmdk> -d rdm:<device> <dst.vmdk>`` run on the ESXi host
        (Storage vMotion rejects an RDM backing, and VirtualDiskManager.CopyVirtualDisk
        to an RDM only creates the mapping without copying). So this SSHes the ESXi
        host that runs the VM and clones via vmkfstools (VAAI-accelerated on the array).

        The source VM is powered off first (cold copy; the vmdk must be unused). The
        original VMFS disks are left intact, so the source stays recoverable.
        """
        from phif.migrate.spec import DiskIdentity
        from phif.migrate.steps import MigrationError

        disk_types = spec.raw.get("vsphere_disk_types", {})
        vmfs_indices = [(i, d) for i, d in enumerate(spec.disks)
                        if disk_types.get(d.source_ref) == "vmfs"]
        if not vmfs_indices:
            return spec

        await self.ctx.emit(
            f"[vsphere] {len(vmfs_indices)} VMFS disk(s) will be cloned onto dedicated "
            "FlashArray volumes (RDM via vmkfstools) before migration")

        host_group = self.migration_host_group()
        if not host_group and not self._mock_or_dry():
            raise MigrationError(
                "vSphere migration requires a host_group to present FA volumes to ESXi")
        cluster = self.ctx.target.get("cluster") or ""
        vm_ref = spec.source_ref

        if self._mock_or_dry():
            for idx, disk in vmfs_indices:
                spec.disks[idx].identity = DiskIdentity(
                    fa_volume=f"phifmig-src-{vm_ref}-disk{disk.order}",
                    serial="mock-rdm-serial", size_bytes=disk.identity.size_bytes)
                disk_types[disk.source_ref] = "rdm"
            spec.raw["vsphere_disk_types"] = disk_types
            return spec

        # Copy mode with the source left running: snapshot the VM so its base vmdks
        # freeze read-only, clone the consistent base, then delete the snapshot
        # (consolidating the delta back). Otherwise (move, or copy+shutdown) power
        # the VM off and clone the disks cold.
        mode = (options.get("mode") or "move").lower()
        hot_clone = mode == "copy" and not bool(options.get("shutdown_source", False))

        # Acquire ESXi SSH on the host running the VM. The local account is used
        # ONLY for SSH (vmkfstools); every vCenter/API operation below (account
        # creation, snapshot, power, deprovision) uses the vCenter connection.
        # Captures each disk's base vmdk path (before any snapshot is taken).
        info = await asyncio.to_thread(
            self._provision_esxi_ssh_sync, vm_ref,
            [d.source_ref for _, d in vmfs_indices],
            self.ctx.target.get("esxi_user") or "root",
            self.ctx.target.get("esxi_password") or "")
        esxi_addr, esxi_user, esxi_pwd = info["addr"], info["user"], info["password"]
        await self.ctx.emit(
            f"[vsphere] ESXi host {esxi_addr}: cloning over SSH as {esxi_user!r}")

        async def _esxi(cmd: str, timeout: float) -> str:
            return await self.ctx.runner.run_ssh(
                esxi_addr, cmd, username=esxi_user, password=esxi_pwd,
                check=True, timeout=timeout)

        snapshot_taken = False
        try:
            if hot_clone:
                await self.ctx.emit(
                    "[vsphere] copy mode (source left running): snapshotting VM to "
                    "freeze a consistent base disk for cloning")
                await asyncio.to_thread(self._create_snapshot_sync, vm_ref)
                snapshot_taken = True
            else:
                await self.ctx.emit("[vsphere] powering off source VM for a cold copy")
                stop = await self.stop_vm(
                    vm_ref, force=bool(options.get("force_stop", False)))
                if not stop.success:
                    raise MigrationError(stop.message)

            scratch: list[dict[str, str]] = []
            for idx, disk in vmfs_indices:
                size = disk.identity.size_bytes or 0
                size_gib = max(1, -(-int(size) // (1024 ** 3)))
                vol_name = f"phifmig-src-{vm_ref}-disk{disk.order}"

                await self.ctx.emit(
                    f"[vsphere] provisioning FA volume {vol_name} ({size_gib}G) "
                    f"for disk {disk.source_ref}")
                try:
                    await self.ctx.array.create_volume(vol_name, f"{size_gib}G")
                except Exception as exc:
                    if "exist" not in str(exc).lower():
                        raise
                try:
                    await self.ctx.array.connect_volume_to_group(host_group, vol_name)
                except Exception as exc:
                    if "exist" not in str(exc).lower():
                        raise
                serial = (await self.ctx.array.get_volume(vol_name) or {}).get("serial") or ""
                device_path = f"/vmfs/devices/disks/{self._naa_from_serial(serial)}"

                # The base vmdk (read-only under the snapshot, or quiesced by the
                # power-off) — captured before the snapshot so it's the base, not
                # the delta.
                src_vmdk = info["vmdks"][disk.source_ref]
                dst_dir = src_vmdk.rsplit("/", 1)[0]
                dst_vmdk = f"{dst_dir}/{vol_name}.vmdk"
                dst_base = f"{dst_dir}/{vol_name}"  # clean <name>.vmdk + <name>-rdm.vmdk

                # Rescan on the ESXi host so the new LUN's device node exists, then
                # clone the disk data onto it as an RDM. Remove any pointer left by
                # a prior attempt first (vmkfstools won't overwrite).
                await self.ctx.emit(
                    f"[vsphere] cloning {disk.source_ref} -> FA volume {vol_name} via vmkfstools")
                await _esxi(
                    f"esxcli storage core adapter rescan --all >/dev/null 2>&1 || true; "
                    f"vmkfstools -U '{dst_vmdk}' >/dev/null 2>&1 || true; "
                    f"rm -f '{dst_base}'*.vmdk >/dev/null 2>&1 || true", 300)
                await _esxi(
                    f"vmkfstools -i '{src_vmdk}' -d rdm:'{device_path}' '{dst_vmdk}'", 3600)

                spec.disks[idx].identity = DiskIdentity(
                    fa_volume=vol_name, serial=serial, size_bytes=size)
                disk_types[disk.source_ref] = "rdm"
                # Record the scratch clone (volume + RDM pointer vmdk) so the
                # migration cleans it up at completion/rollback (see
                # cleanup_migration_scratch).
                scratch.append({
                    "volume": vol_name,
                    "host_group": host_group,
                    "ds_vmdk": self._vmfs_path_to_datastore(dst_vmdk),
                })
                await self.ctx.emit(
                    f"[vsphere] disk {disk.source_ref} now backed by FA volume {vol_name!r}")
            spec.raw["vsphere_scratch"] = scratch
        finally:
            if snapshot_taken:
                await self.ctx.emit("[vsphere] deleting migration snapshot (consolidating)")
                await asyncio.to_thread(self._remove_snapshot_sync, vm_ref)
            if info.get("created"):
                await asyncio.to_thread(self._deprovision_esxi_ssh_sync, vm_ref, esxi_user)

        spec.raw["vsphere_disk_types"] = disk_types
        return spec

    async def _svmotion_disk(self, vm_ref: str,
                             disk_key: str, *, backing: dict[str, Any]) -> None:
        """Storage vMotion a single disk to a new backing via pyVmomi RelocateSpec.

        ``backing`` dict keys:
        * ``"type"`` — ``"RAW_DEVICE_MAPPING"`` or ``"VIRTUAL_VOLUME_BACKING"``
        * ``"device_name"`` — device path (RDM only)
        * ``"datastore"`` — datastore name or moId (vVol or datastore relocate)
        """
        def _svmotion_sync():
            from pyVmomi import vim
            vm_obj = self._get_vm_sync(vm_ref)
            disk_key_int = int(disk_key) if str(disk_key).lstrip("-").isdigit() else None

            # Find the disk device
            disk_dev = None
            for dev in vm_obj.config.hardware.device:
                if isinstance(dev, vim.vm.device.VirtualDisk):
                    if disk_key_int is not None and dev.key == disk_key_int:
                        disk_dev = dev
                        break
                    if str(dev.key) == str(disk_key):
                        disk_dev = dev
                        break
            if disk_dev is None:
                raise ValueError(f"Disk key {disk_key!r} not found on VM {vm_ref!r}")

            disk_locator = vim.vm.RelocateSpec.DiskLocator(deviceKey=disk_dev.key)

            b_type = (backing.get("type") or "").upper()
            if b_type == "RAW_DEVICE_MAPPING":
                device_name = backing.get("device_name", "")
                disk_locator.diskBackingInfo = (
                    vim.vm.device.VirtualDisk.RawDiskMappingVer1BackingInfo(
                        deviceName=device_name,
                        compatibilityMode="physicalMode",
                        diskMode="persistent",
                    )
                )

            ds_ref = None
            ds_name = backing.get("datastore")
            if ds_name:
                ds_ref = self._find_one_sync([vim.Datastore], ds_name)
                if ds_ref is None:
                    # Try by moId
                    for ds in self._find_all_sync([vim.Datastore]):
                        if ds._moId == ds_name:
                            ds_ref = ds
                            break
                if ds_ref:
                    disk_locator.datastore = ds_ref

            relocate_spec = vim.vm.RelocateSpec(disk=[disk_locator])
            task = vm_obj.RelocateVM_Task(spec=relocate_spec)
            self._wait_task_sync(task, timeout=600)

        await asyncio.to_thread(_svmotion_sync)

    async def _get_vm_disk(self, session: Any, vm_ref: str,
                           disk_key: str) -> dict[str, Any]:
        """Re-read a single VM disk's config from vCenter via pyVmomi after an svmotion.

        The ``session`` parameter is kept for signature compatibility but unused;
        pyVmomi is used instead.
        """
        def _get_sync():
            from pyVmomi import vim
            vm_obj = self._get_vm_sync(vm_ref)
            disk_key_int = int(disk_key) if str(disk_key).lstrip("-").isdigit() else None
            for dev in vm_obj.config.hardware.device:
                if not isinstance(dev, vim.vm.device.VirtualDisk):
                    continue
                if disk_key_int is not None and dev.key == disk_key_int:
                    pass
                elif str(dev.key) != str(disk_key):
                    continue
                backing = dev.backing
                result: dict[str, Any] = {"key": dev.key}
                if isinstance(
                        backing,
                        vim.vm.device.VirtualDisk.RawDiskMappingVer1BackingInfo):
                    result["backing"] = {
                        "type": "RAW_DEVICE_MAPPING",
                        "device_name": backing.deviceName or "",
                    }
                elif isinstance(backing, vim.vm.device.VirtualDisk.FlatVer2BackingInfo):
                    try:
                        is_vvol = (backing.datastore is not None and
                                   backing.datastore.summary.type == "VVOL")
                    except Exception:
                        is_vvol = False
                    if is_vvol:
                        # Attempt to extract vVol id from backing file path
                        fp = getattr(backing, "fileName", "") or ""
                        result["backing"] = {
                            "type": "VIRTUAL_VOLUME_BACKING",
                            "vvol_id": fp,
                        }
                    else:
                        result["backing"] = {"type": "VMDK_FILE",
                                             "file_name": getattr(backing, "fileName", "")}
                else:
                    result["backing"] = {"type": "UNKNOWN"}
                return result
            return {}

        try:
            return await asyncio.to_thread(_get_sync)
        except Exception:
            return {}

    # ----------------------------------------- VM lifecycle (MIGRATE dest) ---

    async def stop_vm(self, vm_ref: str, *, force: bool = False) -> OpResult:
        """Power off a VM, gracefully first.

        Always attempts a graceful guest shutdown (requires VMware Tools). If that
        fails or the guest does not power off within the timeout, the VM is hard
        powered off ONLY when ``force`` is set; otherwise this returns a failure so
        the migration can surface that 'force power off' is required.
        """
        if await self.power_state(vm_ref) == "stopped":
            return OpResult.ok(f"VM {vm_ref} already stopped")

        def _stop_sync() -> bool:
            from pyVmomi import vim
            vm_obj = self._get_vm_sync(vm_ref)
            # Graceful guest shutdown first (no-op return if Tools absent).
            graceful_requested = False
            try:
                vm_obj.ShutdownGuest()
                graceful_requested = True
            except Exception:
                graceful_requested = False
            if graceful_requested:
                deadline = time.monotonic() + 120
                while time.monotonic() < deadline:
                    if vm_obj.runtime.powerState == vim.VirtualMachine.PowerState.poweredOff:
                        return True
                    time.sleep(5)
            # Graceful failed or timed out — hard power off only if allowed.
            if force:
                self._wait_task_sync(vm_obj.PowerOffVM_Task())
                return True
            return False

        stopped = await asyncio.to_thread(_stop_sync)
        if not stopped:
            return OpResult.fail(
                f"VM {vm_ref} did not shut down gracefully (VMware Tools missing or "
                "the guest ignored the request). Enable 'force power off' to hard-stop it.")
        return OpResult.ok(f"Stopped VM {vm_ref}")

    async def start_vm(self, vm_ref: str) -> OpResult:
        if await self.power_state(vm_ref) == "running":
            return OpResult.ok(f"VM {vm_ref} already running")

        # Rescan ESXi storage first: a destination RDM's FA volume may have just
        # been (re)written by an array-level copy, leaving the host's view of the
        # LUN stale — power-on then fails with "Unable to enumerate all disks".
        # Retry the power-on across rescans so the device settles.
        cluster = self.ctx.target.get("cluster") or ""

        def _start_sync():
            vm_obj = self._get_vm_sync(vm_ref)
            self._wait_task_sync(vm_obj.PowerOnVM_Task())

        last_exc: Exception | None = None
        for attempt in range(4):
            await self._rescan_storage(cluster)
            try:
                await asyncio.to_thread(_start_sync)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if "enumerate" not in str(exc).lower() or attempt == 3:
                    break
                await self.ctx.emit(
                    f"[vsphere] power-on attempt {attempt + 1}: {exc}; rescanning + retrying")
                await asyncio.sleep(5)
        if last_exc is not None:
            return OpResult.fail(f"power-on failed: {last_exc}")
        return OpResult.ok(f"Started VM {vm_ref}")

    @staticmethod
    def _rdm_pointer_path_sync(vm_obj, suffix: str) -> str:
        """Build a path for an RDM mapping pointer .vmdk in the VM's home directory.

        ``vm_obj.config.files.vmPathName`` is like ``[ds1] myvm/myvm.vmx``; the RDM
        pointer must live alongside it (``[ds1] myvm/<suffix>``). An RDM device added
        WITHOUT an explicit ``fileName`` + ``fileOperation=create`` leaves vCenter
        with no mapping file on the datastore, so power-on fails with "The file
        specified is not a virtual disk"."""
        import re
        vmx = vm_obj.config.files.vmPathName or ""
        m = re.match(r"^\[(.+?)\]\s*(.*)$", vmx)
        if not m:
            return f"[{vmx}] {suffix}"
        ds, path = m.group(1), m.group(2)
        folder = path.rsplit("/", 1)[0] if "/" in path else ""
        prefix = f"{folder}/" if folder else ""
        return f"[{ds}] {prefix}{suffix}"

    def _pick_vm_datastore_sync(self) -> str:
        """Return a datastore name for the destination VM's home (config + RDM
        pointer files). Honors target['vm_datastore']; else the first accessible
        VMFS datastore (RDM pointers must live on VMFS, not vVol)."""
        from pyVmomi import vim
        override = self.ctx.target.get("vm_datastore")
        dss = self._find_all_sync([vim.Datastore])
        if override:
            for ds in dss:
                if ds.name == override or ds._moId == override:
                    return ds.name
        for ds in dss:
            try:
                if ds.summary.accessible and ds.summary.type == "VMFS":
                    return ds.name
            except Exception:
                continue
        for ds in dss:
            try:
                if ds.summary.accessible:
                    return ds.name
            except Exception:
                continue
        return ""

    def _host_for_storage_sync(self):
        """An ESXi HostSystem to drive datastore (VMFS create/remove) operations."""
        from pyVmomi import vim
        cluster_name = self.ctx.target.get("cluster") or ""
        if cluster_name:
            cl = self._find_one_sync([vim.ClusterComputeResource], cluster_name)
            if cl and cl.host:
                return list(cl.host)[0]
        hosts = self._find_all_sync([vim.HostSystem])
        return hosts[0] if hosts else None

    def _fa_backed_vmfs_sync(self) -> list[tuple[str, str]]:
        """Return (datastore_name, extent_serial) for every accessible VMFS datastore,
        so the caller can keep only those whose extent LUN is on this FlashArray (the
        precondition for an XCOPY-accelerated, same-array conversion)."""
        from pyVmomi import vim
        out: list[tuple[str, str]] = []
        for ds in self._find_all_sync([vim.Datastore]):
            try:
                if not (ds.summary.accessible and ds.summary.type == "VMFS"):
                    continue
                extents = getattr(getattr(ds.info, "vmfs", None), "extent", []) or []
                for ex in extents:
                    serial = self._serial_from_naa(ex.diskName)
                    if serial:
                        out.append((ds.name, serial))
                        break
            except Exception:
                continue
        return out

    async def _pick_fa_backed_vmfs(self, preferred: str | None = None) -> str:
        """Return the name of a VMFS datastore whose extent LUN is on THIS connector's
        FlashArray (so a same-array, XCOPY-offloaded clone is possible). Honors a
        ``preferred`` name when it is itself FA-backed; else the first FA-backed VMFS."""
        pairs = await asyncio.to_thread(self._fa_backed_vmfs_sync)
        fa: list[str] = []
        for name, serial in pairs:
            if self.ctx.array is not None and \
                    await self.ctx.array.find_volume_name_by_serial(serial):
                fa.append(name)
        if preferred and preferred in fa:
            return preferred
        return fa[0] if fa else ""

    async def _provision_scratch_vmfs(self, tag: str,
                                      size_gib: int = 16) -> tuple[str, str]:
        """Create a temporary FA volume and format it as a VMFS datastore on the host.

        Returns (datastore_name, fa_volume_name). The destination VM's home + RDM
        pointers go on this scratch datastore so the later whole-VM Storage vMotion to
        the destination VMFS is a REAL cross-datastore move (which converts the
        virtual-mode RDMs to VMDKs); and because the scratch VMFS, the RDM raw LUNs,
        and the destination VMFS are all on the same FlashArray, that conversion copy
        is XCOPY-offloaded to the array.

        ``size_gib`` must cover the VM's disks: a virtual-mode RDM is accounted against
        the datastore holding its pointer, so vSphere rejects the conversion svMotion
        with "Insufficient disk space" if the scratch is smaller than the disks. The FA
        volume is thin-provisioned, so an over-sized scratch costs nothing until used."""
        if self.ctx.array is None:
            raise RuntimeError("no FlashArray to provision a scratch VMFS datastore")
        vol = f"phifmig-scratch-{tag}"
        ds_name = f"phifmig-scratch-{tag}-ds"
        hg = self.migration_host_group()
        await self.ctx.array.create_volume(vol, f"{max(16, int(size_gib))}G")
        if hg:
            await self.ctx.array.connect_volume_to_group(hg, vol)
        info = await self.ctx.array.get_volume(vol) or {}
        serial = info.get("serial") or ""
        await self._rescan_storage(self.ctx.target.get("cluster") or "")
        devpath = f"/vmfs/devices/disks/{self._naa_from_serial(serial)}"

        def _make() -> str:
            host = self._host_for_storage_sync()
            if host is None:
                raise RuntimeError("no ESXi host to create the scratch VMFS")
            dss = host.configManager.datastoreSystem
            opts = dss.QueryVmfsDatastoreCreateOptions(devpath)
            if not opts:
                raise RuntimeError(f"no VMFS create options for {devpath}")
            spec = opts[0].spec
            spec.vmfs.volumeName = ds_name
            dss.CreateVmfsDatastore(spec)
            return ds_name

        name = await asyncio.to_thread(_make)
        await self.ctx.emit(
            f"[vsphere] provisioned scratch VMFS {name} (FA volume {vol})")
        return name, vol

    async def _teardown_scratch_vmfs(self, ds_name: str, vol: str) -> None:
        """Remove the scratch VMFS datastore and eradicate its FA volume. Idempotent."""
        if ds_name:
            def _remove():
                from pyVmomi import vim
                host = self._host_for_storage_sync()
                if host is None:
                    return
                dss = host.configManager.datastoreSystem
                for d in self._find_all_sync([vim.Datastore]):
                    if d.name == ds_name:
                        dss.RemoveDatastore(d)
                        break
            try:
                await asyncio.to_thread(_remove)
            except Exception as exc:  # noqa: BLE001
                await self.ctx.emit(
                    f"[vsphere] WARNING: could not remove scratch VMFS {ds_name}: {exc}")
        if vol and self.ctx.array is not None:
            hg = self.migration_host_group()
            try:
                if hg:
                    await self.ctx.array.disconnect_volume_from_group(hg, vol)
                await self.ctx.array.delete_volume(vol, eradicate=True)
                await self.ctx.emit(f"[vsphere] removed scratch FA volume {vol}")
            except Exception as exc:  # noqa: BLE001
                await self.ctx.emit(
                    f"[vsphere] WARNING: could not delete scratch volume {vol}: {exc}")

    async def create_vm(self, spec: "VmSpec", *,
                        network_map: dict[str, str],
                        placement: dict[str, Any] | None = None) -> OpResult:
        """Create a VM shell (no disks) in vCenter with the spec's CPU/RAM/NICs via pyVmomi."""
        from phif.migrate.spec import VmSpec as VS  # noqa: F401

        existing_names = {v.get("name") for v in await self.list_vms()}
        name = spec.name
        if name in existing_names:
            i = 2
            while f"{spec.name}-{i}" in existing_names:
                i += 1
            name = f"{spec.name}-{i}"
            await self.ctx.emit(
                f"Name {spec.name!r} already exists; using {name!r} instead")

        if self._mock_or_dry():
            await self.ctx.emit(f"[dry-run] would create vCenter VM {name!r}")
            return OpResult.ok(f"VM {name} created", artifacts={"vm_ref": f"vm-mock-{name}"})

        # Validate all NIC mappings before touching vCenter
        for nic in spec.nics:
            if not network_map.get(nic.source_network):
                return OpResult.fail(
                    f"no destination network mapped for source NIC {nic.source_network!r}")

        firmware_str = spec.firmware
        memory_mb = max(16, spec.memory_bytes // (1024 * 1024))
        vcpus = max(1, spec.vcpus)
        nics_info = [(nic.mac, network_map[nic.source_network]) for nic in spec.nics]

        # When the migration will convert RDMs to VMFS VMDKs (opt-in), self-provision a
        # temporary FA-backed VMFS datastore and put the VM home + RDM pointers on it,
        # so the later whole-VM Storage vMotion to the destination VMFS converts the
        # vRDMs (a same-datastore relocate is a no-op). Conversion is a one-time host-
        # side copy (no method offloads a raw-LUN→VMFS copy via XCOPY); keeping RDMs is
        # the zero-copy default.
        forced_home = ""
        if bool((placement or {}).get("convert_to_vmfs")):
            import re
            import uuid
            # Unique per migration (uuid suffix) so concurrent migrations of like-named
            # VMs never collide on the scratch volume/datastore name.
            base = re.sub(r"[^A-Za-z0-9-]", "-", name)[:24].strip("-") or "vm"
            tag = f"{base}-{uuid.uuid4().hex[:8]}"
            # The scratch only holds the VM home (config/.nvram/logs) + the RDM pointer
            # files, which are tiny regardless of the mapped LUN size (a 2 TB vRDM
            # pointer fits on a small datastore). A small fixed size is plenty.
            scratch_ds, scratch_vol = await self._provision_scratch_vmfs(tag, 16)
            self._scratch_vmfs = {"ds": scratch_ds, "vol": scratch_vol}
            forced_home = scratch_ds
        else:
            # Keep-RDM: honor the operator's chosen datastore (the wizard's "Storage"
            # selection) for the VM home + RDM pointers; else auto-pick a VMFS.
            forced_home = (placement or {}).get("storage") or ""

        def _create_sync():
            from pyVmomi import vim
            pool = self._default_resource_pool_sync()
            folder = self._default_vm_folder_sync()
            if pool is None:
                raise RuntimeError("No resource pool found in vCenter")
            if folder is None:
                raise RuntimeError("No VM folder found in vCenter")

            ds_home = forced_home or self._pick_vm_datastore_sync()
            if not ds_home:
                raise RuntimeError("No accessible datastore for the VM home/config files")

            config_spec = vim.vm.ConfigSpec(
                name=name,
                numCPUs=vcpus,
                memoryMB=memory_mb,
                guestId="otherGuest64",
                # CreateVM_Task requires a datastore for the .vmx/home (and RDM
                # pointer .vmdk files land here too).
                files=vim.vm.FileInfo(vmPathName=f"[{ds_home}]"),
            )
            if firmware_str == "uefi":
                config_spec.firmware = "efi"

            device_changes = []
            # Add a SCSI controller
            scsi_ctrl = vim.vm.device.VirtualLsiLogicSASController(
                key=-100,
                busNumber=0,
                sharedBus=vim.vm.device.VirtualSCSIController.Sharing.noSharing,
            )
            device_changes.append(vim.vm.device.VirtualDeviceSpec(
                operation=vim.vm.device.VirtualDeviceSpec.Operation.add,
                device=scsi_ctrl,
            ))

            for i, (mac, dest_net_id) in enumerate(nics_info):
                # Resolve network object
                net_obj = None
                # Try by moId first
                for n in self._find_all_sync(
                        [vim.Network, vim.dvs.DistributedVirtualPortgroup]):
                    if n._moId == dest_net_id or n.name == dest_net_id:
                        net_obj = n
                        break

                if net_obj is None:
                    raise ValueError(f"Network {dest_net_id!r} not found in vCenter")

                nic_dev = vim.vm.device.VirtualVmxnet3()
                nic_dev.key = -(200 + i)
                if mac:
                    # Preserve the source MAC.
                    nic_dev.addressType = "manual"
                    nic_dev.macAddress = mac
                else:
                    # Source reported no MAC (e.g. HPE VME) — let vCenter assign one
                    # ("manual" + a null macAddress is an invalid device config).
                    nic_dev.addressType = "generated"

                if isinstance(net_obj, vim.dvs.DistributedVirtualPortgroup):
                    try:
                        switch_uuid = net_obj.config.distributedVirtualSwitch.uuid
                    except Exception:
                        switch_uuid = ""
                    nic_dev.backing = (
                        vim.vm.device.VirtualEthernetCard.DistributedVirtualPortBackingInfo(
                            port=vim.dvs.PortConnection(
                                portgroupKey=net_obj._moId,
                                switchUuid=switch_uuid,
                            )
                        )
                    )
                else:
                    nic_dev.backing = (
                        vim.vm.device.VirtualEthernetCard.NetworkBackingInfo(
                            network=net_obj,
                            deviceName=net_obj.name,
                        )
                    )

                device_changes.append(vim.vm.device.VirtualDeviceSpec(
                    operation=vim.vm.device.VirtualDeviceSpec.Operation.add,
                    device=nic_dev,
                ))

            config_spec.deviceChange = device_changes
            task = folder.CreateVM_Task(config=config_spec, pool=pool)
            new_vm = self._wait_task_sync(task)
            return new_vm._moId

        try:
            vm_id = await asyncio.to_thread(_create_sync)
        except Exception:
            # VM creation failed AFTER a scratch datastore was provisioned — no vm_ref
            # will be returned, so the rollback's delete_vm never runs. Tear the
            # scratch down here so it does not leak.
            scratch = getattr(self, "_scratch_vmfs", None)
            if scratch:
                await self._teardown_scratch_vmfs(
                    scratch.get("ds", ""), scratch.get("vol", ""))
                self._scratch_vmfs = None
            raise
        await self.ctx.emit(f"[vsphere] created VM {vm_id} ({name})")
        return OpResult.ok(f"VM {vm_id} created", artifacts={"vm_ref": vm_id})

    async def create_managed_disk(self, vm_ref: str, *, size_bytes: int,
                                  order: int, boot: bool) -> str:
        """Create a new FA-backed disk on the destination VM."""
        if self._mock_or_dry():
            return f"phifmig-dst-{vm_ref}-disk{order}"

        size_gib = max(1, -(-int(size_bytes or 1024 ** 3) // (1024 ** 3)))
        vvol_ds = await self._find_vvol_datastore()

        if vvol_ds:
            # vVol path: create disk through vCenter, VASA provisions FA volume
            def _create_vvol_disk_sync():
                from pyVmomi import vim
                vm_obj = self._get_vm_sync(vm_ref)
                ctrl = next(
                    (d for d in vm_obj.config.hardware.device
                     if isinstance(d, vim.vm.device.VirtualSCSIController)),
                    None)
                if ctrl is None:
                    raise RuntimeError("No SCSI controller found on VM")
                used_units = {
                    d.unitNumber for d in vm_obj.config.hardware.device
                    if (hasattr(d, "controllerKey") and d.controllerKey == ctrl.key
                        and hasattr(d, "unitNumber"))
                }
                unit_num = next(u for u in range(16) if u not in used_units and u != 7)

                ds_obj = None
                for ds in self._find_all_sync([vim.Datastore]):
                    if ds._moId == vvol_ds or ds.name == vvol_ds:
                        ds_obj = ds
                        break

                backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo(
                    diskMode="persistent",
                    thinProvisioned=True,
                )
                if ds_obj:
                    backing.datastore = ds_obj

                disk_dev = vim.vm.device.VirtualDisk(
                    backing=backing,
                    controllerKey=ctrl.key,
                    unitNumber=unit_num,
                    key=-(300 + order),
                    capacityInKB=size_gib * 1024 * 1024,
                )
                dev_spec = vim.vm.device.VirtualDeviceSpec(
                    operation=vim.vm.device.VirtualDeviceSpec.Operation.add,
                    fileOperation=vim.vm.device.VirtualDeviceSpec.FileOperation.create,
                    device=disk_dev,
                )
                task = vm_obj.ReconfigVM_Task(vim.vm.ConfigSpec(deviceChange=[dev_spec]))
                self._wait_task_sync(task)
                # Re-read to find the new disk key
                vm_obj2 = self._get_vm_sync(vm_ref)
                for dev in vm_obj2.config.hardware.device:
                    if (isinstance(dev, vim.vm.device.VirtualDisk)
                            and dev.unitNumber == unit_num
                            and dev.controllerKey == ctrl.key):
                        return str(dev.key)
                return ""

            disk_key = await asyncio.to_thread(_create_vvol_disk_sync)
            if disk_key and self.ctx.array is not None:
                dsk = await self._get_vm_disk(None, vm_ref, disk_key)
                vvol_raw = (dsk.get("backing") or {}).get("vvol_id") or ""
                vvol_id = (vvol_raw.get("id") if isinstance(vvol_raw, dict)
                           else str(vvol_raw)) if vvol_raw else ""
                if vvol_id:
                    fa_volume = await self.ctx.array.find_volume_name_by_serial(vvol_id) or ""
                    if fa_volume:
                        await self.ctx.emit(
                            f"[vsphere] created vVol disk {fa_volume} ({size_gib}G) "
                            f"on VM {vm_ref}")
                        return fa_volume

        # RDM path: create the FA volume ONLY. The host-group connection + the RDM
        # device are deferred to finalize_destination_disks, which runs AFTER the
        # array copy_volume(overwrite). The copy resizes the volume to the source's
        # exact size, so attaching the RDM beforehand would bake in a stale capacity
        # (full-disk reads then run off the end → "LBA out of range"/"Not supported").
        # Doing all the array-side work first, then attach+rescan+create-RDM, records
        # the correct final geometry.
        vol_name = f"phifmig-dst-{vm_ref}-disk{order}"
        if self.ctx.array is not None:
            await self.ctx.array.create_volume(vol_name, f"{size_gib}G")
        await self.ctx.emit(
            f"[vsphere] created managed FA volume {vol_name} ({size_gib}G) for VM "
            f"{vm_ref} (RDM attached after copy)")
        return vol_name

    async def attach_existing_volumes(self, vm_ref: str,
                                      disks: "list[DiskSpec]") -> OpResult:
        """Attach pre-existing FA volumes to the destination VM as RDMs via pyVmomi."""
        if self._mock_or_dry():
            return OpResult.ok(f"[mock] attached {len(disks)} disk(s) to VM {vm_ref}")

        cluster = self.ctx.target.get("cluster") or ""
        await self._rescan_storage(cluster)

        for i, disk in enumerate(disks):
            serial = disk.identity.serial
            if not serial and disk.identity.fa_volume and self.ctx.array is not None:
                vol = await self.ctx.array.get_volume(disk.identity.fa_volume) or {}
                serial = vol.get("serial") or ""
            if not serial:
                return OpResult.fail(
                    f"cannot attach disk {disk.identity.fa_volume!r}: no serial available")
            naa = self._naa_from_serial(serial)
            device_path = f"/vmfs/devices/disks/{naa}"
            disk_index = i  # capture for closure

            def _attach_sync(dev_path=device_path, idx=disk_index):
                from pyVmomi import vim
                vm_obj = self._get_vm_sync(vm_ref)
                ctrl = next(
                    (d for d in vm_obj.config.hardware.device
                     if isinstance(d, vim.vm.device.VirtualSCSIController)),
                    None)
                if ctrl is None:
                    raise RuntimeError("No SCSI controller found on VM")
                used_units = {
                    d.unitNumber for d in vm_obj.config.hardware.device
                    if (hasattr(d, "controllerKey") and d.controllerKey == ctrl.key
                        and hasattr(d, "unitNumber"))
                }
                unit_num = next(u for u in range(16) if u not in used_units and u != 7)
                pointer = self._rdm_pointer_path_sync(
                    vm_obj, f"{vm_obj.name}_rdm{idx}.vmdk")
                rdm_backing = vim.vm.device.VirtualDisk.RawDiskMappingVer1BackingInfo(
                    fileName=pointer,
                    deviceName=dev_path,
                    compatibilityMode="virtualMode",
                    # independent_persistent: avoids the full-disk snapshot-space
                    # reservation on the pointer's datastore (see create_managed_disk).
                    diskMode="independent_persistent",
                )
                rdm_disk = vim.vm.device.VirtualDisk(
                    backing=rdm_backing,
                    controllerKey=ctrl.key,
                    unitNumber=unit_num,
                    key=-(100 + idx),
                )
                dev_spec = vim.vm.device.VirtualDeviceSpec(
                    operation=vim.vm.device.VirtualDeviceSpec.Operation.add,
                    # create the mapping pointer .vmdk on the datastore (required).
                    fileOperation=vim.vm.device.VirtualDeviceSpec.FileOperation.create,
                    device=rdm_disk,
                )
                task = vm_obj.ReconfigVM_Task(vim.vm.ConfigSpec(deviceChange=[dev_spec]))
                self._wait_task_sync(task)

            await asyncio.to_thread(_attach_sync)
            await self.ctx.emit(f"[vsphere] attached RDM {naa} to VM {vm_ref}")

        return OpResult.ok(f"Attached {len(disks)} disk(s) to VM {vm_ref}")

    async def finalize_destination_disks(self, vm_ref: str,
                                         disks: "list[DiskSpec]") -> OpResult:
        """Attach the managed FA volumes as RDMs AFTER the array copy.

        ``create_managed_disk`` only creates the FA volume; ``copy_volume(overwrite)``
        then makes it identical to the source — INCLUDING its size (the source may not
        be GiB-aligned, e.g. HPE VME). Connecting + creating the RDM HERE — after the
        copy — records the device's FINAL geometry. (Creating the RDM before the copy
        baked in a stale capacity, so a full-disk read ran off the end of the device:
        ``ILLEGAL REQUEST / LBA out of range`` → buffer-cache read "Not supported",
        seen as the convert svMotion's "Error caused by file <…>-rdm.vmdk". Boot reads
        only real data so it tolerated the mismatch; the conversion reads the whole
        disk.) Idempotent: skips disks already attached (e.g. vVol)."""
        if self._mock_or_dry():
            return OpResult.ok(f"[mock] finalize {len(disks)} disk(s) on {vm_ref}")
        if self.ctx.array is None:
            return OpResult.ok(f"no array; nothing to finalize on {vm_ref}")

        host_group = self.migration_host_group()
        # Resolve serial per disk and connect each managed volume to the host group
        # (idempotent), THEN rescan once so the post-copy devices appear at full size.
        pending = []  # (vol, serial_lower, order)
        for disk in disks:
            vol = disk.identity.fa_volume
            if not vol:
                continue
            serial = disk.identity.serial
            if not serial:
                v = await self.ctx.array.get_volume(vol) or {}
                serial = v.get("serial") or ""
            if not serial:
                continue
            if host_group:
                try:
                    await self.ctx.array.connect_volume_to_group(host_group, vol)
                except Exception as exc:  # noqa: BLE001
                    if "exist" not in str(exc).lower():
                        raise
            pending.append((vol, serial.lower(), disk.order))
        if not pending:
            return OpResult.ok(f"no RDM disks to finalize on {vm_ref}")

        await self._rescan_storage(self.ctx.target.get("cluster") or "")

        def _attach_sync(items: list[tuple[str, str, int]]) -> int:
            from pyVmomi import vim
            created = 0
            for vol, serial, order in items:
                vm_obj = self._get_vm_sync(vm_ref)
                # Already represented as a device (e.g. vVol, or a prior attach)? skip.
                attached = False
                for dev in vm_obj.config.hardware.device:
                    if not isinstance(dev, vim.vm.device.VirtualDisk):
                        continue
                    b = dev.backing
                    blob = ((getattr(b, "deviceName", "") or "") + " "
                            + (getattr(b, "lunUuid", "") or "") + " "
                            + (getattr(b, "fileName", "") or "") + " "
                            + str(getattr(b, "backingObjectId", "") or "")).lower()
                    if serial in blob:
                        attached = True
                        break
                if attached:
                    continue
                ctrl = next((d for d in vm_obj.config.hardware.device
                             if isinstance(d, vim.vm.device.VirtualSCSIController)), None)
                if ctrl is None:
                    raise RuntimeError("No SCSI controller found on VM")
                used = {d.unitNumber for d in vm_obj.config.hardware.device
                        if getattr(d, "controllerKey", None) == ctrl.key
                        and hasattr(d, "unitNumber")}
                # Prefer unit == disk order (so set_boot's unit==order match holds),
                # else the next free slot; never the controller's reserved unit 7.
                unit = order if (order not in used and order != 7) else \
                    next(u for u in range(16) if u not in used and u != 7)
                device_path = f"/vmfs/devices/disks/{self._naa_from_serial(serial)}"
                pointer = self._rdm_pointer_path_sync(
                    vm_obj, f"{vm_obj.name}_rdm{unit}.vmdk")
                backing = vim.vm.device.VirtualDisk.RawDiskMappingVer1BackingInfo(
                    fileName=pointer,
                    deviceName=device_path,
                    compatibilityMode="virtualMode",
                    # independent_persistent: a snapshottable ("persistent") vRDM makes
                    # vCenter reserve the full disk size on the pointer's datastore,
                    # overflowing a small scratch. Independent → no reservation (matches
                    # the vSphere UI's RDM-create spec).
                    diskMode="independent_persistent",
                )
                disk_dev = vim.vm.device.VirtualDisk(
                    backing=backing, controllerKey=ctrl.key, unitNumber=unit,
                    key=-(400 + order))
                add = vim.vm.device.VirtualDeviceSpec(
                    operation=vim.vm.device.VirtualDeviceSpec.Operation.add,
                    # create the mapping pointer .vmdk (without this the device
                    # references a file that never gets written).
                    fileOperation=vim.vm.device.VirtualDeviceSpec.FileOperation.create,
                    device=disk_dev)
                self._wait_task_sync(vm_obj.ReconfigVM_Task(
                    vim.vm.ConfigSpec(deviceChange=[add])))
                created += 1
            return created

        n = await asyncio.to_thread(_attach_sync, pending)
        await self.ctx.emit(
            f"[vsphere] attached {n} RDM(s) on {vm_ref} (post-copy, correct geometry)")
        return OpResult.ok(f"finalized {n} RDM disk(s) on {vm_ref}")

    async def convert_disks_to_native(self, vm_ref: str,
                                      disks: "list[DiskSpec]", *,
                                      datastore: str | None = None) -> OpResult:
        """Convert the VM's virtual-mode RDMs to native VMFS VMDKs via a whole-VM
        Storage vMotion off the scratch datastore onto a FlashArray-backed VMFS, then
        free the per-disk ``phifmig-dst-*`` FA volumes and tear down the scratch
        datastore. The RDM pointers live on the scratch datastore, so relocating the VM
        to a DIFFERENT VMFS converts the vRDMs (a same-datastore relocate is a no-op).

        NOTE: this is a one-time HOST-side copy. No method offloads a raw-LUN→VMFS copy
        via XCOPY (a vmkfstools clone reading an RDM host-copies too, and a thick clone
        fails), so the array-offloaded zero-copy option is to KEEP the disks as RDMs
        (convert_to_vmfs off). svMotion is used here because it is the supported vCenter
        operation (no ESXi SSH, reliable) for the conversion when the operator opts in.
        FA volumes are freed ONLY after each disk is verified no longer an RDM."""
        if self._mock_or_dry():
            return OpResult.ok(f"[mock] convert {len(disks)} disk(s) on {vm_ref}")

        cluster = self.ctx.target.get("cluster") or ""
        await self._rescan_storage(cluster)

        # Match RDM devices to their FA volume by the serial in deviceName/lunUuid.
        serial_to_vol: dict[str, str] = {}
        for disk in disks:
            vol = disk.identity.fa_volume
            serial = disk.identity.serial
            if not serial and vol and self.ctx.array is not None:
                v = await self.ctx.array.get_volume(vol) or {}
                serial = v.get("serial") or ""
            if serial and vol:
                serial_to_vol[serial.lower()] = vol
        if not serial_to_vol:
            return OpResult.ok(f"no RDM disks to convert on {vm_ref}")

        # Target VMFS: FA-backed (same array), not the scratch datastore the pointers
        # live on (a same-datastore relocate would not convert). Honor `datastore` only
        # if it is itself FA-backed.
        scratch_ds = (getattr(self, "_scratch_vmfs", None) or {}).get("ds")
        fa_pairs = await asyncio.to_thread(self._fa_backed_vmfs_sync)
        fa_targets: list[str] = []
        for name, serial in fa_pairs:
            if name == scratch_ds:
                continue
            if self.ctx.array is not None and \
                    await self.ctx.array.find_volume_name_by_serial(serial):
                fa_targets.append(name)
        if datastore and datastore in fa_targets:
            target_name = datastore
        elif fa_targets:
            target_name = fa_targets[0]
        else:
            return OpResult.fail(
                "no FlashArray-backed VMFS datastore available as a conversion target; "
                "FA volumes left intact")

        def _relocate_sync(s2v: dict[str, str], tgt_name: str) -> list[str]:
            """svMotion the WHOLE VM to the target VMFS, converting each vRDM to a VMDK,
            then VERIFY the backings flipped. Returns ONLY the FA volumes whose disk is
            confirmed no longer an RDM (never frees a volume that is still live)."""
            from pyVmomi import vim
            vm_obj = self._get_vm_sync(vm_ref)
            target_ds = next((ds for ds in self._find_all_sync([vim.Datastore])
                              if ds.name == tgt_name), None)
            if target_ds is None:
                raise RuntimeError(f"target datastore {tgt_name!r} not found")

            matches = []  # (dev_key, vol)
            for dev in vm_obj.config.hardware.device:
                if not isinstance(dev, vim.vm.device.VirtualDisk):
                    continue
                b = dev.backing
                if not isinstance(
                        b, vim.vm.device.VirtualDisk.RawDiskMappingVer1BackingInfo):
                    continue
                ident = ((b.deviceName or "") + " "
                         + (getattr(b, "lunUuid", "") or "")).lower()
                vol = next((v for s, v in s2v.items() if s and s in ident), None)
                if vol is None:
                    continue
                matches.append((dev.key, vol))
            if not matches:
                return []

            locators = []
            for dev_key, _vol in matches:
                new_backing = vim.vm.device.VirtualDisk.FlatVer2BackingInfo(
                    datastore=target_ds, diskMode="persistent", thinProvisioned=True)
                locators.append(vim.vm.RelocateSpec.DiskLocator(
                    diskId=dev_key, datastore=target_ds, diskBackingInfo=new_backing))
            self._wait_task_sync(vm_obj.RelocateVM_Task(
                vim.vm.RelocateSpec(datastore=target_ds, disk=locators)))

            vm2 = self._get_vm_sync(vm_ref)
            bykey = {d.key: d for d in vm2.config.hardware.device
                     if isinstance(d, vim.vm.device.VirtualDisk)}
            verified = []
            for dev_key, vol in matches:
                d2 = bykey.get(dev_key)
                if d2 is None:
                    continue
                if isinstance(d2.backing,
                              vim.vm.device.VirtualDisk.RawDiskMappingVer1BackingInfo):
                    continue  # still an RDM — do NOT free
                verified.append(vol)
            return verified

        await self.ctx.emit(
            f"[vsphere] converting RDM→VMDK via Storage vMotion to {target_name} "
            "(one-time host-side copy)")
        try:
            converted = await asyncio.to_thread(_relocate_sync, serial_to_vol, target_name)
        except Exception as exc:  # noqa: BLE001
            return OpResult.fail(f"Storage vMotion (RDM→VMFS) failed: {exc}")
        if not converted:
            return OpResult.fail(
                "RDM→VMFS conversion did not take effect (disks still RDMs); "
                "FA volumes left intact")

        await self._rescan_storage(cluster)
        host_group = self.migration_host_group()
        freed = 0
        for vol in converted:
            if not vol or self.ctx.array is None:
                continue
            try:
                if host_group:
                    await self.ctx.array.disconnect_volume_from_group(host_group, vol)
                await self.ctx.array.delete_volume(vol, eradicate=True)
                freed += 1
                await self.ctx.emit(
                    f"[vsphere] converted {vol} → VMDK on {target_name}; freed FA volume")
            except Exception as exc:  # noqa: BLE001
                await self.ctx.emit(
                    f"[vsphere] WARNING: converted but could not free {vol}: {exc}")

        # VM home moved off the scratch datastore — tear it down.
        scratch = getattr(self, "_scratch_vmfs", None)
        if scratch:
            await self._teardown_scratch_vmfs(scratch.get("ds", ""), scratch.get("vol", ""))
            self._scratch_vmfs = None

        return OpResult.ok(
            f"converted {len(converted)} RDM(s) to VMDK on {target_name} via Storage "
            f"vMotion; freed {freed} FA volume(s) + scratch datastore")

    async def set_boot_order(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        """Set the VM to boot from the disk marked as boot via pyVmomi."""
        if self._mock_or_dry():
            return OpResult.ok(f"[mock] set boot order on VM {vm_ref}")

        boot_disk = next((d for d in sorted(disks, key=lambda x: (not x.boot, x.order))), None)
        if not boot_disk:
            return OpResult.ok("no disks; boot order unchanged")

        source_ref = boot_disk.source_ref
        disk_order = boot_disk.order

        def _boot_sync():
            from pyVmomi import vim
            vm_obj = self._get_vm_sync(vm_ref)
            # Find device key for boot disk
            key = None
            for dev in vm_obj.config.hardware.device:
                if not isinstance(dev, vim.vm.device.VirtualDisk):
                    continue
                if source_ref and str(dev.key) == str(source_ref):
                    key = dev.key
                    break
                if dev.unitNumber == disk_order:
                    key = dev.key
            if key is None:
                raise ValueError(
                    f"Boot disk source_ref={source_ref!r} / order={disk_order} "
                    f"not found on VM {vm_ref!r}")
            boot_options = vim.vm.BootOptions(
                bootOrder=[vim.vm.BootOptions.BootableDiskDevice(deviceKey=key)]
            )
            task = vm_obj.ReconfigVM_Task(
                vim.vm.ConfigSpec(bootOptions=boot_options))
            self._wait_task_sync(task)

        await asyncio.to_thread(_boot_sync)
        return OpResult.ok(
            f"Set boot order: disk {boot_disk.source_ref} first on VM {vm_ref}")

    async def detach_volumes(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        """Remove disks from the VM config without touching the FA volume via pyVmomi."""
        if self._mock_or_dry():
            return OpResult.ok(f"[mock] detached {len(disks)} disk(s) from VM {vm_ref}")

        def _detach_sync():
            from pyVmomi import vim
            vm_obj = self._get_vm_sync(vm_ref)
            dev_specs = []
            for disk in disks:
                key = disk.source_ref
                if not key:
                    continue
                key_int = int(key) if str(key).lstrip("-").isdigit() else None
                for dev in vm_obj.config.hardware.device:
                    if not isinstance(dev, vim.vm.device.VirtualDisk):
                        continue
                    if (key_int is not None and dev.key == key_int) or str(dev.key) == str(key):
                        # operation=remove without fileOperation preserves backing
                        dev_specs.append(vim.vm.device.VirtualDeviceSpec(
                            operation=vim.vm.device.VirtualDeviceSpec.Operation.remove,
                            device=dev,
                        ))
                        break
            if dev_specs:
                task = vm_obj.ReconfigVM_Task(
                    vim.vm.ConfigSpec(deviceChange=dev_specs))
                self._wait_task_sync(task)

        await asyncio.to_thread(_detach_sync)
        for disk in disks:
            if disk.source_ref:
                await self.ctx.emit(
                    f"[vsphere] detached disk {disk.source_ref} from VM {vm_ref}")
        return OpResult.ok(f"Detached {len(disks)} disk(s) from VM {vm_ref}")

    async def delete_vm(self, vm_ref: str, *, keep_disks: bool = True) -> OpResult:
        """Delete the VM from vCenter via pyVmomi.

        With ``keep_disks=True`` (migration use), removes all VirtualDisk devices
        first (without fileOperation) so the backing FA volumes are preserved, then
        destroys the VM definition.
        """
        if self._mock_or_dry():
            return OpResult.ok(f"[mock] deleted VM {vm_ref}")

        def _delete_sync():
            from pyVmomi import vim
            vm_obj = self._get_vm_sync(vm_ref)
            if keep_disks:
                # Remove all VirtualDisk devices without deleting backing storage
                disk_specs = []
                for dev in vm_obj.config.hardware.device:
                    if isinstance(dev, vim.vm.device.VirtualDisk):
                        disk_specs.append(vim.vm.device.VirtualDeviceSpec(
                            operation=vim.vm.device.VirtualDeviceSpec.Operation.remove,
                            device=dev,
                            # No fileOperation → backing preserved
                        ))
                if disk_specs:
                    task = vm_obj.ReconfigVM_Task(
                        vim.vm.ConfigSpec(deviceChange=disk_specs))
                    self._wait_task_sync(task)
                # Re-fetch after reconfigure
                vm_obj = self._get_vm_sync(vm_ref)
            task = vm_obj.Destroy_Task()
            self._wait_task_sync(task)

        await asyncio.to_thread(_delete_sync)

        # On rollback (keep_disks=False) free the per-disk managed FA volumes this
        # migration created (phifmig-dst-<vm_ref>-disk<N>). Destroy_Task removes the VM
        # and the RDM pointer files but NEVER the raw LUNs, so without this they leak —
        # left connected to the host group and not eradicated.
        if not keep_disks and self.ctx.array is not None:
            hg = self.migration_host_group()
            i = 0
            while i < 32:
                vol = f"phifmig-dst-{vm_ref}-disk{i}"
                info = await self.ctx.array.get_volume(vol)
                if not info:
                    break  # disks are contiguous from disk0
                if not info.get("destroyed"):
                    try:
                        if hg:
                            await self.ctx.array.disconnect_volume_from_group(hg, vol)
                        await self.ctx.array.delete_volume(vol, eradicate=True)
                        await self.ctx.emit(f"[vsphere] freed managed volume {vol}")
                    except Exception as exc:  # noqa: BLE001 — best-effort rollback cleanup
                        await self.ctx.emit(
                            f"[vsphere] WARNING: could not free {vol}: {exc}")
                i += 1

        # If a scratch VMFS datastore was provisioned for this VM (convert_to_vmfs)
        # and conversion didn't already tear it down, remove it now (rollback path).
        scratch = getattr(self, "_scratch_vmfs", None)
        if scratch:
            await self._teardown_scratch_vmfs(scratch.get("ds", ""), scratch.get("vol", ""))
            self._scratch_vmfs = None

        return OpResult.ok(f"Deleted VM {vm_ref}")

    # ----------------------------------------------------------- dispatch ---
    def _DISPATCH(self) -> dict[str, str]:  # noqa: N802 (instance helper)
        return {
            **self._DEFAULT_DISPATCH,
            "provision_datastore": "provision_datastore",
        }

    async def dispatch(self, action_id: str, params: dict[str, Any]) -> OpResult:
        if action_id == "provision_datastore":
            return await self.provision_datastore(**params)
        return await super().dispatch(action_id, params)
