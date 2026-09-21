"""Nutanix Cloud Platform (AHV) connector — Prism Central driven.

Nutanix AHV can consume an Everpure FlashArray as **external storage** over
NVMe-oF/TCP. When it does, every VM virtual disk (vDisk) is backed 1:1 by a
FlashArray volume, and Prism reports that volume's name on the disk as
``externalStorageInfo.volumeName`` (the "External Volume" field in the Prism
UI). That makes a Nutanix disk the *simplest* case PHIF handles: it is already
a standalone FA volume, like a vSphere RDM, so migration needs no file-level
copy on either side.

What this connector does
------------------------
* **VM inventory** — list VMs/networks and capture a VM's logical spec, with
  each disk resolved to its backing FA volume and serial.
* **Migration source** — a Nutanix VM's disks are FA volumes already, so they
  migrate directly.
* **Migration destination** — follows the vendor-documented pattern: build a
  shell VM with matching vCPU/RAM, create one vDisk per source disk **at a
  matching size**, read back the FA volume Nutanix provisioned for each, then
  let the migration service overwrite those volumes from the source on the
  array. See :meth:`create_managed_disk`.
* **Day-2 ops** — snapshot/clone/resize/QoS against the backing FA volumes.

What this connector does NOT do
-------------------------------
**Registering the FlashArray as external storage in Prism is not implemented.**
That is the ``deploy`` / ``configure`` step (array service account, realm, pod,
NVMe-oF/TCP interface setup, and the Prism Element external-storage
registration). This connector assumes an AHV cluster where that has already
been done, and :meth:`validate_connection` says so explicitly when it finds no
FlashArray-backed external storage. Tracked as a known issue.

API notes (verified against Prism Central on AOS 7.6 / AHV 11.2)
----------------------------------------------------------------
* Port **9440**, HTTP basic auth.
* v4 namespaces present: ``vmm``, ``clustermgmt``, ``volumes``. The v4
  ``storage`` and ``dataprotection`` namespaces are **absent** on this build
  (404), so storage containers and external-storage registrations are read
  through the v3 ``groups`` API — which is what the Prism UI itself queries.
* ``/PrismGateway/services/rest/v2.0/...`` on the cluster VIP rejected the same
  credentials that work on Prism Central (401), so everything here goes through
  Prism Central.
* v4 mutations want an ``NTNX-Request-Id`` idempotency key, and in-place
  updates need ``If-Match`` carrying the ETag from a prior GET.

Metadata (``-md``) volumes
--------------------------
Before AOS 7.6.0 / Purity//FA 6.12.0, Nutanix created a companion MetaData
volume per vDisk, and SyncRep-protected VMs still do. Those ``-md`` volumes are
Nutanix-internal bookkeeping, never guest data, so they are filtered out of
disk resolution — migrating one would copy metadata over a data disk.
"""

from __future__ import annotations

import base64
import uuid
from typing import Any

from phif.connectors.base import (
    ActionSpec,
    Capability,
    ClusterNode,
    ConnectionValidationError,
    ConnectorContext,
    DiscoveryKind,
    FieldType,
    FormField,
    HypervisorConnector,
    OpResult,
    Protocol,
)
from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec

# Prism Central listens on 9440 for both the v3 and v4 APIs.
PRISM_PORT = 9440

# `vendor` value the Prism `external_storage` entity carries for a FlashArray.
# This is how an Everpure-backed registration is told apart from any other
# external storage attached to the cluster.
PURE_EXTERNAL_STORAGE_VENDOR = "kPureStorage"

# Nutanix-internal MetaData volume suffix. Pre-AOS-7.6 and SyncRep VMs pair one
# of these with every data vDisk; it holds CBT bookkeeping, not guest data.
MD_VOLUME_SUFFIX = "-md"

# Hard ceiling v4 enforces on `$limit`; a larger value is rejected with a 400.
V4_PAGE_LIMIT = 100


class NutanixConnector(HypervisorConnector):
    key = "nutanix"
    name = "Nutanix Cloud Platform (AHV)"
    # `preview`, not `ga`: the read paths are validated against live hardware but
    # the write paths (create_vm / create_managed_disk / detach / delete) are
    # covered only by unit tests so far.
    maturity = "preview"
    description = (
        "Manages Nutanix AHV virtual machines whose disks are backed by an "
        "Everpure FlashArray registered as external storage (NVMe-oF/TCP). "
        "Provides VM inventory, VM lifecycle, cross-hypervisor migration, and "
        "day-2 volume operations. Registering the FlashArray as external "
        "storage in Prism is not implemented — do that in Prism first."
    )

    CAPABILITIES = {
        Capability.SNAPSHOT,
        Capability.CLONE,
        Capability.RESIZE,
        Capability.DELETE,
        Capability.QOS,
        Capability.HEALTH,
        Capability.VM_INVENTORY,
        Capability.VM_LIFECYCLE,
        Capability.MIGRATE,
    }

    # AHV external storage on FlashArray is NVMe-oF/TCP only (per the Everpure
    # compatibility matrix); iSCSI is not a supported transport for this path.
    SUPPORTED_PROTOCOLS = {Protocol.NVME_TCP}

    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)
        self._etags: dict[str, str] = {}

    # ------------------------------------------------------------- schema ---
    @classmethod
    def target_schema(cls) -> list[FormField]:
        return [
            FormField("pc_host", "Prism Central host", FieldType.STRING,
                      placeholder="prism-central.example.com",
                      help="Prism Central address. Prism Element is not used; "
                           "its v2.0 API rejects Prism Central credentials."),
            FormField("pc_user", "Prism Central username", FieldType.STRING,
                      placeholder="admin"),
            FormField("pc_password", "Prism Central password", FieldType.SECRET),
            FormField("cluster", "AHV cluster name", FieldType.STRING,
                      required=False,
                      help="Restricts inventory and VM placement to one cluster. "
                           "Leave blank to span every cluster this Prism Central "
                           "manages."),
            FormField("storage_container", "Storage container", FieldType.STRING,
                      required=False, options_source="storage_containers",
                      help="FlashArray-backed storage container new disks are "
                           "created in. Defaults to the container already used by "
                           "the VM being written to, else the only "
                           "FlashArray-backed container on the cluster."),
        ]

    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        return [
            ActionSpec(
                Capability.SNAPSHOT, "snapshot", "Snapshot disk volume",
                "Snapshot the FlashArray volume backing a vDisk. Prism is not "
                "involved, so this does not appear as a Prism recovery point; "
                "AOS 7.6+ with Purity 6.11.8/6.12.0+ can still recover from it.",
                fields=[
                    FormField("volume", "FlashArray volume", FieldType.STRING,
                              help="The disk's External Volume name, as shown on "
                                   "the VM's Disks tab in Prism."),
                    FormField("suffix", "Snapshot suffix", FieldType.STRING,
                              required=False),
                ],
            ),
            ActionSpec(
                Capability.CLONE, "clone", "Clone disk volume",
                "Copy a vDisk's FlashArray volume to a new volume.",
                fields=[
                    FormField("source", "Source FlashArray volume", FieldType.STRING),
                    FormField("dest", "New volume name", FieldType.STRING),
                ],
            ),
            ActionSpec(
                Capability.RESIZE, "resize", "Resize disk volume",
                "Extend the FlashArray volume backing a vDisk. Grow the vDisk in "
                "Prism as well so Nutanix's view matches the array.",
                fields=[
                    FormField("volume", "FlashArray volume", FieldType.STRING),
                    FormField("size", "New size", FieldType.SIZE),
                ],
            ),
            ActionSpec(
                Capability.QOS, "set_qos", "Set volume QoS",
                fields=[
                    FormField("volume", "FlashArray volume", FieldType.STRING),
                    FormField("iops_limit", "IOPS limit", FieldType.INT,
                              required=False),
                    FormField("bw_limit", "Bandwidth limit (bytes/s)", FieldType.INT,
                              required=False),
                ],
            ),
            ActionSpec(
                Capability.DELETE, "delete", "Delete disk volume",
                "Destroy a FlashArray volume. Detach the vDisk in Prism first.",
                destructive=True,
                fields=[
                    FormField("volume", "FlashArray volume", FieldType.STRING),
                    FormField("eradicate", "Eradicate immediately", FieldType.BOOL,
                              default=False, required=False),
                ],
            ),
            ActionSpec(Capability.HEALTH, "health_check", "Health check",
                       long_running=False),
        ]

    @classmethod
    def wizard_steps(cls) -> list[str]:
        # No deploy/configure step: external-storage registration is done in
        # Prism, not here. Health check is the only meaningful cluster-wide run.
        return ["health_check"]

    # ---------------------------------------------------------- transport ---
    def _mock_or_dry(self) -> bool:
        """True when no live Prism Central should be contacted."""
        return bool(self.ctx.dry_run or getattr(self.ctx.runner, "mock", False)
                    or getattr(self.ctx.runner, "dry_run", False))

    def _base_url(self) -> str:
        host = self.ctx.target.get("pc_host", "") or ""
        if not host:
            raise ConnectionValidationError("pc_host is not set on this hypervisor")
        return f"https://{host}:{PRISM_PORT}"

    def _auth_header(self) -> dict[str, str]:
        user = self.ctx.target.get("pc_user", "") or ""
        pwd = self.ctx.target.get("pc_password", "") or ""
        token = base64.b64encode(f"{user}:{pwd}".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    async def _api(self, method: str, path: str, *, json_body: Any = None,
                   etag_for: str | None = None,
                   expected: tuple[int, ...] = (200, 201, 202, 204)) -> Any:
        """Call a Prism Central API and return the decoded body.

        ``etag_for`` names a cache key: on a GET the response's ETag is recorded
        under it, and on a mutation the recorded ETag is replayed as
        ``If-Match``. v4 rejects an in-place update without it.
        """
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            **self._auth_header(),
        }
        if method.upper() in ("POST", "PUT", "PATCH", "DELETE"):
            # v4 uses this as an idempotency key, so a retried request cannot
            # apply the same mutation twice.
            headers["NTNX-Request-Id"] = str(uuid.uuid4())
        if etag_for and (tag := self._etags.get(etag_for)):
            headers["If-Match"] = tag

        res = await self.ctx.runner.run_http(
            method, f"{self._base_url()}{path}",
            headers=headers, json_body=json_body, verify=False, expected=expected)

        # The ETag arrives as a response *header*, not in the body — and Prism
        # spells it "Etag", so rely on run_http's lower-cased header map rather
        # than matching a casing.
        if etag_for:
            tag = (res.get("headers") or {}).get("etag")
            if tag:
                self._etags[etag_for] = tag
        return res.get("json") or {}

    async def _api_list(self, path: str) -> list[dict[str, Any]]:
        """GET every page of a v4 list endpoint and return the concatenated data.

        v4 caps ``$limit`` at 100 and rejects anything larger with a 400, so a
        single "just ask for everything" call both fails outright and, if the cap
        were raised silently, would truncate. ``$page`` is 0-based and
        ``metadata.totalAvailableResults`` carries the true total.
        """
        sep = "&" if "?" in path else "?"
        rows: list[dict[str, Any]] = []
        page = 0
        while True:
            payload = await self._api(
                "GET", f"{path}{sep}$limit={V4_PAGE_LIMIT}&$page={page}")
            data = (payload or {}).get("data") or []
            rows.extend(data)
            total = (((payload or {}).get("metadata") or {})
                     .get("totalAvailableResults"))
            if not data:
                break
            if total is not None and len(rows) >= int(total):
                break
            if len(data) < V4_PAGE_LIMIT:
                break
            page += 1
            # Defensive stop: never spin forever if the server stops advancing.
            if page > 1000:
                await self.ctx.emit(
                    f"[nutanix] stopped paging {path} after {page} pages")
                break
        return rows

    async def _groups(self, entity_type: str, attributes: list[str], *,
                      count: int = 200) -> list[dict[str, Any]]:
        """Query the v3 ``groups`` API and flatten it to plain dicts.

        The v4 ``storage`` namespace is absent on AOS 7.6, so storage containers
        and external-storage registrations are only reachable this way. The
        response is deeply nested (per-attribute time-series), hence the
        flattening here rather than at every call site.
        """
        body = {
            "entity_type": entity_type,
            "group_member_count": count,
            "group_member_attributes": [{"attribute": a} for a in attributes],
        }
        payload = await self._api("POST", "/api/nutanix/v3/groups", json_body=body)
        rows: list[dict[str, Any]] = []
        for group in (payload or {}).get("group_results") or []:
            for entity in group.get("entity_results") or []:
                row: dict[str, Any] = {"entity_id": entity.get("entity_id")}
                for item in entity.get("data") or []:
                    values = item.get("values") or []
                    # Newest time-series entry first; take its first value.
                    inner = (values[0].get("values") or []) if values else []
                    row[item.get("name")] = inner[0] if inner else None
                rows.append(row)
        return rows

    # --------------------------------------------------------- connection ---
    async def validate_connection(self) -> OpResult:
        if self._mock_or_dry():
            await self.ctx.emit("[mock/dry-run] would validate Prism Central login")
            return OpResult.ok("Dry-run: Prism Central connection assumed valid")

        clusters = await self._api("POST", "/api/nutanix/v3/clusters/list",
                                   json_body={"kind": "cluster", "length": 50})
        names = []
        for e in (clusters or {}).get("entities") or []:
            status = e.get("status") or {}
            if status.get("name"):
                names.append(status["name"])
        if not names:
            return OpResult.fail("Connected to Prism Central but it reports no clusters")

        # An FA-backed cluster is the whole premise of this connector, so say so
        # plainly when the external-storage registration is missing rather than
        # failing later inside a migration.
        ext = await self._fa_external_storage()
        if not ext:
            return OpResult.fail(
                f"Prism Central login OK (clusters: {', '.join(sorted(names))}), but no "
                f"FlashArray-backed external storage is registered. Register the "
                f"FlashArray as an External Storage target in Prism Element first — "
                f"this connector does not perform that registration.")

        await self.ctx.emit(
            f"FlashArray external storage found: "
            f"{', '.join(sorted({e['name'] for e in ext if e.get('name')}))}")
        return OpResult.ok(
            f"Connected to Prism Central ({len(names)} cluster(s))",
            clusters=sorted(names),
            external_storage=sorted({e["name"] for e in ext if e.get("name")}),
        )

    async def _fa_external_storage(self) -> list[dict[str, Any]]:
        """Return the FlashArray-backed external-storage registrations.

        Prism models these as ``external_storage`` entities carrying a
        ``vendor``; only ``kPureStorage`` ones are ours.
        """
        rows = await self._groups("external_storage", ["name", "vendor"])
        return [r for r in rows if r.get("vendor") == PURE_EXTERNAL_STORAGE_VENDOR]

    async def health_check(self, **_: Any) -> OpResult:
        info = await self.ctx.array.info() if self.ctx.array else {}
        conn = await self.validate_connection()
        return OpResult.ok(
            "Healthy" if conn.success else f"Degraded: {conn.message}",
            array=info,
            prism_central=self.ctx.target.get("pc_host"),
            prism_ok=conn.success,
        )

    # ---------------------------------------------------------- discovery ---
    async def list_nodes(self) -> list[ClusterNode]:
        """Return the AHV hosts of the configured cluster (or all clusters)."""
        if self._mock_or_dry():
            return [ClusterNode(name="ahv-node-1", host="192.0.2.11"),
                    ClusterNode(name="ahv-node-2", host="192.0.2.12")]

        want = (self.ctx.target.get("cluster") or "").strip()
        payload = await self._api("POST", "/api/nutanix/v3/clusters/list",
                                  json_body={"kind": "cluster", "length": 50})
        nodes: list[ClusterNode] = []
        for e in (payload or {}).get("entities") or []:
            status = e.get("status") or {}
            if want and status.get("name") != want:
                continue
            cluster_name = status.get("name") or ""
            res = status.get("resources") or {}
            for n in ((res.get("nodes") or {}).get("hypervisor_server_list") or []):
                ip = n.get("ip") or ""
                # Prism lists a loopback placeholder entry per cluster alongside
                # the real hypervisor addresses; it is not a node.
                if not ip or ip.startswith("127."):
                    continue
                nodes.append(ClusterNode(
                    name=ip, host=ip,
                    info={"cluster": cluster_name,
                          "hypervisor": n.get("type") or "",
                          "version": n.get("version") or ""}))
        return nodes

    async def discover_options(self, kind: str) -> list[dict[str, Any]]:
        if kind != "storage_containers":
            return []
        if self._mock_or_dry():
            return [{"id": "mock-container", "name": "FA-container"}]
        return [{"id": c["entity_id"], "name": c["name"]}
                for c in await self._storage_containers()]

    async def _storage_containers(self) -> list[dict[str, Any]]:
        """Return storage containers, scoped to the configured cluster.

        Only ``container_name`` and ``cluster_name`` populate on this entity —
        the provider/vendor attributes come back empty, so the FlashArray
        linkage has to come from :meth:`_fa_external_storage` instead.
        """
        want = (self.ctx.target.get("cluster") or "").strip()
        rows = await self._groups("storage_container",
                                  ["container_name", "cluster_name"])
        out = []
        for r in rows:
            if want and r.get("cluster_name") != want:
                continue
            if not r.get("container_name"):
                continue
            out.append({"entity_id": r["entity_id"], "name": r["container_name"],
                        "cluster": r.get("cluster_name")})
        return out

    # ------------------------------------------------------- VM inventory ---
    async def _list_vms_raw(self) -> list[dict[str, Any]]:
        return await self._api_list("/api/vmm/v4.0/ahv/config/vms")

    async def list_vms(self) -> list[dict[str, Any]]:
        if self._mock_or_dry():
            return [{"id": "mock-vm-1", "name": "mock-vm", "power_state": "ON",
                     "vcpus": 2, "memory_bytes": 4 * 1024**3}]
        out = []
        for vm in await self._list_vms_raw():
            out.append({
                "id": vm.get("extId"),
                "name": vm.get("name"),
                "power_state": vm.get("powerState"),
                "vcpus": self._vcpus(vm),
                "memory_bytes": vm.get("memorySizeBytes") or 0,
                "disks": len(vm.get("disks") or []),
            })
        return out

    @staticmethod
    def _vcpus(vm: dict[str, Any]) -> int:
        """AHV models CPU as sockets x cores-per-socket; PHIF wants a total."""
        sockets = int(vm.get("numSockets") or 1)
        cores = int(vm.get("numCoresPerSocket") or 1)
        return max(1, sockets * cores)

    async def list_networks(self) -> list[dict[str, Any]]:
        if self._mock_or_dry():
            return [{"id": "mock-subnet", "name": "vlan0"}]
        return [{"id": s.get("extId"), "name": s.get("name"),
                 "vlan": s.get("networkId")}
                for s in await self._api_list("/api/networking/v4.0/config/subnets")]

    async def list_placements(self) -> list[dict[str, Any]]:
        """Storage containers are the placement target for a new VM's disks."""
        if self._mock_or_dry():
            return [{"id": "mock-container", "name": "FA-container", "kind": "container"}]
        return [{"id": c["entity_id"], "name": c["name"], "kind": "container"}
                for c in await self._storage_containers()]

    async def _find_vm(self, vm_ref: str) -> dict[str, Any]:
        """Fetch one VM by extId, recording its ETag for later in-place updates."""
        payload = await self._api("GET", f"/api/vmm/v4.0/ahv/config/vms/{vm_ref}",
                                  etag_for=vm_ref)
        data = (payload or {}).get("data") or {}
        if not data:
            raise ConnectionValidationError(f"VM {vm_ref!r} not found in Prism Central")
        return data

    @staticmethod
    def _disk_volume_name(disk: dict[str, Any]) -> str:
        """Return the FA volume backing a vDisk, or "" when it is not external.

        ``externalStorageInfo.volumeName`` is the array volume name — the
        "External Volume" field in the Prism UI. A disk without it is on
        Nutanix-native storage (ADSF) and has no FA volume to migrate.
        """
        backing = disk.get("backingInfo") or {}
        ext = backing.get("externalStorageInfo") or {}
        return (ext.get("volumeName") or "").strip()

    @staticmethod
    def _is_volume_group_disk(disk: dict[str, Any]) -> bool:
        """True for a disk that is a Nutanix Volume Group reference.

        A VG is a cluster-level object that can be shared by several VMs and is
        itself a collection of vDisks, so it is not one FA volume and cannot be
        migrated as part of a single VM. (A Nutanix Volume Group is also not the
        same thing as a FlashArray volume group.)
        """
        backing = disk.get("backingInfo") or {}
        return "volumegroup" in (backing.get("$objectType") or "").lower()

    @classmethod
    def _is_md_volume(cls, volume_name: str) -> bool:
        return volume_name.endswith(MD_VOLUME_SUFFIX)

    async def capture_vm_spec(self, vm_ref: str) -> VmSpec:
        """Read a Nutanix VM's logical hardware, resolving disks to FA volumes.

        Each vDisk names its backing FA volume directly, so resolution is a
        volume-name lookup — no tag indirection and no size matching.
        """
        vm = await self._find_vm(vm_ref)
        name = vm.get("name") or vm_ref

        disks: list[DiskSpec] = []
        unmappable: list[str] = []
        volume_groups: list[str] = []
        for order, disk in enumerate(vm.get("disks") or []):
            backing = disk.get("backingInfo") or {}
            disk_ext_id = backing.get("diskExtId") or f"disk{order}"
            size_bytes = int(backing.get("diskSizeBytes") or 0)
            volume = self._disk_volume_name(disk)

            if self._is_volume_group_disk(disk):
                volume_groups.append(str(disk_ext_id))
                continue
            if self._is_md_volume(volume):
                # Nutanix-internal CBT bookkeeping, not guest data.
                await self.ctx.emit(
                    f"[nutanix] skipping metadata volume {volume}")
                continue
            if not volume:
                unmappable.append(disk_ext_id)
                continue

            serial = None
            if self.ctx.array is not None:
                # Prism reports the leaf volume name; on the array the volume is
                # pod/realm-scoped (pod::volume), so resolve before looking it up.
                resolved = await self.ctx.array.resolve_volume_name(volume)
                vol = await self.ctx.array.get_volume(resolved) if resolved else None
                if vol and not vol.get("destroyed"):
                    volume = vol["name"]
                    serial = vol.get("serial")
                    await self.ctx.emit(
                        f"[nutanix] disk {disk_ext_id} -> {volume} serial={serial}")
                else:
                    # The disk claims an external volume that this array does not
                    # have — usually a different array. Report it rather than
                    # migrating an unidentified disk.
                    unmappable.append(f"{disk_ext_id} ({volume})")
                    continue

            addr = disk.get("diskAddress") or {}
            disks.append(DiskSpec(
                DiskIdentity(fa_volume=volume, serial=serial, size_bytes=size_bytes),
                bus=self._logical_bus(addr.get("busType")),
                order=int(addr.get("index") if addr.get("index") is not None else order),
                boot=(order == 0),
                source_ref=str(disk_ext_id),
            ))

        if volume_groups:
            raise ConnectionValidationError(
                f"VM {name!r} has Nutanix Volume Group disk(s) "
                f"({', '.join(volume_groups)}). A Volume Group is a cluster-level "
                f"object that may be shared with other VMs and is itself a collection "
                f"of vDisks, so it cannot be migrated as part of a single VM. Detach "
                f"it and migrate the VM's own vDisks, then re-present the Volume Group "
                f"on the destination.")
        if unmappable:
            raise ConnectionValidationError(
                f"VM {name!r} has disk(s) ({', '.join(unmappable)}) with no FlashArray "
                f"volume on the connected array. Only disks backed by the connected "
                f"FlashArray as external storage can be migrated; disks on Nutanix "
                f"native storage (ADSF) and disks on another array are not supported.")

        nics: list[NicSpec] = []
        for i, nic in enumerate(vm.get("nics") or []):
            backing = nic.get("backingInfo") or {}
            net = nic.get("networkInfo") or {}
            subnet = (net.get("subnet") or {}).get("extId") or ""
            nics.append(NicSpec(
                mac=(backing.get("macAddress") or "").lower(),
                source_network=subnet,
                model=self._logical_nic_model(backing.get("$objectType")),
                order=i,
            ))

        boot = vm.get("bootConfig") or {}
        # AHV reports UEFI by the boot-config object type it returns.
        is_uefi = "uefi" in (boot.get("$objectType") or "").lower()

        return VmSpec(
            name=name,
            source_ref=vm_ref,
            vcpus=self._vcpus(vm),
            memory_bytes=int(vm.get("memorySizeBytes") or 0),
            firmware="uefi" if is_uefi else "bios",
            secure_boot=bool(boot.get("isSecureBootEnabled")),
            disks=disks,
            nics=nics,
            guest_os_hint=(vm.get("guestOsType") or ""),
            raw={"cluster": ((vm.get("cluster") or {}).get("extId") or ""),
                 "machine_type": vm.get("machineType") or ""},
        )

    @staticmethod
    def _logical_nic_model(object_type: str | None) -> str:
        """Derive a logical NIC model from the AHV backing object type.

        An AHV NIC has no ``model`` field; the device type is the backing
        object's type (``...config.EmulatedNic``, ``...config.VirtioNic``).
        PHIF only needs a logical hint the destination maps to its best fit.
        """
        t = (object_type or "").rsplit(".", 1)[-1].lower()
        if "virtio" in t:
            return "virtio"
        if "emulated" in t:
            # AHV's emulated NIC presents as e1000 to the guest.
            return "e1000"
        return "virtio"

    @staticmethod
    def _logical_bus(bus_type: str | None) -> str:
        """Map an AHV busType to PHIF's logical family.

        v4 reports busType unprefixed ("SCSI"), but tolerate a leading "k" in
        case an older/other build uses the prefixed enum spelling.
        """
        raw = (bus_type or "").lower()
        if raw.startswith("k"):
            raw = raw[1:]
        return raw if raw in {"scsi", "ide", "sata", "nvme", "virtio"} else "scsi"

    # ------------------------------------------------------- VM lifecycle ---
    async def power_state(self, vm_ref: str) -> str:
        if self._mock_or_dry():
            return "off"
        vm = await self._find_vm(vm_ref)
        return "on" if (vm.get("powerState") or "").upper() == "ON" else "off"

    async def stop_vm(self, vm_ref: str, *, force: bool = False) -> OpResult:
        if self._mock_or_dry():
            return OpResult.ok(f"Dry-run: would power off VM {vm_ref}")
        # A guest-coordinated shutdown needs Nutanix Guest Tools; power-off is
        # always available. Migration is a cold cutover, so either is acceptable
        # and power-off is the one that cannot hang.
        action = "power-off" if force else "shutdown"
        try:
            await self._api("POST",
                            f"/api/vmm/v4.0/ahv/config/vms/{vm_ref}/${action}",
                            etag_for=vm_ref)
        except Exception as exc:
            if force:
                raise
            await self.ctx.emit(
                f"[nutanix] guest shutdown failed ({exc}); falling back to power-off")
            await self._api("POST",
                            f"/api/vmm/v4.0/ahv/config/vms/{vm_ref}/$power-off",
                            etag_for=vm_ref)
        return OpResult.ok(f"VM {vm_ref} powered off")

    async def start_vm(self, vm_ref: str) -> OpResult:
        if self._mock_or_dry():
            return OpResult.ok(f"Dry-run: would power on VM {vm_ref}")
        await self._api("POST", f"/api/vmm/v4.0/ahv/config/vms/{vm_ref}/$power-on",
                        etag_for=vm_ref)
        return OpResult.ok(f"VM {vm_ref} powered on")

    async def create_vm(self, spec: VmSpec, *, name: str = "",
                        placement: str = "", network_map: Any = None,
                        **_: Any) -> OpResult:
        """Create a shell VM with matching vCPU/RAM and no disks.

        Disks are added afterwards by :meth:`create_managed_disk`, one per source
        disk, so each gets its own FlashArray volume to be overwritten.
        """
        vm_name = name or spec.name
        if self._mock_or_dry():
            return OpResult.ok(f"Dry-run: would create VM {vm_name}",
                               artifacts={"vm_ref": f"mock-{vm_name}"})

        cluster_ext_id = await self._cluster_ext_id()
        body: dict[str, Any] = {
            "name": vm_name,
            "numSockets": max(1, spec.vcpus),
            "numCoresPerSocket": 1,
            "memorySizeBytes": int(spec.memory_bytes),
            "cluster": {"extId": cluster_ext_id},
        }
        if spec.firmware == "uefi":
            body["bootConfig"] = {
                "$objectType": "vmm.v4.ahv.config.UefiBoot",
                "isSecureBootEnabled": bool(spec.secure_boot),
            }

        nic_bodies = []
        mapping = network_map if isinstance(network_map, dict) else {}
        for nic in spec.nics:
            subnet = mapping.get(nic.source_network) or mapping.get(str(nic.order))
            if not subnet:
                continue
            entry: dict[str, Any] = {
                "networkInfo": {"subnet": {"extId": subnet}},
            }
            # MAC preservation matters for licence-bound guests, so carry it over
            # whenever the source had one.
            if nic.mac:
                entry["backingInfo"] = {"macAddress": nic.mac}
            nic_bodies.append(entry)
        if nic_bodies:
            body["nics"] = nic_bodies

        await self.ctx.emit(
            f"[nutanix] creating VM {vm_name} ({spec.vcpus} vCPU, "
            f"{spec.memory_bytes // 1024**3} GiB, firmware={spec.firmware})")
        payload = await self._api("POST", "/api/vmm/v4.0/ahv/config/vms",
                                  json_body=body)
        vm_ref = ((payload or {}).get("data") or {}).get("extId") or ""
        if not vm_ref:
            return OpResult.fail(
                f"Prism accepted the create for {vm_name} but returned no VM extId")
        return OpResult.ok(f"Created VM {vm_name}",
                           artifacts={"vm_ref": vm_ref, "name": vm_name})

    async def _cluster_ext_id(self) -> str:
        want = (self.ctx.target.get("cluster") or "").strip()
        payload = await self._api("POST", "/api/nutanix/v3/clusters/list",
                                  json_body={"kind": "cluster", "length": 50})
        candidates = []
        for e in (payload or {}).get("entities") or []:
            status = e.get("status") or {}
            res = status.get("resources") or {}
            nodes = (res.get("nodes") or {}).get("hypervisor_server_list") or []
            # Prism Central itself appears in this list with no hypervisors; it
            # is not a placement target.
            if not nodes:
                continue
            ext_id = (e.get("metadata") or {}).get("uuid") or ""
            if want and status.get("name") != want:
                continue
            candidates.append((status.get("name"), ext_id))
        if not candidates:
            raise ConnectionValidationError(
                f"No AHV cluster{f' named {want!r}' if want else ''} found in Prism Central")
        if len(candidates) > 1:
            names = ", ".join(sorted(n for n, _ in candidates))
            raise ConnectionValidationError(
                f"Prism Central manages multiple AHV clusters ({names}); set "
                f"'cluster' on the hypervisor connection to choose one")
        return candidates[0][1]

    async def create_managed_disk(self, vm_ref: str, *, size_bytes: int,
                                  order: int = 0, bus: str = "scsi",
                                  **_: Any) -> OpResult:
        """Add a vDisk and return the FlashArray volume Nutanix provisioned.

        This is the vendor-documented migration/clone pattern: Nutanix creates
        the disk object *and* its backing array volume, then the array volume is
        overwritten from the source. PHIF cannot name that volume itself, so it
        is read back from the created disk.

        The disk is created at the source's exact size. Sizing it here (rather
        than resizing the volume on the array afterwards) is deliberate: Nutanix
        owns the volume, and an array-side resize behind its back leaves Prism's
        view of the disk wrong. A size mismatch before the overwrite is the most
        common way this workflow is gotten wrong.
        """
        if self._mock_or_dry():
            vol = f"mock-nx-{vm_ref}-{order}-dt"
            return OpResult.ok(f"Dry-run: would create vDisk {order} ({size_bytes} B)",
                               artifacts={"fa_volume": vol, "volume": vol})

        container = await self._target_container(vm_ref)
        body = {
            "backingInfo": {
                "$objectType": "vmm.v4.ahv.config.VmDisk",
                "diskSizeBytes": int(size_bytes),
                "storageContainer": {"extId": container},
            },
            "diskAddress": {
                "busType": self._ahv_bus(bus),
                "index": int(order),
            },
        }
        await self.ctx.emit(
            f"[nutanix] adding vDisk index={order} size={size_bytes} B "
            f"container={container}")
        await self._api("POST", f"/api/vmm/v4.0/ahv/config/vms/{vm_ref}/disks",
                        json_body=body, etag_for=vm_ref)

        # Read the VM back to learn which FA volume Nutanix created for the disk.
        vm = await self._find_vm(vm_ref)
        for disk in vm.get("disks") or []:
            addr = disk.get("diskAddress") or {}
            if int(addr.get("index", -1)) != int(order):
                continue
            if self._ahv_bus(bus).lower() != str(addr.get("busType", "")).lower():
                continue
            volume = self._disk_volume_name(disk)
            if not volume:
                return OpResult.fail(
                    f"vDisk index {order} was created on VM {vm_ref} but reports no "
                    f"external volume — the storage container {container!r} is not "
                    f"FlashArray-backed, so there is no array volume to migrate into.")
            await self.ctx.emit(f"[nutanix] vDisk {order} -> FA volume {volume}")
            return OpResult.ok(
                f"Created vDisk {order} backed by {volume}",
                artifacts={"fa_volume": volume, "volume": volume,
                           "disk_ext_id": (disk.get("backingInfo") or {})
                           .get("diskExtId", ""),
                           "size_bytes": int(size_bytes)},
            )
        return OpResult.fail(
            f"Created vDisk index {order} on VM {vm_ref} but could not find it when "
            f"reading the VM back")

    @staticmethod
    def _ahv_bus(bus: str) -> str:
        """Map PHIF's logical bus family to the AHV busType spelling.

        v4 reports these unprefixed ("SCSI"), unlike most AHV enums which carry
        a leading "k". AHV has no virtio-blk bus, so virtio maps to SCSI.
        """
        return {"scsi": "SCSI", "ide": "IDE", "sata": "SATA",
                "nvme": "NVME", "virtio": "SCSI"}.get((bus or "scsi").lower(), "SCSI")

    async def _target_container(self, vm_ref: str) -> str:
        """Pick the storage container new disks go into.

        Prefers an explicit setting, then the container the VM already uses (so
        added disks land beside existing ones), then the sole container on the
        cluster. Ambiguity is an error rather than a guess — putting a migrated
        disk on Nutanix-native storage would silently produce a VM with no FA
        volume to overwrite.
        """
        if explicit := (self.ctx.target.get("storage_container") or "").strip():
            for c in await self._storage_containers():
                if explicit in (c["name"], c["entity_id"]):
                    return c["entity_id"]
            raise ConnectionValidationError(
                f"storage_container {explicit!r} not found on this cluster")

        vm = await self._find_vm(vm_ref)
        for disk in vm.get("disks") or []:
            backing = disk.get("backingInfo") or {}
            if ext_id := ((backing.get("storageContainer") or {}).get("extId")):
                return ext_id

        containers = await self._storage_containers()
        if len(containers) == 1:
            return containers[0]["entity_id"]
        names = ", ".join(sorted(c["name"] for c in containers)) or "(none)"
        raise ConnectionValidationError(
            f"Cannot choose a storage container for VM {vm_ref} (candidates: {names}). "
            f"Set 'storage_container' on the hypervisor connection.")

    async def set_boot_order(self, vm_ref: str, boot_disk_ref: str = "",
                             **_: Any) -> OpResult:
        """AHV boots the lowest-indexed disk, so index 0 is the boot disk.

        capture_vm_spec preserves each disk's index and create_managed_disk
        recreates it, so the source's boot disk already lands at index 0 and
        there is no separate boot-order object to set.
        """
        await self.ctx.emit(
            "[nutanix] boot order follows disk index; index 0 is the boot disk")
        return OpResult.ok("Boot order is implied by disk index on AHV",
                           artifacts={"boot_disk": boot_disk_ref})

    async def delete_vm(self, vm_ref: str, *, keep_disks: bool = True) -> OpResult:
        if self._mock_or_dry():
            return OpResult.ok(f"Dry-run: would delete VM {vm_ref}")
        if keep_disks:
            # Deleting an AHV VM deletes its vDisks, and with them the backing FA
            # volumes. Refusing is safer than silently destroying migrated data.
            return OpResult.fail(
                f"Refusing to delete VM {vm_ref} with keep_disks=True: deleting an AHV "
                f"VM also deletes its vDisks and their FlashArray volumes. Detach the "
                f"disks in Prism first, or call again with keep_disks=False.")
        await self._api("DELETE", f"/api/vmm/v4.0/ahv/config/vms/{vm_ref}",
                        etag_for=vm_ref)
        return OpResult.ok(f"Deleted VM {vm_ref} and its vDisks")

    # --------------------------------------------------------- migration ----
    async def prepare_source_disks(self, spec: VmSpec, **_: Any) -> OpResult:
        """Nothing to stage: a Nutanix vDisk is already its own FA volume.

        Unlike a VMFS-resident VMDK, there is no file to clone onto a new volume
        first — capture_vm_spec has already resolved every disk to the array
        volume that backs it.
        """
        volumes = [d.identity.fa_volume for d in spec.disks if d.identity.fa_volume]
        await self.ctx.emit(
            f"[nutanix] {len(volumes)} disk(s) already map 1:1 to FlashArray "
            f"volumes; no source staging needed")
        return OpResult.ok("Source disks are FlashArray volumes already",
                           artifacts={"volumes": volumes})

    async def cleanup_migration_scratch(self, spec: VmSpec) -> None:
        """No scratch objects are created on the Nutanix side."""
        return None

    async def detach_volumes(self, vm_ref: str, volumes: Any = None,
                             **_: Any) -> OpResult:
        """Detach vDisks whose backing FA volumes are in ``volumes``."""
        wanted = {v.strip() for v in (volumes or []) if str(v).strip()}
        if self._mock_or_dry():
            return OpResult.ok(f"Dry-run: would detach {len(wanted)} disk(s) "
                               f"from VM {vm_ref}")
        vm = await self._find_vm(vm_ref)
        detached = []
        for disk in vm.get("disks") or []:
            volume = self._disk_volume_name(disk)
            if wanted and volume not in wanted:
                continue
            disk_ext_id = (disk.get("backingInfo") or {}).get("diskExtId")
            if not disk_ext_id:
                continue
            await self._api(
                "DELETE",
                f"/api/vmm/v4.0/ahv/config/vms/{vm_ref}/disks/{disk_ext_id}",
                etag_for=vm_ref)
            detached.append(volume or disk_ext_id)
            # The ETag changes with every mutation, so refresh it before the next.
            vm = await self._find_vm(vm_ref)
        return OpResult.ok(f"Detached {len(detached)} disk(s) from VM {vm_ref}",
                           artifacts={"detached": detached})

    # ---------------------------------------------------------- day-2 ops ---
    async def snapshot(self, volume: str, suffix: str = "", **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: snapshot of {volume} planned")
        await self.ctx.array.create_snapshot(volume, suffix=suffix or None)
        # Worth stating: this is an array snapshot, not a Prism recovery point.
        await self.ctx.emit(
            "[nutanix] array-side snapshot taken; it will not appear as a Prism "
            "recovery point, but AOS 7.6+ with Purity 6.11.8/6.12.0+ can recover "
            "from any FlashArray snapshot of a Nutanix volume")
        return OpResult.ok(f"Snapshot of {volume} created",
                           artifacts={"volume": volume, "suffix": suffix})

    async def clone(self, source: str, dest: str, **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: clone {source} -> {dest} planned")
        await self.ctx.array.clone_volume(source, dest)
        await self.ctx.emit(
            f"[nutanix] cloned {source} -> {dest}. To attach it to a VM, create a "
            f"vDisk of exactly matching size in Prism and overwrite its volume "
            f"from {dest}")
        return OpResult.ok(f"Cloned {source} -> {dest}", artifacts={"volume": dest})

    async def resize(self, volume: str, size: str, **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: resize {volume} -> {size} planned")
        await self.ctx.array.extend_volume(volume, size)
        await self.ctx.emit(
            f"[nutanix] extended {volume} to {size} on the array. Nutanix owns this "
            f"volume: grow the vDisk in Prism to the same size so Prism's view "
            f"matches, otherwise the extra capacity stays invisible to the guest")
        return OpResult.ok(f"Resized {volume} to {size}",
                           artifacts={"volume": volume, "size": size})

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

    async def delete(self, volume: str, eradicate: bool = False,
                     **_: Any) -> OpResult:
        if self.ctx.array is None:
            return OpResult.fail("No FlashArray associated with this hypervisor")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry-run: delete {volume} planned")
        await self.ctx.array.delete_volume(volume, eradicate=eradicate)
        return OpResult.ok(f"Deleted volume {volume}",
                           artifacts={"volume": volume, "eradicated": eradicate})
