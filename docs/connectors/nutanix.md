# Nutanix Cloud Platform (AHV) connector

Drives **Prism Central** for VM inventory, VM lifecycle, and cross-hypervisor
migration against an Everpure FlashArray registered as **external storage** for
an AHV cluster over NVMe-oF/TCP.

## Model

Nutanix AHV can consume a FlashArray as external storage. When it does, every VM
virtual disk (vDisk) is backed **1:1 by a FlashArray volume**, and Prism reports
that volume on the disk as `externalStorageInfo.volumeName` — the "External
Volume" field in the Prism UI.

That makes a Nutanix disk the simplest case PHIF handles: it is already a
standalone FA volume, like a vSphere RDM, so migration needs no file-level copy
on either side. Contrast the vSphere connector, where a VMFS-resident VMDK is a
file inside a shared datastore and has to be cloned onto a new volume first.

## Not implemented: external-storage registration

**Registering the FlashArray as external storage in Prism is out of scope for
this connector.** That step — array service account, realm, pod, NVMe-oF/TCP
interface configuration, and the Prism Element external-storage registration —
must be done in Prism first. See the known-issues section of the README.

Consequently the connector does **not** advertise `deploy_plugin`, `configure`,
`provision_datastore`, `provision_volume`, `connectivity`, or `host_register`.
`validate_connection` fails with an explicit message when it finds no
FlashArray-backed external storage, rather than letting a migration fail later.

## Connection (target_schema)

| Field | Type | Required | Notes |
|---|---|---|---|
| `pc_host` | string | yes | Prism Central address. Prism Element is **not** used — its v2.0 API rejects Prism Central credentials (401). |
| `pc_user` | string | yes | |
| `pc_password` | secret | yes | |
| `cluster` | string | no | Restricts inventory and VM placement to one cluster. Required when Prism Central manages more than one AHV cluster, since VM creation cannot guess a placement target. |
| `storage_container` | string | no | FlashArray-backed container new disks are created in. Defaults to the container the target VM already uses, else the only container on the cluster. |

## Capabilities and actions

| Capability | Action | Notes |
|---|---|---|
| CONNECT | `validate_connection` | Verifies Prism login **and** that FlashArray external storage is registered |
| VM_INVENTORY | — | `list_vms`, `list_networks`, `list_placements`, `capture_vm_spec` |
| VM_LIFECYCLE | — | `create_vm`, `create_managed_disk`, `detach_volumes`, `start_vm`/`stop_vm`, `delete_vm` |
| MIGRATE | — | Source and destination; see below |
| SNAPSHOT | `snapshot` | Array snapshot of the backing volume (not a Prism recovery point) |
| CLONE | `clone` | Array volume copy |
| RESIZE | `resize` | Extends the array volume; grow the vDisk in Prism to match |
| QOS | `set_qos` | IOPS/bandwidth limit on the backing volume |
| DELETE | `delete` | Destroys a backing volume (detach the vDisk in Prism first) |
| HEALTH | `health_check` | |

## Disk resolution

```
GET /api/vmm/v4.0/ahv/config/vms/{extId}
  disks[].backingInfo.externalStorageInfo.volumeName    # "nx-<id>-<n>-dt"
    → FlashArrayClient.resolve_volume_name()            # adds the pod/realm scope
    → FlashArray volume + serial → scsi_wwid() / nvme_eui()
```

**Prism reports only the leaf volume name.** On the array the volume is
pod-scoped, and realm-scoped inside a realm — observed live as
`<realm>::<pod>::nx-<id>-<n>-dt` for a leaf of `nx-<id>-<n>-dt`. A plain lookup on
the reported name therefore misses every disk, so
`resolve_volume_name` tries the name as given and then falls back to a
server-side `name='*::<leaf>'` suffix match. A leaf matching more than one
pod-scoped volume raises rather than picking one.

Three kinds of disk are deliberately **not** migrated:

- **Metadata (`-md`) volumes** are skipped. Before AOS 7.6.0 / Purity//FA 6.12.0
  Nutanix paired one with every vDisk, and SyncRep-protected VMs still do. They
  hold CBT bookkeeping, not guest data, so migrating one would copy metadata
  over a data disk.
- **Nutanix Volume Group disks** (`ADSFVolumeGroupReference`) are rejected with a
  clear error. A Volume Group is a cluster-level object that may be shared by
  several VMs and is itself a collection of vDisks, so it is not one FA volume
  and cannot ride along with a single VM. (A Nutanix Volume Group is also *not*
  the same thing as a FlashArray volume group; FA volume groups are not supported
  on Nutanix.)
- **Disks with no external volume**, or whose volume is not on the connected
  array, are rejected. These are on Nutanix native storage (ADSF) or on a
  different array.

## Migration

**As a source** there is nothing to stage: `prepare_source_disks` is a no-op
because each vDisk is already its own FA volume.

**As a destination** the connector follows the vendor-documented clone/restore
pattern:

1. `create_vm` builds a shell VM with matching vCPU/RAM, firmware and NICs
   (MACs preserved), and no disks.
2. `create_managed_disk` adds one vDisk per source disk **at the source's exact
   size**, then reads the VM back to learn which FA volume Nutanix provisioned
   for it.
3. The migration service overwrites those volumes from the source on the array.

PHIF cannot name the destination volume itself — Nutanix creates the disk object
*and* its backing array volume — hence the read-back in step 2.

Sizing the disk correctly in step 2 (rather than resizing the volume on the array
afterwards) is deliberate: Nutanix owns the volume, and an array-side resize
behind its back leaves Prism's view of the disk wrong. A size mismatch before the
overwrite is the most common way this workflow is gotten wrong.

Boot order needs no separate call — AHV boots the lowest-indexed disk, and disk
index is preserved from the source.

`delete_vm` **refuses** `keep_disks=True`: deleting an AHV VM also deletes its
vDisks and with them the backing FA volumes, so the request cannot be honoured
and is not silently ignored.

### A *move* source needs manual cleanup

`_finalize_move` calls `src.delete_vm(vm_ref, keep_disks=True)`. AHV cannot
delete a VM while keeping its vDisks, so this connector refuses that call.

That used to be dangerous: the failure was only a warning and the finalize went
on to `delete_volume(vol, eradicate=True)`, so the source VM survived while its
volumes were eradicated. The migration service now **skips the volume deletion
entirely unless the source VM was confirmed removed**, and logs exactly what is
left behind.

So a Nutanix *move* source is safe but incomplete: the destination runs off its
own copies, and the source VM plus its volumes remain for the operator to remove.
Nutanix as a **destination**, or as a **copy** source (`_finalize_copy` leaves
the source untouched), is unaffected.

### Detaching a vDisk does not free its array volume

Measured on AOS 7.6 / Purity 6.12.2: after `detach_volumes` removed a vDisk, the
backing FlashArray volume stayed **live and still connected to a Nutanix
stargate host** for at least 150 s, and was not reclaimed. `delete_vm` did not
reclaim it either. Teardown and rollback paths must therefore clean up FA
volumes themselves rather than assume Nutanix releases them.

Cleaning one up requires a **disconnect first**: FlashArray refuses to destroy a
connected volume with an HTTP 400, and Nutanix connects each vDisk's volume to an
individual *stargate host* rather than to a host group — so disconnecting only by
host group leaves it attached. The `delete` action handles this (it resolves the
pod-scoped name, disconnects from whatever host or host group holds the volume,
then destroys it) via `FlashArrayClient.list_volume_connections`.

## API notes

Verified against Prism Central on **AOS 7.6 / AHV 11.2**.

- Port **9440**, HTTP basic auth. Everything goes through Prism Central;
  `/PrismGateway/services/rest/v2.0/...` on the cluster VIP returned 401 for the
  same credentials.
- v4 namespaces present: `vmm`, `clustermgmt`, `volumes`, `networking`. The v4
  **`storage` and `dataprotection` namespaces are absent** (404) on this build,
  so storage containers and external-storage registrations are read through the
  v3 `groups` API — which is what the Prism UI itself queries.
- FlashArray external storage is identified by an `external_storage` entity whose
  `vendor` is `kPureStorage`. On the `storage_container` entity the
  `storage_provider` / `external_storage_type` / `vendor` attributes all come
  back empty, so the provider linkage must come from `external_storage`.
- **`$limit` is capped at 100**; a larger value is rejected with a 400. List
  endpoints are paged with 0-based `$page`, using
  `metadata.totalAvailableResults` as the total.
- The **ETag arrives as a response header spelled `Etag`**, not in the body
  (`$reserved` carries only `$fv`). v4 rejects an in-place update without
  `If-Match`, so `run_http` returns a lower-cased header map for callers to read
  it without guessing the casing.
- Mutations carry an `NTNX-Request-Id` idempotency key so a retry cannot
  double-apply.
- `diskAddress.busType` is reported and accepted **unprefixed** (`"SCSI"`),
  unlike most AHV enums which carry a leading `k`. The connector tolerates both
  on read.
- A NIC has no `model` field; its device type is the backing object type
  (`EmulatedNic` → e1000, `VirtioNic` → virtio). Firmware is signalled the same
  way, by `bootConfig.$objectType` (`LegacyBoot` vs `UefiBoot`).
- Prism lists a `127.0.0.1` placeholder entry per cluster in
  `hypervisor_server_list`; it is not a node.

## Requirements

Per the Everpure compatibility matrix: Prism Central / AOS **7.5+**, AHV **11+**,
a **3-node minimum**, Purity//FA **6.10.3+** (6.11.9 / 6.12.0+ recommended), and
NVMe-oF/TCP enabled on 2+ 25 GbE ports per controller.

## Hardware validation

Read paths and disk resolution were exercised against a live Prism Central
(AOS 7.6 / AHV 11.2) with a FlashArray on Purity 6.12.1: cluster/node discovery,
storage containers, subnets, VM listing, and `capture_vm_spec` across 7 VMs — 5
captured with every disk resolved to its pod-scoped FA volume and serial
(including a 4-disk VM), and 2 correctly rejected for holding Volume Group
disks.

The **write paths** — `create_vm`, `create_managed_disk`, `detach_volumes`,
power operations and `delete_vm` — are covered by unit tests against recorded
payload shapes but have **not** been exercised against live hardware. The lab
cluster available for this work is also 2-node, below the supported 3-node
minimum, so it is suitable for API validation but is not a supportability claim.
