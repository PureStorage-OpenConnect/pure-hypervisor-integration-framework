# HPE VM Essentials (VME) connector

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../../DISCLAIMER.md`](../../DISCLAIMER.md) first.

- **Key:** `hpevme`
- **Name:** HPE VM Essentials
- **Maturity:** `ga`. The Python connector is verified end-to-end in
  `PHIF_MOCK_MODE`, and the **native plugin** (`files/morpheus-plugin/`) is
  **compiled against the real `morpheus-plugin-api` and validated on a live VME
  appliance**: provision, image-based deploy, clone-from-running-VM, and snapshot
  create/revert/delete all operate on the FlashArray. The VME/Morpheus REST +
  Plugin SDK were confirmed against the Morpheus OpenAPI spec and
  `developer.morpheusdata.com`.
  See [The native plugin](#the-native-everpure-data-plugin) and
  [Support status](#support-status).

> **Model note.** HPE VME is rebranded **Morpheus** and exposes the
> `morpheus-plugin-core` SDK, so the *proper* per-VM-disk integration is a **native
> storage provider plugin** that VME loads — VME then orchestrates per-disk volume
> CRUD, array-offloaded snapshot/clone/resize, **and** the KVM/libvirt attach
> itself (no SSH/virsh behind VME's control plane). The other vendor-documented
> path is a *shared datastore* (GFS2 Pool over iSCSI, or NFS); that
> remains available via the [`register_datastore_fallback`](#support-status) helper.

## Goal: per-VM-disk via a native Morpheus/VME plugin

> **ONE FlashArray volume per VM disk**, attached to the KVM VM as a raw
> multipathed block device, with **array-offloaded snapshot / clone / resize** —
> implemented as a native VME plugin so VME drives the lifecycle.

The plugin (Java/Groovy, built under `files/morpheus-plugin/`) registers an Everpure
`StorageServerType` + a `DatastoreTypeProvider`; its `MvmProvisionFacet` emits the
libvirt `<disk type='block'>` config so VME attaches `/dev/mapper/<wwid>` natively.

### Flow

1. **DEPLOY** — upload the Everpure plugin JAR to the VME Manager (`POST /api/plugins/upload`,
   multipart). Makes the "Everpure FlashArray" storage-server type available.
   Idempotent (skips if already installed).
2. **CONFIGURE** — register the FlashArray as a VME **storage server** of that type
   (`POST /api/storage-servers`, `type=pure-flasharray-vme.storage`). Endpoint +
   token come from the **associated array** (`ctx.array.endpoint` /
   `ctx.resolve_token()`); host group + protocol go in the server config.
3. **HOST_REGISTER** — register the VME KVM host initiators (IQNs / WWNs / NQNs) as
   a FlashArray host group (`ctx.array.create_host` + `create_host_group`).
   Initiators are **auto-discovered** from the KVM host when not typed
   (see [Initiator auto-discovery](#initiator-auto-discovery)).
4. **CONNECTIVITY** — prepare iSCSI / FC / NVMe transport + Everpure multipath on the
   KVM hosts (see [Multipath configuration](#multipath-configuration) and
   [Array portal + target discovery](#array-portal--target-discovery)).

After deploy + configure + the array-side prerequisites, **disks are provisioned
in VME** on the Everpure datastore — the plugin runs `createVolume` →
`prepareHostForVolume` → `buildDiskConfig`. The connector also keeps **direct-array**
day-2 ops for management outside VME's flow:

- **PROVISION_VOLUME** — `create_volume` + `connect_volume` (array-side); the guest
  attach is performed by VME via the plugin, not over SSH.
- **SNAPSHOT / CLONE / RESIZE** — array-native (`create_snapshot` / `clone_volume`
  / `extend_volume`).
- **HEALTH** — VME Manager health + instances, plus array info.
- **REMOVE** — disconnect the volume from the host group on the array (the plugin's
  `releaseVolumeFromHost` handles the host-side detach + multipath flush).

## The native Everpure Data plugin

Built under `backend/phif/connectors/hpevme/files/morpheus-plugin/` (see its
[README](../../backend/phif/connectors/hpevme/files/morpheus-plugin/README.md)).

| SDK contract | Class | Purpose |
|---|---|---|
| `StorageProvider` + `StorageServerType` | `PureStorageProvider` | Register the FlashArray as a storage server (verify / capacity refresh) + connection OptionTypes. |
| `StorageProviderVolumes` | `PureStorageProvider` | Direct array volume CRUD (secondary). |
| `DatastoreTypeProvider` | `PureDatastoreProvider` | Per-VM-disk `createVolume` / `removeVolume` / `cloneVolume` / `resizeVolume`. |
| `DatastoreTypeProvider.SnapshotFacet` | `PureDatastoreProvider` | Array-offloaded snapshots + clone-from-snapshot. |
| `DatastoreTypeProvider.MvmProvisionFacet` | `PureDatastoreProvider` | VME per-disk attach: `prepareHostForVolume` (multipath rescan), `buildDiskConfig` → `MvmDiskConfig` (libvirt disk), `releaseVolumeFromHost` (flush). |

Build: produced **automatically** by a `gradle:8.5-jdk11` stage in
`backend/Dockerfile` during the image build (or standalone with
`gradle shadowJar`). The `deploy` action uploads the `*-all.jar` via the Plugins
API. Targets **Java 11** + `com.morpheusdata:morpheus-plugin-api:1.2.9` (+ Karman
`karman-core:2.0.5` at compile time). Items still needing on-appliance
validation are listed in the plugin README and under
[Open items](#open-items-validate-on-a-live-vme-appliance).

All VME Manager calls go through `ctx.runner.run_http` (mock-safe); array-side
via `ctx.array`; host-side (libvirt/multipath) via `ctx.runner.run_ssh`.
`ctx.dry_run` is honored (no FA volumes/host groups created; no attach).

## Initiator auto-discovery

The operator does **not** have to type host initiator IDs. When the manual
initiator fields on `register_hosts` (`iqns` / `wwns` / `nqns`) are left blank —
and no `host_wwns` is configured for FC — the connector **auto-discovers** them
from the VME KVM host via the shared `JobRunner.discover_initiators(...)` helper,
which reads the standard Linux locations over SSH:

| Protocol | Source | FA registration field |
|---|---|---|
| `iscsi` | `/etc/iscsi/initiatorname.iscsi` | IQN |
| `nvme-tcp` | `/etc/nvme/hostnqn` | NQN |
| `fc` | `/sys/class/fc_host/*/port_name` (`0x`-stripped) | WWNs (normalized to colon form) |

Rules:

- **Explicit values always override discovery.** Anything typed in the action
  fields (or `host_wwns` for FC) is used as-is and discovery is skipped for that
  protocol.
- The relevant initiator is picked **per protocol**: `iscsi` → IQN, `nvme-tcp` →
  NQN, `fc` → WWNs. Only the protocol-relevant set is registered (FC hosts carry
  WWNs only, never IQN/NQN).
- The fields are **optional** in the UI with the help text
  *"auto-discovered from the KVM host if left blank"*. There is **no hard-fail**
  when initiators are absent — the connector discovers them instead. (FC still
  fails only if neither explicit nor discovered WWNs are available and not in
  dry-run.)
- **Mock / dry-run safe:** `discover_initiators` returns synthetic values in
  `PHIF_MOCK_MODE` / dry-run, so flows stay exercisable, and dry-run still creates
  no FA host/host group.
- `create_host` / `create_host_group` are **idempotent + additive**, so re-running
  registration after discovery safely converges to the desired initiator set.

### Initiator display in the UI

`discover_options("initiators")` returns the KVM host's discovered IQN / NQN / WWN
**tagged with the matching `register_hosts` form field** (`iqns` / `nqns` / `wwns`)
plus a human `label`, so the UI can show the operator what was found and pre-fill
the corresponding input:

| Discovered | `field` | Example `label` |
|---|---|---|
| iSCSI IQN | `iqns` | `iSCSI IQN — iqn.1993-08.org.debian:01:<tag>` |
| NVMe NQN | `nqns` | `NVMe NQN — nqn.2014-08.org.nvmexpress:uuid:<tag>` |
| FC WWNs | `wwns` | `FC WWNs — 21:00:00:24:ff:00:00:01, …` (colon-normalized) |

### FlashArray host name sanitize

FlashArray object names allow only `[A-Za-z0-9-]`. For a **single-host**
deployment the FA host name is derived from the host group (`<host_group>-vme`);
for a **cluster** each KVM host gets its own FA host named
`<host_group>-<node_name>` (see [Cluster support](#cluster-multi-kvm-host-support)).
Since a host group may itself be derived from a hostname/IP (e.g.
`192.0.2.58`), the connector **sanitizes** any invalid character to `-` (so
`192.0.2.58` → `192-0-2-58-vme`) and trims edge hyphens.

## Cluster (multi-KVM-host) support

HPE VME is a clustered hypervisor: the VME Manager manages **several KVM hosts**.
The connector implements the shared cluster contract from
`phif.connectors.base` so node-specific actions fan out across every KVM host
while cluster-level actions run once.

### `list_nodes()`

Queries the VME Manager REST for the cluster's KVM hosts and returns one
`ClusterNode(name, host=<mgmt/ip>)` per host. The `host` is the SSH/management
address used for host-side work (multipath, rescan, libvirt attach).

- **Live:** `GET /api/servers` (Morpheus lineage), parsing each host's name and
  management IP. `TODO(doc-validate)`: the exact resource (`/api/servers` vs
  `/api/hosts`), the host *type* that identifies a VME KVM host, and which field
  carries the management IP (`sshHost` / `internalIp` / `externalIp`).
- **Mock / dry-run:** the manager REST short-circuits (`run_http` returns `{}`),
  so a **synthetic 2-host cluster** (`vme-kvm1`, `vme-kvm2`) is returned to keep
  the cluster flow exercisable.
- **Fallback:** if no hosts are returned (or the shape is unexpected), a **single
  host** derived from the connection is returned, so a standalone deployment
  works unchanged.

### Fan-out vs. cluster-level

| Action | Scope | Behavior |
|---|---|---|
| `register_hosts` | **per host** | One FA host per KVM host, **all in the single shared `host_group`**. Each host's initiators are auto-discovered per node; explicit action-field initiators apply to the **connection node only**. |
| `setup_connectivity` | **per host** | The transport setup (interface binding) + the Everpure multipath drop-in / rescan run on **each** KVM host over SSH. The array portal/target discovery is **cluster-level** and runs **once**. |
| `deploy` / `configure` / datastore registration | **cluster-level** | The VME storage integration is registered once for the cluster. |
| `provision` / `snapshot` / `clone` / `resize` | **array-level** | Per-disk array operations are unchanged. |

Single-host behavior is preserved: when `list_nodes()` returns one node,
`register_hosts` keeps the historical `<host_group>-vme` FA host name and
`setup_connectivity` behaves exactly as before.

### `validate_cluster(**params)`

For the active protocol, discovers the protocol-relevant interfaces on **each**
KVM host (`iscsi` → `nics`, `fc` → `fc_hbas`, `nvme-tcp` → `nvme_sources`) via the
shared `runner.discover_interfaces(...)` helper and compares the sets across hosts
with `compare_node_interfaces(...)`. Returns **ok** when every host exposes the
same interface set (so the chosen storage binding is valid cluster-wide), or
**fail** with a per-host detail when they differ. A single node always validates.

For the `nics` kind (iSCSI/NVMe-TCP), each host's NICs are first filtered to the
array's **storage subnet** via `nics_on_array_subnets(...)` before the comparison,
so only the storage NICs are compared — not every mgmt NIC / VM tap / VLAN, which
legitimately differ between hosts. `fc_hbas` are compared as discovered.

### `wizard_steps()`

`["deploy", "configure", "register_hosts", "setup_connectivity"]` — deploy +
configure the cluster-level VME storage integration first, then register each KVM
host on the array and set up per-host connectivity. Only ids present in
`action_schemas` are used by the orchestrator (all four are).

## Array portal + target discovery

For the **IP transports** (`iscsi`, `nvme-tcp`), `setup_connectivity` discovers
the connection targets from the FlashArray so the operator never types them:

- **Portal IPs** — `ctx.array.get_data_interfaces(service)` where `service` is
  `iscsi` or `nvme-tcp` (the array's enabled data-network IPs serving that
  transport).
- **Target identifier** — `ctx.array.get_target_ports()` → the iSCSI target IQN
  (`iscsi`) or the NVMe subsystem NQN (`nvme-tcp`).

What was found is emitted via `ctx.emit` and echoed in the `OpResult.data`
(`portals`, `target_iqn`, `target_nqn`).

**Fail-fast:** if an IP transport yields **no portals** (and not in dry-run), the
action fails with an actionable message — the array's `iscsi`/`nvme-tcp`
interfaces have no IP configured (the array may be **Fibre Channel / NVMe-FC
only**); the operator should configure that transport's networking on the
FlashArray, switch the hypervisor `protocol` to `fc`, or pass portals explicitly.

**FC** has no portals (connectivity is fabric zoning), so portal/target discovery
is **skipped** for `protocol=fc`.

## Multipath configuration

For the **SCSI transports** (`iscsi`, `fc`), `setup_connectivity` installs the
**Everpure-recommended multipath drop-in** at `/etc/multipath/conf.d/pure.conf` on the
VME KVM host over SSH and restarts `multipathd` (`_write_multipath_conf`, mirroring
the Proxmox / XCP-ng connectors). Using a drop-in (not overwriting the operator's
`/etc/multipath.conf`) preserves any local-disk blacklists / other-array config.
The written config sets:

- `find_multipaths no` so **every** Everpure LUN is auto-claimed into a multipath map
  (not just ones already seen twice) — important when a freshly-connected volume
  must appear immediately on each host;
- `user_friendly_names no` so devices are **always WWID-named**
  (`/dev/mapper/3624a9370<serial>`), never `mpathN` aliases;
- a `device { vendor "PURE" product "FlashArray" ... }` stanza with ALUA
  (`prio alua`, `hardware_handler "1 alua"`, `path_grouping_policy group_by_prio`),
  `path_selector "service-time 0"`, `path_checker tur`, `failback immediate`, and
  the recommended `fast_io_fail_tmo` / `dev_loss_tmo` / `no_path_retry` values.

This runs for `iscsi` and `fc` (the `setup_connectivity` result `steps` includes
`multipath_conf`). **NVMe-TCP and NVMe-FC** use native NVMe multipath and do
**not** get a device-mapper drop-in written (no `multipath_conf` step). The write
is a **dry-run no-op** and mock-safe.

### SCSI WWID → `/dev/mapper` device path

The multipath device path is built by `_scsi_wwid`, mirroring the Proxmox
`PureFAPlugin.pm` logic. A FlashArray volume's SCSI WWID is NAA IEEE Registered
Extended (type 6) with the Everpure OUI:

```
wwid = "3" + "624a9370" + lc(serial)      # e.g. 3624a93700123456789abcdef0bb82813
```

The FA REST returns the 24-hex serial **without** the `624a9370` OUI, so the
helper prepends it — and **tolerates** a serial that already includes `624a937`
(and strips a leading `0x`). The resolved device is `/dev/mapper/<wwid>`.

`_resolve_mpath` looks the **real array-assigned serial** up via
`ctx.array.get_volume(name)` (added to the shared FlashArray client) and builds
the WWID from it — *not* from the volume name. If the serial can't be fetched
(dry-run / no associated array) it falls back to deriving the id from the name so
the flow stays exercisable. For **NVMe-oF** (`nvme-tcp` / `nvme-fc`) the namespace
is identified by its EUI instead (`/dev/disk/by-id/nvme-eui.<lc(serial)>`).

## Fibre Channel (FC)

FC is a fully implemented transport for this connector (alongside `iscsi` and
`nvme-tcp`), and differs from the IP transports in three ways: there is **no
software login**, hosts are registered **by WWN**, and devices are reached purely
by **multipath WWID**.

### Prerequisite: SAN zoning

> **SAN zoning is a hard prerequisite for FC and is performed on the fabric
> switch, outside this connector.** The VME KVM host's HBA port WWNs must be zoned
> to the FlashArray's FC target ports before any volume can be seen. There is no
> `iscsiadm`/`nvme connect` equivalent for FC — connectivity is established at the
> fabric layer. Everpure's recommendation is single-initiator / multi-target zoning
> per HBA port.

### Host WWNs

Set `protocol=fc` and optionally provide the KVM host HBA port WWNs via the
`host_wwns` connection field (comma-separated), or per-action via the `wwns` field
on `register_hosts`. If neither is supplied, the connector **auto-discovers them**
from the KVM host via the shared `JobRunner.discover_initiators(...)` helper
(reads `/sys/class/fc_host/*/port_name`, normalized from `0x<16hex>` to
colon-separated WWNs). See [Initiator auto-discovery](#initiator-auto-discovery).

### FC flow

1. **HOST_REGISTER** — when `protocol=fc`, the VME KVM host is registered on the
   array **by WWN only** (`ctx.array.create_host(name, wwns=[...])`), never by
   IQN/NQN. If no WWNs are configured they are auto-discovered via the shared
   `discover_initiators` helper.
2. **CONNECTIVITY** — no `iscsiadm` / `nvme connect`. The connector documents the
   zoning prerequisite and performs a host-side **FC/SCSI rescan over SSH**
   (`rescan-scsi-bus.sh -a`, falling back to `issue_lip` on each
   `/sys/class/fc_host/*` plus a `- - -` scan on each `/sys/class/scsi_host/host*`),
   writes the Everpure-recommended `/etc/multipath.conf` and restarts `multipathd`
   (see [Multipath configuration](#multipath-configuration)), then refreshes
   multipath (`multipath -r`).
3. **PROVISION** (direct-array over FC) — create the FA volume and connect it to
   the FC host group; the device resolves **by multipath WWID** via `_scsi_wwid`
   (`3` + Everpure OUI `624a9370` + `lc(serial)`; see
   [SCSI WWID → `/dev/mapper` device path](#scsi-wwid--devmapper-device-path)),
   with **no login**. The guest attach (host rescan + libvirt disk) is performed by
   **VME via the plugin's `MvmProvisionFacet`** when the disk is added to a VM, not
   over SSH.
4. **SNAPSHOT / CLONE / RESIZE** — array-native and unchanged across transports.

**NVMe-FC** follows the same fabric pattern: the host is registered by **NQN**
(the array identifies all NVMe transports by NQN), connectivity assumes fabric
zoning and nudges the autoconnect (`nvme connect-all -t fc`), and the namespace is
reached via native NVMe multipath (`/dev/disk/by-id/nvme-eui.<serial>`).

## Interface binding

Because volumes are attached **per VM disk** directly to VMs, there is no shared
datastore to pin transport to — instead the **host interface binding** governs
which KVM-host interfaces carry the storage sessions. `setup_connectivity`
exposes optional, dynamically-populated binding fields and applies the binding
**only for the active `protocol`** (over SSH, mock-safe, honoring `ctx.dry_run`).

### Discovering choices

`discover_options(kind)` delegates to the shared
`JobRunner.discover_interfaces(kvm_host, kind, ...)` helper (SSH to the VME KVM
host; synthetic entries in `PHIF_MOCK_MODE`/dry-run). The UI calls it via
`GET /hypervisors/{id}/discover/{kind}` to populate the multiselects:

| `kind` | Discovers | Source on host |
|---|---|---|
| `nics` | Ethernet NICs for iSCSI `iface` binding | `ip -o link show` |
| `nvme_sources` | IP-bearing interfaces for NVMe-TCP `host-traddr` | `ip -o -4 addr show` |
| `fc_hbas` | Fibre Channel HBAs (WWPNs) | `/sys/class/fc_host/*` |

**Storage-subnet NIC filtering.** For the `nics` kind, when the hypervisor has an
associated FlashArray, `discover_options("nics")` filters the discovered NICs down
to only those whose network (from each NIC's `cidr`) **contains one of the array's
storage portal IPs** — via the shared `nics_on_array_subnets(...)` helper, using
`ctx.array.get_data_interfaces(service)` for the active protocol's service
(`nvme-tcp` when `protocol=nvme-tcp`, else `iscsi`). This drops management NICs, VM
taps/bridges, and unrelated VLANs so the operator only binds NICs that are actually
on the storage network. If **nothing matches** (or there is **no associated array /
no portals**), the NICs are returned **unfiltered** so selection still works.
`fc_hbas` and `nvme_sources` are **not** subnet-filtered (FC has no portals; NVMe
source addresses are matched separately). Mirrors the Proxmox connector.

> `TODO(doc-validate)`: the KVM-host SSH credential source for discovery is
> reused from the connection (`username`/`password`/`ssh_key`); a dedicated
> KVM-host SSH credential is still unspecified.

### Binding fields on `setup_connectivity`

| Field | Type | `options_source` | Applies when |
|---|---|---|---|
| `iscsi_nics` | multiselect (optional) | `nics` | `protocol=iscsi` |
| `nvme_sources` | multiselect (optional) | `nvme_sources` | `protocol=nvme-tcp` |
| `nvme_options` | string (optional) | — | `protocol=nvme-tcp` |
| `fc_hbas` | multiselect (optional) | `fc_hbas` | `protocol=fc` |

### Apply behavior (per active protocol)

- **iSCSI** — for each selected NIC, create/update an `iscsiadm` `iface` object
  pinned to the NIC's netdev (`iface.net_ifacename`) so sessions egress that
  interface.
- **NVMe-TCP** — `nvme connect -t tcp -w <host-traddr>` per selected source
  address, with any `nvme_options` appended. `TODO(doc-validate)`: the target
  transport address/port and subsystem NQN (`-a`/`-s`/`-n`) are VME/array
  discovery specifics — only the host-side source (`-w`) is pinned here.
- **FC** — no software login; the selected HBA WWPNs are matched to their
  `/sys/class/fc_host/hostN` and only those `scsi_host`s are rescanned.
  `TODO(doc-validate)`: WWPN → `fc_host` mapping and SAN zoning for the selected
  HBAs.

The chosen binding is **persisted in the connector state**
(`ctx.target.connection["interface_binding"]`) when not in dry-run, and echoed in
the `OpResult.data["binding"]`. In dry-run the binding is planned/logged but no
SSH binding commands run and nothing is persisted.

## Support status

**Researched June 2026** against the Morpheus OpenAPI spec (VME Manager *is*
rebranded Morpheus 8.x; the hypervisor is "HVM" in the API), the HPE VME docs
(`hpevm-docs.morpheusdata.com`), and Everpure's HPE VME solution bundle.

- **No *built-in* Everpure storage-provider type** ships in Morpheus/VME (the
  built-in `storage-server` types are 3Par / Dell ECS / Dell Isilon / HPE Alletra)
  — but the **plugin SDK** lets us add one. The native plugin under
  `files/morpheus-plugin/` registers an Everpure `StorageServerType` + a
  `DatastoreTypeProvider` with `MvmProvisionFacet`, so VME drives per-disk volume
  CRUD, array-offloaded snapshot/clone/resize, **and the libvirt attach** itself.
- **The vendor also documents a *datastore* model**: a FlashArray volume
  formatted as a **GFS2 Pool** (iSCSI) or an **NFS** export. The
  `register_datastore_fallback(...)` helper covers this (`POST /api/data-stores`);
  it is intentionally **NOT** in `CAPABILITIES` / `action_schemas`.
- **The Python connector's job is deploy + configure + array-side prerequisites:**
  upload the plugin JAR (`POST /api/plugins/upload`), register the storage server
  (`POST /api/storage-servers`), register the FA host group, and set up
  transport + multipath. Per-disk provisioning then happens **in VME** via the
  plugin. Array ops are real today via `ctx.array` (FlashArray REST v2).
- **Confirmed VME/Morpheus API** (now baked in, no longer guessed): OAuth is
  `POST /oauth/token` **form-encoded**; hosts are `GET /api/servers?vmHypervisor=true`
  with the management address in `sshHost`; liveness `GET /api/ping`, identity
  `GET /api/whoami`; datastores `GET/POST /api/data-stores`; health `GET /api/health`.
- **Array side is real today:** volume create / connect / disconnect / serial
  lookup / snapshot / clone / resize all use `ctx.array` and are exercised against
  the mock array.

| Aspect | Status |
|---|---|
| FA volume-per-disk create/connect | Implemented (array-side, mock-verified) |
| FA snapshot / clone / resize | Implemented (array-side, mock-verified) |
| Host (initiator) registration on FA | Implemented (array-side, mock-verified) |
| Cluster support (multi-KVM-host) | Implemented (mock-verified): `list_nodes` (VME REST → synthetic 2-host cluster in mock; single-host fallback), per-host fan-out for `register_hosts` + `setup_connectivity`, `validate_cluster`, `wizard_steps`; exact VME host-list endpoint/shape **TODO(doc-validate)** |
| Initiator auto-discovery (IQN/NQN/WWN) | Implemented via shared `discover_initiators` (mock-verified); operator no longer types initiators |
| Initiator display (`discover_options("initiators")`) | Implemented (mock-verified); IQN/NQN/WWN tagged with `register_hosts` fields for UI pre-fill |
| Array portal + target discovery (IP transports) | Implemented via `get_data_interfaces` / `get_target_ports` (mock-verified); fail-fast when array is FC-only |
| Native plugin (StorageProvider + DatastoreTypeProvider + MvmProvisionFacet + SnapshotFacet) | `files/morpheus-plugin/`; **compiles against the real `morpheus-plugin-api` (built in the backend image) and validated on a live VME appliance** (provision, image deploy, clone-from-VM, snapshot create/revert/delete) |
| Deploy = upload plugin JAR | Implemented — `POST /api/plugins/upload` (multipart), idempotent (skips if installed) |
| Configure = register storage server | Implemented — `POST /api/storage-servers` `type=pure-flasharray-vme.storage`; endpoint/token from associated array |
| FC host register by WWN + WWN discovery | Implemented (array-side, mock-verified); zoning is an operator prerequisite |
| FC connectivity (rescan, no iscsiadm/login) | Implemented (host-side over SSH, mock-verified) |
| FC provision (direct-array create + connect) | Implemented; guest attach is VME's job via the plugin |
| Interface binding (iSCSI iface / NVMe `-w` / FC HBA select) | Implemented (host-side over SSH, mock-verified); NVMe target + FC WWPN→fc_host specifics **TODO(doc-validate)** |
| Interface discovery (`nics`/`nvme_sources`/`fc_hbas`) | Implemented via shared `discover_interfaces` (mock-verified) |
| Storage-subnet NIC filtering (`nics`) | Implemented via shared `nics_on_array_subnets` (mock-verified): `discover_options("nics")` and `validate_cluster` keep only NICs on the array's iSCSI/NVMe-TCP portal subnet; unfiltered when no array/portals/match; mirrors Proxmox |
| VME Manager auth (OAuth) | Implemented — `POST /oauth/token` **form-encoded** (confirmed) |
| Host enumeration | `GET /api/servers?vmHypervisor=true`, mgmt IP `sshHost` (confirmed) |
| Per-VM-disk guest attach | Done by VME via the plugin's `MvmProvisionFacet` (`prepareHostForVolume` + `buildDiskConfig` → libvirt disk); no SSH/virsh from PHIF |
| Multipath drop-in on KVM hosts (iSCSI/FC) | Implemented (host-side over SSH, mock-verified): `/etc/multipath/conf.d/pure.conf` (PURE/FlashArray ALUA stanza, `find_multipaths no`, `user_friendly_names no`) + `multipathd` restart |
| SCSI WWID build (`3` + `624a9370` + lc(serial)) | Implemented via `_scsi_wwid` from the **real array serial** (`ctx.array.get_volume`); NVMe uses `eui.<serial>`; mirrors Proxmox/XCP-ng |
| Datastore model (GFS2/NFS) | Helper present (`register_datastore_fallback`, `POST /api/data-stores`); not a declared capability — this is the vendor-documented model |

## Capabilities

`CONNECT` (implicit), `HOST_REGISTER`, `CONNECTIVITY`, `DEPLOY_PLUGIN`,
`CONFIGURE`, `PROVISION_VOLUME`, `SNAPSHOT`, `CLONE`, `RESIZE`, `HEALTH`,
`REMOVE`.

`PROVISION_DATASTORE` is **not** declared (per-disk model is the target).

**Supported protocols:** `iscsi`, `fc`, `nvme-tcp`, `nvme-fc`. SCSI transports
(`iscsi`/`fc`) use dm-multipath; NVMe transports (`nvme-tcp`/`nvme-fc`) use native
NVMe multipath and are registered on the array by NQN.

## Connection fields (`target_schema`)

| Field | Type | Notes |
|---|---|---|
| `vme_manager_url` | string | Base URL of the VME Manager appliance (e.g. `https://vme-mgr.example.local`). |
| `username` | string | VME Manager user. Default `admin`. |
| `password` | secret | Stored encrypted in the vault. |
| `protocol` | enum | `iscsi` (default), `fc`, `nvme-tcp`, `nvme-fc`. |
| `host_group` | string (optional) | Default FlashArray host group for the VME KVM nodes. |
| `host_wwns` | string (optional) | FC HBA port WWNs of the VME KVM host(s), comma-separated. Used only when `protocol=fc`; auto-discovered from the KVM host if left blank. |

## Day-2 actions

| Action id | Capability | Description |
|---|---|---|
| `register_hosts` | HOST_REGISTER | FA host group from VME host initiators (IQN/WWN/NQN fields optional — auto-discovered from the KVM host when blank). |
| `setup_connectivity` | CONNECTIVITY | Prepare transport + multipath. For IP transports, discover array portals + target IQN/NQN and fail-fast if none (array may be FC-only). |
| `deploy` | DEPLOY_PLUGIN | Upload the native Everpure plugin JAR to the VME Manager (`POST /api/plugins/upload`, multipart). Idempotent — skips if `pure-flasharray-vme` is already installed. The JAR is built automatically into the backend image (or set `plugin_jar`). |
| `configure` | CONFIGURE | Register the FlashArray as a VME storage server (`POST /api/storage-servers`, `type=pure-flasharray-vme.storage`). Endpoint/token from the associated array (not form fields); host group + protocol in the server config. |
| `provision` | PROVISION_VOLUME | Direct-array: create FA volume → connect to host group. Guest attach is VME's job via the plugin (no SSH). |
| `snapshot` | SNAPSHOT | Array snapshot of a volume. |
| `clone` | CLONE | Array clone of a volume. |
| `resize` | RESIZE | Extend a volume. |
| `health_check` | HEALTH | VME health + instances + array info. |
| `teardown` | REMOVE | Disconnect the volume from the host group on the array. FA volume preserved (not destroyed); the plugin's `releaseVolumeFromHost` does the host-side detach + multipath flush. |

## VME Manager REST API (Morpheus lineage — confirmed)

Authentication is the standard Morpheus OAuth flow with a **form-encoded**
(`application/x-www-form-urlencoded`) body:

```
POST /oauth/token            # NOT under /api
  grant_type=password
  client_id=morph-api
  scope=write
  username=<user>            # sub-tenant: subdomain\username
  password=<pass>
→ { access_token, refresh_token, expires_in, token_type:"Bearer", scope }
```

Subsequent `/api/*` calls send `Authorization: Bearer <access_token>`.

| Concern | Endpoint | Notes |
|---|---|---|
| Liveness | `GET /api/ping` | Unauthenticated; fail-fast reachability probe. |
| Identity | `GET /api/whoami` | Validates the token; returns user + permissions. |
| Hosts | `GET /api/servers?vmHypervisor=true` | UI "Hosts" == API `servers` (there is **no** `/api/hosts`). Mgmt IP in `sshHost` → `internalIp` → `externalIp`; host kind in `computeServerType.code`. |
| Instances (VMs) | `GET /api/instances` | Disks are `StorageVolume` objects managed at the **server** level. |
| Plugins | `GET /api/plugins`, `POST /api/plugins/upload` | List installed plugins; upload a plugin JAR (multipart, file part named `plugin`). `POST /api/plugins` is GET-only → 404. Used by `deploy`. |
| Storage servers | `GET/POST /api/storage-servers`, `GET /api/storage-server-types` | The Everpure type (`pure-flasharray-vme.storage`) is provided by **our plugin**, not built-in. Used by `configure`. |
| Datastores | `GET/POST /api/data-stores` | Hyphenated. `datastore.storageServer.id` links a storage server. API-driven creation is restricted. |
| Attach existing volume | `PUT /api/servers/{id}/volumes/{volumeId}/attach` | "HVM only"; attaches a volume Morpheus already tracks. The plugin's `MvmProvisionFacet` is the path used for per-disk attach, not this REST call. |
| Health | `GET /api/health` | Appliance health/alarms (authenticated). |

> **Per-disk guest attach is handled inside VME by the plugin** (`MvmProvisionFacet`:
> `prepareHostForVolume` rescans multipath, `buildDiskConfig` emits the libvirt
> `<disk type='block'>`), so PHIF never attaches over SSH/virsh.

## References

- HPE VME docs (clusters / storage): <https://hpevm-docs.morpheusdata.com>
- Morpheus API — Authentication / Get Access Token:
  <https://apidocs.morpheusdata.com/reference/getaccesstoken>
- Morpheus API — Ping: <https://apidocs.morpheusdata.com/reference/ping-1>
- Morpheus API — Servers / Storage Servers / Datastores:
  <https://apidocs.morpheusdata.com/reference/getstorageservers>,
  <https://apidocs.morpheusdata.com/reference/savedatastore>
- Morpheus OpenAPI source (authoritative paths/schemas):
  <https://github.com/HewlettPackard/morpheus-openapi>
- Everpure HPE VME solution bundle (iSCSI/NFS best practices, GFS2 Pool datastore):
  <https://support.purestorage.com/bundle/m_hewlett_packard_enterprise>

## Resolved by research (June 2026)

The following were previously `TODO(doc-validate)` and are now **confirmed** in
`connector.py`:

- **OAuth** — `POST /oauth/token`, **form-encoded** body (`grant_type=password`,
  `client_id=morph-api`, `scope=write`). `run_http` gained a `data=` (form) arg.
- **Reachability / identity** — `GET /api/ping` (liveness) + `GET /api/whoami`.
- **Host enumeration** — `GET /api/servers?vmHypervisor=true`; mgmt IP `sshHost`
  → `internalIp` → `externalIp`; host kind `computeServerType.code`. There is no
  `/api/hosts`.
- **Native plugin is the right path** — the Morpheus Plugin SDK exposes
  `StorageProvider`/`StorageServerType`, `DatastoreTypeProvider` (+`SnapshotFacet`),
  and `MvmProvisionFacet` (VME per-disk libvirt attach). `deploy` uploads the JAR
  (`POST /api/plugins/upload`); `configure` registers the storage server
  (`POST /api/storage-servers`, plugin type code).
- **Datastore path** — `/api/data-stores` (hyphenated), not `/api/datastores`.
- **Health** — `GET /api/health` + `GET /api/instances`.
- **WWID from real serial** — `ctx.array.get_volume(name).serial` drives
  `_scsi_wwid`; multipath drop-in uses `find_multipaths no` + `user_friendly_names no`.

## Open items (validate on a live VME appliance)

These are the `TODO(validate-on-appliance)` markers in the plugin source plus the
build/run gap:

1. **Load the plugin** — it now compiles against the real SDK
   (`morpheus-plugin-api:1.2.9`, built in the backend image); upload it via the
   `deploy` action and confirm the Everpure StorageServerType + datastore type appear
   in VME.
2. **MVM provision type code** — confirm `getProvisionTypeCode()` (the plugin uses
   `"mvm"` as a placeholder) binds the datastore type to VME's KVM provisioner.
3. **`MvmDiskConfig` → libvirt mapping** — confirm how `MvmDiskConfig` +
   `StorageVolume.deviceName`/`wwn` produce the `<disk><source dev=…/>` for a raw
   multipath block device (the SDK's public fields are only `diskMode`/`diskType`/
   `deviceName`/`deviceType`).
4. **`executeCommandOnServer` signature** — used in `prepareHostForVolume` /
   `releaseVolumeFromHost` for the multipath rescan/flush; confirm signature + return.
5. **StorageServer config persistence** — `refreshStorageServer` capacity sync and
   whether full per-volume inventory sync is desired.
6. **NVMe-oF device path** — confirm the namespace path on a VME KVM host
   (`/dev/disk/by-id/nvme-eui.*` vs nguid) and `nvme-fc` autoconnect.
7. **Datastore model payload** — if the (also-supported) GFS2/NFS datastore model
   is promoted to a capability, confirm the `POST /api/data-stores` payload.

## Verify

```bash
# Python connector (deploy=upload plugin, configure=register storage server, ...)
cd backend
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest tests/test_hpevme.py -q

# Native plugin: built automatically by the backend Docker image
# (gradle:8.5-jdk11 stage). To build standalone (JDK 11 + Gradle):
cd phif/connectors/hpevme/files/morpheus-plugin && gradle shadowJar
```


## Manual installation (without PHIF)

This procedure builds and installs the native Everpure FlashArray storage plugin on an HPE VM Essentials (VME) / Morpheus appliance **by hand**, mirroring exactly what the PHIF `hpevme` connector's `deploy` and `configure` actions automate. Commands, paths, API routes, and type codes are taken from the plugin sources under `backend/phif/connectors/hpevme/files/morpheus-plugin/` and `connector.py`.

### Prerequisites

- **JDK 11.** The plugin targets Java 11 (`sourceCompatibility`/`targetCompatibility = '11'`) because the published `morpheus-plugin-api` is a Java 11 artifact. Use a JDK 11 + Gradle, or the `gradle:8.5-jdk11` image.
- **VME SDK / toolchain versions** (from `gradle.properties`, matching `morpheus-plugin-core rel-2.10.0`): `morpheusApiVersion=1.3.4`, `groovyVersion=3.0.9`, `karmanVersion=2.0.5` (all `compileOnly`/build-time — the appliance provides them at runtime, so the shadow JAR stays lean).
- A **VME Manager (Morpheus) account** able to upload plugins and create storage servers, plus the appliance base URL.
- A **FlashArray** REST v2 management endpoint and an **API token**.

### 1. Build the plugin shadow ("fat") JAR

```bash
cd backend/phif/connectors/hpevme/files/morpheus-plugin
gradle shadowJar          # or ./gradlew shadowJar
```

The connector's default path (`_PLUGIN_JAR_DEFAULT`) is `build/libs/pure-flasharray-vme-plugin-0.1.0-all.jar`, but `build.gradle` sets `version = '0.1.37'`, so the real file is `pure-flasharray-vme-plugin-0.1.37-all.jar`. The connector globs `build/libs/*-all.jar` (newest wins), so any version works. The JAR manifest carries `Plugin-Class: com.morpheusdata.pure.PureStoragePlugin`.

### 2. Upload the JAR to the VME Manager (Plugins API)

Get a bearer token via the Morpheus OAuth contract (`POST /oauth/token`, form-encoded, no `/api` prefix):

```bash
TOKEN=$(curl -sk -X POST "https://vme-mgr.example.local/oauth/token" \
  -d grant_type=password -d client_id=morph-api -d scope=write \
  -d username=admin -d password='<password>' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
```

Upload the shadow JAR — endpoint **`POST /api/plugins/upload`** (multipart), file part named **`plugin`**:

```bash
curl -sk -X POST "https://vme-mgr.example.local/api/plugins/upload" \
  -H "Authorization: Bearer $TOKEN" \
  -F "plugin=@build/libs/pure-flasharray-vme-plugin-0.1.37-all.jar;type=application/java-archive"
```

The upload is asynchronous (the plugin goes `installing` → `loaded` on every node, **no restart**). Confirm:

```bash
curl -sk "https://vme-mgr.example.local/api/plugins" -H "Authorization: Bearer $TOKEN"
# look for "code" == "pure-flasharray-vme"
```

**Or via the VME UI:** Administration → Integrations → Plugins → Choose File → upload the `*-all.jar`. Once loaded, the plugin registers a **`Everpure FlashArray`** `StorageServerType` and the per-disk `DatastoreTypeProvider`.

### 3. Register the FlashArray as a VME storage server

Mirrors `configure`: **`POST /api/storage-servers`** with a `storageServer` whose `type` is the plugin's `StorageServerType` code **`pure-flasharray-vme.storage`** (`PureStorageProvider.PROVIDER_CODE`):

```bash
curl -sk -X POST "https://vme-mgr.example.local/api/storage-servers" \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{
        "storageServer": {
          "name": "pure-<endpoint>",
          "type": "pure-flasharray-vme.storage",
          "serviceUrl": "https://flasharray.example.local",
          "serviceToken": "<FlashArray REST API token>",
          "config": { "hostGroup": "<vme-host-group>", "protocol": "iscsi" }
        }
      }'
```

Fields (from `PureStorageProvider.storageServerOptionTypes()`):
- `serviceUrl` — FlashArray management IP/DNS (REST v2 endpoint).
- `serviceToken` — FlashArray API token.
- `config.hostGroup` — the FlashArray host group containing the VME KVM hosts; per-disk volumes connect to it (required).
- `config.protocol` — `iscsi` | `fc` | `nvme-tcp` (default `iscsi`).

**Or via the VME UI:** Infrastructure → Storage → Storage Servers → add a server of type **Everpure FlashArray** (Management Endpoint, API Token, Host Group, Protocol, optional "Eradicate on Delete").

### 4. Register the KVM host(s) on the FlashArray as a host group

Mirrors `register_hosts` (array-side). For each VME KVM host:

1. Discover its initiator: iSCSI `/etc/iscsi/initiatorname.iscsi`, NVMe `/etc/nvme/hostnqn`, FC `/sys/class/fc_host/*/port_name`.
2. On the FlashArray, create **one host per KVM node** with that node's protocol-appropriate initiator, then place all into one shared **host group**. Single node → host `<host_group>-vme`; cluster → `<host_group>-<node-name>` (names allow only `[A-Za-z0-9-]`). The group name must match `config.hostGroup` from step 3.
3. **FC**: the WWNs must already be zoned to the array (no software login). **iSCSI/NVMe-TCP**: set up session login + the Everpure multipath drop-in `/etc/multipath/conf.d/pure.conf` (the connector's `setup_connectivity`).

After this, disk provisioning happens **in VME** on the Everpure datastore: the plugin's `DatastoreTypeProvider` creates one FA volume per VM disk and, via `MvmProvisionFacet`, presents it to the host group and emits the libvirt disk config so VME attaches `/dev/mapper/<wwid>` natively as a raw multipathed block device.

> Re-uploading the **same** plugin version does not re-register provider changes (VME keys registration by version at load). After a code change, bump `version` in `build.gradle` and re-upload (the connector's `deploy` exposes `force=true` for this).
