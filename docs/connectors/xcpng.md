# XCP-ng / XenServer connector

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../../DISCLAIMER.md`](../../DISCLAIMER.md) first.

- **key:** `xcpng`
- **name:** XCP-ng / XenServer (Everpure SR driver)
- **maturity:** `ga` — the SMAPIv3 driver is validated end-to-end on a live
  2-host pool (XCP-ng 8.3 / xapi 25.6)
- **protocols:** iSCSI, FC, NVMe-TCP
- **capabilities:** `connect`, `deploy_plugin`, `host_register`, `connectivity`,
  `provision_volume`, `snapshot`, `clone`, `resize`, `health`, `remove`

## What this connector does (and how it differs from the stock SR)

XCP-ng's stock Everpure path is the **`lvmoiscsi`** Storage Repository: many VDIs are
carved as LVM logical volumes out of **one big shared LUN**. That model hides the
array — you lose per-VDI data reduction visibility, per-VDI array snapshots,
per-VDI replication and per-VDI QoS.

This connector instead deploys a **custom XCP-ng SMAPIv3 storage plugin** —
**`org.xen.xapi.storage.purefa`**, SR type **`purefa`** — modeled on the Everpure
CSI / Cinder / Proxmox "storage plugin" philosophy:

> **Each VDI is its own FlashArray volume**, provisioned on the array and
> presented **directly to the VM** as a multipathed raw block device
> (`/dev/mapper/3624a9370…`), one LUN per VDI. Snapshots and clones are
> **array-native** (FlashArray volume snapshot + volume copy), **not** VHD or LVM
> snapshots.

This gives every VDI the array's full data services at single-disk granularity.

> **Why SMAPIv3, not SMAPIv1?** XCP-ng 8.3 does **not** load dropped-in SMAPIv1
> drivers under `/opt/xensource/sm`, so a SMAPIv1 `PureSR.py` driver is dead
> weight there. Deployment is therefore exclusively the SMAPIv3 plugin below.

### The SMAPIv3 `org.xen.xapi.storage.purefa` plugin

Shipped as a static directory tree in this package under
`backend/phif/connectors/xcpng/files/smapiv3/` and pushed under each pool host's
`xapi-storage-script` tree at deploy time. Three cooperating pieces:

1. **Volume plugin** (`org.xen.xapi.storage.purefa/`) — installed under
   `/usr/libexec/xapi-storage-script/volume/`; `link.sh` creates the per-method
   entrypoint symlinks (`SR.create`, `Volume.create`, …).
   - `purefa_fa.py` — a stdlib-only FlashArray **REST v2 client** (token login +
     version negotiation) plus SR-state persistence and the per-VDI / per-vgroup
     volume operations.
   - `sr.py` — the `SR.*` methods (`probe`/`create`/`attach`/`detach`/`stat`/
     `ls`/`destroy`). The SR has no shared backing store (per-VDI model); create
     just validates the array is reachable and persists the connection config.
   - `volume.py` — the `Volume.*` methods (`create`/`destroy`/`stat`/`snapshot`/
     `clone`/`resize`/…). `create` makes a FlashArray volume + connects it to the
     SR's host group; `snapshot`/`clone` are array-native; `destroy` disconnects
     then destroys/eradicates and flushes the stale multipath map pool-wide.
2. **Datapath plugin** (`datapath/purefa/`) — installed under
   `/usr/libexec/xapi-storage-script/datapath/`, named by the URI **scheme**
   (`purefa`). The Volume plugin emits a `purefa://` URI; this datapath hands the
   raw multipath device to the guest (the stock tapdisk/qdisk datapaths only
   serve VHD/qcow files).
3. **XAPI host plugin** (`hostplugin/purefa-mpath`) — installed at
   `/etc/xapi.d/plugins/`. `Volume.destroy` calls it via `xe host-call-plugin`
   to flush a deleted volume's stale dm-multipath map on **every** pool host
   (with `find_multipaths=no` every host auto-maps a connected LUN, so a deleted
   one would otherwise linger and wedge multipathd).

The REST client **normalizes a bare host/IP endpoint to an absolute `https://`
URL** (a bare IP otherwise fails as "URL must be absolute") and **negotiates a
concrete REST version via `GET /api/api_version`** (highest supported, falling
back to `2.0`) rather than baking a literal `/api/2.x/` segment. Endpoint + token
+ transport + host group come from the SR `device-config`. `SR.ls` enumerates
the array's volumes via `list_volumes`, which **skips `destroyed` (soft-deleted /
pending-eradication) volumes** so a VDI removed on the array does not linger in
the SR.

### Per-VM FlashArray volume groups (multi-disk consistency)

**Newly-created** VDIs are created as members of a per-VM FlashArray **volume
group**, so a multi-disk VM's disks can be snapshotted **consistently** as a
group. The member volume name is **`<vg>/<vol>`** (the `/` is URL-encoded as
`%2F` in REST paths); the vgroup is the SR-scoped grouping the plugin already
derives (the SMAPIv3 Volume interface does not hand the plugin a VM uuid at
create time). The FlashArray REST calls used:

| Operation | REST call |
|---|---|
| Create vgroup (idempotent — already-exists = success) | `POST volume-groups?names=<vg>` |
| Create member volume (vgroup must exist first) | `POST volumes?names=<vg>%2F<vol>` `{provisioned:<bytes>}` |
| Name-based ops (resize / destroy / connect / get / snapshot) | use the full `<vg>/<vol>` name (`%2F`-encoded) |
| Group-consistent snapshot | `POST volume-group-snapshots?source_names=<vg>&suffix=<suffix>` |
| Destroy empty vgroup on last-member delete | `PATCH volume-groups?names=<vg>` `{destroyed:true}` then `DELETE` (eradicate) |

The vgroup teardown is **idempotent / defensive**: it only fires once the
group's last live member is gone, and a missing group or failed PATCH/DELETE is
swallowed.

**Additive safety — existing standalone VDIs are untouched.** A stored volume
name **without** a `<vg>/` prefix (a bare `<sr_id>-<vdi_uuid>` — a pre-existing
standalone VDI) follows the **original** code path exactly (`obj_name` returns it
verbatim, no vgroup is created or torn down). The vgroup behavior is engaged
**only** for newly-created volumes.

The **dm-multipath device is keyed off the volume `serial`**, which is
**unchanged** by vgroup membership — so the host-side device path
(`/dev/mapper/3624a9370<serial>`) is unaffected; only the FlashArray object name
changes.

### Device-path resolution (WWID vs NVMe namespace)

`purefa_fa.device_path(serial, protocol)` resolves the device-mapper node from
the FlashArray volume `serial`, branching by transport (matching the Proxmox
plugin's `_scsi_wwid` / `_device_path`):

- **iSCSI / FC (SCSI transports):** the multipath WWID is the NAA IEEE
  Registered Extended (type 6) name = `"3"` + the Everpure OUI `"624a9370"` +
  `lc(serial)`. The REST `serial` is the 24-hex tail **without** the OUI, so the
  driver prepends it (tolerating a serial that already carries `624a937`).
  Example: serial `0123456789ABCDEF0BB82813` →
  `/dev/mapper/3624a93700123456789abcdef0bb82813`.
- **NVMe-TCP / NVMe-FC:** the device is the NVMe namespace globally-unique id
  (EUI/NGUID) at `/dev/mapper/eui.<serial>` — **not** the SCSI WWID.

## Cluster / pool (multi-node) support

XCP-ng hosts are managed as a **pool**. This connector is pool-aware: it
enumerates the pool's member hosts and fans node-specific work out across every
host, while pool-wide work (the SR itself) runs once on the master.

### Node enumeration — `list_nodes()`

SSH to the pool master and list the pool's hosts with
`xe host-list params=uuid,name-label,address --minimal`. Each record
(`uuid,name-label,address`) becomes a `ClusterNode(name, host=address)` whose
`host` is the management address we drive `xe` / host-side commands against.

- **Mock / dry-run:** `xe` is not executed, so a **synthetic 2-host pool** rooted
  at the configured pool master is returned, keeping the multi-node flows
  exercisable without a real pool.
- **Fallback:** on a real run that yields no parseable hosts, a **single node**
  for the pool master is returned, so single-host behavior stays correct.

### Which operations fan out (and which run once)

| Operation | Scope | Behavior |
|---|---|---|
| `deploy` | plugin **per host**, `sr-create` **once** | The SMAPIv3 plugin (`org.xen.xapi.storage.purefa` + the `purefa` datapath + the `purefa-mpath` host plugin) must exist on **every** pool host (any host may plug the PBD), so it is pushed to each node and (re)enumerated. `xe sr-create` is a **single** call on the master — the SR is pool-wide (`shared=true`). |
| `register_hosts` | **per host** | One FlashArray host is registered **per pool node**, each carrying that node's own discovered initiators, all added to the **one** shared host group. When the operator passes **explicit** initiators for the active protocol, a single `<host_group>-pool` array host is registered with them instead (one operator-typed initiator set can't be split across nodes). A one-node pool also uses the single `<host_group>-pool` name. |
| `setup_connectivity` | **per host** | The multipath drop-in + transport login/rescan run on **each** pool host (every dom0 mounts the PBD locally). Array-side portal/target discovery runs once; the SR `device-config` binding persistence (`xe sr-param-set`) is a single master call. |

Single-host behavior is preserved: when `list_nodes()` returns one node, each of
the above collapses to the original single-host flow.

### Cluster validation — `validate_cluster()`

Confirms the pool is uniformly configured for storage connectivity before
binding a path pool-wide. For the active protocol it discovers the
protocol-relevant interfaces on **each** node and compares them with the shared
`compare_node_interfaces` helper:

| Protocol | Interface kind compared |
|---|---|
| `iscsi` | `nics` |
| `nvme-tcp` | `nvme_sources` |
| `fc` | `fc_hbas` |

For iSCSI (kind `nics`) the comparison is restricted to NICs **on the array's
storage subnet** — each node's discovered NICs are filtered with the shared
`nics_on_array_subnets(nics, portals)` helper (portals from
`ctx.array.get_data_interfaces`) before comparing, so only the storage-path NICs
are compared rather than every VM tap / bridge / VLAN (which legitimately differ
between nodes and would otherwise cause spurious "inconsistent" results). With no
associated array (or when nothing matches) the NICs are compared unfiltered.
`fc_hbas` / `nvme_sources` are compared as-is.

Returns `OpResult.ok` with per-node detail when every node exposes the same
interface set (a single-node pool always validates), else `OpResult.fail` naming
the divergence. Mock/dry-run uses the runner's synthetic per-host inventory.

### Deployment wizard — `wizard_steps()`

The wizard runs `["deploy", "register_hosts", "setup_connectivity"]`. There is
**no separate `configure` step** for xcpng (the base default's trailing
`configure` is dropped): SR creation happens **inside `deploy`** via
`xe sr-create`. Every wizard step maps to a real action id on this connector.

## Connecting (target schema)

| Field | Type | Notes |
|---|---|---|
| `pool_master_host` | string | XCP-ng pool master we SSH to to drive `xe`. |
| `ssh_user` | string | default `root`. |
| `ssh_password` | secret | provide this **or** `ssh_key`. |
| `ssh_key` | text/secret | PEM private key. |
| `protocol` | enum | `iscsi` (default), `nvme-tcp`, `fc`. |
| `sr_name` | string | SR name-label, default `purefa`. |
| `host_group` | string | FlashArray host group for this pool's hosts. |
| `host_wwns` | string | FC HBA port WWNs (comma-separated) for the pool hosts. Used when `protocol=fc`. **Optional — auto-discovered from the pool master if left blank** (see below). |

`validate_connection` runs `xe host-list` over SSH (mock-safe) and, if a
FlashArray is associated, `ctx.array.info()`.

## Initiator auto-discovery

Operators no longer need to type host initiators. When `register_hosts` is run
without explicit `iqns` / `nqns` / `wwns` (and without a `host_wwns` target
value), the connector calls the shared
`ctx.runner.discover_initiators(pool_master_host, username=…, password=…, key=…)`
helper, which reads the standard locations over SSH:

- iSCSI IQN → `/etc/iscsi/initiatorname.iscsi`
- NVMe NQN → `/etc/nvme/hostnqn`
- FC WWNs → `/sys/class/fc_host/*/port_name` (`0x` prefixes stripped)

The connector picks only the protocol-relevant ID(s): `iscsi → iqn`,
`nvme-tcp → nqn`, `fc → wwns`. **Explicit values always override discovery.**
The helper is mock-safe (returns synthetic IDs in `PHIF_MOCK_MODE`/dry-run), and
discovery is skipped entirely when the required ID is already supplied.

### Showing discovered initiators in the UI

The connector also implements `discover_options("initiators")`, which returns
the pool master's IQN / NQN / FC WWNs **tagged with the matching `register_hosts`
form field** so the UI can display / pre-fill them:

| Discovered | `field` | Used for |
|---|---|---|
| iSCSI IQN | `iqns` | iSCSI host registration |
| NVMe NQN | `nqns` | NVMe-TCP host registration |
| FC WWNs (comma-separated) | `wwns` | FC host registration |

Each option is `{"field": …, "value": …, "label": …}`. This is the same shared
`ctx.runner.discover_initiators` helper used by auto-discovery, so it is
mock-safe.

## Endpoint + token come from the array (not operator-entered)

The FlashArray **endpoint** and **API token** the SMAPIv3 plugin uses are taken
from the array associated with this hypervisor — `ctx.array.endpoint` and
`ctx.resolve_token()` (the array's original token, reused rather than minting a
new one where possible). They are **not** operator-entered form fields: the
`deploy` action exposes no `endpoint` / `token` inputs. The `deploy` method
still accepts an optional `endpoint=` kwarg as an override for advanced use.

## Fibre Channel (FC) support

The connector implements a true FC data path end to end — array-side **and**
host-side via the SMAPIv3 plugin — distinct from the IP transports:

- **SAN zoning is a prerequisite.** Unlike iSCSI/NVMe-TCP there is **no IP login
  step**. The pool hosts' FC HBA port WWNs must already be zoned to the
  FlashArray's FC target ports on the switch fabric (done switch-side, outside
  this connector). The connector assumes zoning is in place.
- **`register_hosts` (protocol=fc):** registers the pool hosts on the array **by
  WWN** (`ctx.array.create_host(name, wwns=[…])`) — never by IQN/NQN. WWNs come
  from the `host_wwns` target field or the `wwns` action field; if neither is
  given, they are **auto-discovered** via the shared
  `ctx.runner.discover_initiators` helper (which reads
  `/sys/class/fc_host/*/port_name`, `0x…` prefixes stripped). `create_host` /
  `create_host_group` are idempotent + additive, so re-running registration is
  safe. A host group is then created.
- **`setup_connectivity` (protocol=fc):** the common pool-multipathing +
  Everpure ALUA stanza (`/etc/multipath/conf.d/pure.conf`) is applied on **every
  pool host**. For FC it then runs an **FC/SCSI rescan only** — no `iscsiadm`/`nvme`
  login. It tries `rescan-scsi-bus.sh -a`, falling back to `issue_lip` on each
  `/sys/class/fc_host/host*/issue_lip` plus a SCSI host scan
  (`echo '- - -' > /sys/class/scsi_host/host*/scan`), then `multipath -r`.
- **`deploy` (protocol=fc):** `xe sr-create type=purefa …` passes
  `device-config:protocol=fc`, which the SMAPIv3 plugin reads (and persists in
  the SR state so `Volume.*` can reach the array).
- **`provision` over FC:** create the FlashArray volume → connect it to the FC
  **host group** → host-side FC rescan → the VDI resolves to the multipath
  device `/dev/mapper/3624a9370<serial>` (WWID = Everpure OUI prefix + volume
  serial) and is handed to the VM as a raw block device.

### SMAPIv3 plugin FC device logic

- `device-config` carries `protocol` (`iscsi | fc | nvme-tcp`), persisted in the
  SR state by `SR.create`/`SR.attach`.
- The host-side FC rescan (`rescan-scsi-bus.sh`/`issue_lip` + sysfs scan, **no**
  iSCSI/NVMe login) is driven by `setup_connectivity`; the `purefa` datapath
  plugin then resolves the device **by the volume's SCSI serial (WWID)** at
  `/dev/mapper/3624a9370<serial>` for all SCSI transports.
- `Volume.destroy` flushes the multipath map pool-wide (via the `purefa-mpath`
  host plugin); `Volume.resize` extends the FA volume.

## Array portal + target discovery (connectivity)

`setup_connectivity` no longer requires the operator to know the array's portal
IPs or target identifiers. For the IP transports, when they are left blank it
discovers them from the associated FlashArray:

- **Portals** — `ctx.array.get_data_interfaces(service)` where `service` is
  `nvme-tcp` for the NVMe-TCP protocol, else `iscsi`. The returned data-network
  IPs are the portals the host logs in to.
- **iSCSI target IQN** — `ctx.array.get_target_ports()["iqn"]` when `target_iqn`
  is blank.
- **NVMe subsystem NQN** — `ctx.array.get_target_ports()["nqn"]` when
  `subsystem_nqn` is blank.

What was discovered is emitted to the job log. Explicitly-supplied portals /
target IDs always win and skip discovery. **Fibre Channel has no portals** (the
fabric handles reachability via zoning), so no portal/target discovery runs for
`protocol=fc`.

### Fail-fast for IP transports with no portals

If the protocol is `iscsi` or `nvme-tcp` and **no portals** can be found on the
array (and we are not in dry-run), `setup_connectivity` returns a clear
`OpResult.fail` explaining that the array exposes no portals for that transport
(it may be Fibre Channel / NVMe-FC only) and advising the operator to configure
that transport on the array, **switch the hypervisor protocol to `fc`**, or pass
portals explicitly — instead of running `iscsiadm` / `nvme` and failing
cryptically. iSCSI additionally fails fast when no target IQN can be found.

## Interface binding

`setup_connectivity` can optionally **pin the host-side storage path to specific
local interfaces / HBAs**, so a freshly-connected LUN is reached only over the
intended transport path. The relevant fields are **optional** and apply **only
to the active protocol** (selections for the other protocols are ignored):

| Field | Type | Protocol | Notes |
|---|---|---|---|
| `iscsi_nics` | multiselect (`options_source="nics"`) | iSCSI | Local NICs to bind the iSCSI ifaces to. |
| `nvme_sources` | multiselect (`options_source="nvme_sources"`) | NVMe-TCP | Local source addresses (`host-traddr`). |
| `nvme_options` | string | NVMe-TCP | Extra `nvme connect` options to persist (e.g. `ctrl-loss-tmo=600`). |
| `fc_hbas` | multiselect (`options_source="fc_hbas"`) | FC | HBA WWPNs to pin (zoning is external/switch-side). |

### Discovering choices

The multiselects are populated dynamically. The connector implements
`discover_options(kind)`, which delegates to the shared
`ctx.runner.discover_interfaces(pool_master_host, kind, username=…, password=…, key=…)`
helper for `kind` in `nics` / `nvme_sources` / `fc_hbas`. It reads, over SSH on
the pool master:

- `nics` → `ip -o -4 addr show` (IP-bearing Ethernet NICs, each with `address`
  + `cidr`; `lo` excluded).
- `nvme_sources` → `ip -o -4 addr show` (IP-bearing interfaces, as host-traddr).
- `fc_hbas` → `/sys/class/fc_host/*` (`port_name`, `port_state`, `speed`).

The helper is **mock-safe** (returns synthetic entries in
`PHIF_MOCK_MODE`/dry-run), so the dropdowns are exercisable without a real host.

#### Storage-subnet NIC filtering (iSCSI)

For the `nics` kind, when a FlashArray is associated the discovered NICs are
**filtered to the array's storage subnet** before being offered, using the shared
`nics_on_array_subnets(nics, portals)` helper. Portals come from
`ctx.array.get_data_interfaces(service)` where `service` is `nvme-tcp` for the
NVMe-TCP protocol, else `iscsi`. A NIC is kept only when its `cidr` network
contains one of the array's storage portal IPs — so mgmt NICs, VM taps/bridges,
and unrelated VLANs are dropped and the operator only picks from real storage
NICs. If nothing matches (or no portals / no array), the NICs are returned
unfiltered so selection still works. `nvme_sources` / `fc_hbas` are **not**
filtered. (Mirrors the Proxmox connector's `discover_options("nics")` branch.)

### How the binding is applied (per active protocol, over SSH)

- **iSCSI:** an `iscsiadm` iface (`pure-<nic>`) is created and bound to each
  selected NIC (`iface.net_ifacename`), and discovery + login are scoped to
  those ifaces (`-I pure-<nic>`). The selected NICs are also persisted into the
  SR `device-config` so the driver can keep the path pinned on attach.
  `TODO(doc-validate)`: the exact XCP-ng iface-binding mechanism (iscsiadm
  `iface` vs. binding via a local IP in the PBD/SR `device-config`).
- **NVMe-TCP:** `nvme connect-all` is issued once per selected source with
  `-w <host-traddr>`, and any extra `nvme_options` are appended and persisted.
- **FC:** there is no host-side login to bind; the selected HBA WWPNs are
  persisted into the SR `device-config` (zoning remains switch-side).

### Persistence into the SR

When a binding is selected, it is written into the `purefa` SR via
`xe sr-param-set uuid=<SR> device-config:iscsi_nics=… device-config:nvme_sources=…
device-config:nvme_options=… device-config:fc_hbas=…`. The host-side binding
(per-NIC iscsiadm ifaces / `nvme connect -w` sources / pinned FC HBAs) is applied
by `setup_connectivity` itself; persisting the selection into the SR
`device-config` keeps it recorded alongside the SR.

All binding work respects `ctx.dry_run` (planned, no commands run) and is
mock-safe.

## Day-2 actions

| Action id | Capability | What it does |
|---|---|---|
| `deploy` | `deploy_plugin` | Push the `org.xen.xapi.storage.purefa` SMAPIv3 plugin (volume + datapath + host plugin) to **every** pool host (via `list_nodes()`), (re)enumerate it, and `xe sr-create type=purefa name-label=… device-config:endpoint=… device-config:token=…` as a **single** call on the master (the SR is pool-wide). The **endpoint + token come from the associated array** (`ctx.array.endpoint` + `ctx.resolve_token()`), not from form fields; the array's existing token is reused, falling back to minting a scoped one only if none is available. |
| `register_hosts` | `host_register` | Register one FlashArray host **per pool node** (each carrying that node's own IQN/NQN/WWN), all in the **one** shared host group. Per-node initiators are **auto-discovered** via `ctx.runner.discover_initiators`; explicit initiators register a single `<host_group>-pool` host and override discovery. Idempotent (safe to re-run). See [Cluster / pool (multi-node) support](#cluster--pool-multi-node-support). |
| `setup_connectivity` | `connectivity` | On **every pool host**: enable pool multipathing (`other-config:multipathing=true`), install the Everpure ALUA multipath stanza in `/etc/multipath/conf.d/pure.conf`, and connect the iSCSI / NVMe-TCP transport. **Portals + target IQN/NQN are auto-discovered from the array** when blank (see [Array portal + target discovery](#array-portal--target-discovery-connectivity)); fails fast if an IP transport has no portals. Optionally **pins the path to selected NICs / NVMe sources / FC HBAs** (see [Interface binding](#interface-binding)). The SR `device-config` binding is persisted once on the master. |
| `provision` | `provision_volume` | Create a VDI backed by a **new dedicated FlashArray volume** (a per-VM **vgroup member**) mapped directly to the VM. |
| `snapshot` | `snapshot` | Array snapshot via the SMAPIv3 plugin (`xe vdi-snapshot` / `ctx.array.create_snapshot`). |
| `clone` | `clone` | Array volume copy via the SMAPIv3 plugin (`xe vdi-clone` / `ctx.array.clone_volume`). |
| `resize` | `resize` | FlashArray extend + `xe vdi-resize`. |
| `health_check` | `health` | `xe sr-list type=purefa` + `multipath -ll`. |
| `teardown` | `remove` | `xe pbd-unplug` / `xe sr-forget` + remove the SMAPIv3 plugin (volume + datapath + host plugin) from every pool host. |

Standard action ids are used, so the base `dispatch` routes them with no
override.

## Multipath / connectivity reference (verified)

Per the Everpure XCP-ng best-practices docs (Last Updated May 19 2026):

- XCP-ng 8.3, CentOS/RHEL-based dom0 (`yum`), XAPI + SMAPI, managed via Xen
  Orchestra or `xe`. `iscsi-initiator-utils` and `device-mapper-multipath` are
  pre-installed.
- Custom multipath config goes in `/etc/multipath/conf.d/custom.conf` (persists
  across updates). **Never** edit `/etc/multipath.xenserver/multipath.conf`.
- Everpure ALUA device stanza: `vendor "PURE"`, `product "FlashArray"`,
  `path_grouping_policy group_by_prio`, `prio alua`,
  `hardware_handler "1 alua"`, `failback immediate`, `no_path_retry 0`, plus a
  `defaults { find_multipaths yes }` block so multipathd groups the multiple
  portal/HBA paths to one wwid. `setup_connectivity` writes this via the
  `_write_multipath_conf` helper (analogous to the Proxmox connector) to
  `/etc/multipath/conf.d/pure.conf` on **every pool host** — **not**
  `/etc/multipath.conf`. (A PHIF-owned `pure.conf` drop-in is used rather than
  `custom.conf` so an operator's own `custom.conf` is never clobbered.)
- Enable multipathing pool-wide: `xe host-param-set uuid=<HOST>
  other-config:multipathing=true` and `…:multipathhandle=dmp`.

Doc sources:
- iSCSI on XCP-ng – CLI Quick Start: <https://support.purestorage.com/bundle/m_linux/page/Solutions/Linux/topics/t_xcpng_iscsi_quickstart.html>
- XCP-ng-Specific Considerations: <https://support.purestorage.com/bundle/m_linux/page/Solutions/Linux/topics/c_xcpng_iscsi_best-practices_xcp-ng-specific_considerations.html>
- Multipath Configuration: <https://support.purestorage.com/bundle/m_linux/page/Solutions/Linux/topics/c_xcpng_iscsi_best-practices_multipath_configuration.html>

## `TODO(doc-validate)` items

Everpure publishes guidance **only** for the stock `lvmoiscsi` shared SR. There is
**no Everpure-published custom SMAPI driver**, so the per-VDI `purefa` SMAPIv3 design
is ours. The core SMAPIv3 path (volume + datapath + host plugin, plus the per-VM
vgroup behavior) is **validated on a live 2-host XCP-ng 8.3 pool (xapi 25.6)**;
the items below are remaining points to reconfirm on other Purity//FA REST
versions / dom0 builds before broad production rollout:

1. **FlashArray REST version + auth flow** (`POST /api/<ver>/login` with
   `api-token` → `x-auth-token`). The plugin negotiates the version via
   `GET /api/api_version` (highest supported, fallback `2.0`) and normalizes a
   bare endpoint to `https://`; confirm the `api_version`/login response shapes
   against the target array's Purity//FA REST version.
2. **Volume-group REST contracts** — the per-VM vgroup flow uses
   `POST volume-groups?names=<vg>` (idempotent), member volumes
   `POST volumes?names=<vg>%2F<vol>`, group-consistent snapshots
   `POST volume-group-snapshots?source_names=<vg>&suffix=<suffix>`, and empty-group
   teardown `PATCH volume-groups?names=<vg>{destroyed:true}` + `DELETE`. Confirm
   the `%2F` member-name encoding and the already-exists/empty-group response
   shapes on the target Purity//FA version.
3. **SCSI/multipath device naming** — for iSCSI/FC the plugin builds the WWID as
   `"3" + "624a9370" + lc(serial)` (e.g. `/dev/mapper/3624a937054f3f...`); for
   NVMe it uses the namespace EUI/NGUID at `/dev/mapper/eui.<serial>`. This is
   keyed off the volume **serial** and is **unaffected by vgroup membership**.
   Confirm the WWID prefix, serial casing, and the NVMe `eui.`/`nguid.` by-id
   naming emitted by the target dom0 kernel/multipath.
4. **Pool-wide plugin distribution mechanism** — `deploy` pushes the SMAPIv3
   plugin (volume + datapath + host plugin) to **every** pool host enumerated by
   `list_nodes()` (one heredoc write per file per dom0). Confirm the preferred
   production mechanism — a supplemental pack vs. per-dom0 rsync — and that the
   per-host heredoc install + `link.sh` enumeration lands correctly on each host.
5. **Pool host enumeration + per-host initiator discovery** — `list_nodes()`
   parses `xe host-list params=uuid,name-label,address --minimal` to enumerate
   pool hosts, and `register_hosts` now discovers each host's own initiators and
   registers one FlashArray host **per node** in the shared host group. Confirm
   the `xe host-list` `--minimal` record format (field order / separators) and
   that each member host's management `address` is SSH-reachable from PHIF;
   confirm `validate_cluster`'s per-host interface comparison matches the real
   per-node inventory.
6. **Initiator source-of-truth on dom0** — confirm `/etc/iscsi/initiatorname.iscsi`,
   `/etc/nvme/hostnqn`, and `/sys/class/fc_host/*/port_name` (`0x` prefix, casing)
   layouts on the target XCP-ng dom0, which the shared `discover_initiators`
   helper relies on.
7. **FC rescan tooling** — whether `rescan-scsi-bus.sh` (sg3_utils) ships in
   dom0; the `issue_lip` + `scsi_host` sysfs-scan fallback is always available.
8. **Interface-binding mechanism** — the exact XCP-ng iSCSI iface-binding
    approach (iscsiadm `iface` vs. binding via a local IP in the PBD/SR
    `device-config`) and the precise `nvme connect -w <host-traddr>` form on the
    target nvme-cli (`iscsi_nics` / `nvme_sources` / `nvme_options` / `fc_hbas`
    are persisted into the SR `device-config`).
9. **Storage-subnet NIC filtering** — `discover_options("nics")` and
    `validate_cluster` filter NICs to the array's storage subnet via
    `nics_on_array_subnets`, matching each node NIC's `cidr` (from
    `ip -o -4 addr show`) against the array portal IPs from
    `ctx.array.get_data_interfaces`. Confirm dom0's `ip -o -4 addr show` reports
    the storage-NIC CIDR as expected and that the array's data-interface IPs land
    on the same subnet as the host storage NICs (single-subnet vs. multi-subnet
    iSCSI/NVMe-TCP topologies); the helper falls back to unfiltered when nothing
    matches.

## Testing

```bash
cd backend
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest tests/test_xcpng.py -q
```

All tests run against the mock FlashArray + mock SSH runner; nothing touches a
real array or host.


## Manual installation (without PHIF)

This procedure installs the custom Everpure `purefa` SMAPIv3 storage plugin by hand on an XCP-ng 8.3 pool, mirroring exactly what PHIF's `deploy` action (`deploy_integration` / `_install_smapiv3_plugin`) does. XCP-ng 8.3 does **not** load dropped-in SMAPIv1 `/opt/xensource/sm` drivers, so this is an SMAPIv3-only install. Run all `xe` commands from the **pool master**; the plugin files must be present on **every pool host**.

> Source files: `backend/phif/connectors/xcpng/files/smapiv3/` — the volume plugin `org.xen.xapi.storage.purefa/`, the datapath plugin `datapath/purefa/`, and the host plugin `hostplugin/purefa-mpath`.

### 0. Prerequisites

- An XCP-ng 8.3 pool (validated on xapi 25.6). dom0 ships Python 3 + the iscsi/multipath/nvme/sg3_utils tools and the `xapi.storage` / `XenAPIPlugin` libraries. The plugin's `purefa_fa.py` uses only the Python standard library — **no pip packages required**.
- A FlashArray reachable over its **management endpoint** (HTTPS 443) **from every pool host** (the plugin runs on each dom0). PHIF preflights this with a `/dev/tcp/<host>/443` probe.
- A FlashArray **API token** scoped to manage volumes/host groups.

### 1. Install the three plugin components on EVERY pool host

**a) Volume plugin → `/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.purefa/`** (files: `purefa_fa.py`, `plugin.py`, `sr.py`, `volume.py`, `link.sh`):

```sh
mkdir -p /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.purefa
# copy purefa_fa.py plugin.py sr.py volume.py link.sh into that dir, then:
sh /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.purefa/link.sh
```

`link.sh` chmod +x's the scripts and creates the SMAPIv3 per-method entrypoint **symlinks** (`Plugin.Query`/`diagnostics`→plugin.py; `SR.*`→sr.py; `Volume.*`→volume.py).

**b) Datapath plugin → `/usr/libexec/xapi-storage-script/datapath/purefa/`** (files: `plugin.py`, `datapath.py`, `link.sh`) — presents the FA volume to the guest as a raw multipath block device via Blkback:

```sh
mkdir -p /usr/libexec/xapi-storage-script/datapath/purefa
# copy plugin.py datapath.py link.sh into that dir, then:
sh /usr/libexec/xapi-storage-script/datapath/purefa/link.sh
```

**c) XAPI host plugin → `/etc/xapi.d/plugins/purefa-mpath`** (pool-wide multipath flush/rescan, called via `xe host-call-plugin`):

```sh
mkdir -p /etc/xapi.d/plugins
# copy purefa-mpath to /etc/xapi.d/plugins/purefa-mpath, then:
chmod +x /etc/xapi.d/plugins/purefa-mpath
```

**d) Re-enumerate the storage-script daemon on that host:**

```sh
systemctl restart xapi-storage-script.service || systemctl restart xapi-storage-script || true
```

### 2. Per-host vs master-only

| Item | Where |
|---|---|
| Volume + Datapath plugin dirs (+ `link.sh`), `/etc/xapi.d/plugins/purefa-mpath` | **Every pool host** |
| `xapi-storage-script` restart, toolstack restart, multipath + transport login | **Every pool host** |
| `xe sm-list` verification, `xe sr-create`, PBD plug | **Pool master only** |

### 3. Restart the toolstack on every pool host

XAPI enumerates SM types at toolstack start:

```sh
xe-toolstack-restart </dev/null >/tmp/phif-tsr.out 2>&1; rc=$?; cat /tmp/phif-tsr.out; exit $rc
```

Wait for xapi to answer (`xe host-list --minimal` returns a UUID) before continuing.

### 4. Verify the plugin registered (pool master)

```sh
for i in $(seq 1 18); do
  xe sm-list params=type --minimal 2>/dev/null | tr ',' '\n' | grep -qx purefa && { echo REGISTERED; break; }
  sleep 5
done
xe sm-list params=type --minimal
# if it doesn't register, run the plugin query directly to surface the error:
/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.purefa/Plugin.Query phif </dev/null
```

### 5. FlashArray-side prerequisites + host connectivity (every pool host)

**a) Register the pool hosts on the array (host group by initiator).** Create one FlashArray host per pool node carrying that node's initiator (iSCSI IQN `cat /etc/iscsi/initiatorname.iscsi`, NVMe NQN `cat /etc/nvme/hostnqn`, or FC HBA WWNs), and add all hosts to one shared **host group**. `xe sr-create` binds to this group; deploy preflights the host objects exist.

**b) Enable pool multipathing + the Everpure ALUA stanza** (the `host-param-set` toggle is pool-wide/idempotent):

```sh
systemctl enable --now multipathd
for h in $(xe host-list --minimal | tr ',' ' '); do
  xe host-param-set uuid=$h other-config:multipathing=true
  xe host-param-set uuid=$h other-config:multipathhandle=dmp
done
mkdir -p /etc/multipath/conf.d
```

Write `/etc/multipath/conf.d/pure.conf` (never edit `/etc/multipath.xenserver/multipath.conf`):

```
defaults {
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
    failback immediate
    rr_weight uniform
    no_path_retry 0
  }
}
```

```sh
systemctl restart multipathd
```

**c) Connect the transport (per host).**

- **iSCSI** (port 3260):
  ```sh
  systemctl enable --now iscsid
  # optional NIC binding: per selected NIC create a pure-<nic> iface
  #   iscsiadm -m iface -I pure-<nic> -o new
  #   iscsiadm -m iface -I pure-<nic> -o update -n iface.net_ifacename -v <nic>
  iscsiadm -m discovery -t sendtargets -p <portal>:3260 [-I pure-<nic> ...]
  iscsiadm -m node [-T <target_iqn>] [-I pure-<nic> ...] --login
  ```
  When binding multiple NICs on a shared subnet, PHIF also applies the shared **ARP-flux** fix (`iscsi_net.arp_flux_cmd`) on the selected NICs — `net.ipv4.conf.<nic>.arp_ignore = 2` and `arp_announce = 2` (persisted to `/etc/sysctl.d/99-phif-iscsi-arp.conf` + live `sysctl -w`) — so dual-NIC iSCSI paths don't mis-bind.

- **NVMe-TCP** (port 4420), per portal/source:
  ```sh
  nvme connect-all -t tcp -a <portal> -s 4420 [-n <subsystem_nqn>] [-w <host-traddr>]
  ```

- **Fibre Channel:** no IP login — SAN zoning is a prerequisite (the hosts' FC HBA WWNs must already be zoned to the array's FC target ports). Only rescan:
  ```sh
  rescan-scsi-bus.sh -a 2>/dev/null || {
    for h in /sys/class/fc_host/host*/issue_lip; do echo 1 > $h 2>/dev/null || true; done
    for s in /sys/class/scsi_host/host*/scan; do echo '- - -' > $s 2>/dev/null || true; done; }
  multipath -r
  ```

### 6. Create the SR (pool master only)

`shared=true` creates+plugs a PBD on every host. `device-config:` keys: `endpoint`, `token`, `protocol`, `hostgroup`, `eradicate`:

```sh
timeout 240 xe sr-create type=purefa name-label='purefa' \
  shared=true content-type=user \
  device-config:endpoint='<ARRAY_MGMT_ENDPOINT>' \
  device-config:token='<FA_API_TOKEN>' \
  device-config:protocol=<iscsi|nvme-tcp|fc> \
  device-config:hostgroup='<HOST_GROUP>' \
  device-config:eradicate=false
```

- `endpoint` — FlashArray management endpoint (IP/host/`host:port`/URL; default port 443).
- `token` — FlashArray API token.
- `protocol` — `iscsi` | `nvme-tcp` | `fc`.
- `hostgroup` — the host group from step 5a.
- `eradicate` — `true` = hard-delete a volume on VDI delete; `false` = recoverable for 24h (PHIF default).

Optional interface-binding keys (re-read by the plugin on next `Volume.attach`): `device-config:iscsi_nics=`, `nvme_sources=`, `nvme_options=`, `fc_hbas=`.

Then ensure every host's PBD is plugged:

```sh
SR_UUID=$(xe sr-list name-label='purefa' --minimal)
for p in $(xe pbd-list sr-uuid=$SR_UUID params=uuid --minimal | tr ',' ' '); do xe pbd-plug uuid=$p || true; done
```

Plugin-side errors land in `/var/log/SMlog` (grep `purefa`/`Traceback`), not `xe` stdout.

### 7. (Optional) remove the SR non-destructively (does NOT eradicate array volumes)

```sh
for p in $(xe pbd-list sr-uuid=<SR_UUID> --minimal | tr ',' ' '); do xe pbd-unplug uuid=$p; done
xe sr-forget uuid=<SR_UUID>
```

Then remove the plugin dirs/files from each host and restart `xapi-storage-script` / the toolstack.
