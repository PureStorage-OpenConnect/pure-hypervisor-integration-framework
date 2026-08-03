# Proxmox VE connector (Everpure FlashArray `purefa` storage plugin)

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../../DISCLAIMER.md`](../../DISCLAIMER.md) first.

- **Key:** `proxmox`
- **Name:** Proxmox VE (Everpure storage plugin)
- **Maturity:** `ga`
- **Protocols:** NVMe-TCP (recommended), iSCSI, FC

This connector integrates Proxmox VE with an Everpure FlashArray using a **true
custom storage plugin**, modeled on the way the **Everpure CSI driver** and the
**OpenStack Cinder Everpure driver** work — *not* on Proxmox LVM-thick and *not* on
a shared LVM pool.

## The storage model: one FlashArray volume per VM disk

The storage type is **`purefa`**, implemented by a Perl plugin
(`PureFAPlugin.pm`, a subclass of `PVE::Storage::Plugin`) installed on each node
at `/usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm`.

- **Each VM disk is its own FlashArray volume** named `vm-<vmid>-disk-<N>`,
  created on the array (one LUN per disk).
- The volume is **presented directly to the VM as a raw multipath block device**
  (e.g. `/dev/mapper/<wwid>`, attached as virtio-scsi). **There is no LVM
  layer** and no shared LVM pool — this mirrors Cinder/CSI block mode.
- **Snapshots and clones are performed on the array** (FlashArray volume
  snapshots and volume copy), not via Proxmox/LVM/qcow2.

This gives you array-native data reduction, instant snapshots/clones, and
per-disk QoS/replication, with each guest disk independently mapped.

### Why not LVM?

Proxmox's built-in shared-LUN options (LVM-thick over a shared iSCSI/FC LUN)
put a host-side LVM layer between the guest and the array, lose array-native
snapshots/clones, and serialize all VMs onto one LUN. The `purefa` plugin
instead allocates one array volume per disk and lets the array do snapshots and
copies — the same architecture as Everpure's CSI and Cinder drivers.

## Components shipped

| File | Purpose |
|---|---|
| `backend/phif/connectors/proxmox/files/PureFAPlugin.pm` | The Perl `purefa` storage plugin (static, pushed to nodes). |
| `backend/phif/connectors/proxmox/connector.py` | PHIF connector: deploy + day-2 ops. |
| `ansible/proxmox/deploy_purefa_plugin.yml` | Alternative Ansible-driven plugin deploy. |
| `ansible/proxmox/files/PureFAPlugin.pm` | Copy of the plugin for the playbook. |

## The Perl plugin (`PureFAPlugin.pm`)

Implements the `PVE::Storage::Plugin` API against the FlashArray REST v2 API
(token + endpoint from `storage.cfg`):

| Method | Behavior |
|---|---|
| `api`, `type` (`purefa`), `plugindata`, `properties`, `options` | Plugin identity + config schema. |
| `parse_volname` | Parses `vm-<vmid>-disk-<N>` / `base-...`. |
| `alloc_image` | Creates a FA volume + connects it to the host/host group. |
| `free_image` | Disconnects + destroys (optionally eradicates) the FA volume. |
| `list_images` | Lists `vm-*-disk-*` volumes from the array. |
| `status` | Reports array capacity/used from `arrays/space`. |
| `activate_storage` / `deactivate_storage` | Ensures transport + multipath services. |
| `activate_volume` / `deactivate_volume` | Branches by transport: iSCSI/NVMe do session login/discovery; **FC does NO login — it only rescans the SCSI bus and maps the volume by multipath WWID** (`/dev/mapper/3<serial>`). Then `multipath -r`. |
| `_device_path` / `_fa_volume_serial` | Resolves the multipath device by WWID from the FA volume serial: SCSI (iSCSI/FC) → `/dev/mapper/3<lowercased-serial>`; NVMe → `/dev/mapper/eui.<serial>`. |
| `volume_size_info` / `volume_resize` | FA extend + device rescan. |
| `volume_snapshot` / `_rollback` / `_delete` | FA snapshots. |
| `create_base` | Converts a VM disk into a template by renaming the FA volume `vm-…` → `base-…` in place (PATCH name). Enables **array-offloaded linked clones** — see note below. |
| `clone_image` | FA volume copy + connect (array offload). |
| `volume_has_feature` | Advertises `snapshot`, `clone`, `copy`, `sparseinit`, `template`. |

> **Array-offloaded clones require a template.** Proxmox only routes a clone
> through `clone_image` (our array-side FA volume copy) for a **linked clone**, i.e.
> cloning a *template*. A plain `qm clone` of a normal VM is a **full** clone, which
> PVE performs with a host-side `qemu-img` / drive-mirror block copy — there is no
> storage hook to offload that. To get array-offloaded clones: `qm template <vmid>`
> (→ `create_base`, instant array rename), then clone the template (→ `clone_image`,
> array-side copy, space-efficient via FlashArray dedup).

### `storage.cfg` stanza

```
purefa: purefa
        pure_endpoint   https://flasharray.example.com
        pure_api_token  <fa-api-token>
        protocol        nvme-tcp          # iscsi | fc | nvme-tcp
        host_group      pve-cluster       # FA host group the nodes belong to
        eradicate       0                 # 1 = hard-delete on free_image
        content         images,rootdir
        shared          1
```

## Connecting (target schema)

| Field | Notes |
|---|---|
| `node_host` | A Proxmox cluster member reachable over SSH. |
| `ssh_user` | Default `root`. |
| `ssh_password` *or* `ssh_key` | One credential required. |
| `protocol` | `nvme-tcp` (default), `iscsi`, `fc`. |
| `storage_id` | Proxmox storage ID (default `purefa`). |
| `host_group` | FA host group the cluster nodes belong to. |
| `node_iqn` / `node_nqn` | Initiator IDs for iSCSI / NVMe-TCP host registration. **Optional — auto-discovered from the node if left blank.** |
| `node_wwns` | FC HBA port WWPNs (comma-separated) for host registration. **Optional — auto-discovered from the node if left blank.** |

> **Initiators are auto-discovered.** You normally do **not** type initiator
> IDs. When `node_iqn` / `node_nqn` / `node_wwns` are left blank, host
> registration calls `runner.discover_initiators()` over SSH (reading
> `/etc/iscsi/initiatorname.iscsi`, `/etc/nvme/hostnqn`, and
> `/sys/class/fc_host/*/port_name`) and registers the initiator matching the
> chosen protocol. Supplying an explicit value overrides discovery.

`validate_connection` runs `pveversion` over SSH (mock-safe) and, if a
FlashArray is associated, calls `ctx.array.info()`.

## Capabilities / day-2 actions

| Action id | Capability | What it does |
|---|---|---|
| `deploy` | `DEPLOY_PLUGIN` | Pushes `PureFAPlugin.pm` to the node, installs perl REST deps, reloads `pvedaemon pveproxy pvestatd`, verifies `pvesm status`. |
| `configure` | `CONFIGURE` | Writes the `purefa` stanza into `/etc/pve/storage.cfg` (endpoint/token/protocol/host_group) and reloads the daemons. |
| `register_hosts` | `HOST_REGISTER` | Registers the node as a FA host + host group via `ctx.array`. Protocol-aware: iSCSI→IQN, NVMe-TCP→NQN, **FC→WWN(s)**. Initiators are **auto-discovered** over SSH (`runner.discover_initiators`) when not explicitly provided; explicit values override. Idempotent — safe to re-run. |
| `setup_connectivity` | `CONNECTIVITY` | Branches by protocol. iSCSI/NVMe-TCP: install transport tooling, log in to portals, multipath. **FC: NO login** — install `sg3-utils`/`multipath-tools`, issue an FC LIP + rescan the SCSI bus (`rescan-scsi-bus.sh`), then multipath. SAN zoning is a fabric prerequisite. |
| `provision` | `PROVISION_VOLUME` | Creates a FA volume (one LUN per disk), connects it to the host group, then attaches it directly to the VM (`qm set <vmid> -scsiN <storage_id>:<name>`) as a raw multipath block device. |
| `snapshot` | `SNAPSHOT` | Array snapshot of the disk's volume (`ctx.array.create_snapshot`). |
| `clone` | `CLONE` | Array volume copy (`ctx.array.clone_volume`) producing a new directly-attached volume. |
| `resize` | `RESIZE` | FA extend + device rescan + `qm resize`. |
| `health_check` | `HEALTH` | `pvesm status` + `multipath -ll` / `nvme list-subsys`. |
| `teardown` | `REMOVE` | Removes the `purefa` storage entry and the plugin file. |

All operations honor `ctx.dry_run` (validate/plan, no mutation) and stream
progress via `ctx.emit`.

## Cluster (multi-node) support

Proxmox is a clustered hypervisor: a single `storage.cfg` is shared cluster-wide
via **pmxcfs** (`/etc/pve`), but the Perl storage plugin and the host-side
transport/multipath configuration must exist on **every** node. The connector
is cluster-aware:

### `list_nodes()`

SSHes to the configured `node_host` and reads the PVE cluster membership file
**`/etc/pve/.members`** (pmxcfs), whose JSON looks like:

```json
{"nodename": "pve01",
 "nodelist": {"pve01": {"id": 1, "ip": "192.0.2.11", "online": 1},
              "pve02": {"id": 2, "ip": "192.0.2.12", "online": 1}}}
```

It returns one `ClusterNode(name, host=<ip>)` per member (each node's cluster IP
is its SSH/management host). Behavior:

- **Standalone node / parse failure / unreadable `.members`** → falls back to the
  single connection host (so non-clustered installs work unchanged).
- **Mock / dry-run** → the runner short-circuits SSH, so a synthetic **2-node**
  cluster is returned to keep the cluster flow exercisable end-to-end.

### Fan-out vs. single-call operations

| Operation | Scope | Why |
|---|---|---|
| `deploy` | **Per node** | `PureFAPlugin.pm` must exist on every node; pushes the `.pm` and reloads `pvedaemon pveproxy pvestatd` on each. |
| `register_hosts` | **Per node** | One FlashArray host per node (each from that node's own initiators), **all added to the one cluster host group**. The connection node honors operator-supplied `host_name` / `node_iqn|nqn|wwns`; other members auto-discover. |
| `setup_connectivity` | **Per node** | Transport login/rescan + the multipath drop-in are node-local; the loop runs the same per-node body against each `node.host`. The interface binding is persisted **once** (`pvesm set`) since `storage.cfg` is cluster-wide. |
| `configure` | **Once** (connection host) | `pvesm add` writes the cluster-wide `storage.cfg` via pmxcfs; running it on every node would be redundant. |
| `provision` / `snapshot` / `clone` / `resize` / `health_check` / `teardown` | Connection host | Array-side work + a single node's rescan/`qm` calls. |

When `list_nodes()` returns a single node, every fan-out collapses to the
original single-node behavior.

### `validate_cluster(**params)`

Run by the deployment wizard in cluster scope to ensure the nodes match before
binding storage connectivity. For the active protocol it discovers the
relevant interface kind on **each** node via
`runner.discover_interfaces(node.host, kind, …)`:

| Protocol | `kind` |
|---|---|
| `iscsi` | `nics` |
| `fc` | `fc_hbas` |
| `nvme-tcp` | `nvme_sources` |

It then calls `compare_node_interfaces({node: [values]})` and returns
`OpResult.ok` when every node exposes the same set, or `OpResult.fail` with a
per-node breakdown when they differ (so the operator can fix the odd node out
before binding storage cluster-wide).

### Wizard steps

`wizard_steps()` → `["deploy", "configure", "register_hosts", "setup_connectivity"]`:
install the plugin and define the cluster-wide storage first, then register each
node on the array and configure per-node connectivity.

## Deploy walkthrough

1. **Register hosts** — `register_hosts` creates a FA host from the node's
   IQN/NQN/WWN (auto-discovered from the node over SSH unless you supply them)
   and a host group (`pve-cluster`). Re-running is safe (idempotent).
2. **Set up connectivity** — `setup_connectivity` installs `nvme-cli`
   (or `open-iscsi`/`multipath-tools`), enables native NVMe multipath
   (`nvme_core multipath=Y`), sets the `queue-depth` IO policy, and connects to
   the array portals persistently.
3. **Deploy the plugin** — `deploy` pushes `PureFAPlugin.pm` to
   `/usr/share/perl5/PVE/Storage/Custom/` and reloads the PVE daemons so the
   `purefa` storage type registers. (Or run `ansible/proxmox/deploy_purefa_plugin.yml`.)
4. **Define the storage** — `configure` writes the `purefa` stanza into
   `/etc/pve/storage.cfg`.
5. **Provision disks** — `provision` creates one FA volume per disk and attaches
   it directly to the VM.

## Fibre Channel (FC)

Fibre Channel differs fundamentally from the IP transports (iSCSI, NVMe-TCP):
**there is no host-side login or discovery step.** A LUN becomes visible once

1. the node's HBA WWPNs are **zoned** to the array's FC target ports on the SAN
   switch (operator action, off-host — PHIF does not configure the fabric), and
2. the node is **registered on the array by WWN** and the volume is connected to
   its host group.

After that, the node only needs to **rescan the FC/SCSI bus** and let multipath
assemble the device, which is then mapped **by WWID**.

### Host registration (by WWN)

`register_hosts` with `protocol=fc` calls
`ctx.array.create_host(name, wwns=[...])` — never IQN/NQN. WWNs come from:

- **SSH auto-discovery** (the default): when `node_wwns` is blank the connector
  calls `runner.discover_initiators()`, which reads `/sys/class/fc_host/*/port_name`
  on the node; the connector normalises each `0x21000024ff000001` to
  `21:00:00:24:ff:00:00:01`, or
- the `node_wwns` field (comma-separated WWPNs), which **overrides** discovery.

Registration fails only if discovery yields nothing **and** nothing was supplied
(and not in a dry-run). `create_host` / `create_host_group` are idempotent, so
re-running registration is safe.

### Connectivity (rescan, no login)

`setup_connectivity` with `protocol=fc` does **not** run `iscsiadm` or
`nvme connect`. It:

1. installs `multipath-tools` + `sg3-utils` and enables `multipathd`,
2. issues an FC LIP per HBA (`echo 1 > /sys/class/fc_host/hostX/issue_lip`),
3. rescans every SCSI host (`echo "- - -" > /sys/class/scsi_host/hostX/scan`)
   and runs `rescan-scsi-bus.sh -a` when present,
4. runs `multipath -r`.

> **Prerequisite:** SAN zoning must already pair the node HBA WWPNs with the
> array's FC target WWPNs. PHIF assumes this is done on the fabric switch.

### Provision / attach over FC

`provision` with `protocol=fc`: create the FA volume → connect it to the FC host
group → **SCSI rescan** (no login) → `qm set <vmid> -scsiN <storage>:<name>`.
The `purefa` Perl plugin's `activate_volume` resolves the multipath device by
WWID (`/dev/mapper/3<serial>`) and hands the raw block device to the guest.

### Perl plugin FC path (`PureFAPlugin.pm`)

- The `protocol` option accepts `iscsi | fc | nvme-tcp`.
- `activate_volume` branches: iSCSI/NVMe-TCP do session login/discovery; **FC
  only rescans** the SCSI bus (LIP + `scsi_host/scan` + `rescan-scsi-bus.sh`).
- `_device_path` (via `_fa_volume_serial`) maps a FA volume to its multipath
  WWID — for SCSI transports (iSCSI/FC) `/dev/mapper/3<lowercased-serial>` — so
  FC needs **no login** to find the device.

## Interface binding

`setup_connectivity` lets operators pin each transport to specific host
interfaces/HBAs instead of letting the kernel pick. The fields are
**MULTISELECT** and their choices are discovered live from the node via
`discover_options(kind)` → `runner.discover_interfaces(self._host(), kind, …)`
(mock-safe). Only the field for the **active protocol** is applied; selections
are persisted into the `purefa` `storage.cfg` stanza so `PureFAPlugin.pm` can
honor them.

| Field | Protocol | `options_source` | Effect |
|---|---|---|---|
| `iscsi_nics` | iSCSI | `nics` | One open-iscsi iface (`phif_<nic>`) is created and bound to each NIC's `iface.net_ifacename`; discovery and login run bound to those ifaces. |
| `nvme_sources` | NVMe-TCP | `nvme_sources` | One `nvme connect … -w <host_source>` per selected source address (host-traddr). |
| `nvme_options` | NVMe-TCP | — | Extra flags appended verbatim to each `nvme connect`. |
| `fc_hbas` | FC | `fc_hbas` | Records/limits to the selected HBAs (zoning is external on the fabric switch). |

### iSCSI (iface binding)

For each selected NIC the connector runs:

```bash
iscsiadm -m iface -I phif_<nic> --op=new
iscsiadm -m iface -I phif_<nic> --op=update -n iface.net_ifacename -v <nic>
iscsiadm -m discovery -t st -p <portal>:3260 -I phif_<nic>
iscsiadm -m node [-T <iqn_target>] -I phif_<nic> --login
```

With no NICs selected it falls back to the default unbound
`sendtargets` discovery + `--login`.

### NVMe-TCP (host-traddr)

For each selected source, one connection is bound via `-w`:

```bash
nvme connect -t tcp -a <array_traddr> -s 4420 -n <subsystem_nqn> \
  -w <host_source> --ctrl-loss-tmo=1800 --reconnect-delay=10 <nvme_options>
```

> `TODO(doc-validate)`: the target `array_traddr` is currently the
> operator-supplied portal; resolving the array's NVMe data IPs from the
> FlashArray REST API is not yet wired.

### FC (selection only)

FC has no host-side login, so `fc_hbas` is a **selection** that is recorded and
persisted; SAN zoning remains a fabric prerequisite.

### Persistence

Selected bindings are appended to the storage's `storage.cfg` stanza as
device-config keys (`iscsi_nics` / `nvme_sources` / `fc_hbas`), which
`PureFAPlugin.pm` declares in `properties`/`options`.

> `TODO(doc-validate)`: how `PureFAPlugin.pm` consumes these per-device binding
> keys (limit iSCSI ifaces / NVMe host-traddr / FC HBAs) for a given PVE release
> is not yet wired in the plugin — today they are recorded for the connector and
> audit.

All of this honors `ctx.dry_run` (plan only, no node mutation or persistence).

## Verify

```bash
cd backend
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest tests/test_proxmox.py -q
```

## Doc sources

- Everpure "Proxmox with FlashArray" solution (bundle `m_proxmox`):
  <https://support.purestorage.com/bundle/m_proxmox/page/Solutions/Proxmox/topics/c_proxmox_with_flasharray_quick_start_guide.html>
- Connecting Proxmox to FlashArray (iSCSI / FC / NVMe-TCP) topics under
  `m_proxmox`.
- Community per-volume PVE storage plugin reference (model for the per-disk
  volume design): <https://github.com/kolesa-team/pve-purestorage-plugin>

## `TODO(doc-validate)` items

The Everpure-published `m_proxmox` topic pages render as a JS-only shell to
non-browser fetchers, so the following specifics are implemented to documented
FlashArray/Proxmox semantics and should be confirmed:

- **Perl plugin (`PureFAPlugin.pm`):**
  - Exact FlashArray REST v2 version segment (`/api/2.x/...`) and login flow
    (`api-token` header → `x-auth-token`).
  - Precise JSON bodies for volume create / connect / snapshot / copy / resize.
  - `PVE::Storage::Plugin` `api()` version (`APIVER`) to pin against your PVE
    release.
  - Multipath device-path discovery (FA serial → SCSI wwid `3<serial>` vs NVMe
    `eui.<nguid>`) per transport — including the exact GET /volumes `serial`
    field name and the FC WWPN format `create_host(wwns=...)` expects.
  - Snapshot rollback / overwrite semantics (`overwrite=true`) for your Purity//FA version.
- **Connector:**
  - iSCSI `multipath.conf` device stanza (vendor `PURE`, `find_multipaths no`)
    against the `m_proxmox` iSCSI topic.
  - FC host-side rescan specifics (preferred `rescan-scsi-bus.sh` from
    `sg3-utils` vs `issue_lip` + `scsi_host/scan`) against the `m_proxmox` FC
    topic, plus the WWPN normalisation applied to the bare-hex WWNs returned by
    `runner.discover_initiators` (colon-delimited vs bare hex for
    `create_host(wwns=...)`).
  - perl REST dependency package names and the exact PVE daemon set.
  - **Cluster membership:** the `/etc/pve/.members` JSON shape
    (`nodelist.<name>.{ip,online}`) is per current pmxcfs behavior; confirm the
    field names/`online` semantics against the target PVE release (an alternative
    is `pvecm status`/`pvesh get /cluster/status`). Also confirm that pushing the
    plugin + reloading daemons on every node (rather than relying on pmxcfs to
    replicate `/etc/pve` only — which does **not** cover
    `/usr/share/perl5/...`) matches the intended deploy model.


## Manual installation (without PHIF)

This procedure reproduces, by hand, exactly what the PHIF Proxmox connector's **deploy** (`deploy_integration`) and **configure** (`configure`) actions do, plus the `register_hosts` and `setup_connectivity` steps. It installs the `purefa` Proxmox VE custom storage plugin on a PVE cluster.

> **Per-node vs cluster-wide.** Steps that touch the local filesystem or transport stack (plugin file, Perl deps, daemon reload, host registration, connectivity/multipath/ARP) must be done on **every** cluster node (the connector fans these out over SSH). The storage *definition* lives in pmxcfs (`/etc/pve/storage.cfg`) and is **cluster-wide** — run it **once** on any node. Per-node steps are marked **[EACH NODE]**; cluster-wide steps **[ONCE]**.

Set these shell variables to match your environment (mirrors the connector's `storage.cfg` keys):

```sh
STORAGE_ID=purefa                                  # Proxmox storage ID
PURE_ENDPOINT=https://flasharray.example.com       # FlashArray REST endpoint
PURE_API_TOKEN=<fa-api-token>                       # FlashArray REST API token
PROTOCOL=nvme-tcp                                   # iscsi | fc | nvme-tcp
HOST_GROUP=pve-cluster                              # FA host group the nodes belong to
CONTENT=images,rootdir
```

### 1. Install the plugin file on each node **[EACH NODE]**

```sh
mkdir -p /usr/share/perl5/PVE/Storage/Custom
cp PureFAPlugin.pm /usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm
```

### 2. Install the Perl / REST dependencies **[EACH NODE]**

```sh
apt-get update
apt-get install -y libjson-perl liblwp-protocol-https-perl libwww-perl multipath-tools
perl -MJSON -e1 || echo 'ERROR: perl JSON missing'   # the plugin won't load without it
```

### 3. Reload the PVE daemons **[EACH NODE]**

```sh
systemctl reload pvedaemon pveproxy pvestatd
pvesm status
```

### 4. Define the `purefa` storage stanza **[ONCE]**

Do **not** hand-append to `/etc/pve/storage.cfg` (PVE merges it into the previous section and rejects it). Use `pvesm`, which validates and writes a correctly separated section. Create it **disabled**, then enable after steps 5–6:

```sh
pvesm add purefa "$STORAGE_ID" \
    --pure_endpoint "$PURE_ENDPOINT" \
    --pure_api_token "$PURE_API_TOKEN" \
    --protocol "$PROTOCOL" \
    --host_group "$HOST_GROUP" \
    --content "$CONTENT" \
    --shared 1 \
    --disable 1
```

Resulting stanza:

```
purefa: purefa
        pure_endpoint   https://flasharray.example.com
        pure_api_token  <fa-api-token>
        protocol        nvme-tcp
        host_group      pve-cluster
        content         images,rootdir
        shared          1
        disable         1
```

`pure_endpoint`, `pure_api_token`, and `protocol` are **fixed** plugin properties (not changeable with `pvesm set` afterward).

### 5. FlashArray-side: host group + host registration (per node, on the array)

> Skip if `protocol=nfs` (NFS uses a mounted FlashArray File export, no initiators).

For **each** node, register its initiator(s) for the chosen protocol as a FlashArray host and add it to the host group. FlashArray host names allow only `[A-Za-z0-9-]` (sanitize, e.g. `192.0.2.58` → `192-0-2-58`). Discover each node's initiator:

```sh
cat /etc/iscsi/initiatorname.iscsi          # iSCSI IQN
cat /etc/nvme/hostnqn                       # NVMe-TCP NQN
cat /sys/class/fc_host/host*/port_name      # FC WWPNs
```

On the array (purefa CLI shown):

```sh
purehgroup create pve-cluster                                       # idempotent
purehost create --iqnlist iqn.1993-08.org.debian:01:<node> pve-node1   # iSCSI
purehost create --nqnlist nqn.2014-08.org.nvmexpress:uuid:<node> pve-node1  # NVMe-TCP
purehost create --wwnlist 21:00:00:24:ff:00:00:01,... pve-node1     # FC
purehgroup setattr --hostlist pve-node1,pve-node2 pve-cluster
```

For **FC**, also zone each node HBA WWPN to the array's FC target ports on the fabric switch (no host-side FC login step).

### 6. Node connectivity + multipath **[EACH NODE]**

**Everpure multipath drop-in (iSCSI + FC)** — a drop-in so it never clobbers an existing `/etc/multipath.conf`:

```sh
mkdir -p /etc/multipath/conf.d
test -f /etc/multipath.conf || touch /etc/multipath.conf
cat > /etc/multipath/conf.d/pure.conf <<'EOF'
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
EOF
systemctl enable --now multipathd
systemctl reload multipathd || systemctl restart multipathd
multipath -r || true
```

**iSCSI:**

```sh
apt-get install -y open-iscsi multipath-tools
systemctl enable --now iscsid multipathd
# (optional NIC binding) per selected storage NIC <nic>:
iscsiadm -m iface -I phif_<nic> --op=new
iscsiadm -m iface -I phif_<nic> --op=update -n iface.net_ifacename -v <nic>
# (multi-NIC ARP-flux fix on the bound NICs — persisted + live)
printf '%s\n' \
  'net.ipv4.conf.<nic>.arp_ignore = 2' \
  'net.ipv4.conf.<nic>.arp_announce = 2' \
  > /etc/sysctl.d/99-phif-iscsi-arp.conf
sysctl -w net.ipv4.conf.<nic>.arp_ignore=2 net.ipv4.conf.<nic>.arp_announce=2
# discover + login to each array portal (port 3260; add -I phif_<nic> when bound):
iscsiadm -m discovery -t sendtargets -p <portal-ip>:3260
iscsiadm -m node -T <array-target-iqn> --login
```

**NVMe-TCP** (uses native NVMe multipath — the dm-multipath drop-in is not needed):

```sh
apt-get install -y nvme-cli
modprobe nvme nvme-tcp nvme-core
printf 'nvme\nnvme-tcp\nnvme-core\n' > /etc/modules-load.d/nvme-tcp.conf
echo 'options nvme_core multipath=Y' > /etc/modprobe.d/nvme-tcp.conf
mkdir -p /etc/nvme; [ -f /etc/nvme/hostnqn ] || nvme gen-hostnqn > /etc/nvme/hostnqn
printf 'ACTION=="add", SUBSYSTEM=="nvme-subsystem", ATTR{iopolicy}="queue-depth"\n' \
    > /etc/udev/rules.d/99-nvme-iopolicy.rules
udevadm control --reload-rules && udevadm trigger
# connect to each array NVMe-TCP portal (connect port 4420; discovery 8009):
nvme connect -t tcp -a <portal-ip> -s 4420 -n <subsystem-nqn> --ctrl-loss-tmo=1800 --reconnect-delay=10
systemctl enable nvmf-autoconnect.service
nvme connect-all
```

**Fibre Channel** (no host-side login — zoning in step 5 + array host-group connection; node only rescans):

```sh
apt-get install -y multipath-tools sg3-utils
systemctl enable --now multipathd
for f in /sys/class/fc_host/host*/issue_lip; do [ -w "$f" ] && echo 1 > "$f" || true; done
for h in /sys/class/scsi_host/host*/scan; do echo '- - -' > "$h"; done
command -v rescan-scsi-bus.sh >/dev/null 2>&1 && rescan-scsi-bus.sh -a || true
```

### 7. Enable and verify **[ONCE]**

```sh
pvesm set "$STORAGE_ID" --disable 0
pvesm status
```
