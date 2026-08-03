# VMware vSphere connector

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../../DISCLAIMER.md`](../../DISCLAIMER.md) first.

**Key:** `vsphere` · **Maturity:** `ga` (migration validated on live vCenter; plugin/VASA deploy not yet hardware-validated) · **Protocols:** iSCSI, FC, NVMe-FC, NVMe-RoCE

Wraps the Everpure Data VMware integration:

- **Everpure Data vSphere Client Remote Plugin** — registered with vCenter so the
  VMware admin can drive FlashArray storage from the vSphere Client. As of
  vSphere 8.0, VMware supports only *remote* plugins (local plugins are removed),
  so this connector registers the remote plugin via the vCenter Extension /
  REST API.
- **VASA Provider (vVols)** — the Everpure VASA provider is registered with vCenter
  as a *Storage Provider* to enable Virtual Volumes (vVols). The provider
  endpoint is `https://<array>:8084` and requires FlashArray admin credentials.
  Real arrays expose two controllers (CT0/CT1); register both for HA.
- **Datastores** — VMFS (FlashArray volume backed), NFS, and vVol.

## Connection (target_schema)

| Field | Type | Required | Notes |
|---|---|---|---|
| `vcenter_host` | string | yes | vCenter FQDN/IP |
| `vcenter_user` | string | yes | e.g. `administrator@vsphere.local` |
| `vcenter_password` | secret | yes | stored encrypted in the vault |
| `datacenter` | string | no | default datacenter for datastore ops |
| `cluster` | string | no | default cluster for datastore/host ops |
| `host_group` | string | no | FA host group the ESXi cluster belongs to; default for `register_hosts` / `setup_connectivity` when their `host_group` field is blank |

> The FlashArray **endpoint and API token are taken from the associated array**
> (`ctx.array.endpoint` / `ctx.resolve_token()`) — there is no operator-entered
> array endpoint or token field here or in any action. The vCenter host/user/
> password above are the vCenter side only.

`validate_connection` opens a vCenter REST session (`POST /api/session` with HTTP
Basic auth → session token used as the `vmware-api-session-id` header) and, when
a FlashArray is associated, confirms `ctx.array.info()`. Mock-safe via
`ctx.runner.run_http`.

## Capabilities and actions

| Capability | Action id | What it does |
|---|---|---|
| DEPLOY_PLUGIN | `deploy` | Register the remote plugin + VASA provider with vCenter |
| CONFIGURE | `configure` | (Re)register / refresh the VASA Storage Provider |
| HOST_REGISTER | `register_hosts` | Create a FlashArray host group for the ESXi cluster; initiators are auto-discovered from vCenter (protocol-aware: iSCSI→IQN, FC→WWN, NVMe→NQN) unless typed explicitly (Ansible) |
| CONNECTIVITY | `setup_connectivity` | Create per-host FlashArray hosts for iSCSI/FC/NVMe-oF and group them; initiators auto-discovered from vCenter when not supplied; FC triggers an HBA rescan; optional interface binding (iSCSI port binding / FC HBA / NVMe-oF adapter selection) |
| PROVISION_DATASTORE | `provision_datastore` | Volume → connect to host group → rescan → create VMFS/vVol (iSCSI/FC/NVMe-oF); or NFS |
| PROVISION_VOLUME | `provision` | Create a FlashArray volume (optionally connect to a host group) |
| SNAPSHOT | `snapshot` | FlashArray volume snapshot |
| CLONE | `clone` | FlashArray volume clone (optionally attach to host group) |
| RESIZE | `resize` | Extend the FlashArray volume and grow the backing datastore |
| QOS | `set_qos` | Set volume IOPS / bandwidth limits |
| REPLICATION | `configure_replication` | Create a FlashArray protection group |
| HEALTH | `health_check` | Report array info + vCenter session status |
| REMOVE | `teardown` | Unregister the VASA provider and remote plugin |

## Plugin container images

The vSphere Client Plugin ships as two containers, both published on Docker Hub:

| Container | Image | Port |
|---|---|---|
| Plugin server | [`everpure/client-plugin-vsphere`](https://hub.docker.com/r/everpure/client-plugin-vsphere) | 8080 (internal) |
| Reverse proxy | [`everpure/client-plugin-vsphere-reverse-proxy`](https://hub.docker.com/r/everpure/client-plugin-vsphere-reverse-proxy) | 9443 |

Both default to tag **`5.6.1`**. The tag is pinned on purpose: neither repository
publishes a `latest` tag, so an unpinned reference fails to resolve with
`no such manifest`. The two images are released together — when you move to a
newer plugin release, bump both.

There is nothing to build or download by hand. Pick whichever install path suits
you — both pull the same images:

- **From the PHIF UI (default).** Go to the **vSphere Plugin** page and click
  **Install from Docker Hub**. PHIF pulls both images, generates the shared
  plugin secret, and starts the containers. Progress is reported on the page;
  the pull runs in the background, so navigating away is safe.
- **From compose.** `docker compose --profile vsphere up -d` starts both
  containers, pulling as needed. Generate the secret first:
  `openssl rand -base64 128 | tr -d '\n' > certs/vsphere-plugin-secret`

To pin a version or pull from a private registry mirror instead, set
`PHIF_VSPHERE_PLUGIN_IMAGE` and `PHIF_VSPHERE_PLUGIN_PROXY_IMAGE` (each a full
`repo:tag` reference) and restart the backend.

**Air-gapped hosts.** If the PHIF host cannot reach Docker Hub, save the images
on a connected machine, copy the tars over, and upload them under **Offline
install** on the same page:

```bash
docker pull everpure/client-plugin-vsphere:5.6.1
docker save everpure/client-plugin-vsphere:5.6.1 -o vsphere-plugin.tar
docker pull everpure/client-plugin-vsphere-reverse-proxy:5.6.1
docker save everpure/client-plugin-vsphere-reverse-proxy:5.6.1 \
  -o vsphere-plugin-reverse-proxy.tar
```

Once the containers are running, register the plugin with vCenter using the
deploy flow below.

## Deploy flow

1. **Add hypervisor** with the vCenter details above; associate a FlashArray.
2. **Deploy** (`deploy`) — registers the remote plugin extension and the VASA
   provider. The VASA control-plane URL is derived from the associated array's
   own management endpoint (`ctx.array.endpoint` → `https://<array-host>:8084`),
   not from a typed array field; an optional `vasa_host` override is available.
3. **Register hosts** (`register_hosts`) — runs `ansible/vsphere/register_hosts.yml`
   using the `purestorage.flasharray` collection (`purefa_host`, `purefa_hostgroup`)
   to create the array host group for the ESXi cluster. ESXi initiators
   (IQN/WWN/NQN) are **auto-discovered from vCenter** — the operator no longer types
   them. The array `create_host`/`create_host_group` calls are idempotent, so
   re-running registration safely converges.
4. **Provision a datastore** (`provision_datastore`) — for VMFS/vVol the connector
   creates the FlashArray volume, connects it to the host group, triggers an ESXi
   storage rescan, then creates the datastore in vCenter. For NFS it registers the
   export with vCenter directly.

## Initiator auto-discovery (from vCenter)

ESXi host initiators are **discovered from vCenter**, not typed by the operator.
`register_hosts` and `setup_connectivity` call `_discover_esxi_initiators(protocol)`,
which enumerates the configured cluster/datacenter's ESXi hosts and reads their
storage-adapter initiators, selecting the kind that matches the chosen transport:

- `iscsi` → iSCSI adapter **IQNs**
- `fc` → FC HBA **port WWNs**
- `nvme-fc` / `nvme-roce` → NVMe-oF **NQNs**

The address form fields are all optional and carry the help text
*"(auto-discovered from vCenter if left blank)"*. Typing explicit initiators
**overrides** discovery for that call. In mock/dry-run mode the helper returns
synthetic-but-realistic initiators (e.g. `iqn.1998-01.com.vmware:esxiNN`,
`21:00:00:24:ff:...`, `nqn.2014-08.com.vmware:nvme:esxiNN`) so the flows stay
exercisable without a live vCenter, and dry-run still makes no array changes.

### Initiator display (`discover_options("initiators")`)

So the UI can pre-fill / display the ESXi initiators on the `register_hosts`
form, `discover_options("initiators")` reuses `_discover_esxi_initiators(...)`
for each transport family and returns options tagged with the form field they
populate:

```json
[
  {"field": "iqns", "value": "iqn.1998-01.com.vmware:esxi01", "label": "iSCSI IQN — ..."},
  {"field": "wwns", "value": "21:00:00:24:ff:00:00:01",       "label": "FC WWN — ..."},
  {"field": "nqns", "value": "nqn.2014-08.com.vmware:nvme:esxi01", "label": "NVMe NQN — ..."}
]
```

`field` is the matching `register_hosts` input (`iqns` / `wwns` / `nqns`). In
mock/dry-run the synthetic discovery set is returned, so the form stays
exercisable without a live vCenter. (Array iSCSI/NVMe *portal* discovery is not
applicable to vSphere datastores and is intentionally not implemented; iSCSI
port-binding / FC HBA / NVMe adapter discovery remains under the `nics` /
`fc_hbas` / `nvme_sources` kinds.)

## FlashArray endpoint & token (from the associated array)

VASA registration and the array-side host-registration playbook take the
FlashArray management endpoint from `ctx.array.endpoint` and the API token from
`ctx.resolve_token()` (the single shared array token, override via the optional
`fa_api_token` kwarg on `register_hosts`). There are **no** operator-entered
array endpoint/token form fields on `deploy`, `configure`, `register_hosts`, or
`setup_connectivity` — the connected array is the single source of truth.

## Cluster (multi-ESXi-host) support

vSphere is inherently clustered: a vCenter manages one or more clusters, each
containing several ESXi hosts. The connector is cluster-aware:

- **`list_nodes()`** enumerates the ESXi hosts in the configured `cluster` (falling
  back to `datacenter`, then to a single host) and returns one `ClusterNode` per
  ESXi host (`name == host == ESXi hostname`). In mock/dry-run it returns a
  synthetic 2-host cluster (`esxi01.example.com`, `esxi02.example.com`). On a live
  vCenter it queries `GET /api/vcenter/host` filtered by cluster/datacenter
  (`TODO(doc-validate)` — see below); if nothing is enumerated it falls back to a
  single node so non-clustered / unreachable setups still work.
- **`wizard_steps()`** returns the vSphere-specific order
  `["deploy", "configure", "register_hosts", "setup_connectivity", "provision_datastore"]`
  (only ids present in `action_schemas` are listed). `deploy`/`configure`/
  `provision_datastore` are cluster-wide (single vCenter calls); `register_hosts`
  and `setup_connectivity` fan out across the cluster's ESXi hosts.

### Fan-out vs. single calls

| Operation | Scope |
|---|---|
| `deploy` (plugin + VASA), `configure` (VASA refresh), `provision_datastore` | **Cluster-wide** — one call against vCenter |
| `register_hosts`, `setup_connectivity` | **Per ESXi host** — one FlashArray host per ESXi node, all in one host group |

`register_hosts` fans out so **every** ESXi host's initiators are registered (all
in the one host group):

- Named hosts (`hosts=esxA,esxB`) → one array host per named ESXi host, each
  carrying its discovered (or explicit) initiators.
- No hosts and no typed initiators → the connector enumerates the cluster via
  `list_nodes()` and registers each discovered ESXi host.
- A flat explicit initiator list (e.g. `iqns=iqn.a,iqn.b`) with no host list is
  treated as a single implicit host (back-compat — a flat list is not per-host).

`setup_connectivity` likewise applies iSCSI port-binding / HBA selection **per
ESXi host**; when no `hosts` are given it fans out across the whole cluster.

### Cluster validation (`validate_cluster`)

`validate_cluster(protocol=...)` reads each ESXi host's storage-adapter
initiators (for the chosen transport) and compares them across hosts via
`compare_node_interfaces()`. A uniform set means the chosen host-group binding is
valid cluster-wide; a mismatch fails with a per-host detail string so the operator
can see which hosts diverge. Mock/dry-run yields a consistent synthetic cluster
(validates as uniform).

## Host group carry-over

`register_hosts` and `setup_connectivity` fall back to the `host_group` set on
the hypervisor connection (`target_schema`) when their own `host_group` field is
left blank. If neither is set, the action fails with a clear message
(`No host_group provided or configured on the hypervisor ...`).

## Fibre Channel (FC)

The connector supports FC as a first-class block transport alongside iSCSI and
NVMe-oF. FC differs from iSCSI in how the host is registered on the array and in
how ESXi discovers the LUN.

### Prerequisite: SAN zoning

FC has **no IP target configuration and no iSCSI login step**. Before registering
hosts you must zone the ESXi HBA port WWNs to the FlashArray's FC ports on the SAN
fabric (done switch-side, outside PHIF). Once zoned, connecting a volume to the
host group presents the LUN over the fabric automatically.

### WWN-based host registration

`register_hosts` / `setup_connectivity` are protocol-aware. With `protocol=fc`:

- ESXi hosts are registered on the array **by WWN** (`ctx.array.create_host(name,
  wwns=[...])`), never by IQN/NQN. Stray IQN/NQN inputs are ignored for FC.
- The ESXi HBA port WWNs are **auto-discovered from vCenter** by default. You may
  still supply them explicitly to override — via the `wwns` field
  (`register_hosts`) or the per-host `initiators` map (`setup_connectivity`, e.g.
  `esx1=21:00:00:24:ff:aa:bb:cc;esx2=...`).
- FC with no WWNs no longer hard-fails: the connector auto-discovers them from
  vCenter (registration only fails if discovery also yields nothing).
- The Ansible playbook receives `protocol=fc` and only the `wwns` list, so
  `purefa_host` is created with `protocol: fc`.

### Rescan-based discovery

For FC there is no dynamic-target login. After the volume is connected on the
array, the connector triggers a vCenter/ESXi **HBA storage rescan**
(`POST /api/vcenter/cluster/<cluster>/storage/rescan`) so the fabric-zoned device
appears, then creates the datastore. `setup_connectivity` over FC also performs a
rescan after grouping the hosts (its result reports `rescanned: true`); the iSCSI
path does not rescan (`rescanned: false`).

### VMFS-over-FC provisioning flow

`provision_datastore(..., type="vmfs", protocol="fc")`:

1. Create the FlashArray volume.
2. Connect it to the FC host group (host group members are WWN-registered + zoned).
3. Trigger an ESXi HBA rescan so the new LUN is seen.
4. Create the VMFS datastore in vCenter.

## Interface binding (iSCSI port binding / HBA selection)

`setup_connectivity` can optionally bind the active transport's storage path to
specific ESXi adapters. In vSphere this means:

- **iSCSI** → *iSCSI port binding*: bind the ESXi iSCSI software adapter to the
  selected VMkernel (`vmk`) NICs (`esxcli iscsi networkportal add` / VMkernel
  binding). There is no SSH; this is driven via vCenter/ESXi.
- **FC** → select which FC HBAs (`vmhbaN`) to use for the transport.
- **NVMe-oF** → select the NVMe-oF adapters / vmknics.

### Discoverable form fields

The three fields on `setup_connectivity` are all **optional** `MULTISELECT`s whose
choices are populated dynamically via the connector's `discover_options(kind)`
(surfaced through `GET /hypervisors/{id}/discover/{kind}`):

| Field | `options_source` (`DiscoveryKind`) | Binds |
|---|---|---|
| `iscsi_vmknics` | `nics` | VMkernel NICs for iSCSI port binding |
| `fc_hbas` | `fc_hbas` | ESXi FC HBAs |
| `nvme_adapters` | `nvme_sources` | NVMe-oF adapters / vmknics |

`discover_options(kind)` queries vCenter for the cluster's ESXi adapters:
`"nics"` → VMkernel adapters usable for iSCSI port binding, `"fc_hbas"` → FC HBAs,
`"nvme_sources"` → NVMe-oF adapters/vmknics. In mock/dry-run mode it returns a
small synthetic set (e.g. `vmk1 (iSCSI)`, `vmhba1 (FC HBA ...)`) so the binding UI
stays exercisable without a live vCenter.

### Apply behavior

Binding is applied **per active protocol** after the hosts are grouped (and after
the FC HBA rescan), using `ctx.runner.run_http` with the same `404`-tolerant
handling as the rest of the connector:

- iSCSI: a port-binding call is made for each selected VMkernel NIC on each host;
  the result reports `artifacts["interface_binding"]["iscsi_port_binding"]`.
- FC: the selected HBAs are recorded (`interface_binding.fc_hbas`); HBA selection
  is otherwise a SAN-side concern with no per-LUN bind call.
- NVMe-oF: an adapter-select call is made per host
  (`interface_binding.nvme_adapters`).

Only the kind matching the chosen transport is applied; leaving a field blank
skips binding (no `interface_binding` artifact). `ctx.dry_run` is respected — the
early dry-run return in `setup_connectivity` means no binding calls are made.

## Array-side Ansible

`ansible/vsphere/register_hosts.yml` (collection `purestorage.flasharray`):

```bash
ansible-galaxy collection install purestorage.flasharray
```

Modules used: `purefa_host`, `purefa_hostgroup` (and `purefa_volume`,
`purefa_connection`, `purefa_pg` are available for extended array-side flows).
The playbook receives `fa_url` from `ctx.array.endpoint` and `fa_api_token` from
`ctx.resolve_token()` (the shared array token, overridable via the optional
`fa_api_token` action kwarg) — not from operator-entered array fields. Minting a
fresh token via `ctx.array.create_api_token(...)` remains available where the
environment allows it.

## dry-run

All mutating operations honor `ctx.dry_run`: they validate/plan, emit progress,
and make no array or vCenter changes.

## Verify

```bash
cd backend
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest tests/test_vsphere.py -q
```

## doc-validate TODOs

The following specifics were not fully verified against the current Everpure/VMware
REST references and should be confirmed before GA sign-off:

- `# TODO(doc-validate):` Exact vCenter REST paths/payloads for remote-plugin
  registration. vCenter 8.x registers extensions via the Extension Manager
  (vim25 / `RegisterExtension`); the public `/api/vcenter/extension` REST surface
  and exact body used here are approximations. The connector tolerates `404` on
  these calls so mock/real flows do not hard-fail on path drift.
- `# TODO(doc-validate):` Exact REST path for registering a VASA Storage Provider
  (`/api/vcenter/storage/provider`) and the datastore-create / rescan / grow
  endpoints. In production these are commonly driven via pyVmomi
  (`vim.host.StorageSystem`, `RegisterVasaProvider`) rather than REST.
- `# TODO(doc-validate):` Everpure remote plugin extension key
  (`com.purestorage.purestoragehtml`) and the exact supported plugin versions for
  the target vCenter release.
- `# TODO(doc-validate):` Number/identity of VASA endpoints to register per array
  (CT0/CT1) and minimum Purity version for vVols.
- `# TODO(doc-validate):` Exact vCenter REST path/payload for an HBA storage
  rescan (`/api/vcenter/cluster/<cluster>/storage/rescan`) used for FC LUN
  discovery — in production this is commonly driven via pyVmomi
  (`vim.host.StorageSystem.RescanAllHba` / `RescanVmfs`). The connector tolerates
  `404` so mock/real flows do not hard-fail on path drift.
- `# TODO(doc-validate):` Exact vCenter API path/shape for **ESXi initiator
  auto-discovery**. The connector currently approximates this as
  `GET /api/vcenter/cluster/<cluster>/host/storage/adapter` and tolerates `404`.
  Production almost certainly uses pyVmomi `HostStorageSystem`
  (`storageDeviceInfo.hostBusAdapter` → `HostInternetScsiHba.iScsiName` for IQNs,
  `HostFibreChannelHba.portWorldWideName` for FC WWNs) and the host's NVMe-oF host
  NQN, rather than this REST surface. Confirm path, response shape, and the field
  names parsed in `_parse_discovered_initiators` before GA sign-off.
- `# TODO(doc-validate):` Exact vCenter API path/shape for enumerating ESXi
  adapters in `discover_options` (`nics` / `fc_hbas` / `nvme_sources`). The
  connector approximates this as
  `GET /api/vcenter/cluster/<cluster>/host/adapter/<kind>` and tolerates `404`.
  Production almost certainly uses pyVmomi (`HostNetworkSystem.networkInfo.vnic`
  for VMkernel NICs, `HostStorageSystem` `HostFibreChannelHba` for FC HBAs, and
  the NVMe-oF adapters/vmknics). Confirm path, response shape, and the field names
  parsed in `_parse_discovered_adapters`.
- `# TODO(doc-validate):` Exact REST/esxcli used to **apply** interface binding.
  iSCSI port binding likely runs `esxcli iscsi networkportal add -A <iscsi-hba> -n
  <vmk>` (or pyVmomi `HostStorageSystem`); the
  `/api/vcenter/host/<host>/storage/iscsi/port-binding` and `.../nvme/adapter-select`
  REST paths used here are approximations (the connector tolerates `404`). Also
  confirm whether/how the target vCenter exposes restricting an FC transport to
  specific HBAs (`esxcli storage core adapter` / pyVmomi).
- `# TODO(doc-validate):` FC host registration uses `purefa_host` with
  `protocol: fc` and a `wwns` list; confirm the WWN format expected by the target
  `purestorage.flasharray` collection version (colon-delimited vs. bare hex).
- `# TODO(doc-validate):` `discover_options("initiators")` reuses the same
  approximated vCenter storage-adapter query as `_discover_esxi_initiators` (see
  the ESXi initiator auto-discovery TODO above). Confirm the real vCenter path/
  shape so the UI initiator display reflects live ESXi adapters rather than the
  synthetic mock set.
- `# TODO(doc-validate):` Exact vCenter API path/shape for **listing the ESXi
  hosts in a cluster** (cluster awareness / `list_nodes`). The connector
  approximates this as `GET /api/vcenter/host?clusters=<cluster>` (falling back to
  `?datacenters=<dc>`) and tolerates `404`. The real REST surface filters by the
  cluster/datacenter **MOID** (not the display name), and production may instead
  use pyVmomi (`ClusterComputeResource.host` → `HostSystem.name`). Confirm the
  path, the cluster→MOID lookup, and the response shape parsed in
  `_parse_cluster_hosts` (per-host `name`/`host`) before GA sign-off.
- `# TODO(doc-validate):` The VASA control-plane host is derived from
  `ctx.array.endpoint` (host[:port] before the `:8084` port). Confirm the array's
  configured management endpoint is the correct VASA provider host (vs. a
  dedicated CT0/CT1 VASA address) for the target Purity/array configuration.
