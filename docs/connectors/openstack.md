# OpenStack (Cinder / Everpure FlashArray volume driver)

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../../DISCLAIMER.md`](../../DISCLAIMER.md) first.

- **Connector key:** `openstack`
- **Display name:** OpenStack (Cinder / Everpure driver)
- **Maturity:** `ga` (Pure-Cinder deploy validated end-to-end on a live controller)
- **Protocols:** iSCSI, FC, NVMe-TCP, NVMe-RoCE

This connector **wraps the upstream Cinder FlashArray volume driver** rather than
installing anything new. The driver ships inside Cinder itself
(`cinder.volume.drivers.pure.PureISCSIDriver` / `PureFCDriver` / `PureNVMEDriver`),
so "deploying the plugin" is purely a matter of configuring a backend stanza in
`cinder.conf`, adding it to `enabled_backends`, and restarting `cinder-volume`.

All controller-side work is driven over SSH (`ctx.runner.run_ssh`) to the
OpenStack controller; the `openstack`/`cinder` CLIs handle day-2 operations.
Everything is mock-safe (`PHIF_MOCK_MODE=1`) and honors `ctx.dry_run`.

## References

- Everpure Data OpenStack Driver Best Practices —
  <https://support.purestorage.com/bundle/m_openstack/page/Solutions/OpenStack/OpenStack_Reference/topics/concept/c_pure_storage_openstack_driver_best_practices.html>
- Everpure FlashArray Volume Driver for OpenStack — Release Notes (Flamingo
  2025.2 / Epoxy 2025.1 / Dalmatian 2024.2 / Caracal 2024.1 / Antelope / etc.) —
  <https://support.purestorage.com/bundle/m_openstack/page/Solutions/OpenStack/OpenStack_Reference/topics/concept/c_pure_storage_flasharray_volume_driver_for_openstack_release.html>

The Everpure support docs confirm the driver is upstream in Cinder and publish the
per-release best-practice guides as downloadable PDFs. The exact `cinder.conf`
key names below are taken from the upstream OpenStack Cinder Everpure driver
configuration reference.

> TODO(doc-validate): The detailed `cinder.conf` key reference is distributed as
> per-release PDFs on the Everpure support site (not inline HTML). The key names used
> here (`volume_driver`, `san_ip`, `pure_api_token`, `volume_backend_name`,
> `use_multipath_for_image_xfer`, `pure_eradicate_on_delete`, `pure_nvme_transport`,
> `replication_device`) match the upstream Cinder driver docs; confirm against the
> PDF for your target release before production use.
>
> TODO(doc-validate): Confirm the FC zoning best-practice (single-initiator /
> single-target zoning) and the FC driver's automatic array-side host management
> behavior against the per-release Everpure OpenStack FC driver PDF before production
> use.

## Connect (target schema)

| Field | Type | Notes |
|---|---|---|
| `controller_host` | string | SSH-reachable OpenStack controller running `cinder-volume`. |
| `ssh_user` | string | Default `stack`. |
| `ssh_password` | secret | Provide this **or** `ssh_key`. |
| `ssh_key` | text/secret | PEM private key alternative to the password. |
| `cinder_conf_path` | string | Default `/etc/cinder/cinder.conf`. |
| `protocol` | enum | `iscsi`, `fc`, `nvme-tcp`, `nvme-roce`. |
| `backend_name` | string | Cinder backend / `volume_backend_name`. Default `pure`. |

> **FlashArray credentials come from the associated array, not the form.** The
> Cinder `san_ip` and `pure_api_token` are **not** operator-entered fields. They
> are derived from the FlashArray associated with this hypervisor:
>
> - `san_ip` ← the array's management endpoint (`ctx.array.endpoint`), with any
>   `scheme://` and trailing `:port` stripped to a bare host.
> - `pure_api_token` ← `ctx.resolve_token()` (the token the array was connected
>   with); if none is available, a token is minted from the array
>   (`ctx.array.create_api_token`).
>
> This matches the consistent PHIF UX across connectors (see the Proxmox
> connector's `configure`, which derives `pure_endpoint`/`pure_api_token` the
> same way). The `deploy` action still accepts optional `san_ip` /
> `pure_api_token` **method kwargs** as advanced overrides. If no array is
> associated and nothing is passed explicitly, `deploy` fails with a clear
> message (dry-run still plans with a placeholder token).

`validate_connection` SSHes to the controller, runs `openstack --version`, and
checks that `cinder.conf` exists — all non-fatal in mock mode.

## Capabilities & actions

| Capability | Action id | What it does |
|---|---|---|
| DEPLOY_PLUGIN | `deploy` | Renders the `[<backend>]` stanza (`san_ip` + `pure_api_token` **derived from the associated array**), **shows a diff**, appends it to `cinder.conf`, updates `enabled_backends` (via `crudini`), restarts `cinder-volume`. Idempotent: skips if the stanza already exists. Optional fields: `pure_iscsi_cidr` / `pure_iscsi_cidr_list` (iSCSI subnet binding), `nvme_options` (NVMe), and a discoverable `nics` multiselect — see [Interface binding](#interface-binding-pure_iscsi_cidr--host-side). Advanced override kwargs: `san_ip`, `pure_api_token`. |
| CONFIGURE | `configure` | `openstack volume type create` + sets `volume_backend_name` extra spec; optional QoS spec. |
| PROVISION_VOLUME | `provision` | `openstack volume create --size <GiB> --type <type>`. |
| SNAPSHOT | `snapshot` | `openstack volume snapshot create --volume <vol>`. |
| CLONE | `clone` | `openstack volume create --source <src>`. |
| RESIZE | `resize` | `cinder extend <vol> <GiB>`. |
| QOS | `set_qos` | `openstack volume qos create` + `qos associate <type>`. |
| REPLICATION | `configure_replication` | Sets `replication_enabled='<is> True'` extra spec on a volume type (requires a `replication_device` in the backend stanza). |
| HEALTH | `health_check` | `openstack volume service list`. |
| REMOVE | `teardown` | Removes the backend section + drops it from `enabled_backends` (via `crudini`), restarts `cinder-volume`. |

## Example rendered `cinder.conf` stanza (iSCSI)

```ini
[pure]
volume_backend_name = pure
volume_driver = cinder.volume.drivers.pure.PureISCSIDriver
san_ip = 192.0.2.10                 # from the associated array's mgmt endpoint
pure_api_token = <FlashArray API token>   # from ctx.resolve_token / minted
use_multipath_for_image_xfer = true
pure_eradicate_on_delete = false
```

For NVMe protocols the stanza additionally sets `pure_nvme_transport`
(`tcp` for `nvme-tcp`, `roce` for `nvme-roce`) and uses `PureNVMEDriver`.
`enabled_backends` in `[DEFAULT]` is updated to include `pure`.

### Fibre Channel (FC)

Setting `protocol = fc` renders a stanza that selects the **FC** volume driver:

```ini
[pure]
volume_backend_name = pure
volume_driver = cinder.volume.drivers.pure.PureFCDriver
san_ip = 192.0.2.10
pure_api_token = <FlashArray API token>
use_multipath_for_image_xfer = true
pure_eradicate_on_delete = false
```

Notes specific to FC:

- The driver class is `cinder.volume.drivers.pure.PureFCDriver` — distinct from
  the iSCSI (`PureISCSIDriver`) and NVMe (`PureNVMEDriver`) drivers. The renderer
  selects it automatically from the `protocol` field.
- **iSCSI/NVMe-only keys are not emitted** for FC. In particular no
  `pure_iscsi_cidr` and no `pure_nvme_transport` line appears in an FC stanza.
- **Zoning prerequisite:** the Nova compute hosts' FC HBAs must be zoned to the
  FlashArray's FC target ports on the SAN fabric (single-initiator/single-target
  zoning per Everpure best practice). This is a switch-side step done outside this
  connector; without it, attaches will fail at the host even though the
  `cinder.conf` backend is valid.
- **Automatic host management:** the Everpure FC driver auto-manages FlashArray host
  objects by initiator WWN at attach time. As long as the compute hosts are
  zoned, the driver creates/updates the array-side host objects on demand, so
  **manual host registration on the FlashArray is usually not required** for FC.
  (Contrast with the iSCSI workflow, which logs in over IP.)

## Interface binding (`pure_iscsi_cidr` / host-side)

In OpenStack the Everpure Cinder driver plus os-brick handle attach on the Nova
compute hosts, so "interface binding" is expressed in two distinct layers:

1. **Driver-level subnet selection (what this connector renders).** For iSCSI,
   `pure_iscsi_cidr` (single CIDR) or `pure_iscsi_cidr_list` (comma-separated
   list) restricts which of the array's iSCSI target subnet(s) the driver
   advertises to clients. This is the driver-level "which interface/subnet"
   control. Supply it on the `deploy` action (or as a target field) and it is
   rendered into the iSCSI `[backend]` stanza:

   ```ini
   [pure]
   volume_backend_name = pure
   volume_driver = cinder.volume.drivers.pure.PureISCSIDriver
   san_ip = 192.0.2.10
   pure_api_token = <FlashArray API token>
   use_multipath_for_image_xfer = true
   pure_eradicate_on_delete = false
   pure_iscsi_cidr = 192.0.2.0/24
   ```

   These keys are **iSCSI-only**: they are never emitted for FC or NVMe stanzas
   (see the FC section above). For NVMe protocols, any extra transport-tuning
   options can be passed via the free-form `nvme_options` field, which appends
   `key = value` lines to the NVMe stanza.

2. **Compute-host iface binding (host-managed, separate).** The actual
   `iscsiadm` iface binding (and the NVMe-TCP `host-traddr` source selection)
   that carries data-path traffic lives on each Nova **compute** host and is
   managed there (host networking, multipath, `iscsiadm` ifaces). This connector
   does not configure it. The `deploy` action exposes a `nics` multiselect
   (`options_source="nics"`) and `discover_options("nics" | "nvme_sources" |
   "fc_hbas")` enumerates interfaces on the **controller** over SSH for
   reference/documentation only.

> TODO(doc-validate): The interfaces enumerated by `discover_options` are the
> controller's NICs/HBAs, not the compute hosts' — compute-host iSCSI/NVMe iface
> binding is separate and host-managed. Confirm the desired UX (surface
> compute-host NICs) and the exact iSCSI CIDR key names
> (`pure_iscsi_cidr` vs `pure_iscsi_cidr_list`) against the per-release Everpure
> OpenStack driver PDF.
>
> TODO(doc-validate): NVMe-TCP target transport-selection option names passed via
> `nvme_options` are not yet validated against the shipping Everpure driver release.
>
> For FC, HBA usage is zoning-driven and usually automatic (see the FC section);
> no iSCSI/NVMe binding keys are rendered.

## Cluster awareness

For OpenStack the "cluster" is the **controller node(s)** that run
`cinder-volume` (where the Everpure backend config in `cinder.conf` lives) plus the
**Nova compute hosts** (where os-brick performs the actual iSCSI/NVMe/FC attach).
Because the Cinder backend config is controller-side and os-brick handles the
compute-side attach, cluster awareness here is mostly node listing plus the
right wizard steps — not per-node connectivity setup.

### `wizard_steps()`

```
["deploy", "configure"]
```

Unlike the host-managed hypervisors, OpenStack **omits** `register_hosts` and
`setup_connectivity`: the Everpure Cinder driver auto-manages FlashArray host objects
at attach time, and os-brick handles the iSCSI/NVMe/FC attach on the compute
hosts. So the wizard just deploys the `cinder.conf` backend (and restarts
`cinder-volume`), then creates the volume type.

### `list_nodes()`

SSHes to the controller and reads the service catalogs to enumerate the cluster:

- `openstack volume service list -f json` → `cinder-volume` hosts (role
  `controller`)
- `openstack compute service list -f json` → `nova-compute` hosts (role
  `compute`)

Each host becomes a `ClusterNode` with `info.role` set to `controller` or
`compute`. A Cinder service `Host` is `hostname@backend`; the `@backend` suffix
is stripped to the hostname. In mock/dry-run the CLIs return no output, so a
synthetic controller + two compute hosts (`compute-0`, `compute-1`) are returned
so wizard/cluster flows stay exercisable. If discovery yields nothing (CLIs
unavailable), it falls back to the single configured `controller_host`.

### `validate_cluster(**params)`

In OpenStack, interface binding is **host-managed**: os-brick performs the attach
on each compute host using that host's own `iscsiadm` ifaces / NVMe `host-traddr`
/ FC HBAs. The array-side control is the iSCSI target subnet (`pure_iscsi_cidr`)
rendered into `cinder.conf` on the controller. A strict per-node NIC check (as
`compare_node_interfaces` does for clustered host hypervisors) is therefore not
generally available from the controller, so this connector does **not** attempt
one. It returns `OpResult.ok` with the node inventory and a clear note that
compute-host iSCSI/NVMe iface config is host-managed, and confirms the array
iSCSI CIDR (if set) that each compute host must be able to reach.

> TODO(doc-validate): Surfacing/validating per-compute-host NICs (and their
> reachability to the array iSCSI CIDR) would require SSH to each compute host;
> the controller CLIs do not expose this. Confirm the desired UX before relying
> on cluster validation for production connectivity guarantees.

### Replication

Add a `replication_device` line to the backend stanza pointing at the secondary
array, e.g.:

```ini
replication_device = backend_id:secondary,san_ip:198.51.100.10,api_token:<token>
```

then enable it on the volume type via the `configure_replication` action.

## Idempotency

`deploy` reads the current `cinder.conf` and uses `pure.stanza_exists` /
`pure.build_enabled_backends` to detect an existing backend, returning a no-op
result instead of duplicating the stanza. `teardown` likewise reports cleanly if
the stanza is already gone.

## Dry run

With `ctx.dry_run`, `deploy` prints the full planned stanza and the new
`enabled_backends` value but makes no changes; all other actions log their
intended CLI calls and return a `planned` status.

## Testing

```bash
cd backend
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest tests/test_openstack.py -q
```
