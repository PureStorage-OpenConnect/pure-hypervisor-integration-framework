# Red Hat OpenShift (Portworx / px-csi) connector

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../../DISCLAIMER.md`](../../DISCLAIMER.md) first.

`key = "openshift"` &nbsp;•&nbsp; `name = "Red Hat OpenShift (Portworx)"` &nbsp;•&nbsp; `maturity = "ga"` — **validated end-to-end on hardware**: OCP 4.22 / k8s 1.35 single-node + FA-X20R3, Portworx px-csi 26.2.0 (operator → StorageCluster `Running` → `px-fa-direct-access` PVC bound, FA volume provisioned, and KubeVirt VM migration both directions vs Proxmox/XCP-ng/HPE/vSphere).

Manages Everpure storage on Red Hat OpenShift / Kubernetes using **Portworx
(`px-csi`, provisioner `pxd.portworx.com`)** — installed as the **Portworx
Operator** plus a **StorageCluster**. The StorageCluster comes from a spec the
customer generated in **Portworx Central** (`central.portworx.com`), supplied to
PHIF as a spec URL or pasted YAML, or — when none is supplied — from PHIF's
generated **FlashArray Direct Access (FADA)** spec.

> **Operator install method.** The Portworx Operator is installed from the
> **Portworx-hosted manifest** (`oc apply -f https://install.portworx.com/<ver>?comp=pxoperator&…`),
> NOT from OperatorHub. This is deliberate: the Portworx Operator is frequently
> **absent from a cluster's OperatorHub catalogs** (verified on a stock OCP 4.22
> lab — 513 operator packages, zero Portworx), so an OLM Subscription to
> `portworx-certified` fails to resolve (`ResolutionFailed: no operators found in
> package portworx-certified`) and never registers the CRD. The manifest install
> works regardless of OperatorHub contents. The manifest deploys the `px-operator`
> Deployment + RBAC; the operator then **registers the
> `storageclusters.core.libopenstorage.org` CRD at runtime**. The OLM
> OperatorGroup + Subscription remain only as a **fallback** for clusters whose
> OperatorHub genuinely carries the package.

> **The legacy Service Orchestrator (`pure-csi`) driver has been
> retired and removed.** That CSI no longer functions; the Helm-based PSO install,
> its `values.yaml`/`flasharray.sanType` generation, and the `driver` choice are
> all gone. Portworx is the only driver.

All cluster operations run `kubectl` / `oc` on the PHIF management host via
`ctx.runner.run_local(...)`. Authentication uses a **kubeconfig** supplied as a
target secret (or `oc login` with `api_url` + username/password); each method
writes it to a temp file and passes `--kubeconfig <file>` to the CLI. Manifests
are rendered in-code (`manifests.py`) and applied with `oc apply -f <tmpfile>`.

## The Portworx Central step (what the customer does)

Portworx Enterprise / PX-CSI installs are driven by a spec generated in **Portworx
Central**, which carries the customer's **license** and any console tuning — PHIF
cannot synthesize that. The flow:

1. The customer signs in to `central.portworx.com` → **Generate Spec** (or **Create
   New Spec → PX-CSI**), selects platform **Everpure FlashArray**, the Portworx
   version, OpenShift, and their options.
2. Central produces an install command of the form
   `oc apply -f '<spec-url>'` for the **StorageCluster** (it also offers a
   downloadable YAML), and a companion **operator** manifest URL
   (`…?comp=pxoperator&…`).
3. The customer pastes that **StorageCluster spec URL** (or the **YAML**) into
   PHIF's `deploy` action. PHIF installs the operator + the `px-pure-secret` and
   then applies **that** spec verbatim.

PHIF **derives the operator manifest URL from the StorageCluster spec URL**
automatically (same base + version, `comp=pxoperator`, carrying `kbver`/`ns` and
`osft=true`), so a single pasted spec URL is enough. The operator URL can also be
supplied explicitly via the **`px_operator_url`** deploy field (e.g. when only a
pasted YAML is provided, since a URL can't be derived from YAML).

This is **semi-automated**: PHIF automates everything around the spec (operator
install, FlashArray secret, namespace, CRD wait, optional FC pre-registration,
upgrade/teardown), but the licensed spec itself is generated in the console and
handed to PHIF. If the customer leaves both spec fields blank, PHIF applies its
own generated FADA StorageCluster instead (no console license tuning).

## Prerequisites

Verified end-to-end on OCP 4.22 (k8s 1.35) single-node + FA-X20R3, Portworx
px-csi 26.2.0. Have all of the following in place before running the wizard:

### Management host (PHIF backend)
- **`kubectl` and `oc`** on the management host (the backend image ships `oc`).
- **Cluster access**, one of:
  - a **kubeconfig with cluster-admin** (the operator install creates CRDs, RBAC,
    ClusterRoles, a DaemonSet), or
  - **`api_url` + username/password** for `oc login`. PHIF then promotes that login
    into a durable, least-privilege **ServiceAccount token** (`phif-operator`,
    scoped ClusterRole) and persists it as the kubeconfig for day-2 ops.
- Management-host egress to **`install.portworx.com`** and **`central.portworx.com`**
  (PHIF fetches the operator + StorageCluster specs from the appliance).

### FlashArray
- A FlashArray **associated with this hypervisor**. The connector derives the
  management endpoint (`ctx.array.endpoint`) and API token (`ctx.resolve_token()`,
  else a freshly minted one) to build the `px-pure-secret` — operators do **not**
  type these. No associated array ⇒ `deploy` fails with a clear message.
- A FlashArray **iSCSI (or FC) data interface reachable from the worker nodes'
  storage NIC subnet** (for example: array data port `192.0.2.70`, node storage NIC `ens19`).

### Portworx Central (license / spec)
- A **Portworx Central account** (`central.portworx.com`) to generate the **PX-CSI
  StorageCluster spec** (platform **Everpure FlashArray**, OpenShift, your version).
  This carries the **license** and tuning PHIF can't synthesize. Paste its **spec
  URL** (or YAML) into the `deploy` action; PHIF derives the operator manifest URL
  from it. (Leaving it blank uses PHIF's generated FADA spec — unlicensed/basic.)

### Cluster: networking & DNS (IMPORTANT — easy to get wrong)
- **Pod egress + correct in-cluster DNS.** The px-operator (running as a *pod*)
  fetches its version manifest from `https://install.portworx.com`, and the console
  / components resolve in-cluster `*.svc.cluster.local` service names. If the
  cluster has an **over-broad wildcard DNS** (e.g. `*.<cluster-base-domain>` → the
  ingress router) combined with Kubernetes' default `ndots:5`, a pod appends the
  search domain and resolves names like `install.portworx.com` **or** a
  `…svc.cluster.local` service to the router (wrong TLS cert / connection refused).
  Symptoms: the px StorageCluster stays **Degraded** with no px pods, and console
  plugins fail to load.
  - **Fix:** scope the wildcard to **`*.apps.<base-domain>`** so it doesn't shadow
    external names or in-cluster service FQDNs. Verify from a pod (not the node —
    the node uses `ndots:1` and resolves correctly):
    `oc debug -n openshift-cnv --image=registry.redhat.io/rhel9/support-tools -- \
    getent hosts install.portworx.com` → public IP, not the router. After fixing
    upstream DNS, **flush the cluster cache**: `oc -n openshift-dns rollout restart
    daemonset/dns-default` (CoreDNS caches the stale wildcard answer), then restart
    any affected workloads (e.g. `oc -n openshift-console rollout restart
    deployment/console`).
- **Worker nodes must be able to pull the px-csi images** (`portworx/px-operator`,
  `portworx/px-pure-csi-driver`, …) — from the internet or a mirror registry the
  nodes can reach. (Image pulls happen on the node, which usually resolves
  correctly; only pod-level DNS hit the issue above.)

### Cluster: operators & node config
- **Kubernetes-NMState operator** *(recommended)* — installs the
  `NodeNetworkState` (`nns`) CRD that PHIF uses to **auto-discover node storage
  NICs** for the interface-binding form. **Optional but recommended:** without it
  PHIF falls back to reading `/sys/class/net` via a short-lived **`oc debug
  node`** pod (which requires debug pods to be schedulable); if neither works you
  type the NIC name manually.
- **Portworx Operator does NOT need to be in OperatorHub.** PHIF installs it from
  the `comp=pxoperator` **manifest**, so a cluster whose OperatorHub lacks Portworx
  (the common case — verified: 513 packages, zero Portworx on a stock OCP 4.22) is
  fully supported. The OLM Subscription path is only a fallback.
- **`/etc/multipath.conf` + multipathd + iscsid on the nodes.** The px-csi
  **node-plugin fatally requires `/etc/multipath.conf`** at startup, and RHCOS
  ships neither that file nor enabled `multipathd`/`iscsid`. PHIF's
  **`configure_binding`** MachineConfig writes them (plus the iSCSI iface binding
  and ARP sysctls). **Applying it triggers a rolling reboot of the nodes** (on a
  single-node cluster, a full control-plane reboot). The wizard runs
  `configure_binding` when you select storage NICs.
  - **Single-node OpenShift:** set **`machine_config_role = master`** — the SNO
    node is in the *master* MachineConfigPool, so a `worker` MachineConfig won't
    apply (and you'll see no reboot / no `/etc/multipath.conf`).

## Target schema (Add hypervisor)

| Field | Type | Default | Notes |
|---|---|---|---|
| `kubeconfig` | text (secret-ish) | — | Full kubeconfig; written to a temp file per call. Provide this OR `api_url`+`username`+`password`. |
| `api_url` / `username` / `password` / `insecure_skip_tls_verify` | string/secret/bool | — | `oc login` path when no kubeconfig is supplied. |
| `namespace` | string | `portworx` | Namespace Portworx is installed into. |
| `protocol` | enum | `iscsi` | FlashArray transport: `iscsi`, `fc`, `nvme-tcp`. `fc` optionally pre-registers worker-node HBAs (see `node_wwns`). |
| `node_wwns` | string | — | **Optional.** Comma-separated FC HBA WWNs of the worker nodes to *pre-register* as FlashArray hosts when `protocol=fc`. Not required: Portworx auto-registers worker nodes at attach time; only set this for pre-zoning. |

There is **no `driver` field** — Portworx is the only driver. The **FlashArray
endpoint + API token** are intentionally **not** target fields; they come from the
associated array (`ctx.array.endpoint` / `ctx.resolve_token()`), matching the
consistent PHIF UX. `deploy` / `rotate_credentials` accept optional
`array_endpoint` / `api_token` keyword overrides for advanced use, but the UI
never prompts.

## Capabilities & actions

| Capability | Action id | What it does |
|---|---|---|
| DEPLOY_PLUGIN | `deploy` | Install Portworx: Namespace → **Portworx Operator** (from the `comp=pxoperator` manifest URL — explicit `px_operator_url`, else derived from the spec URL; OLM Subscription only as fallback) → **poll until the `storageclusters.core.libopenstorage.org` CRD is registered + Established** → `px-pure-secret` → **StorageCluster** (from `px_spec_url` / `px_spec_yaml`, else PHIF's generated FADA spec). Adopts an existing Portworx install if one is present. |
| CONFIGURE | `configure` | `oc apply` a StorageClass (`backend: pure_block` FADA) + VolumeSnapshotClass for `pxd.portworx.com`. Reuses existing Portworx StorageClasses unless `force`. |
| CONFIGURE | `configure_binding` | Bind the storage data path to specific node interfaces by `oc apply`-ing a **MachineConfig** (see below). |
| PROVISION_VOLUME | `provision` | `oc apply` a PersistentVolumeClaim. |
| SNAPSHOT | `snapshot` | `oc apply` a `VolumeSnapshot` CR of a source PVC. |
| CLONE | `clone` | `oc apply` a PVC with a `dataSource` (PVC or VolumeSnapshot). |
| RESIZE | `resize` | `kubectl patch pvc … storage=<size>` (SC must allow expansion). |
| ROTATE_CREDENTIALS | `rotate_credentials` | Re-apply the `px-pure-secret` with a new / freshly minted FlashArray token. |
| UPGRADE | `upgrade` | `kubectl patch storagecluster … spec.image=portworx/oci-monitor:<version>`; the operator performs the rolling upgrade. |
| HEALTH | `health_check` | `oc get pods -n <ns>` + `oc get csidrivers` + array info. |
| REMOVE | `teardown` | Delete the StorageCluster, then the Portworx operator Subscription. |

`CONNECT` (validate_connection) runs `kubectl version` and `oc whoami` against the
temp kubeconfig and, if a FlashArray is associated, confirms `ctx.array.info()`.

**Supported protocols:** iSCSI, FC, NVMe-TCP.

### `deploy` fields

| Field | Type | Notes |
|---|---|---|
| `release` | string (`px-cluster`) | StorageCluster name (generated spec only; a Central spec names itself). |
| `cluster_id` | string | Unique id tagged onto this cluster's resources. |
| `chart_version` | string | Portworx image version (generated spec only). |
| `fa_direct_access` | bool (`true`) | Generated spec only: FADA (1 PVC = 1 FA volume, no pooled SDS) vs Enterprise cloud-drive pooling. Ignored when a Central spec is supplied. |
| `px_spec_url` | string | **Portworx Central StorageCluster spec URL** — applied verbatim with `oc apply -f <url>`. |
| `px_spec_yaml` | text | Pasted Central StorageCluster YAML — applied verbatim. Wins over the URL if both are given. |
| `px_operator_url` | string | **Portworx Operator manifest URL** (`comp=pxoperator`). Optional — derived from `px_spec_url` when blank. Used when the operator isn't in OperatorHub (the common case). |

### `px-pure-secret`

Portworx reads the FlashArray credential from a `px-pure-secret` whose `pure.json`
is:

```json
{ "FlashArrays": [ { "MgmtEndPoint": "<array_endpoint>", "APIToken": "<api_token>" } ] }
```

`MgmtEndPoint` is filled from the associated array and `APIToken` from
`ctx.resolve_token()` (else a minted token); the token lives in the secret's
`stringData` (server-side), never on a command line, so it stays out of the job
log. `rotate_credentials` simply re-applies this secret with a new token.

## Fibre Channel (FC)

Selecting `protocol = fc`:

- **Portworx auto-registers worker nodes.** Like the OpenStack Cinder driver,
  Portworx (px-csi) registers the worker nodes as FlashArray hosts (host objects
  by initiator) at volume-attach time, so manual node-WWN registration is
  **optional**, and FC `deploy` never fails for missing WWNs.
- **Manual host registration by WWN is an optional pre-stage.** When `protocol=fc`
  **and** `node_wwns` is supplied, `deploy` pre-registers the worker-node HBAs:
  a host group `<cluster_id>-ocp` + host `<cluster_id>-ocp-h1` carrying the WWNs
  via `ctx.array.create_host(..., wwns=[...])`. Calls are **idempotent + additive**
  (re-running is safe; WWNs aren't duplicated). When `node_wwns` is empty/absent
  (or no array is associated), the connector logs a clean skip and lets Portworx
  auto-register. `dry_run` / mock make no array changes.
- **Fabric zoning** may still be needed switch-side; pre-registering WWNs is useful
  when the fabric is zoned by WWN ahead of the first attach.

## Interface binding via MachineConfig

OpenShift worker nodes run **RHCOS** and are configured **declaratively** — the
Machine Config Operator (MCO) owns the node; operators do **not** SSH nodes. So
`configure_binding` renders a **MachineConfig**
(`machineconfiguration.openshift.io/v1`) labelled for a machine-config-pool role
and applies it with `oc apply -f`. The MCO rolls the embedded Ignition out to
every node carrying that role label (draining / rebooting as needed).

### Action fields (`configure_binding`)

| Field | Type | Source | Notes |
|---|---|---|---|
| `iscsi_nics` | multiselect | `nics` | NICs to bind iSCSI ifaces to. |
| `nvme_sources` | multiselect | `nvme_sources` | Host source interfaces/addresses (`host-traddr`) for NVMe-TCP. |
| `nvme_options` | string | — | Extra args passed verbatim to `nvme connect`. |
| `fc_hbas` | multiselect | `fc_hbas` | FC HBAs to record (zoning is switch-side). |
| `machine_config_role` | enum (`worker`/`master`) | — | MCP role label the MachineConfig targets. Default `worker`. |

All fields are optional; an empty selection still renders a valid (no-op)
MachineConfig. The multiselect fields are populated via the connector's
`discover_options(kind)`, which attempts a best-effort live `oc` query (NMState
`NodeNetworkState` for NICs) and otherwise returns synthetic options (mock /
dry-run).

### What the Ignition writes

- **iSCSI** — one iscsiadm iface file per selected NIC at
  `/etc/iscsi/ifaces/phif_<nic>` (`iface.net_ifacename = <nic>`), enables
  `iscsid.service`, **and writes the ARP-flux sysctl drop-in**
  `/etc/sysctl.d/99-phif-iscsi-arp.conf` with `net.ipv4.conf.<nic>.arp_ignore = 2`
  and `arp_announce = 2` per selected NIC. This is the **same ARP-flux fix the
  SSH-driven Linux connectors apply** (Proxmox / XCP-ng / HPE VME / OpenStack via
  `phif.connectors.iscsi_net.arp_flux_cmd`) — delivered declaratively here so the
  node, with multiple storage NICs on one subnet, doesn't answer/announce ARP on
  the wrong interface and bind iSCSI sessions to the wrong path. systemd-sysctl
  applies the drop-in on the node (the MCO reboots the node rolling the change
  out). The drop-in is **gated on `iscsi_nics`** — NVMe-TCP and FC don't need it.
- **Multipath (iSCSI + FC)** — writes the Everpure FlashArray `/etc/multipath.conf`
  (ALUA, `group_by_prio`, `find_multipaths no`) and enables `multipathd.service`.
  Skipped for NVMe-only (native NVMe multipath).
- **NVMe-TCP** — `/etc/nvme/phif-connect.conf` (one `host-traddr` per source) + a
  oneshot `phif-nvme-connect.service` running `nvme connect-all -w <source>
  <nvme_options>` per source.
- **FC** — HBA selection is **zoning-driven** (switch-side), so **no node file is
  written**; the selected HBAs are recorded as a MachineConfig annotation
  (`phif.purestorage.com/fc-hbas`) for auditability only.

YAML generation is a pure function
(`manifests.interface_binding_machineconfig_yaml(...)`), unit-tested in isolation.
`ctx.dry_run` renders + prints the MachineConfig but applies nothing.

> `# TODO(doc-validate):` confirm the exact on-RHCOS paths/units against Everpure +
> RHCOS docs before treating these as authoritative (iscsiadm iface files under
> `/etc/iscsi/ifaces/`; the sysctl.d ARP drop-in; `nvme-cli` presence + canonical
> `/etc/nvme/*` paths; Ignition `3.2.0` for OCP 4.6+).

## Cluster awareness

OpenShift is **already a cluster-level connector**: the Portworx install
(`deploy`), the StorageClass (`configure`), and the interface-binding MachineConfig
(`configure_binding`) all apply to the **whole cluster** in a single operation,
and Portworx auto-registers worker nodes as FlashArray hosts at attach time. So
there is **no per-node fan-out**.

- **`wizard_steps()` → `["deploy", "configure"]`** (omits `register_hosts` /
  `setup_connectivity` — handled cluster-wide).
- **`list_nodes()`** discovers worker nodes via `oc get nodes -l
  node-role.kubernetes.io/worker -o json`. Mock / dry-run synthesizes a 3-worker
  cluster; a reachable-but-unparseable result yields a single logical `cluster`
  node.
- **`validate_cluster()`** best-effort checks that all worker nodes expose the
  same storage NICs (so an iSCSI/NVMe MachineConfig binding is valid
  cluster-wide), via NMState `NodeNetworkState`. When discovery isn't feasible it
  returns `ok` with a note (advisory — never blocks deploy).

## ServiceAccount promotion

When connecting with `api_url` + username/password, `validate_connection` can
promote the login into a durable, **least-privilege** ServiceAccount token
(`phif-operator` in `kube-system`, bound to a scoped ClusterRole — **not**
cluster-admin). The ClusterRole is enumerated for exactly what PHIF touches:
Portworx install (apps/core resources, CRDs, RBAC `bind`+`escalate`, the OLM
`operators.coreos.com` objects, and the `core.libopenstorage.org` StorageCluster),
storage day-2 (StorageClass / snapshot CRs / PVC lifecycle), and the
interface-binding MachineConfig (+ read-only nodes / NMState). The minted
kubeconfig is persisted so subsequent day-2 ops use the durable token.

## Dry-run

Every mutating action honours `ctx.dry_run`: it streams a `[dry-run]` plan line
and returns success without executing any `oc`/`kubectl` command.

## TODOs / validation

- `# TODO(doc-validate):` confirm against the current Portworx-on-OpenShift install
  guide: operator package/channel/catalog (`portworx-certified` in
  `certified-operators`; OCP 4.20+ surfaces it under *Ecosystem → Software
  Catalog*, but the OLM Subscription still applies), the StorageCluster apiVersion
  (`core.libopenstorage.org/v1`) and the FADA shape (KVDB device when no
  cloudStorage), the default px image tag, and the FADA StorageClass parameter set.
- The generated FADA StorageCluster is a fallback for clusters without a Central
  spec; the **Central spec URL/YAML is the recommended path** (license + tuning).

## Testing

```bash
cd backend
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest tests/test_openshift.py -q
```

All tests use the `make_context` fixture (mock FlashArray + mock JobRunner); no
real cluster or array is contacted.


## VM migration (KubeVirt VMs ↔ FlashArray)

OpenShift is a migration **source and destination** for the cross-hypervisor
migration framework (`MIGRATE` / `VM_INVENTORY` / `VM_LIFECYCLE` capabilities;
`openshift` is in the Migration UI's connector set). The model mirrors OpenStack:
**each KubeVirt VM disk is a px-csi (FADA) PVC = one FlashArray volume**, so the
orchestrator can FlashArray-copy data into/out of it. `vm_ref` is `"<namespace>/<name>"`;
the namespace comes from the `vm_namespace` target field (default `default`).

**px-csi FADA volume naming (the mapping):** a bound PVC's backing FlashArray
volume is `px_<clusterUid[:8]>-<pvName>`, where `clusterUid` is the Portworx
StorageCluster `status.clusterUid` and `pvName` is the PVC's `spec.volumeName`
(`pvc-<uid>`). PHIF derives this name and resolves the serial/size via the
FlashArray API.

**As a destination** (`X → OpenShift`):
* `create_vm` creates, per source disk, a **`ReadWriteMany` + `volumeMode: Block`**
  PVC on the `px-fa-direct-access` StorageClass (RWX block is required for KubeVirt
  LiveMigration/HA + node mobility; Block is required because the migration copy
  writes a whole-disk image with its own GPT/filesystems — a Filesystem PVC would
  fail px-csi `NodeStageVolume`), then a stopped `VirtualMachine` CR referencing
  them (inline `cpu.cores` + `memory.guest`, NIC MAC preserved on the pod network
  or a Multus NAD, `bootOrder` on the boot disk; UEFI → `firmware.bootloader.efi`
  with `secureBoot: false` unless secure boot is requested, in which case
  `features.smm` is added).
* `create_managed_disk` returns each PVC's FA volume for the orchestrator's
  copy-with-overwrite; `start_vm` boots the now-populated VM.

**As a source** (`OpenShift → X`):
* `capture_vm_spec` reads the `VirtualMachine` CR — vCPU/RAM (inline **or** resolved
  from a referenced instancetype), firmware, each disk's DataVolume/PVC → PV →
  FADA FA volume + serial, and NIC MACs (from the running VMI's status). Disks not
  backed by a PVC (cloud-init, containerDisk) are skipped.
* `stop_vm` / `start_vm` honor whichever lifecycle field the VM uses (`running` vs
  `runStrategy`).

**Validated end-to-end on hardware (OCP 4.22 single-node + FA-X20R3, copy mode,
boot-confirmed)** in **both directions** against Proxmox, XCP-ng, HPE VME, and
vSphere (VMFS and RDM sources). See the [Migration](../../README.md#vm-migration)
section for the full flow, modes (copy/move), and rollback behavior.
