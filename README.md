# PHIF — Universal Hypervisor Integration Framework

> ## ⚠️ Not a supported product — use at your own risk
>
> **PHIF is an independent, experimental project.** It is **not** a supported
> product, it is **not** covered by any support agreement, warranty, or SLA, and
> **support cases will not be accepted for it.**
>
> PHIF actively changes storage and hypervisor state: it creates and destroys
> array volumes, rewrites host multipath and iSCSI configuration over SSH,
> applies Kubernetes operators, and powers off, creates, and deletes VMs. It can
> cause **irreversible data loss or downtime**. The bundled Proxmox, XCP-ng, and
> HPE VME storage plugins are **custom, unofficial, uncertified** integrations.
>
> **Run it in a lab, against data you can afford to lose, and start with
> `PHIF_MOCK_MODE=1`.** Please read **[DISCLAIMER.md](DISCLAIMER.md)** before you
> point this at anything you care about.

One framework + web UI to connect a FlashArray, mint the API keys integrations
need, attach hypervisors, deploy the right storage integration onto each, and run
day-2 storage operations — across **vSphere, OpenShift, OpenStack, Proxmox,
XCP-ng, and HPE VM Essentials**.

PHIF does not reinvent the underlying integrations. It **automates the deployment
and configuration** of them (vSphere plugin + VASA, the Kubernetes/OpenShift CSI
driver, the OpenStack Cinder driver, the Proxmox storage path, XCP-ng SRs, and the
HPE VME path) and exposes the operations each supports behind one common,
capability-driven interface.

## Architecture

```
┌──────────────┐     ┌─────────────────────────────────────────────┐
│  React SPA   │────▶│  FastAPI backend                            │
│ (capability- │ ws  │  • FlashArray client (+ API-token minting)  │
│  driven UI)  │◀────│  • Connector registry (auto-discovery)      │
└──────────────┘     │  • Job engine (Ansible / SSH / HTTP + logs) │
                     │  • Encrypted secrets vault                  │
                     │  • Connectors: vsphere, openshift, openstack│
                     │    proxmox, xcpng, hpevme (+ example)       │
                     └─────────────────────────────────────────────┘
                                  │ REST / SSH / Ansible / k8s API
                     ┌────────────┴────────────┐
                     ▼                          ▼
              Everpure FlashArray            Hypervisor targets
```

* **Plugin contract** — `backend/phif/connectors/base.py` defines
  `HypervisorConnector`, the `Capability` vocabulary, and the UI form schemas.
  Each hypervisor implements the subset of operations it supports; the UI enables
  actions from the declared capabilities. See `docs/CONNECTOR_GUIDE.md`.
* **Shared services** — a tested FlashArray REST client (volumes, snapshots,
  hosts, QoS, protection groups, and **API-token generation**), a job engine that
  runs Ansible/SSH/HTTP work with live log streaming, and a Fernet-encrypted
  secrets vault.

## Quick start (Docker Compose)

```bash
# 1. Generate a vault key (written to .env)
echo "PHIF_VAULT_MASTER_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")" > .env

# 2. Generate a TLS cert/key into ./certs (pass the host IP/DNS for the SAN)
deploy/gen-certs.sh <host-ip-or-dns>      # e.g. deploy/gen-certs.sh 192.0.2.50

# 3. (Optional) demo without hardware
echo "PHIF_MOCK_MODE=1" >> .env

# 4. Run
docker compose up --build -d
```

Both the UI and API are served over **TLS**:

* Web UI: `https://<host>` (also `https://<host>:8443`); plain HTTP on `:80` redirects to HTTPS.
* API: `https://<host>:8000` (`/healthz`, OpenAPI at `/docs`) — also reachable, TLS-terminated, via the UI's `/api` proxy.

The self-signed cert from `gen-certs.sh` will trigger a browser trust warning; supply a CA-signed cert as `certs/tls.crt` + `certs/tls.key` for production.

## Quick start (Kubernetes / Helm)

```bash
helm install phif deploy/helm/phif \
  --set vaultMasterKey=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
# expose via --set ingress.enabled=true --set ingress.host=phif.example.com
```

## The six workflows

1. **FlashArrays** — connect an array (validated, then stored encrypted).
2. **API Keys** — mint/rotate FlashArray API tokens for integrations that need one.
3. **Hypervisors** — connect a hypervisor and install its Everpure integration.
4. **Operations** — capability-driven day-2 actions (provision, snapshot, clone,
   resize, QoS, replication, upgrade, rotate, health, remove) with live job logs.
5. **Migrations** — cold-cutover VM migrations between any two supported hypervisors
   that share the same FlashArray. No data is copied — disks are already FA volumes,
   so the orchestrator simply unmaps from the source host group and maps to the
   destination. See [Migration](#vm-migration) for details.
6. **Jobs** — history and streamed logs of every operation.

## Development

```bash
# Backend
cd backend
python -m venv .venv && .venv/Scripts/pip install ".[dev]"
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest -q
PHIF_MOCK_MODE=1 .venv/Scripts/python -m uvicorn phif.main:app --reload

# Frontend
cd frontend && npm install && npm run dev
```

`PHIF_MOCK_MODE=1` stubs all array/hypervisor I/O so the entire flow is
exercisable without real infrastructure.

## Connector status

`maturity` (declared per connector and surfaced as a UI badge) is **`ga`**,
`preview`, or `scaffold`.

| Connector | Maturity | Model | Hardware-validated |
|---|---|---|---|
| **Proxmox** | `ga` | Custom PVE storage plugin (`purefa`): one FA volume per disk, array snapshots/clones | ✅ live 2-node PVE cluster |
| **XCP-ng** | `ga` | Custom **SMAPIv3** driver (volume + datapath + host plugin): one FA volume per VDI, array snapshots/clones | ✅ live 2-host pool, XCP-ng 8.3 / xapi 25.6 |
| **HPE VME** | `ga` | Native **Morpheus/VME storage plugin** (Java/Groovy under `connectors/hpevme/files/morpheus-plugin/`): per-VM-disk FA volumes, array-offloaded snap/clone/resize, VME-native libvirt attach via `MvmProvisionFacet`. PHIF connector uploads the plugin (`deploy`) + registers the storage server (`configure`). | ✅ live VME appliance: provision, image deploy, clone-from-VM, snapshot create/revert/delete |
| vSphere | `ga` | vSphere Client plugin + VASA/vVols, FlashArray REST, `purestorage.flasharray` | ⚠️ **migration** validated on live vCenter (VMFS + RDM source, both directions vs OpenShift); plugin/VASA deploy not yet hardware-validated |
| **OpenShift** | `ga` | **Portworx (px-csi)** via the Portworx Operator (manifest) + StorageCluster (Portworx Central spec or generated FADA); PSO retired | ✅ **live OCP 4.22 single-node + FA-X20R3**: Portworx install, PVC provision, and **migration both directions** (boots) vs Proxmox/XCP-ng/HPE/vSphere |
| OpenStack | `ga` | Cinder driver (PureISCSI/FC/NVME) | ✅ Pure-Cinder deploy validated end-to-end on a live controller (deploy → configure → provision) |

Proxmox, XCP-ng, and HPE VME follow the CSI/Cinder "storage plugin" model — each VM disk is
its own FlashArray volume presented directly to the VM (no LVM), with snapshots and
clones performed **on the array**.

### What's tested and working

**Backend / framework** — the suite is **753 tests, of which 714 pass** in mock
mode (`PHIF_MOCK_MODE=1`), covering the connector registry, FlashArray client +
API-token reuse, job engine, vault, cluster discovery/validation, the deployment
wizard (prepare → discover nodes/interfaces → run), and per-connector action
schemas. Deployed over TLS via Docker Compose.

> **39 tests currently fail** — all of them in `tests/test_vsphere.py` (22),
> `tests/test_openstack.py` (16), and `tests/test_xcpng.py` (1). These are stale
> *tests*, not known-broken features: the vSphere and OpenStack connectors gained
> behaviour (a `setup_connectivity` action, a `sudo=` runner kwarg, maturity
> bumps) that their tests were never updated for, and the vSphere tests attempt
> real network calls instead of being fully mock-isolated. See
> [Known issues](#known-issues).

**Proxmox** — end-to-end on a live 2-node cluster: cluster-wide plugin deploy,
`purefa` storage definition (created disabled until fully configured), host-group
registration on the array, iSCSI NIC-binding connectivity + Everpure ALUA multipath,
per-disk volume provision/snapshot/clone/resize, and cluster-aware teardown. FC
supported. Volume naming avoids pending-eradication name reuse.

**XCP-ng (SMAPIv3, validated on XCP-ng 8.3)** — the per-VDI design works
end-to-end on a live 2-host pool:

* Pool discovery; cluster-wide install of the **volume plugin**
  (`org.xen.xapi.storage.purefa`), the custom **`purefa` datapath plugin**, and the
  **`purefa-mpath` host plugin**; plugin registration (`xe sm-list`); `sr-create`
  + PBD plug on every host.
* iSCSI login + multipath on every host (Everpure ALUA stanza,
  `find_multipaths no`, `user_friendly_names no` so devices are always WWID-named).
* **VDI create** → one FlashArray volume per VDI, presented as a raw multipath
  block device; **VM power-on** boots via the datapath (tapdisk/`vbd3`).
* **VDI delete** → array volume disconnected + destroyed; the dm-multipath map is
  flushed **pool-wide** (host plugin) and per-host on detach. Idempotent.
* **Clone** (`VDI.clone`) → array-native FlashArray volume copy (offloaded).
* **Snapshot** (`VDI.snapshot`) → real FlashArray volume-snapshot (thin,
  offloaded); deleting the snapshot VDI destroys it on the array (no leak).
  Clone-from-snapshot materialises a snapshot into an attachable volume.
* Wizard / "clobber existing SR" / per-host array-reachability preflight; teardown
  forgets all SRs and removes the plugins pool-wide.

> Note: a snapshot VDI isn't directly attachable to a running VM (FlashArray
> snapshots can't be host-connected) — clone it first to use its data; XAPI does
> this automatically for "new VM from snapshot". XCP-ng 8.3 does not load
> dropped-in SMAPIv1 drivers, so the integration ships only the SMAPIv3 plugin
> under `connectors/xcpng/files/smapiv3/` (`org.xen.xapi.storage.purefa`).

**HPE VM Essentials (validated on a live VME appliance)** — the native
Morpheus/VME storage plugin (`connectors/hpevme/files/morpheus-plugin/`) delivers
the per-VM-disk block model end-to-end:

> **Host prerequisite:** the KVM host must be reachable over SSH from the PHIF
> container and the SSH user must have **passwordless `sudo`**. PHIF uses SSH to
> enable `iscsid`, perform iSCSI discovery/login, and write the multipath
> configuration. No other direct host configuration is required — all other
> operations go through the VME Manager API.

* PHIF uploads the compiled plugin JAR via the VME Manager API (`deploy`) and
  registers the FlashArray as a VME storage server (`configure`).
* **Provision** → one FA volume per VM disk, presented to the KVM host group and
  attached by VME via `MvmProvisionFacet` as a raw `/dev/mapper/<wwid>` device.
* **Image deploy** (`getImageTargetCapable`) and **clone-from-running-VM** →
  `qemu-img convert` of the source image/disk onto the raw FA device so the disk
  boots; min-volume floor and device-assembly waits handle the array/host race.
* **Snapshot create / revert / delete** → real FlashArray volume-snapshots routed
  through VME's `SnapshotFacet` (per-volume `SnapshotFile`s), so revert and delete
  act on the array and leave nothing behind.
* **Cluster membership** (`assess_cluster` / `reconcile_cluster`) matches FA hosts
  by every plausible name (bare hostname, mgmt IP, or the `{group}-{node}`
  convention) so existing hosts aren't mis-flagged as both new and departed.
* VM/disk delete cleans the array volume + snapshots and the host multipath maps
  cluster-wide.

## VM Migration

PHIF supports **cold-cutover VM migration** between Proxmox, XCP-ng, HPE VME,
**OpenShift Virtualization (KubeVirt)**, and VMware vSphere where both hypervisors
share the same FlashArray. Every VM disk is a dedicated FlashArray volume on all
supported hypervisors (for OpenShift, a Portworx px-csi **FADA** PVC = one FA
volume; for vSphere VMFS, PHIF first clones the VMDK onto a per-disk FA volume —
see below). The orchestrator:

1. Captures the source VM's logical spec (vCPU, RAM, firmware, per-disk FA volume + serial, NIC MACs).
2. Creates a matching VM on the destination and, **through its storage plugin**, a managed disk (its own FA volume) per source disk.
3. **Copies** each source volume's data onto the destination volume with a FlashArray copy-with-overwrite (instant + thin, same-array; replication for cross-array). The **source volume is only ever read.**
4. Maps NICs to the chosen destination networks (MAC addresses preserved), sets boot order, and boots the destination VM.

Two modes: **copy** (default) leaves the source VM fully intact (crash-consistent,
or cleanly shut down with `shutdown_source`); **move** powers the source off first
and removes it (+ deletes its source volumes) only after the destination is
confirmed running. On failure the migration **rolls back** — the half-built
destination VM is stopped and deleted and its new volumes freed; the source is
never modified in copy mode.

**Validated end-to-end on hardware (same-array, boot-confirmed):** OpenShift ↔
Proxmox, OpenShift ↔ XCP-ng, OpenShift ↔ HPE VME, and vSphere ↔ OpenShift (VMFS
and RDM sources). vSphere is deferred for X↔X in favor of VMware's own MTV.

### Migration wizard options

| Option | Description |
|---|---|
| **Source hypervisor** | Any Proxmox, XCP-ng, HPE VME, OpenShift, or VMware vSphere connection with the `MIGRATE` capability |
| **VM** | A VM whose disks are all FA-backed — Everpure FA volumes, OpenShift px-csi FADA PVCs, or (vSphere) VMFS / RDM disks on the shared array. Non-FA disks cause a clear preflight error. |
| **Destination hypervisor** | Must share the same FlashArray as the source |
| **Destination cluster** | For HPE VME: which Morpheus group (cluster) to place the VM in. Only clusters with an Everpure storage connection are shown. |
| **Destination storage** | For HPE VME: which Everpure-backed datastore to use. For XCP-ng: which `purefa` SR to use. For OpenShift: the px-csi FADA StorageClass (`px-fa-direct-access`) and target VM namespace (`vm_namespace`); each disk becomes a RWX **Block** PVC = one FA volume. |
| **NIC mapping** | Each source NIC is mapped to a destination network; MAC addresses are preserved. |
| **Convert disks to VMFS VMDKs** | *(VMware vSphere destination only, optional, default off)* Convert the destination disks from RDMs to native VMFS VMDKs after the copy. See [vSphere disk handling](#vsphere-disk-handling-vmfs--rdm--vvol). |

### vSphere disk handling (VMFS / RDM / vVol)

VMware vSphere is supported as both a migration **source** and **destination**. Unlike
the other hypervisors (where every disk is already its own FA volume), vSphere disks
need special handling because a VMFS-resident VMDK is a *file* inside a shared VMFS
datastore, not a 1:1 FA volume.

**vSphere as a source:**

* **RDM** disks already map 1:1 to an FA volume and are migrated directly: PHIF reads the
  RDM's ESXi device id (either `naa.624a9370<serial>` or the `vml.…624a9370<serial>…`
  form), resolves the 24-hex serial against the **connected** FlashArray, and uses that
  volume. A disk (RDM or vVol) whose serial does **not** resolve to a volume on the
  connected array (a non-Everpure RDM, or an Everpure volume on a different array) is rejected
  with a clear preflight error. *(Native vVol-source resolution is not yet implemented —
  a vVol-backed VM is currently rejected as unmappable rather than silently failing.)*
* **VMFS** disks have no per-disk FA volume. PHIF provisions a new FA volume per disk,
  presents it to the ESXi host as a raw device mapping (RDM), and clones the VMDK's
  data onto it with `vmkfstools` (VAAI/XCOPY-accelerated on the array). For a *copy*
  with the source left running, this is done from a temporary VM snapshot so the base
  disk is consistent. The temporary clone volumes are cleaned up automatically on both
  completion and rollback.
* The clone runs over SSH using a **temporary local ESXi account** that PHIF creates
  and removes itself (every other operation uses the vCenter connection); no pre-shared
  root password is needed. Optional `esxi_user` / `esxi_password` connection fields
  override this.

**vSphere as a destination — disk format:**

* **Default (RDM, zero host-side copy):** each destination disk is attached as a
  **virtual-mode RDM** backed by a per-disk FA volume. The only data movement is the
  array-side volume copy — **no data crosses the host network**. This is the
  recommended default.
* **Optional — convert to native VMFS VMDKs:** enable *Convert disks to VMFS VMDKs* to
  end up with ordinary VMDKs on a FlashArray-backed VMFS datastore (and free the
  temporary RDM volumes). This is a **one-time host-side copy** during the migration —
  a raw-LUN→VMFS copy cannot be offloaded to the array via XCOPY, so the bytes move
  through the ESXi host's datamover. The VM is offline (cold) during the conversion.
  PHIF performs it with a whole-VM Storage vMotion and verifies each disk converted
  before freeing any volume.

**Scratch datastore (conversion only):** to convert RDMs to VMDKs, PHIF self-provisions
a **small temporary FA-backed VMFS scratch datastore** to hold the VM's home + RDM
pointer files (a same-datastore relocate would not convert; the pointers must move to a
different datastore, and the converted VMDKs land on the target VMFS — *not* the
scratch). The scratch is a **fixed 16 GiB** — an RDM pointer file is tiny regardless of
the mapped LUN size (a 2 TB vRDM pointer fits on a small datastore), so the scratch does
**not** need to match the disk sizes. The scratch datastore and its FA volume are torn
down automatically after the conversion (and on rollback).

### Requirements

* Source and destination hypervisors must be associated with the **same FlashArray** in PHIF.
* All source VM disks must be Everpure FA volumes — non-Everpure disks are rejected at preflight.
* **HPE VME destination:** the KVM host must have SSH access with passwordless sudo (same requirement as the HPE connector generally; see above).
* **XCP-ng destination:** the `purefa` SMAPIv3 SR must be configured and running on the pool.
* **Proxmox destination:** the `purefa` storage definition must exist in the cluster.
* **VMware vSphere destination:** at least one **FlashArray-backed VMFS datastore** must
  exist on the target host/cluster — it hosts the VM's home/config files, and (if
  *Convert disks to VMFS VMDKs* is enabled) the final VMDKs and the temporary scratch
  datastore. RDM mapping pointer files live on it as well.
* The source VM is **not** required to be powered off before starting — PHIF powers it off as part of the migration.

### What migration does not do

* Copy data, **except**: a VMFS source disk is cloned onto an FA volume (`vmkfstools`,
  array-offloaded), and an optional vSphere-destination *convert to VMFS* is a one-time
  host-side copy. For all other cases only FA volume map/unmap (or array-side
  copy-with-overwrite for *copy* mode) is performed — no host data movement.
* Migrate live (hot) VMs — this is a cold cutover only.
* Transfer UEFI NVRAM / efivars — a fresh efidisk is created on UEFI destinations.
* Move non-Everpure disks — only FA-backed volumes (or vSphere VMFS/vVol/RDM disks) are supported.
* Replication — source and destination must share the same physical array.

## Known issues

Being an experimental project, PHIF has rough edges that are documented rather
than hidden. Please read these before filing an issue.

* **39 of 753 tests fail** (`tests/test_vsphere.py`, `tests/test_openstack.py`,
  `tests/test_xcpng.py`). The tests have drifted behind the connectors they
  cover — stale assertions (`maturity`, action lists), a runner signature that
  gained a `sudo=` kwarg, and vSphere tests that resolve real hostnames instead
  of being mock-isolated. The affected *features* are exercised on hardware; it
  is the test fixtures that need updating. Contributions welcome.
* **Connector maturity is declared inconsistently.** Every connector declares
  `maturity = "ga"` in code, but the [connector status](#connector-status) table
  above records the actual hardware-validation state, which is lower for vSphere
  and OpenStack. **Trust the table, not the UI badge.**
* **vSphere vVol sources are rejected, not migrated.** Native vVol-source
  resolution is unimplemented; a vVol-backed VM fails preflight with a clear
  error rather than silently mismigrating.
* **Migration is cold-cutover only** and requires source and destination to share
  one physical array. There is no live migration and no cross-array path.
* **UEFI NVRAM / efivars are not transferred** — a UEFI destination VM gets a
  fresh efidisk, so custom boot entries and Secure Boot enrolment are lost.
* **No authentication on the PHIF UI or API.** PHIF serves over TLS but has no
  user login, RBAC, or audit identity. Anyone who can reach the port can drive
  every operation, including destructive ones. Keep it on a management network.
* **The vault master key is unrecoverable.** Lose `PHIF_VAULT_MASTER_KEY` and
  every stored array token and hypervisor credential becomes undecryptable.

## Documentation

**Start here:**

* [`DISCLAIMER.md`](DISCLAIMER.md) — **read first.** What "unsupported" means, what
  PHIF changes on your infrastructure, and how to evaluate it safely.
* [`SECURITY.md`](SECURITY.md) — the security model, its known limitations (no
  authentication, TLS verification off by default), and how to report a
  vulnerability privately.

**Using it:**

* [`docs/DEPLOYMENT.md`](docs/DEPLOYMENT.md) — Docker Compose, Kubernetes/Helm,
  remote-host notes, config/TLS, and first-run.
* `docs/connectors/<key>.md` — deploy + day-2 details for each hypervisor:
  [proxmox](docs/connectors/proxmox.md) ·
  [xcpng](docs/connectors/xcpng.md) ·
  [hpevme](docs/connectors/hpevme.md) ·
  [openshift](docs/connectors/openshift.md) ·
  [vsphere](docs/connectors/vsphere.md) ·
  [openstack](docs/connectors/openstack.md)

**Working on it:**

* [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md) — what PHIF is, the components,
  and how a day-2 operation flows through the stack.
* [`docs/CONNECTOR_GUIDE.md`](docs/CONNECTOR_GUIDE.md) — how to add or extend a
  hypervisor connector.
* [`CONTRIBUTING.md`](CONTRIBUTING.md) — dev setup, testing, and the rule against
  committing environment-specific data.

## License

Licensed under the **Apache License, Version 2.0** — see [`LICENSE`](LICENSE) and
[`NOTICE`](NOTICE).

PHIF is an independent project and is **not a supported product** — see
[`DISCLAIMER.md`](DISCLAIMER.md). All trademarks are the property of their
respective owners and are used for identification purposes only, without any
implication of affiliation or endorsement.
