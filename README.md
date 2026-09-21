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
XCP-ng, HPE VM Essentials, and Nutanix AHV**.

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
                     │    proxmox, xcpng, hpevme, nutanix         │
                     │    (+ example)                              │
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
| Proxmox | `ga` | Custom PVE storage plugin (`purefa`): one FA volume per disk, array snapshots/clones | ✅ live 2-node PVE cluster |
| XCP-ng | `ga` | Custom SMAPIv3 driver (volume + datapath + host plugin): one FA volume per VDI, array snapshots/clones | ✅ live 2-host pool, XCP-ng 8.3 / xapi 25.6 |
| HPE VME | `ga` | Native Morpheus/VME storage plugin (Java/Groovy under `connectors/hpevme/files/morpheus-plugin/`): per-VM-disk FA volumes, array-offloaded snap/clone/resize, VME-native libvirt attach via `MvmProvisionFacet`. PHIF connector uploads the plugin (`deploy`) + registers the storage server (`configure`). | ✅ live VME appliance: provision, image deploy, clone-from-VM, snapshot create/revert/delete |
| vSphere | `ga` | vSphere Client plugin + VASA/vVols, FlashArray REST, `purestorage.flasharray` | ✅ live vCenter: plugin + VASA deploy, VMFS/RDM datastore provisioning, vVol→FA volume resolution for migration |
| OpenShift | `ga` | Portworx (px-csi) via the Portworx Operator (manifest) + StorageCluster (Portworx Central spec or generated FADA) | ✅ live OCP 4.22 single-node + FA-X20R3: Portworx install, PVC provision |
| OpenStack | `ga` | Cinder driver (PureISCSI/FC/NVME) | ✅ live controller: Cinder backend deploy → configure → provision |
| Nutanix AHV | `preview` | FlashArray as AHV **external storage** (NVMe-oF/TCP): one FA volume per vDisk, driven via Prism Central. External-storage registration itself is **not** implemented — do it in Prism. | ⚠️ live Prism Central (AOS 7.6 / AHV 11.2, 3-node) + FA-XL130 on Purity 6.12.2: inventory, vDisk→FA volume resolution, and VM lifecycle (create VM, add disk, detach, delete) all exercised on hardware. Migration validated by **dry run in both directions**; no full migration executed end-to-end yet |

Proxmox, XCP-ng, and HPE VME follow the CSI/Cinder "storage plugin" model — each VM disk is
its own FlashArray volume presented directly to the VM (no LVM), with snapshots and
clones performed on the array.

**VM migration is validated in both directions between Proxmox, XCP-ng, HPE VME,
OpenShift Virtualization and vSphere**, on live hardware sharing one FlashArray.
The Nutanix connector's migration paths are **not** hardware-validated yet — see
the connector table above.

## VM Migration

PHIF supports **cold-cutover VM migration** between Proxmox, XCP-ng, HPE VME,
OpenShift Virtualization (KubeVirt), and VMware vSphere where both hypervisors
share the same FlashArray. Migration is validated in both directions between all
of them on live hardware. Every VM disk is a dedicated FlashArray volume on all
supported hypervisors (for OpenShift, a Portworx px-csi FADA PVC = one FA
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

**Validated end-to-end on hardware (same-array, boot-confirmed):** every pair of
supported hypervisors, in both directions — Proxmox, XCP-ng, HPE VME, OpenShift,
and vSphere (VMFS and RDM sources).

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
  with a clear preflight error.
* **vVol** disks are resolved exactly, with no size or name guessing. A vVol has no
  device serial of its own, so PHIF reads the disk's vVol id
  (`backingObjectId`, e.g. `rfc4122.<uuid>`) from vCenter and looks it up against the
  FlashArray tag `PURE_VVOL_ID` in the `vasa-integration.purestorage.com` namespace —
  the mapping the Everpure VASA provider maintains. That yields the backing volume and
  its serial, after which a vVol disk migrates on the same path as an RDM. A vVol id
  that does not resolve on the connected array (commonly because the vVol lives on a
  *different* array) is reported as unmappable rather than guessed at.
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
* Recreate CD-ROM / optical drives — the destination VM is built with **no optical
  drive**, even when the source had one. See the known issue below.
* Move non-Everpure disks — only FA-backed volumes (or vSphere VMFS/vVol/RDM disks) are supported.
* Replication — source and destination must share the same physical array.

## Known issues

Being an experimental project, PHIF has rough edges that are documented rather
than hidden. Please read these before filing an issue.

* **A Nutanix *move* source does not finish cleanly.** AHV cannot delete a VM
  while keeping its vDisks, so the connector refuses
  `delete_vm(keep_disks=True)`. `_finalize_move` now detects that and **leaves
  the source volumes alone** rather than eradicating them behind a VM that still
  owns them, and says so in the job log. The migration still succeeds — the
  destination runs off its own copies — but the source VM and its volumes remain
  and must be removed by hand. Nutanix as a migration **destination**, or as a
  **copy** source, is unaffected.
* **Nutanix: detaching a vDisk does not free its FlashArray volume.** The volume
  stays live and connected to a stargate host; teardown must clean it up
  explicitly.
* **Nutanix: registering the FlashArray as external storage is not implemented.**
  The Nutanix connector consumes an AHV cluster that *already* has the array
  registered as an External Storage target; it does not perform that
  registration (array service account, realm, pod, NVMe-oF/TCP interface
  configuration, and the Prism Element registration). Do that in Prism first —
  `validate_connection` fails with an explicit message if it is missing. The
  connector accordingly does not advertise `deploy_plugin`, `configure`,
  `provision_datastore`, `provision_volume`, `connectivity` or `host_register`.
  Its VM-lifecycle and migration **write** paths are also unit-tested only, not
  yet hardware-validated.
* **vVol datastore provisioning is not implemented.** vVols are deprecated, so
  `provision_datastore` refuses `type=vvol` (it previously fell through to the
  VMFS path and produced a VMFS datastore merely *labelled* vvol). Create the
  vVol datastore in vCenter — storage provider plus storage container — and PHIF
  will migrate VMs on and off it. Registering/refreshing the VASA provider is
  still supported via the `deploy` and `configure` actions.
* **Migration is cold-cutover only** and requires source and destination to share
  one physical array. There is no live migration and no cross-array path.
* **UEFI NVRAM / efivars are not transferred** — a UEFI destination VM gets a
  fresh efidisk, so custom boot entries and Secure Boot enrolment are lost.
* **CD-ROM / optical drives are not migrated — this affects every connector.**
  `VmSpec`, the normalized model every migration passes through, has no field for
  optical devices, and no connector's `capture_vm_spec` reads one. A source
  CD-ROM is therefore dropped at *capture* time, before any destination is
  involved, so the destination VM is built without an optical drive.

  Two consequences worth planning for:

  * **Installing guest tools by the platform's native method usually needs a
    CD-ROM.** Nutanix Guest Tools, VMware Tools, XCP-ng/XenServer guest tools
    and the virtio-win drivers are all normally delivered as an ISO. On a
    migrated VM there is no drive to attach it to, so **add a CD-ROM drive to
    the destination VM first**, then mount the ISO. (Package-manager installs —
    `qemu-guest-agent` and similar — need no drive.)
  * Device enumeration can shift. A guest that saw an optical device on the
    source will not see one on the destination, which can renumber devices or
    leave a stale fstab/boot entry referencing it.

  The media itself would not be portable anyway: a drive backed by node-local
  storage (e.g. Proxmox `local:iso/…`) has no equivalent on the destination, so
  any future support would recreate an *empty* drive rather than carry the ISO.
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
