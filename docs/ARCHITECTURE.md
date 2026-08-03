# PHIF Architecture

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../DISCLAIMER.md`](../DISCLAIMER.md) first.

## What PHIF is

**PHIF — the Universal Hypervisor Integration Framework** — is one
framework plus a web UI that lets an operator connect an Everpure FlashArray, mint the
API keys integrations need, attach hypervisors, deploy the right Everpure integration
onto each, and run day-2 storage operations — across **vSphere, OpenShift,
OpenStack, Proxmox, XCP-ng, and HPE VM Essentials**.

PHIF does **not** reinvent the vendor's integrations. It automates the deployment and
configuration of the real ones (the vSphere Client plugin + VASA, the
Kubernetes/OpenShift CSI driver, the OpenStack Cinder driver, the Proxmox storage
path, XCP-ng SRs, and the HPE VME path) and exposes the union of operations each
supports behind one common, capability-driven contract.

## High-level shape

```
┌──────────────┐     ┌─────────────────────────────────────────────┐
│  React SPA   │────▶│  FastAPI backend                            │
│ (capability- │ ws  │  • FlashArray client (+ API-token minting)  │
│  driven UI)  │◀────│  • Connector registry (auto-discovery)      │
└──────────────┘     │  • Job engine (Ansible / SSH / HTTP + logs) │
                     │  • Encrypted secrets vault                  │
                     │  • DB (arrays, creds, hypervisors, jobs)    │
                     │  • Connectors: vsphere, openshift, openstack│
                     │    proxmox, xcpng, hpevme (+ example)       │
                     └─────────────────────────────────────────────┘
                                  │ REST / SSH / Ansible / k8s API
                     ┌────────────┴────────────┐
                     ▼                          ▼
              Everpure FlashArray            Hypervisor targets
```

## Repository layout

```
phif/
├─ docker-compose.yml          # Postgres + backend + frontend (TLS)
├─ deploy/
│  ├─ gen-certs.sh             # self-signed TLS cert helper
│  └─ helm/phif/               # Kubernetes deployment of PHIF itself
├─ backend/
│  └─ phif/
│     ├─ main.py               # FastAPI app, router mount, WS log stream
│     ├─ config.py             # settings (env-driven)
│     ├─ db/                   # SQLAlchemy models + Alembic migrations
│     ├─ vault/                # Fernet envelope-encryption of secrets at rest
│     ├─ flasharray/           # py-pure-client (REST v2) wrapper + API-token mint
│     ├─ jobs/                 # async job engine: ansible-runner / asyncssh / HTTP
│     ├─ connectors/
│     │  ├─ base.py            # HypervisorConnector ABC + Capability + OpResult
│     │  ├─ registry.py        # auto-discovery of connector packages
│     │  ├─ example/           # fully-worked reference connector (copy this)
│     │  └─ vsphere/ openshift/ openstack/ proxmox/ xcpng/ hpevme/
│     └─ api/                  # routers: /arrays /credentials /hypervisors /operations /jobs
├─ frontend/src/               # Arrays, API Keys, Hypervisors, Operations, Jobs pages
├─ ansible/<hypervisor>/       # playbooks/roles invoked by connectors
└─ docs/                       # this guide, deployment, per-connector docs
```

## Core components

### The plugin contract (`connectors/base.py`)

Every hypervisor is a `HypervisorConnector` subclass that:

* declares static metadata — `key`, `name`, `description`, `maturity`
  (`ga | preview | scaffold`), `CAPABILITIES`, and `SUPPORTED_PROTOCOLS`;
* exposes two UI-facing classmethods — `target_schema()` (connection form) and
  `action_schemas()` (one `ActionSpec` per day-2 action, each tied to a
  `Capability`);
* implements the `async` operation methods for the capabilities it declares,
  each returning an `OpResult` and streaming progress to the job log.

The UI reads each connector's `CAPABILITIES` to enable or grey-out actions, so a
connector can only ever offer what it actually implements.

**Capability vocabulary** (union across hypervisors):
`CONNECT, DEPLOY_PLUGIN, CONFIGURE, PROVISION_DATASTORE, PROVISION_VOLUME,
SNAPSHOT, CLONE, RESIZE, DELETE, HOST_REGISTER, CONNECTIVITY, QOS, REPLICATION,
RECONCILE_CLUSTER, RECOVERY, UPGRADE, ROTATE_CREDENTIALS, HEALTH, REMOVE,
VM_INVENTORY, VM_LIFECYCLE, MIGRATE`. The last three drive **cross-hypervisor VM
migration** (`migrate/` — capture → create destination VM + per-disk managed
volume → FlashArray copy-with-overwrite → boot; copy/move modes, same- or
cross-array). Implemented by Proxmox, XCP-ng, HPE VME, OpenShift, and vSphere.

### Connector registry (`connectors/registry.py`)

Connectors are **auto-discovered** by walking the `connectors/` package. No
shared registration file is edited when adding one — so connectors can be built
in parallel without merge conflicts, and a new subpackage is picked up
automatically.

### Operation context (`ctx`)

Each operation runs with a context object providing the shared services:

| `ctx.…` | purpose |
|---|---|
| `ctx.array` | `FlashArrayClient` — volumes, snapshots, hosts, host groups, QoS, protection groups, **API-token minting**. May be `None` if no array is associated. |
| `ctx.target` | the hypervisor connection + **decrypted** secrets. |
| `ctx.runner` | `run_ssh` / `run_ansible` / `run_http` / `run_local`, each streaming output. |
| `ctx.emit(msg)` | push a progress line to the job log / UI. |
| `ctx.dry_run` | when True, validate and plan but make **no** changes. |

### Shared services

* **FlashArray client** (`flasharray/`) — one tested REST v2 wrapper for all
  array-side operations plus **API-key generation** (mint/rotate tokens for
  integrations that need one). A mock client backs `PHIF_MOCK_MODE=1`.
* **Job engine** (`jobs/`) — every deploy/day-2 action is a tracked job with logs
  streamed over WebSocket; supports Ansible (`ansible-runner`), SSH (`asyncssh`),
  HTTP, and local execution.
* **Vault** (`vault/`) — Fernet envelope-encrypted secret storage keyed by the app
  master key (`PHIF_VAULT_MASTER_KEY`). Pluggable so an external vault can be added.
* **Database** (`db/`) — SQLAlchemy models for arrays, credentials, hypervisors,
  and jobs, with Alembic migrations applied on startup.

## Per-hypervisor models

Two storage models appear across the connectors:

* **Per-VM-disk block volumes** (the CSI/Cinder "storage plugin" model) — each VM
  disk is its own FlashArray volume presented directly to the VM, with snapshots
  and clones offloaded **to the array**. Used by Proxmox, XCP-ng, and the HPE VME
  native plugin.
* **Shared datastore / control-plane integration** — the vendor's own datastore or
  CSI/Cinder driver manages volumes (VMFS/NFS/vVols, StorageClasses, Cinder
  backends).

| Hypervisor | Integration wrapped | Deploy mechanism |
|---|---|---|
| **vSphere** | vSphere Client plugin + VASA/vVols | vCenter REST register + FlashArray REST; `purestorage.flasharray` Ansible |
| **OpenShift** | Portworx (px-csi) `pxd.portworx.com` (PSO retired) | Portworx Operator (`comp=pxoperator` manifest; OLM Subscription fallback) + StorageCluster via the k8s API |
| **OpenStack** | Cinder driver (PureISCSI/FC/NVME) | render `cinder.conf` backend + restart `cinder-volume` |
| **Proxmox** | Custom `purefa` PVE storage plugin | SSH install + `storage.cfg` + multipath; array host-group registration |
| **XCP-ng** | Custom **SMAPIv3** driver (volume + datapath + host plugin) | `xe sm`/`sr-create` over SSH/XAPI; array host-group registration |
| **HPE VME** | Native **Morpheus/VME storage plugin** (Java/Groovy) | upload the plugin JAR via the VME Manager API; register the storage server |

See [`../README.md`](../README.md#connector-status) for the current maturity and
hardware-validation status of each, and `docs/connectors/<key>.md` for the
deploy + day-2 details of each one.

## Request lifecycle (a day-2 operation)

1. The UI shows the actions a connector's `CAPABILITIES` allow and collects the
   `action_schemas()` form fields.
2. The API creates a **job** and dispatches the action on the connector with a
   `ctx` (decrypted target, array client, runner).
3. The connector runs the work — array REST calls and/or `ctx.runner` SSH/Ansible
   — `ctx.emit`-ing progress that streams to the UI over WebSocket.
4. The connector returns an `OpResult`; the job records success/failure and the
   full log for the **Jobs** page.

## Security model

* Secrets (API tokens, hypervisor credentials) are **encrypted at rest** with the
  vault master key and only decrypted into a `ctx.target` for the duration of an
  operation.
* All HTTP is TLS-terminated (API and UI).
* The management host is assumed to have direct, authenticated reach to the
  FlashArray REST API and each hypervisor's API/SSH endpoint.
