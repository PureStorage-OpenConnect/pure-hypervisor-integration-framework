# Deploying PHIF

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../DISCLAIMER.md`](../DISCLAIMER.md) first.

PHIF ships as a three-service stack — **Postgres**, the **FastAPI backend**, and
the **nginx-served React frontend** — fronted by TLS. You can run it with Docker
Compose (single host) or Kubernetes/Helm. This guide covers both, plus
remote-host deployment, config/TLS, and first-run.

> **Prerequisites (the management host)**
> * Docker + Docker Compose v2 *(Compose path)*, or a Kubernetes cluster + Helm 3 *(Helm path)*.
> * Network reachability from the host to the **FlashArray REST API** and to each
>   **hypervisor API/SSH** endpoint you intend to manage.
> * Python 3.11+ available locally to mint the vault key and (optionally) run the
>   backend for development.

---

## 1. Required configuration

| Setting | Env var | Required | Notes |
|---|---|---|---|
| Vault master key | `PHIF_VAULT_MASTER_KEY` | **yes** | Fernet key; encrypts all stored secrets at rest. Generate once, keep it safe — losing it makes stored credentials unrecoverable. |
| Mock mode | `PHIF_MOCK_MODE` | no (`0`) | `1` stubs **all** array/hypervisor I/O for a no-hardware demo or CI. |
| Database URL | `PHIF_DATABASE_URL` | no | Defaults to the bundled Postgres (`postgresql+psycopg://phif:phif@db:5432/phif`). |
| CORS origins | `PHIF_CORS_ORIGINS` | no | JSON array of allowed browser origins. |

Generate a vault key:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### TLS certificates

Both the API (uvicorn) and the UI (nginx) serve HTTPS and read a cert/key pair
from `./certs/tls.crt` and `./certs/tls.key`. Generate a self-signed pair (with
the host IP/DNS baked into the SAN) for testing:

```bash
deploy/gen-certs.sh <host-ip-or-dns>     # e.g. deploy/gen-certs.sh 192.0.2.50
```

For production, drop a CA-signed `tls.crt` + `tls.key` into `certs/` instead.

---

## 2. Docker Compose (single host)

```bash
# 1. Vault key -> .env
echo "PHIF_VAULT_MASTER_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")" > .env

# 2. TLS cert/key into ./certs
deploy/gen-certs.sh <host-ip-or-dns>

# 3. (optional) no-hardware demo
echo "PHIF_MOCK_MODE=1" >> .env

# 4. Build + start
docker compose up --build -d
```

Endpoints once healthy:

* **Web UI** — `https://<host>` (also `https://<host>:8443`); plain HTTP on `:80`
  redirects to HTTPS.
* **API** — `https://<host>:8000` — health at `/healthz`, OpenAPI at `/docs`. Also
  reachable TLS-terminated through the UI's `/api` proxy.

Operate the stack:

```bash
docker compose ps                 # service status
docker compose logs -f backend    # stream backend logs
docker compose down               # stop (keeps the phif-db volume)
docker compose down -v            # stop and delete the database volume
```

The self-signed cert triggers a browser trust warning — expected for the
`gen-certs.sh` cert; supply a CA-signed cert for production.

---

## 3. Kubernetes / Helm

```bash
helm install phif deploy/helm/phif \
  --set vaultMasterKey=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")

# expose through an ingress
helm upgrade phif deploy/helm/phif \
  --set ingress.enabled=true --set ingress.host=phif.example.com
```

The chart (`deploy/helm/phif/`) renders Postgres, the backend, the frontend, an
optional ingress, and a `Secret` holding the vault key. Tune image tags, the
ingress host/TLS, and `mockMode` via `values.yaml` or `--set`. Validate a render
without applying:

```bash
helm template phif deploy/helm/phif | less
```

---

## 4. Deploying to a remote host

There is no bundled remote-deploy script — deployment paths are too
environment-specific to generalise. The pattern that works well is to copy the
tree to the target host and run the Compose stack there, **leaving the host's
`.env` and `certs/` in place** so the vault key and TLS material are never
shipped over the wire or overwritten:

```bash
# on the target host, once: seed the vault key
cd /opt/phif
echo "PHIF_VAULT_MASTER_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")" > .env
# add PHIF_MOCK_MODE=1 to that .env for a no-hardware run

# then, per deploy
docker compose up --build -d
curl -sk https://localhost:8000/healthz
```

If you script this, exclude `.env`, `certs/`, `node_modules/`, `.venv/`, and
build artifacts from whatever you copy, and keep the script out of version
control — `/deploy-vm.sh` and `/deploy-vm.ps1` are already `.gitignore`d for
exactly this reason, since such scripts tend to accumulate real hostnames and
usernames.

---

## 5. First run

1. Open the UI, go to **FlashArrays**, add an array (mgmt IP + API token). PHIF
   validates the connection, then stores the token encrypted.
2. **API Keys** — mint/rotate FlashArray API tokens for integrations that need one.
3. **Hypervisors** — add a hypervisor target (credentials stored encrypted), then
   **deploy** its Everpure integration. Watch the streamed job log.
4. **Operations** — run capability-driven day-2 actions (provision, snapshot,
   clone, resize, QoS, replication, upgrade, rotate, health, remove).
5. **Jobs** — review history and streamed logs for every operation.

---

## 6. Upgrades & backups

* **Upgrade** — pull the new tree and re-run `docker compose up --build -d`
  (or `helm upgrade`). Schema migrations run automatically on backend start.
* **Back up** the vault key (`.env` / the Helm `Secret`) and the Postgres volume
  (`phif-db`). The encrypted secrets are useless without the key — store it in
  your own secret manager.

See [`ARCHITECTURE.md`](ARCHITECTURE.md) for how the pieces fit together and
[`CONNECTOR_GUIDE.md`](CONNECTOR_GUIDE.md) to add a new hypervisor.
