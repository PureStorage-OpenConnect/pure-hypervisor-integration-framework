# Security

## Scope and expectations

PHIF is an **unsupported, experimental project** (see
[DISCLAIMER.md](DISCLAIMER.md)). It has no security guarantees, no formal review
process, and no patch SLA. Do not deploy it as though it were a hardened product.

## Known security characteristics

These are design realities of the current code, not undisclosed vulnerabilities.
Understand them before deploying:

- **There is no authentication or authorisation.** The PHIF API and UI have no
  login, no RBAC, and no per-user audit identity. **Anyone who can reach the
  port can perform every operation**, including destroying array volumes and
  deleting VMs. Restrict access at the network layer and keep PHIF on a trusted
  management network.
- **PHIF is a credential aggregator and therefore a high-value target.** It
  stores FlashArray API tokens and hypervisor administrative credentials. A
  compromise of the PHIF host is effectively a compromise of every array and
  hypervisor registered in it.
- **Secrets at rest** are encrypted with Fernet (AES-128-CBC + HMAC) using
  `PHIF_VAULT_MASTER_KEY`. That key is supplied via environment variable or a
  Kubernetes `Secret`. Anyone who can read the key and the database can decrypt
  every stored credential. Losing the key makes them permanently unrecoverable.
- **Secrets in use** are decrypted into process memory for the duration of an
  operation.
- **TLS** fronts both API and UI, but `deploy/gen-certs.sh` produces a
  **self-signed** certificate intended for testing. Use a CA-signed certificate
  for anything else.
- **Certificate verification to managed endpoints is off by default.** An array's
  `verify_ssl` flag **defaults to `False`**, and the vSphere and XCP-ng code paths
  hard-code `check_hostname = False` / `ssl.CERT_NONE`; the bundled VME plugin
  likewise disables hostname verification. This is a deliberate concession to
  self-signed storage and hypervisor endpoints, but it means those channels —
  which carry API tokens and administrative credentials — are **not protected
  against an active man-in-the-middle**. Enable `verify_ssl` where your endpoints
  present trusted certificates, and treat the management network as part of your
  trust boundary.
- **PHIF executes commands on remote hosts over SSH as a privileged user** and
  requires passwordless `sudo` on some targets. It writes multipath, iSCSI, and
  storage-plugin configuration and restarts system services.
- **Mock mode is the safe default for evaluation.** `PHIF_MOCK_MODE=1` performs
  no real array, host, or hypervisor I/O.

## Hardening suggestions

- Put PHIF on an isolated management network and firewall it to known admin hosts.
- Terminate TLS with a CA-signed certificate.
- Give PHIF a **dedicated, least-privilege** array and hypervisor account rather
  than a primary administrative credential, so its blast radius is bounded and its
  actions are attributable.
- Store `PHIF_VAULT_MASTER_KEY` in a real secret manager, back it up separately
  from the database, and rotate the credentials PHIF holds if the host is ever
  suspect.
- Back up the Postgres volume (`phif-db`) and the vault key independently.

## Reporting a vulnerability

Please **do not** open a public issue for a security problem in PHIF.

Instead, report it privately through GitHub's **Report a vulnerability** flow
under this repository's **Security** tab (Security → Advisories → Report a
vulnerability), which opens a private advisory visible only to the maintainers.

Please include a description of the issue, the affected version or commit, and
reproduction steps or a proof of concept. Being a best-effort project, fixes are
made when time allows and no response time is guaranteed.

### Report elsewhere, not here

If you find a vulnerability in an **upstream product** that PHIF merely deploys or
configures — a FlashArray, Purity, Portworx, vSphere, OpenShift, OpenStack,
Proxmox VE, XCP-ng, or HPE VM Essentials — report it to that vendor through their
own security process. PHIF's maintainers cannot triage or fix upstream issues, and
this repository is not a channel to any vendor's security team.
