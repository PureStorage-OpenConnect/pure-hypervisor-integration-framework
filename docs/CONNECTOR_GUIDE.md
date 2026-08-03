# Writing a PHIF connector

> **⚠️ Not a supported product.** PHIF is an independent, experimental project
> that is not covered by any support agreement or warranty, and it can cause
> irreversible data loss. Read [`../DISCLAIMER.md`](../DISCLAIMER.md) first.

Every hypervisor integration is a **connector**: a subclass of
`HypervisorConnector` (`backend/phif/connectors/base.py`) living in its own
subpackage `backend/phif/connectors/<key>/`. Connectors are **auto-discovered** —
you never edit a shared registration file, so connectors can be built in parallel
without merge conflicts.

The fully-worked reference is `backend/phif/connectors/example/`. Copy it.

## What a connector owns (and only this)

```
backend/phif/connectors/<key>/__init__.py     # exports your connector class
backend/phif/connectors/<key>/connector.py    # the implementation
backend/phif/connectors/<key>/...              # helper modules as needed
backend/tests/test_<key>.py                    # unit tests (mock array + runner)
ansible/<key>/...                              # any playbooks/roles you call
docs/connectors/<key>.md                       # deploy + day-2 doc for users
```

Do **not** modify `base.py`, `registry.py`, shared services, the API layer, or
other connectors. If you think the contract is missing something, note it in your
connector doc — do not change `base.py`.

## The contract

Declare static metadata as class attributes:

```python
class MyConnector(HypervisorConnector):
    key = "myhv"               # unique slug, matches the directory name
    name = "My Hypervisor"
    description = "..."
    maturity = "ga"            # "ga" | "preview" | "scaffold"
    CAPABILITIES = {Capability.DEPLOY_PLUGIN, Capability.PROVISION_VOLUME, ...}
    SUPPORTED_PROTOCOLS = {Protocol.ISCSI, Protocol.NVME_TCP}
```

Implement two UI-facing classmethods:

* `target_schema()` → `list[FormField]` — what's needed to connect (host, creds,
  protocol). Fields of type `FieldType.SECRET` are stored encrypted; everything
  else is stored as non-secret connection detail.
* `action_schemas()` → `list[ActionSpec]` — one entry per day-2 action, with the
  `Capability` it maps to and the `FormField`s the UI should collect. The action
  `id` is what the UI sends back; route it in `dispatch` (the default routing
  handles the standard ids — see `_DEFAULT_DISPATCH`).

Implement the operation methods matching your `CAPABILITIES`. Each is `async`,
returns `OpResult.ok(...)` / `OpResult.fail(...)`, and uses `self.ctx`:

| `self.ctx.…` | use |
|---|---|
| `ctx.array` | `FlashArrayClient` — volumes, snapshots, hosts, host groups, QoS, protection groups, **API-token minting**. May be `None` if no array is associated. |
| `ctx.target` | hypervisor connection + **decrypted** secrets (`ctx.target.get("host")`). |
| `ctx.runner` | `run_ssh` / `run_ansible` / `run_http` / `run_local` — each streams output to the UI. |
| `ctx.emit("…")` | stream a progress line to the job log / UI. |
| `ctx.dry_run` | when True, validate and plan but make **no** changes. |

`validate_connection()` is required of every connector.

## Rules

1. **Wrap the real vendor integration** — do not reinvent it. Read the vendor's
   published documentation for your hypervisor and pin exact component names,
   versions, config keys, and supported operations. Cite them in your
   connector doc.
2. **Be honest about capabilities.** Only list a `Capability` you actually
   implement; the UI enables actions from this set.
3. **Stream progress** with `ctx.emit` and the runner — operations are long-running
   jobs the user watches live.
4. **Respect `ctx.dry_run`.**
5. **Test with mocks.** Use the `make_context` fixture (mock FlashArray + runner).
   No test may touch a real array or hypervisor. `PHIF_MOCK_MODE=1` is set in CI.
6. **Set `maturity` honestly.** `ga` = working logic validated on hardware
   (Proxmox, XCP-ng, HPE VME today); `preview` = working but not yet
   hardware-validated; `scaffold` = wired end-to-end in mock mode with explicit
   `# TODO(doc-validate):` markers where behavior is unconfirmed (vSphere,
   OpenShift, OpenStack today). Regardless of maturity, `validate_connection`,
   `target_schema`, `action_schemas`, and `dispatch` must work end-to-end in mock
   mode.
7. **License your files** under Apache-2.0 (the project license). Add the SPDX
   header `# SPDX-License-Identifier: Apache-2.0` (or the language-appropriate
   comment) to new source files; do not introduce code under an incompatible
   license.

## Verify

```bash
cd backend
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest tests/test_<key>.py -q
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest -q          # whole suite still green
```

Your connector must appear in `GET /api/connectors` and be drivable from the
Hypervisors → Operations UI.
