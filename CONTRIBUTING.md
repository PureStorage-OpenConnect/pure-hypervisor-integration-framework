# Contributing to PHIF

Thanks for taking an interest. PHIF is an **unsupported, experimental** project
(see [DISCLAIMER.md](DISCLAIMER.md)) maintained on a best-effort basis. Issues and
pull requests are welcome, but please set expectations accordingly: there is no
SLA on responses, and no commitment to accept any given change.

## Ground rules

**Never commit environment-specific or sensitive data.** This is the single most
important rule. The following must never appear in a commit:

- Real IP addresses, hostnames, FQDNs, or usernames from your environment
- API tokens, passwords, SSH keys, TLS private keys, or `.env` files
- Array names, serial numbers, or WWNs from real hardware
- Internal ticket numbers, internal URLs, or customer names

Use documentation-range placeholders instead — `192.0.2.0/24`
([RFC 5737](https://datatracker.ietf.org/doc/html/rfc5737)), `example.com`,
`vcenter.example.com`. The `.gitignore` already excludes `.env`, `certs/`, `*.key`,
`*.crt`, `/deploy-vm.sh`, and local scratch files; do not remove those entries,
and keep environment-specific helper scripts untracked.

Before pushing, it is worth a quick self-check:

```bash
git diff --cached | grep -nEi '([0-9]{1,3}\.){3}[0-9]{1,3}|password|api_?token|BEGIN .*PRIVATE KEY'
```

## Development setup

```bash
# Backend
cd backend
python -m venv .venv
.venv/Scripts/pip install ".[dev]"        # .venv/bin/pip on Linux/macOS
PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest -q
PHIF_MOCK_MODE=1 .venv/Scripts/python -m uvicorn phif.main:app --reload

# Frontend
cd frontend && npm install && npm run dev
```

`PHIF_MOCK_MODE=1` stubs **all** array and hypervisor I/O, so the full UI and
every workflow can be exercised with no hardware. Develop against mock mode by
default.

## Testing

```bash
cd backend && PHIF_MOCK_MODE=1 .venv/Scripts/python -m pytest -q
```

The suite is **753 tests, all passing**. Please keep it that way — a red suite on
`main` makes every subsequent contribution harder to review.

When you change something, please:

- Add or update tests that run green under `PHIF_MOCK_MODE=1` with no network
  access. A test that resolves a real hostname or opens a socket is a bug.
- Confirm the suite is still fully green: `pytest -q 2>&1 | tail -1`.
- Keep to the existing style — `ruff` and `black` are configured in
  `backend/pyproject.toml` at a 100-column line length.

## Adding a connector

Read [`docs/CONNECTOR_GUIDE.md`](docs/CONNECTOR_GUIDE.md) and copy
`backend/phif/connectors/example/`. Connectors are auto-discovered by walking the
`connectors/` package, so no central registry file needs editing — new connectors
can be developed in parallel without merge conflicts.

Two things matter most:

1. **Declare only the capabilities you actually implement.** The UI enables
   actions straight from `CAPABILITIES`, so an over-declared capability becomes a
   broken button.
2. **Be honest in `maturity`.** Use `scaffold` for untested logic, `preview` for
   partially validated, and `ga` only for genuinely hardware-validated paths. Also
   update the connector status table in the [README](README.md#connector-status)
   with what you actually tested, on what hardware.

## Documenting hardware validation

If you validate a connector against real hardware, please say so in
`docs/connectors/<key>.md` and in the README table — describing the hypervisor
version and what specifically was exercised, **without** including hostnames, IPs,
or array identifiers from your environment.

## Licensing

PHIF is [Apache 2.0](LICENSE). By submitting a contribution you agree it is
licensed on the same terms.
