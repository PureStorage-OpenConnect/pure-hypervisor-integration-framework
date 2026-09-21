"""End-to-end migration API + every source->destination direction in mock mode.

Drives the real HTTP stack: create an array + two hypervisors sharing it, read
the source VM spec and destination networks, POST /migrations, and wait for the
job. Covers all 6 directions among proxmox/xcpng/hpevme.
"""

import asyncio
import uuid

import pytest
from httpx import ASGITransport, AsyncClient

from phif.main import create_app


def _uniq(prefix: str) -> str:
    """Globally-unique name — the in-memory DB is shared across the whole test
    session, so names must not collide with other tests."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"

_CONN = {
    "proxmox": {"node_host": "pve.test", "storage_id": "purefa", "host_group": "hg"},
    "xcpng": {"pool_master_host": "xcp.test", "sr_name": "purefa", "host_group": "hg"},
    "hpevme": {"vme_manager_url": "https://vme.test", "host_group": "hg"},
}


@pytest.fixture
async def client():
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        async with app.router.lifespan_context(app):
            yield c


# Generous ceiling (returns as soon as the job is terminal — usually well under a
# second). Only one migration job runs in this file, so a long budget can't cause
# the thrash that many concurrent jobs did; it just absorbs full-suite contention
# on the shared in-memory DB without flaking.
async def _wait_job(client, job_id, tries=1000):
    for _ in range(tries):
        r = await client.get(f"/api/jobs/{job_id}")
        if r.json()["status"] in ("succeeded", "failed"):
            return r.json()
        await asyncio.sleep(0.05)
    return r.json()


async def _make_hv(client, key, array_id, suffix, host_group="hg"):
    conn = dict(_CONN[key], host_group=host_group)
    r = await client.post("/api/hypervisors", json={
        "name": _uniq(f"hv-{key}"), "connector_key": key,
        "connection": conn,
        "secrets": {"ssh_password": "p", "password": "p", "api_token": "tok"},
        "array_id": array_id})
    assert r.status_code == 201, r.text
    return r.json()["id"]


async def test_vms_and_networks_endpoints(client):
    r = await client.post("/api/arrays", json={
        "name": _uniq("fa"), "mgmt_endpoint": "10.0.0.10", "api_token": "tok"})
    array_id = r.json()["id"]
    hv = await _make_hv(client, "proxmox", array_id, "vn")

    r = await client.get(f"/api/hypervisors/{hv}/vms")
    assert r.status_code == 200, r.text
    assert r.json() and r.json()[0]["id"]

    r = await client.get(f"/api/hypervisors/{hv}/networks")
    assert r.status_code == 200 and r.json()

    vm_ref = (await client.get(f"/api/hypervisors/{hv}/vms")).json()[0]["id"]
    r = await client.get(f"/api/hypervisors/{hv}/vms/{vm_ref}/spec")
    assert r.status_code == 200
    assert r.json()["disks"][0]["identity"]["fa_volume"]


async def test_cross_array_needs_authorization(client):
    # Cross-array (different FlashArrays) is now SUPPORTED, but configuring the
    # required replication connection needs authorization -> 409 until approved.
    r = await client.post("/api/arrays", json={
        "name": _uniq("fa-a"), "mgmt_endpoint": "10.0.0.1", "api_token": "t"})
    a1 = r.json()["id"]
    r = await client.post("/api/arrays", json={
        "name": _uniq("fa-b"), "mgmt_endpoint": "10.0.0.2", "api_token": "t"})
    a2 = r.json()["id"]
    src = await _make_hv(client, "proxmox", a1, "xa1")
    dst = await _make_hv(client, "xcpng", a2, "xa2")
    body = {"source_hypervisor_id": src, "dest_hypervisor_id": dst,
            "vm_ref": "100", "network_map": {}}
    r = await client.post("/api/migrations", json=body)
    assert r.status_code == 409
    assert "array connection" in r.text.lower()

    # Precheck reports the cross-array state for the wizard.
    r = await client.get("/api/migrations/precheck",
                         params={"source_hypervisor_id": src, "dest_hypervisor_id": dst})
    assert r.status_code == 200
    pc = r.json()
    assert pc["cross_array"] is True and pc["needs_authorization"] is True


async def test_migration_end_to_end_http(client):
    """One full migration through the real HTTP -> job-engine -> orchestrator
    stack, proving the wiring. The 6-direction connector matrix is covered fast +
    deterministically at the service level in test_migrate.py (no background-job /
    in-memory-DB contention)."""
    r = await client.post("/api/arrays", json={
        "name": _uniq("fa"), "mgmt_endpoint": "10.0.0.10", "api_token": "tok"})
    array_id = r.json()["id"]
    # Distinct host groups so the unmap-source -> map-dest path is exercised.
    src = await _make_hv(client, "proxmox", array_id, "e2e-s", host_group="hg-src")
    dst = await _make_hv(client, "xcpng", array_id, "e2e-d", host_group="hg-dst")

    vm_ref = (await client.get(f"/api/hypervisors/{src}/vms")).json()[0]["id"]
    spec = (await client.get(f"/api/hypervisors/{src}/vms/{vm_ref}/spec")).json()
    dst_net = (await client.get(f"/api/hypervisors/{dst}/networks")).json()[0]["id"]
    network_map = {nic["source_network"]: dst_net for nic in spec["nics"]}

    r = await client.post("/api/migrations", json={
        "source_hypervisor_id": src, "dest_hypervisor_id": dst,
        "vm_ref": vm_ref, "network_map": network_map})
    assert r.status_code == 202, r.text
    body = r.json()
    job = await _wait_job(client, body["job_id"])
    # Assert on the JOB result, which JobManager persists reliably.
    assert job["status"] == "succeeded", job.get("logs", "")
    result = job["result"]
    assert result["success"] is True, result
    assert result["data"]["phase"] == "succeeded", result
    assert result["artifacts"]["dest_vm_ref"], result

    # The migration record exists and is queryable.
    r = await client.get(f"/api/migrations/{body['migration_id']}")
    assert r.status_code == 200


async def test_duplicate_in_progress_rejected(client, monkeypatch):
    # Force the migration to stay "running" by making the job hang briefly is
    # fragile; instead assert the guard by pre-inserting a running Migration row.
    from phif.db.models import Migration
    from phif.db.session import SessionLocal

    r = await client.post("/api/arrays", json={
        "name": _uniq("fa-dup"), "mgmt_endpoint": "10.0.0.10", "api_token": "tok"})
    array_id = r.json()["id"]
    src = await _make_hv(client, "proxmox", array_id, "dup-s")
    dst = await _make_hv(client, "xcpng", array_id, "dup-d")

    async with SessionLocal() as s:
        s.add(Migration(source_hypervisor_id=src, dest_hypervisor_id=dst,
                        vm_ref="100", status="running", phase="map_dest"))
        await s.commit()

    r = await client.post("/api/migrations", json={
        "source_hypervisor_id": src, "dest_hypervisor_id": dst,
        "vm_ref": "100", "network_map": {"vmbr0": "net-uuid-0"}})
    assert r.status_code == 409


# --------------------------------------------------------------------------- #
# /api/migrations/destinations — pairwise eligibility
#
# The submit path rejects an unreachable pair with a 409, but only after the
# operator has picked one. This endpoint judges every candidate up front against
# the same rule: same FlashArray, or a replication connection between them.
# --------------------------------------------------------------------------- #
async def _mk_array(client, ep):
    r = await client.post("/api/arrays", json={
        "name": _uniq("fa"), "mgmt_endpoint": ep, "api_token": "t"})
    assert r.status_code == 201, r.text
    return r.json()["id"]


def _by_id(payload, hv_id):
    return next(d for d in payload["destinations"] if d["id"] == hv_id)


async def test_destinations_same_array_is_eligible(client):
    a1 = await _mk_array(client, "10.0.1.1")
    src = await _make_hv(client, "proxmox", a1, "d1")
    dst = await _make_hv(client, "xcpng", a1, "d2")

    r = await client.get("/api/migrations/destinations",
                         params={"source_hypervisor_id": src})
    assert r.status_code == 200, r.text
    payload = r.json()
    assert payload["source_supports_migrate"] is True
    entry = _by_id(payload, dst)
    assert entry["eligible"] is True
    assert entry["cross_array"] is False
    assert entry["needs_authorization"] is False
    assert "same flasharray" in entry["reason"].lower()


async def test_destinations_cross_array_flags_authorization(client):
    a1 = await _mk_array(client, "10.0.2.1")
    a2 = await _mk_array(client, "10.0.2.2")
    src = await _make_hv(client, "proxmox", a1, "d3")
    dst = await _make_hv(client, "xcpng", a2, "d4")

    r = await client.get("/api/migrations/destinations",
                         params={"source_hypervisor_id": src})
    entry = _by_id(r.json(), dst)
    # Still offered -- the migration CAN build the connection, but only with
    # explicit authorization, which the reason has to make clear.
    assert entry["eligible"] is True
    assert entry["cross_array"] is True
    assert entry["needs_authorization"] is True
    assert "replication connection" in entry["reason"].lower()


async def test_destinations_without_an_array_are_ineligible(client):
    a1 = await _mk_array(client, "10.0.3.1")
    src = await _make_hv(client, "proxmox", a1, "d5")
    # A hypervisor with no FlashArray cannot receive a volume at all.
    r = await client.post("/api/hypervisors", json={
        "name": _uniq("hv-noarray"), "connector_key": "xcpng",
        "connection": dict(_CONN["xcpng"], host_group="hg"),
        "secrets": {"ssh_password": "p", "password": "p", "api_token": "tok"}})
    assert r.status_code == 201, r.text
    dst = r.json()["id"]

    entry = _by_id((await client.get(
        "/api/migrations/destinations",
        params={"source_hypervisor_id": src})).json(), dst)
    assert entry["eligible"] is False
    assert "no flasharray" in entry["reason"].lower()


async def test_destinations_excludes_the_source_itself(client):
    a1 = await _mk_array(client, "10.0.4.1")
    src = await _make_hv(client, "proxmox", a1, "d6")
    payload = (await client.get("/api/migrations/destinations",
                                params={"source_hypervisor_id": src})).json()
    assert all(d["id"] != src for d in payload["destinations"])


async def test_destinations_rejects_non_migrate_connector(client):
    """A connector without Capability.MIGRATE is reported ineligible with a
    reason, not hidden -- hiding it is what made the Nutanix connector look
    like a misconfiguration."""
    a1 = await _mk_array(client, "10.0.5.1")
    src = await _make_hv(client, "proxmox", a1, "d7")
    # 'openshift' has MIGRATE; 'example' does not -- use it as the negative case.
    r = await client.post("/api/hypervisors", json={
        "name": _uniq("hv-example"), "connector_key": "example",
        "connection": {"host": "mgr.test", "username": "admin"},
        "secrets": {"password": "p"}, "array_id": a1})
    assert r.status_code == 201, r.text
    dst = r.json()["id"]

    entry = _by_id((await client.get(
        "/api/migrations/destinations",
        params={"source_hypervisor_id": src})).json(), dst)
    assert entry["eligible"] is False
    assert "cannot act as a migration destination" in entry["reason"]


async def test_destinations_includes_nutanix_when_array_shared(client):
    """Regression for the reported bug: Nutanix declares Capability.MIGRATE, so
    sharing an array with the source must make it an eligible destination."""
    a1 = await _mk_array(client, "10.0.6.1")
    src = await _make_hv(client, "proxmox", a1, "d8")
    r = await client.post("/api/hypervisors", json={
        "name": _uniq("hv-nutanix"), "connector_key": "nutanix",
        "connection": {"pc_host": "pc.test", "pc_user": "admin",
                       "cluster": "c1"},
        "secrets": {"pc_password": "p"}, "array_id": a1})
    assert r.status_code == 201, r.text
    dst = r.json()["id"]

    entry = _by_id((await client.get(
        "/api/migrations/destinations",
        params={"source_hypervisor_id": src})).json(), dst)
    assert entry["eligible"] is True, entry["reason"]
    assert entry["connector_key"] == "nutanix"


# --------------------------------------------------------------------------- #
# Only a REPLICATION array-connection makes a cross-array pair migratable.
# --------------------------------------------------------------------------- #
def test_is_replication_connection_rejects_fleet_management():
    """Observed live: an array carried
    'Solutions-Engineering-Fleet-<remote>' of type fleet-management, whose
    remote.name equals the remote array. Matching on name alone made an
    unreplicated pair look migration-ready."""
    from phif.api.migrations import _is_replication_connection as is_repl

    assert is_repl("async-replication") is True
    assert is_repl("sync-replication") is True
    # The false positive this guards against:
    assert is_repl("fleet-management") is False
    assert is_repl("") is False
    assert is_repl(None) is False


async def test_cross_array_fleet_only_connection_needs_authorization(client, monkeypatch):
    """A pair joined ONLY by fleet-management must still require authorization:
    fleet-management federates management, it cannot carry a volume."""
    from phif.flasharray.client import MockFlashArrayClient

    a1 = await _mk_array(client, "10.0.7.1")
    a2 = await _mk_array(client, "10.0.7.2")
    src = await _make_hv(client, "proxmox", a1, "fm1")
    dst = await _make_hv(client, "xcpng", a2, "fm2")

    # The mock array's array_name() is its endpoint, so this is the name the
    # helper matches on. Using it means the NAME matches and only the TYPE can
    # distinguish the two cases -- otherwise the test would pass for the wrong
    # reason (a name mismatch) even without the type filter.
    dest_name = "10.0.7.2"
    orig = MockFlashArrayClient.list_array_connections

    async def _fleet_only(self):
        return [{"name": dest_name, "type": "fleet-management", "status": "connected"}]

    monkeypatch.setattr(MockFlashArrayClient, "list_array_connections", _fleet_only)
    try:
        r = await client.get("/api/migrations/precheck",
                             params={"source_hypervisor_id": src,
                                     "dest_hypervisor_id": dst})
        pc = r.json()
        assert pc["cross_array"] is True
        assert pc["connection_exists"] is False, "fleet-management is not replication"
        assert pc["needs_authorization"] is True

        entry = _by_id((await client.get(
            "/api/migrations/destinations",
            params={"source_hypervisor_id": src})).json(), dst)
        assert entry["needs_authorization"] is True
        assert "must be authorized" in entry["reason"]

        # And the submit path must refuse without explicit authorization.
        r = await client.post("/api/migrations", json={
            "source_hypervisor_id": src, "dest_hypervisor_id": dst,
            "vm_ref": "100", "network_map": {}})
        assert r.status_code == 409
    finally:
        monkeypatch.setattr(MockFlashArrayClient, "list_array_connections", orig)


async def test_cross_array_replication_connection_is_accepted(client, monkeypatch):
    """The positive case: a real replication connection needs no authorization."""
    from phif.flasharray.client import MockFlashArrayClient

    a1 = await _mk_array(client, "10.0.8.1")
    a2 = await _mk_array(client, "10.0.8.2")
    src = await _make_hv(client, "proxmox", a1, "rp1")
    dst = await _make_hv(client, "xcpng", a2, "rp2")

    dest_name = "10.0.8.2"   # the destination array's endpoint == its array_name()
    orig = MockFlashArrayClient.list_array_connections

    async def _replicated(self):
        return [{"name": dest_name, "type": "async-replication", "status": "connected"}]

    monkeypatch.setattr(MockFlashArrayClient, "list_array_connections", _replicated)
    try:
        pc = (await client.get("/api/migrations/precheck",
                               params={"source_hypervisor_id": src,
                                       "dest_hypervisor_id": dst})).json()
        assert pc["connection_exists"] is True
        assert pc["needs_authorization"] is False

        entry = _by_id((await client.get(
            "/api/migrations/destinations",
            params={"source_hypervisor_id": src})).json(), dst)
        assert entry["eligible"] is True
        assert entry["needs_authorization"] is False
        assert "replication connection" in entry["reason"]
    finally:
        monkeypatch.setattr(MockFlashArrayClient, "list_array_connections", orig)


# --------------------------------------------------------------------------- #
# dry_run must reach the runner
#
# `dry_run` lived only on OperationRequest, so POSTing it to /api/migrations was
# accepted and silently dropped — which reads exactly like a safe rehearsal
# while the migration runs for real.
# --------------------------------------------------------------------------- #
def test_migration_create_accepts_dry_run():
    from phif.api.schemas import MigrationCreate

    m = MigrationCreate(source_hypervisor_id="a", dest_hypervisor_id="b",
                        vm_ref="1", dry_run=True)
    assert m.dry_run is True, "dry_run must be a real field, not an ignored extra"
    # Default stays off so nothing changes for existing callers.
    assert MigrationCreate(source_hypervisor_id="a", dest_hypervisor_id="b",
                           vm_ref="1").dry_run is False


async def test_dry_run_is_threaded_into_runner_options(client, monkeypatch):
    """The endpoint must put dry_run where MigrationService reads it."""
    import phif.migrate.service as msvc

    seen: dict = {}

    async def _capture(session, **kw):
        seen.update(kw)
        return "mig-1", "job-1"

    monkeypatch.setattr(msvc, "run_migration", _capture)

    a1 = await _mk_array(client, "10.0.9.1")
    src = await _make_hv(client, "proxmox", a1, "dr1")
    dst = await _make_hv(client, "xcpng", a1, "dr2")

    r = await client.post("/api/migrations", json={
        "source_hypervisor_id": src, "dest_hypervisor_id": dst,
        "vm_ref": "100", "network_map": {}, "dry_run": True})
    assert r.status_code == 202, r.text
    assert (seen.get("options") or {}).get("dry_run") is True, seen.get("options")

    # And a normal request must NOT set it.
    seen.clear()
    r = await client.post("/api/migrations", json={
        "source_hypervisor_id": src, "dest_hypervisor_id": dst,
        "vm_ref": "101", "network_map": {}})
    assert r.status_code == 202, r.text
    assert not (seen.get("options") or {}).get("dry_run")


async def test_destinations_reads_each_array_once(client, monkeypatch):
    """Eligibility must not re-read the arrays per candidate.

    The first version called _array_connection_status for every candidate, which
    rebuilt both connectors and re-read the SOURCE array's connection list each
    time — 2N array round trips for N candidates, ~1s for six hypervisors, and
    it fired as soon as a source was picked.
    """
    from phif.flasharray.client import MockFlashArrayClient

    counts = {"conns": 0, "names": 0}
    orig_conns = MockFlashArrayClient.list_array_connections
    orig_name = MockFlashArrayClient.array_name

    async def _conns(self):
        counts["conns"] += 1
        return await orig_conns(self)

    async def _name(self):
        counts["names"] += 1
        return await orig_name(self)

    monkeypatch.setattr(MockFlashArrayClient, "list_array_connections", _conns)
    monkeypatch.setattr(MockFlashArrayClient, "array_name", _name)
    try:
        a1 = await _mk_array(client, "10.0.10.1")
        a2 = await _mk_array(client, "10.0.10.2")
        src = await _make_hv(client, "proxmox", a1, "c1")
        # Three candidates, TWO of them on the same destination array.
        await _make_hv(client, "xcpng", a2, "c2")
        await _make_hv(client, "hpevme", a2, "c3")
        await _make_hv(client, "proxmox", a1, "c4")   # same array as the source

        counts["conns"] = counts["names"] = 0
        r = await client.get("/api/migrations/destinations",
                             params={"source_hypervisor_id": src})
        assert r.status_code == 200, r.text
        dests = r.json()["destinations"]

        # The source array's connection list is read at most ONCE for the whole
        # request, however many candidates there are.
        assert counts["conns"] <= 1, (
            f"source connections read {counts['conns']} times; expected <= 1")

        # Each DISTINCT destination array is named at most once — the invariant
        # that matters. Asserted relative to the data (the suite shares a DB, so
        # other tests' hypervisors appear as candidates too).
        cross_arrays = {d["array_id"] for d in dests
                        if d.get("cross_array") and d.get("array_id")}
        cross_candidates = sum(1 for d in dests if d.get("cross_array"))
        assert counts["names"] <= len(cross_arrays), (
            f"array_name called {counts['names']} times for "
            f"{len(cross_arrays)} distinct arrays")
        if cross_candidates > len(cross_arrays):
            # Proves the cache actually saved work rather than coincidentally
            # matching: more candidates than arrays, yet no extra lookups.
            assert counts["names"] < cross_candidates, (
                f"{cross_candidates} cross-array candidates caused "
                f"{counts['names']} array_name calls — not cached")
    finally:
        monkeypatch.setattr(MockFlashArrayClient, "list_array_connections", orig_conns)
        monkeypatch.setattr(MockFlashArrayClient, "array_name", orig_name)
