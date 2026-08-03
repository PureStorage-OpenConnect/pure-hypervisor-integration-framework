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
