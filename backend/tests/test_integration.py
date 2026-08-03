"""Cross-connector integration: drive every real connector through the full
HTTP -> job-engine -> connector -> FlashArray stack in mock mode.

Uses the universal `health_check` action (present on all connectors, no required
params) so this stays connector-agnostic while still exercising dispatch, the
job manager, log streaming persistence, and the FlashArray client for each one.
"""

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from phif.connectors.registry import discover
from phif.main import create_app

# Every shipped connector except the reference/example one.
REAL_CONNECTORS = sorted(k for k in discover() if k != "example")


@pytest.fixture
async def client():
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        async with app.router.lifespan_context(app):
            yield c


# Generous ceiling (returns as soon as the job is terminal). Background jobs share
# one in-memory-DB connection pool + event loop across the suite, so a job can be
# briefly starved under load — a short budget makes this a flaky timeout.
async def _wait_job(client, job_id, tries=600):
    for _ in range(tries):
        r = await client.get(f"/api/jobs/{job_id}")
        if r.json()["status"] in ("succeeded", "failed"):
            return r.json()
        await asyncio.sleep(0.03)
    return r.json()


def test_all_real_connectors_present():
    # The six target hypervisors must all be discovered.
    assert set(REAL_CONNECTORS) == {
        "vsphere", "openshift", "openstack", "proxmox", "xcpng", "hpevme"
    }


@pytest.mark.parametrize("connector_key", REAL_CONNECTORS)
async def test_connector_end_to_end_health(client, connector_key):
    # 1. array
    r = await client.post("/api/arrays", json={
        "name": f"fa-{connector_key}", "mgmt_endpoint": "10.0.0.10", "api_token": "tok"})
    assert r.status_code == 201, r.text
    array_id = r.json()["id"]

    # 2. hypervisor for this connector (minimal connection/secrets; mock mode)
    r = await client.post("/api/hypervisors", json={
        "name": f"hv-{connector_key}", "connector_key": connector_key,
        "connection": {"host": "mgr.test", "vcenter_host": "vc.test",
                       "node_host": "pve.test", "pool_master_host": "xcp.test",
                       "controller_host": "osc.test", "vme_manager_url": "https://vme.test"},
        "secrets": {"password": "p", "vcenter_password": "p", "ssh_password": "p",
                    "api_token": "tok",
                    "kubeconfig": "apiVersion: v1\nkind: Config\nclusters: []\n"},
        "array_id": array_id})
    assert r.status_code == 201, r.text
    hv_id = r.json()["id"]

    # 3. health_check action through the job engine
    r = await client.post(f"/api/hypervisors/{hv_id}/operations", json={
        "action_id": "health_check", "params": {}})
    assert r.status_code == 202, r.text
    job = await _wait_job(client, r.json()["job_id"])
    assert job["status"] == "succeeded", f"{connector_key}: {job}"
