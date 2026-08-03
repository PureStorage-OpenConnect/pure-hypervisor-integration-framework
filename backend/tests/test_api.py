"""End-to-end API smoke test against the full app in mock mode."""

import asyncio

import pytest
from httpx import ASGITransport, AsyncClient

from phif.main import create_app


@pytest.fixture
async def client():
    app = create_app()
    # Trigger lifespan (table creation) via the transport.
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        async with app.router.lifespan_context(app):
            yield c


async def test_health(client):
    r = await client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["mock_mode"] is True


async def test_connectors_catalog(client):
    r = await client.get("/api/connectors")
    assert r.status_code == 200
    keys = {c["key"] for c in r.json()}
    assert "example" in keys


async def test_full_flow(client):
    # 1. Connect an array (mock — always succeeds).
    r = await client.post("/api/arrays", json={
        "name": "fa1", "mgmt_endpoint": "10.0.0.10", "api_token": "tok"})
    assert r.status_code == 201, r.text
    array_id = r.json()["id"]

    # 2. Mint an API key for an integration.
    r = await client.post("/api/api-keys", json={
        "array_id": array_id, "array_user": "csi-user", "purpose": "openshift-csi"})
    assert r.status_code == 201, r.text
    assert r.json()["token"].startswith("mock-token-")

    # 3. Add a hypervisor using the example connector.
    r = await client.post("/api/hypervisors", json={
        "name": "hv1", "connector_key": "example",
        "connection": {"host": "mgr.test", "username": "admin"},
        "secrets": {"password": "p"}, "array_id": array_id})
    assert r.status_code == 201, r.text
    hv_id = r.json()["id"]

    # 4. Validate connection.
    r = await client.post(f"/api/hypervisors/{hv_id}/validate")
    assert r.status_code == 200 and r.json()["success"]

    # 5. Run a provision operation (async job).
    r = await client.post(f"/api/hypervisors/{hv_id}/operations", json={
        "action_id": "provision", "params": {"name": "vol1", "size": "1T"}})
    assert r.status_code == 202, r.text
    job_id = r.json()["job_id"]

    # 6. Poll the job to completion.
    for _ in range(50):
        r = await client.get(f"/api/jobs/{job_id}")
        if r.json()["status"] in ("succeeded", "failed"):
            break
        await asyncio.sleep(0.02)
    body = r.json()
    assert body["status"] == "succeeded", body
    assert "vol1" in body["logs"]


async def test_api_key_reuse_existing_token(client):
    # Connect an array with a token, then reuse that token (no minting).
    r = await client.post("/api/arrays", json={
        "name": "fa-reuse", "mgmt_endpoint": "10.0.0.40", "api_token": "the-original-token"})
    array_id = r.json()["id"]
    r = await client.post("/api/api-keys", json={
        "array_id": array_id, "purpose": "openshift-csi", "use_existing": True})
    assert r.status_code == 201, r.text
    body = r.json()
    # The returned token must be the array's original token, not a freshly minted one.
    assert body["token"] == "the-original-token"
    assert body["purpose"] == "openshift-csi"


async def test_wizard_full_deploy(client):
    # Connect an array, then one-shot wizard-deploy the example connector.
    r = await client.post("/api/arrays", json={
        "name": "fa-wiz", "mgmt_endpoint": "10.0.0.60", "api_token": "tok"})
    array_id = r.json()["id"]
    r = await client.post("/api/hypervisors/wizard", json={
        "name": "wiz-hv", "connector_key": "example", "array_id": array_id,
        "connection": {"host": "mgr.test"}, "secrets": {"password": "p"},
        "scope": "cluster", "params": {"host_group": "hg", "name": "vol1", "size": "1T"}})
    assert r.status_code == 202, r.text
    body = r.json()
    hv_id, job_id = body["hypervisor_id"], body["job_id"]

    for _ in range(80):
        jr = await client.get(f"/api/jobs/{job_id}")
        if jr.json()["status"] in ("succeeded", "failed"):
            break
        await asyncio.sleep(0.02)
    jb = jr.json()
    assert jb["status"] == "succeeded", jb
    assert "wizard complete" in jb["logs"]

    # The hypervisor was created and its node list is reachable.
    r = await client.get(f"/api/hypervisors/{hv_id}/nodes")
    assert r.status_code == 200 and len(r.json()["nodes"]) == 1


async def test_wizard_two_step_prepare_then_run(client):
    r = await client.post("/api/arrays", json={
        "name": "fa-wiz2", "mgmt_endpoint": "10.0.0.61", "api_token": "tok"})
    array_id = r.json()["id"]
    # Page 1: prepare (create + validate).
    r = await client.post("/api/hypervisors/wizard/prepare", json={
        "name": "wiz2-hv", "connector_key": "example", "array_id": array_id,
        "connection": {"host": "mgr.test"}, "secrets": {"password": "p"}})
    assert r.status_code == 201, r.text
    hv_id = r.json()["hypervisor_id"]
    # Page 2a: discover nodes (for the interface-selection page).
    r = await client.get(f"/api/hypervisors/{hv_id}/nodes")
    assert r.status_code == 200 and len(r.json()["nodes"]) == 1
    # Page 2b: deploy with selected params.
    r = await client.post(f"/api/hypervisors/{hv_id}/wizard/run", json={
        "scope": "cluster", "params": {"host_group": "hg", "name": "v1", "size": "1T"}})
    assert r.status_code == 202, r.text
    job_id = r.json()["job_id"]
    for _ in range(80):
        jr = await client.get(f"/api/jobs/{job_id}")
        if jr.json()["status"] in ("succeeded", "failed"):
            break
        await asyncio.sleep(0.02)
    assert jr.json()["status"] == "succeeded", jr.json()


async def test_wizard_prepare_duplicate_name_is_clean_error(client):
    r = await client.post("/api/arrays", json={
        "name": "fa-dup", "mgmt_endpoint": "10.0.0.62", "api_token": "tok"})
    array_id = r.json()["id"]
    body = {"name": "dup-hv", "connector_key": "example", "array_id": array_id,
            "connection": {"host": "mgr.test"}, "secrets": {"password": "p"}}
    r = await client.post("/api/hypervisors/wizard/prepare", json=body)
    assert r.status_code == 201, r.text
    # Re-using the name must return a clean 400 (not a 500 from the DB).
    r = await client.post("/api/hypervisors/wizard/prepare", json=body)
    assert r.status_code == 400, r.text
    assert "already exists" in r.json()["detail"]
    # Exactly one record exists for that name.
    r = await client.get("/api/hypervisors")
    assert sum(h["name"] == "dup-hv" for h in r.json()) == 1


async def test_reconcile_orphaned_jobs(client):
    # A job left "running"/"pending" (its worker died) must be reconciled to
    # "failed" so it doesn't linger forever and make the UI look hung.
    # (Uses the `client` fixture so the schema is created.)
    from phif.db.models import Job
    from phif.db.session import SessionLocal
    from phif.jobs.reconcile import reconcile_orphaned_jobs

    async with SessionLocal() as s:
        s.add(Job(action="deploy", status="running", logs="started"))
        s.add(Job(action="wizard", status="pending", logs=""))
        s.add(Job(action="health_check", status="succeeded", logs="done"))
        await s.commit()
    async with SessionLocal() as s:
        n = await reconcile_orphaned_jobs(s)
        assert n == 2
    async with SessionLocal() as s:
        from sqlalchemy import select
        jobs = (await s.execute(select(Job))).scalars().all()
        by_action = {j.action: j.status for j in jobs}
        assert by_action["deploy"] == "failed"
        assert by_action["wizard"] == "failed"
        assert by_action["health_check"] == "succeeded"  # untouched


async def test_discover_endpoint(client):
    # Add an array + hypervisor, then hit the discovery endpoint.
    r = await client.post("/api/arrays", json={
        "name": "fa-disc", "mgmt_endpoint": "10.0.0.30", "api_token": "tok"})
    array_id = r.json()["id"]
    r = await client.post("/api/hypervisors", json={
        "name": "hv-disc", "connector_key": "example", "array_id": array_id})
    hv_id = r.json()["id"]
    # The example connector returns [] by default; the endpoint must still 200.
    r = await client.get(f"/api/hypervisors/{hv_id}/discover/nics")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["kind"] == "nics" and isinstance(body["options"], list)


async def test_update_hypervisor(client):
    r = await client.post("/api/arrays", json={
        "name": "fa-edit", "mgmt_endpoint": "10.0.0.50", "api_token": "tok"})
    array_id = r.json()["id"]
    r = await client.post("/api/hypervisors", json={
        "name": "hv-edit", "connector_key": "example",
        "connection": {"host": "old.test", "username": "admin"},
        "secrets": {"password": "orig"}})
    hv_id = r.json()["id"]

    # Patch name + connection + attach array; omit secrets (must be preserved).
    r = await client.patch(f"/api/hypervisors/{hv_id}", json={
        "name": "hv-edited", "connection": {"host": "new.test", "username": "admin"},
        "array_id": array_id})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["name"] == "hv-edited"
    assert body["connection"]["host"] == "new.test"
    assert body["array_id"] == array_id

    # The hypervisor must still validate (secret preserved) and run ops.
    r = await client.post(f"/api/hypervisors/{hv_id}/validate")
    assert r.status_code == 200 and r.json()["success"]

    # Detach the array with array_id="".
    r = await client.patch(f"/api/hypervisors/{hv_id}", json={"array_id": ""})
    assert r.status_code == 200 and r.json()["array_id"] is None


async def test_reject_unknown_action(client):
    r = await client.post("/api/arrays", json={
        "name": "fa2", "mgmt_endpoint": "10.0.0.11", "api_token": "tok"})
    array_id = r.json()["id"]
    r = await client.post("/api/hypervisors", json={
        "name": "hv2", "connector_key": "example", "array_id": array_id})
    hv_id = r.json()["id"]
    r = await client.post(f"/api/hypervisors/{hv_id}/operations", json={
        "action_id": "does_not_exist", "params": {}})
    assert r.status_code == 400
