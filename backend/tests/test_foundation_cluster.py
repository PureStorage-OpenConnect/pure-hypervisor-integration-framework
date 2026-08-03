"""Foundation tests for cluster reconcile + monitoring + NFS scaffolding.

Covers the shared pieces the per-connector work builds on: the FlashArray client's
host-removal + NFS methods, the base `_prune_departed_fa_hosts` helper, the
settings store, and the settings API.
"""

import pytest
from httpx import ASGITransport, AsyncClient

from phif.connectors.base import HypervisorConnector
from phif.connectors.example.connector import ExampleConnector
from phif.flasharray.client import MockFlashArrayClient
from phif.main import create_app


# ---- FlashArray client: host removal ----
async def test_mock_host_group_membership_and_removal():
    fa = MockFlashArrayClient()
    await fa.create_host("h1", iqns=["iqn.a"])
    await fa.create_host("h2", iqns=["iqn.b"])
    await fa.create_host_group("hg", ["h1", "h2"])
    assert sorted(await fa.get_host_group_members("hg")) == ["h1", "h2"]

    await fa.remove_host_from_group("hg", "h2")
    assert await fa.get_host_group_members("hg") == ["h1"]
    await fa.delete_host("h2")
    assert "h2" not in fa.hosts


# ---- FlashArray client: NFS / file ----
async def test_mock_nfs_export_lifecycle():
    fa = MockFlashArrayClient()
    await fa.create_filesystem("fs1")
    exp = await fa.create_nfs_export("vme-iso", "fs1", "/iso", policy="open")
    assert exp["policy"] == "open"
    assert await fa.get_nfs_exports("fs1") == [exp]
    assert await fa.get_nfs_data_interfaces()  # non-empty portal list

    await fa.delete_nfs_export("vme-iso")
    assert await fa.get_nfs_exports("fs1") == []
    await fa.delete_filesystem("fs1", eradicate=True)
    assert "fs1" not in fa.filesystems


# ---- base _prune_departed_fa_hosts (via the reference connector) ----
async def test_prune_departed_flags_without_removing(make_context):
    ctx = make_context(connector_key="example")
    await ctx.array.create_host("hg-n1")
    await ctx.array.create_host("hg-n2")
    await ctx.array.create_host_group("hg", ["hg-n1", "hg-n2"])
    c = ExampleConnector(ctx)

    # n2 departed (expected only has n1). Without apply: flagged, not removed.
    summary = await c._prune_departed_fa_hosts("hg", {"hg-n1"}, apply_removals=False)
    assert summary["departed"] == ["hg-n2"]
    assert summary["removed"] == []
    assert "hg-n2" in ctx.array.hosts  # still present


def test_score_host_readiness():
    # Ready iSCSI host: reachable, has IQN, NIC on the array's portal subnet.
    portals = ["10.10.10.10"]
    nics = [{"value": "eth0", "cidr": "10.10.10.5/24"}]
    r = HypervisorConnector.score_host_readiness(
        "iscsi", reachable=True, initiators={"iqn": "iqn.a"},
        host_nics=nics, array_portals=portals)
    assert r["ready"] is True and r["reasons"] == []

    # Unreachable -> not ready, short-circuits.
    r = HypervisorConnector.score_host_readiness(
        "iscsi", reachable=False, initiators={}, host_nics=[], array_portals=portals)
    assert r["ready"] is False and "unreachable" in r["reasons"][0]

    # No IQN + no storage NIC on subnet -> two reasons.
    r = HypervisorConnector.score_host_readiness(
        "iscsi", reachable=True, initiators={},
        host_nics=[{"value": "eth9", "cidr": "192.168.0.5/24"}], array_portals=portals)
    assert r["ready"] is False
    assert any("IQN" in x for x in r["reasons"])
    assert any("portal subnet" in x for x in r["reasons"])

    # FC: needs WWNs, no subnet requirement.
    r = HypervisorConnector.score_host_readiness(
        "fc", reachable=True, initiators={"wwns": ["21:00:..."]},
        host_nics=[], array_portals=[])
    assert r["ready"] is True

    # Subnet mismatch vs baseline.
    r = HypervisorConnector.score_host_readiness(
        "iscsi", reachable=True, initiators={"iqn": "iqn.a"},
        host_nics=nics, array_portals=portals, baseline_subnets={"10.99.0.0/24"})
    assert r["ready"] is False
    assert any("doesn't match existing" in x for x in r["reasons"])


async def test_prune_departed_removes_when_applied(make_context):
    ctx = make_context(connector_key="example")
    await ctx.array.create_host("hg-n1")
    await ctx.array.create_host("hg-n2")
    await ctx.array.create_host_group("hg", ["hg-n1", "hg-n2"])
    c = ExampleConnector(ctx)

    summary = await c._prune_departed_fa_hosts("hg", {"hg-n1"}, apply_removals=True)
    assert summary["removed"] == ["hg-n2"]
    assert "hg-n2" not in ctx.array.hosts
    assert await ctx.array.get_host_group_members("hg") == ["hg-n1"]


# ---- settings API ----
@pytest.fixture
async def client():
    app = create_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as c:
        async with app.router.lifespan_context(app):
            yield c


async def test_settings_monitoring_defaults_and_patch(client):
    r = await client.get("/api/settings")
    assert r.status_code == 200
    mon = r.json()["monitoring"]
    assert mon["enabled"] is False
    assert mon["interval_seconds"] == 300

    r = await client.patch("/api/settings/monitoring",
                           json={"enabled": True, "interval_seconds": 60})
    assert r.status_code == 200
    assert r.json() == {"enabled": True, "interval_seconds": 60}

    # Floor clamp: interval can't go below 30.
    r = await client.patch("/api/settings/monitoring", json={"interval_seconds": 5})
    assert r.json()["interval_seconds"] == 30
    assert r.json()["enabled"] is True  # unchanged


async def test_per_hypervisor_monitoring_optout(client):
    # Create an array + hypervisor, then toggle its monitoring opt-out.
    r = await client.post("/api/arrays", json={
        "name": "fa-mon", "mgmt_endpoint": "10.0.0.10", "api_token": "tok"})
    array_id = r.json()["id"]
    r = await client.post("/api/hypervisors", json={
        "name": "hv-mon", "connector_key": "proxmox",
        "connection": {"node_host": "pve.test"},
        "secrets": {"ssh_password": "p"}, "array_id": array_id})
    hv_id = r.json()["id"]

    r = await client.patch(f"/api/hypervisors/{hv_id}/monitoring",
                           json={"enabled": False})
    assert r.status_code == 200
    assert r.json()["state"]["monitoring_disabled"] is True

    r = await client.patch(f"/api/hypervisors/{hv_id}/monitoring",
                           json={"enabled": True})
    assert r.json()["state"]["monitoring_disabled"] is False
