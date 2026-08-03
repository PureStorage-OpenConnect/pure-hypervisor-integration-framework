"""Foundation tests: contract, registry, vault, FlashArray client, example connector."""

import pytest

from phif.connectors.base import Capability, HypervisorConnector, OpResult
from phif.connectors.registry import discover, get_connector_class, list_descriptors
from phif.connectors.example.connector import ExampleConnector
from phif.flasharray.client import MockFlashArrayClient, _to_bytes
from phif.vault import FernetVault


# ---- registry / discovery ----
def test_example_connector_is_discovered():
    registry = discover()
    assert "example" in registry
    assert get_connector_class("example") is ExampleConnector


def test_descriptors_serialize():
    descriptors = list_descriptors()
    example = next(d for d in descriptors if d["key"] == "example")
    assert "connect" in example["capabilities"]  # CONNECT always present
    assert any(a["id"] == "provision" for a in example["actions"])
    assert example["target_schema"][0]["name"] == "host"


# ---- contract ----
def test_capabilities_always_include_connect():
    assert Capability.CONNECT in ExampleConnector.capabilities()


async def test_unsupported_capability_raises():
    """A connector that doesn't override an op inherits a method that raises."""
    from phif.connectors.base import (
        CapabilityNotSupported,
        ConnectorContext,
        HypervisorTarget,
    )
    from phif.jobs.runner import JobRunner

    class Bare(HypervisorConnector):
        key = "bare"
        name = "Bare"

        async def validate_connection(self):
            return OpResult.ok()

    async def emit(_):
        return None

    ctx = ConnectorContext(
        target=HypervisorTarget(id="x", connector_key="bare", name="b"),
        log=emit, runner=JobRunner(emit),
    )
    with pytest.raises(CapabilityNotSupported):
        await Bare(ctx).snapshot(volume="v")


# ---- vault ----
def test_vault_roundtrip():
    v = FernetVault(master_key=FernetVault.generate_key())
    blob = v.encrypt({"api_token": "abc", "n": 1})
    assert v.decrypt(blob) == {"api_token": "abc", "n": 1}


def test_vault_rejects_bad_key():
    with pytest.raises(ValueError):
        FernetVault(master_key="not-a-valid-fernet-key")


# ---- flasharray helpers ----
@pytest.mark.parametrize("value,expected", [
    ("1T", 1024**4), ("500G", 500 * 1024**3), ("10M", 10 * 1024**2), (12345, 12345),
])
def test_to_bytes(value, expected):
    assert _to_bytes(value) == expected


# ---- example connector end-to-end (mock) ----
async def test_example_connector_flow(make_context):
    ctx = make_context()
    c = ExampleConnector(ctx)

    assert (await c.validate_connection()).success
    assert (await c.deploy_integration()).success

    r = await c.register_hosts(host_group="hg1", iqns="iqn.a, iqn.b")
    assert r.success and r.artifacts["host_group"] == "hg1"

    r = await c.provision(name="vol1", size="2T", host_group="hg1")
    assert r.success
    assert "vol1" in ctx.array.volumes

    assert (await c.snapshot(volume="vol1")).success
    assert (await c.clone(source="vol1", dest="vol2")).success
    assert "vol2" in ctx.array.volumes
    assert (await c.resize(volume="vol1", size="3T")).success
    assert (await c.health_check()).success


async def test_dispatch_routes_actions(make_context):
    ctx = make_context()
    c = ExampleConnector(ctx)
    result = await c.dispatch("provision", {"name": "dvol", "size": "1T"})
    assert result.success
    assert "dvol" in ctx.array.volumes


async def test_dispatch_unknown_action(make_context):
    c = ExampleConnector(make_context())
    result = await c.dispatch("nope", {})
    assert not result.success


async def test_mock_array_data_interfaces_and_target_ports():
    arr = MockFlashArrayClient()
    assert await arr.get_data_interfaces("iscsi")  # non-empty
    assert await arr.get_data_interfaces("nvme-tcp")
    assert await arr.get_data_interfaces("nfs") == []
    ports = await arr.get_target_ports()
    assert ports["iqn"].startswith("iqn.")
    assert ports["nqn"].startswith("nqn.")
    assert len(ports["wwns"]) >= 1


async def test_mock_array_records_calls():
    arr = MockFlashArrayClient()
    await arr.create_volume("v", "1T")
    await arr.create_api_token("svc-user")
    assert arr.api_tokens["svc-user"].startswith("mock-token-")
    assert ("create_volume", {"name": "v", "size": "1T"}) in arr.calls


async def test_create_host_group_is_idempotent_and_additive():
    arr = MockFlashArrayClient()
    # Re-running registration must not fail and must converge to merged members.
    await arr.create_host_group("hg", ["h1"])
    await arr.create_host_group("hg", ["h1", "h2"])  # h1 already a member
    assert arr.host_groups["hg"]["hosts"] == ["h1", "h2"]


async def test_create_host_is_idempotent_and_merges_initiators():
    arr = MockFlashArrayClient()
    await arr.create_host("h1", wwns=["21000024ff000001"])
    await arr.create_host("h1", wwns=["21000024ff000001", "21000024ff000002"])
    assert arr.hosts["h1"]["wwns"] == ["21000024ff000001", "21000024ff000002"]


async def test_runner_discovers_initiators_in_mock_mode(log_emitter):
    from phif.jobs.runner import JobRunner

    runner = JobRunner(log_emitter)  # mock mode is on in conftest
    found = await runner.discover_initiators("node1.test", username="root")
    assert found["iqn"].startswith("iqn.")
    assert found["nqn"].startswith("nqn.")
    assert len(found["wwns"]) >= 1


@pytest.mark.parametrize("kind", ["nics", "nvme_sources", "fc_hbas"])
async def test_runner_discovers_interfaces_in_mock_mode(log_emitter, kind):
    from phif.jobs.runner import JobRunner

    runner = JobRunner(log_emitter)
    opts = await runner.discover_interfaces("node1.test", kind, username="root")
    assert len(opts) >= 1
    assert all("value" in o and "label" in o for o in opts)


def test_compare_node_interfaces():
    from phif.connectors.base import compare_node_interfaces

    ok, _ = compare_node_interfaces({"n1": ["eth0", "eth1"], "n2": ["eth1", "eth0"]})
    assert ok
    bad, detail = compare_node_interfaces({"n1": ["eth0"], "n2": ["eth0", "eth1"]})
    assert not bad and "differ" in detail
    assert compare_node_interfaces({"n1": ["eth0"]})[0]  # single node always ok


def test_nics_on_common_subnets():
    from phif.connectors.base import nics_on_common_subnets

    # Candidate NICs on the connection host.
    host = [
        {"value": "eth0", "cidr": "10.10.10.5/24"},   # storage subnet
        {"value": "eth1", "cidr": "10.20.20.5/24"},   # only on node1
        {"value": "ens3", "cidr": "192.168.9.5/24"},  # mgmt — only on node1
    ]
    per_node = {
        # node1 has all three subnets; node2 has ONLY the 10.10.10.0/24 subnet.
        "node1": host,
        "node2": [{"value": "eno1", "cidr": "10.10.10.8/24"}],
    }
    kept = {n["value"] for n in nics_on_common_subnets(host, per_node)}
    # Only the subnet configured on EVERY node survives.
    assert kept == {"eth0"}

    # Single node (or missing per-node data) -> unchanged.
    assert nics_on_common_subnets(host, {"node1": host}) == host
    assert nics_on_common_subnets(host, {"n1": host, "n2": []}) == host
    # No common subnet at all -> fall back to candidates (don't strand the user).
    none_common = nics_on_common_subnets(
        host, {"a": [{"value": "x", "cidr": "10.10.10.5/24"}],
               "b": [{"value": "y", "cidr": "172.16.0.5/24"}]})
    assert none_common == host


async def test_default_list_nodes_single(make_context):
    # The reference connector has no cluster -> a single node from the connection.
    from phif.connectors.example.connector import ExampleConnector

    c = ExampleConnector(make_context(connection={"host": "mgr.test"}))
    nodes = await c.list_nodes()
    assert len(nodes) == 1 and nodes[0].host == "mgr.test"
    vc = await c.validate_cluster()
    assert vc.success


def test_descriptor_includes_wizard_steps():
    from phif.connectors.example.connector import ExampleConnector

    d = ExampleConnector.descriptor()
    assert isinstance(d["wizard_steps"], list) and d["wizard_steps"]


def test_resolve_token_prefers_explicit_then_array_token():
    from phif.connectors.base import ConnectorContext, HypervisorTarget
    from phif.jobs.runner import JobRunner

    async def emit(_):
        return None

    ctx = ConnectorContext(
        target=HypervisorTarget(id="x", connector_key="k", name="n"),
        log=emit, runner=JobRunner(emit), array_token="array-orig-token",
    )
    assert ctx.resolve_token("explicit") == "explicit"  # explicit wins
    assert ctx.resolve_token("") == "array-orig-token"  # else the array's token
    assert ctx.resolve_token() == "array-orig-token"


def test_multiselect_field_and_options_source_serialize():
    from phif.connectors.base import FieldType, FormField

    f = FormField("nics", "NICs", FieldType.MULTISELECT, options_source="nics")
    d = f.to_dict()
    assert d["type"] == "multiselect"
    assert d["options_source"] == "nics"
