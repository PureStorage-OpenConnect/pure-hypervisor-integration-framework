"""Unit tests for the VMware vSphere connector (mock array + runner)."""

import pytest

from phif.connectors.base import Capability, Protocol
from phif.connectors.registry import discover, get_connector_class
from phif.connectors.vsphere.connector import VSphereConnector


VSPHERE_CONN = {
    "vcenter_host": "vcenter.example.com",
    "vcenter_user": "administrator@vsphere.local",
    "datacenter": "DC1",
    "cluster": "Cluster1",
}
VSPHERE_SECRETS = {"vcenter_password": "s3cret"}


def _ctx(make_context, **kw):
    return make_context(
        connector_key="vsphere",
        connection=dict(VSPHERE_CONN),
        secrets=dict(VSPHERE_SECRETS),
        **kw,
    )


# ---- discovery / metadata ----
def test_vsphere_is_discovered():
    assert "vsphere" in discover()
    assert get_connector_class("vsphere") is VSphereConnector


def test_descriptor_metadata():
    d = VSphereConnector.descriptor()
    assert d["key"] == "vsphere"
    assert d["name"] == "VMware vSphere"
    assert d["maturity"] == "ga"
    caps = set(d["capabilities"])
    for c in ("connect", "deploy_plugin", "configure", "host_register", "connectivity",
              "provision_datastore", "provision_volume", "snapshot", "clone", "resize",
              "qos", "replication", "health", "remove"):
        assert c in caps
    assert set(d["protocols"]) == {"iscsi", "fc", "nvme-fc", "nvme-roce"}
    assert d["target_schema"][0]["name"] == "vcenter_host"
    # password field is a secret
    pw = next(f for f in d["target_schema"] if f["name"] == "vcenter_password")
    assert pw["type"] == "secret"


def test_protocols_and_capabilities():
    assert Protocol.NVME_FC in VSphereConnector.SUPPORTED_PROTOCOLS
    assert Capability.CONNECT in VSphereConnector.capabilities()
    assert Capability.PROVISION_DATASTORE in VSphereConnector.capabilities()


def test_action_ids_present():
    ids = {a.id for a in VSphereConnector.action_schemas()}
    assert {"deploy", "configure", "register_hosts", "setup_connectivity",
            "provision_datastore", "provision", "snapshot", "clone", "resize",
            "set_qos", "configure_replication", "health_check", "teardown"} <= ids


# ---- validate_connection ----
async def test_validate_connection(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.validate_connection()
    assert r.success
    assert r.artifacts["vcenter"] == "vcenter.example.com"
    assert r.data["array"]["name"] == "mock-array"


async def test_validate_connection_no_host(make_context):
    from phif.connectors.base import ConnectionValidationError
    ctx = _ctx(make_context)
    ctx.target.connection["vcenter_host"] = ""
    with pytest.raises(ConnectionValidationError):
        await VSphereConnector(ctx).validate_connection()


# ---- deploy + configure (plugin + VASA) ----
async def test_deploy_registers_plugin_and_vasa(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.deploy_integration(plugin_url="https://plugin.local/plugin.json")
    assert r.success
    assert r.artifacts["plugin"] == "com.purestorage.purestoragehtml"
    assert "vasa_url" in r.artifacts and r.artifacts["vasa_url"].endswith(":8084")


async def test_configure_refreshes_vasa(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.configure(provider_name="Everpure-VASA-test")
    assert r.success
    assert r.artifacts["vasa_provider"] == "Everpure-VASA-test"


# ---- host registration / connectivity ----
async def test_register_hosts_creates_group(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-vsphere", iqns="iqn.a, iqn.b")
    assert r.success
    assert "hg-vsphere" in ctx.array.host_groups
    assert "hg-vsphere" in ctx.array.hosts  # implicit host carrying initiators


async def test_register_hosts_no_array(make_context):
    c = VSphereConnector(_ctx(make_context, with_array=False))
    r = await c.register_hosts(host_group="hg")
    assert not r.success


# ---- Fibre Channel (FC) host registration ----
async def test_register_hosts_fc_uses_wwns(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(
        host_group="hg-fc", protocol="fc", wwns="21:00:00:24:ff:aa:bb:cc, 21:00:00:24:ff:dd:ee:ff",
    )
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    assert "hg-fc" in ctx.array.host_groups
    # The implicit host must carry the FC WWNs, not IQNs/NQNs.
    host = ctx.array.hosts["hg-fc"]
    assert host["wwns"] == ["21:00:00:24:ff:aa:bb:cc", "21:00:00:24:ff:dd:ee:ff"]
    assert host["iqns"] == [] and host["nqns"] == []


async def test_register_hosts_fc_ignores_iqns(make_context):
    """FC must register by WWN even if stray IQNs are also supplied."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(
        host_group="hg-fc2", protocol="fc",
        iqns="iqn.should.be.ignored", wwns="21:00:00:24:ff:00:11:22",
    )
    assert r.success
    host = ctx.array.hosts["hg-fc2"]
    assert host["wwns"] == ["21:00:00:24:ff:00:11:22"]
    assert host["iqns"] == []


async def test_register_hosts_fc_auto_discovers_wwns(make_context):
    """FC with no WWNs typed: initiators are auto-discovered and fanned out
    across the cluster's ESXi hosts."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-fc3", protocol="fc")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    # Cluster fan-out: one array host per discovered ESXi host (mock => 2).
    assert len(r.artifacts["hosts"]) == 2
    for hostname in r.artifacts["hosts"]:
        host = ctx.array.hosts[hostname]
        # Discovered (synthetic, mock-mode) WWNs reached the array; no IQN/NQN.
        assert host["wwns"]
        assert all(w.startswith("21:") for w in host["wwns"])
        assert host["iqns"] == [] and host["nqns"] == []


# ---- auto-discovery of ESXi initiators from vCenter ----
async def test_register_hosts_auto_discovers_iqns(make_context):
    """iSCSI with no IQNs typed: discovered IQNs reach the mock array, fanned
    out across the cluster's ESXi hosts."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-auto", protocol="iscsi")
    assert r.success
    assert len(r.artifacts["hosts"]) == 2  # cluster fan-out (mock => 2 ESXi hosts)
    assert "hg-auto" in ctx.array.host_groups
    for hostname in r.artifacts["hosts"]:
        host = ctx.array.hosts[hostname]
        assert host["iqns"]
        assert all(i.startswith("iqn.") for i in host["iqns"])
        assert host["wwns"] == [] and host["nqns"] == []


async def test_register_hosts_explicit_overrides_discovery(make_context):
    """Explicitly typed initiators win over auto-discovery."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-ovr", protocol="iscsi", iqns="iqn.explicit.a")
    assert r.success
    assert ctx.array.hosts["hg-ovr"]["iqns"] == ["iqn.explicit.a"]


async def test_register_hosts_nvme_auto_discovers_nqns(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-nvme", protocol="nvme-fc")
    assert r.success
    assert len(r.artifacts["hosts"]) == 2  # cluster fan-out
    for hostname in r.artifacts["hosts"]:
        host = ctx.array.hosts[hostname]
        assert host["nqns"]
        assert all(n.startswith("nqn.") for n in host["nqns"])
        assert host["iqns"] == [] and host["wwns"] == []


async def test_setup_connectivity_auto_discovers(make_context):
    """setup_connectivity with no per-host initiators discovers from vCenter."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(host_group="hg-disc", protocol="iscsi", hosts="esx1,esx2")
    assert r.success
    assert ctx.array.hosts["esx1"]["iqns"]
    assert ctx.array.hosts["esx2"]["iqns"]


async def test_register_hosts_dry_run_no_changes(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-dry", protocol="iscsi")
    assert r.success
    assert ctx.array.hosts == {}  # no host created in dry-run


async def test_setup_connectivity_iscsi(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg1", protocol="iscsi",
        hosts="esx1,esx2", initiators="esx1=iqn.1,iqn.2;esx2=iqn.3",
    )
    assert r.success
    assert "esx1" in ctx.array.hosts and "esx2" in ctx.array.hosts
    assert ctx.array.host_groups["hg1"]["hosts"] == ["esx1", "esx2"]


async def test_setup_connectivity_fc_rescans_no_login(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg-fc", protocol="fc",
        hosts="esx1,esx2",
        initiators="esx1=21:00:00:24:ff:aa:bb:cc;esx2=21:00:00:24:ff:dd:ee:ff",
    )
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    assert r.artifacts["rescanned"] is True
    # Hosts created by WWN.
    assert ctx.array.hosts["esx1"]["wwns"] == ["21:00:00:24:ff:aa:bb:cc"]
    assert ctx.array.hosts["esx1"]["iqns"] == []
    # FC discovery is rescan-based; there must be a rescan call and NO iscsiadm login.
    logs = "\n".join(captured_logs)
    assert "rescan" in logs.lower()
    assert "iscsiadm" not in logs.lower()
    # A rescan HTTP call was made.
    assert any("storage/rescan" in line for line in captured_logs)


async def test_setup_connectivity_iscsi_does_not_rescan(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg-iscsi", protocol="iscsi", hosts="esx1",
        initiators="esx1=iqn.a",
    )
    assert r.success
    assert r.artifacts.get("rescanned") is False


async def test_setup_connectivity_bad_protocol(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.setup_connectivity(host_group="hg1", protocol="bogus", hosts="esx1")
    assert not r.success


# ---- interface binding (iSCSI port binding / HBA selection) ----
async def test_discover_options_returns_mock_options(make_context):
    c = VSphereConnector(_ctx(make_context))
    nics = await c.discover_options("nics")
    assert nics and all({"value", "label"} <= set(o) for o in nics)
    assert any(o["value"] == "vmk1" for o in nics)

    fc = await c.discover_options("fc_hbas")
    assert fc and any(o["value"].startswith("vmhba") for o in fc)

    nvme = await c.discover_options("nvme_sources")
    assert nvme and all({"value", "label"} <= set(o) for o in nvme)


async def test_discover_options_unknown_kind(make_context):
    c = VSphereConnector(_ctx(make_context))
    assert await c.discover_options("bogus") == []


def test_setup_connectivity_has_binding_fields():
    spec = next(a for a in VSphereConnector.action_schemas()
                if a.id == "setup_connectivity")
    by_name = {f.name: f for f in spec.fields}
    from phif.connectors.base import FieldType
    assert by_name["iscsi_vmknics"].type == FieldType.MULTISELECT
    assert by_name["iscsi_vmknics"].options_source == "nics"
    assert by_name["iscsi_vmknics"].required is False
    assert by_name["fc_hbas"].options_source == "fc_hbas"
    assert by_name["nvme_adapters"].options_source == "nvme_sources"


async def test_setup_connectivity_iscsi_port_binding(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg-pb", protocol="iscsi", hosts="esx1,esx2",
        initiators="esx1=iqn.a;esx2=iqn.b",
        iscsi_vmknics=["vmk1", "vmk2"],
    )
    assert r.success
    assert r.artifacts["interface_binding"]["iscsi_port_binding"] == ["vmk1", "vmk2"]
    logs = "\n".join(captured_logs)
    # Port-binding HTTP calls were made for the selected VMkernel NICs.
    assert any("iscsi/port-binding" in line for line in captured_logs)
    assert "port binding" in logs.lower()
    assert "vmk1" in logs and "vmk2" in logs


async def test_setup_connectivity_iscsi_no_binding_when_unselected(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg-nb", protocol="iscsi", hosts="esx1", initiators="esx1=iqn.a")
    assert r.success
    assert "interface_binding" not in r.artifacts
    assert not any("port-binding" in line for line in captured_logs)


async def test_setup_connectivity_fc_hba_selection(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg-fcs", protocol="fc", hosts="esx1",
        initiators="esx1=21:00:00:24:ff:aa:bb:cc",
        fc_hbas=["vmhba1", "vmhba2"],
    )
    assert r.success
    assert r.artifacts["interface_binding"]["fc_hbas"] == ["vmhba1", "vmhba2"]


async def test_setup_connectivity_nvme_adapter_selection(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg-nvme", protocol="nvme-fc", hosts="esx1",
        nvme_adapters=["vmhba64"],
    )
    assert r.success
    assert r.artifacts["interface_binding"]["nvme_adapters"] == ["vmhba64"]
    assert any("nvme/adapter-select" in line for line in captured_logs)


async def test_setup_connectivity_binding_csv_string(make_context):
    """MULTISELECT value may arrive as a comma-separated string."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg-csv", protocol="iscsi", hosts="esx1",
        initiators="esx1=iqn.a", iscsi_vmknics="vmk1, vmk2",
    )
    assert r.success
    assert r.artifacts["interface_binding"]["iscsi_port_binding"] == ["vmk1", "vmk2"]


async def test_setup_connectivity_binding_dry_run_no_calls(make_context, captured_logs):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(
        host_group="hg-dry", protocol="iscsi", hosts="esx1",
        iscsi_vmknics=["vmk1"],
    )
    assert r.success
    assert ctx.array.hosts == {}
    assert not any("port-binding" in line for line in captured_logs)


# ---- datastore provisioning ----
async def test_provision_vmfs_datastore(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.provision_datastore(name="ds1", size="2T", host_group="hg1", type="vmfs")
    assert r.success
    assert "ds1" in ctx.array.volumes
    assert ("connect_volume", {"host": "hg1", "volume": "ds1"}) in ctx.array.calls
    assert r.artifacts["type"] == "vmfs"


async def test_provision_vmfs_datastore_fc(make_context, captured_logs):
    """VMFS over FC: create volume -> connect to FC host group -> rescan -> VMFS."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.provision_datastore(
        name="ds-fc", size="2T", host_group="hg-fc", type="vmfs", protocol="fc")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    assert "ds-fc" in ctx.array.volumes
    assert ("connect_volume", {"host": "hg-fc", "volume": "ds-fc"}) in ctx.array.calls
    # FC LUN is discovered via a rescan.
    assert any("storage/rescan" in line for line in captured_logs)


async def test_provision_vmfs_requires_host_group(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.provision_datastore(name="ds1", size="1T", type="vmfs")
    assert not r.success


async def test_provision_nfs_datastore(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.provision_datastore(
        name="nfsds", type="nfs", nfs_server="10.0.0.5", nfs_path="/export/ds")
    assert r.success
    assert r.artifacts["type"] == "nfs"


async def test_provision_nfs_missing_params(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.provision_datastore(name="nfsds", type="nfs")
    assert not r.success


# ---- volume / day-2 ops ----
async def test_provision_volume(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.provision(name="vol1", size="500G", host_group="hg1")
    assert r.success and "vol1" in ctx.array.volumes


async def test_snapshot_clone_resize_qos_replication(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    await c.provision(name="vol1", size="1T")

    assert (await c.snapshot(volume="vol1", suffix="s1")).success
    assert ctx.array.snapshots[-1]["suffix"] == "s1"

    r = await c.clone(source="vol1", dest="vol2", host_group="hg1")
    assert r.success and "vol2" in ctx.array.volumes

    r = await c.resize(volume="vol1", size="2T", datastore="ds1")
    assert r.success and ctx.array.volumes["vol1"]["size"] == "2T"

    r = await c.set_qos(volume="vol1", iops_limit=1000, bw_limit=None)
    assert r.success
    assert ("set_qos", {"volume": "vol1", "iops_limit": 1000, "bw_limit": None}) in ctx.array.calls

    r = await c.configure_replication(name="pg1", volumes="vol1, vol2")
    assert r.success and "pg1" in ctx.array.protection_groups
    assert ctx.array.protection_groups["pg1"]["volumes"] == ["vol1", "vol2"]


async def test_health_check(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.health_check()
    assert r.success
    assert r.data["array"]["name"] == "mock-array"
    assert r.data["vcenter"] == "vcenter.example.com"


async def test_teardown(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.teardown()
    assert r.success and r.data["status"] == "not_deployed"


# ---- dispatch routing ----
async def test_dispatch_provision_datastore(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.dispatch("provision_datastore",
                         {"name": "dds", "size": "1T", "host_group": "hg1", "type": "vmfs"})
    assert r.success and "dds" in ctx.array.volumes


async def test_dispatch_standard_action(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.dispatch("provision", {"name": "dv", "size": "1T"})
    assert r.success and "dv" in ctx.array.volumes


async def test_dispatch_unknown(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.dispatch("nope", {})
    assert not r.success


# ---- VASA URL derives from the array endpoint (not an operator field) ----
async def test_vasa_url_uses_array_endpoint(make_context):
    """VASA provider URL is built from ctx.array.endpoint, not a typed field."""
    ctx = _ctx(make_context)
    ctx.array.endpoint = "https://fa01.example.com/api/2.x"
    c = VSphereConnector(ctx)
    r = await c.deploy_integration(plugin_url="https://plugin.local/plugin.json")
    assert r.success
    # host[:port] extracted from the array endpoint, suffixed with the VASA port.
    assert r.artifacts["vasa_url"] == "https://fa01.example.com:8084"


async def test_no_array_endpoint_or_token_form_fields(make_context):
    """Deploy/configure must not expose operator-entered array endpoint/token."""
    specs = {a.id: a for a in VSphereConnector.action_schemas()}
    for action_id in ("deploy", "configure"):
        names = {f.name for f in specs[action_id].fields}
        assert not (names & {"pure_endpoint", "pure_api_token", "array_endpoint",
                             "array_token", "fa_url", "fa_api_token"})


async def test_register_hosts_passes_array_token_to_ansible(make_context):
    """register_hosts hands the resolved array token + endpoint to Ansible."""
    ctx = _ctx(make_context)
    ctx.array.endpoint = "https://fa01.example.com"
    ctx.array_token = "tok-shared-123"
    captured: dict = {}

    async def _fake_run_ansible(playbook, extravars=None, **__):
        captured["extravars"] = extravars or {}
        return {"ok": True}

    ctx.runner.run_ansible = _fake_run_ansible  # type: ignore[assignment]
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-tok", protocol="iscsi", iqns="iqn.a")
    assert r.success
    assert captured["extravars"]["fa_url"] == "https://fa01.example.com"
    assert captured["extravars"]["fa_api_token"] == "tok-shared-123"


# ---- host_group carry-over from the hypervisor connection ----
async def test_register_hosts_carries_over_host_group(make_context):
    conn = dict(VSPHERE_CONN, host_group="hg-from-target")
    ctx = make_context(connector_key="vsphere", connection=conn,
                       secrets=dict(VSPHERE_SECRETS))
    c = VSphereConnector(ctx)
    r = await c.register_hosts(protocol="iscsi", iqns="iqn.a")  # no host_group
    assert r.success
    assert "hg-from-target" in ctx.array.host_groups


async def test_register_hosts_no_host_group_fails_clearly(make_context):
    ctx = _ctx(make_context)  # VSPHERE_CONN has no host_group
    c = VSphereConnector(ctx)
    r = await c.register_hosts(protocol="iscsi", iqns="iqn.a")
    assert not r.success
    assert "host_group" in r.message


async def test_setup_connectivity_carries_over_host_group(make_context):
    conn = dict(VSPHERE_CONN, host_group="hg-conn-target")
    ctx = make_context(connector_key="vsphere", connection=conn,
                       secrets=dict(VSPHERE_SECRETS))
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(protocol="iscsi", hosts="esx1",
                                   initiators="esx1=iqn.a")
    assert r.success
    assert "hg-conn-target" in ctx.array.host_groups


async def test_setup_connectivity_no_host_group_fails_clearly(make_context):
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(protocol="iscsi", hosts="esx1")
    assert not r.success
    assert "host_group" in r.message


def test_target_schema_has_host_group():
    by_name = {f["name"]: f for f in VSphereConnector.descriptor()["target_schema"]}
    assert "host_group" in by_name
    assert by_name["host_group"]["required"] is False


# ---- initiator display via discover_options ----
async def test_discover_options_initiators(make_context):
    """discover_options('initiators') returns ESXi initiators tagged by field."""
    c = VSphereConnector(_ctx(make_context))
    opts = await c.discover_options("initiators")
    assert opts
    # Every option carries field/value/label, and field is a register_hosts field.
    for o in opts:
        assert {"field", "value", "label"} <= set(o)
        assert o["field"] in {"iqns", "wwns", "nqns"}
    fields = {o["field"] for o in opts}
    # Synthetic discovery yields all three transport families in mock mode.
    assert fields == {"iqns", "wwns", "nqns"}
    iqn_opt = next(o for o in opts if o["field"] == "iqns")
    assert iqn_opt["value"].startswith("iqn.")


# ---- cluster (multi-ESXi-host) support ----
async def test_list_nodes_synthetic_cluster(make_context):
    """Mock mode returns a synthetic 2-host ESXi cluster."""
    c = VSphereConnector(_ctx(make_context))
    nodes = await c.list_nodes()
    assert len(nodes) == 2
    for n in nodes:
        assert n.name == n.host
        assert "esxi" in n.name
        assert n.info["cluster"] == "Cluster1"


async def test_list_nodes_parse_real_reply():
    """_parse_cluster_hosts extracts ESXi host names from a vCenter-shaped reply."""
    body = [
        {"host": "host-12", "name": "esxi-a.example.com", "connection_state": "CONNECTED"},
        {"host": "host-13", "name": "esxi-b.example.com"},
    ]
    nodes = VSphereConnector._parse_cluster_hosts(body, "C1", "DC1")
    assert [n.name for n in nodes] == ["esxi-a.example.com", "esxi-b.example.com"]
    assert all(n.info["cluster"] == "C1" for n in nodes)


async def test_validate_cluster_uniform(make_context):
    """Mock cluster has identical per-host initiators -> uniform/ok."""
    c = VSphereConnector(_ctx(make_context))
    r = await c.validate_cluster(protocol="iscsi")
    assert r.success
    assert len(r.data["nodes"]) == 2
    assert r.data["protocol"] == "iscsi"
    # Every node reports the same IQN set.
    per_node = r.data["per_node"]
    assert len(per_node) == 2
    sets = list(per_node.values())
    assert sets[0] == sets[1]


async def test_validate_cluster_detects_mismatch(make_context, monkeypatch):
    """A host exposing different initiators fails cluster validation."""
    c = VSphereConnector(_ctx(make_context))

    async def _fake(nodes, protocol, kind):
        return {nodes[0].name: ["iqn.a", "iqn.b"], nodes[1].name: ["iqn.a", "iqn.c"]}

    monkeypatch.setattr(c, "_per_host_initiators", _fake)
    r = await c.validate_cluster(protocol="iscsi")
    assert not r.success
    assert "NOT uniform" in r.message


async def test_validate_cluster_bad_protocol(make_context):
    c = VSphereConnector(_ctx(make_context))
    r = await c.validate_cluster(protocol="bogus")
    assert not r.success


def test_wizard_steps_order_and_known_ids():
    steps = VSphereConnector.wizard_steps()
    assert steps == ["deploy", "configure", "register_hosts",
                     "setup_connectivity", "provision_datastore"]
    known = {a.id for a in VSphereConnector.action_schemas()}
    assert all(s in known for s in steps)
    # deploy/configure/register/connectivity present at minimum.
    assert {"deploy", "configure", "register_hosts", "setup_connectivity"} <= set(steps)


async def test_register_hosts_explicit_hosts_fanout(make_context):
    """Named ESXi hosts each become an array host in the one host group."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-cl", protocol="iscsi", hosts="esxA,esxB")
    assert r.success
    assert set(r.artifacts["hosts"]) == {"esxA", "esxB"}
    assert "esxA" in ctx.array.hosts and "esxB" in ctx.array.hosts
    assert ctx.array.hosts["esxA"]["iqns"]  # auto-discovered per host
    assert ctx.array.host_groups["hg-cl"]["hosts"] == ["esxA", "esxB"]


async def test_register_hosts_explicit_initiators_single_host(make_context):
    """A flat explicit initiator list stays a single implicit host (no fan-out)."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.register_hosts(host_group="hg-flat", protocol="iscsi", iqns="iqn.a,iqn.b")
    assert r.success
    assert r.artifacts["hosts"] == ["hg-flat"]
    assert ctx.array.hosts["hg-flat"]["iqns"] == ["iqn.a", "iqn.b"]


async def test_setup_connectivity_fans_out_when_no_hosts(make_context):
    """setup_connectivity with no host list applies to the whole cluster."""
    ctx = _ctx(make_context)
    c = VSphereConnector(ctx)
    r = await c.setup_connectivity(host_group="hg-clc", protocol="iscsi")
    assert r.success
    assert len(r.artifacts["hosts"]) == 2  # cluster fan-out
    for h in r.artifacts["hosts"]:
        assert ctx.array.hosts[h]["iqns"]


# ---- dry-run respected ----
async def test_dry_run_makes_no_changes(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = VSphereConnector(ctx)
    r = await c.provision_datastore(name="ds1", size="1T", host_group="hg1", type="vmfs")
    assert r.success
    assert ctx.array.volumes == {}  # no volume actually created


def test_serial_from_naa_handles_naa_and_vml_forms():
    """RDM device ids come as either naa.624a9370<serial> or
    vml.0200..<624a9370><serial><ascii> — both must yield the 24-hex serial; a
    non-Everpure device (no Everpure OUI) yields None (so capture rejects it)."""
    from phif.connectors.vsphere.connector import VSphereConnector as V
    assert V._serial_from_naa("naa.624a93700123456789abcdef0bebde3b") == "0123456789abcdef0bebde3b"
    assert V._serial_from_naa("vml.0200fc0000624a93700123456789abcdef0be6aa1b466c61736841") == "0123456789abcdef0be6aa1b"
    assert V._serial_from_naa("/vmfs/devices/disks/naa.624a93700123456789abcdef0bebde3b") == "0123456789abcdef0bebde3b"
    assert V._serial_from_naa("naa.6000970000123456789abcdef0123456") is None  # non-Everpure
    assert V._serial_from_naa("") is None
