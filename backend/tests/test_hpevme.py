"""Unit tests for the HPE VM Essentials (VME) connector (mock array + runner).

Corrected design: ONE FlashArray volume per VM disk, direct-attached to the VM
as a raw multipathed block device (Everpure CSI/Cinder/Proxmox-style), with
array-based snapshot/clone/resize. NOT a shared datastore pool.
"""

import pytest

from phif.connectors.base import (
    Capability,
    ConnectionValidationError,
    FieldType,
    Protocol,
)
from phif.connectors.hpevme.connector import HpeVmeConnector


def _ctx(make_context, **overrides):
    connection = {
        "vme_manager_url": "https://vme-mgr.test.local",
        "username": "admin",
        "protocol": "iscsi",
        "host_group": "vme-hg",
    }
    connection.update(overrides.pop("connection", {}))
    return make_context(connector_key="hpevme", connection=connection, **overrides)


# ---- metadata / descriptor (honesty) ----
def test_descriptor_and_maturity():
    desc = HpeVmeConnector.descriptor()
    assert desc["key"] == "hpevme"
    assert desc["name"] == "HPE VM Essentials"
    assert desc["maturity"] == "ga"
    assert "connect" in desc["capabilities"]  # implicit
    for cap in ("host_register", "connectivity", "deploy_plugin", "configure",
                "provision_volume", "snapshot", "clone", "resize", "health", "remove"):
        assert cap in desc["capabilities"]


def test_capabilities_are_honest():
    # Per-disk model: PROVISION_VOLUME yes, datastore-pool capability NOT declared.
    caps = HpeVmeConnector.capabilities()
    assert Capability.PROVISION_VOLUME in caps
    assert Capability.PROVISION_DATASTORE not in caps
    # Capabilities we do not implement must be absent.
    for absent in (Capability.QOS, Capability.REPLICATION, Capability.RECOVERY,
                   Capability.UPGRADE, Capability.ROTATE_CREDENTIALS,
                   Capability.DELETE):
        assert absent not in caps


def test_supported_protocols():
    assert HpeVmeConnector.SUPPORTED_PROTOCOLS == {
        Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP, Protocol.NVME_FC,
        Protocol.NFS,
    }


def test_target_schema_fields():
    names = [f["name"] for f in HpeVmeConnector.descriptor()["target_schema"]]
    assert names == ["vme_manager_url", "username", "password", "protocol",
                     "host_group", "host_wwns", "kvm_ssh_username",
                     "kvm_ssh_password", "kvm_ssh_key", "plugin_jar"]
    proto = next(f for f in HpeVmeConnector.descriptor()["target_schema"]
                 if f["name"] == "protocol")
    assert proto["type"] == "enum"
    assert proto["default"] == "iscsi"
    assert set(proto["options"]) == {"iscsi", "fc", "nvme-tcp", "nvme-fc", "nfs"}
    hg = next(f for f in HpeVmeConnector.descriptor()["target_schema"]
              if f["name"] == "host_group")
    assert hg["required"] is False


def test_action_ids_cover_capabilities():
    ids = {a.id for a in HpeVmeConnector.action_schemas()}
    assert {"register_hosts", "setup_connectivity", "deploy", "configure",
            "provision", "snapshot", "clone", "resize", "health_check",
            "teardown"} <= ids


# ---- validate_connection ----
async def test_validate_connection_ok(make_context, captured_logs):
    c = HpeVmeConnector(_ctx(make_context))
    r = await c.validate_connection()
    assert r.success
    assert r.data["url"] == "https://vme-mgr.test.local"
    assert r.data["protocol"] == "iscsi"
    assert any("VME Manager" in line for line in captured_logs)
    assert any("FlashArray reachable" in line for line in captured_logs)


async def test_validate_connection_no_url(make_context):
    ctx = _ctx(make_context, connection={"vme_manager_url": ""})
    ctx.target.connection.pop("host", None)  # clear the fallback host
    with pytest.raises(ConnectionValidationError):
        await HpeVmeConnector(ctx).validate_connection()


async def test_validate_connection_without_array(make_context):
    c = HpeVmeConnector(_ctx(make_context, with_array=False))
    r = await c.validate_connection()
    assert r.success


# ---- host register ----
async def test_register_hosts(make_context):
    # In mock mode list_nodes() yields a synthetic 2-host cluster; the explicit
    # iqns apply to the connection node (vme-kvm1), the second node auto-discovers.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg", iqns="iqn.a, iqn.b")
    assert r.success
    assert r.artifacts["host_group"] == "vme-hg"
    assert "vme-hg" in ctx.array.host_groups
    # One FA host per KVM node, all in the single shared host group.
    assert "vme-hg-vme-kvm1" in ctx.array.hosts
    assert "vme-hg-vme-kvm2" in ctx.array.hosts
    assert ctx.array.hosts["vme-hg-vme-kvm1"]["iqns"] == ["iqn.a", "iqn.b"]
    assert ctx.array.host_groups["vme-hg"]["hosts"] == [
        "vme-hg-vme-kvm1", "vme-hg-vme-kvm2"]


async def test_register_hosts_uses_default_host_group(make_context):
    ctx = _ctx(make_context)  # host_group="vme-hg" in connection
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(iqns="iqn.a")
    assert r.success
    assert "vme-hg" in ctx.array.host_groups


async def test_register_hosts_no_array(make_context):
    c = HpeVmeConnector(_ctx(make_context, with_array=False))
    r = await c.register_hosts(host_group="hg")
    assert not r.success


# ---- host register: AUTO-DISCOVERY when no manual initiators supplied ----
async def test_register_hosts_iscsi_auto_discovers_iqn(make_context):
    # No manual iqns/wwns/nqns: the connector auto-discovers from the KVM host and
    # the discovered IQN must reach the mock array (mock discover_initiators returns
    # iqn.1993-08.org.debian:01:<host-tag>).
    ctx = _ctx(make_context)  # protocol=iscsi
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg")
    assert r.success
    host = ctx.array.hosts["vme-hg-vme-kvm1"]
    assert host["iqns"] and host["iqns"][0].startswith("iqn.")
    # iscsi registers by IQN only -- no WWNs/NQNs.
    assert host["wwns"] == [] and host["nqns"] == []


async def test_register_hosts_nvme_tcp_auto_discovers_nqn(make_context):
    ctx = _ctx(make_context, connection={"protocol": "nvme-tcp"})
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg")
    assert r.success
    host = ctx.array.hosts["vme-hg-vme-kvm1"]
    assert host["nqns"] and host["nqns"][0].startswith("nqn.")


async def test_register_hosts_fc_auto_discovers_wwns(make_context):
    # No manual WWNs and none configured: discover from the KVM host. The mock
    # helper returns bare-hex WWNs which must be normalized to colon form.
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg")
    assert r.success
    host = ctx.array.hosts["vme-hg-vme-kvm1"]
    assert host["wwns"] and all(":" in w for w in host["wwns"])
    assert host["iqns"] == [] and host["nqns"] == []


async def test_register_hosts_explicit_iqn_overrides_discovery(make_context):
    # Explicit initiators win -- discovery must NOT replace them.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg", iqns="iqn.explicit.a")
    assert r.success
    # Explicit iqns apply to the connection node only.
    assert ctx.array.hosts["vme-hg-vme-kvm1"]["iqns"] == ["iqn.explicit.a"]


async def test_register_hosts_auto_discover_dry_run_no_op(make_context):
    # Dry-run: auto-discovery may run (mock-safe) but no host/group is created.
    ctx = _ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg")
    assert r.success
    assert "vme-hg" not in ctx.array.host_groups
    assert "vme-hg-vme" not in ctx.array.hosts


# ---- connectivity ----
async def test_setup_connectivity(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["protocol"] == "iscsi"


# ---- deploy (upload plugin) / configure (register storage server) ----
async def test_deploy_uploads_plugin(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    r = await c.deploy_integration()
    assert r.success
    assert r.data["status"] == "deployed"
    assert r.data["plugin"] == "pure-flasharray-vme"


async def test_configure_registers_storage_server(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    r = await c.configure(host_group="vme-hg")
    assert r.success
    assert r.data["host_group"] == "vme-hg"
    assert r.data["storage_server_type"] == "pure-flasharray-vme.storage"


# ---- provision: direct-array create + connect (VME plugin does the guest attach) ----
async def test_provision_creates_volume_and_connects(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.provision(name="vm1-disk0", size="2T", host_group="vme-hg")
    assert r.success
    # FA volume created for the disk.
    assert "vm1-disk0" in ctx.array.volumes
    assert ("create_volume", {"name": "vm1-disk0", "size": "2T"}) in ctx.array.calls
    # Connected to the host group.
    assert ("connect_volume", {"host": "vme-hg", "volume": "vm1-disk0"}) in ctx.array.calls
    assert r.artifacts["volume"] == "vm1-disk0"
    # VME/plugin owns the guest attach -- no SSH attach artifact, no datastore pool.
    assert r.artifacts["managed_by"] == "vme-pure-plugin"
    assert "attach" not in r.artifacts
    assert "datastore" not in r.artifacts


async def test_provision_defaults_host_group_from_connection(make_context):
    ctx = _ctx(make_context)  # host_group="vme-hg"
    c = HpeVmeConnector(ctx)
    r = await c.provision(name="vm2-disk0", size="1T")
    assert r.success
    assert ("connect_volume", {"host": "vme-hg", "volume": "vm2-disk0"}) in ctx.array.calls


async def test_provision_resolves_device_path(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.provision(name="orphan", size="1T")
    assert r.success
    assert "orphan" in ctx.array.volumes
    # Reports the host device path (informational; VME's plugin attaches it).
    assert r.artifacts["device"].startswith("/dev/mapper/3624a9370")


async def test_provision_no_array(make_context):
    c = HpeVmeConnector(_ctx(make_context, with_array=False))
    r = await c.provision(name="x")
    assert not r.success


# ---- day-2 array ops ----
async def test_snapshot_calls_array(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    await c.provision(name="v1", size="1T", vm="vm1")
    r = await c.snapshot(volume="v1", suffix="s1")
    assert r.success
    assert ctx.array.snapshots[-1]["volume"] == "v1"
    assert ctx.array.snapshots[-1]["suffix"] == "s1"


async def test_clone_calls_array(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    await c.provision(name="v1", size="1T", vm="vm1")
    r = await c.clone(source="v1", dest="v2")
    assert r.success and "v2" in ctx.array.volumes
    assert ("clone_volume", {"source": "v1", "dest": "v2"}) in ctx.array.calls


async def test_resize_calls_array(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    await c.provision(name="v1", size="1T", vm="vm1")
    r = await c.resize(volume="v1", size="3T")
    assert r.success and ctx.array.volumes["v1"]["size"] == "3T"
    assert ("extend_volume", {"name": "v1", "size": "3T"}) in ctx.array.calls


async def test_day2_ops_no_array(make_context):
    c = HpeVmeConnector(_ctx(make_context, with_array=False))
    assert not (await c.snapshot(volume="v")).success
    assert not (await c.clone(source="a", dest="b")).success
    assert not (await c.resize(volume="v", size="2T")).success


# ---- health ----
async def test_health_check(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    r = await c.health_check()
    assert r.success
    assert "vme_health" in r.data
    assert "vme_instances" in r.data
    assert r.data["array"]["name"] == "mock-array"


# ---- teardown ----
async def test_teardown(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    r = await c.teardown(volume="v1", vm="vm1")
    assert r.success
    assert r.data["status"] == "not_deployed"


# ---- dry run: no array changes ----
async def test_dry_run_provision_makes_no_changes(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = HpeVmeConnector(ctx)
    r = await c.provision(name="dvol", size="1T", vm="vm1")
    assert r.success
    assert "dvol" not in ctx.array.volumes


async def test_register_hosts_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="hg", iqns="iqn.a")
    assert r.success
    assert "hg" not in ctx.array.host_groups


# ---- datastore fallback helper (NOT the primary path) ----
async def test_datastore_fallback_helper(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.register_datastore_fallback(datastore="ds1", backing="vol1")
    assert r.success
    assert r.artifacts["datastore"] == "ds1"
    assert "vol1" in ctx.array.volumes


# ---- dispatch routing ----
async def test_dispatch_routes_provision(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.dispatch("provision", {"name": "dispvol", "vm": "vm1"})
    assert r.success
    assert "dispvol" in ctx.array.volumes


async def test_dispatch_routes_register_hosts(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.dispatch("register_hosts", {"host_group": "hg2", "iqns": "iqn.x"})
    assert r.success
    assert "hg2" in ctx.array.host_groups


async def test_dispatch_unknown(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    r = await c.dispatch("does_not_exist", {})
    assert not r.success


# ====================================================================== #
# Fibre Channel (FC) path
# ====================================================================== #
def _fc_ctx(make_context, **overrides):
    connection = {
        "vme_manager_url": "https://vme-mgr.test.local",
        "username": "root",
        "protocol": "fc",
        "host_group": "vme-hg",
    }
    connection.update(overrides.pop("connection", {}))
    return make_context(connector_key="hpevme", connection=connection, **overrides)


# ---- target schema exposes the FC WWN field ----
def test_target_schema_has_host_wwns_field():
    names = [f["name"] for f in HpeVmeConnector.descriptor()["target_schema"]]
    assert "host_wwns" in names
    wwn = next(f for f in HpeVmeConnector.descriptor()["target_schema"]
               if f["name"] == "host_wwns")
    assert wwn["required"] is False


# ---- host register over FC: by WWN, NOT IQN/NQN ----
async def test_register_hosts_fc_uses_wwns(make_context):
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg",
                               wwns="21:00:00:24:ff:00:00:01, 21:00:00:24:ff:00:00:02")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    assert "vme-hg-vme-kvm1" in ctx.array.hosts
    host = ctx.array.hosts["vme-hg-vme-kvm1"]
    # Connection node registered by the explicit WWNs only -- no IQNs / NQNs.
    assert host["wwns"] == ["21:00:00:24:ff:00:00:01", "21:00:00:24:ff:00:00:02"]
    assert host["iqns"] == []
    assert host["nqns"] == []


async def test_register_hosts_fc_uses_connection_host_wwns(make_context):
    ctx = _fc_ctx(make_context,
                  connection={"host_wwns": "21:00:00:24:ff:aa:bb:cc"})
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg")  # no wwns passed to the action
    assert r.success
    # Connection-node WWNs come from connection host_wwns.
    assert ctx.array.hosts["vme-hg-vme-kvm1"]["wwns"] == ["21:00:00:24:ff:aa:bb:cc"]


async def test_register_hosts_fc_discovers_wwns_when_missing(make_context, monkeypatch):
    # Single-node FC deployment: list_nodes() returns one host so discovered WWNs
    # back the connection node's FA host.
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)

    async def one_node():
        from phif.connectors.base import ClusterNode
        return [ClusterNode(name="vme-host", host="vme.test.local")]

    async def fake_discover(host):
        return {"iqn": None, "nqn": None, "wwns": ["5001438011223344"]}

    monkeypatch.setattr(c, "list_nodes", one_node)
    monkeypatch.setattr(c, "_discover_initiators_on", fake_discover)
    r = await c.register_hosts(host_group="vme-hg")
    assert r.success
    assert ctx.array.hosts["vme-hg-vme"]["wwns"] == ["50:01:43:80:11:22:33:44"]


async def test_register_hosts_fc_no_wwns_fails(make_context, monkeypatch):
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)

    async def empty(host):
        return {"iqn": None, "nqn": None, "wwns": []}

    monkeypatch.setattr(c, "_discover_initiators_on", empty)
    r = await c.register_hosts(host_group="vme-hg")
    assert not r.success


async def test_discover_host_wwns_uses_shared_helper(make_context, monkeypatch):
    # _discover_host_wwns now delegates to the shared runner.discover_initiators
    # helper and normalizes bare-hex WWNs to colon-separated form.
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)

    async def fake_discover():
        return {"iqn": None, "nqn": None,
                "wwns": ["2100002432aabbcc", "0x2100002432ddeeff"]}

    monkeypatch.setattr(c, "_discover_initiators", fake_discover)
    wwns = await c._discover_host_wwns()
    assert wwns == ["21:00:00:24:32:aa:bb:cc", "21:00:00:24:32:dd:ee:ff"]


# ---- connectivity over FC: rescan, no iscsiadm / nvme connect ----
async def test_setup_connectivity_fc_rescans_no_iscsiadm(make_context, captured_logs):
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["protocol"] == "fc"
    assert "fc_scsi_rescan" in r.data["steps"]
    # Performs an FC/SCSI rescan; never RUNS an iscsiadm / nvme connect command.
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert any(("fc_host" in ln or "scsi_host" in ln or "rescan-scsi-bus" in ln)
               for ln in ssh_cmds)
    assert not any("iscsiadm" in ln for ln in ssh_cmds)
    assert not any("nvme connect" in ln for ln in ssh_cmds)


async def test_setup_connectivity_iscsi_still_works(make_context):
    c = HpeVmeConnector(_ctx(make_context))  # iscsi
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["protocol"] == "iscsi"


# ---- provision over FC: connect -> rescan -> multipath device ----
async def test_provision_fc_resolves_multipath_device(make_context):
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.provision(name="fcvol", size="1T", host_group="vme-hg")
    assert r.success
    # FA volume created + connected to the FC host group.
    assert "fcvol" in ctx.array.volumes
    assert ("connect_volume", {"host": "vme-hg", "volume": "fcvol"}) in ctx.array.calls
    assert r.artifacts["protocol"] == "fc"
    # Reports a /dev/mapper/<wwid> device; VME's plugin attaches it (no SSH attach).
    assert r.artifacts["device"].startswith("/dev/mapper/")
    assert r.artifacts["managed_by"] == "vme-pure-plugin"


async def test_provision_fc_just_creates_and_connects(make_context):
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.provision(name="fcorphan", size="1T", host_group="vme-hg")
    assert r.success
    assert "fcorphan" in ctx.array.volumes
    assert "attach" not in r.artifacts


async def test_register_hosts_fc_dry_run(make_context):
    ctx = _fc_ctx(make_context)
    ctx.dry_run = True
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="hg", wwns="21:00:00:24:ff:00:00:01")
    assert r.success
    assert "hg" not in ctx.array.host_groups
    assert r.artifacts["protocol"] == "fc"


# ====================================================================== #
# Interface binding (discover_options + setup_connectivity per protocol)
# ====================================================================== #
def _nvme_ctx(make_context, **overrides):
    connection = {
        "vme_manager_url": "https://vme-mgr.test.local",
        "username": "root",
        "protocol": "nvme-tcp",
        "host_group": "vme-hg",
    }
    connection.update(overrides.pop("connection", {}))
    return make_context(connector_key="hpevme", connection=connection, **overrides)


# ---- discover_options returns mock options per kind ----
async def test_discover_options_nics_filtered_to_array_subnet(make_context):
    # iSCSI: the array's iSCSI portals (10.10.10.x) live on eth0's subnet
    # (10.10.10.5/24), so only eth0 is offered for NIC binding. eth1 (nvme-tcp
    # subnet) and ens192 (mgmt) are filtered out.
    c = HpeVmeConnector(_ctx(make_context))  # iscsi
    opts = await c.discover_options("nics")
    assert opts and all("value" in o and "label" in o for o in opts)
    assert {o["value"] for o in opts} == {"eth0"}


async def test_discover_options_nics_nvme_filters_to_nvme_subnet(make_context):
    # NVMe-TCP: the array's nvme-tcp portals (10.20.20.x) live on eth1's subnet
    # (10.20.20.5/24), so only eth1 is offered.
    c = HpeVmeConnector(_nvme_ctx(make_context))
    opts = await c.discover_options("nics")
    assert {o["value"] for o in opts} == {"eth1"}


async def test_discover_options_nics_unfiltered_without_array(make_context):
    # No associated array -> no portals -> NICs returned unfiltered.
    c = HpeVmeConnector(_ctx(make_context, with_array=False))
    opts = await c.discover_options("nics")
    assert {o["value"] for o in opts} == {"eth0", "eth1", "ens192"}


async def test_discover_options_nvme_sources(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    opts = await c.discover_options("nvme_sources")
    assert opts and all("address" in o for o in opts)
    assert all(o["value"] for o in opts)


async def test_discover_options_fc_hbas(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    opts = await c.discover_options("fc_hbas")
    assert opts and all("wwpn" in o for o in opts)


async def test_discover_options_unknown_kind(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    assert await c.discover_options("nonsense") == []


# ---- binding fields advertised on the setup_connectivity action ----
def test_setup_connectivity_advertises_binding_fields():
    spec = next(a for a in HpeVmeConnector.action_schemas()
                if a.id == "setup_connectivity")
    by_name = {f.name: f for f in spec.fields}
    assert by_name["iscsi_nics"].type == FieldType.MULTISELECT
    assert by_name["iscsi_nics"].options_source == "nics"
    assert by_name["nvme_sources"].options_source == "nvme_sources"
    assert by_name["fc_hbas"].options_source == "fc_hbas"
    assert by_name["nvme_options"].type == FieldType.STRING
    for name in ("iscsi_nics", "nvme_sources", "nvme_options", "fc_hbas"):
        assert by_name[name].required is False


# ---- iSCSI binding applies iscsiadm iface binding ----
async def test_setup_connectivity_binds_iscsi_nics(make_context, captured_logs):
    ctx = _ctx(make_context)  # iscsi
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg", iscsi_nics=["eth0", "eth1"])
    assert r.success
    assert "iscsi_iface_binding" in r.data["steps"]
    assert r.data["binding"]["iscsi_nics"] == ["eth0", "eth1"]
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert any("iscsiadm -m iface" in ln and "eth0" in ln for ln in ssh_cmds)
    assert any("eth1" in ln for ln in ssh_cmds)
    # Persisted in connector state.
    assert ctx.target.connection["interface_binding"]["iscsi_nics"] == ["eth0", "eth1"]


# ---- NVMe-TCP binding applies nvme connect -w ----
async def test_setup_connectivity_binds_nvme_sources(make_context, captured_logs):
    ctx = _nvme_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg",
                                   nvme_sources=["192.168.10.11"],
                                   nvme_options="-l 600")
    assert r.success
    assert "nvme_source_binding" in r.data["steps"]
    assert r.data["binding"]["nvme_sources"] == ["192.168.10.11"]
    assert r.data["binding"]["nvme_options"] == "-l 600"
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert any("nvme connect" in ln and "-w 192.168.10.11" in ln for ln in ssh_cmds)
    assert any("-l 600" in ln for ln in ssh_cmds)


# ---- FC binding selects HBAs and rescans them ----
async def test_setup_connectivity_binds_fc_hbas(make_context, captured_logs):
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg",
                                   fc_hbas=["21000024ff000001"])
    assert r.success
    assert "fc_hba_binding" in r.data["steps"]
    assert r.data["binding"]["fc_hbas"] == ["21000024ff000001"]
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert any("fc_host" in ln and "21000024ff000001" in ln for ln in ssh_cmds)
    # FC must never run a software login.
    assert not any("iscsiadm" in ln for ln in ssh_cmds)
    assert not any("nvme connect" in ln for ln in ssh_cmds)


# ---- binding acts only on the selected protocol ----
async def test_setup_connectivity_ignores_offprotocol_binding(make_context, captured_logs):
    # protocol=iscsi: fc_hbas / nvme_sources must be ignored.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg",
                                   fc_hbas=["21000024ff000001"],
                                   nvme_sources=["192.168.10.11"])
    assert r.success
    assert "fc_hba_binding" not in r.data["steps"]
    assert "nvme_source_binding" not in r.data["steps"]
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert not any("nvme connect" in ln for ln in ssh_cmds)


# ---- dry-run binding is a no-op (no SSH binding commands, no state persisted) ----
async def test_setup_connectivity_binding_dry_run_no_op(make_context, captured_logs):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg", iscsi_nics=["eth0"])
    assert r.success
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert not any("iscsiadm -m iface" in ln for ln in ssh_cmds)
    assert "interface_binding" not in ctx.target.connection


# ====================================================================== #
# Array portal + target discovery in setup_connectivity (Proxmox-style UX)
# ====================================================================== #
async def test_setup_connectivity_iscsi_discovers_portals_and_target(make_context,
                                                                     captured_logs):
    # iSCSI: portals + target IQN come from the array (get_data_interfaces /
    # get_target_ports), not from operator input.
    ctx = _ctx(make_context)  # iscsi
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["portals"] == ["10.10.10.10", "10.10.11.10"]
    assert r.data["target_iqn"].startswith("iqn.")
    assert r.data["target_nqn"] == ""
    assert ("get_data_interfaces", {"service": "iscsi"}) in ctx.array.calls
    assert any("Discovered array iscsi portals" in ln for ln in captured_logs)
    assert any("target IQN" in ln for ln in captured_logs)


async def test_setup_connectivity_nvme_discovers_portals_and_subsystem(make_context):
    ctx = _nvme_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["portals"] == ["10.20.20.20", "10.20.21.20"]
    assert r.data["target_nqn"].startswith("nqn.")
    assert r.data["target_iqn"] == ""
    assert ("get_data_interfaces", {"service": "nvme-tcp"}) in ctx.array.calls


async def test_setup_connectivity_fc_skips_portal_discovery(make_context):
    # FC has no portals (fabric zoning only): no get_data_interfaces call.
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["portals"] == []
    assert not any(op == "get_data_interfaces" for op, _ in ctx.array.calls)


# ---- FAIL-FAST: IP transport with no portals (array is FC-only) ----
async def test_setup_connectivity_iscsi_no_portals_fails(make_context, monkeypatch):
    ctx = _ctx(make_context)  # iscsi

    async def no_portals(service):
        return []

    monkeypatch.setattr(ctx.array, "get_data_interfaces", no_portals)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert not r.success
    assert "portal" in r.message.lower()
    assert "fc" in r.message.lower()


async def test_setup_connectivity_no_portals_dry_run_does_not_fail(make_context,
                                                                   monkeypatch):
    ctx = _ctx(make_context)  # iscsi
    ctx.dry_run = True
    ctx.runner.dry_run = True

    async def no_portals(service):
        return []

    monkeypatch.setattr(ctx.array, "get_data_interfaces", no_portals)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success  # dry-run never fails on missing portals


# ====================================================================== #
# Initiator display via discover_options("initiators")
# ====================================================================== #
async def test_discover_options_initiators_tags_register_fields(make_context):
    # Returns the KVM host's IQN/NQN/WWN tagged with the register_hosts field
    # names so the UI can display + pre-fill them.
    c = HpeVmeConnector(_ctx(make_context))
    opts = await c.discover_options("initiators")
    by_field = {o["field"]: o for o in opts}
    assert by_field["iqns"]["value"].startswith("iqn.")
    assert by_field["nqns"]["value"].startswith("nqn.")
    # WWNs normalized to colon form.
    assert ":" in by_field["wwns"]["value"]
    assert all("label" in o for o in opts)


async def test_discover_options_initiators_field_names_match_register_hosts():
    # The tagged fields must be real register_hosts field names.
    spec = next(a for a in HpeVmeConnector.action_schemas()
                if a.id == "register_hosts")
    field_names = {f.name for f in spec.fields}
    assert {"iqns", "nqns", "wwns"} <= field_names


# ====================================================================== #
# FA host name sanitize
# ====================================================================== #
async def test_register_hosts_sanitizes_fa_host_name(make_context, monkeypatch):
    # A host group derived from an IP/FQDN must yield an FA host name with only
    # [A-Za-z0-9-] (dots mapped to hyphens).
    # Single-node deployment keeps the historical {host_group}-vme FA host name.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)

    async def one_node():
        from phif.connectors.base import ClusterNode
        return [ClusterNode(name="vme-host", host="vme.test.local")]

    monkeypatch.setattr(c, "list_nodes", one_node)
    r = await c.register_hosts(host_group="192.0.2.58", iqns="iqn.a")
    assert r.success
    host = r.artifacts["host"]
    assert host == "192-0-2-58-vme"
    assert all(ch.isalnum() or ch == "-" for ch in host)
    assert host in ctx.array.hosts


# ====================================================================== #
# Deploy: FlashArray endpoint/token sourced from the array, not form fields
# ====================================================================== #
def test_deploy_and_configure_have_no_endpoint_token_fields():
    # Endpoint/token must come from the array — never operator form fields.
    for action_id in ("deploy", "configure"):
        spec = next(a for a in HpeVmeConnector.action_schemas() if a.id == action_id)
        names = {f.name for f in spec.fields}
        assert "endpoint" not in names
        assert "pure_endpoint" not in names
        assert "api_token" not in names
        assert "pure_api_token" not in names


async def test_configure_uses_array_endpoint(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.configure(host_group="vme-hg")
    assert r.success
    # Endpoint sourced from ctx.array.endpoint (not operator form fields).
    assert r.data["endpoint"] == ctx.array.endpoint


async def test_configure_token_override_kwarg_accepted(make_context):
    # The kwargs remain available as optional overrides.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.configure(host_group="vme-hg", pure_endpoint="fa-override.example",
                          pure_api_token="t0k")
    assert r.success
    assert r.data["endpoint"] == "fa-override.example"


# ====================================================================== #
# SCSI WWID: /dev/mapper/3 + 624a9370 + lc(serial)  (mirrors Proxmox _scsi_wwid)
# ====================================================================== #
def test_scsi_wwid_prepends_naa6_pure_oui():
    # 24-hex serial WITHOUT the OUI prefix -> 3 + 624a9370 + lc(serial).
    serial = "0123456789ABCDEF0BB82813"
    assert HpeVmeConnector._scsi_wwid(serial) == "3624a93700123456789abcdef0bb82813"


def test_scsi_wwid_tolerates_already_prefixed_serial():
    # A serial that already includes 624a937 must not be double-prefixed.
    assert HpeVmeConnector._scsi_wwid("624a937001234567") == "3624a937001234567"
    # Tolerate a 0x prefix too.
    assert HpeVmeConnector._scsi_wwid("0x624a937001234567") == "3624a937001234567"


def test_scsi_wwid_lowercases():
    assert HpeVmeConnector._scsi_wwid("ABCDEF").startswith("3624a9370abcdef")


async def test_resolve_mpath_uses_scsi_wwid(make_context):
    # _resolve_mpath builds the device path via _scsi_wwid (NAA-6 Everpure OUI prefix).
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    path = await c._resolve_mpath("vm1-disk0", "fc")
    assert path == "/dev/mapper/3624a9370vm1-disk0"
    assert path.startswith("/dev/mapper/3624a9370")


async def test_provision_fc_device_path_uses_array_serial(make_context):
    # The /dev/mapper WWID is built from the array-assigned serial (looked up via
    # get_volume), NOT the volume name -- this is correct on real hardware.
    import hashlib

    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.provision(name="fcvol", size="1T", host_group="vme-hg")
    assert r.success
    serial = hashlib.sha1(b"fcvol").hexdigest()[:24]
    assert r.artifacts["device"] == f"/dev/mapper/3624a9370{serial}"


# ====================================================================== #
# Multipath config: setup_connectivity writes Everpure /etc/multipath.conf (SCSI)
# ====================================================================== #
async def test_setup_connectivity_iscsi_writes_multipath_conf(make_context, captured_logs):
    ctx = _ctx(make_context)  # iscsi
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert "multipath_conf" in r.data["steps"]
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    # Wrote the Everpure device stanza (PURE / FlashArray) + find_multipaths and
    # reconfigured the running multipathd (never `restart`, which hangs on hosts
    # with active maps and stalls the SSH channel).
    assert any("/etc/multipath.conf" in ln for ln in ssh_cmds)
    assert any('vendor "PURE"' in ln and "find_multipaths" in ln for ln in ssh_cmds)
    assert any("multipathd reconfigure" in ln for ln in ssh_cmds)
    assert not any("restart multipathd" in ln for ln in ssh_cmds)


async def test_setup_connectivity_fc_writes_multipath_conf(make_context, captured_logs):
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert "multipath_conf" in r.data["steps"]
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert any('product "FlashArray"' in ln for ln in ssh_cmds)


async def test_setup_connectivity_nvme_skips_multipath_conf(make_context, captured_logs):
    # NVMe-TCP uses native NVMe multipath -- no /etc/multipath.conf is written.
    ctx = _nvme_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert "multipath_conf" not in r.data["steps"]
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert not any("/etc/multipath.conf" in ln for ln in ssh_cmds)


async def test_setup_connectivity_multipath_conf_dry_run_no_op(make_context, captured_logs):
    ctx = _ctx(make_context)  # iscsi
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert not any("/etc/multipath.conf" in ln for ln in ssh_cmds)


# ====================================================================== #
# Cluster (multi-KVM-host) support
# ====================================================================== #
from phif.connectors.base import ClusterNode  # noqa: E402


async def _two_nodes():
    return [
        ClusterNode(name="vme-kvm1", host="10.0.0.1"),
        ClusterNode(name="vme-kvm2", host="192.0.2.2"),
    ]


async def _one_node():
    return [ClusterNode(name="vme-host", host="vme.test.local")]


# ---- list_nodes: synthetic cluster in mock mode, single-host fallback ----
async def test_list_nodes_mock_returns_synthetic_cluster(make_context):
    c = HpeVmeConnector(_ctx(make_context))
    nodes = await c.list_nodes()
    assert len(nodes) == 2
    assert {n.name for n in nodes} == {"vme-kvm1", "vme-kvm2"}
    assert all(isinstance(n, ClusterNode) for n in nodes)


async def test_list_nodes_dry_run_returns_synthetic_cluster(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = HpeVmeConnector(ctx)
    nodes = await c.list_nodes()
    assert len(nodes) == 2


# ---- wizard_steps: sensible order using this connector's action ids ----
def test_wizard_steps_order_and_known_ids():
    steps = HpeVmeConnector.wizard_steps()
    assert steps == ["deploy", "configure", "register_hosts", "setup_connectivity"]
    ids = {a.id for a in HpeVmeConnector.action_schemas()}
    assert all(s in ids for s in steps)


def test_descriptor_exposes_wizard_steps():
    assert HpeVmeConnector.descriptor()["wizard_steps"] == [
        "deploy", "configure", "register_hosts", "setup_connectivity"]


# ---- register_hosts fans out: one FA host per node, one shared group ----
async def test_register_hosts_fans_out_across_cluster(make_context, monkeypatch):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _two_nodes)
    r = await c.register_hosts(host_group="vme-hg", iqns="iqn.conn")
    assert r.success
    assert ctx.array.host_groups["vme-hg"]["hosts"] == [
        "vme-hg-vme-kvm1", "vme-hg-vme-kvm2"]
    assert "vme-hg-vme-kvm1" in ctx.array.hosts
    assert "vme-hg-vme-kvm2" in ctx.array.hosts
    assert r.artifacts["nodes"] == ["vme-kvm1", "vme-kvm2"]


async def test_register_hosts_single_node_keeps_legacy_name(make_context, monkeypatch):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _one_node)
    r = await c.register_hosts(host_group="vme-hg", iqns="iqn.a")
    assert r.success
    assert "vme-hg-vme" in ctx.array.hosts
    assert ctx.array.hosts["vme-hg-vme"]["iqns"] == ["iqn.a"]
    assert ctx.array.host_groups["vme-hg"]["hosts"] == ["vme-hg-vme"]


async def test_register_hosts_cluster_dry_run_no_changes(make_context, monkeypatch):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _two_nodes)
    r = await c.register_hosts(host_group="vme-hg", iqns="iqn.a")
    assert r.success
    assert "vme-hg" not in ctx.array.host_groups
    assert r.artifacts["hosts"] == ["vme-hg-vme-kvm1", "vme-hg-vme-kvm2"]


# ---- setup_connectivity fans out the transport setup across each node ----
async def test_setup_connectivity_fans_out_per_node(make_context, monkeypatch,
                                                    captured_logs):
    ctx = _ctx(make_context)  # iscsi
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _two_nodes)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["per_node"]["vme-kvm1"] and r.data["per_node"]["vme-kvm2"]
    assert "multipath_conf" in r.data["per_node"]["vme-kvm1"]
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert any("@10.0.0.1" in ln for ln in ssh_cmds)
    assert any("@192.0.2.2" in ln for ln in ssh_cmds)
    # Array portal discovery (cluster-level) runs once.
    assert sum(1 for op, _ in ctx.array.calls if op == "get_data_interfaces") == 1


async def test_setup_connectivity_single_node_unchanged(make_context, monkeypatch):
    ctx = _ctx(make_context)  # iscsi
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _one_node)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["protocol"] == "iscsi"
    assert "multipath_conf" in r.data["steps"]
    assert list(r.data["per_node"]) == ["vme-host"]


# ---- validate_cluster: per-host interface discovery + comparison ----
async def test_validate_cluster_consistent(make_context, monkeypatch):
    ctx = _ctx(make_context)  # iscsi -> kind "nics"
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _two_nodes)
    r = await c.validate_cluster()
    assert r.success
    assert r.data["protocol"] == "iscsi"
    assert r.data["kind"] == "nics"
    assert set(r.data["per_node"]) == {"vme-kvm1", "vme-kvm2"}
    # NICs are filtered to the array's iSCSI storage subnet (eth0 only); mgmt /
    # nvme-tcp NICs are excluded from the cross-host comparison.
    assert r.data["per_node"]["vme-kvm1"] == ["eth0"]
    assert r.data["per_node"]["vme-kvm2"] == ["eth0"]


async def test_validate_cluster_inconsistent(make_context, monkeypatch):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _two_nodes)

    async def lopsided(host, kind, **kw):
        if host == "10.0.0.1":
            return [{"value": "eth0"}, {"value": "eth1"}]
        return [{"value": "eth0"}]  # node 2 missing eth1

    monkeypatch.setattr(ctx.runner, "discover_interfaces", lopsided)
    r = await c.validate_cluster()
    assert not r.success
    assert "inconsistent" in r.message.lower()


async def test_validate_cluster_single_node_ok(make_context, monkeypatch):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _one_node)
    r = await c.validate_cluster()
    assert r.success


async def test_validate_cluster_protocol_kind_mapping(make_context, monkeypatch):
    ctx = _fc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    monkeypatch.setattr(c, "list_nodes", _two_nodes)
    r = await c.validate_cluster()
    assert r.success
    assert r.data["kind"] == "fc_hbas"


# ====================================================================== #
# Researched VME/Morpheus API correctness (no fabricated endpoints)
# ====================================================================== #
def _capture_http(ctx, monkeypatch):
    """Record every run_http call as (method, url, kwargs)."""
    calls: list[tuple] = []

    async def capture(method, url, **kw):
        calls.append((method, url, kw))
        return {"status_code": 200, "json": {}}

    monkeypatch.setattr(ctx.runner, "run_http", capture)
    return calls


async def test_authenticate_uses_form_encoded_oauth(make_context, monkeypatch):
    # OAuth must be POST /oauth/token with a FORM-encoded body (data=), not JSON.
    ctx = _ctx(make_context)
    calls = _capture_http(ctx, monkeypatch)
    token = await HpeVmeConnector(ctx)._authenticate()
    assert token
    oauth = [c for c in calls if c[1].endswith("/oauth/token")]
    assert oauth, "no /oauth/token call made"
    method, _url, kw = oauth[0]
    assert method == "POST"
    assert kw.get("data", {}).get("grant_type") == "password"
    assert kw["data"]["client_id"] == "morph-api"
    # Must NOT send the credentials as a JSON body.
    assert kw.get("json_body") is None


async def test_deploy_uploads_plugin_not_storage_server(make_context, monkeypatch):
    # deploy uploads the plugin JAR; it must NOT POST a storage server (that's
    # configure's job, and only after the plugin type exists).
    ctx = _ctx(make_context)
    calls = _capture_http(ctx, monkeypatch)
    r = await HpeVmeConnector(ctx).deploy_integration()
    assert r.success and r.data["status"] == "deployed"
    assert r.data["plugin"] == "pure-flasharray-vme"
    assert not any("storage-servers" in url for _m, url, _kw in calls)


async def test_configure_posts_storage_server_with_plugin_type(make_context, monkeypatch):
    ctx = _ctx(make_context)
    calls = _capture_http(ctx, monkeypatch)
    r = await HpeVmeConnector(ctx).configure(host_group="vme-hg")
    assert r.success
    posts = [c for c in calls if c[0] == "POST" and "storage-servers" in c[1]]
    assert posts, "configure did not POST a storage server"
    body = posts[0][2].get("json_body", {}).get("storageServer", {})
    assert body["type"] == "pure-flasharray-vme.storage"
    assert body["config"]["hostGroup"] == "vme-hg"


async def test_list_nodes_uses_servers_vmhypervisor(make_context, monkeypatch):
    # Real (non-mock) path queries GET /api/servers?vmHypervisor=true.
    ctx = _ctx(make_context)
    monkeypatch.setattr(ctx.runner, "mock", False)  # force the REST path (auto-reverted)
    calls = _capture_http(ctx, monkeypatch)
    await HpeVmeConnector(ctx).list_nodes()
    assert any("/api/servers?vmHypervisor=true" in url for _m, url, _kw in calls)
    assert not any("/api/hosts" in url for _m, url, _kw in calls)


async def test_validate_connection_pings_before_auth(make_context, monkeypatch):
    ctx = _ctx(make_context)
    calls = _capture_http(ctx, monkeypatch)
    await HpeVmeConnector(ctx).validate_connection()
    urls = [url for _m, url, _kw in calls]
    assert any(u.endswith("/api/ping") for u in urls)
    assert any(u.endswith("/api/whoami") for u in urls)


# ---- NVMe-FC: registers by NQN (like NVMe-TCP), fabric connectivity ----
def _nvmefc_ctx(make_context, **overrides):
    connection = {
        "vme_manager_url": "https://vme-mgr.test.local",
        "username": "root",
        "protocol": "nvme-fc",
        "host_group": "vme-hg",
    }
    connection.update(overrides.pop("connection", {}))
    return make_context(connector_key="hpevme", connection=connection, **overrides)


async def test_register_hosts_nvme_fc_uses_nqn(make_context):
    # NVMe over FC: the array identifies the host by NQN, not WWN.
    ctx = _nvmefc_ctx(make_context)
    r = await HpeVmeConnector(ctx).register_hosts(host_group="vme-hg")
    assert r.success
    host = ctx.array.hosts["vme-hg-vme-kvm1"]
    assert host["nqns"] and host["nqns"][0].startswith("nqn.")
    assert host["wwns"] == [] and host["iqns"] == []


async def test_setup_connectivity_nvme_fc_is_fabric(make_context, captured_logs):
    # NVMe-FC: fabric autoconnect, native NVMe multipath -> no multipath.conf, no
    # iscsiadm, no IP `nvme connect`.
    ctx = _nvmefc_ctx(make_context)
    r = await HpeVmeConnector(ctx).setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["protocol"] == "nvme-fc"
    assert "nvme_fc_connect" in r.data["steps"]
    assert "multipath_conf" not in r.data["steps"]
    assert r.data["portals"] == []  # fabric: no IP portal discovery
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert any("nvme connect-all -t fc" in ln for ln in ssh_cmds)
    assert not any("iscsiadm" in ln for ln in ssh_cmds)


async def test_resolve_mpath_nvme_uses_eui(make_context):
    ctx = _nvmefc_ctx(make_context)
    c = HpeVmeConnector(ctx)
    await c.provision(name="nv1", size="1T", vm="vm1", host_group="vme-hg")
    path = await c._resolve_mpath("nv1", "nvme-fc")
    assert "/nvme-eui." in path


# ---- teardown: libvirt detach + array disconnect (no fabricated endpoint) ----
async def test_teardown_disconnects_volume_from_array(make_context, monkeypatch):
    ctx = _ctx(make_context)
    calls = _capture_http(ctx, monkeypatch)
    r = await HpeVmeConnector(ctx).teardown(volume="v1", vm="vm1", host_group="vme-hg")
    assert r.success and r.data["status"] == "not_deployed"
    # Disconnected from the host group on the array.
    assert ("disconnect_volume", {"host": "vme-hg", "volume": "v1"}) in ctx.array.calls
    # Never calls the fabricated /api/instances/{vm}/volumes endpoint.
    assert not any("/volumes" in url for _m, url, _kw in calls)


# ====================================================================== #
# Feature 1: assess_cluster + reconcile_cluster (membership drift)
# ====================================================================== #
def _nfs_ctx(make_context, **overrides):
    connection = {
        "vme_manager_url": "https://vme-mgr.test.local",
        "username": "admin",
        "protocol": "nfs",
        "host_group": "vme-hg",
    }
    connection.update(overrides.pop("connection", {}))
    return make_context(connector_key="hpevme", connection=connection, **overrides)


# ---- capability + action specs advertised ----
def test_reconcile_capability_and_actions():
    assert Capability.RECONCILE_CLUSTER in HpeVmeConnector.capabilities()
    ids = {a.id for a in HpeVmeConnector.action_schemas()}
    assert {"assess_cluster", "reconcile_cluster"} <= ids
    assess = next(a for a in HpeVmeConnector.action_schemas()
                  if a.id == "assess_cluster")
    assert assess.long_running is False
    rec = next(a for a in HpeVmeConnector.action_schemas()
               if a.id == "reconcile_cluster")
    assert rec.destructive is True
    apply_f = next(f for f in rec.fields if f.name == "apply_removals")
    assert apply_f.type == FieldType.BOOL
    assert apply_f.default is False


# ---- assess_cluster: read-only, scores new nodes, reports departed ----
async def test_assess_cluster_reports_new_and_departed(make_context):
    # Synthetic 2-host cluster, EMPTY host group -> both nodes are "new" and (iscsi,
    # mock discovery) ready. No FA mutation occurs (read-only).
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.assess_cluster()
    assert r.success
    assert set(r.data["nodes"]) == {"vme-kvm1", "vme-kvm2"}
    new_by_node = {h["node"]: h for h in r.data["new_hosts"]}
    assert set(new_by_node) == {"vme-kvm1", "vme-kvm2"}
    assert all(h["ready"] for h in r.data["new_hosts"])
    assert new_by_node["vme-kvm1"]["host"] == "vme-hg-vme-kvm1"
    assert r.data["departed_hosts"] == []
    # READ-ONLY: nothing created on the array.
    assert "vme-hg" not in ctx.array.host_groups
    assert not any(op == "create_host" for op, _ in ctx.array.calls)


async def test_assess_cluster_reports_departed_member(make_context):
    # A member with no matching current node is reported as departed.
    ctx = _ctx(make_context)
    await ctx.array.create_host_group("vme-hg", ["vme-hg-vme-kvm1", "stale-host"])
    c = HpeVmeConnector(ctx)
    r = await c.assess_cluster()
    assert r.success
    assert r.data["departed_hosts"] == ["stale-host"]
    # kvm1 already a member -> only kvm2 is new.
    assert {h["node"] for h in r.data["new_hosts"]} == {"vme-kvm2"}


async def test_assess_cluster_matches_members_named_by_bare_hostname(make_context):
    # REGRESSION: FA hosts created by the VME morpheus-plugin / operator are named
    # by the node's bare hostname, NOT the {host_group}-{node} convention. The
    # match must recognize those, else EVERY host is flagged as both new AND
    # departed.
    ctx = _ctx(make_context)
    await ctx.array.create_host_group("vme-hg", ["vme-kvm1", "vme-kvm2"])
    c = HpeVmeConnector(ctx)
    r = await c.assess_cluster()
    assert r.success
    # Both nodes recognized as already configured -> nothing new, nothing departed.
    assert r.data["new_hosts"] == []
    assert r.data["departed_hosts"] == []


async def test_assess_cluster_not_ready_when_unreachable(make_context, monkeypatch):
    # A node whose initiator discovery fails (unreachable) scores not-ready.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)

    async def fail_discover(host):
        if host == "192.0.2.2":
            raise OSError("unreachable")
        return {"iqn": "iqn.kvm1", "nqn": None, "wwns": []}

    monkeypatch.setattr(c, "_discover_initiators_on", fail_discover)
    r = await c.assess_cluster()
    assert r.success
    by_node = {h["node"]: h for h in r.data["new_hosts"]}
    assert by_node["vme-kvm2"]["ready"] is False
    assert "vme-kvm2" in r.data["not_ready"]
    assert any("unreachable" in reason for reason in by_node["vme-kvm2"]["reasons"])


async def test_assess_cluster_nfs_nodes_only(make_context):
    # NFS has no block host group -> nodes-only assessment, no array calls.
    ctx = _nfs_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.assess_cluster()
    assert r.success
    assert set(r.data["nodes"]) == {"vme-kvm1", "vme-kvm2"}
    assert r.data["new_hosts"] == []
    assert r.data["departed_hosts"] == []
    assert not any(op == "get_host_group_members" for op, _ in ctx.array.calls)


# ---- reconcile_cluster: configures ready new hosts ----
async def test_reconcile_cluster_configures_ready_new_hosts(make_context):
    # Empty group -> both nodes new + ready -> both configured + placed in group.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.reconcile_cluster()
    assert r.success
    assert set(r.data["configured"]) == {"vme-kvm1", "vme-kvm2"}
    assert r.data["not_ready"] == []
    # register_hosts ran for the ready subset -> FA hosts created + grouped.
    assert "vme-hg-vme-kvm1" in ctx.array.hosts
    assert "vme-hg-vme-kvm2" in ctx.array.hosts
    assert "vme-hg" in ctx.array.host_groups


async def test_reconcile_cluster_skips_not_ready(make_context, monkeypatch):
    # kvm2 unreachable -> skipped (flagged not_ready), kvm1 configured.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)

    async def fail_discover(host):
        if host == "192.0.2.2":
            raise OSError("unreachable")
        return {"iqn": "iqn.kvm1", "nqn": None, "wwns": []}

    monkeypatch.setattr(c, "_discover_initiators_on", fail_discover)
    r = await c.reconcile_cluster()
    assert r.success
    assert r.data["configured"] == ["vme-kvm1"]
    assert r.data["not_ready"] == ["vme-kvm2"]
    # Only the ready node's FA host exists; the not-ready node was never touched.
    assert "vme-hg-vme-kvm1" in ctx.array.hosts
    assert "vme-hg-vme-kvm2" not in ctx.array.hosts


# ---- reconcile_cluster: departed hosts flag (default) vs remove ----
async def test_reconcile_cluster_flags_departed_by_default(make_context):
    ctx = _ctx(make_context)
    await ctx.array.create_host_group("vme-hg", ["vme-hg-vme-kvm1", "stale-host"])
    c = HpeVmeConnector(ctx)
    r = await c.reconcile_cluster()  # apply_removals defaults to False
    assert r.success
    assert r.data["pending_removals"] == ["stale-host"]
    assert "removed" not in r.data
    # Flag-only: the stale member is still in the group, host still exists.
    assert "stale-host" in ctx.array.host_groups["vme-hg"]["hosts"]


async def test_reconcile_cluster_removes_departed_when_flagged(make_context):
    ctx = _ctx(make_context)
    await ctx.array.create_host_group("vme-hg", ["vme-hg-vme-kvm1", "stale-host"])
    ctx.array.hosts["stale-host"] = {"iqns": [], "wwns": [], "nqns": []}
    c = HpeVmeConnector(ctx)
    r = await c.reconcile_cluster(apply_removals=True)
    assert r.success
    assert r.data["removed"] == ["stale-host"]
    assert "pending_removals" not in r.data
    # Removed from the group and deleted from the array.
    assert "stale-host" not in ctx.array.host_groups["vme-hg"]["hosts"]
    assert "stale-host" not in ctx.array.hosts


async def test_reconcile_cluster_keeps_bare_named_hosts_on_removal(make_context):
    # REGRESSION: members named by bare hostname must NOT be flagged departed and
    # deleted when apply_removals=True (they back live VMs). Only a truly stale
    # member is removed.
    ctx = _ctx(make_context)
    await ctx.array.create_host_group(
        "vme-hg", ["vme-kvm1", "vme-kvm2", "stale-host"])
    ctx.array.hosts["vme-kvm1"] = {"iqns": [], "wwns": [], "nqns": []}
    ctx.array.hosts["vme-kvm2"] = {"iqns": [], "wwns": [], "nqns": []}
    ctx.array.hosts["stale-host"] = {"iqns": [], "wwns": [], "nqns": []}
    c = HpeVmeConnector(ctx)
    r = await c.reconcile_cluster(apply_removals=True)
    assert r.success
    assert r.data["removed"] == ["stale-host"]
    assert "vme-kvm1" in ctx.array.host_groups["vme-hg"]["hosts"]
    assert "vme-kvm2" in ctx.array.host_groups["vme-hg"]["hosts"]
    assert "vme-kvm1" in ctx.array.hosts and "vme-kvm2" in ctx.array.hosts
    assert "stale-host" not in ctx.array.hosts


async def test_reconcile_cluster_routes_via_dispatch(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.dispatch("reconcile_cluster", {"apply_removals": False})
    assert r.success
    assert "configured" in r.data


# ====================================================================== #
# Feature 2: NFS datastore for QCOW2 VM images
# ====================================================================== #
async def test_register_hosts_nfs_skips_block_setup(make_context):
    # NFS: no block host registration -> no FA host/group created.
    ctx = _nfs_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.register_hosts(host_group="vme-hg")
    assert r.success
    assert r.artifacts["protocol"] == "nfs"
    assert "vme-hg" not in ctx.array.host_groups
    assert not any(op == "create_host" for op, _ in ctx.array.calls)


async def test_setup_connectivity_nfs_skips_block_setup(make_context, captured_logs):
    ctx = _nfs_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_connectivity(host_group="vme-hg")
    assert r.success
    assert r.data["protocol"] == "nfs"
    assert r.data["steps"] == []
    # No portal discovery, no multipath / iscsiadm.
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert not any("multipath" in ln or "iscsiadm" in ln for ln in ssh_cmds)


async def test_provision_nfs_datastore(make_context, monkeypatch):
    # Creates FA file system + export, mounts on the KVM hosts, registers a VME
    # 'nfs' datastore for QCOW2 VM images. Marked not hardware-validated.
    ctx = _nfs_ctx(make_context)
    calls = _capture_http(ctx, monkeypatch)
    c = HpeVmeConnector(ctx)
    r = await c.provision_nfs_datastore(name="vme-nfs")
    assert r.success
    assert r.artifacts["type"] == "nfs"
    assert r.artifacts["hardware_validated"] is False
    # FA File system + export created; NFS portal discovered.
    assert "vme-nfs" in ctx.array.filesystems
    assert any(op == "create_nfs_export" for op, _ in ctx.array.calls)
    assert any(op == "get_nfs_data_interfaces" for op, _ in ctx.array.calls)
    assert r.artifacts["portal"] == "10.30.30.30"
    # Registered a VME 'nfs'-type datastore for images.
    posts = [c for c in calls if c[0] == "POST" and "data-stores" in c[1]]
    assert posts
    ds = posts[0][2].get("json_body", {}).get("datastore", {})
    assert ds["type"] == "nfs"
    assert ds["content"] == "images"


async def test_provision_nfs_datastore_mounts_on_kvm_hosts(make_context, captured_logs):
    ctx = _nfs_ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.provision_nfs_datastore(name="vme-nfs", mountpoint="/mnt/vme-nfs")
    assert r.success
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    # OS NFS mount of <portal>:<export> on the KVM hosts.
    assert any("mount -t nfs 10.30.30.30:" in ln for ln in ssh_cmds)
    assert any("/etc/fstab" in ln for ln in ssh_cmds)


async def test_provision_nfs_datastore_dry_run(make_context):
    ctx = _nfs_ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = HpeVmeConnector(ctx)
    r = await c.provision_nfs_datastore(name="vme-nfs")
    assert r.success
    assert "vme-nfs" not in ctx.array.filesystems


# ====================================================================== #
# Feature 3: ISO storage SPECIAL CASE (kernel NFS on the VME Manager host)
# ====================================================================== #
async def test_setup_iso_storage_uses_directory_datastore_not_nfs(make_context,
                                                                  monkeypatch):
    # The ISO workaround: FA export + kernel NFS mount on the MANAGER host + a VME
    # 'directory' (local) datastore. It must NOT register an 'nfs'-type datastore
    # (VME's Java NFS client uses high source ports the FlashArray blocks).
    ctx = _ctx(make_context)
    calls = _capture_http(ctx, monkeypatch)
    c = HpeVmeConnector(ctx)
    r = await c.setup_iso_storage(name="vme-iso")
    assert r.success
    assert r.artifacts["datastore_type"] == "directory"
    assert r.artifacts["hardware_validated"] is False
    # FA File system + NFS export created.
    assert "vme-iso" in ctx.array.filesystems
    assert any(op == "create_nfs_export" for op, _ in ctx.array.calls)
    # Registered a VME 'directory' datastore -- NOT 'nfs'.
    posts = [c for c in calls if c[0] == "POST" and "data-stores" in c[1]]
    assert posts
    ds = posts[0][2].get("json_body", {}).get("datastore", {})
    assert ds["type"] == "directory"
    assert ds["type"] != "nfs"
    assert ds["content"] == "iso"
    assert ds["directoryPath"] == r.artifacts["mountpoint"]


async def test_setup_iso_storage_mounts_on_manager_host(make_context, captured_logs):
    # The kernel NFS mount must target the VME MANAGER host (not the KVM hosts) so
    # it uses privileged source ports the array allows.
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.setup_iso_storage(name="vme-iso", mountpoint="/mnt/iso")
    assert r.success
    assert r.artifacts["manager_host"] == "vme-mgr.test.local"
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    # Mount runs against the manager host, with a kernel `mount -t nfs`.
    assert any("@vme-mgr.test.local" in ln and "mount -t nfs" in ln
               for ln in ssh_cmds)
    # NOT mounted on the KVM compute hosts (10.0.0.x / mgr fallback).
    assert not any("@192.0.2.2" in ln for ln in ssh_cmds)


async def test_setup_iso_storage_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    ctx.runner.dry_run = True
    c = HpeVmeConnector(ctx)
    r = await c.setup_iso_storage(name="vme-iso")
    assert r.success
    assert r.artifacts["datastore_type"] == "directory"
    assert "vme-iso" not in ctx.array.filesystems


async def test_teardown_iso_storage_unmounts_and_deletes(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    await c.setup_iso_storage(name="vme-iso", mountpoint="/mnt/iso")
    r = await c.teardown_iso_storage(name="vme-iso", mountpoint="/mnt/iso",
                                     eradicate=True)
    assert r.success
    # Export + file system deleted (eradicated) on the array.
    assert "vme-iso" not in ctx.array.filesystems
    assert any(op == "delete_nfs_export" for op, _ in ctx.array.calls)
    assert any(op == "delete_filesystem" and kw.get("eradicate") is True
               for op, kw in ctx.array.calls)
    # Unmount + fstab cleanup ran on the manager host.
    ssh_cmds = [ln for ln in captured_logs if ln.startswith("[ssh ")]
    assert any("@vme-mgr.test.local" in ln and "umount" in ln for ln in ssh_cmds)
    assert any("sed -i" in ln and "/etc/fstab" in ln for ln in ssh_cmds)


async def test_setup_iso_storage_routes_via_dispatch(make_context):
    ctx = _ctx(make_context)
    c = HpeVmeConnector(ctx)
    r = await c.dispatch("setup_iso_storage", {"name": "vme-iso"})
    assert r.success
    assert r.artifacts["datastore_type"] == "directory"
