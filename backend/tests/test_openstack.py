"""Unit tests for the OpenStack (Cinder / Everpure driver) connector.

All tests run against the mock FlashArray + mock JobRunner via the
``make_context`` fixture (PHIF_MOCK_MODE=1), so nothing touches a real array,
controller, or network.
"""

import dataclasses

import pytest

from phif.connectors.base import Capability, OpResult, Protocol
from phif.connectors.openstack import pure
from phif.connectors.openstack.connector import OpenStackConnector


# --------------------------------------------------------------------------- #
# Context helper
# --------------------------------------------------------------------------- #
def _os_ctx(make_context, *, dry_run=False, with_array=True, array_endpoint="10.0.0.10",
            array_token="tok-123", **conn):
    # NOTE: san_ip and pure_api_token are NOT in the connection — they are
    # derived from the hypervisor's associated FlashArray (endpoint + token).
    connection = {
        "controller_host": "controller.test.local",
        "ssh_user": "stack",
        "cinder_conf_path": "/etc/cinder/cinder.conf",
        "protocol": "iscsi",
        "backend_name": "pure",
    }
    connection.update(conn)
    ctx = make_context(connector_key="openstack", connection=connection,
                       secrets={"ssh_password": "pw"},
                       with_array=with_array)
    # The associated array supplies san_ip (from its mgmt endpoint) and the
    # API token (via ctx.resolve_token / array_token).
    if with_array and array_endpoint is not None:
        ctx.array.endpoint = array_endpoint
    if array_token is not None:
        ctx = dataclasses.replace(ctx, array_token=array_token)
    if dry_run:
        ctx = dataclasses.replace(ctx, dry_run=True)
    return ctx


# --------------------------------------------------------------------------- #
# Static metadata / schema
# --------------------------------------------------------------------------- #
def test_metadata():
    assert OpenStackConnector.key == "openstack"
    assert OpenStackConnector.maturity == "ga"
    caps = OpenStackConnector.capabilities()
    assert Capability.CONNECT in caps
    for c in (Capability.DEPLOY_PLUGIN, Capability.CONFIGURE,
              Capability.PROVISION_VOLUME, Capability.SNAPSHOT, Capability.CLONE,
              Capability.RESIZE, Capability.QOS, Capability.REPLICATION,
              Capability.HEALTH, Capability.REMOVE):
        assert c in caps
    assert Protocol.ISCSI in OpenStackConnector.SUPPORTED_PROTOCOLS
    assert Protocol.FC in OpenStackConnector.SUPPORTED_PROTOCOLS
    assert Protocol.NVME_TCP in OpenStackConnector.SUPPORTED_PROTOCOLS
    assert Protocol.NVME_ROCE in OpenStackConnector.SUPPORTED_PROTOCOLS


def test_descriptor_serializes():
    d = OpenStackConnector.descriptor()
    assert d["key"] == "openstack"
    assert "deploy_plugin" in d["capabilities"]
    field_names = {f["name"] for f in d["target_schema"]}
    assert {"controller_host", "ssh_user", "protocol",
            "backend_name"}.issubset(field_names)
    # san_ip and pure_api_token come from the associated array, not the form.
    assert "san_ip" not in field_names
    assert "pure_api_token" not in field_names
    action_ids = {a["id"] for a in d["actions"]}
    assert {"deploy", "configure", "provision", "snapshot", "clone", "resize",
            "set_qos", "configure_replication", "health_check",
            "teardown"}.issubset(action_ids)


def test_is_discovered():
    from phif.connectors.registry import discover, get_connector_class
    registry = discover()
    assert "openstack" in registry
    assert get_connector_class("openstack") is OpenStackConnector


# --------------------------------------------------------------------------- #
# pure.py stanza helpers
# --------------------------------------------------------------------------- #
def test_driver_class_for():
    assert pure.driver_class_for("iscsi").endswith("PureISCSIDriver")
    assert pure.driver_class_for("fc").endswith("PureFCDriver")
    assert pure.driver_class_for("nvme-tcp").endswith("PureNVMEDriver")
    assert pure.driver_class_for("nvme-roce").endswith("PureNVMEDriver")
    with pytest.raises(ValueError):
        pure.driver_class_for("nfs")


def test_render_stanza_iscsi():
    s = pure.render_stanza(backend_name="pure", protocol="iscsi",
                           san_ip="10.0.0.10", pure_api_token="tok")
    assert "[pure]" in s
    assert "volume_backend_name = pure" in s
    assert "cinder.volume.drivers.pure.PureISCSIDriver" in s
    assert "san_ip = 10.0.0.10" in s
    assert "pure_api_token = tok" in s
    assert "pure_eradicate_on_delete = false" in s
    assert "pure_nvme_transport" not in s


def test_render_stanza_fc_selects_fc_driver():
    s = pure.render_stanza(backend_name="pure", protocol="fc",
                           san_ip="10.0.0.10", pure_api_token="tok")
    assert "[pure]" in s
    assert "volume_backend_name = pure" in s
    # FC must use the FC driver, not the iSCSI/NVMe driver.
    assert "cinder.volume.drivers.pure.PureFCDriver" in s
    assert "PureISCSIDriver" not in s
    assert "PureNVMEDriver" not in s
    # No iSCSI-only or NVMe-only keys should appear in an FC stanza.
    assert "pure_iscsi_cidr" not in s
    assert "pure_nvme_transport" not in s


def test_render_stanza_nvme_includes_transport():
    s = pure.render_stanza(backend_name="pure", protocol="nvme-roce",
                           san_ip="10.0.0.10", pure_api_token="tok",
                           replication_device="backend_id:sec,san_ip:1.1.1.1")
    assert "pure_nvme_transport = roce" in s
    assert "replication_device = backend_id:sec,san_ip:1.1.1.1" in s


def test_render_stanza_iscsi_cidr():
    s = pure.render_stanza(backend_name="pure", protocol="iscsi",
                           san_ip="10.0.0.10", pure_api_token="tok",
                           pure_iscsi_cidr="10.0.0.0/24")
    assert "pure_iscsi_cidr = 10.0.0.0/24" in s
    # single-cidr only -> no list line
    assert "pure_iscsi_cidr_list" not in s


def test_render_stanza_iscsi_cidr_list():
    s = pure.render_stanza(backend_name="pure", protocol="iscsi",
                           san_ip="10.0.0.10", pure_api_token="tok",
                           pure_iscsi_cidr_list="10.0.0.0/24,10.0.1.0/24")
    assert "pure_iscsi_cidr_list = 10.0.0.0/24,10.0.1.0/24" in s


def test_render_stanza_fc_omits_iscsi_cidr():
    # An iSCSI CIDR passed with the FC protocol must NOT leak into the stanza.
    s = pure.render_stanza(backend_name="pure", protocol="fc",
                           san_ip="10.0.0.10", pure_api_token="tok",
                           pure_iscsi_cidr="10.0.0.0/24")
    assert "pure_iscsi_cidr" not in s
    assert "cinder.volume.drivers.pure.PureFCDriver" in s


def test_render_stanza_nvme_options():
    s = pure.render_stanza(backend_name="pure", protocol="nvme-tcp",
                           san_ip="10.0.0.10", pure_api_token="tok",
                           nvme_options="pure_nvme_cidr = 10.0.2.0/24")
    assert "pure_nvme_transport = tcp" in s
    assert "pure_nvme_cidr = 10.0.2.0/24" in s
    # iSCSI cidr must not be rendered on the NVMe stanza even if supplied.
    s2 = pure.render_stanza(backend_name="pure", protocol="nvme-tcp",
                            san_ip="10.0.0.10", pure_api_token="tok",
                            pure_iscsi_cidr="10.0.0.0/24")
    assert "pure_iscsi_cidr" not in s2


def test_stanza_exists():
    conf = "[DEFAULT]\nenabled_backends = lvm\n\n[pure]\nvolume_driver = x\n"
    assert pure.stanza_exists(conf, "pure")
    assert not pure.stanza_exists(conf, "pure2")
    assert not pure.stanza_exists("", "pure")


def test_build_enabled_backends():
    assert pure.build_enabled_backends("", "pure") == "pure"
    assert pure.build_enabled_backends("enabled_backends = lvm\n", "pure") == "lvm,pure"
    # idempotent: already present -> None
    assert pure.build_enabled_backends("enabled_backends = lvm,pure\n", "pure") is None


# --------------------------------------------------------------------------- #
# validate_connection
# --------------------------------------------------------------------------- #
async def test_validate_connection_ok(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.validate_connection()
    assert r.success
    assert r.data["host"] == "controller.test.local"


async def test_validate_connection_no_host(make_context):
    from phif.connectors.base import ConnectionValidationError
    ctx = _os_ctx(make_context, controller_host="")
    c = OpenStackConnector(ctx)
    with pytest.raises(ConnectionValidationError):
        await c.validate_connection()


# --------------------------------------------------------------------------- #
# deploy_integration
# --------------------------------------------------------------------------- #
async def test_deploy_integration(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.deploy_integration()
    assert r.success
    assert r.artifacts["backend"] == "pure"
    # diff was shown
    assert any("planned cinder.conf changes" in line for line in captured_logs)
    assert any("PureISCSIDriver" in line for line in captured_logs)


async def test_deploy_fc_renders_fc_driver(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context, protocol="fc"))
    r = await c.deploy_integration()
    assert r.success
    assert r.artifacts["backend"] == "pure"
    # The planned diff selects the FC driver, not iSCSI/NVMe.
    assert any("PureFCDriver" in line for line in captured_logs)
    assert not any("PureISCSIDriver" in line for line in captured_logs)
    # No iSCSI-only key leaks into an FC stanza.
    assert not any("pure_iscsi_cidr" in line for line in captured_logs)
    assert not any("pure_nvme_transport" in line for line in captured_logs)


async def test_deploy_nvme_renders_nvme_driver(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context, protocol="nvme-tcp"))
    r = await c.deploy_integration()
    assert r.success
    assert any("PureNVMEDriver" in line for line in captured_logs)
    assert any("pure_nvme_transport = tcp" in line for line in captured_logs)
    assert not any("PureFCDriver" in line for line in captured_logs)


async def test_deploy_dry_run_makes_no_changes(make_context, captured_logs):
    ctx = _os_ctx(make_context, dry_run=True)
    c = OpenStackConnector(ctx)
    r = await c.deploy_integration()
    assert r.success
    assert r.data["status"] == "planned"
    assert "stanza" in r.data
    # no append/restart command issued
    assert not any("cat >>" in line for line in captured_logs)
    assert not any("systemctl restart" in line for line in captured_logs)


async def test_deploy_mints_token_when_absent(make_context):
    # No array_token available -> mint from mock array.
    ctx = make_context(connector_key="openstack",
                       connection={"controller_host": "c.local", "protocol": "iscsi",
                                   "backend_name": "pure"},
                       secrets={"ssh_password": "pw"})
    ctx.array.endpoint = "10.0.0.1"
    c = OpenStackConnector(ctx)
    r = await c.deploy_integration()
    assert r.success
    assert "openstack-cinder" in ctx.array.api_tokens


async def test_deploy_derives_san_ip_from_array(make_context, captured_logs):
    # No san_ip/pure_api_token provided: both come from the associated array.
    c = OpenStackConnector(_os_ctx(make_context, array_endpoint="10.9.9.9"))
    r = await c.deploy_integration()
    assert r.success
    # san_ip rendered from the array's mgmt endpoint, token from array_token.
    assert any("san_ip = 10.9.9.9" in line for line in captured_logs)
    assert any("pure_api_token = tok-123" in line for line in captured_logs)


async def test_deploy_strips_scheme_from_array_endpoint(make_context, captured_logs):
    # A URL-style endpoint (with scheme/port) reduces to a bare host for san_ip.
    c = OpenStackConnector(_os_ctx(make_context, array_endpoint="https://10.8.8.8:443"))
    r = await c.deploy_integration()
    assert r.success
    assert any("san_ip = 10.8.8.8" in line for line in captured_logs)


async def test_deploy_san_ip_explicit_override(make_context, captured_logs):
    # An explicit san_ip kwarg overrides the array-derived value.
    c = OpenStackConnector(_os_ctx(make_context, array_endpoint="10.9.9.9"))
    r = await c.deploy_integration(san_ip="10.1.2.3", pure_api_token="override-tok")
    assert r.success
    assert any("san_ip = 10.1.2.3" in line for line in captured_logs)
    assert any("pure_api_token = override-tok" in line for line in captured_logs)


async def test_deploy_no_array_fails(make_context):
    # No associated array and nothing explicit -> clear failure (not dry-run).
    c = OpenStackConnector(_os_ctx(make_context, with_array=False, array_token=None))
    r = await c.deploy_integration()
    assert not r.success
    assert "associated" in r.message.lower()


async def test_deploy_no_array_dry_run_plans(make_context):
    # In dry-run, planning still proceeds with a placeholder token.
    c = OpenStackConnector(_os_ctx(make_context, with_array=False, array_token=None,
                                   dry_run=True))
    r = await c.deploy_integration()
    assert r.success
    assert r.data["status"] == "planned"


# --------------------------------------------------------------------------- #
# interface binding (pure_iscsi_cidr / discovery)
# --------------------------------------------------------------------------- #
async def test_deploy_iscsi_with_cidr_from_param(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.deploy_integration(pure_iscsi_cidr="10.0.0.0/24")
    assert r.success
    assert any("pure_iscsi_cidr = 10.0.0.0/24" in line for line in captured_logs)


async def test_deploy_iscsi_with_cidr_from_target(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context, pure_iscsi_cidr="10.5.0.0/24"))
    r = await c.deploy_integration()
    assert r.success
    assert any("pure_iscsi_cidr = 10.5.0.0/24" in line for line in captured_logs)


async def test_deploy_fc_omits_iscsi_cidr(make_context, captured_logs):
    # Even if a CIDR is supplied, an FC deploy must not render it.
    c = OpenStackConnector(_os_ctx(make_context, protocol="fc"))
    r = await c.deploy_integration(pure_iscsi_cidr="10.0.0.0/24")
    assert r.success
    assert not any("pure_iscsi_cidr" in line for line in captured_logs)


async def test_deploy_nvme_with_options(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context, protocol="nvme-tcp"))
    r = await c.deploy_integration(nvme_options="pure_nvme_cidr = 10.0.2.0/24")
    assert r.success
    assert any("pure_nvme_transport = tcp" in line for line in captured_logs)
    assert any("pure_nvme_cidr = 10.0.2.0/24" in line for line in captured_logs)


async def test_deploy_iscsi_cidr_dry_run(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context, dry_run=True))
    r = await c.deploy_integration(pure_iscsi_cidr="10.0.0.0/24")
    assert r.success
    assert r.data["status"] == "planned"
    assert "pure_iscsi_cidr = 10.0.0.0/24" in r.data["stanza"]
    assert not any("cat >>" in line for line in captured_logs)


async def test_discover_options_nics(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    opts = await c.discover_options("nics")
    assert opts
    assert all("value" in o and "label" in o for o in opts)
    assert any(o["value"] == "eth0" for o in opts)


async def test_discover_options_nvme_sources(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    opts = await c.discover_options("nvme_sources")
    assert opts
    assert all("value" in o for o in opts)


async def test_discover_options_fc_hbas(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    opts = await c.discover_options("fc_hbas")
    assert opts
    assert all("value" in o for o in opts)


async def test_discover_options_unknown_kind(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    assert await c.discover_options("bogus") == []


async def test_discover_options_no_host(make_context):
    c = OpenStackConnector(_os_ctx(make_context, controller_host=""))
    assert await c.discover_options("nics") == []


def test_deploy_action_has_binding_fields():
    specs = {a.id: a for a in OpenStackConnector.action_schemas()}
    deploy_fields = {f.name: f for f in specs["deploy"].fields}
    assert "pure_iscsi_cidr" in deploy_fields
    assert deploy_fields["pure_iscsi_cidr"].required is False
    # Array credentials are derived from the associated array, not form fields.
    assert "san_ip" not in deploy_fields
    assert "pure_api_token" not in deploy_fields
    # nics field is a discoverable multiselect
    from phif.connectors.base import FieldType
    assert deploy_fields["nics"].type == FieldType.MULTISELECT
    assert deploy_fields["nics"].options_source == "nics"


# --------------------------------------------------------------------------- #
# configure / volume types / qos
# --------------------------------------------------------------------------- #
async def test_configure_volume_type(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.configure(volume_type="pure-gold")
    assert r.success
    assert r.artifacts["volume_type"] == "pure-gold"


async def test_configure_with_qos(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.configure(volume_type="pure-gold", qos_name="gold-qos",
                          max_iops=10000)
    assert r.success
    assert r.artifacts["qos"] == "gold-qos"


async def test_set_qos(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.set_qos(qos_name="q1", volume_type="pure", max_iops=5000, max_bw=1024)
    assert r.success
    assert r.artifacts["qos"] == "q1"


# --------------------------------------------------------------------------- #
# volume lifecycle
# --------------------------------------------------------------------------- #
async def test_provision(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.provision(name="vol1", size=20, volume_type="pure")
    assert r.success
    assert r.artifacts["volume"] == "vol1"


async def test_snapshot(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.snapshot(volume="vol1", name="snap1")
    assert r.success
    assert r.artifacts["snapshot"] == "snap1"


async def test_snapshot_default_name(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.snapshot(volume="vol1")
    assert r.success
    assert r.artifacts["snapshot"] == "vol1-snap"


async def test_clone(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.clone(source="vol1", dest="vol2", size=30)
    assert r.success
    assert r.artifacts["volume"] == "vol2"


async def test_resize(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.resize(volume="vol1", size=50)
    assert r.success


async def test_configure_replication(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.configure_replication(volume_type="pure-repl")
    assert r.success
    assert any("replication_device" in line for line in captured_logs)


async def test_health_check(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.health_check()
    assert r.success
    assert "output" in r.data


# --------------------------------------------------------------------------- #
# teardown
# --------------------------------------------------------------------------- #
async def test_teardown(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.teardown()
    assert r.success
    assert r.data["status"] == "removed"


async def test_teardown_dry_run(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context, dry_run=True))
    r = await c.teardown()
    assert r.success
    assert r.data["status"] == "planned"
    assert not any("systemctl restart" in line for line in captured_logs)


# --------------------------------------------------------------------------- #
# dispatch routing
# --------------------------------------------------------------------------- #
async def test_dispatch_provision(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.dispatch("provision", {"name": "dvol", "size": 10})
    assert r.success
    assert r.artifacts["volume"] == "dvol"


async def test_dispatch_unknown(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.dispatch("nope", {})
    assert not r.success


# --------------------------------------------------------------------------- #
# cluster awareness: wizard_steps / list_nodes / validate_cluster
# --------------------------------------------------------------------------- #
def test_wizard_steps():
    # OpenStack has no register_hosts step: the Cinder driver auto-manages array
    # hosts. setup_connectivity IS included — it tunes the compute-host data path
    # (multipath / iSCSI iface / ARP), which os-brick does not do for you.
    steps = OpenStackConnector.wizard_steps()
    assert steps == ["deploy", "configure", "setup_connectivity"]
    assert "register_hosts" not in steps


def test_descriptor_includes_wizard_steps():
    d = OpenStackConnector.descriptor()
    assert d["wizard_steps"] == ["deploy", "configure", "setup_connectivity"]


async def test_list_nodes_mock_synthetic(make_context):
    # In mock mode run_ssh returns nothing, so we synthesize a controller + 2 compute.
    c = OpenStackConnector(_os_ctx(make_context))
    nodes = await c.list_nodes()
    roles = [n.info.get("role") for n in nodes]
    assert roles.count("controller") == 1
    assert roles.count("compute") == 2
    # the controller node uses the configured controller_host
    controller = next(n for n in nodes if n.info.get("role") == "controller")
    assert controller.host == "controller.test.local"


async def test_list_nodes_dry_run_synthetic(make_context):
    c = OpenStackConnector(_os_ctx(make_context, dry_run=True))
    nodes = await c.list_nodes()
    assert len(nodes) == 3
    assert all("name" in n.to_dict() and "host" in n.to_dict() for n in nodes)


async def test_list_nodes_parses_service_list(make_context, monkeypatch):
    # Simulate real (non-mock) SSH output from the openstack CLIs.
    c = OpenStackConnector(_os_ctx(make_context))
    c.ctx.runner.mock = False  # force the non-mock parse path

    vol_json = (
        '[{"Binary": "cinder-volume", "Host": "ctrl1@pure", '
        '"Status": "enabled", "State": "up"}]')
    comp_json = (
        '[{"Binary": "nova-compute", "Host": "cmp1", "Status": "enabled", '
        '"State": "up"}, {"Binary": "nova-conductor", "Host": "ctrl1"}]')

    async def fake_ssh(command, **_kw):   # tolerate sudo/redact/host kwargs
        if "volume service list" in command:
            return vol_json
        if "compute service list" in command:
            return comp_json
        return ""

    monkeypatch.setattr(c, "_ssh", fake_ssh)
    nodes = await c.list_nodes()
    by_name = {n.name: n for n in nodes}
    assert "ctrl1" in by_name  # hostname@backend stripped to hostname
    assert by_name["ctrl1"].info["role"] == "controller"
    assert "cmp1" in by_name
    assert by_name["cmp1"].info["role"] == "compute"
    # nova-conductor (not nova-compute) is ignored
    assert all(n.info.get("binary") != "nova-conductor" for n in nodes)


async def test_list_nodes_fallback_single_controller(make_context, monkeypatch):
    # Non-mock with empty CLI output -> fall back to the single controller_host.
    c = OpenStackConnector(_os_ctx(make_context))
    c.ctx.runner.mock = False

    async def empty_ssh(command, **_kw):
        return ""

    monkeypatch.setattr(c, "_ssh", empty_ssh)
    nodes = await c.list_nodes()
    assert len(nodes) == 1
    assert nodes[0].host == "controller.test.local"
    assert nodes[0].info["role"] == "controller"


async def test_validate_cluster_ok(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.validate_cluster()
    assert r.success
    assert r.data["iface_binding"] == "host-managed"
    assert len(r.data["controllers"]) == 1
    assert len(r.data["computes"]) == 2
    assert "host-managed" in r.message
    assert any("host-managed" in line for line in captured_logs)


async def test_validate_cluster_reports_iscsi_cidr(make_context, captured_logs):
    c = OpenStackConnector(_os_ctx(make_context, pure_iscsi_cidr="10.0.0.0/24"))
    r = await c.validate_cluster()
    assert r.success
    assert r.data["iscsi_cidr"] == "10.0.0.0/24"
    assert any("10.0.0.0/24" in line for line in captured_logs)


async def test_validate_cluster_cidr_param_override(make_context):
    c = OpenStackConnector(_os_ctx(make_context))
    r = await c.validate_cluster(pure_iscsi_cidr="10.7.0.0/24")
    assert r.success
    assert r.data["iscsi_cidr"] == "10.7.0.0/24"
