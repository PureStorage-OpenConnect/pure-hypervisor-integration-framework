"""Unit tests for the XCP-ng / XenServer connector (mock array + runner).

Covers the per-VDI-volume SMAPIv3 'purefa' design: capability/maturity honesty,
the bundled SMAPIv3 plugin (volume + datapath + host plugin), per-VM FlashArray
volume groups, and end-to-end mock-mode runs of deploy/register/provision/
snapshot/clone/resize/health that exercise ctx.array, plus dry-run no-ops and
dispatch routing.
"""

import importlib.util
import os
import sys

import pytest

from phif.connectors.base import (
    Capability,
    ConnectionValidationError,
    ConnectorContext,
    HypervisorTarget,
    Protocol,
)
from phif.connectors.xcpng import XcpngConnector
from phif.connectors.xcpng import connector as xcpng_mod
from phif.jobs.runner import JobRunner


# ----------------------------------------------------------------- helpers ---
def _ctx(make_context, *, protocol="iscsi", with_array=True, **conn):
    connection = {
        "pool_master_host": "xcp-master.test.local",
        "ssh_user": "root",
        "protocol": protocol,
        "sr_name": "purefa",
        "host_group": "xcp-pool-hg",
    }
    connection.update(conn)
    return make_context(
        connector_key="xcpng",
        connection=connection,
        secrets={"ssh_password": "s3cret"},
        with_array=with_array,
    )


def _seed_hg(ctx, name="xcp-pool-hg"):
    """Pre-register a populated host group on the mock array. deploy now requires
    host objects to exist before creating the SR (register-before-deploy), so deploy
    tests seed the group the way the wizard's register_hosts step would."""
    ctx.array.host_groups[name] = {"hosts": [f"{name}-h1"]}
    return ctx


def _load_fa_module():
    """Import the bundled SMAPIv3 purefa_fa.py off-host for direct unit testing.

    purefa_fa.py is stdlib-only (json/os/ssl/urllib), so it imports cleanly off
    a dom0 -- no xapi.storage shims needed (unlike sr.py / volume.py)."""
    fa_path = os.path.join(xcpng_mod._SMAPIV3_SRC_DIR, "purefa_fa.py")
    spec = importlib.util.spec_from_file_location("purefa_fa_under_test", fa_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["purefa_fa_under_test"] = mod
    spec.loader.exec_module(mod)
    return mod


# --------------------------------------------------------------- metadata ---
def test_descriptor_metadata():
    d = XcpngConnector.descriptor()
    assert d["key"] == "xcpng"
    assert d["name"] == "XCP-ng / XenServer (Everpure SR driver)"
    assert d["maturity"] == "ga"
    caps = set(d["capabilities"])
    for expected in ("connect", "deploy_plugin", "host_register", "connectivity",
                     "provision_volume", "snapshot", "clone", "resize",
                     "health", "remove"):
        assert expected in caps
    assert set(d["protocols"]) == {"iscsi", "fc", "nvme-tcp", "nfs"}


def test_supported_protocols():
    assert XcpngConnector.SUPPORTED_PROTOCOLS == {
        Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP, Protocol.NFS}


def test_capabilities_include_connect():
    assert Capability.CONNECT in XcpngConnector.capabilities()


def test_capability_honesty_no_overclaim():
    # Only declared capabilities should be present; we must not claim e.g.
    # REPLICATION / QOS / CONFIGURE we do not implement.
    caps = XcpngConnector.capabilities()
    assert Capability.REPLICATION not in caps
    assert Capability.QOS not in caps
    assert Capability.CONFIGURE not in caps


def test_target_schema_fields():
    names = {f.name for f in XcpngConnector.target_schema()}
    assert {"pool_master_host", "ssh_user", "ssh_password", "ssh_key",
            "protocol", "sr_name", "host_group"} <= names


def test_target_schema_protocol_includes_fc():
    proto = next(f for f in XcpngConnector.target_schema() if f.name == "protocol")
    assert "fc" in (proto.options or [])


def test_target_schema_has_host_wwns_field():
    names = {f.name for f in XcpngConnector.target_schema()}
    assert "host_wwns" in names


def test_action_schemas_have_standard_ids():
    ids = {a.id for a in XcpngConnector.action_schemas()}
    assert {"deploy", "register_hosts", "setup_connectivity", "provision",
            "snapshot", "clone", "resize", "health_check", "teardown"} <= ids


# ------------------------------------------------------- validate_connection ---
async def test_validate_connection_ok(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context))
    r = await c.validate_connection()
    assert r.success
    assert r.data["host"] == "xcp-master.test.local"
    assert any("host-list" in line for line in captured_logs)
    assert r.data["array"]["name"] == "mock-array"


async def test_validate_connection_no_array(make_context):
    c = XcpngConnector(_ctx(make_context, with_array=False))
    r = await c.validate_connection()
    assert r.success
    assert r.data["array"] == {}


async def test_validate_connection_requires_host():
    async def emit(_):
        return None

    target = HypervisorTarget(id="x", connector_key="xcpng", name="x",
                              connection={"protocol": "iscsi"}, secrets={})
    ctx = ConnectorContext(target=target, log=emit, runner=JobRunner(emit), array=None)
    with pytest.raises(ConnectionValidationError):
        await XcpngConnector(ctx).validate_connection()


# ------------------------------------------------------------ deploy_plugin ---
async def test_deploy_creates_sr(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    _seed_hg(ctx, "xcp-pool-hg")
    r = await c.deploy_integration(endpoint="10.0.0.5", sr_name="purefa",
                                   host_group="xcp-pool-hg")
    assert r.success
    assert r.artifacts["sr_type"] == "purefa"
    assert r.artifacts["sr_name"] == "purefa"
    joined = "\n".join(captured_logs)
    assert "xe sr-create type=purefa" in joined
    # a scoped API token is minted for the SR
    assert "xcpng-puresr" in ctx.array.api_tokens


async def test_deploy_installs_smapiv3_plugin(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    _seed_hg(ctx, "hg")
    r = await c.deploy_integration(endpoint="10.0.0.5", host_group="hg")
    assert r.success
    joined = "\n".join(captured_logs)
    # XCP-ng 8.3 uses SMAPIv3: the plugin dir is dropped under the storage-script
    # volume/ tree and link.sh creates the per-method entrypoints.
    assert "org.xen.xapi.storage.purefa" in joined
    assert "link.sh" in joined
    # The real plugin body is written (a marker from plugin.py's Query response).
    assert '"plugin": "purefa"' in joined
    # storage-script + toolstack are restarted so XAPI registers the plugin.
    assert "xapi-storage-script" in joined
    assert "xe-toolstack-restart" in joined
    assert r.artifacts.get("sr_uuid")


async def test_multipath_conf_written_as_standalone_heredoc(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context, protocol="iscsi"))
    await c._write_multipath_conf(host="10.0.0.10")
    # The heredoc that writes pure.conf must be its OWN command, never &&-joined
    # with the next one -- otherwise " && systemctl restart multipathd" lands on
    # the EOF line, the heredoc never closes, and that text is written into the
    # file as trailing junk (the reported bug).
    conf_cmds = [l for l in captured_logs
                 if "/etc/multipath/conf.d/pure.conf <<'EOF'" in l]
    assert len(conf_cmds) == 1
    assert "EOF && " not in conf_cmds[0]
    assert "systemctl restart multipathd" not in conf_cmds[0]
    # restart is issued as a separate command.
    assert any(l.split("$ ", 1)[-1].strip() == "systemctl restart multipathd"
               for l in captured_logs)


def test_smapiv3_datapath_plugin_bundled_and_vbd3():
    # The custom datapath fronts the raw FA device with tapdisk and returns a v5
    # backend: {"implementations": [["XenDisk", {..., "backend_type": "vbd3"}], ...]}.
    # (xenopsd only accepts qdisk/vbd3 and requires a XenDisk; the old singular
    # "implementation"/"Blkback" form caused SR_BACKEND_FAILURE KeyError on power-on.)
    files = XcpngConnector._read_smapiv3_datapath_files()
    assert set(files) == {"plugin.py", "datapath.py", "link.sh"}
    dp = files["datapath.py"]
    assert "xapi.storage.api.v5.datapath" in dp
    assert "Datapath_skeleton" in dp
    assert '"implementations"' in dp             # correct v5 backend key (plural)
    assert '"XenDisk"' in dp and '"backend_type": "vbd3"' in dp
    assert "tap-ctl" in dp                        # tapdisk fronts the raw device
    # the broken singular return must be gone
    assert '"implementation":' not in dp
    # detach must flush the multipath map + SCSI paths so a deleted VDI leaves no
    # stale map (which wedges multipathd and blocks new LUNs from assembling).
    assert "def _flush_device" in dp
    assert '"multipath", "-f"' in dp
    assert "device/delete" in dp
    assert "_flush_device(dev)" in dp
    assert files["plugin.py"].count('"plugin": "purefa"') == 1
    assert "Datapath.attach" in files["link.sh"]


def test_smapiv3_hostplugin_and_poolwide_flush_on_delete():
    # A deleted volume's stale multipath map must be flushed on EVERY pool host
    # (find_multipaths=no auto-maps it everywhere; datapath detach only cleans the
    # resident host). Volume.destroy fans a flush out via the purefa-mpath host
    # plugin (xe host-call-plugin).
    hp = XcpngConnector._read_smapiv3_hostplugin()
    assert "XenAPIPlugin.dispatch" in hp
    assert "def flush" in hp and "multipath -f" in hp
    fa_src = XcpngConnector._read_smapiv3_files()["purefa_fa.py"]
    assert "def flush_multipath_poolwide" in fa_src
    assert "host-call-plugin" in fa_src and "purefa-mpath" in fa_src
    vol_src = XcpngConnector._read_smapiv3_files()["volume.py"]
    assert "flush_multipath_poolwide" in vol_src   # called from Volume.destroy


def test_smapiv3_snapshot_is_real_snapshot_and_deletes():
    # A snapshot must be a REAL FlashArray volume-snapshot (thin PIT), NOT a full
    # volume copy/clone, and deleting the snapshot VDI must destroy it on the array
    # (it used to leak/persist after delete).
    files = XcpngConnector._read_smapiv3_files()
    fa_src = files["purefa_fa.py"]
    assert "volume-snapshots" in fa_src
    assert "def destroy_snapshot" in fa_src and "def get_snapshot" in fa_src
    vol = files["volume.py"]
    # snapshot() creates an FA snapshot (not copy_volume)...
    assert "snapshot_volume(src, suffix)" in vol
    # ...and destroy() routes snapshot keys to destroy_snapshot.
    assert "is_snapshot_key(key)" in vol and "destroy_snapshot(key" in vol
    sr_src = files["sr.py"]
    assert "def is_snapshot_key" in sr_src and "list_snapshots" in sr_src


def test_smapiv3_volume_uri_uses_purefa_datapath_scheme():
    # The volume plugin must emit the 'purefa://' scheme handled by our datapath
    # plugin -- NOT the unsupported 'raw+block://' that broke VM start.
    src = XcpngConnector._read_smapiv3_files()["sr.py"]
    assert '"purefa://"' in src
    assert "raw+block" not in src


def test_smapiv3_destroy_disconnects_before_destroying():
    # A connected FA volume can't be destroyed -- the plugin must disconnect from
    # the host group first, else the VDI's volume lingers after a VM/disk delete.
    fa_src = XcpngConnector._read_smapiv3_files()["purefa_fa.py"]
    assert "def disconnect_volume" in fa_src
    assert "def destroy_volume" in fa_src
    assert "self.disconnect_volume(name, hostgroup)" in fa_src
    vol_src = XcpngConnector._read_smapiv3_files()["volume.py"]
    assert "hostgroup=conf.get(\"hostgroup\")" in vol_src


def test_deploy_action_has_eradicate_field_and_passes_device_config():
    fields = {f.name: f for a in XcpngConnector.action_schemas()
              if a.id == "deploy" for f in a.fields}
    assert "eradicate" in fields and fields["eradicate"].type.value == "bool"


async def test_deploy_passes_eradicate_device_config(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context))
    _seed_hg(c.ctx, "hg")
    await c.deploy_integration(endpoint="10.0.0.5", host_group="hg", eradicate=True)
    joined = "\n".join(captured_logs)
    assert "device-config:eradicate=true" in joined


def test_deploy_action_has_clobber_field():
    fields = {f.name: f for a in XcpngConnector.action_schemas()
              if a.id == "deploy" for f in a.fields}
    assert "clobber" in fields and fields["clobber"].type.value == "bool"


async def test_forget_sr_unplugs_then_forgets(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context))
    await c._forget_sr("sr-uuid-123")
    joined = "\n".join(captured_logs)
    assert "xe pbd-list sr-uuid=sr-uuid-123" in joined
    assert "xe pbd-unplug" in joined
    assert "xe sr-forget uuid=sr-uuid-123" in joined


async def test_deploy_fc_passes_protocol(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="fc")
    c = XcpngConnector(ctx)
    _seed_hg(ctx, "xcp-fc-hg")
    r = await c.deploy_integration(endpoint="10.0.0.5", host_group="xcp-fc-hg")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    joined = "\n".join(captured_logs)
    # sr-create must carry device-config:protocol=fc
    assert "xe sr-create type=purefa" in joined
    assert "device-config:protocol=fc" in joined


async def test_deploy_dry_run(make_context, captured_logs):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.deploy_integration()
    assert r.success
    # no token minted, no sr-create issued in dry-run
    assert "xcpng-puresr" not in ctx.array.api_tokens
    assert not any("xe sr-create" in line for line in captured_logs)


# ---------------------------------------------------------- host_register ---
async def test_register_hosts_iscsi(make_context):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="xcp-pool-hg",
                               iqns="iqn.1998-01.com.vmware:host1")
    assert r.success
    assert "xcp-pool-hg" in ctx.array.host_groups
    host_name = r.artifacts["host"]
    assert ctx.array.hosts[host_name]["iqns"] == ["iqn.1998-01.com.vmware:host1"]


async def test_preflight_ok_when_array_reachable(make_context):
    r = await XcpngConnector(_ctx(make_context, protocol="iscsi")).preflight("wizard", {})
    assert r.success


async def test_preflight_ok_when_no_array(make_context):
    r = await XcpngConnector(_ctx(make_context, with_array=False)).preflight("wizard", {})
    assert r.success


async def test_preflight_fails_when_array_unreachable(make_context):
    ctx = _ctx(make_context, protocol="iscsi")

    async def _boom():
        raise RuntimeError("connection refused")

    ctx.array.info = _boom   # required object (the array) is unreachable
    r = await XcpngConnector(ctx).preflight("wizard", {})
    assert not r.success
    assert "not reachable" in r.message.lower()


async def test_register_hosts_reuses_existing_array_host(make_context, captured_logs):
    # The XCP hosts were already registered on the array by hand (a host owns the
    # IQN). Registration must REUSE that host, not create a duplicate (which would
    # fail: an initiator can belong to only one FA host -> "Host does not exist").
    ctx = _ctx(make_context, protocol="iscsi")
    ctx.array.hosts["preexisting-xcp"] = {
        "iqns": ["iqn.2026-01.com.example:75b1c852"], "wwns": [], "nqns": []}
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="XCP-Lab",
                               iqns="iqn.2026-01.com.example:75b1c852")
    assert r.success
    # Group contains the pre-existing host ...
    assert "preexisting-xcp" in ctx.array.host_groups["XCP-Lab"]["hosts"]
    # ... and no second host was created holding the same IQN.
    owners = [n for n, h in ctx.array.hosts.items()
              if "iqn.2026-01.com.example:75b1c852" in h["iqns"]]
    assert owners == ["preexisting-xcp"]
    assert any("Reusing existing FlashArray host" in line for line in captured_logs)


async def test_register_hosts_adopts_existing_group(make_context, captured_logs):
    # The pool host is already in a host group on the array -> ADOPT that group as
    # the integration's host group, rather than forcing the requested one.
    ctx = _ctx(make_context, protocol="iscsi")
    iqn = "iqn.2026-01.com.example:75b1c852"
    ctx.array.hosts["preexisting-xcp"] = {"iqns": [iqn], "wwns": [], "nqns": []}
    ctx.array.host_groups["OtherGroup"] = {"hosts": ["preexisting-xcp"]}
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="XCP-Lab", iqns=iqn)
    assert r.success
    # Effective host group is the pre-existing one, flagged as adopted.
    assert r.artifacts["host_group"] == "OtherGroup"
    assert r.artifacts["adopted_host_group"] is True
    assert any("adopting it" in line.lower() for line in captured_logs)
    # The requested group was NOT created (we used the existing one).
    assert "XCP-Lab" not in ctx.array.host_groups


async def test_register_hosts_nvme(make_context):
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="hg", nqns="nqn.2014-08.org.nvmexpress:uuid:x")
    assert r.success
    host_name = r.artifacts["host"]
    assert ctx.array.hosts[host_name]["nqns"] == ["nqn.2014-08.org.nvmexpress:uuid:x"]


async def test_register_hosts_fc_by_wwn(make_context):
    ctx = _ctx(make_context, protocol="fc")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="xcp-fc-hg",
                               wwns="21000024ff1a2b3c,21000024ff1a2b3d")
    assert r.success
    host_name = r.artifacts["host"]
    host = ctx.array.hosts[host_name]
    # registered BY WWN only -- no IQN / NQN for the FC path
    assert host["wwns"] == ["21000024ff1a2b3c", "21000024ff1a2b3d"]
    assert host["iqns"] == []
    assert host["nqns"] == []
    assert "xcp-fc-hg" in ctx.array.host_groups


async def test_register_hosts_fc_auto_discovers_wwns(make_context, captured_logs):
    # No WWNs supplied -> the connector fans out across the (synthetic 2-host)
    # pool, discovering each host's WWNs via runner.discover_initiators and
    # registering one FA host per pool node, all in the one host group.
    ctx = _ctx(make_context, protocol="fc")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="xcp-fc-hg")
    assert r.success
    joined = "\n".join(captured_logs)
    assert "Discovering initiators on pool host" in joined
    # one FA host per pool node (mock pool has 2), all registered BY WWN only.
    assert r.artifacts["node_count"] == 2
    assert len(r.artifacts["hosts"]) == 2
    for hn in r.artifacts["hosts"]:
        host = ctx.array.hosts[hn]
        assert host["wwns"] == ["21000024ff000001", "21000024ff000002"]
        assert host["iqns"] == []
        assert host["nqns"] == []
    # all per-node FA hosts live in the single shared host group
    assert set(r.artifacts["hosts"]) == set(
        ctx.array.host_groups["xcp-fc-hg"]["hosts"])
    assert r.artifacts["protocol"] == "fc"


async def test_register_hosts_iscsi_auto_discovers_iqn(make_context):
    # No IQN supplied -> fan out, auto-discover each pool host's IQN.
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="xcp-pool-hg")
    assert r.success
    assert r.artifacts["node_count"] == 2
    for hn in r.artifacts["hosts"]:
        host = ctx.array.hosts[hn]
        # synthetic per-node IQN from runner.discover_initiators reached the array
        assert host["iqns"] and host["iqns"][0].startswith("iqn.1993-08.org.debian")
        assert host["nqns"] == []
        assert host["wwns"] == []


async def test_register_hosts_nvme_auto_discovers_nqn(make_context):
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="hg")
    assert r.success
    assert r.artifacts["node_count"] == 2
    for hn in r.artifacts["hosts"]:
        host = ctx.array.hosts[hn]
        assert host["nqns"] and host["nqns"][0].startswith(
            "nqn.2014-08.org.nvmexpress:uuid")
        assert host["iqns"] == []
        assert host["wwns"] == []


async def test_register_hosts_explicit_overrides_discovery(make_context, captured_logs):
    # Explicit iqns provided -> no auto-discovery, explicit value wins.
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="xcp-pool-hg", iqns="iqn.explicit:1")
    assert r.success
    host = ctx.array.hosts[r.artifacts["host"]]
    assert host["iqns"] == ["iqn.explicit:1"]
    assert "Auto-discovering pool master initiators" not in "\n".join(captured_logs)


async def test_register_hosts_no_array(make_context):
    c = XcpngConnector(_ctx(make_context, with_array=False))
    r = await c.register_hosts(host_group="hg")
    assert not r.success


async def test_register_hosts_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="hg", iqns="iqn.x")
    assert r.success
    assert "hg" not in ctx.array.host_groups  # no mutation in dry-run


async def test_register_hosts_dry_run_auto_discover_no_op(make_context):
    # No explicit initiators + dry-run: discovery may run (mock-safe) but the
    # array must not be mutated.
    ctx = _ctx(make_context, protocol="fc")
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="xcp-fc-hg")
    assert r.success
    assert "xcp-fc-hg" not in ctx.array.host_groups
    assert not ctx.array.hosts


# ----------------------------------------------------------- connectivity ---
async def test_setup_connectivity_iscsi(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context, protocol="iscsi"))
    r = await c.setup_connectivity(portals="10.0.0.10,10.0.0.11",
                                   target_iqn="iqn.target")
    assert r.success
    assert r.artifacts["protocol"] == "iscsi"
    joined = "\n".join(captured_logs)
    assert "iscsiadm -m discovery" in joined
    assert "multipathing=true" in joined
    assert "alua" in joined  # Everpure ALUA multipath stanza applied


async def test_setup_connectivity_nvme(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context, protocol="nvme-tcp"))
    r = await c.setup_connectivity(portals="10.0.0.20", subsystem_nqn="nqn.sub")
    assert r.success
    assert r.artifacts["protocol"] == "nvme-tcp"
    assert any("nvme connect-all" in line for line in captured_logs)


async def test_setup_connectivity_fc_rescans_no_iscsiadm(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context, protocol="fc"))
    r = await c.setup_connectivity(portals="")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    joined = "\n".join(captured_logs)
    # FC performs an FC/SCSI rescan + multipath, never an iSCSI/NVMe login.
    assert "iscsiadm -m discovery" not in joined
    assert "iscsiadm -m node" not in joined
    assert "nvme connect-all" not in joined
    assert ("issue_lip" in joined) or ("rescan-scsi-bus.sh" in joined)
    assert "multipath -r" in joined
    # multipath/ALUA still configured (common path runs for all protocols)
    assert "multipathing=true" in joined
    assert "alua" in joined


async def test_setup_connectivity_fc_dry_run(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="fc")
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="")
    assert r.success
    assert not any("issue_lip" in line for line in captured_logs)


async def test_setup_connectivity_dry_run(make_context, captured_logs):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.10")
    assert r.success
    assert not any("iscsiadm -m discovery" in line for line in captured_logs)


# --------------------------------------------------------------- provision ---
async def test_provision_creates_volume_and_connects(make_context):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    r = await c.provision(name="vdi-1", size="200G", host_group="xcp-pool-hg")
    assert r.success
    # per-VDI volume created on the array and mapped to the host group
    assert "vdi-1" in ctx.array.volumes
    assert ("connect_volume", {"host": "xcp-pool-hg", "volume": "vdi-1"}) in ctx.array.calls


async def test_provision_uses_target_host_group_default(make_context):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    r = await c.provision(name="vdi-2", size="50G")
    assert r.success
    assert "vdi-2" in ctx.array.volumes
    # default host_group from target connection
    assert ("connect_volume", {"host": "xcp-pool-hg", "volume": "vdi-2"}) in ctx.array.calls


async def test_provision_fc_connects_to_host_group(make_context):
    ctx = _ctx(make_context, protocol="fc")
    c = XcpngConnector(ctx)
    r = await c.provision(name="vdi-fc", size="100G", host_group="xcp-fc-hg")
    assert r.success
    # FA volume created and connected to the FC host group; VDI maps over FC.
    assert "vdi-fc" in ctx.array.volumes
    assert ("connect_volume", {"host": "xcp-fc-hg", "volume": "vdi-fc"}) in ctx.array.calls


async def test_provision_no_array(make_context):
    c = XcpngConnector(_ctx(make_context, with_array=False))
    r = await c.provision(name="v", size="1T")
    assert not r.success


async def test_provision_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.provision(name="vdry", size="1T")
    assert r.success
    assert "vdry" not in ctx.array.volumes


# ----------------------------------------------------------- snapshot/clone ---
async def test_snapshot_uses_array(make_context):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    await c.provision(name="vdi-1", size="1T")
    r = await c.snapshot(volume="vdi-1")
    assert r.success
    assert any(s["volume"] == "vdi-1" for s in ctx.array.snapshots)


async def test_snapshot_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.snapshot(volume="vdi-1")
    assert r.success
    assert not ctx.array.snapshots


async def test_clone_uses_array(make_context):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    await c.provision(name="vdi-1", size="1T")
    r = await c.clone(source="vdi-1", dest="vdi-1-clone")
    assert r.success
    assert "vdi-1-clone" in ctx.array.volumes
    assert ("clone_volume", {"source": "vdi-1", "dest": "vdi-1-clone"}) in ctx.array.calls


async def test_clone_no_array(make_context):
    c = XcpngConnector(_ctx(make_context, with_array=False))
    r = await c.clone(source="a", dest="b")
    assert not r.success


# ------------------------------------------------------------------ resize ---
async def test_resize(make_context):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    await c.provision(name="vdi-1", size="1T")
    r = await c.resize(volume="vdi-1", size="3T")
    assert r.success
    assert ctx.array.volumes["vdi-1"]["size"] == "3T"


async def test_resize_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    await c.provision(name="vdi-1", size="1T")  # dry-run: not created
    r = await c.resize(volume="vdi-1", size="3T")
    assert r.success
    assert "vdi-1" not in ctx.array.volumes


# ------------------------------------------------------------------ health ---
async def test_health_check(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context))
    r = await c.health_check()
    assert r.success
    # SR list is pool-wide; multipath is gathered per pool host under "nodes".
    assert "sr_list" in r.data and "nodes" in r.data and r.data["nodes"]
    for entry in r.data["nodes"].values():
        assert "paths" in entry and "host" in entry
    assert r.data["array"]["name"] == "mock-array"
    joined = "\n".join(captured_logs)
    assert "xe sr-list type=purefa" in joined
    assert "multipath -ll" in joined


# ---------------------------------------------------------------- teardown ---
async def test_teardown(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context))
    r = await c.teardown()
    assert r.success
    assert r.data["status"] == "not_deployed"
    joined = "\n".join(captured_logs)
    # SRs are enumerated for forgetting by BOTH type and name-label (mock returns
    # none, so the actual sr-forget loop is exercised in the unit test below).
    assert "sr-list type=purefa" in joined
    assert "sr-list name-label=" in joined
    assert "forgotten_srs" in r.artifacts
    # The SMAPIv3 plugin dir is removed on every pool host, and the sm-plugins
    # allowlist edit is reverted (sed on /etc/xapi.conf).
    nodes = r.artifacts["nodes"]
    assert len(nodes) >= 1
    assert joined.count(
        "rm -rf /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.purefa"
    ) == len(nodes)
    assert "/etc/xapi.conf" in joined


async def test_forget_sr_unplugs_then_forgets_each(make_context, captured_logs):
    # _forget_sr (used by teardown + clobber for each SR) unplugs PBDs then forgets.
    c = XcpngConnector(_ctx(make_context))
    await c._forget_sr("sr-aaa")
    joined = "\n".join(captured_logs)
    assert "pbd-list sr-uuid=sr-aaa" in joined and "pbd-unplug" in joined
    assert "xe sr-forget uuid=sr-aaa" in joined


async def test_teardown_dry_run(make_context, captured_logs):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.teardown()
    assert r.success
    assert r.data["status"] == "planned"
    assert not any("xe sr-forget" in line for line in captured_logs)


# ----------------------------------------------------- interface binding ---
def test_setup_connectivity_has_binding_fields():
    sc = next(a for a in XcpngConnector.action_schemas()
              if a.id == "setup_connectivity")
    by_name = {f.name: f for f in sc.fields}
    # MULTISELECT binding fields wired to discover_options via options_source
    assert by_name["iscsi_nics"].type.value == "multiselect"
    assert by_name["iscsi_nics"].options_source == "nics"
    assert by_name["nvme_sources"].type.value == "multiselect"
    assert by_name["nvme_sources"].options_source == "nvme_sources"
    assert by_name["fc_hbas"].type.value == "multiselect"
    assert by_name["fc_hbas"].options_source == "fc_hbas"
    assert by_name["nvme_options"].type.value == "string"
    # all binding fields are optional
    for n in ("iscsi_nics", "nvme_sources", "nvme_options", "fc_hbas"):
        assert by_name[n].required is False


async def test_discover_options_nics(make_context):
    # iSCSI (default) + array present: NICs are filtered to the array's iSCSI
    # storage subnet. Mock iSCSI portals are 10.10.10.x, so only eth0
    # (10.10.10.5/24) matches; eth1 (10.20.20.x) / ens192 (192.168.1.x) drop out.
    c = XcpngConnector(_ctx(make_context, protocol="iscsi"))
    opts = await c.discover_options("nics")
    vals = {o["value"] for o in opts}
    assert vals == {"eth0"}


async def test_discover_options_nics_nvme_filters_to_nvme_subnet(make_context):
    # nvme-tcp + array present: filter to the NVMe-TCP storage subnet
    # (10.20.20.x), so only eth1 (10.20.20.5/24) matches.
    c = XcpngConnector(_ctx(make_context, protocol="nvme-tcp"))
    opts = await c.discover_options("nics")
    vals = {o["value"] for o in opts}
    assert vals == {"eth1"}


async def test_discover_options_nics_unfiltered_without_array(make_context):
    # No associated array -> no portals to filter against, so every discovered
    # NIC is offered unfiltered.
    c = XcpngConnector(_ctx(make_context, protocol="iscsi", with_array=False))
    opts = await c.discover_options("nics")
    vals = {o["value"] for o in opts}
    assert {"eth0", "eth1", "ens192"} <= vals


async def test_discover_options_nvme_sources(make_context):
    c = XcpngConnector(_ctx(make_context, protocol="nvme-tcp"))
    opts = await c.discover_options("nvme_sources")
    assert any(o["value"] == "192.168.10.11" for o in opts)
    assert all("interface" in o and "address" in o for o in opts)


async def test_discover_options_fc_hbas(make_context):
    c = XcpngConnector(_ctx(make_context, protocol="fc"))
    opts = await c.discover_options("fc_hbas")
    assert any(o["value"] == "21000024ff000001" for o in opts)


async def test_discover_options_unknown_kind(make_context):
    c = XcpngConnector(_ctx(make_context))
    assert await c.discover_options("bogus") == []


async def test_setup_connectivity_iscsi_binds_nics(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.10",
                                   iscsi_nics=["eth0", "eth1"])
    assert r.success
    assert r.artifacts["binding"] == {"iscsi_nics": ["eth0", "eth1"]}
    joined = "\n".join(captured_logs)
    # an iscsiadm iface is created per selected NIC and bound to it
    assert "iscsiadm -m iface -I pure-eth0 -o new" in joined
    assert "iface.net_ifacename -v eth1" in joined
    # discovery + login are scoped to the bound ifaces
    assert "-I pure-eth0 -I pure-eth1" in joined
    # binding persisted into the SR device-config
    assert "device-config:iscsi_nics='eth0,eth1'" in joined


async def test_setup_connectivity_nvme_binds_sources(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.20", subsystem_nqn="nqn.sub",
                                   nvme_sources=["192.168.10.11"],
                                   nvme_options="ctrl-loss-tmo=600")
    assert r.success
    assert r.artifacts["binding"]["nvme_sources"] == ["192.168.10.11"]
    assert r.artifacts["binding"]["nvme_options"] == "ctrl-loss-tmo=600"
    joined = "\n".join(captured_logs)
    # nvme connect carries the host source (-w) and persisted options
    assert "-w 192.168.10.11" in joined
    assert "ctrl-loss-tmo=600" in joined
    assert "device-config:nvme_sources='192.168.10.11'" in joined
    assert "device-config:nvme_options='ctrl-loss-tmo=600'" in joined


async def test_setup_connectivity_fc_pins_hbas(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="fc")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="",
                                   fc_hbas=["21000024ff000001",
                                            "21000024ff000002"])
    assert r.success
    assert r.artifacts["binding"]["fc_hbas"] == ["21000024ff000001",
                                                 "21000024ff000002"]
    joined = "\n".join(captured_logs)
    # FC has no iSCSI/NVMe login even with a binding selected
    assert "iscsiadm -m node" not in joined
    assert "nvme connect-all" not in joined
    # HBAs persisted into device-config
    assert "device-config:fc_hbas='21000024ff000001,21000024ff000002'" in joined


async def test_setup_connectivity_binding_only_active_protocol(make_context,
                                                               captured_logs):
    # iSCSI active: nvme/fc selections must be ignored entirely.
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.10",
                                   iscsi_nics=["eth0"],
                                   nvme_sources=["192.168.10.11"],
                                   fc_hbas=["21000024ff000001"])
    assert r.success
    assert r.artifacts["binding"] == {"iscsi_nics": ["eth0"]}
    joined = "\n".join(captured_logs)
    assert "device-config:nvme_sources" not in joined
    assert "device-config:fc_hbas" not in joined


async def test_setup_connectivity_no_binding_no_sr_param_set(make_context,
                                                             captured_logs):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.10")
    assert r.success
    assert r.artifacts["binding"] == {}
    joined = "\n".join(captured_logs)
    # no binding -> no iface creation, no device-config persistence
    assert "iscsiadm -m iface" not in joined
    assert "sr-param-set" not in joined


async def test_setup_connectivity_binding_dry_run_no_op(make_context,
                                                        captured_logs):
    ctx = _ctx(make_context, protocol="iscsi")
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.10", iscsi_nics=["eth0"])
    assert r.success
    # binding is planned but no commands run in dry-run
    assert r.artifacts["binding"] == {"iscsi_nics": ["eth0"]}
    joined = "\n".join(captured_logs)
    assert "iscsiadm -m iface" not in joined
    assert "sr-param-set" not in joined


# ------------------------------------------------------------------ dispatch ---
async def test_dispatch_provision(make_context):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    r = await c.dispatch("provision", {"name": "dvol", "size": "1T"})
    assert r.success
    assert "dvol" in ctx.array.volumes


async def test_dispatch_snapshot(make_context):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    r = await c.dispatch("snapshot", {"volume": "vdi-1"})
    assert r.success
    assert any(s["volume"] == "vdi-1" for s in ctx.array.snapshots)


async def test_dispatch_clone(make_context):
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    r = await c.dispatch("clone", {"source": "s", "dest": "d"})
    assert r.success
    assert "d" in ctx.array.volumes


async def test_dispatch_unknown(make_context):
    c = XcpngConnector(_ctx(make_context))
    r = await c.dispatch("nope", {})
    assert not r.success


# ----------------------------------- endpoint/token come from the array -----
def test_deploy_schema_omits_endpoint_and_token_fields():
    # Endpoint + token are taken from the associated array, never operator-
    # entered: the deploy action must expose no endpoint/token form fields.
    deploy = next(a for a in XcpngConnector.action_schemas() if a.id == "deploy")
    names = {f.name for f in deploy.fields}
    for forbidden in ("endpoint", "pure_endpoint", "token", "api_token",
                      "pure_api_token"):
        assert forbidden not in names
    # the useful fields remain
    assert {"sr_name", "host_group"} <= names


async def test_deploy_uses_array_endpoint_and_token(make_context, captured_logs):
    # With no explicit endpoint kwarg, deploy uses ctx.array.endpoint and the
    # array's resolved token in the sr-create device-config.
    ctx = _ctx(make_context)
    ctx.array_token = "orig-array-token"
    c = XcpngConnector(ctx)
    _seed_hg(ctx, "xcp-pool-hg")
    r = await c.deploy_integration(host_group="xcp-pool-hg")
    assert r.success
    assert r.artifacts["endpoint"] == ctx.array.endpoint
    joined = "\n".join(captured_logs)
    assert f"device-config:endpoint='{ctx.array.endpoint}'" in joined
    assert "device-config:token='orig-array-token'" in joined
    # the original token was reused -- no new scoped token minted
    assert "xcpng-puresr" not in ctx.array.api_tokens


# ------------------------------------------ discover_options("initiators") ---
async def test_discover_options_initiators_iscsi(make_context):
    c = XcpngConnector(_ctx(make_context, protocol="iscsi"))
    opts = await c.discover_options("initiators")
    by_field = {o["field"]: o for o in opts}
    # IQN tagged with the register_hosts `iqns` form field for UI pre-fill
    assert by_field["iqns"]["value"] == (
        "iqn.1993-08.org.debian:01:xcp-master-test-local")
    assert "label" in by_field["iqns"]
    # NQN + WWNs are also surfaced, tagged with their register_hosts fields
    assert by_field["nqns"]["field"] == "nqns"
    assert by_field["wwns"]["value"] == "21000024ff000001,21000024ff000002"


# ---------------------------- connectivity discovers portals/target ----------
async def test_setup_connectivity_iscsi_discovers_portals_and_target(
        make_context, captured_logs):
    # No portals/target supplied -> discovered from the array.
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="")
    assert r.success
    assert r.artifacts["portals"] == ["10.10.10.10", "10.10.11.10"]
    joined = "\n".join(captured_logs)
    assert "Discovered array iscsi portals" in joined
    assert "Discovered array iSCSI target IQN" in joined
    # the discovered target IQN is used in the login command
    assert "flasharray.mock0001" in joined


async def test_setup_connectivity_nvme_discovers_portals_and_nqn(
        make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="")
    assert r.success
    assert r.artifacts["portals"] == ["10.20.20.20", "10.20.21.20"]
    joined = "\n".join(captured_logs)
    assert "Discovered array nvme-tcp portals" in joined
    assert "Discovered array NVMe subsystem NQN" in joined
    assert "nvme-subsystem.mock0001" in joined


async def test_setup_connectivity_explicit_portals_skip_discovery(
        make_context, captured_logs):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.99", target_iqn="iqn.explicit")
    assert r.success
    assert r.artifacts["portals"] == ["10.0.0.99"]
    joined = "\n".join(captured_logs)
    assert "Discovered array iscsi portals" not in joined
    assert "Discovered array iSCSI target IQN" not in joined


async def test_setup_connectivity_fail_fast_no_portals(make_context):
    # iSCSI but the array exposes no portals (e.g. FC-only) -> clear failure,
    # no iscsiadm/login attempted.
    ctx = _ctx(make_context, protocol="iscsi", with_array=False)
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="")
    assert not r.success
    assert "no" in r.message.lower() and "portal" in r.message.lower()
    assert "fc" in r.message.lower()


async def test_setup_connectivity_fail_fast_dry_run_ok(make_context):
    # Same scenario but dry-run: no fail-fast, planning succeeds.
    ctx = _ctx(make_context, protocol="iscsi", with_array=False)
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="")
    assert r.success


# ----------------------------------------- host_group carry-over -------------
async def test_register_hosts_host_group_carryover_from_target(make_context):
    # Blank action host_group falls back to the target connection value.
    ctx = _ctx(make_context, protocol="iscsi")  # target host_group=xcp-pool-hg
    c = XcpngConnector(ctx)
    r = await c.register_hosts(iqns="iqn.x")
    assert r.success
    assert r.artifacts["host_group"] == "xcp-pool-hg"
    assert "xcp-pool-hg" in ctx.array.host_groups


async def test_register_hosts_fails_when_host_group_empty(make_context):
    # No action host_group and none on the target -> clear failure.
    ctx = _ctx(make_context, protocol="iscsi", host_group="")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(iqns="iqn.x")
    assert not r.success
    assert "host_group" in r.message


def test_register_hosts_sanitizes_derived_host_name(make_context):
    # A host group label with dots must yield an FA host name with no dots.
    ctx = _ctx(make_context, protocol="iscsi", host_group="pool.example.com")
    c = XcpngConnector(ctx)
    name = c._fa_name("pool.example.com-pool")
    assert "." not in name
    assert name == "pool-example-com-pool"


# ============================================================================ #
# SMAPIv3 purefa_fa.py: WWID / REST / vgroup behavior (mirrors Proxmox plugin)
# ============================================================================ #

# ---------------------------------------- 1. SCSI WWID (NAA-6 + Everpure OUI) ----
def test_fa_scsi_wwid_prepends_oui():
    m = _load_fa_module()
    # REST `serial` is the 24-hex tail WITHOUT the OUI -> prepend 3 + 624a9370.
    assert m.scsi_wwid("0123456789ABCDEF0BB82813") == (
        "3624a93700123456789abcdef0bb82813")
    # lower-cased and 0x-tolerant
    assert m.scsi_wwid("0x0123456789ABCDEF0BB82813") == (
        "3624a93700123456789abcdef0bb82813")


def test_fa_scsi_wwid_tolerates_existing_oui():
    m = _load_fa_module()
    # A serial that already includes the Everpure OUI ("624a937...") gets only the
    # NAA-6 type digit prepended, NOT a second OUI (mirrors Proxmox _scsi_wwid).
    already = "624a93700123456789abcdef0bb82813"
    assert m.scsi_wwid(already) == "3" + already
    assert m.scsi_wwid(already.upper()) == "3" + already


def test_fa_device_path_iscsi_fc_use_scsi_wwid():
    m = _load_fa_module()
    serial = "0123456789ABCDEF0BB82813"
    expected = "/dev/mapper/3624a93700123456789abcdef0bb82813"
    assert m.device_path(serial, "iscsi") == expected
    assert m.device_path(serial, "fc") == expected


def test_fa_device_path_nvme_uses_eui_path():
    m = _load_fa_module()
    serial = "0123456789ABCDEF0BB82813"
    # NVMe keeps the namespace EUI/NGUID path, NOT the SCSI WWID. (The host-side
    # device path is keyed off the serial -- UNCHANGED by vgroup membership.)
    assert m.device_path(serial, "nvme-tcp") == (
        "/dev/mapper/eui.0123456789abcdef0bb82813")


# --------------------------------- 2. FA REST session: version negotiation ---
def test_fa_rest_normalizes_bare_ip_to_https():
    m = _load_fa_module()
    fa = m.FlashArray("10.0.0.10", "tok")
    assert fa.endpoint == "https://10.0.0.10"
    # an explicit scheme is preserved
    assert m.FlashArray("http://fa.local/", "t").endpoint == "http://fa.local"
    # no literal API version baked in until negotiated
    assert fa._apiver is None


def test_fa_rest_negotiates_api_version(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    calls = {}

    def fake_http(method, url, body=None, headers=None):
        calls["url"] = url
        return {"version": ["2.0", "2.11", "2.38"]}

    monkeypatch.setattr(fa, "_http", fake_http)
    ver = fa._api_version()
    # highest supported version is chosen; probe hits /api/api_version (no /2.x/)
    assert ver == "2.38"
    assert calls["url"].endswith("/api/api_version")


def test_fa_rest_api_version_fallback(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")

    def boom(*a, **k):
        raise RuntimeError("unreachable")

    monkeypatch.setattr(fa, "_http", boom)
    # probe failure -> safe 2.0 fallback, never crashes login()
    assert fa._api_version() == "2.0"


def test_fa_request_builds_versioned_path(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    fa._apiver = "2.20"
    fa._auth = "x-auth"   # skip login
    seen = {}

    def fake_http(method, url, body=None, headers=None):
        seen["url"] = url
        return {}

    monkeypatch.setattr(fa, "_http", fake_http)
    fa.request("GET", "volumes")
    assert seen["url"] == "https://fa.local/api/2.20/volumes"


# ------------------------------------- 3. list skips destroyed volumes -------
def test_fa_list_volumes_skips_destroyed(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")

    def fake_request(method, path, body=None):
        return {"items": [
            {"name": "vdi-live-1", "destroyed": False},
            {"name": "vdi-gone", "destroyed": True},
            {"name": "vdi-live-2"},  # absent => live
        ]}

    monkeypatch.setattr(fa, "request", fake_request)
    names = [v["name"] for v in fa.list_volumes("phif-abcd1234-")]
    assert names == ["vdi-live-1", "vdi-live-2"]
    assert "vdi-gone" not in names


def test_fa_source_documents_skip_destroyed():
    src = _read(os.path.join(xcpng_mod._SMAPIV3_SRC_DIR, "purefa_fa.py"))
    assert "destroyed" in src
    assert "def list_volumes" in src


# ============================================================================ #
# Task 2: per-VM FlashArray volume groups (vgroups) in the SMAPIv3 plugin
# ============================================================================ #
class _FakeFA(object):
    """Records the REST-shaped calls a vgroup-aware flow makes on FlashArray."""

    def __init__(self):
        self.calls = []
        self.members = {}          # vg -> set of live member names
        self.volumes = {}          # name -> dict
        self.pgroups = {}          # pg -> set of member volume names
        self.pgroup_snapshots = {}  # "<pg>.<sfx>.<vg>/<vol>" -> dict

    # vgroup ops
    def create_vgroup(self, vg):
        self.calls.append(("create_vgroup", vg))
        self.members.setdefault(vg, set())

    def vgroup_member_count(self, vg):
        return len(self.members.get(vg, set()))

    def destroy_vgroup(self, vg):
        self.calls.append(("destroy_vgroup", vg))
        if not self.members.get(vg):
            self.members.pop(vg, None)

    def group_snapshot(self, vg, suffix):
        self.calls.append(("group_snapshot", vg, suffix))
        return {"items": []}

    # protection-group ops (crash-consistent snapshots)
    def create_pgroup(self, pg):
        self.calls.append(("create_pgroup", pg))
        self.pgroups.setdefault(pg, set())

    def add_pgroup_volumes(self, pg, member_names):
        self.calls.append(("add_pgroup_volumes", pg, list(member_names)))
        self.pgroups.setdefault(pg, set()).update(member_names)

    def snapshot_pgroup(self, pg, suffix):
        self.calls.append(("snapshot_pgroup", pg, suffix))
        # Record one member snapshot per pgroup member: "<pg>.<sfx>.<vg>/<vol>".
        for m in self.pgroups.get(pg, set()):
            self.pgroup_snapshots["%s.%s.%s" % (pg, suffix, m)] = {
                "name": "%s.%s.%s" % (pg, suffix, m), "provisioned": 0}
        return {}

    def get_pgroup_snapshot_member(self, name):
        return self.pgroup_snapshots.get(name)

    def destroy_pgroup_snapshot(self, name, eradicate=False):
        self.calls.append(("destroy_pgroup_snapshot", name, eradicate))
        # Drop every member snapshot under this pgroup snapshot "<pg>.<sfx>".
        for k in [k for k in self.pgroup_snapshots if k.startswith(name + ".")]:
            self.pgroup_snapshots.pop(k, None)

    def destroy_pgroup(self, pg):
        self.calls.append(("destroy_pgroup", pg))
        self.pgroups.pop(pg, None)

    def list_vgroup_volumes(self, vg):
        return [self.volumes[n] for n in sorted(self.members.get(vg, set()))
                if n in self.volumes]

    # volume ops
    def create_volume(self, name, size_bytes):
        self.calls.append(("create_volume", name, int(size_bytes)))
        self.volumes[name] = {"name": name, "provisioned": int(size_bytes),
                              "serial": "0123456789abcdef0bb82813", "tags": {}}
        if "/" in name:
            self.members.setdefault(name.split("/", 1)[0], set()).add(name)

    def connect_volume(self, name, hostgroup):
        self.calls.append(("connect_volume", name, hostgroup))

    def disconnect_volume(self, name, hostgroup):
        self.calls.append(("disconnect_volume", name, hostgroup))

    def destroy_volume(self, name, hostgroup=None, eradicate=False):
        self.calls.append(("destroy_volume", name, hostgroup, eradicate))
        if "/" in name:
            self.members.get(name.split("/", 1)[0], set()).discard(name)
        self.volumes.pop(name, None)

    def volume(self, name):
        return self.volumes.get(name)

    def set_tag(self, name, key, value):
        self.calls.append(("set_tag", name, key, value))

    def copy_volume(self, source, dest):
        self.calls.append(("copy_volume", source, dest))
        self.volumes[dest] = {"name": dest, "provisioned": 0,
                              "serial": "deadbeef", "tags": {}}
        if "/" in dest:
            self.members.setdefault(dest.split("/", 1)[0], set()).add(dest)

    # Per-volume snapshot ops (XCP's snapshot model -- not pgroups).
    def snapshot_volume(self, name, suffix):
        self.calls.append(("snapshot_volume", name, suffix))
        snap = "%s.%s" % (name, suffix)
        return {"items": [{"name": snap, "provisioned": 0, "serial": "5"}]}

    def get_snapshot(self, name):
        return {"name": name, "provisioned": 0, "serial": "5"}

    def destroy_snapshot(self, name, eradicate=False):
        self.calls.append(("destroy_snapshot", name, eradicate))


def _load_sr_only():
    """Import sr.py with a stub xapi.storage so it loads off-host (no shims)."""
    return _import_smapiv3("sr", "sr.py")


def _import_smapiv3(modname, filename):
    """Import an SMAPIv3 sibling module (sr/volume) under a stubbed xapi.storage.

    The plugin imports ``xapi.storage.api.v5.volume`` + ``xapi.storage.log`` and
    its sibling ``purefa_fa``; none are available off-host, so stub them so the
    module body (the pure logic we test) imports cleanly."""
    import types

    # Stub the xapi.storage package tree, wiring each submodule as an attribute
    # of its parent so dotted access (xapi.storage.api.v5.volume.X) resolves.
    for name in ("xapi", "xapi.storage", "xapi.storage.api",
                 "xapi.storage.api.v5", "xapi.storage.api.v5.volume"):
        if name not in sys.modules:
            sys.modules[name] = types.ModuleType(name)
        if "." in name:
            parent, child = name.rsplit(".", 1)
            setattr(sys.modules[parent], child, sys.modules[name])
    v5 = sys.modules["xapi.storage.api.v5.volume"]
    for sk in ("Volume_skeleton", "SR_skeleton"):
        if not hasattr(v5, sk):
            setattr(v5, sk, type(sk, (object,), {}))
    if not hasattr(v5, "Volume_does_not_exist"):
        v5.Volume_does_not_exist = type("Volume_does_not_exist", (Exception,), {})
    log = sys.modules.setdefault("xapi.storage.log",
                                 types.ModuleType("xapi.storage.log"))
    log.log_call_argv = lambda *a, **k: None
    log.debug = lambda *a, **k: None
    sys.modules["xapi.storage"].log = log
    # purefa_fa must be importable by its bare name (sibling import).
    fa_mod = _load_fa_module()
    sys.modules["purefa_fa"] = fa_mod

    path = os.path.join(xcpng_mod._SMAPIV3_SRC_DIR, filename)
    spec = importlib.util.spec_from_file_location(modname + "_under_test", path)
    mod = importlib.util.module_from_spec(spec)
    # sr.py / volume.py do `import sr` / `import purefa_fa`; register under the
    # bare names they expect.
    sys.modules[modname] = mod
    spec.loader.exec_module(mod)
    return mod


def test_fa_create_vgroup_idempotent_treats_existing_as_success(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")

    def boom(method, path, body=None):
        raise RuntimeError("400 already exists")

    monkeypatch.setattr(fa, "request", boom)
    # already-exists must NOT raise (idempotent create).
    fa.create_vgroup("phif-abcd1234")


def test_fa_create_vgroup_posts_volume_groups(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []
    monkeypatch.setattr(fa, "request",
                        lambda method, path, body=None: seen.append((method, path)) or {})
    fa.create_vgroup("phif-abcd1234")
    # POST to create, then a best-effort PATCH destroyed=false to RECOVER a
    # soft-destroyed (pending-eradication) vgroup so its name is reusable.
    assert seen == [("POST", "volume-groups?names=phif-abcd1234"),
                    ("PATCH", "volume-groups?names=phif-abcd1234")]


def test_fa_member_name_url_encodes_slash(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []
    monkeypatch.setattr(fa, "request",
                        lambda method, path, body=None: seen.append(path) or {})
    # A member volume name "<vg>/<vol>" must encode the "/" as %2F in the path.
    fa.create_volume("phif-abcd1234/phif-abcd1234-vdi1", 1024)
    assert seen == ["volumes?names=phif-abcd1234%2Fphif-abcd1234-vdi1"]


def test_fa_has_no_volume_group_snapshot_endpoint():
    # FlashArray has NO volume-GROUP snapshot endpoint (a vgroup is a namespace,
    # not a consistency group -- POST volume-group-snapshots 404s). The driver must
    # NOT reference it; xapi drives per-VDI Volume.snapshot (per-volume snapshots).
    m = _load_fa_module()
    assert not hasattr(m.FlashArray, "group_snapshot")
    import inspect
    # not CALLED (a comment may still mention the name to explain why it's avoided)
    assert "volume-group-snapshots?" not in inspect.getsource(m)


def test_fa_destroy_vgroup_only_when_empty(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []

    def fake_request(method, path, body=None):
        seen.append((method, path))
        # vgroup_member_count queries volumes with a wildcard NAME FILTER
        # (volumes?filter=name='<vg>/*'), not names= -- Purity 400s on a
        # wildcard in names=. Report 1 live member.
        if method == "GET" and path.startswith("volumes?filter="):
            return {"items": [{"name": "phif-x/phif-x-a", "destroyed": False}]}
        return {}

    monkeypatch.setattr(fa, "request", fake_request)
    fa.destroy_vgroup("phif-x")
    # The member-count probe must actually have matched, or this test would pass
    # for the wrong reason (an unmatched probe reads as "empty").
    assert any(m == "GET" and p.startswith("volumes?filter=") for m, p in seen)
    # Non-empty -> no PATCH/DELETE on the volume-group.
    assert not any(p.startswith("volume-groups") for _, p in seen)


def test_fa_destroy_vgroup_eradicates_when_empty(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []

    def fake_request(method, path, body=None):
        seen.append((method, path))
        if method == "GET" and "volumes?names=" in path:
            return {"items": []}   # empty group
        return {}

    monkeypatch.setattr(fa, "request", fake_request)
    fa.destroy_vgroup("phif-x")
    assert ("PATCH", "volume-groups?names=phif-x") in seen
    assert ("DELETE", "volume-groups?names=phif-x") in seen


# --------------------------- sr.py key/name helpers (additive safety) --------
def test_sr_member_name_and_obj_name_roundtrip():
    sr = _load_sr_only()
    # vgroup == sr_id; member volume is "<vg>/<sr_id>-<uuid>".
    assert sr.vgroup_name("phif-x") == "phif-x"
    member = sr.member_name("phif-x", "vdi1")
    assert member == "phif-x/phif-x-vdi1"
    # A vgroup-member key round-trips to the FULL name (verbatim).
    assert sr.is_vgroup_key(member) is True
    assert sr.obj_name("phif-x", member) == member
    assert sr.vgroup_of(member) == "phif-x"


def test_sr_legacy_standalone_key_unchanged():
    # ADDITIVE SAFETY: a bare-uuid key (no '/') keeps the original "<sr_id>-<key>"
    # path -- existing standalone VDIs are untouched.
    sr = _load_sr_only()
    assert sr.is_vgroup_key("vdi1") is False
    assert sr.vgroup_of("vdi1") == ""
    assert sr.obj_name("phif-x", "vdi1") == "phif-x-vdi1"
    # snapshot keys (with '.') also pass through verbatim.
    assert sr.obj_name("phif-x", "phif-x/phif-x-vdi1.s1") == "phif-x/phif-x-vdi1.s1"


def test_sr_vol_to_vdi_key_roundtrip_vgroup_and_legacy():
    sr = _load_sr_only()
    # vgroup member -> key is the full "<vg>/<vol>" name.
    vdi = sr._vol_to_vdi("phif-x", {"name": "phif-x/phif-x-vdi1",
                                    "serial": "abc", "provisioned": 8}, "iscsi")
    assert vdi["key"] == "phif-x/phif-x-vdi1"
    assert vdi["uuid"] == "vdi1"
    # legacy standalone -> key is the bare uuid (unchanged behavior).
    vdi2 = sr._vol_to_vdi("phif-x", {"name": "phif-x-old", "serial": "abc",
                                     "provisioned": 8}, "iscsi")
    assert vdi2["key"] == "old"
    assert vdi2["uuid"] == "old"


# ------------------------- volume.py create/destroy thread the vgroup --------
def test_volume_create_makes_member_in_vgroup():
    vol = _import_smapiv3("volume", "volume.py")
    fake = _FakeFA()
    conf = {"endpoint": "fa.local", "token": "t", "hostgroup": "hg",
            "protocol": "iscsi"}
    # Patch the module's FA accessors so create() drives the fake client.
    vol.fa.load_sr_state = lambda sr: conf
    vol._fa = lambda c: fake
    impl = vol.Implementation()
    vdi = impl.create("dbg", "phif-x", "diskA", "", 4096, False)
    kinds = [c[0] for c in fake.calls]
    # vgroup created FIRST, then the member volume, then connected to the hg.
    assert kinds.index("create_vgroup") < kinds.index("create_volume")
    created = next(c for c in fake.calls if c[0] == "create_volume")
    assert created[1].startswith("phif-x/phif-x-")     # "<vg>/<sr_id>-<uuid>"
    assert ("connect_volume", created[1], "hg") in fake.calls
    # the returned VDI key round-trips to the member name.
    assert vdi["key"] == created[1]


def test_volume_destroy_last_member_tears_down_vgroup():
    vol = _import_smapiv3("volume", "volume.py")
    fake = _FakeFA()
    conf = {"endpoint": "fa.local", "token": "t", "hostgroup": "hg",
            "protocol": "iscsi"}
    vol.fa.load_sr_state = lambda sr: conf
    vol._fa = lambda c: fake
    # Seed one live member then destroy it via its vgroup-member key.
    member = "phif-x/phif-x-vdi1"
    fake.create_volume(member, 4096)
    fake.calls = []
    impl = vol.Implementation()
    impl.destroy("dbg", "phif-x", member)
    kinds = [c[0] for c in fake.calls]
    assert "destroy_volume" in kinds
    # last member gone -> the empty vgroup is torn down.
    assert "destroy_vgroup" in kinds
    assert fake.vgroup_member_count("phif-x") == 0


def test_volume_destroy_legacy_standalone_no_vgroup_teardown():
    # ADDITIVE SAFETY: a legacy bare-uuid key (no vgroup) must NOT attempt a
    # vgroup teardown -- it follows the original standalone destroy path.
    vol = _import_smapiv3("volume", "volume.py")
    fake = _FakeFA()
    conf = {"endpoint": "fa.local", "token": "t", "hostgroup": "hg",
            "protocol": "iscsi"}
    vol.fa.load_sr_state = lambda sr: conf
    vol._fa = lambda c: fake
    fake.volumes["phif-x-old"] = {"name": "phif-x-old", "serial": "s", "tags": {}}
    impl = vol.Implementation()
    impl.destroy("dbg", "phif-x", "old")
    kinds = [c[0] for c in fake.calls]
    assert ("destroy_volume", "phif-x-old", "hg", False) in fake.calls
    assert "destroy_vgroup" not in kinds


# ============================================================================ #
# Protection-group (pgroup) crash-consistent snapshots
# ============================================================================ #

# --------------------------- purefa_fa.py pgroup REST shapes -----------------
def test_fa_create_pgroup_posts_protection_groups(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []
    monkeypatch.setattr(fa, "request",
                        lambda method, path, body=None: seen.append((method, path)) or {})
    fa.create_pgroup("phif-x-pg")
    assert seen == [("POST", "protection-groups?names=phif-x-pg")]


def test_fa_create_pgroup_idempotent_swallows_exists(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")

    def boom(method, path, body=None):
        raise RuntimeError("400 already exists")

    monkeypatch.setattr(fa, "request", boom)
    fa.create_pgroup("phif-x-pg")   # must NOT raise


def test_fa_add_pgroup_volumes_encodes_member_slash(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []
    monkeypatch.setattr(fa, "request",
                        lambda method, path, body=None: seen.append((method, path)) or {})
    fa.add_pgroup_volumes("phif-x-pg",
                          ["phif-x/phif-x-a", "phif-x/phif-x-b"])
    method, path = seen[0]
    assert method == "POST"
    assert path.startswith("protection-groups/volumes?group_names=phif-x-pg")
    # member volume "/" must be %2F-encoded; members comma-joined
    assert "member_names=phif-x%2Fphif-x-a,phif-x%2Fphif-x-b" in path


def test_fa_snapshot_pgroup_posts_protection_group_snapshots(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []
    monkeypatch.setattr(fa, "request",
                        lambda method, path, body=None: seen.append((method, path)) or {})
    fa.snapshot_pgroup("phif-x-pg", "sdeadbeef")
    assert seen == [("POST",
                     "protection-group-snapshots?source_names=phif-x-pg&suffix=sdeadbeef")]


def test_fa_destroy_pgroup_snapshot_patches_then_deletes(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []

    def fake_request(method, path, body=None):
        seen.append((method, path, body))
        if method == "GET":
            return {"items": [{"name": "phif-x-pg.s1", "destroyed": False}]}
        return {}

    monkeypatch.setattr(fa, "request", fake_request)
    fa.destroy_pgroup_snapshot("phif-x-pg.s1", eradicate=True)
    methods = [(mth, p) for mth, p, _ in seen]
    assert ("PATCH", "protection-group-snapshots?names=phif-x-pg.s1") in methods
    assert ("DELETE", "protection-group-snapshots?names=phif-x-pg.s1") in methods
    # the destroyed flag is sent
    assert any(b == {"destroyed": True} for _, _, b in seen)


def test_fa_destroy_pgroup_snapshot_idempotent_when_absent(monkeypatch):
    m = _load_fa_module()
    fa = m.FlashArray("fa.local", "tok")
    seen = []

    def fake_request(method, path, body=None):
        seen.append((method, path))
        if method == "GET":
            return {"items": []}   # already gone
        return {}

    monkeypatch.setattr(fa, "request", fake_request)
    fa.destroy_pgroup_snapshot("phif-x-pg.s1")   # no eradicate, absent -> no-op
    # absent + not eradicating -> no PATCH/DELETE issued
    assert not any(mth in ("PATCH", "DELETE") for mth, _ in seen)


def test_fa_never_uses_volume_group_snapshot_endpoint():
    # The bogus "volume-group-snapshots" endpoint must never be referenced; only
    # the real "protection-group-snapshots" endpoint is used.
    import inspect
    m = _load_fa_module()
    src = inspect.getsource(m)
    assert "volume-group-snapshots?" not in src
    assert "protection-group-snapshots?source_names=" in src


# --------------------------- sr.py snapshot-key encode/decode ----------------
def test_sr_pgroup_name():
    sr = _load_sr_only()
    assert sr.pgroup_name("phif-x") == "phif-x-pg"
    assert sr.pgroup_name("") == ""


def test_sr_per_volume_snapshot_key_recognized():
    # XCP uses PER-VOLUME snapshots (its vgroup is SR-scoped, so a pgroup snapshot
    # would capture the whole SR -- pgroups are for the per-VM Proxmox/HPE plugins).
    # A snapshot key is "<volume>.<sfx>"; the volume may be a vgroup member, so the
    # suffix '.' is in the LAST '/'-segment.
    sr = _load_sr_only()
    member_snap = "phif-x/phif-x-vdi1.s1"
    assert sr.is_snapshot_key(member_snap) is True
    # a plain vgroup-member VOLUME (no '.' in its last segment) is NOT a snapshot
    assert sr.is_snapshot_key("phif-x/phif-x-vdi1") is False
    # a legacy per-volume snapshot (no '/') is a snapshot
    assert sr.is_snapshot_key("phif-x-vdi1.s1") is True
    # obj_name returns a snapshot/member key verbatim (it IS the copy/destroy name)
    assert sr.obj_name("phif-x", member_snap) == member_snap
    # the bad pgroup-snapshot helpers were removed (XCP is per-volume)
    assert not hasattr(sr, "is_pgroup_snapshot_key")
    assert not hasattr(sr, "pgroup_snapshot_of")


# --------------------------- volume.py pgroup snapshot flow ------------------
def _vol_with_fake():
    vol = _import_smapiv3("volume", "volume.py")
    fake = _FakeFA()
    conf = {"endpoint": "fa.local", "token": "t", "hostgroup": "hg",
            "protocol": "iscsi"}
    vol.fa.load_sr_state = lambda sr: conf
    vol._fa = lambda c: fake
    return vol, fake


def test_volume_snapshot_member_is_per_volume():
    # Snapshotting a vgroup-member VDI takes a PER-VOLUME FA snapshot of that disk
    # ("<vg>/<vol>.<sfx>"). XCP must NOT take a pgroup snapshot here: its vgroup is
    # SR-scoped, so a pgroup snapshot would capture every VDI in the SR. xapi
    # orchestrates VM-level consistency by snapshotting each VDI itself.
    vol, fake = _vol_with_fake()
    a = "phif-x/phif-x-vdiA"
    fake.create_volume(a, 4096)
    fake.calls = []
    impl = vol.Implementation()
    vdi = impl.snapshot("dbg", "phif-x", a)
    snap = next(c for c in fake.calls if c[0] == "snapshot_volume")
    assert snap[1] == a                       # snapshot the member volume itself
    suffix = snap[2]
    assert vdi["key"] == "%s.%s" % (a, suffix)   # key = "<vg>/<vol>.<sfx>"
    assert vdi["read_write"] is False
    # MUST NOT use a pgroup snapshot (would over-capture the whole SR) ...
    assert not any(c[0] == "snapshot_pgroup" for c in fake.calls)
    # ... nor the bogus volume-group-snapshot helper.
    assert not any(c[0] == "group_snapshot" for c in fake.calls)


def test_volume_snapshot_legacy_standalone_still_per_volume():
    # ADDITIVE SAFETY: a legacy bare-uuid VDI keeps the per-volume snapshot path.
    vol = _import_smapiv3("volume", "volume.py")
    conf = {"endpoint": "fa.local", "token": "t", "hostgroup": "hg",
            "protocol": "iscsi"}
    calls = []

    class _LegacyFA(object):
        def snapshot_volume(self, name, suffix):
            calls.append(("snapshot_volume", name, suffix))
            return {"items": [{"name": "%s.%s" % (name, suffix),
                               "provisioned": 8}]}

        def get_snapshot(self, name):
            return {"name": name, "provisioned": 8}

    vol.fa.load_sr_state = lambda sr: conf
    vol._fa = lambda c: _LegacyFA()
    impl = vol.Implementation()
    vdi = impl.snapshot("dbg", "phif-x", "old")
    # standalone -> "<sr_id>-old.<sfx>" per-volume snapshot, NOT a pgroup.
    assert calls and calls[0][0] == "snapshot_volume"
    assert calls[0][1] == "phif-x-old"
    assert vdi["key"].startswith("phif-x-old.")


def test_volume_destroy_member_snapshot_is_per_volume():
    # Destroying a snapshot VDI destroys the PER-VOLUME FA snapshot "<vg>/<vol>.<sfx>"
    # (XCP per-volume model). No volume destroy for a snapshot key.
    vol, fake = _vol_with_fake()
    snap_key = "phif-x/phif-x-vdiA.s1"
    fake.calls = []
    impl = vol.Implementation()
    impl.destroy("dbg", "phif-x", snap_key)
    assert ("destroy_snapshot", snap_key, False) in fake.calls
    assert "destroy_volume" not in [c[0] for c in fake.calls]


def test_volume_destroy_member_snapshot_eradicate_passes_flag():
    vol = _import_smapiv3("volume", "volume.py")
    fake = _FakeFA()
    conf = {"endpoint": "fa.local", "token": "t", "hostgroup": "hg",
            "protocol": "iscsi", "eradicate": True}
    vol.fa.load_sr_state = lambda sr: conf
    vol._fa = lambda c: fake
    impl = vol.Implementation()
    impl.destroy("dbg", "phif-x", "phif-x/phif-x-vdiA.s1")
    assert ("destroy_snapshot", "phif-x/phif-x-vdiA.s1", True) in fake.calls


def test_volume_destroy_legacy_snapshot_still_per_volume():
    # ADDITIVE SAFETY: a legacy per-volume snapshot key (no '/') uses destroy_snapshot.
    vol = _import_smapiv3("volume", "volume.py")
    conf = {"endpoint": "fa.local", "token": "t", "hostgroup": "hg",
            "protocol": "iscsi"}
    calls = []

    class _LegacyFA(object):
        def destroy_snapshot(self, name, eradicate=False):
            calls.append(("destroy_snapshot", name, eradicate))

    vol.fa.load_sr_state = lambda sr: conf
    vol._fa = lambda c: _LegacyFA()
    impl = vol.Implementation()
    impl.destroy("dbg", "phif-x", "phif-x-old.s1")
    assert calls == [("destroy_snapshot", "phif-x-old.s1", False)]


def test_volume_clone_from_snapshot_copies_snapshot_name():
    vol, fake = _vol_with_fake()
    snap_key = "phif-x/phif-x-vdiA.s1"           # per-volume member snapshot
    impl = vol.Implementation()
    vdi = impl.clone("dbg", "phif-x", snap_key)
    # clone copies FROM the snapshot name INTO a new vgroup member.
    copy = next(c for c in fake.calls if c[0] == "copy_volume")
    assert copy[1] == snap_key                     # source = snapshot name
    assert copy[2].startswith("phif-x/phif-x-")    # dest = new vgroup member
    assert vdi["key"] == copy[2]


def test_volume_destroy_last_member_tears_down_pgroup_too():
    vol, fake = _vol_with_fake()
    member = "phif-x/phif-x-vdi1"
    fake.create_volume(member, 4096)
    fake.create_pgroup("phif-x-pg")
    fake.calls = []
    impl = vol.Implementation()
    impl.destroy("dbg", "phif-x", member)
    kinds = [c[0] for c in fake.calls]
    # last member gone -> both the vgroup AND its pgroup are torn down.
    assert "destroy_vgroup" in kinds
    assert ("destroy_pgroup", "phif-x-pg") in fake.calls


# ------------------------------- 4. multipath stanza in custom.conf ----------
async def test_setup_connectivity_writes_custom_conf_path(make_context, captured_logs):
    c = XcpngConnector(_ctx(make_context, protocol="iscsi"))
    r = await c.setup_connectivity(portals="10.0.0.10", target_iqn="iqn.t")
    assert r.success
    joined = "\n".join(captured_logs)
    # Everpure-recommended XCP-ng location (NOT /etc/multipath.conf)
    assert "/etc/multipath/conf.d/pure.conf" in joined
    assert "/etc/multipath.conf <<" not in joined
    # find_multipaths + PURE/FlashArray ALUA stanza + pool toggle + restart
    assert "find_multipaths no" in joined
    assert "vendor \"PURE\"" in joined
    assert "multipathing=true" in joined
    assert "systemctl restart multipathd" in joined


def test_multipath_conf_constant_has_pure_stanza():
    conf = xcpng_mod._MULTIPATH_CONF
    assert xcpng_mod._MULTIPATH_CONF_PATH == "/etc/multipath/conf.d/pure.conf"
    assert "find_multipaths no" in conf
    assert "user_friendly_names no" in conf       # always WWID-named
    assert 'vendor "PURE"' in conf
    assert 'product "FlashArray"' in conf
    assert "prio alua" in conf


# ============================================================================ #
# Cluster / pool (multi-node) support
# ============================================================================ #

# ----------------------------------------------------------- wizard_steps ---
def test_wizard_steps_no_configure_sr_in_deploy():
    # SR creation happens inside `deploy` (xe sr-create); there is no separate
    # `configure` step. register_hosts runs BEFORE deploy so the FlashArray host
    # objects exist before the SR that binds to them is created.
    steps = XcpngConnector.wizard_steps()
    assert steps == ["register_hosts", "deploy", "setup_connectivity"]
    assert steps.index("register_hosts") < steps.index("deploy")
    assert "configure" not in steps
    # every wizard step maps to a real action id on this connector
    action_ids = {a.id for a in XcpngConnector.action_schemas()}
    assert set(steps) <= action_ids


def test_descriptor_includes_wizard_steps():
    d = XcpngConnector.descriptor()
    assert d["wizard_steps"] == ["register_hosts", "deploy", "setup_connectivity"]


async def test_deploy_fails_without_host_objects(make_context):
    # Validate host objects exist before creating the SR: deploy must fail clearly
    # when the host group has no members (hosts not registered yet).
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    r = await c.deploy_integration(endpoint="10.0.0.5", host_group="xcp-pool-hg")
    assert not r.success
    assert "host group" in r.message.lower()
    assert "register" in r.message.lower()


# -------------------------------------------------------------- list_nodes ---
async def test_list_nodes_mock_synthesizes_two_host_pool(make_context):
    c = XcpngConnector(_ctx(make_context))
    nodes = await c.list_nodes()
    assert len(nodes) == 2
    # rooted at the configured pool master
    assert nodes[0].host == "xcp-master.test.local"
    assert nodes[0].info.get("role") == "master"
    assert nodes[1].info.get("role") == "member"
    # serializable for the API/UI
    assert nodes[0].to_dict()["host"] == "xcp-master.test.local"


def test_parse_host_list_multi_record():
    # The DEFAULT (non-minimal) `xe host-list params=...` block output: one record
    # per host, fields as "<key> ( RO|RW): <value>", records separated by blanks.
    out = (
        "uuid ( RO)                : uuid-a\n"
        "          name-label ( RW): master.example.local\n"
        "             address ( RO): 10.0.0.10\n"
        "\n\n"
        "uuid ( RO)                : uuid-b\n"
        "          name-label ( RW): member.example.local\n"
        "             address ( RO): 10.0.0.11\n"
    )
    nodes = XcpngConnector._parse_host_list(out)
    # BOTH pool hosts must be parsed (regression: only one was ever seen).
    assert [(n.name, n.host) for n in nodes] == [
        ("master.example.local", "10.0.0.10"),
        ("member.example.local", "10.0.0.11"),
    ]
    assert nodes[0].info["uuid"] == "uuid-a"


def test_parse_host_list_falls_back_to_name_when_no_address():
    # A record with no address field uses the name-label as the SSH host.
    out = ("uuid ( RO)                : uuid-a\n"
           "          name-label ( RW): only-name\n")
    nodes = XcpngConnector._parse_host_list(out)
    assert len(nodes) == 1
    assert nodes[0].host == "only-name"


def test_parse_host_list_empty_returns_nothing():
    assert XcpngConnector._parse_host_list("") == []
    assert XcpngConnector._parse_host_list("\n  \n") == []


# ---------------------------------------------------------- validate_cluster ---
async def test_validate_cluster_consistent_iscsi(make_context, captured_logs):
    # Mock interface inventory is identical across nodes -> consistent.
    c = XcpngConnector(_ctx(make_context, protocol="iscsi"))
    r = await c.validate_cluster()
    assert r.success
    assert r.data["protocol"] == "iscsi"
    assert r.data["interface_kind"] == "nics"
    assert len(r.data["nodes"]) == 2
    # every node compared on the same NIC set
    assert set(r.data["per_node"]) == {n["name"] for n in r.data["nodes"]}


async def test_validate_cluster_picks_kind_by_protocol(make_context):
    c = XcpngConnector(_ctx(make_context, protocol="fc"))
    r = await c.validate_cluster()
    assert r.success
    assert r.data["interface_kind"] == "fc_hbas"

    c2 = XcpngConnector(_ctx(make_context, protocol="nvme-tcp"))
    r2 = await c2.validate_cluster()
    assert r2.data["interface_kind"] == "nvme_sources"


async def test_validate_cluster_single_node_always_ok(make_context):
    # A real single-node enumeration (parsed) always validates.
    c = XcpngConnector(_ctx(make_context, protocol="iscsi"))
    # Force a single-node list_nodes via the parser path.
    from unittest.mock import AsyncMock
    one = [xcpng_mod.ClusterNode(name="solo", host="10.0.0.10")]
    c.list_nodes = AsyncMock(return_value=one)
    r = await c.validate_cluster()
    assert r.success
    assert len(r.data["nodes"]) == 1


async def test_validate_cluster_inconsistent_fails(make_context, monkeypatch):
    # Different interface sets per node -> validation fails with detail.
    c = XcpngConnector(_ctx(make_context, protocol="iscsi"))

    async def fake_discover(host, kind, **kw):
        if host.endswith("-2"):
            return [{"value": "eth0"}]  # member missing eth1
        return [{"value": "eth0"}, {"value": "eth1"}]

    monkeypatch.setattr(c.ctx.runner, "discover_interfaces", fake_discover)
    r = await c.validate_cluster()
    assert not r.success
    assert "differ" in r.message or "inconsistent" in r.message.lower()


# ----------------------------------------------- deploy driver fan-out -------
async def test_deploy_pushes_plugin_to_every_pool_host(make_context, captured_logs):
    # The SMAPIv3 plugin must be installed on ALL pool hosts (2 in mock), while
    # xe sr-create stays a SINGLE call on the master.
    ctx = _ctx(make_context)
    c = XcpngConnector(ctx)
    _seed_hg(ctx, "xcp-pool-hg")
    r = await c.deploy_integration(host_group="xcp-pool-hg")
    assert r.success
    joined = "\n".join(captured_logs)
    assert "Installing SMAPIv3 volume plugin org.xen.xapi.storage.purefa" in joined
    assert "on 2 pool host(s)" in joined
    # the volume plugin dir is created on each host (2 nodes)
    assert sum(
        l.split("$ ", 1)[-1].lstrip().startswith(
            "mkdir -p /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.purefa")
        for l in captured_logs) == 2
    # ...and the custom datapath plugin dir on each host too.
    assert sum(
        l.split("$ ", 1)[-1].lstrip().startswith(
            "mkdir -p /usr/libexec/xapi-storage-script/datapath/purefa")
        for l in captured_logs) == 2
    # exactly one sr-create issued (pool-wide SR), matched on the invocation line
    # (now wrapped in `timeout` + output redirected to avoid the SSH fd-hang).
    assert sum(
        "$ " in l and "xe sr-create type=purefa" in l.split("$ ", 1)[-1]
        and "cat /tmp/phif-srcreate.out" in l
        for l in captured_logs) == 1


# ----------------------------------- connectivity fan-out across pool --------
async def test_setup_connectivity_fans_out_across_pool(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.10", target_iqn="iqn.t")
    assert r.success
    joined = "\n".join(captured_logs)
    assert "Configuring connectivity on 2 pool host(s)" in joined
    # the multipath stanza is written on EACH pool host
    assert joined.count("on xcp-master.test.local-2") >= 1
    # iSCSI login attempted on both hosts (one ssh log line per host)
    assert sum("iscsiadm -m node" in l for l in captured_logs) == 2


async def test_setup_connectivity_fc_fans_out(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="fc")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity(portals="")
    assert r.success
    joined = "\n".join(captured_logs)
    # FC rescan happens on each pool host, never an iSCSI login
    assert "iscsiadm -m node" not in joined
    assert sum("multipath -r" in l for l in captured_logs) == 2


# ============================================================================ #
# Feature 1: assess_cluster + reconcile_cluster
# ============================================================================ #
from unittest.mock import AsyncMock  # noqa: E402


def _seed_group(ctx, host_group="xcp-pool-hg", members=("xcp-pool-hg-extra",)):
    """Pre-populate the mock array host group with given members."""
    ctx.array.host_groups[host_group] = {"hosts": list(members)}
    for m in members:
        ctx.array.hosts.setdefault(m, {"iqns": [], "wwns": [], "nqns": []})


def test_reconcile_cluster_capability_and_actions():
    assert Capability.RECONCILE_CLUSTER in XcpngConnector.capabilities()
    actions = {a.id: a for a in XcpngConnector.action_schemas()}
    assert "assess_cluster" in actions and "reconcile_cluster" in actions
    assert actions["assess_cluster"].long_running is False
    rc = actions["reconcile_cluster"]
    assert rc.destructive is True
    apply = {f.name: f for f in rc.fields}["apply_removals"]
    assert apply.type.value == "bool" and apply.default is False


async def test_assess_cluster_reports_new_hosts_ready(make_context):
    # Empty host group -> both synthetic pool nodes are NEW. iSCSI mock NICs put
    # eth0 on the array's iSCSI portal subnet, so each new host scores ready.
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.assess_cluster()
    assert r.success
    assert len(r.data["nodes"]) == 2
    assert len(r.data["new_hosts"]) == 2
    assert all(h["ready"] for h in r.data["new_hosts"])
    # FA host names follow the per-node scheme used by register_hosts.
    names = {h["host"] for h in r.data["new_hosts"]}
    assert names == {"xcp-pool-hg-xcp-master-test-local",
                     "xcp-pool-hg-xcp-master-test-local-2"}
    assert r.data["departed_hosts"] == []


async def test_assess_cluster_detects_departed_and_existing(make_context):
    # Seed the group with the master's FA host (so it's NOT new) plus a stray
    # member with no matching node (departed).
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    _seed_group(ctx, members=["xcp-pool-hg-xcp-master-test-local",
                              "xcp-pool-hg-ghost"])
    r = await c.assess_cluster()
    assert r.success
    # only the second node is new now
    new_names = {h["node"] for h in r.data["new_hosts"]}
    assert new_names == {"xcp-master.test.local-2"}
    assert r.data["departed_hosts"] == ["xcp-pool-hg-ghost"]


async def test_assess_cluster_readiness_not_ready_when_unreachable(make_context):
    # A node whose discovery raises is unreachable -> not ready.
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)

    real = ctx.runner.discover_initiators

    async def flaky(host, **kw):
        if host.endswith("-2"):
            raise RuntimeError("ssh timeout")
        return await real(host, **kw)

    ctx.runner.discover_initiators = flaky
    r = await c.assess_cluster()
    by_node = {h["node"]: h for h in r.data["new_hosts"]}
    assert by_node["xcp-master.test.local"]["ready"] is True
    bad = by_node["xcp-master.test.local-2"]
    assert bad["ready"] is False
    assert any("unreachable" in reason for reason in bad["reasons"])


async def test_reconcile_configures_ready_skips_not_ready(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)

    async def flaky(host, **kw):
        if host.endswith("-2"):
            raise RuntimeError("ssh timeout")
        return {"iqn": f"iqn.test:{host}", "nqn": None, "wwns": []}

    ctx.runner.discover_initiators = flaky
    r = await c.reconcile_cluster()
    assert r.success
    # only the reachable master node was configured + registered on the array
    assert r.data["configured"] == ["xcp-pool-hg-xcp-master-test-local"]
    assert [h["node"] for h in r.data["not_ready"]] == ["xcp-master.test.local-2"]
    assert "xcp-pool-hg-xcp-master-test-local" in ctx.array.hosts
    assert "xcp-pool-hg-xcp-master-test-local-2" not in ctx.array.hosts
    # plugin install + connectivity ran ONLY for the one ready node
    joined = "\n".join(captured_logs)
    assert "Configuring 1 ready new host(s)" in joined


async def test_reconcile_flags_departed_by_default(make_context):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    _seed_group(ctx, members=["xcp-pool-hg-ghost"])
    r = await c.reconcile_cluster()  # apply_removals defaults False
    assert r.success
    assert r.data["pending_removals"] == ["xcp-pool-hg-ghost"]
    assert "removed" not in r.data
    # ghost host NOT removed from the group / array
    assert "xcp-pool-hg-ghost" in ctx.array.host_groups["xcp-pool-hg"]["hosts"]
    assert "xcp-pool-hg-ghost" in ctx.array.hosts


async def test_reconcile_removes_departed_when_flag_set(make_context):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    _seed_group(ctx, members=["xcp-pool-hg-ghost"])
    r = await c.reconcile_cluster(apply_removals=True)
    assert r.success
    assert r.data["removed"] == ["xcp-pool-hg-ghost"]
    assert "pending_removals" not in r.data
    assert "xcp-pool-hg-ghost" not in ctx.array.host_groups["xcp-pool-hg"]["hosts"]
    assert "xcp-pool-hg-ghost" not in ctx.array.hosts


async def test_reconcile_dry_run_no_mutation(make_context):
    ctx = _ctx(make_context, protocol="iscsi")
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    _seed_group(ctx, members=["xcp-pool-hg-ghost"])
    r = await c.reconcile_cluster(apply_removals=True)
    assert r.success
    # register_hosts is a no-op in dry-run, so no new FA hosts created
    assert "xcp-pool-hg-xcp-master-test-local" not in ctx.array.hosts
    # departed host still present (apply_removals honored but array untouched is
    # acceptable -- prune runs but dry-run register/connectivity make no changes)


async def test_reconcile_dispatch_routes(make_context):
    ctx = _ctx(make_context, protocol="iscsi")
    c = XcpngConnector(ctx)
    r = await c.dispatch("assess_cluster", {})
    assert r.success and "new_hosts" in r.data
    r2 = await c.dispatch("reconcile_cluster", {"apply_removals": False})
    assert r2.success and "configured" in r2.data


# ============================================================================ #
# Feature 2: NFS protocol support (stock XCP-ng NFS SR)
# ============================================================================ #
def test_nfs_in_supported_protocols_and_options():
    assert Protocol.NFS in XcpngConnector.SUPPORTED_PROTOCOLS
    proto = next(f for f in XcpngConnector.target_schema()
                 if f.name == "protocol")
    assert "nfs" in (proto.options or [])
    ids = {a.id for a in XcpngConnector.action_schemas()}
    assert {"provision_nfs", "teardown_nfs"} <= ids


async def test_nfs_register_hosts_skipped(make_context):
    # NFS has no block initiators: register_hosts is a no-op (no array hosts).
    ctx = _ctx(make_context, protocol="nfs")
    c = XcpngConnector(ctx)
    r = await c.register_hosts(host_group="xcp-pool-hg")
    assert r.success
    assert r.artifacts["skipped"] is True
    assert not ctx.array.hosts


async def test_nfs_setup_connectivity_skipped(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nfs")
    c = XcpngConnector(ctx)
    r = await c.setup_connectivity()
    assert r.success
    assert r.artifacts["skipped"] is True
    joined = "\n".join(captured_logs)
    assert "iscsiadm" not in joined
    assert "nvme connect" not in joined
    assert "multipathd" not in joined  # no multipath stanza/restart for NFS


async def test_nfs_provision_creates_export_and_sr_create(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nfs")
    c = XcpngConnector(ctx)
    r = await c.provision_nfs(name="nfs-sr", sr_name="nfs-sr")
    assert r.success
    # FA file system + NFS export created
    assert "nfs-sr" in ctx.array.filesystems
    assert "nfs-sr" in ctx.array.nfs_exports
    assert r.artifacts["server"] in ("10.30.30.30", "10.30.31.30")
    # stock NFS SR created over SSH on the pool master
    joined = "\n".join(captured_logs)
    assert "xe sr-create type=nfs" in joined
    assert "device-config:server=" in joined
    assert "device-config:serverpath=/nfs-sr" in joined
    # NOT the purefa block SR
    assert "type=purefa" not in joined


async def test_nfs_provision_dry_run(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nfs")
    ctx.dry_run = True
    c = XcpngConnector(ctx)
    r = await c.provision_nfs(name="nfs-sr")
    assert r.success
    assert "nfs-sr" not in ctx.array.filesystems
    assert not any("xe sr-create" in l for l in captured_logs)


async def test_nfs_teardown_removes_export_and_filesystem(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nfs")
    c = XcpngConnector(ctx)
    await c.provision_nfs(name="nfs-sr", sr_name="nfs-sr")
    r = await c.teardown_nfs(name="nfs-sr", sr_name="nfs-sr", eradicate=True)
    assert r.success
    assert "nfs-sr" not in ctx.array.filesystems
    assert "nfs-sr" not in ctx.array.nfs_exports
    assert ("delete_filesystem", {"name": "nfs-sr", "eradicate": True}) in ctx.array.calls


async def test_nfs_dispatch_routes(make_context):
    ctx = _ctx(make_context, protocol="nfs")
    c = XcpngConnector(ctx)
    r = await c.dispatch("provision_nfs", {"name": "d-nfs"})
    assert r.success
    assert "d-nfs" in ctx.array.filesystems
    r2 = await c.dispatch("teardown_nfs", {"name": "d-nfs"})
    assert r2.success
    assert "d-nfs" not in ctx.array.filesystems


# ----------------------------------------------- doc-validation honesty ---
def _read(path):
    with open(path, "r", encoding="utf-8") as fh:
        return fh.read()


def _smapiv3_path(*parts):
    return os.path.join(xcpng_mod._SMAPIV3_SRC_DIR, *parts)


def test_validated_smapiv3_path_has_no_doc_validate_markers():
    """The SMAPIv3 core path is hardware-validated on XCP-ng 8.3, so its
    TODO(doc-validate) markers must be resolved into confirmed statements."""
    connector_src = _read(xcpng_mod.__file__)
    assert "TODO(doc-validate)" not in connector_src
    for fname in ("purefa_fa.py", "sr.py", "volume.py"):
        assert "TODO(doc-validate)" not in _read(_smapiv3_path(fname)), fname


def test_nfs_path_left_honestly_unvalidated():
    """The NFS path is NOT hardware-validated; its honest markers must remain."""
    connector_src = _read(xcpng_mod.__file__)
    assert connector_src.count("TODO(hardware-validate)") == 2


def test_legacy_smapiv1_driver_removed():
    """The non-functional SMAPIv1 PureSR.py driver is deleted (XCP-ng 8.3 does
    not load dropped-in SMAPIv1 drivers); only the SMAPIv3 plugin ships."""
    assert not os.path.exists(
        os.path.join(xcpng_mod._DRIVER_DIR, "PureSR.py"))
    # and the now-dead legacy symbols are gone from the connector module.
    for sym in ("_DRIVER_PATH", "_DRIVER_FILENAME", "_SM_INSTALL_PATH",
                "_read_driver_source"):
        assert not hasattr(xcpng_mod, sym), sym


def test_module_docstring_states_smapiv3_validated():
    assert "validated on a live 2-host XCP-ng 8.3 pool" in xcpng_mod.__doc__
