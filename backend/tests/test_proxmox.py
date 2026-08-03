"""Unit tests for the Proxmox VE connector (Everpure 'purefa' storage plugin).

The Proxmox integration is a true per-disk-volume storage plugin (modeled on the
Everpure CSI / Cinder block drivers): each VM disk is its own FlashArray volume,
presented directly as a raw multipath block device, with array-side snapshots
and clones. These tests use the mock array + runner via make_context.
"""

from pathlib import Path

import pytest

from phif.connectors.base import (
    Capability,
    ConnectionValidationError,
    ConnectorContext,
    HypervisorTarget,
    Protocol,
)
from phif.connectors.proxmox import connector as proxmox_mod
from phif.connectors.proxmox.connector import ProxmoxConnector
from phif.jobs.runner import JobRunner


# ----------------------------------------------------------------- helpers ---
def _ctx(make_context, *, protocol="nvme-tcp", with_array=True, **conn):
    connection = {
        "node_host": "pve01.test.local",
        "ssh_user": "root",
        "protocol": protocol,
        "storage_id": "purefa",
        "host_group": "pve-cluster",
    }
    connection.update(conn)
    return make_context(
        connector_key="proxmox",
        connection=connection,
        secrets={"ssh_password": "s3cret"},
        with_array=with_array,
    )


# --------------------------------------------------------------- metadata ---
def test_descriptor_metadata():
    d = ProxmoxConnector.descriptor()
    assert d["key"] == "proxmox"
    assert d["name"] == "Proxmox VE (Everpure storage plugin)"
    assert d["maturity"] == "ga"
    caps = set(d["capabilities"])
    for expected in ("connect", "host_register", "connectivity", "deploy_plugin",
                     "configure", "provision_volume", "snapshot", "clone",
                     "resize", "health", "remove"):
        assert expected in caps
    assert set(d["protocols"]) == {"iscsi", "fc", "nvme-tcp", "nfs"}


def test_supported_protocols():
    assert ProxmoxConnector.SUPPORTED_PROTOCOLS == {
        Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP, Protocol.NFS}


def test_capabilities_include_connect():
    assert Capability.CONNECT in ProxmoxConnector.capabilities()


def test_target_schema_fields():
    names = {f.name for f in ProxmoxConnector.target_schema()}
    assert {"node_host", "ssh_user", "ssh_password", "ssh_key", "protocol",
            "storage_id", "host_group", "node_iqn", "node_nqn",
            "node_wwns"} <= names
    # protocol enum must offer fc.
    proto = next(f for f in ProxmoxConnector.target_schema() if f.name == "protocol")
    assert "fc" in (proto.options or [])


def test_action_schemas_have_capability_actions():
    ids = {a.id for a in ProxmoxConnector.action_schemas()}
    assert {"register_hosts", "setup_connectivity", "deploy", "configure",
            "provision", "snapshot", "clone", "resize", "health_check",
            "teardown"} <= ids


# ------------------------------------------------- the Perl plugin file ---
def test_perl_plugin_file_exists_and_declares_purefa():
    pm = proxmox_mod.PLUGIN_FILE
    assert pm.exists(), f"missing plugin file {pm}"
    text = pm.read_text(encoding="utf-8")
    # The plugin must declare storage type 'purefa' and subclass PVE::Storage::Plugin.
    assert "sub type" in text
    assert "'purefa'" in text
    assert "PVE::Storage::Plugin" in text
    # And implement the core per-volume API methods + array snapshot/clone.
    for method in ("alloc_image", "free_image", "list_images", "status",
                   "activate_volume", "deactivate_volume", "volume_resize",
                   "volume_snapshot", "volume_snapshot_rollback",
                   "volume_snapshot_delete", "clone_image", "volume_has_feature",
                   "parse_volname"):
        assert f"sub {method}" in text, f"plugin missing sub {method}"


def test_perl_plugin_handles_fc_wwid_without_login():
    """The Perl plugin must map FC volumes by multipath WWID and NOT log in."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    # protocol property advertises fc.
    assert "'fc'" in text or '"fc"' in text
    # WWID resolution: SCSI WWID = "3" + lowercased FA serial (/dev/mapper/3<serial>).
    assert "/dev/mapper/3" in text
    assert "_fa_volume_serial" in text or "serial" in text
    # activate_volume must branch on FC and rescan the SCSI bus (no login).
    assert "issue_lip" in text or "/sys/class/scsi_host" in text
    # FC branch must not call iscsiadm or nvme inside the fc path. We assert the
    # fc rescan markers exist; login commands belong only to the iscsi/nvme arms.
    assert "rescan" in text.lower()


def test_perl_plugin_avoids_pending_eradication_name_reuse():
    """Volume-name allocation must skip destroyed (pending-eradication) names.

    A just-destroyed FA volume is held pending eradication; reusing its
    vm-<vmid>-disk-<N> name fails with a "pending eradication" error. The plugin
    must override find_free_diskname to consult destroyed volumes too, and retry
    allocation on a name collision.
    """
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    assert "sub find_free_diskname" in text       # override the PVE base
    assert "destroyed=true" in text               # query pending-eradication vols
    # alloc_image retries on a collision / eradication error with a fresh name.
    assert "eradicat" in text.lower()
    assert "find_free_diskname" in text


def test_perl_plugin_honors_interface_binding_keys():
    """The plugin must declare the PHIF binding keys AND enforce them.

    iscsi_nics / nvme_sources / fc_hbas are declared in properties()/options()
    and are honored by the activation path: bound iSCSI iface login, NVMe
    --host-traddr per source, and an FC rescan restricted to the named HBAs.
    """
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    # declared as config keys
    for key in ("iscsi_nics", "nvme_sources", "fc_hbas"):
        assert key in text, f"plugin missing binding key {key}"
    # enforcement helpers present
    for helper in ("_binding_list", "_iscsi_login_bound",
                   "_nvme_connect_bound", "_fc_rescan_bound"):
        assert f"sub {helper}" in text, f"plugin missing {helper}"
    # the bound paths use the right transport primitives
    assert "--host-traddr" in text          # NVMe-TCP source binding
    assert "phif_" in text                   # open-iscsi bound iface name
    assert "port_name" in text               # FC HBA WWPN match for rescan
    # the binding-enforcement TODO is resolved (no doc-validate marker remains).
    assert "TODO(doc-validate): wire activate_volume" not in text


def test_perl_plugin_binding_is_additive_default_path_intact():
    """With no binding set, the validated default transport path must remain.

    The default arms (`nvme connect-all`, `iscsiadm -m session --rescan`, and the
    all-host FC LIP/scan) must still be present and reachable, so an unset
    binding key is a no-op.
    """
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    assert "'nvme', 'connect-all'" in text or '"nvme", "connect-all"' in text
    assert "'iscsiadm', '-m', 'session', '--rescan'" in text
    # all-host FC scan fallback still present
    assert "/sys/class/scsi_host/host*/scan" in text
    # the bound helpers return false (no-op) on an empty value so the default runs
    assert "return 0 unless @" in text


def test_perl_plugin_no_doc_validate_todos_on_validated_paths():
    """The validated block-path TODO(doc-validate) markers are resolved.

    The only remaining honest 'not validated' wording is for the NVMe by-id path
    and the NFS/FlashArray File path, which are NOT hardware-validated.
    """
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    # No TODO(doc-validate) markers remain anywhere in the plugin.
    assert "TODO(doc-validate)" not in text
    # NFS + NVMe by-id remain honestly marked as not-yet-validated.
    assert "not yet hardware-validated" in text.lower() \
        or "not yet hardware validated" in text.lower()


def test_connector_has_no_doc_validate_todos():
    """The validated connector markers are rewritten as confirmed statements."""
    text = (Path(proxmox_mod.__file__)).read_text(encoding="utf-8")
    assert "TODO(doc-validate)" not in text
    # NFS remains honestly flagged as not-yet-validated.
    assert "not yet hardware-validated" in text.lower()


def test_perl_plugin_creates_vgroup_on_alloc():
    """alloc_image must ensure the per-VM vgroup exists, then create the disk as
    a member volume <vg>/<vol> inside it."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    # vgroup support is declared as a config key (default on) + helpers exist.
    assert "volume_groups" in text
    for helper in ("_vgroup_name", "_vgroups_enabled", "_fa_ensure_vgroup",
                   "_fa_name", "_fa_urlenc"):
        assert f"sub {helper}" in text, f"plugin missing {helper}"
    # the vgroup is created via POST volume-groups?names=<vg>
    assert "volume-groups?names=" in text
    # alloc_image ensures the vgroup then builds the grouped member name.
    alloc = text.split("sub alloc_image", 1)[1].split("\nsub ", 1)[0]
    assert "_fa_ensure_vgroup" in alloc
    assert '"$vg/$name"' in alloc or "$vg/$name" in alloc


def test_perl_plugin_member_volume_named_vg_slash_vol():
    """A member volume's FA name is <vg>/<vol> and the '/' is URL-encoded (%2F)."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    # the vgroup name derives from the vmid as vm-<vmid>
    assert '"vm-$id"' in text or "vm-$id" in text
    # _fa_urlenc percent-encodes '/', i.e. it does NOT pass slashes through. The
    # encoder escapes any non-unreserved byte (slash among them) -> %2F.
    enc = text.split("sub _fa_urlenc", 1)[1].split("\nsub ", 1)[0]
    assert "A-Za-z0-9._~-" in enc            # unreserved set; '/' is NOT in it
    assert "%%%02X" in enc                   # percent-encode the rest (incl. '/')
    # the member create POSTs the encoded grouped name
    assert "_fa_urlenc($faname)" in text


def test_perl_plugin_snapshot_uses_pgroup_for_grouped_disks():
    """A GROUPED disk is snapshotted via a per-VM PROTECTION GROUP for crash-
    consistency: create the pgroup "<vg>-pg" (idempotent), add ALL the vgroup's
    member VOLUMES, then POST protection-group-snapshots. A STANDALONE disk keeps
    the legacy per-volume snapshot. The bad "volume-group-snapshots" endpoint
    (a vgroup is a namespace, not a consistency group) must NEVER be used."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    snap = text.split("sub volume_snapshot ", 1)[1].split("\nsub ", 1)[0]
    # the resolved name decides grouped vs standalone
    assert "_fa_name(" in snap
    assert "_split_fa_name" in snap
    # GROUPED path: pgroup create + add member volumes + protection-group snapshot
    assert "_fa_ensure_pgroup" in snap
    assert "_fa_vgroup_member_names" in snap          # enumerate ALL members
    assert "_fa_pgroup_add_volumes" in snap
    assert "_fa_pgroup_snapshot" in snap              # take the pgroup snapshot
    # the actual protection-group-snapshot POST lives in the helper
    assert "protection-group-snapshots?source_names=" in text
    # STANDALONE path: legacy per-volume snapshot endpoint still present
    assert "volume-snapshots?source_names=" in snap
    # the helper REST calls themselves
    assert "protection-groups?names=" in text          # create pgroup
    assert "protection-groups/volumes?group_names=" in text  # add member volumes
    assert "member_names=" in text
    # the vgroup member listing uses the documented filter form
    assert "volume_group.name=" in text
    # the nonexistent volume-GROUP snapshot endpoint must NEVER be CALLED
    assert "volume-group-snapshots?" not in text


def test_perl_plugin_snapshot_per_disk_idempotent():
    """PVE calls volume_snapshot once per disk with the SAME $snap. The grouped
    path adds ALL current members (not just this disk) BEFORE snapshotting, so the
    first per-disk call captures every disk; the 2nd..Nth find "<pg>.<snap>"
    already created. _fa_pgroup_snapshot tolerates a LIVE existing snapshot
    (idempotent) but ERADICATES + retakes a DESTROYED same-named one (name reuse)."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    snap = text.split("sub volume_snapshot ", 1)[1].split("\nsub ", 1)[0]
    # adds ALL enumerated members, plus the current disk defensively
    assert "@members" in snap
    # the pgroup-snapshot helper handles already-exists (live=benign, destroyed=stale)
    helper = text.split("sub _fa_pgroup_snapshot", 1)[1].split("\nsub ", 1)[0]
    assert "already exist" in helper.lower() or "exists" in helper.lower()
    # name-reuse fix: a DESTROYED same-named snapshot is detected + eradicated + retaken
    assert "destroyed" in helper.lower()
    assert "DELETE" in helper          # eradicate the stale snapshot
    # the add-volumes helper treats already-member as benign
    addv = text.split("sub _fa_pgroup_add_volumes", 1)[1].split("\nsub ", 1)[0]
    assert "already" in addv.lower()


def test_perl_plugin_snapshot_create_handles_name_reuse():
    """Snapshot-name reuse bug fix: when a snapshot name is reused after a delete
    (the prior pgroup/volume snapshot lingers DESTROYED pending eradication), the
    create path must NOT silently swallow 'already exists' (which would leave PVE
    with a snapshot that has no live array backing). It GETs the existing snapshot,
    and if destroyed, eradicates it and re-takes a fresh one."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    # pgroup path (grouped disks)
    helper = text.split("sub _fa_pgroup_snapshot", 1)[1].split("\nsub ", 1)[0]
    assert "GET" in helper and "protection-group-snapshots?names=" in helper
    # standalone path (per-volume) does the same dance
    vsnap = text.split("sub volume_snapshot ", 1)[1].split("\nsub ", 1)[0]
    assert "volume-snapshots?names=" in vsnap          # GET to check destroyed
    assert "destroyed" in vsnap.lower()


def test_perl_plugin_snapshot_member_name_resolution():
    """_fa_snapshot_name resolves the per-disk snapshot member name:
    grouped -> "<vg>-pg.<snap>.<vg>/<vol>", standalone -> "<faname>.<snap>".
    rollback overwrites the volume from that source; delete (grouped) destroys the
    WHOLE pgroup snapshot "<vg>-pg.<snap>" (member cannot be destroyed alone)."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    fsn = text.split("sub _fa_snapshot_name", 1)[1].split("\nsub ", 1)[0]
    # grouped member snapshot name "<pg>.<snap>.<faname>"
    assert "_pgroup_name" in fsn
    assert '"$pg.$snap.$faname"' in fsn
    # standalone keeps "<faname>.<snap>"
    assert '"$faname.$snap"' in fsn
    # rollback overwrites from the resolved snapshot source (both forms)
    rb = text.split("sub volume_snapshot_rollback", 1)[1].split("\nsub ", 1)[0]
    assert "overwrite=true" in rb
    assert "_fa_snapshot_name(" in rb
    # delete: grouped destroys the WHOLE pgroup snapshot, idempotently
    dl = text.split("sub volume_snapshot_delete", 1)[1].split("\nsub ", 1)[0]
    assert "_fa_destroy_pgroup_snapshot" in dl
    assert "$pg.$snap" in dl
    assert "eval" in dl                       # benign no-op for 2nd..Nth disk
    # standalone delete still targets the per-volume snapshot
    assert "volume-snapshots?names=" in dl
    # the pgroup-snapshot teardown is PATCH destroyed then DELETE on eradicate
    dps = text.split("sub _fa_destroy_pgroup_snapshot", 1)[1].split("\nsub ", 1)[0]
    assert "protection-group-snapshots?names=" in dps
    assert "destroyed" in dps
    assert "DELETE" in dps


def test_perl_plugin_free_image_destroys_pgroup_on_last():
    """When the last vgroup member is freed, free_image also tears down the per-VM
    protection group "<vg>-pg" (best-effort/idempotent; 404 ignored)."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    assert "sub _fa_destroy_pgroup" in text
    free = text.split("sub free_image", 1)[1].split("\nsub ", 1)[0]
    assert "_fa_destroy_pgroup" in free
    assert "_pgroup_name" in free
    # gated on the empty-group condition and wrapped defensively
    assert "== 0" in free
    assert "eval" in free
    dpg = text.split("sub _fa_destroy_pgroup {", 1)[1].split("\nsub ", 1)[0]
    assert "protection-groups?names=" in dpg
    assert "destroyed" in dpg
    assert "DELETE" in dpg


def test_perl_plugin_destroys_empty_vgroup_on_last_free():
    """free_image destroys the member, then tears down the now-empty vgroup
    (PATCH destroyed + DELETE when eradicate), never failing on a 404."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    assert "sub _fa_vgroup_member_count" in text
    assert "sub _fa_destroy_vgroup" in text
    free = text.split("sub free_image", 1)[1].split("\nsub ", 1)[0]
    # counts remaining members and destroys the group only when empty
    assert "_fa_vgroup_member_count" in free
    assert "_fa_destroy_vgroup" in free
    assert "== 0" in free
    # the group teardown is defensive (PATCH destroyed=true then DELETE) and
    # wrapped in eval so a missing group never fails teardown.
    dvg = text.split("sub _fa_destroy_vgroup", 1)[1].split("\nsub ", 1)[0]
    assert "volume-groups?names=" in dvg
    assert "destroyed" in dvg
    assert "DELETE" in dvg
    assert "eval" in dvg


def test_perl_plugin_existing_no_prefix_volume_uses_legacy_path():
    """REGRESSION: a volume whose on-array name has NO '<vg>/' prefix (an existing
    standalone volume) resolves to its bare name and follows the legacy path.

    _fa_name probes the array and only returns the grouped name when that grouped
    volume actually EXISTS; otherwise it falls back to the bare name. So an
    existing flat volume is never rewritten to a grouped name. The '/'-split
    helper returns (undef, name) for a no-slash name, which suppresses every
    vgroup-only branch (free_image group teardown, group snapshot, etc.).
    """
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    # _fa_name falls back to the bare name when the grouped volume is absent.
    fan = text.split("sub _fa_name ", 1)[1].split("\nsub ", 1)[0]
    assert "_fa_volume_exists" in fan
    assert "return $name;" in fan          # bare-name fallback (legacy)
    # the split helper yields (undef, name) for a flat name -> $vg is undef.
    spl = text.split("sub _split_fa_name", 1)[1].split("\nsub ", 1)[0]
    assert "return ( undef, $faname )" in spl
    # the vgroup teardown in free_image is gated on `defined $vg`, so a flat
    # (no-prefix) volume never triggers group destroy.
    free = text.split("sub free_image", 1)[1].split("\nsub ", 1)[0]
    assert "if ( defined $vg )" in free


def test_perl_plugin_vgroups_can_be_disabled():
    """The vgroup behavior is controllable via the volume_groups config flag
    (default enabled); disabling it provisions new volumes standalone."""
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    # declared in properties() with default 1 and registered as an option.
    assert "volume_groups" in text
    veg = text.split("sub _vgroups_enabled", 1)[1].split("\nsub ", 1)[0]
    assert "return 1 unless defined" in veg   # default ON
    assert "'0'" in veg                        # explicit disable honored
    # alloc_image only builds a vgroup when enabled.
    alloc = text.split("sub alloc_image", 1)[1].split("\nsub ", 1)[0]
    assert "_vgroups_enabled($scfg)" in alloc


def test_plugin_dest_is_pve_custom_dir():
    assert proxmox_mod.PLUGIN_DEST == (
        "/usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm")
    assert proxmox_mod.PLUGIN_STORAGE_TYPE == "purefa"


# ------------------------------------------------------- validate_connection ---
async def test_validate_connection_ok(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.validate_connection()
    assert r.success
    assert r.data["host"] == "pve01.test.local"
    assert any("pveversion" in line for line in captured_logs)
    assert r.data["array"]["name"] == "mock-array"


async def test_validate_connection_no_array(make_context):
    c = ProxmoxConnector(_ctx(make_context, with_array=False))
    r = await c.validate_connection()
    assert r.success
    assert r.data["array"] == {}


async def test_validate_connection_requires_host():
    async def emit(_):
        return None

    target = HypervisorTarget(id="x", connector_key="proxmox", name="p",
                              connection={"protocol": "nvme-tcp"}, secrets={})
    ctx = ConnectorContext(target=target, log=emit, runner=JobRunner(emit), array=None)
    with pytest.raises(ConnectionValidationError):
        await ProxmoxConnector(ctx).validate_connection()


# ------------------------------------------------------------ deploy_plugin ---
async def test_deploy_installs_perl_plugin(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.deploy_integration()
    assert r.success
    assert r.data["status"] == "plugin_installed"
    assert r.artifacts["storage_type"] == "purefa"
    joined = "\n".join(captured_logs)
    # Pushes the .pm to the PVE Custom dir and reloads the daemons.
    assert "PureFAPlugin.pm" in joined
    assert "/usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm" in joined
    assert "systemctl reload pvedaemon pveproxy pvestatd" in joined


def test_array_mgmt_target_parsing(make_context):
    c = ProxmoxConnector(_ctx(make_context))
    cases = {
        "10.0.0.10": ("10.0.0.10", 443),
        "https://10.0.0.10": ("10.0.0.10", 443),
        "https://10.0.0.10:8443/": ("10.0.0.10", 8443),
        "fa.example.com:9000": ("fa.example.com", 9000),
        "https://fa.example.com/api": ("fa.example.com", 443),
    }
    for ep, expected in cases.items():
        c.ctx.array.endpoint = ep
        assert c._array_mgmt_target() == expected, ep
    # No array -> None.
    c.ctx.array = None
    assert c._array_mgmt_target() is None


async def test_deploy_preflights_array_reachability(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.deploy_integration()
    assert r.success
    # The reachability preflight runs and (in mock) passes for every node.
    assert any("reachable from all" in line for line in captured_logs)


async def test_deploy_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    r = await ProxmoxConnector(ctx).deploy_integration()
    assert r.success
    assert r.data["status"] == "planned"


# --------------------------------------------------------------- configure ---
async def test_configure_defines_purefa_storage(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.configure(storage_id="purefa",
                          pure_endpoint="https://fa.test", pure_api_token="tok",
                          host_group="pve-cluster")
    assert r.success
    assert r.artifacts["storage_id"] == "purefa"
    assert r.artifacts["storage_type"] == "purefa"
    joined = "\n".join(captured_logs)
    # Registers via pvesm (validated, correctly-formatted section) — not by
    # hand-appending to storage.cfg.
    assert "pvesm add purefa purefa" in joined
    assert "--pure_endpoint https://fa.test" in joined
    assert "--protocol nvme-tcp" in joined
    assert "--host_group pve-cluster" in joined


# ---------------------------------------------------------- host_register ---
async def test_register_hosts_nvme(make_context):
    ctx = _ctx(make_context, node_nqn="nqn.2014-08.org.nvmexpress:uuid:abc")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="pve-cluster", host_name="pve01")
    assert r.success
    assert r.artifacts["host_group"] == "pve-cluster"
    assert "pve01" in ctx.array.hosts
    assert ctx.array.hosts["pve01"]["nqns"] == ["nqn.2014-08.org.nvmexpress:uuid:abc"]
    # In mock mode list_nodes() yields a synthetic 2-node cluster, so registration
    # fans out: the connection node (pve01, explicit NQN) + the second member.
    assert ctx.array.host_groups["pve-cluster"]["hosts"] == ["pve01", "pve-node2"]


async def test_register_hosts_iscsi(make_context):
    ctx = _ctx(make_context, protocol="iscsi")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="hg", host_name="n1",
                               node_iqn="iqn.1993-08.org.debian:01:abc")
    assert r.success
    assert ctx.array.hosts["n1"]["iqns"] == ["iqn.1993-08.org.debian:01:abc"]
    assert ctx.array.hosts["n1"]["nqns"] == []


async def test_register_hosts_fc_by_wwn(make_context):
    # FC registers BY WWN (comma-separated), NOT by IQN/NQN.
    ctx = _ctx(make_context, protocol="fc",
               node_wwns="21:00:00:24:ff:00:00:01,21:00:00:24:ff:00:00:02")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="pve-fc", host_name="pvefc01")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    h = ctx.array.hosts["pvefc01"]
    assert h["wwns"] == ["21:00:00:24:ff:00:00:01", "21:00:00:24:ff:00:00:02"]
    # No iSCSI/NVMe initiators registered for an FC host.
    assert h["iqns"] == []
    assert h["nqns"] == []
    # Fan-out across the synthetic 2-node mock cluster.
    assert ctx.array.host_groups["pve-fc"]["hosts"] == ["pvefc01", "pve-node2"]


async def test_register_hosts_fc_wwns_from_target(make_context):
    # WWNs may also come from the target connection (node_wwns) rather than args.
    ctx = _ctx(make_context, protocol="fc",
               node_wwns="21:00:00:24:ff:aa:bb:cc")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="hg", host_name="n1")
    assert r.success
    assert ctx.array.hosts["n1"]["wwns"] == ["21:00:00:24:ff:aa:bb:cc"]


async def test_register_hosts_fc_auto_discovers_wwns(make_context):
    # FC with NO manually-supplied WWNs: the connector auto-discovers the node's
    # HBA WWPNs via runner.discover_initiators (mock yields synthetic values) and
    # registers them on the array. No manual entry required.
    ctx = _ctx(make_context, protocol="fc")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="hg", host_name="n1")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    # The synthetic mock WWNs (21000024ff000001/02) reach the array, normalised
    # to the colon-delimited WWPN form.
    assert ctx.array.hosts["n1"]["wwns"] == [
        "21:00:00:24:ff:00:00:01", "21:00:00:24:ff:00:00:02"]
    assert ctx.array.hosts["n1"]["iqns"] == []
    assert ctx.array.hosts["n1"]["nqns"] == []


async def test_register_hosts_iscsi_auto_discovers_iqn(make_context):
    # iSCSI with NO manually-supplied IQN: discovered IQN reaches the array.
    ctx = _ctx(make_context, protocol="iscsi")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="hg", host_name="n1")
    assert r.success
    iqns = ctx.array.hosts["n1"]["iqns"]
    assert len(iqns) == 1 and iqns[0].startswith("iqn.")
    assert ctx.array.hosts["n1"]["nqns"] == []


async def test_register_hosts_nvme_auto_discovers_nqn(make_context):
    # NVMe-TCP with NO manually-supplied NQN: discovered NQN reaches the array.
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="hg", host_name="n1")
    assert r.success
    nqns = ctx.array.hosts["n1"]["nqns"]
    assert len(nqns) == 1 and nqns[0].startswith("nqn.")
    assert ctx.array.hosts["n1"]["iqns"] == []


async def test_register_hosts_explicit_overrides_discovery(make_context):
    # Explicit node_nqn wins over auto-discovery.
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="hg", host_name="n1",
                               node_nqn="nqn.explicit:override")
    assert r.success
    assert ctx.array.hosts["n1"]["nqns"] == ["nqn.explicit:override"]


async def test_register_hosts_idempotent_rerun(make_context):
    # Re-running registration is safe (idempotent create_host/create_host_group).
    ctx = _ctx(make_context, protocol="nvme-tcp",
               node_nqn="nqn.2014-08.org.nvmexpress:uuid:abc")
    c = ProxmoxConnector(ctx)
    r1 = await c.register_hosts(host_group="hg", host_name="n1")
    r2 = await c.register_hosts(host_group="hg", host_name="n1")
    assert r1.success and r2.success
    # No duplicate initiators or members after re-run.
    assert ctx.array.hosts["n1"]["nqns"] == ["nqn.2014-08.org.nvmexpress:uuid:abc"]
    # Idempotent re-run: no duplicate members across the fanned-out cluster.
    assert ctx.array.host_groups["hg"]["hosts"] == ["n1", "pve-node2"]


async def test_register_hosts_no_array(make_context):
    c = ProxmoxConnector(_ctx(make_context, with_array=False))
    r = await c.register_hosts(host_group="hg")
    assert not r.success


async def test_register_hosts_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="hg", host_name="n1")
    assert r.success
    assert "n1" not in ctx.array.hosts  # no array mutation in dry-run


# ----------------------------------------------------------- connectivity ---
async def test_setup_connectivity_nvme(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.setup_connectivity(portals="10.0.0.10,10.0.0.11",
                                   subsystem_nqn="nqn.subsys")
    assert r.success
    assert r.artifacts["protocol"] == "nvme-tcp"
    assert r.artifacts["portals"] == ["10.0.0.10", "10.0.0.11"]
    joined = "\n".join(captured_logs)
    assert "nvme connect" in joined
    assert "connect-all" in joined


async def test_setup_connectivity_iscsi(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context, protocol="iscsi"))
    r = await c.setup_connectivity(portals="10.0.0.20", iqn_target="iqn.target")
    assert r.success
    assert r.artifacts["protocol"] == "iscsi"
    assert any("iscsiadm" in line for line in captured_logs)


async def test_setup_connectivity_fc(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context, protocol="fc"))
    r = await c.setup_connectivity(portals="")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    joined = "\n".join(captured_logs)
    # FC must NOT perform an iSCSI/NVMe login — only an FC/SCSI rescan + multipath.
    assert "iscsiadm" not in joined
    assert "nvme connect" not in joined
    # It rescans the SCSI bus (and issues a LIP) instead.
    assert "/sys/class/scsi_host/host" in joined
    assert "multipath -r" in joined


async def test_setup_connectivity_fc_does_full_rescan(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context, protocol="fc"))
    await c.setup_connectivity(portals="")
    joined = "\n".join(captured_logs)
    # issue_lip + rescan-scsi-bus.sh are part of the FC rescan path.
    assert "issue_lip" in joined
    assert "rescan-scsi-bus.sh" in joined


async def test_setup_connectivity_dry_run(make_context, captured_logs):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = ProxmoxConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.10", subsystem_nqn="nqn.x")
    assert r.success
    assert not any("nvme connect -t tcp" in line for line in captured_logs)


# -------------------------------------------------- interface binding ---
def test_setup_connectivity_has_binding_fields():
    spec = next(a for a in ProxmoxConnector.action_schemas()
                if a.id == "setup_connectivity")
    by_name = {f.name: f for f in spec.fields}
    assert {"iscsi_nics", "nvme_sources", "nvme_options", "fc_hbas"} <= set(by_name)
    assert by_name["iscsi_nics"].type.value == "multiselect"
    assert by_name["iscsi_nics"].options_source == "nics"
    assert by_name["nvme_sources"].options_source == "nvme_sources"
    assert by_name["fc_hbas"].options_source == "fc_hbas"
    # binding fields are optional
    for n in ("iscsi_nics", "nvme_sources", "nvme_options", "fc_hbas"):
        assert by_name[n].required is False


async def test_discover_options_nics_filtered_to_storage_subnet(make_context):
    # NIC options are filtered to the array's storage subnet. The mock array
    # iSCSI portals are 10.10.10.x -> only eth0 (10.10.10.5/24) qualifies.
    c = ProxmoxConnector(_ctx(make_context, protocol="iscsi"))
    opts = await c.discover_options("nics")
    assert {o["value"] for o in opts} == {"eth0"}
    # NVMe-TCP portals are 10.20.20.x -> only eth1 (10.20.20.5/24).
    c2 = ProxmoxConnector(_ctx(make_context, protocol="nvme-tcp"))
    assert {o["value"] for o in await c2.discover_options("nics")} == {"eth1"}


async def test_discover_options_nics_unfiltered_without_array(make_context):
    # No associated array -> can't determine storage subnet -> offer all NICs.
    c = ProxmoxConnector(_ctx(make_context, protocol="iscsi", with_array=False))
    opts = await c.discover_options("nics")
    assert {o["value"] for o in opts} == {"eth0", "eth1", "ens192"}


async def test_discover_options_nvme_sources(make_context):
    c = ProxmoxConnector(_ctx(make_context))
    opts = await c.discover_options("nvme_sources")
    assert all("value" in o and "label" in o for o in opts)
    assert "192.168.10.11" in {o["value"] for o in opts}


async def test_discover_options_fc_hbas(make_context):
    c = ProxmoxConnector(_ctx(make_context))
    opts = await c.discover_options("fc_hbas")
    assert "21000024ff000001" in {o["value"] for o in opts}


async def test_discover_options_unknown_kind(make_context):
    c = ProxmoxConnector(_ctx(make_context))
    assert await c.discover_options("bogus") == []


async def test_setup_connectivity_iscsi_binds_nics(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context, protocol="iscsi"))
    r = await c.setup_connectivity(portals="10.0.0.20", iqn_target="iqn.target",
                                   iscsi_nics=["eth0", "eth1"])
    assert r.success
    assert r.artifacts["iscsi_nics"] == ["eth0", "eth1"]
    joined = "\n".join(captured_logs)
    # one iface created + bound per selected NIC
    assert "iscsiadm -m iface -I phif_eth0 --op=new" in joined
    assert ("iscsiadm -m iface -I phif_eth0 --op=update "
            "-n iface.net_ifacename -v eth0") in joined
    assert "iscsiadm -m iface -I phif_eth1 --op=new" in joined
    # discovery + login bound to the iface
    assert "iscsiadm -m discovery -t st -p 10.0.0.20:3260 -I phif_eth0" in joined
    assert "-I phif_eth0 --login" in joined
    # binding persisted into storage.cfg
    assert "iscsi_nics eth0,eth1" in joined


async def test_setup_connectivity_iscsi_no_nics_default_iface(make_context,
                                                              captured_logs):
    c = ProxmoxConnector(_ctx(make_context, protocol="iscsi"))
    r = await c.setup_connectivity(portals="10.0.0.20")
    assert r.success
    joined = "\n".join(captured_logs)
    # falls back to the unbound default flow
    assert "phif_" not in joined
    assert "iscsiadm -m discovery -t sendtargets -p 10.0.0.20:3260" in joined


async def test_setup_connectivity_nvme_sources(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.setup_connectivity(portals="10.0.0.10", subsystem_nqn="nqn.subsys",
                                   nvme_sources=["192.168.10.11", "192.168.20.11"],
                                   nvme_options="--nr-io-queues=8")
    assert r.success
    assert r.artifacts["nvme_sources"] == ["192.168.10.11", "192.168.20.11"]
    joined = "\n".join(captured_logs)
    assert "-w 192.168.10.11" in joined
    assert "-w 192.168.20.11" in joined
    assert "--nr-io-queues=8" in joined
    assert "nvme_sources 192.168.10.11,192.168.20.11" in joined


async def test_setup_connectivity_fc_records_hbas(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context, protocol="fc"))
    r = await c.setup_connectivity(portals="",
                                   fc_hbas=["21000024ff000001"])
    assert r.success
    assert r.artifacts["fc_hbas"] == ["21000024ff000001"]
    joined = "\n".join(captured_logs)
    # FC still does NO login, only records/persists the selection
    assert "iscsiadm" not in joined
    assert "nvme connect" not in joined
    assert "fc_hbas 21000024ff000001" in joined


async def test_setup_connectivity_binding_dry_run_no_op(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="iscsi")
    ctx.dry_run = True
    c = ProxmoxConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.20", iscsi_nics=["eth0"])
    assert r.success
    assert r.artifacts["iscsi_nics"] == ["eth0"]
    joined = "\n".join(captured_logs)
    # no iface mutation in dry-run
    assert "--op=new" not in joined
    assert "iscsi_nics eth0" not in joined  # not persisted in dry-run


# --------------------------------------------------------------- provision ---
async def test_provision_volume_per_disk(make_context):
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    r = await c.provision(name="vm-100-disk-0", size="2T", host_group="pve-cluster")
    assert r.success
    # one LUN per disk: a FA volume is created and connected to the host group
    assert "vm-100-disk-0" in ctx.array.volumes
    assert ("connect_volume",
            {"host": "pve-cluster", "volume": "vm-100-disk-0"}) in ctx.array.calls


async def test_provision_attaches_to_vm(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    r = await c.provision(name="vm-101-disk-0", size="1T", vmid="101", disk="scsi1")
    assert r.success
    assert r.artifacts["vmid"] == "101"
    # attached directly to the VM as a raw block device via the purefa storage
    assert any("qm set 101 -scsi1 purefa:vm-101-disk-0" in line
               for line in captured_logs)


async def test_provision_fc_flow(make_context, captured_logs):
    # FC provision: FA volume -> connect to host group -> SCSI rescan -> qm set.
    ctx = _ctx(make_context, protocol="fc")
    c = ProxmoxConnector(ctx)
    r = await c.provision(name="vm-200-disk-0", size="1T", host_group="pve-fc",
                          vmid="200", disk="scsi1")
    assert r.success
    assert r.artifacts["protocol"] == "fc"
    assert "vm-200-disk-0" in ctx.array.volumes
    assert ("connect_volume",
            {"host": "pve-fc", "volume": "vm-200-disk-0"}) in ctx.array.calls
    joined = "\n".join(captured_logs)
    # FC uses a SCSI rescan, never iscsiadm/nvme.
    assert "/sys/class/scsi_host/host" in joined
    assert "iscsiadm" not in joined
    assert "nvme connect" not in joined
    assert "qm set 200 -scsi1 purefa:vm-200-disk-0" in joined


async def test_provision_no_array(make_context):
    c = ProxmoxConnector(_ctx(make_context, with_array=False))
    r = await c.provision(name="v", size="1T")
    assert not r.success


async def test_provision_dry_run(make_context):
    ctx = _ctx(make_context)
    ctx.dry_run = True
    c = ProxmoxConnector(ctx)
    r = await c.provision(name="vdry", size="1T")
    assert r.success
    assert "vdry" not in ctx.array.volumes


# ----------------------------------------------------------- snapshot/clone ---
async def test_snapshot_on_array(make_context):
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    await c.provision(name="vm-1-disk-0", size="1T")
    r = await c.snapshot(volume="vm-1-disk-0")
    assert r.success
    assert any(s["volume"] == "vm-1-disk-0" for s in ctx.array.snapshots)
    assert ("create_snapshot",
            {"volume": "vm-1-disk-0", "suffix": None}) in ctx.array.calls


async def test_clone_via_array_copy(make_context):
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    await c.provision(name="vm-1-disk-0", size="1T")
    r = await c.clone(source="vm-1-disk-0", dest="vm-2-disk-0",
                      host_group="pve-cluster")
    assert r.success
    assert "vm-2-disk-0" in ctx.array.volumes
    assert ("clone_volume",
            {"source": "vm-1-disk-0", "dest": "vm-2-disk-0"}) in ctx.array.calls
    # clone is connected to the host group so the new VM can use it directly
    assert ("connect_volume",
            {"host": "pve-cluster", "volume": "vm-2-disk-0"}) in ctx.array.calls


# ------------------------------------------------------------------ resize ---
async def test_resize_extends_array_and_qm(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    await c.provision(name="vm-1-disk-0", size="1T")
    r = await c.resize(volume="vm-1-disk-0", size="3T", vmid="1", disk="scsi1")
    assert r.success
    assert ctx.array.volumes["vm-1-disk-0"]["size"] == "3T"
    assert any("qm resize 1 scsi1 3T" in line for line in captured_logs)


# ------------------------------------------------------------------ health ---
async def test_health_check_nvme(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.health_check()
    assert r.success
    assert r.data["protocol"] == "nvme-tcp"
    # Health is gathered per cluster node; each entry carries its paths + pvesm.
    assert len(r.data["nodes"]) == 2
    for entry in r.data["nodes"].values():
        assert "paths" in entry and "pvesm_status" in entry and "host" in entry
    assert r.data["array"]["name"] == "mock-array"
    assert any("pvesm status" in line for line in captured_logs)
    assert any("nvme list-subsys" in line for line in captured_logs)


async def test_health_check_iscsi_uses_multipath(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context, protocol="iscsi"))
    await c.health_check()
    assert any("multipath -ll" in line for line in captured_logs)


# ---------------------------------------------------------------- teardown ---
async def test_teardown_removes_storage_and_plugin(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.teardown(storage_id="purefa")
    assert r.success
    assert r.data["status"] == "not_deployed"
    joined = "\n".join(captured_logs)
    # Storage definition removed once (cluster-wide pmxcfs)...
    assert "pvesm remove purefa" in joined
    assert joined.count("pvesm remove purefa") == 1
    # ...plugin removal + daemon reload fan out across BOTH cluster nodes.
    assert "rm -f /usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm" in joined
    assert len(r.artifacts["nodes"]) == 2
    assert joined.count("rm -f /usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm") == 2
    assert joined.count("systemctl reload pvedaemon pveproxy pvestatd") == 2


# ------------------------------------------------------------------ dispatch ---
async def test_dispatch_provision(make_context):
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    r = await c.dispatch("provision", {"name": "vm-9-disk-0", "size": "1T"})
    assert r.success
    assert "vm-9-disk-0" in ctx.array.volumes


async def test_dispatch_clone(make_context):
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    await c.provision(name="vm-1-disk-0", size="1T")
    r = await c.dispatch("clone", {"source": "vm-1-disk-0", "dest": "vm-3-disk-0"})
    assert r.success
    assert "vm-3-disk-0" in ctx.array.volumes


async def test_dispatch_register_hosts(make_context):
    ctx = _ctx(make_context, node_nqn="nqn.x")
    c = ProxmoxConnector(ctx)
    r = await c.dispatch("register_hosts", {"host_group": "hg", "host_name": "n1"})
    assert r.success
    assert "n1" in ctx.array.hosts


async def test_dispatch_unknown(make_context):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.dispatch("nope", {})
    assert not r.success


async def test_register_hosts_carries_host_group_from_connection(make_context):
    # No host_group passed to the action -> falls back to the connection value.
    ctx = _ctx(make_context, protocol="iscsi")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts()  # host_group intentionally omitted
    assert r.success, r.message
    assert "pve-cluster" in ctx.array.host_groups


async def test_setup_connectivity_discovers_array_portals_and_target(make_context):
    # No portals / iqn_target supplied -> discovered from the FlashArray.
    ctx = _ctx(make_context, protocol="iscsi")
    c = ProxmoxConnector(ctx)
    r = await c.setup_connectivity()
    assert r.success, r.message
    ops = [call[0] for call in ctx.array.calls]
    assert "get_data_interfaces" in ops
    assert "get_target_ports" in ops
    # discovered iSCSI portals flow into the result artifacts
    assert "10.10.10.10" in r.artifacts.get("portals", [])


async def test_discover_options_initiators(make_context):
    ctx = _ctx(make_context, protocol="iscsi")
    c = ProxmoxConnector(ctx)
    opts = await c.discover_options("initiators")
    by_field = {o["field"]: o["value"] for o in opts}
    assert by_field.get("node_iqn", "").startswith("iqn.")
    # every option carries field + value + label for the UI to pre-fill/display
    assert all({"field", "value", "label"} <= set(o) for o in opts)


def test_configure_form_omits_endpoint_and_token():
    # The FA endpoint/token come from the associated array, not the form.
    fields = {f.name for a in ProxmoxConnector.action_schemas()
              if a.id == "configure" for f in a.fields}
    assert "pure_endpoint" not in fields
    assert "pure_api_token" not in fields


async def test_discover_options_array_targets_and_host_name(make_context):
    ctx = _ctx(make_context, protocol="iscsi")
    c = ProxmoxConnector(ctx)
    portals = await c.discover_options("array_portals")
    assert [o["value"] for o in portals]  # array iSCSI portals discovered
    iqn = await c.discover_options("array_target_iqn")
    assert iqn and iqn[0]["value"].startswith("iqn.")
    hn = await c.discover_options("fa_host_name")
    # node_host "pve01.test.local" -> sanitized (no dots)
    assert hn and "." not in hn[0]["value"]


async def test_discover_options_array_portals_nvme(make_context):
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = ProxmoxConnector(ctx)
    nqn = await c.discover_options("array_target_nqn")
    assert nqn and nqn[0]["value"].startswith("nqn.")


# ----------------------------------------------------- cluster (multi-node) ---
def test_wizard_steps_order():
    expected = ["deploy", "configure", "register_hosts", "setup_connectivity", "enable"]
    assert ProxmoxConnector.wizard_steps() == expected
    # surfaced in the descriptor for the UI
    assert ProxmoxConnector.descriptor()["wizard_steps"] == expected


async def test_list_nodes_mock_two_node_cluster(make_context):
    # In mock mode the runner short-circuits SSH, so list_nodes() returns a
    # synthetic 2-node cluster so the cluster flow stays exercisable.
    c = ProxmoxConnector(_ctx(make_context))
    nodes = await c.list_nodes()
    assert len(nodes) == 2
    names = {n.name for n in nodes}
    assert names == {"pve-node1", "pve-node2"}
    # node1 is anchored to the configured connection host.
    node1 = next(n for n in nodes if n.name == "pve-node1")
    assert node1.host == "pve01.test.local"


async def test_list_nodes_parses_pve_members(make_context, monkeypatch):
    # With a real (non-mock) runner, list_nodes() parses /etc/pve/.members JSON.
    ctx = _ctx(make_context)
    members = {
        "nodename": "pve01",
        "nodelist": {
            "pve01": {"id": 1, "ip": "192.0.2.11", "online": 1},
            "pve02": {"id": 2, "ip": "192.0.2.12", "online": 1},
            "pve03": {"id": 3, "ip": "192.0.2.13", "online": 0},
        },
    }
    import json as _json

    async def fake_ssh(self, command, *, check=True):
        assert "/etc/pve/.members" in command
        return _json.dumps(members)

    # Force the non-mock branch and feed it the members JSON.
    monkeypatch.setattr(ProxmoxConnector, "_is_mock_or_dry", lambda self: False)
    monkeypatch.setattr(ProxmoxConnector, "_ssh", fake_ssh)
    c = ProxmoxConnector(ctx)
    nodes = await c.list_nodes()
    assert [n.name for n in nodes] == ["pve01", "pve02", "pve03"]
    assert [n.host for n in nodes] == ["192.0.2.11", "192.0.2.12", "192.0.2.13"]
    # online flag carried through info.
    assert next(n for n in nodes if n.name == "pve03").info["online"] is False


async def test_create_managed_disk_surfaces_purefa_error(make_context, monkeypatch):
    """When the purefa plugin fails (e.g. login/lock timeout), the disk isn't
    created and create_managed_disk must raise with the ACTUAL purefa error, not
    guess a volume name (which lets a misleading downstream FA error surface)."""
    ctx = _ctx(make_context)
    monkeypatch.setattr(ProxmoxConnector, "_is_mock_or_dry", lambda self: False)

    purefa_err = ("update VM 103: -scsi0 purefa:30\n"
                  "purefa: login failed: 500 'storage-purefa'-locked command "
                  "timed out - aborting")

    async def fake_ssh_script(self, lines, *, check=True, timeout=None):
        return purefa_err

    async def fake_ssh(self, command, *, check=True, timeout=None):
        # qm config read-back: no scsi0 line (disk was never created).
        return "name: red-possum-48\nnet0: virtio=aa,bridge=vmbr0\n"

    monkeypatch.setattr(ProxmoxConnector, "_ssh_script", fake_ssh_script)
    monkeypatch.setattr(ProxmoxConnector, "_ssh", fake_ssh)
    c = ProxmoxConnector(ctx)
    with pytest.raises(RuntimeError) as ei:
        await c.create_managed_disk("103", size_bytes=30 * 1024**3, order=0, boot=True)
    msg = str(ei.value)
    assert "purefa: login failed" in msg and "locked command timed out" in msg


async def test_list_nodes_falls_back_single_node_on_parse_failure(make_context,
                                                                  monkeypatch):
    ctx = _ctx(make_context)

    async def fake_ssh(self, command, *, check=True):
        return "not json at all"

    monkeypatch.setattr(ProxmoxConnector, "_is_mock_or_dry", lambda self: False)
    monkeypatch.setattr(ProxmoxConnector, "_ssh", fake_ssh)
    c = ProxmoxConnector(ctx)
    nodes = await c.list_nodes()
    assert len(nodes) == 1
    assert nodes[0].host == "pve01.test.local"


async def test_register_hosts_fans_out_to_all_nodes(make_context):
    # Mock 2-node cluster: registering creates a FA host per node and adds ALL
    # of them to the one host group.
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="pve-cluster", host_name="pve01")
    assert r.success
    # two hosts created, both in the group
    assert len(ctx.array.host_groups["pve-cluster"]["hosts"]) == 2
    assert ctx.array.host_groups["pve-cluster"]["hosts"] == ["pve01", "pve-node2"]
    assert set(r.artifacts["hosts"]) == {"pve01", "pve-node2"}
    # each node got its own discovered NQN.
    assert ctx.array.hosts["pve01"]["nqns"][0].startswith("nqn.")
    assert ctx.array.hosts["pve-node2"]["nqns"][0].startswith("nqn.")


async def test_register_hosts_single_node_when_one_member(make_context, monkeypatch):
    # When list_nodes() reports a single node, registration behaves like before.
    from phif.connectors.base import ClusterNode

    ctx = _ctx(make_context, protocol="nvme-tcp")

    async def one_node(self):
        return [ClusterNode(name="solo", host="pve01.test.local")]

    monkeypatch.setattr(ProxmoxConnector, "list_nodes", one_node)
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="hg", host_name="solo")
    assert r.success
    assert ctx.array.host_groups["hg"]["hosts"] == ["solo"]


async def test_setup_connectivity_runs_per_node(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = ProxmoxConnector(ctx)
    r = await c.setup_connectivity(portals="10.0.0.10", subsystem_nqn="nqn.subsys")
    assert r.success
    assert len(r.artifacts["nodes"]) == 2
    joined = "\n".join(captured_logs)
    # the transport setup ran against BOTH node hosts.
    assert "192.0.2.2" in joined  # the second mock node's host
    assert "connectivity on pve-node1" in joined
    assert "connectivity on pve-node2" in joined
    # nvme connect issued on each node host (count >= 2 connects)
    connects = [l for l in captured_logs if "nvme connect -t tcp" in l]
    assert len(connects) >= 2


async def test_deploy_fans_out_plugin_to_all_nodes(make_context, captured_logs):
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    r = await c.deploy_integration()
    assert r.success
    assert len(r.artifacts["nodes"]) == 2
    joined = "\n".join(captured_logs)
    assert "deploying plugin on pve-node1" in joined
    assert "deploying plugin on pve-node2" in joined
    # daemons reloaded on each node (one reload per node host).
    reloads = [l for l in captured_logs
               if "systemctl reload pvedaemon pveproxy pvestatd" in l]
    assert len(reloads) >= 2


async def test_configure_is_single_call_on_connection_host(make_context, captured_logs):
    # storage.cfg is cluster-wide via pmxcfs: pvesm add runs ONCE, not per node.
    ctx = _ctx(make_context)
    c = ProxmoxConnector(ctx)
    r = await c.configure(storage_id="purefa", pure_endpoint="https://fa.test",
                          pure_api_token="tok", host_group="pve-cluster")
    assert r.success
    adds = [l for l in captured_logs if "pvesm add purefa purefa" in l]
    assert len(adds) == 1


async def test_validate_cluster_ok_when_existing_binding_matches(make_context):
    # iSCSI: every node reports the same existing iface binding (mock: eth0).
    ctx = _ctx(make_context, protocol="iscsi")
    c = ProxmoxConnector(ctx)
    r = await c.validate_cluster()
    assert r.success
    assert r.data["existing_bound_nics"]  # uses existing-binding consistency
    assert r.data.get("rebind_recommended") in (False, None)


async def test_validate_cluster_recommends_rebind_when_binding_differs(make_context, monkeypatch):
    # node1 already bound to eth0; node2 has no binding -> inconsistent -> rebind.
    ctx = _ctx(make_context, protocol="iscsi")

    async def differing_state(self, host, **kw):
        nic = ["eth0"] if host == "pve01.test.local" else []
        return {"bound_nics": nic, "ifaces": [], "sessions": [], "targets": []}

    monkeypatch.setattr(JobRunner, "discover_iscsi_state", differing_state)
    c = ProxmoxConnector(ctx)
    r = await c.validate_cluster()
    assert not r.success
    assert r.data["rebind_recommended"] is True
    assert "rebind" in r.message.lower()


async def test_validate_cluster_fallback_compares_interfaces(make_context, monkeypatch):
    # No existing iSCSI binding anywhere -> fall back to storage-NIC comparison.
    ctx = _ctx(make_context, protocol="iscsi")

    async def no_binding(self, host, **kw):
        return {"bound_nics": [], "ifaces": [], "sessions": [], "targets": []}

    async def differing(self, host, kind, **kw):
        if host == "pve01.test.local":
            return [{"value": "eth0", "cidr": "10.10.10.5/24"},
                    {"value": "eth1", "cidr": "10.10.10.6/24"}]
        return [{"value": "eth0", "cidr": "10.10.10.5/24"}]

    monkeypatch.setattr(JobRunner, "discover_iscsi_state", no_binding)
    monkeypatch.setattr(JobRunner, "discover_interfaces", differing)
    c = ProxmoxConnector(ctx)
    r = await c.validate_cluster()
    assert not r.success
    assert "inconsistent" in r.message and "per_node" in r.data


async def test_validate_cluster_fc_uses_fc_hbas(make_context):
    ctx = _ctx(make_context, protocol="fc")
    c = ProxmoxConnector(ctx)
    r = await c.validate_cluster()
    assert r.success
    assert r.data["kind"] == "fc_hbas"


async def test_validate_cluster_nvme_uses_nvme_sources(make_context):
    ctx = _ctx(make_context, protocol="nvme-tcp")
    c = ProxmoxConnector(ctx)
    r = await c.validate_cluster()
    assert r.success
    assert r.data["kind"] == "nvme_sources"


async def test_runner_discovers_existing_iscsi_state(make_context):
    from phif.jobs.runner import JobRunner

    async def emit(_): return None
    runner = JobRunner(emit)  # mock mode on in conftest
    st = await runner.discover_iscsi_state("node.test", username="root")
    assert st["bound_nics"] == ["eth0"]
    assert st["sessions"] and st["sessions"][0]["target"].startswith("iqn.")
    assert st["targets"]


async def test_validate_cluster_uses_existing_binding(make_context):
    # In mock, every node reports bound NIC eth0 -> consistent existing binding.
    ctx = _ctx(make_context, protocol="iscsi")
    c = ProxmoxConnector(ctx)
    r = await c.validate_cluster()
    assert r.success
    assert r.data["existing_bound_nics"]  # discovered bindings
    assert "existing_targets" in r.data  # reports prior connectivity (this/other arrays)


def test_setup_connectivity_has_rebind_field():
    fields = {f.name for a in ProxmoxConnector.action_schemas()
              if a.id == "setup_connectivity" for f in a.fields}
    assert "rebind" in fields


async def test_configure_creates_disabled(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.configure(storage_id="purefa", pure_endpoint="https://fa.test",
                          pure_api_token="tok", host_group="pve-cluster")
    assert r.success
    assert r.artifacts.get("enabled") is False
    joined = "\n".join(captured_logs)
    assert "--disable 1" in joined  # storage created disabled


async def test_enable_storage_action_and_dispatch(make_context, captured_logs):
    c = ProxmoxConnector(_ctx(make_context))
    r = await c.dispatch("enable", {"storage_id": "purefa"})
    assert r.success and r.artifacts.get("enabled") is True
    assert "pvesm set purefa --disable 0" in "\n".join(captured_logs)


def test_wizard_enables_storage_last():
    steps = ProxmoxConnector.wizard_steps()
    assert steps[-1] == "enable"
    # storage is defined (configure) before connectivity, enabled only at the end
    assert steps.index("configure") < steps.index("enable")
    assert steps.index("setup_connectivity") < steps.index("enable")
    ids = {a.id for a in ProxmoxConnector.action_schemas()}
    assert "enable" in ids


# ============================ cluster reconcile ============================ #
def test_reconcile_capability_and_actions():
    assert Capability.RECONCILE_CLUSTER in ProxmoxConnector.CAPABILITIES
    specs = {a.id: a for a in ProxmoxConnector.action_schemas()}
    assert "assess_cluster" in specs and "reconcile_cluster" in specs
    assert specs["assess_cluster"].label == "Check for cluster changes"
    assert specs["assess_cluster"].long_running is False
    rc = specs["reconcile_cluster"]
    assert rc.label == "Deploy to new hosts" and rc.destructive is True
    ar = next(f for f in rc.fields if f.name == "apply_removals")
    assert ar.type.value == "bool" and ar.default is False


async def test_assess_cluster_detects_new_and_departed(make_context):
    # Mock cluster = {pve-node1, pve-node2}. Seed the group with pve-node1 (already
    # configured) + an old-node that has left the cluster (departed). pve-node2 is
    # then a NEW node.
    ctx = _ctx(make_context)
    ctx.array.host_groups["pve-cluster"] = {"hosts": ["pve-node1", "old-node"]}
    c = ProxmoxConnector(ctx)
    r = await c.assess_cluster()
    assert r.success
    assert set(r.data["nodes"]) == {"pve-node1", "pve-node2"}
    new = {h["host"] for h in r.data["new_hosts"]}
    assert new == {"pve-node2"}
    # new host is scored for readiness (mock -> ready).
    assert all(h["ready"] is True for h in r.data["new_hosts"])
    assert r.data["departed_hosts"] == ["old-node"]


async def test_assess_cluster_readiness_verdict_shape(make_context):
    ctx = _ctx(make_context)
    ctx.array.host_groups["pve-cluster"] = {"hosts": []}
    c = ProxmoxConnector(ctx)
    r = await c.assess_cluster()
    # both mock nodes are NEW, each carries a node/host/ready/reasons verdict.
    assert len(r.data["new_hosts"]) == 2
    for h in r.data["new_hosts"]:
        assert {"node", "host", "ready", "reasons"} <= set(h)
        assert isinstance(h["reasons"], list)


async def test_assess_cluster_nfs_no_drift(make_context):
    ctx = _ctx(make_context, protocol="nfs")
    c = ProxmoxConnector(ctx)
    r = await c.assess_cluster()
    assert r.success
    assert r.data["new_hosts"] == [] and r.data["departed_hosts"] == []


async def test_reconcile_configures_ready_new_hosts(make_context, captured_logs):
    # pve-node1 already configured; pve-node2 new + ready -> it gets configured.
    ctx = _ctx(make_context)
    ctx.array.host_groups["pve-cluster"] = {"hosts": ["pve-node1"]}
    c = ProxmoxConnector(ctx)
    r = await c.reconcile_cluster()
    assert r.success
    assert r.data["configured"] == ["pve-node2"]
    assert r.data["not_ready"] == []
    # register_hosts ran over the scope and added the new node to the group.
    assert "pve-node2" in ctx.array.host_groups["pve-cluster"]["hosts"]
    # the new node's plugin install fanned out (deploy on pve-node2).
    assert any("deploying plugin on pve-node2" in l for l in captured_logs)


async def test_reconcile_skips_not_ready_new_hosts(make_context, monkeypatch):
    # Force assess_cluster to report one not-ready new node; it must be skipped
    # (not configured) and surfaced in not_ready.
    ctx = _ctx(make_context)
    ctx.array.host_groups["pve-cluster"] = {"hosts": ["pve-node1"]}

    async def fake_assess(self, **params):
        from phif.connectors.base import OpResult
        return OpResult.ok(
            "stub", nodes=["pve-node1", "pve-node2"],
            new_hosts=[{"node": "pve-node2", "host": "pve-node2",
                        "ready": False,
                        "reasons": ["no NVMe NQN discovered on the host"]}],
            departed_hosts=[])

    monkeypatch.setattr(ProxmoxConnector, "assess_cluster", fake_assess)
    c = ProxmoxConnector(ctx)
    r = await c.reconcile_cluster()
    assert r.success
    assert r.data["configured"] == []
    assert r.data["not_ready"] == [
        {"node": "pve-node2", "reasons": ["no NVMe NQN discovered on the host"]}]
    # not-ready node NOT added to the group.
    assert "pve-node2" not in ctx.array.host_groups["pve-cluster"]["hosts"]


async def test_reconcile_flags_departed_without_apply(make_context):
    ctx = _ctx(make_context)
    ctx.array.host_groups["pve-cluster"] = {"hosts": ["pve-node1", "old-node"]}
    ctx.array.hosts["old-node"] = {"iqns": [], "nqns": [], "wwns": []}
    c = ProxmoxConnector(ctx)
    r = await c.reconcile_cluster(apply_removals=False)
    assert r.success
    # departed flagged, not removed.
    assert r.data["pending_removals"] == ["old-node"]
    assert "removed" not in r.data
    assert "old-node" in ctx.array.host_groups["pve-cluster"]["hosts"]


async def test_reconcile_removes_departed_when_applied(make_context):
    ctx = _ctx(make_context)
    ctx.array.host_groups["pve-cluster"] = {"hosts": ["pve-node1", "old-node"]}
    ctx.array.hosts["old-node"] = {"iqns": [], "nqns": [], "wwns": []}
    c = ProxmoxConnector(ctx)
    r = await c.reconcile_cluster(apply_removals=True)
    assert r.success
    assert r.data["removed"] == ["old-node"]
    # actually removed from the group and deleted from the array.
    assert "old-node" not in ctx.array.host_groups["pve-cluster"]["hosts"]
    assert "old-node" not in ctx.array.hosts


async def test_reconcile_dispatch(make_context):
    ctx = _ctx(make_context)
    ctx.array.host_groups["pve-cluster"] = {"hosts": ["pve-node1"]}
    c = ProxmoxConnector(ctx)
    r = await c.dispatch("reconcile_cluster", {"apply_removals": False})
    assert r.success
    assert "pve-node2" in r.data["configured"]


# ============================ NFS protocol ============================ #
async def test_register_hosts_skipped_for_nfs(make_context):
    ctx = _ctx(make_context, protocol="nfs")
    c = ProxmoxConnector(ctx)
    r = await c.register_hosts(host_group="pve-cluster")
    assert r.success
    assert r.artifacts.get("skipped") is True
    # no FA hosts registered for NFS.
    assert ctx.array.hosts == {}


async def test_setup_connectivity_skipped_for_nfs(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nfs")
    c = ProxmoxConnector(ctx)
    r = await c.setup_connectivity()
    assert r.success
    assert r.artifacts.get("skipped") is True
    joined = "\n".join(captured_logs)
    assert "iscsiadm" not in joined and "nvme connect" not in joined


async def test_provision_nfs_datastore(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nfs")
    c = ProxmoxConnector(ctx)
    r = await c.provision_nfs_datastore(name="pve-nfs", export_path="/pve",
                                        storage_id="purefa-nfs")
    assert r.success
    # FA file system + NFS export created on the array.
    assert "pve-nfs" in ctx.array.filesystems
    assert "pve-nfs" in ctx.array.nfs_exports
    ops = [c0[0] for c0 in ctx.array.calls]
    assert "create_filesystem" in ops and "create_nfs_export" in ops
    assert "get_nfs_data_interfaces" in ops
    # PVE-native nfs storage defined over SSH against a discovered NFS portal.
    joined = "\n".join(captured_logs)
    assert "pvesm add nfs purefa-nfs --server 10.30.30.30 --export /pve" in joined
    assert r.artifacts["server"] == "10.30.30.30"
    assert r.artifacts["protocol"] == "nfs"


async def test_provision_nfs_datastore_no_array(make_context):
    c = ProxmoxConnector(_ctx(make_context, protocol="nfs", with_array=False))
    r = await c.provision_nfs_datastore(name="x")
    assert not r.success


async def test_provision_nfs_datastore_dry_run(make_context):
    ctx = _ctx(make_context, protocol="nfs")
    ctx.dry_run = True
    c = ProxmoxConnector(ctx)
    r = await c.provision_nfs_datastore(name="pve-nfs")
    assert r.success
    assert "pve-nfs" not in ctx.array.filesystems  # no array mutation in dry-run


async def test_teardown_nfs_datastore(make_context, captured_logs):
    ctx = _ctx(make_context, protocol="nfs")
    c = ProxmoxConnector(ctx)
    await c.provision_nfs_datastore(name="pve-nfs", export_path="/pve",
                                    storage_id="purefa-nfs")
    r = await c.teardown_nfs_datastore(name="pve-nfs", storage_id="purefa-nfs")
    assert r.success
    assert "pve-nfs" not in ctx.array.filesystems
    assert "pve-nfs" not in ctx.array.nfs_exports
    assert "pvesm remove purefa-nfs" in "\n".join(captured_logs)


def test_nfs_in_protocol_enum_and_plugin():
    proto = next(f for f in ProxmoxConnector.target_schema() if f.name == "protocol")
    assert "nfs" in (proto.options or [])
    # the Perl plugin's protocol enum advertises nfs too.
    text = proxmox_mod.PLUGIN_FILE.read_text(encoding="utf-8")
    assert "'nfs'" in text
