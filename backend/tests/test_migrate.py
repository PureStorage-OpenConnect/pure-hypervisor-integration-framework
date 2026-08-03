"""Cross-hypervisor migration: spec round-trip, orchestrator step sequence +
rollback (against fake connectors + the mock FlashArray), and per-connector
spec-capture parsing in mock mode."""

from __future__ import annotations

import pytest

from phif.connectors.base import (
    Capability,
    ConnectorContext,
    HypervisorConnector,
    HypervisorTarget,
    OpResult,
)
from phif.flasharray.client import MockFlashArrayClient
from phif.jobs.runner import JobRunner
from phif.migrate.service import MigrationService
from phif.migrate.spec import (
    DiskIdentity,
    DiskSpec,
    NicSpec,
    VmSpec,
    nvme_eui,
    scsi_wwid,
)


# ------------------------------------------------------------------ spec ---
def test_wwid_and_eui_derivation():
    assert scsi_wwid("00123456789abcdef0bb8281") == "3624a937000123456789abcdef0bb8281"
    # Tolerates a serial already carrying the OUI prefix, and a 0x prefix.
    assert scsi_wwid("624a9370abc") == "3624a9370abc"
    assert scsi_wwid("0xABCD") == "3624a9370abcd"
    assert nvme_eui("0xABCD") == "eui.abcd"


def test_vmspec_round_trip():
    spec = VmSpec(
        name="db01", source_ref="100", vcpus=4, memory_bytes=8 * 1024**3,
        firmware="uefi", secure_boot=True,
        disks=[DiskSpec(DiskIdentity("vm-100-disk-0", serial="abc", size_bytes=1),
                        bus="scsi", order=0, boot=True, source_ref="scsi0")],
        nics=[NicSpec(mac="AA:BB", source_network="vmbr0", order=0)],
        raw={"scsihw": "virtio-scsi-single"})
    again = VmSpec.from_dict(spec.to_dict())
    assert again.to_dict() == spec.to_dict()
    assert again.boot_disk().identity.fa_volume == "vm-100-disk-0"


# ------------------------------------------------------- fake connectors ---
class FakeConnector(HypervisorConnector):
    key = "fake"
    name = "Fake"
    CAPABILITIES = {Capability.MIGRATE, Capability.VM_INVENTORY,
                    Capability.VM_LIFECYCLE}

    def __init__(self, ctx, *, host_group, networks, spec=None, fail_at=None):
        super().__init__(ctx)
        self._hg = host_group
        self._networks = networks
        self._spec = spec
        self.fail_at = fail_at        # method name to fail on
        self.power = {}               # vm_ref -> state
        self.events: list[str] = []

    def migration_host_group(self):
        return self._hg

    async def validate_connection(self):
        return OpResult.ok("ok")

    async def list_networks(self):
        return self._networks

    async def list_vms(self):
        return [{"id": "src-1", "name": "db01", "power_state": "running"}]

    async def capture_vm_spec(self, vm_ref):
        self.events.append("capture")
        return self._spec

    async def power_state(self, vm_ref):
        return self.power.get(vm_ref, "running")

    def _maybe_fail(self, name):
        if self.fail_at == name:
            return OpResult.fail(f"injected failure at {name}")
        return None

    async def stop_vm(self, vm_ref, *, force=False):
        self.events.append("stop")
        self.power[vm_ref] = "stopped"
        return self._maybe_fail("stop_vm") or OpResult.ok("stopped")

    async def start_vm(self, vm_ref):
        self.events.append(f"start:{vm_ref}")
        self.power[vm_ref] = "running"
        return OpResult.ok("started")

    async def create_vm(self, spec, *, network_map, placement=None):
        self.events.append("create")
        self.placement = placement
        f = self._maybe_fail("create_vm")
        if f:
            return f
        return OpResult.ok("created", artifacts={"vm_ref": "dst-1"})

    async def create_managed_disk(self, vm_ref, *, size_bytes, order, boot):
        if self.fail_at == "create_managed_disk":
            raise RuntimeError("injected failure at create_managed_disk")
        self.events.append(f"mkdisk:{order}")
        name = f"dst-{vm_ref}-disk-{order}"
        # The dest plugin creates a managed volume on THIS connector's array.
        self.ctx.array.volumes[name] = {"size": size_bytes, "serial": f"ser-{name}"}
        return name

    async def set_boot_order(self, vm_ref, disks):
        self.events.append("boot")
        return self._maybe_fail("set_boot_order") or OpResult.ok("boot")

    async def delete_vm(self, vm_ref, *, keep_disks=True):
        self.events.append(f"delete:{vm_ref}:keep={keep_disks}")
        return OpResult.ok("deleted")


def _ctx(array):
    target = HypervisorTarget(id="t", connector_key="fake", name="fake")
    return ConnectorContext(target=target, log=_collect_log,
                            runner=JobRunner(_collect_log), array=array)


_LOG: list[str] = []


async def _collect_log(line: str) -> None:
    _LOG.append(line)


def _build(fail_at=None):
    """A source + destination fake sharing one mock FlashArray, with two FA-backed
    disks and a single NIC mapped src-net -> dst-net."""
    array = MockFlashArrayClient()
    # Disks must exist on the array (resolve_volumes calls get_volume).
    array.volumes["vol-a"] = {"size": "10G", "serial": "aaaa"}
    array.volumes["vol-b"] = {"size": "20G", "serial": "bbbb"}
    array.host_groups["src-hg"] = {"hosts": ["src-h1"]}
    array.host_groups["dst-hg"] = {"hosts": ["dst-h1"]}

    spec = VmSpec(
        name="db01", source_ref="src-1", vcpus=2, memory_bytes=4 * 1024**3,
        disks=[DiskSpec(DiskIdentity("vol-a"), bus="scsi", order=0, boot=True,
                        source_ref="scsi0"),
               DiskSpec(DiskIdentity("vol-b"), bus="scsi", order=1,
                        source_ref="scsi1")],
        nics=[NicSpec(mac="AA:BB:CC:00:00:01", source_network="src-net", order=0)])

    src = FakeConnector(_ctx(array), host_group="src-hg",
                        networks=[{"id": "src-net", "name": "src"}], spec=spec)
    dst = FakeConnector(_ctx(array), host_group="dst-hg",
                        networks=[{"id": "dst-net", "name": "dst"}],
                        fail_at=fail_at)
    return array, src, dst


# Force NON-mock runners so power polling is exercised, but keep it instant.
def _make_real(svc_connectors):
    for c in svc_connectors:
        c.ctx.runner.mock = False
        c.ctx.runner.dry_run = False


def _svc(src, dst, **opts):
    options = {"poll_delay": 0, "poll_attempts": 3, **opts}
    return MigrationService(src, dst, _collect_log, vm_ref="src-1",
                            network_map={"src-net": "dst-net"}, options=options)


async def test_move_creates_managed_dest_and_overwrites_from_source():
    array, src, dst = _build()
    _make_real([src, dst])
    res = await _svc(src, dst, mode="move").run()
    assert res.success, res.message
    assert res.artifacts["dest_vm_ref"] == "dst-1"

    # The destination plugin created a managed disk per source disk...
    assert "mkdisk:0" in dst.events and "mkdisk:1" in dst.events
    # ...and the source data was copied onto them WITH OVERWRITE.
    copies = [kw for op, kw in array.calls if op == "copy_volume"]
    assert copies and all(kw["overwrite"] for kw in copies)
    assert {kw["source"] for kw in copies} == {"vol-a", "vol-b"}
    # Move: source volumes disconnected from host group and eradicated.
    assert any(op == "disconnect_volume_from_group" and kw.get("volume") in ("vol-a", "vol-b")
               for op, kw in array.calls)
    assert any(op == "delete_volume" and kw.get("name") in ("vol-a", "vol-b")
               for op, kw in array.calls)
    # Move: source quiesced + source VM removed.
    assert "stop" in src.events
    assert "delete:src-1:keep=True" in src.events
    # Destination booted; not deleted on success.
    assert "boot" in dst.events
    assert not any(e.startswith("delete:dst-1") for e in dst.events)


async def test_rollback_when_dest_create_fails_touches_nothing():
    array, src, dst = _build(fail_at="create_vm")
    _make_real([src, dst])
    res = await _svc(src, dst, mode="move").run()
    assert not res.success and res.data["phase"] == "rolled_back"
    # create_vm is the first mutating step: nothing was created, copied, or stopped.
    assert not any(e.startswith("delete:") for e in dst.events)
    assert not any(op == "copy_volume" for op, _ in array.calls)
    assert "stop" not in src.events


async def test_rollback_after_disk_create_frees_dest_and_restarts_source():
    array, src, dst = _build(fail_at="create_managed_disk")
    _make_real([src, dst])
    res = await _svc(src, dst, mode="move").run()
    assert not res.success
    # Dest VM removed AND its new disks freed (keep_disks=False).
    assert "delete:dst-1:keep=False" in dst.events
    # Rollback STOPS the dest before deleting it (a failed/partial power-on can
    # leave it running; some platforms refuse to delete a powered-on VM).
    assert "stop" in dst.events
    assert dst.events.index("stop") < dst.events.index("delete:dst-1:keep=False")
    # Source was quiesced (move) then restarted by rollback.
    assert "stop" in src.events and "start:src-1" in src.events
    # Source volumes never deleted.
    assert not any(op == "delete_volume" and kw["name"] in ("vol-a", "vol-b")
                   for op, kw in array.calls)


async def test_await_power_times_out_when_state_never_reached():
    array, src, dst = _build()
    _make_real([src, dst])
    svc = _svc(src, dst)
    # power_state stays "running" (the fake default) while we wait for "stopped".
    from phif.migrate.steps import MigrationError
    with pytest.raises(MigrationError):
        await svc._await_power(src, "src-1", "stopped")


async def test_copy_mode_overwrites_managed_dest_and_leaves_source_untouched():
    array, src, dst = _build()
    _make_real([src, dst])
    res = await _svc(src, dst, mode="copy").run()
    assert res.success, res.message
    assert res.artifacts["mode"] == "copy"
    # Managed dest disks created + overwritten from the source volumes.
    copies = [kw for op, kw in array.calls if op == "copy_volume"]
    assert copies and all(kw["overwrite"] for kw in copies)
    assert {kw["source"] for kw in copies} == {"vol-a", "vol-b"}
    # Source fully untouched in copy mode (no stop/delete; no unmap/rename/delete).
    assert "stop" not in src.events
    assert not any(e.startswith("delete:src-1") for e in src.events)
    assert not any(op in ("disconnect_volume_from_group", "rename_volume",
                          "delete_volume") for op, _ in array.calls)


async def test_copy_mode_shutdown_source_stays_down_on_success():
    array, src, dst = _build()
    _make_real([src, dst])
    res = await _svc(src, dst, mode="copy", shutdown_source=True).run()
    assert res.success, res.message
    # Source stopped for a clean copy and LEFT shut down (not restarted/removed).
    assert "stop" in src.events
    assert not any(e.startswith("start:") for e in src.events)
    assert not any(e.startswith("delete:src-1") for e in src.events)


async def test_copy_mode_shutdown_restart_on_late_failure():
    array, src, dst = _build(fail_at="set_boot_order")
    _make_real([src, dst])
    res = await _svc(src, dst, mode="copy", shutdown_source=True).run()
    assert not res.success
    # Source stopped for the copy, then restarted by rollback (failure path).
    assert "stop" in src.events and "start:src-1" in src.events


async def test_copy_mode_default_leaves_source_running():
    array, src, dst = _build()
    _make_real([src, dst])
    await _svc(src, dst, mode="copy").run()
    assert "stop" not in src.events


async def test_copy_mode_rollback_frees_dest_disks():
    array, src, dst = _build(fail_at="set_boot_order")
    _make_real([src, dst])
    res = await _svc(src, dst, mode="copy").run()
    assert not res.success
    # The dest VM (and its new managed disks) are removed on rollback...
    assert "delete:dst-1:keep=False" in dst.events
    # ...and the SOURCE volumes were never deleted.
    assert not any(op == "delete_volume" and kw["name"] in ("vol-a", "vol-b")
                   for op, kw in array.calls)
    # Source VM untouched throughout.
    assert "stop" not in src.events and "detach" not in src.events


# --------------------------------------------------- cross-array migration ---
def _build_xarray(*, preexisting_conn=False, fail_at=None):
    """Source and destination on SEPARATE arrays (arrayA -> arrayB)."""
    arrA = MockFlashArrayClient("arrayA"); arrA.name = "arrayA"
    arrB = MockFlashArrayClient("arrayB"); arrB.name = "arrayB"
    arrA.volumes["vol-a"] = {"size": "10G", "serial": "aaaa"}
    arrA.host_groups["src-hg"] = {"hosts": ["h1"]}
    arrB.host_groups["dst-hg"] = {"hosts": ["h2"]}
    if preexisting_conn:
        arrA.array_connections = [{"name": "arrayB", "management_address": "arrayB",
                                   "status": "connected", "type": "async-replication"}]
    spec = VmSpec(
        name="db01", source_ref="src-1", vcpus=2, memory_bytes=4 * 1024**3,
        disks=[DiskSpec(DiskIdentity("vol-a"), bus="scsi", order=0, boot=True,
                        source_ref="scsi0")],
        nics=[NicSpec(mac="AA:BB:CC:00:00:01", source_network="src-net", order=0)])

    def ctx(arr):
        target = HypervisorTarget(id="t", connector_key="fake", name="fake")
        return ConnectorContext(target=target, log=_collect_log,
                                runner=JobRunner(_collect_log), array=arr)

    src = FakeConnector(ctx(arrA), host_group="src-hg",
                        networks=[{"id": "src-net", "name": "src"}], spec=spec)
    dst = FakeConnector(ctx(arrB), host_group="dst-hg",
                        networks=[{"id": "dst-net", "name": "dst"}], fail_at=fail_at)
    return arrA, arrB, src, dst


def _xsvc(src, dst, **opts):
    options = {"poll_delay": 0, "replication_settle": 0, "cross_array": True, **opts}
    return MigrationService(src, dst, _collect_log, vm_ref="src-1",
                            network_map={"src-net": "dst-net"}, options=options)


async def test_cross_array_copy_connects_replicates_and_overwrites():
    arrA, arrB, src, dst = _build_xarray()
    _make_real([src, dst])
    res = await _xsvc(src, dst, mode="copy", allow_array_connect=True).run()
    assert res.success, res.message
    assert res.artifacts["dest_vm_ref"] == "dst-1"
    assert res.artifacts["cross_array"] is True
    # Source array connected to the destination array (authorized).
    assert any(op == "connect_to_array" for op, _ in arrA.calls)
    # Volume replicated FROM the source array...
    assert any(op == "replicate_volume_to" for op, _ in arrA.calls)
    # ...then the plugin-managed dest disk OVERWRITTEN from the replica (on dest array).
    copies = [kw for op, kw in arrB.calls if op == "copy_volume"]
    assert copies and all(kw["overwrite"] for kw in copies)
    assert "mkdisk:0" in dst.events
    # Replication pgroup cleaned up; source untouched (copy).
    assert any(op == "cleanup_replication_pgroup" for op, _ in arrA.calls)
    assert "stop" not in src.events and not any(e.startswith("delete:") for e in src.events)


async def test_cross_array_requires_authorization_when_unconnected():
    arrA, arrB, src, dst = _build_xarray()
    _make_real([src, dst])
    res = await _xsvc(src, dst, mode="copy", allow_array_connect=False).run()
    assert not res.success
    assert "authoriz" in res.message.lower()
    # Nothing was configured.
    assert not any(op == "connect_to_array" for op, _ in arrA.calls)


async def test_cross_array_reuses_existing_connection():
    arrA, arrB, src, dst = _build_xarray(preexisting_conn=True)
    _make_real([src, dst])
    res = await _xsvc(src, dst, mode="copy", allow_array_connect=False).run()
    assert res.success, res.message
    # Already connected -> no new connection configured.
    assert not any(op == "connect_to_array" for op, _ in arrA.calls)
    assert any(op == "copy_volume" and kw["overwrite"] for op, kw in arrB.calls)


async def test_cross_array_move_removes_source_and_deletes_source_volume():
    arrA, arrB, src, dst = _build_xarray()
    _make_real([src, dst])
    res = await _xsvc(src, dst, mode="move", allow_array_connect=True).run()
    assert res.success, res.message
    # Move shuts down + removes the source VM...
    assert "stop" in src.events
    assert any(e.startswith("delete:") for e in src.events)
    # ...and eradicates the source-array volume.
    assert any(op == "delete_volume" and kw.get("name") == "vol-a"
               for op, kw in arrA.calls)


# --------------------------------- real-connector matrix (mock mode, no HTTP) ---
import itertools

from phif.connectors.registry import get_connector_class

_MATRIX_CONN = {
    "proxmox": {"node_host": "pve.test", "storage_id": "purefa"},
    "xcpng": {"pool_master_host": "xcp.test", "sr_name": "purefa"},
    "hpevme": {"vme_manager_url": "https://vme.test"},
}


@pytest.mark.parametrize("mode", ["move", "copy"])
@pytest.mark.parametrize("src_key,dst_key",
                         list(itertools.permutations(["proxmox", "xcpng", "hpevme"], 2)))
async def test_real_connector_matrix(make_context, mock_array, src_key, dst_key, mode):
    """Every source->destination direction in BOTH modes, end-to-end through the
    real connector methods (capture/create/attach/power/clone) in mock mode against
    one shared mock FlashArray. Fast + deterministic — no HTTP, background job, DB."""
    mock_array.host_groups["hg-src"] = {"hosts": ["h1"]}
    mock_array.host_groups["hg-dst"] = {"hosts": ["h2"]}
    # Source volumes must exist on the array for copy-mode cloning.
    mock_array.volumes["vm-100/vm-100-disk-7"] = {"size": "32G", "serial": "abc123"}
    mock_array.volumes["vdi-mock-0"] = {"size": "10G", "serial": "def456"}
    mock_array.volumes["vol-mock-0"] = {"size": "10G", "serial": "ghi789"}

    def build(key, hg):
        ctx = make_context(
            connector_key=key,
            connection=dict(_MATRIX_CONN[key], host_group=hg, protocol="iscsi"),
            secrets={"ssh_password": "p", "password": "p", "api_token": "tok"})
        return get_connector_class(key)(ctx)

    src = build(src_key, "hg-src")
    dst = build(dst_key, "hg-dst")

    # Build the per-NIC network map from the captured source spec.
    spec = await src.capture_vm_spec("100")
    dst_nets = await dst.list_networks()
    network_map = {nic.source_network: dst_nets[0]["id"] for nic in spec.nics}

    svc = MigrationService(src, dst, _collect_log, vm_ref="100",
                           network_map=network_map,
                           options={"poll_delay": 0, "mode": mode})
    res = await svc.run()
    assert res.success, res.message
    assert res.artifacts["dest_vm_ref"]
    assert res.artifacts["mode"] == mode


def test_proxmox_vm_name_sanitized():
    from phif.connectors.proxmox.connector import ProxmoxConnector
    assert ProxmoxConnector._pve_vm_name("AlmaLinux 8") == "AlmaLinux-8"
    assert ProxmoxConnector._pve_vm_name("web (prod)/01") == "web-prod-01"
    assert ProxmoxConnector._pve_vm_name("  ") == "migrated-vm"
    assert ProxmoxConnector._pve_vm_name("ok-name.1") == "ok-name.1"


async def test_proxmox_no_rename_adoption(make_context, mock_array):
    """Move-to-Proxmox does NOT rename volumes between vgroups (FlashArray forbids
    moving a volume through rename); plan_volume_adoption always returns []."""
    from phif.connectors.registry import get_connector_class
    from phif.migrate.spec import DiskIdentity, DiskSpec

    ctx = make_context(connector_key="proxmox",
                       connection={"node_host": "pve.test", "storage_id": "purefa"},
                       secrets={"ssh_password": "p"})
    conn = get_connector_class("proxmox")(ctx)
    disk = DiskSpec(identity=DiskIdentity(fa_volume="phif-abc/phif-abc-xyz"),
                    bus="scsi", order=0, boot=True, source_ref="scsi0")
    assert await conn.plan_volume_adoption([disk], "100") == []


async def test_proxmox_attach_foreign_vgroup_uses_raw_device(make_context, mock_array, captured_logs):
    """A volume in a FOREIGN vgroup is attached by its raw multipath device
    (/dev/mapper/<wwid>); a volume in the VM's own vgroup is attached purefa-managed."""
    from phif.connectors.registry import get_connector_class
    from phif.migrate.spec import DiskIdentity, DiskSpec, scsi_wwid

    ctx = make_context(connector_key="proxmox",
                       connection={"node_host": "pve.test", "storage_id": "purefa",
                                   "protocol": "iscsi"},
                       secrets={"ssh_password": "p"})
    # Mock runner still LOGS each command (before short-circuiting), so we can
    # assert the exact `qm set` the attach builds without real SSH.
    conn = get_connector_class("proxmox")(ctx)
    mock_array.volumes["phif-abc/phif-abc-xyz"] = {"size": "10G", "serial": "deadbeef"}
    mock_array.volumes["vm-100/vm-100-disk-0"] = {"size": "10G", "serial": "cafe"}

    foreign = DiskSpec(identity=DiskIdentity(fa_volume="phif-abc/phif-abc-xyz"),
                       bus="scsi", order=0, boot=True, source_ref="scsi0")
    own = DiskSpec(identity=DiskIdentity(fa_volume="vm-100/vm-100-disk-0"),
                   bus="scsi", order=1, source_ref="scsi1")
    await conn.attach_existing_volumes("100", [foreign, own])
    log = "\n".join(captured_logs)
    assert f"qm set 100 --scsi0 /dev/mapper/{scsi_wwid('deadbeef')}" in log  # foreign -> raw
    assert "qm set 100 --scsi1 purefa:vm-100-disk-0" in log                 # own -> managed


async def test_proxmox_copy_target_avoids_destroyed_name(make_context, mock_array):
    """Copy-to-Proxmox picks a clone name not taken by a destroyed (pending-
    eradication) volume, so the clone doesn't fail with 'Volume has been destroyed'."""
    from phif.connectors.registry import get_connector_class

    ctx = make_context(connector_key="proxmox",
                       connection={"node_host": "pve.test", "storage_id": "purefa"},
                       secrets={"ssh_password": "p"})
    conn = get_connector_class("proxmox")(ctx)
    # vm-100/vm-100-disk-0 was destroyed but not eradicated (name still reserved).
    await mock_array.delete_volume("vm-100/vm-100-disk-0")
    target = await conn.prepare_copy_target(base_name="x", order=0, dest_vm_ref="100")
    assert target == "vm-100/vm-100-disk-1"


async def test_proxmox_adoption_skips_when_vmid_already_matches(make_context, mock_array):
    """No rename when the volume already lives in the destination VM's group
    (the 'destination VMID happened to match' case)."""
    from phif.connectors.registry import get_connector_class
    from phif.migrate.spec import DiskIdentity, DiskSpec

    ctx = make_context(connector_key="proxmox",
                       connection={"node_host": "pve.test", "storage_id": "purefa"},
                       secrets={"ssh_password": "p"})
    conn = get_connector_class("proxmox")(ctx)
    disk = DiskSpec(identity=DiskIdentity(fa_volume="vm-100/vm-100-disk-7"),
                    bus="scsi", order=0, boot=True, source_ref="scsi0")
    plan = await conn.plan_volume_adoption([disk], "100")
    assert plan == []


async def test_refresh_block_devices_flushes_then_rescans():
    from phif.connectors.multipath import refresh_block_devices

    cmds: list = []
    async def run(cmd, t):
        cmds.append((cmd, t))

    await refresh_block_devices(run, protocol="iscsi", wwids=["3624a9370abc"])
    joined = " ;; ".join(c for c, _ in cmds)
    # Stale map flushed BEFORE the rescan, then reassembled.
    assert "multipath -f 3624a9370abc" in joined
    assert "iscsiadm -m session --rescan" in joined
    assert "multipath -r" in joined
    flush_i = next(i for i, (c, _) in enumerate(cmds) if c.startswith("multipath -f"))
    rescan_i = next(i for i, (c, _) in enumerate(cmds) if "rescan" in c)
    assert flush_i < rescan_i
    # Every command is time-bounded.
    assert all(t and t > 0 for _, t in cmds)

    # NVMe uses native multipath -> no dm flush/assemble.
    cmds.clear()
    await refresh_block_devices(run, protocol="nvme-tcp", wwids=["x"])
    j2 = " ;; ".join(c for c, _ in cmds)
    assert "multipath -f" not in j2 and "nvme connect-all" in j2


# --------------------------------------------- per-connector capture (mock) ---
@pytest.mark.parametrize("key,conn_kwargs", [
    ("proxmox", {"node_host": "pve.test", "storage_id": "purefa",
                 "host_group": "hg"}),
    ("xcpng", {"pool_master_host": "xcp.test", "sr_name": "purefa",
               "host_group": "hg"}),
    ("hpevme", {"vme_manager_url": "https://vme.test", "host_group": "hg"}),
])
async def test_connector_capture_spec_mock(make_context, key, conn_kwargs):
    from phif.connectors.registry import get_connector_class

    ctx = make_context(connector_key=key, connection=conn_kwargs,
                       secrets={"ssh_password": "p", "password": "p"})
    conn = get_connector_class(key)(ctx)
    assert conn.supports(Capability.MIGRATE)
    spec = await conn.capture_vm_spec("100")
    assert spec.vcpus >= 1
    assert spec.disks and spec.disks[0].identity.fa_volume
    assert any(d.boot for d in spec.disks)
    assert spec.nics and spec.nics[0].mac
    # list_vms / list_networks are exercisable in mock mode.
    assert await conn.list_vms()
    assert await conn.list_networks()


async def test_dest_vm_name_deduped_to_avoid_overwrite():
    """A destination VM of the same name must not be overwritten — the service
    appends -N when the captured name already exists at the destination."""
    array, src, dst = _build()
    _make_real([src, dst])
    # FakeConnector.list_vms reports an existing VM named "db01" -> must rename.
    svc = _svc(src, dst, mode="copy")
    res = await svc.run()
    assert res.success, res.message
    assert svc.spec.name == "db01-1"


async def test_dest_vm_name_kept_when_no_collision():
    array, src, dst = _build()
    _make_real([src, dst])

    async def _no_vms():
        return []
    dst.list_vms = _no_vms  # destination has no VMs -> keep original name
    svc = _svc(src, dst, mode="copy")
    res = await svc.run()
    assert res.success, res.message
    assert svc.spec.name == "db01"
