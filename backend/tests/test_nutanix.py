"""Unit tests for the Nutanix (AHV) connector.

Payload shapes mirror what Prism Central on AOS 7.6 / AHV 11.2 actually
returns — in particular an unprefixed ``busType`` ("SCSI"), the ETag arriving
as a response *header* rather than in the body, ``$limit`` capped at 100, and
``externalStorageInfo.volumeName`` carrying only the leaf volume name while the
array holds it pod-scoped.
"""

import pytest

from phif.connectors.base import Capability, ConnectionValidationError, Protocol
from phif.connectors.nutanix.connector import NutanixConnector
from phif.connectors.registry import discover, get_connector_class

NUTANIX_CONN = {
    "pc_host": "prism-central.example.com",
    "pc_user": "admin",
    "cluster": "ahv-cluster",
}
NUTANIX_SECRETS = {"pc_password": "s3cret"}

# Leaf name as Prism reports it, and the pod-scoped name the array holds.
LEAF_VOL = "nx-1234567890123456789-51-dt"
SCOPED_VOL = "example-realm::example-pod::nx-1234567890123456789-51-dt"
MD_VOL = "nx-1234567890123456789-52-md"


class StubRunner:
    """JobRunner stand-in returning canned bodies/headers per path fragment.

    ``routes`` maps a substring of the request path to either a body dict or a
    ``(body, headers)`` pair. Every call is recorded so tests can assert on what
    was actually sent.
    """

    mock = False
    dry_run = False

    def __init__(self, routes=None):
        self.routes = routes or {}
        self.calls = []

    async def run_http(self, method, url, *, headers=None, json_body=None,
                       data=None, files=None, verify=False,
                       expected=(200, 201, 202, 204)):
        self.calls.append({"method": method, "url": url, "headers": headers or {},
                           "json_body": json_body})
        for frag, resp in self.routes.items():
            if frag in url:
                body, hdrs = resp if isinstance(resp, tuple) else (resp, {})
                return {"status_code": 200, "json": body, "text": "",
                        "headers": {k.lower(): v for k, v in hdrs.items()}}
        return {"status_code": 200, "json": {}, "text": "", "headers": {}}


def _ctx(make_context, routes=None, **kw):
    ctx = make_context(
        connector_key="nutanix",
        connection=dict(NUTANIX_CONN),
        secrets=dict(NUTANIX_SECRETS),
        **kw,
    )
    ctx.runner = StubRunner(routes)
    return ctx


def _vm_payload(disks, *, name="app-01", nics=None, uefi=False):
    boot_type = ("vmm.v4.ahv.config.UefiBoot" if uefi
                 else "vmm.v4.ahv.config.LegacyBoot")
    return {
        "data": {
            "extId": "vm-ext-1",
            "name": name,
            "numSockets": 4,
            "numCoresPerSocket": 2,
            "memorySizeBytes": 8589934592,
            "powerState": "ON",
            "bootConfig": {"$objectType": boot_type},
            "disks": disks,
            "nics": nics or [],
            "cluster": {"extId": "cluster-ext-1"},
        }
    }


def _disk(volume, *, index=0, size=107374182400, disk_id="disk-ext-1",
          bus="SCSI", vg=False):
    backing = {
        "$objectType": ("vmm.v4.ahv.config.ADSFVolumeGroupReference" if vg
                        else "vmm.v4.ahv.config.VmDisk"),
        "diskExtId": disk_id,
        "diskSizeBytes": size,
        "storageContainer": {"extId": "container-ext-1"},
    }
    if volume is not None and not vg:
        backing["externalStorageInfo"] = {"volumeName": volume}
    return {"backingInfo": backing, "diskAddress": {"busType": bus, "index": index}}


# ---- discovery / metadata ----
def test_nutanix_is_discovered():
    assert "nutanix" in discover()
    assert get_connector_class("nutanix") is NutanixConnector


def test_descriptor_metadata():
    d = NutanixConnector.descriptor()
    assert d["key"] == "nutanix"
    # NVMe-oF/TCP is the only transport supported for AHV external storage.
    assert d["protocols"] == ["nvme-tcp"]


def test_capabilities_are_honest_about_deploy():
    """Registering FA as external storage is out of scope, so the connector
    must NOT advertise deploy/configure/provision capabilities it lacks."""
    caps = NutanixConnector.capabilities()
    assert Capability.MIGRATE in caps
    assert Capability.VM_INVENTORY in caps
    assert Capability.VM_LIFECYCLE in caps
    for absent in (Capability.DEPLOY_PLUGIN, Capability.CONFIGURE,
                   Capability.PROVISION_DATASTORE, Capability.PROVISION_VOLUME,
                   Capability.CONNECTIVITY, Capability.HOST_REGISTER):
        assert absent not in caps, f"{absent} is advertised but not implemented"


def test_action_ids_present():
    ids = {a.id for a in NutanixConnector.action_schemas()}
    assert ids == {"snapshot", "clone", "resize", "set_qos", "delete", "health_check"}


# ---- connection ----
async def test_validate_connection_requires_fa_external_storage(make_context):
    """A cluster with no FlashArray external storage is the one case that must
    fail loudly, since every other operation depends on it."""
    routes = {
        "/clusters/list": {"entities": [{"status": {"name": "ahv-cluster"}}]},
        "/groups": {"group_results": [{"entity_results": [{
            "entity_id": "es-1",
            "data": [
                {"name": "name", "values": [{"values": ["SomeOtherArray"]}]},
                {"name": "vendor", "values": [{"values": ["kDellEMC"]}]},
            ],
        }]}]},
    }
    c = NutanixConnector(_ctx(make_context, routes))
    r = await c.validate_connection()
    assert not r.success
    assert "external storage" in r.message.lower()
    assert "does not perform that registration" in r.message


async def test_validate_connection_accepts_pure_external_storage(make_context):
    routes = {
        "/clusters/list": {"entities": [{"status": {"name": "ahv-cluster"}}]},
        "/groups": {"group_results": [{"entity_results": [{
            "entity_id": "es-1",
            "data": [
                {"name": "name", "values": [{"values": ["fa-external-1"]}]},
                {"name": "vendor", "values": [{"values": ["kPureStorage"]}]},
            ],
        }]}]},
    }
    c = NutanixConnector(_ctx(make_context, routes))
    r = await c.validate_connection()
    assert r.success


# ---- pagination ----
async def test_api_list_caps_limit_at_100_and_pages(make_context):
    """v4 rejects $limit>100 with a 400, so the connector must page. A single
    oversized request would either fail or silently truncate."""
    pages = {0: [{"extId": f"vm-{i}"} for i in range(100)],
             1: [{"extId": "vm-100"}]}

    class Pager(StubRunner):
        async def run_http(self, method, url, **kw):
            self.calls.append({"method": method, "url": url})
            page = int(url.split("$page=")[1])
            return {"status_code": 200, "text": "", "headers": {},
                    "json": {"data": pages.get(page, []),
                             "metadata": {"totalAvailableResults": 101}}}

    ctx = _ctx(make_context)
    ctx.runner = Pager()
    c = NutanixConnector(ctx)
    rows = await c._api_list("/api/vmm/v4.0/ahv/config/vms")
    assert len(rows) == 101
    assert all("$limit=100" in x["url"] for x in ctx.runner.calls)
    assert [x["url"].split("$page=")[1] for x in ctx.runner.calls] == ["0", "1"]


# ---- ETag handling ----
async def test_etag_is_read_from_header_and_replayed_as_if_match(make_context):
    """Prism returns the ETag as a response header spelled "Etag" (not in the
    body), and v4 rejects an in-place update without If-Match."""
    routes = {"/vms/vm-ext-1": (_vm_payload([]), {"Etag": "etag-abc"})}
    ctx = _ctx(make_context, routes)
    c = NutanixConnector(ctx)

    await c._find_vm("vm-ext-1")
    assert c._etags["vm-ext-1"] == "etag-abc"

    await c.start_vm("vm-ext-1")
    power_on = [x for x in ctx.runner.calls if "$actions/power-on" in x["url"]][0]
    assert power_on["headers"].get("If-Match") == "etag-abc"
    # Mutations also carry an idempotency key so a retry cannot double-apply.
    assert power_on["headers"].get("NTNX-Request-Id")


# ---- capture_vm_spec ----
async def test_capture_resolves_pod_scoped_volume(make_context, mock_array):
    """Prism reports the leaf volume name; the array holds it pod-scoped."""
    mock_array.volumes[SCOPED_VOL] = {"size": 107374182400, "serial": "ABC123"}
    routes = {"/vms/vm-ext-1": (_vm_payload([_disk(LEAF_VOL)]), {})}
    c = NutanixConnector(_ctx(make_context, routes))

    spec = await c.capture_vm_spec("vm-ext-1")
    assert len(spec.disks) == 1
    d = spec.disks[0]
    assert d.identity.fa_volume == SCOPED_VOL
    assert d.identity.serial == "ABC123"
    assert d.identity.derived_wwid() == "3624a9370abc123"
    assert d.boot is True
    assert spec.vcpus == 8  # 4 sockets x 2 cores
    assert spec.memory_bytes == 8589934592


async def test_capture_skips_metadata_volumes(make_context, mock_array):
    """`-md` volumes are Nutanix CBT bookkeeping, not guest data; migrating one
    would copy metadata over a data disk."""
    mock_array.volumes[SCOPED_VOL] = {"size": 107374182400, "serial": "ABC123"}
    mock_array.volumes[f"example-realm::example-pod::{MD_VOL}"] = {"size": 1024, "serial": "DEF456"}
    routes = {"/vms/vm-ext-1": (_vm_payload([
        _disk(LEAF_VOL, index=0, disk_id="d0"),
        _disk(MD_VOL, index=1, disk_id="d1"),
    ]), {})}
    c = NutanixConnector(_ctx(make_context, routes))

    spec = await c.capture_vm_spec("vm-ext-1")
    assert [d.identity.fa_volume for d in spec.disks] == [SCOPED_VOL]


async def test_capture_rejects_volume_group_disks(make_context, mock_array):
    """A Nutanix Volume Group is a cluster-level object shared between VMs, so
    it is not one FA volume and cannot ride along with a single VM."""
    mock_array.volumes[SCOPED_VOL] = {"size": 107374182400, "serial": "ABC123"}
    routes = {"/vms/vm-ext-1": (_vm_payload([
        _disk(LEAF_VOL, index=0, disk_id="d0"),
        _disk(None, index=1, disk_id="d1", vg=True),
    ]), {})}
    c = NutanixConnector(_ctx(make_context, routes))

    with pytest.raises(ConnectionValidationError) as e:
        await c.capture_vm_spec("vm-ext-1")
    assert "Volume Group" in str(e.value)
    assert "d1" in str(e.value)


async def test_capture_rejects_disk_not_on_connected_array(make_context):
    """A disk naming a volume this array does not hold (another array, or ADSF
    native storage) must be reported, never migrated as an unknown."""
    routes = {"/vms/vm-ext-1": (_vm_payload([_disk(LEAF_VOL)]), {})}
    c = NutanixConnector(_ctx(make_context, routes))  # mock_array has no volumes

    with pytest.raises(ConnectionValidationError) as e:
        await c.capture_vm_spec("vm-ext-1")
    assert "no FlashArray volume on the connected array" in str(e.value)


async def test_capture_rejects_adsf_native_disk(make_context):
    """A disk with no externalStorageInfo at all is on Nutanix native storage."""
    routes = {"/vms/vm-ext-1": (_vm_payload([_disk(None, disk_id="native-0")]), {})}
    c = NutanixConnector(_ctx(make_context, routes))

    with pytest.raises(ConnectionValidationError) as e:
        await c.capture_vm_spec("vm-ext-1")
    assert "native-0" in str(e.value)


async def test_capture_reads_firmware_and_nic_model(make_context, mock_array):
    """AHV signals UEFI by the bootConfig object type, and a NIC's device type
    by its backing object type — neither is a plain field."""
    mock_array.volumes[SCOPED_VOL] = {"size": 107374182400, "serial": "ABC123"}
    nics = [{
        "backingInfo": {"$objectType": "vmm.v4.ahv.config.EmulatedNic",
                        "macAddress": "50:6B:8D:AA:BB:CC"},
        "networkInfo": {"subnet": {"extId": "subnet-1"}},
    }]
    routes = {"/vms/vm-ext-1": (
        _vm_payload([_disk(LEAF_VOL)], nics=nics, uefi=True), {})}
    c = NutanixConnector(_ctx(make_context, routes))

    spec = await c.capture_vm_spec("vm-ext-1")
    assert spec.firmware == "uefi"
    assert len(spec.nics) == 1
    assert spec.nics[0].model == "e1000"
    # MAC is preserved across migration, and normalized to lower case.
    assert spec.nics[0].mac == "50:6b:8d:aa:bb:cc"


@pytest.mark.parametrize("reported,expected", [
    ("SCSI", "scsi"), ("IDE", "ide"), ("SATA", "sata"), ("NVME", "nvme"),
    # Tolerate the prefixed enum spelling other AHV enums use.
    ("kSCSI", "scsi"), ("", "scsi"), (None, "scsi"),
])
def test_logical_bus_handles_prefixed_and_plain(reported, expected):
    assert NutanixConnector._logical_bus(reported) == expected


def test_ahv_bus_is_unprefixed():
    """v4 reports/accepts busType without the "k" prefix most AHV enums use."""
    assert NutanixConnector._ahv_bus("scsi") == "SCSI"
    assert NutanixConnector._ahv_bus("nvme") == "NVME"
    # AHV has no virtio-blk bus.
    assert NutanixConnector._ahv_bus("virtio") == "SCSI"


# ---- destination: managed disk ----
async def test_create_managed_disk_returns_the_fa_volume(make_context):
    """Nutanix creates the disk AND its array volume; the connector reads the
    volume name back so the migration can overwrite it.

    The return must be a bare volume-name str, not an OpResult: the migration
    engine passes it straight into copy_volume() as the copy target.
    """
    created = _vm_payload([_disk(LEAF_VOL, index=2, disk_id="new-disk")])
    routes = {"/vms/vm-ext-1": (created, {"Etag": "e1"})}
    ctx = _ctx(make_context, routes)
    c = NutanixConnector(ctx)

    vol = await c.create_managed_disk("vm-ext-1", size_bytes=107374182400, order=2)
    assert isinstance(vol, str), "must return the volume NAME per the base contract"
    assert vol == LEAF_VOL

    post = [x for x in ctx.runner.calls
            if x["method"] == "POST" and x["url"].endswith("/disks")][0]
    body = post["json_body"]
    # Size must match the source exactly before the array overwrite.
    assert body["backingInfo"]["diskSizeBytes"] == 107374182400
    assert body["diskAddress"]["busType"] == "SCSI"
    assert body["diskAddress"]["index"] == 2


async def test_create_managed_disk_fails_on_non_fa_container(make_context):
    """If the container is not FlashArray-backed there is no array volume to
    migrate into — that has to fail rather than produce a dead VM."""
    created = _vm_payload([_disk(None, index=0, disk_id="new-disk")])
    routes = {"/vms/vm-ext-1": (created, {})}
    c = NutanixConnector(_ctx(make_context, routes))

    from phif.connectors.base import ConnectorError

    with pytest.raises(ConnectorError) as e:
        await c.create_managed_disk("vm-ext-1", size_bytes=1024, order=0)
    assert "not FlashArray-backed" in str(e.value)


# ---- destructive guard ----
async def test_delete_vm_refuses_to_keep_disks(make_context):
    """Deleting an AHV VM deletes its vDisks and their FA volumes, so
    keep_disks=True cannot be honoured and must not be silently ignored."""
    c = NutanixConnector(_ctx(make_context))
    r = await c.delete_vm("vm-ext-1", keep_disks=True)
    assert not r.success
    assert "also deletes its vDisks" in r.message


# ---- migration source ----
async def test_prepare_source_disks_is_a_noop(make_context, mock_array):
    """A Nutanix vDisk is already its own FA volume, so unlike a VMFS VMDK
    there is nothing to stage on the source side."""
    from phif.migrate.spec import DiskIdentity, DiskSpec, VmSpec

    spec = VmSpec(name="app-01", source_ref="vm-ext-1", disks=[
        DiskSpec(DiskIdentity(fa_volume=SCOPED_VOL, serial="ABC123"))])
    c = NutanixConnector(_ctx(make_context))
    r = await c.prepare_source_disks(spec)
    assert r.success
    assert r.artifacts["volumes"] == [SCOPED_VOL]
    # Nothing was provisioned or copied on the array.
    assert not any(op.startswith("create_volume") for op, _ in mock_array.calls)


# --------------------------------------------------------------------------- #
# Asynchronous task handling
#
# Every v4 mutation returns a prism.v4.config.TaskReference whose extId is an
# ergon task id -- NOT the entity id. Treating it as a VM id yields a 400 on the
# next call and orphans the VM, which is exactly what happened on first
# hardware run.
# --------------------------------------------------------------------------- #
TASK_ID = "ZXJnb24=:0efcd3f5-cafb-5f22-871e-ad4e401a2c5a"
NEW_VM_ID = "bcd6b6b6-9ebb-44b2-6f87-3429239b9840"


def _task_ref():
    return {"data": {"$objectType": "prism.v4.config.TaskReference",
                     "extId": TASK_ID}}


def _task(status="SUCCEEDED", entities=(("vmm:ahv:config:vm", NEW_VM_ID),),
          **extra):
    return {"data": {"extId": TASK_ID, "status": status,
                     "entitiesAffected": [{"rel": r, "extId": e}
                                          for r, e in entities],
                     **extra}}


class TaskRunner(StubRunner):
    """Serves a TaskReference for mutations and a task document for polling."""

    def __init__(self, task_doc=None, vm_doc=None, statuses=None):
        super().__init__()
        self.task_doc = task_doc if task_doc is not None else _task()
        self.vm_doc = vm_doc
        # Optional sequence of statuses to walk through before the task_doc.
        self.statuses = list(statuses or [])

    async def run_http(self, method, url, *, headers=None, json_body=None, **kw):
        self.calls.append({"method": method, "url": url, "headers": headers or {},
                           "json_body": json_body})
        if "/config/tasks/" in url:
            if self.statuses:
                return {"status_code": 200, "text": "", "headers": {},
                        "json": _task(status=self.statuses.pop(0), entities=())}
            return {"status_code": 200, "json": self.task_doc, "text": "", "headers": {}}
        if method in ("POST", "DELETE") and "/tasks" not in url:
            return {"status_code": 202, "json": _task_ref(), "text": "", "headers": {}}
        if self.vm_doc is not None:
            return {"status_code": 200, "json": self.vm_doc, "text": "",
                    "headers": {"etag": "e1"}}
        return {"status_code": 200, "json": {}, "text": "", "headers": {}}


async def test_create_vm_takes_id_from_task_not_from_post(make_context, monkeypatch):
    """Regression: the POST's extId is the TASK id. Using it as the VM id 400s
    on the next call and leaves the VM orphaned on the cluster."""
    from phif.migrate.spec import VmSpec

    ctx = _ctx(make_context)
    ctx.runner = TaskRunner()
    c = NutanixConnector(ctx)
    monkeypatch.setattr(c, "_cluster_ext_id",
                        lambda **kw: _async("cluster-ext-1"))

    r = await c.create_vm(VmSpec(name="app-01", source_ref="s", vcpus=2,
                                 memory_bytes=2 * 1024**3))
    assert r.success
    assert r.artifacts["vm_ref"] == NEW_VM_ID
    assert r.artifacts["vm_ref"] != TASK_ID
    # The task must actually have been polled.
    assert any("/config/tasks/" in x["url"] for x in ctx.runner.calls)


def _async(value):
    async def _inner(*a, **k):
        return value
    return _inner()


async def test_create_vm_fails_when_task_names_no_vm(make_context, monkeypatch):
    """A succeeded task with no VM entity means the id is unknown -- say so
    rather than returning a bogus reference."""
    from phif.migrate.spec import VmSpec

    ctx = _ctx(make_context)
    ctx.runner = TaskRunner(task_doc=_task(entities=()))
    c = NutanixConnector(ctx)
    monkeypatch.setattr(c, "_cluster_ext_id",
                        lambda **kw: _async("cluster-ext-1"))

    r = await c.create_vm(VmSpec(name="app-01", source_ref="s"))
    assert not r.success
    assert "affected entities" in r.message


async def test_await_task_raises_on_failed_task(make_context):
    """A v4 mutation returns 202 on acceptance, so a FAILED task would
    otherwise look like success."""
    from phif.connectors.base import ConnectorError

    ctx = _ctx(make_context)
    ctx.runner = TaskRunner(task_doc=_task(status="FAILED",
                                           legacyErrorMessage="boom"))
    c = NutanixConnector(ctx)
    with pytest.raises(ConnectorError) as e:
        await c._await_task(_task_ref(), "create VM x")
    assert "FAILED" in str(e.value)
    assert "boom" in str(e.value)


async def test_await_task_polls_until_terminal(make_context, monkeypatch):
    ctx = _ctx(make_context)
    ctx.runner = TaskRunner(statuses=["QUEUED", "RUNNING"])
    c = NutanixConnector(ctx)
    monkeypatch.setattr("phif.connectors.nutanix.connector.TASK_POLL_SECONDS", 0)

    task = await c._await_task(_task_ref(), "create VM x")
    assert (task.get("status") or "").upper() == "SUCCEEDED"
    polls = [x for x in ctx.runner.calls if "/config/tasks/" in x["url"]]
    assert len(polls) == 3  # QUEUED, RUNNING, then SUCCEEDED


async def test_await_task_ignores_non_task_response(make_context):
    """Reads return plain bodies, not TaskReferences -- those must pass through."""
    c = NutanixConnector(_ctx(make_context))
    assert await c._await_task({"data": {"extId": "plain-vm-id"}}, "x") == {}
    assert await c._await_task({}, "x") == {}


async def test_task_id_is_url_encoded_when_polled(make_context):
    """The ergon task id contains '=' and ':'."""
    ctx = _ctx(make_context)
    ctx.runner = TaskRunner()
    c = NutanixConnector(ctx)
    await c._await_task(_task_ref(), "x")
    poll = [x for x in ctx.runner.calls if "/config/tasks/" in x["url"]][0]
    assert "ZXJnb24%3D%3A" in poll["url"]


async def test_delete_vm_awaits_its_task(make_context):
    ctx = _ctx(make_context)
    ctx.runner = TaskRunner(vm_doc=_vm_payload([]))
    c = NutanixConnector(ctx)
    r = await c.delete_vm("vm-ext-1", keep_disks=False)
    assert r.success
    assert any("/config/tasks/" in x["url"] for x in ctx.runner.calls)


async def test_create_managed_disk_awaits_before_reading_back(make_context):
    """The disk and its array volume do not exist until the task finishes, so
    reading the VM back too early finds nothing."""
    ctx = _ctx(make_context)
    ctx.runner = TaskRunner(
        vm_doc=_vm_payload([_disk(LEAF_VOL, index=0, disk_id="new-disk")]))
    c = NutanixConnector(ctx)

    vol = await c.create_managed_disk("vm-ext-1", size_bytes=DISK_SIZE, order=0)
    assert vol == LEAF_VOL
    urls = [x["url"] for x in ctx.runner.calls]
    post_i = next(i for i, u in enumerate(urls)
                  if u.endswith("/disks"))
    task_i = next(i for i, u in enumerate(urls) if "/config/tasks/" in u)
    readback_i = max(i for i, u in enumerate(urls) if u.endswith("/vms/vm-ext-1"))
    assert post_i < task_i < readback_i, "must poll the task between POST and read-back"


DISK_SIZE = 107374182400


# --------------------------------------------------------------------------- #
# Base-contract conformance
#
# The migration engine calls these positionally and uses their return values
# directly, so a signature or return-type drift breaks Nutanix as a migration
# destination without any unit test noticing.
# --------------------------------------------------------------------------- #
def test_vm_lifecycle_signatures_match_the_base_contract():
    import inspect

    from phif.connectors.base import HypervisorConnector as Base

    for meth in ("create_vm", "create_managed_disk", "detach_volumes",
                 "set_boot_order", "delete_vm", "stop_vm", "start_vm",
                 "capture_vm_spec", "power_state"):
        base_params = list(inspect.signature(getattr(Base, meth)).parameters)
        mine = inspect.signature(getattr(NutanixConnector, meth)).parameters
        # Every positional/keyword name the engine may pass must be accepted,
        # either explicitly or via **kwargs.
        takes_kwargs = any(p.kind is inspect.Parameter.VAR_KEYWORD
                           for p in mine.values())
        for name in base_params:
            assert name in mine or takes_kwargs, (
                f"{meth} does not accept {name!r} from the base contract")


async def test_create_managed_disk_return_feeds_copy_volume(make_context, mock_array):
    """End-to-end shape check at the seam the migration engine actually uses:
        dest_vol = await dst.create_managed_disk(...)
        await dst_arr.copy_volume(copy_src, dest_vol, overwrite=True)
    A non-str return would sail through unit tests and fail on real hardware.
    """
    created = _vm_payload([_disk(LEAF_VOL, index=0, disk_id="new-disk")])
    ctx = _ctx(make_context, {"/vms/vm-ext-1": (created, {"Etag": "e1"})})
    c = NutanixConnector(ctx)

    dest_vol = await c.create_managed_disk("vm-ext-1", size_bytes=DISK_SIZE,
                                           order=0, boot=True)
    mock_array.volumes["source-vol"] = {"size": DISK_SIZE, "serial": "SRC1"}
    await mock_array.copy_volume("source-vol", dest_vol, overwrite=True)
    assert ("copy_volume", {"source": "source-vol", "dest": dest_vol,
                            "overwrite": True}) in mock_array.calls


async def test_detach_volumes_accepts_diskspecs_and_matches_pod_scoped(make_context):
    """The engine passes DiskSpec objects whose fa_volume is pod-scoped, while
    Prism reports only the leaf -- matching must work across that."""
    from phif.migrate.spec import DiskIdentity, DiskSpec

    created = _vm_payload([_disk(LEAF_VOL, index=0, disk_id="d0")])
    ctx = _ctx(make_context, {"/vms/vm-ext-1": (created, {"Etag": "e1"})})
    ctx.runner = TaskRunner(vm_doc=created)
    c = NutanixConnector(ctx)

    r = await c.detach_volumes(
        "vm-ext-1", [DiskSpec(DiskIdentity(fa_volume=SCOPED_VOL, serial="S1"))])
    assert r.success
    assert r.artifacts["detached"] == [LEAF_VOL]


async def test_set_boot_order_accepts_diskspec_list(make_context):
    from phif.migrate.spec import DiskIdentity, DiskSpec

    c = NutanixConnector(_ctx(make_context))
    disks = [DiskSpec(DiskIdentity(fa_volume="v0"), order=0, boot=True,
                      source_ref="d0"),
             DiskSpec(DiskIdentity(fa_volume="v1"), order=1, source_ref="d1")]
    r = await c.set_boot_order("vm-ext-1", disks)
    assert r.success
    assert r.artifacts["boot_disk"] == "d0"


async def test_create_vm_honours_wizard_placement(make_context, monkeypatch):
    """`placement` is a dict from the migration wizard: cluster overrides the
    connection's cluster, storage picks the container for the disks."""
    from phif.migrate.spec import VmSpec

    ctx = _ctx(make_context)
    ctx.runner = TaskRunner()
    c = NutanixConnector(ctx)
    seen = {}

    async def _fake_cluster(*, prefer=""):
        seen["prefer"] = prefer
        return "cluster-ext-9"

    monkeypatch.setattr(c, "_cluster_ext_id", _fake_cluster)
    r = await c.create_vm(VmSpec(name="app-01", source_ref="s"),
                          placement={"cluster": "other-cluster",
                                     "storage": "fa-container"})
    assert r.success
    assert seen["prefer"] == "other-cluster"
    # Remembered for the create_managed_disk calls that follow.
    assert c._placement_storage == "fa-container"


async def test_delete_disconnects_before_destroying(make_context, mock_array):
    """FlashArray refuses to destroy a connected volume (HTTP 400), and Nutanix
    leaves a removed vDisk's volume connected to an individual stargate HOST
    (not a host group) -- so a plain delete fails on exactly the volumes this is
    meant to clean up."""
    mock_array.volumes[SCOPED_VOL] = {"size": DISK_SIZE, "serial": "S1"}
    mock_array.volume_connections[SCOPED_VOL] = [
        {"host": "realm::nx-990-stargate-1", "host_group": None}]
    c = NutanixConnector(_ctx(make_context))

    r = await c.delete(LEAF_VOL)          # leaf name, as Prism reports it
    assert r.success
    assert r.artifacts["volume"] == SCOPED_VOL      # resolved to the pod-scoped name
    assert r.artifacts["disconnected"] == ["realm::nx-990-stargate-1"]
    ops = [op for op, _ in mock_array.calls]
    assert ops.index("disconnect_volume") < ops.index("delete_volume")


async def test_delete_prefers_host_group_when_present(make_context, mock_array):
    mock_array.volumes[SCOPED_VOL] = {"size": DISK_SIZE, "serial": "S1"}
    mock_array.volume_connections[SCOPED_VOL] = [
        {"host": "h1", "host_group": "ntnx-hg"}]
    c = NutanixConnector(_ctx(make_context))
    r = await c.delete(SCOPED_VOL)
    assert r.artifacts["disconnected"] == ["ntnx-hg"]


async def test_delete_unconnected_volume_needs_no_disconnect(make_context, mock_array):
    mock_array.volumes[SCOPED_VOL] = {"size": DISK_SIZE, "serial": "S1"}
    c = NutanixConnector(_ctx(make_context))
    r = await c.delete(SCOPED_VOL)
    assert r.success
    assert r.artifacts["disconnected"] == []
    assert "disconnect_volume" not in [op for op, _ in mock_array.calls]


# --------------------------------------------------------------------------- #
# If-Match / power-action paths — both broke a live migration
# --------------------------------------------------------------------------- #
async def test_mutation_fetches_an_etag_when_none_is_cached(make_context):
    """create_vm learns the VM id from a task and never GETs the VM, so nothing
    had cached an ETag. v4 then rejected the first disk add with HTTP 428
    Precondition Required, and the rollback's delete failed the same way,
    orphaning the destination VM."""
    created = _vm_payload([_disk(LEAF_VOL, index=0, disk_id="new-disk")])
    ctx = _ctx(make_context)
    ctx.runner = TaskRunner(vm_doc=created)
    c = NutanixConnector(ctx)
    # Simulate a freshly created VM: id known, ETag never fetched.
    assert "vm-ext-1" not in c._etags

    await c.create_managed_disk("vm-ext-1", size_bytes=DISK_SIZE, order=0)

    urls = [x["url"] for x in ctx.runner.calls]
    methods = [x["method"] for x in ctx.runner.calls]
    # A GET of the VM must precede the disk POST, so If-Match can be sent.
    get_i = next(i for i, (m, u) in enumerate(zip(methods, urls))
                 if m == "GET" and u.endswith("/vms/vm-ext-1"))
    post_i = next(i for i, (m, u) in enumerate(zip(methods, urls))
                  if m == "POST" and u.endswith("/disks"))
    assert get_i < post_i, "must GET the VM for an ETag before mutating it"
    post = ctx.runner.calls[post_i]
    assert post["headers"].get("If-Match") == "e1"


async def test_delete_vm_fetches_an_etag(make_context):
    ctx = _ctx(make_context)
    ctx.runner = TaskRunner(vm_doc=_vm_payload([]))
    c = NutanixConnector(ctx)
    await c.delete_vm("vm-ext-1", keep_disks=False)
    delete = [x for x in ctx.runner.calls if x["method"] == "DELETE"][0]
    assert delete["headers"].get("If-Match") == "e1"


@pytest.mark.parametrize("method,expected", [
    ("start_vm", "$actions/power-on"),
    ("stop_vm", "$actions/shutdown"),
])
async def test_power_actions_use_the_actions_segment(make_context, method, expected):
    """v4 power actions live under `$actions/`. The bare `/$power-on` form
    returns 404 — verified against a live Prism Central, where `$power-on` 404s
    while `$actions/power-on` returns 428 (valid path, needs If-Match)."""
    ctx = _ctx(make_context)
    ctx.runner = TaskRunner(vm_doc=_vm_payload([]))
    c = NutanixConnector(ctx)
    await getattr(c, method)("vm-ext-1")
    posts = [x["url"] for x in ctx.runner.calls if x["method"] == "POST"]
    assert any(expected in u for u in posts), posts
    # And never the bare form that 404s.
    assert not any("/$power-on" in u or "/$power-off" in u for u in posts), posts


async def test_stale_etag_is_dropped_after_a_mutation(make_context):
    """A mutation invalidates the ETag it consumed; replaying it would get a
    412 Precondition Failed, so the cache entry must be cleared."""
    ctx = _ctx(make_context)

    class NoEtagOnMutation(TaskRunner):
        async def run_http(self, method, url, *, headers=None, json_body=None, **kw):
            self.calls.append({"method": method, "url": url,
                               "headers": headers or {}, "json_body": json_body})
            if "/config/tasks/" in url:
                return {"status_code": 200, "json": self.task_doc, "text": "",
                        "headers": {}}
            if method == "GET":
                return {"status_code": 200, "json": _vm_payload([]), "text": "",
                        "headers": {"etag": "e1"}}
            # Mutation replies carry no ETag.
            return {"status_code": 202, "json": _task_ref(), "text": "", "headers": {}}

    ctx.runner = NoEtagOnMutation()
    c = NutanixConnector(ctx)
    await c._find_vm("vm-ext-1")
    assert c._etags["vm-ext-1"] == "e1"
    await c.delete_vm("vm-ext-1", keep_disks=False)
    assert "vm-ext-1" not in c._etags, "stale ETag must not be replayed"
