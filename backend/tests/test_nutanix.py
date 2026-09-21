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
    power_on = [x for x in ctx.runner.calls if "$power-on" in x["url"]][0]
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
    volume name back so the migration can overwrite it."""
    created = _vm_payload([_disk(LEAF_VOL, index=2, disk_id="new-disk")])
    routes = {"/vms/vm-ext-1": (created, {"Etag": "e1"})}
    ctx = _ctx(make_context, routes)
    c = NutanixConnector(ctx)

    r = await c.create_managed_disk("vm-ext-1", size_bytes=107374182400, order=2)
    assert r.success
    assert r.artifacts["fa_volume"] == LEAF_VOL

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

    r = await c.create_managed_disk("vm-ext-1", size_bytes=1024, order=0)
    assert not r.success
    assert "not FlashArray-backed" in r.message


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
