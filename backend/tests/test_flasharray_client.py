"""Tests for the real PureFlashArrayClient call/idempotency behavior.

These don't touch a real array: a fake pypureclient-style SDK object records calls
and returns canned responses (with status_code/errors/items) the way the SDK does.
"""

import re

import pytest

from phif.flasharray.client import FlashArrayApiError, PureFlashArrayClient


class _Resp:
    def __init__(self, status_code=200, errors=None, items=None):
        self.status_code = status_code
        self.errors = errors
        self.items = items


class _FakeSDK:
    """Records calls; returns _Resp. Methods can be told to fail with a message."""

    def __init__(self, fail=None):
        self.calls = []
        self._fail = fail or {}  # method name -> error message

    def _resp(self, op):
        if op in self._fail:
            return _Resp(status_code=400, errors=self._fail[op])
        return _Resp(items=[])

    def post_host_groups(self, names=None):
        self.calls.append(("post_host_groups", tuple(names or [])))
        return self._resp("post_host_groups")

    def post_host_groups_hosts(self, group_names=None, member_names=None):
        self.calls.append(("post_host_groups_hosts", tuple(group_names or []),
                           tuple(member_names or [])))
        return self._resp("post_host_groups_hosts")


class _Ref:
    def __init__(self, name):
        self.name = name


class _Host:
    def __init__(self, name, iqns=None, wwns=None, nqns=None, host_group=None):
        self.name = name
        self.iqns = iqns or []
        self.wwns = wwns or []
        self.nqns = nqns or []
        self.host_group = _Ref(host_group) if host_group else None


class _HostsSDK(_FakeSDK):
    """Adds host inventory + create/patch so host-reuse logic can be tested."""

    def __init__(self, hosts=None, **kw):
        super().__init__(**kw)
        self._hosts = hosts or []

    def get_hosts(self, names=None, **kw):
        self.calls.append(("get_hosts", tuple(names or [])))
        items = self._hosts
        if names:
            items = [h for h in self._hosts if h.name in names]
        return _Resp(items=items)

    def post_hosts(self, names=None, host=None):
        self.calls.append(("post_hosts", tuple(names or [])))
        return self._resp("post_hosts")

    def patch_hosts(self, names=None, host=None):
        self.calls.append(("patch_hosts", tuple(names or [])))
        return self._resp("patch_hosts")


def _client(fake, monkeypatch):
    c = PureFlashArrayClient("10.0.0.1", api_token="t")
    monkeypatch.setattr(c, "_ensure_client", lambda: fake)
    return c


async def test_find_host_by_initiator_matches_iqn(monkeypatch):
    fake = _HostsSDK(hosts=[_Host("existing-1", iqns=["iqn.x:abc"]),
                           _Host("other", iqns=["iqn.y:zzz"])])
    c = _client(fake, monkeypatch)
    assert await c.find_host_by_initiator(iqns=["iqn.x:abc"]) == "existing-1"
    assert await c.find_host_by_initiator(iqns=["iqn.none:0"]) is None
    # no initiators given -> never matches (avoids reusing an arbitrary host)
    assert await c.find_host_by_initiator() is None


async def test_find_host_by_initiator_wwn_normalized(monkeypatch):
    # Array reports colon-separated upper-case WWN; operator passes bare lower-case.
    fake = _HostsSDK(hosts=[_Host("fchost", wwns=["52:4A:93:7A:BC:DE:00:11"])])
    c = _client(fake, monkeypatch)
    assert await c.find_host_by_initiator(wwns=["524a937abcde0011"]) == "fchost"


async def test_ensure_host_reuses_existing(monkeypatch):
    fake = _HostsSDK(hosts=[_Host("existing-1", iqns=["iqn.x:abc"])])
    c = _client(fake, monkeypatch)
    name = await c.ensure_host("would-be-new", iqns=["iqn.x:abc"])
    assert name == "existing-1"
    # MUST NOT create a second host for an already-owned initiator.
    assert not any(x[0] == "post_hosts" for x in fake.calls)


async def test_ensure_host_creates_when_absent(monkeypatch):
    fake = _HostsSDK(hosts=[])
    c = _client(fake, monkeypatch)
    created = {}

    async def _fake_create(name, *, iqns=None, wwns=None, nqns=None):
        created.update(name=name, iqns=iqns)
        return {"name": name}

    monkeypatch.setattr(c, "create_host", _fake_create)
    name = await c.ensure_host("fresh-host", iqns=["iqn.fresh:1"])
    assert name == "fresh-host"
    # No existing host owned the initiator -> a new host was created.
    assert created == {"name": "fresh-host", "iqns": ["iqn.fresh:1"]}


async def test_register_host_group_adopts_existing_single_group(monkeypatch):
    # The host already belongs to a host group -> ADOPT it instead of the requested.
    fake = _HostsSDK(hosts=[_Host("h1", iqns=["iqn.a:1"], host_group="ProdHG")])
    c = _client(fake, monkeypatch)
    res = await c.register_host_group("Requested", [{"name": "new-1", "iqns": ["iqn.a:1"]}])
    assert res["conflict"] is None
    assert res["host_group"] == "ProdHG"
    assert res["adopted"] is True
    assert res["reused"] == ["h1"]


async def test_register_host_group_conflict_multiple_groups(monkeypatch):
    # Hosts span TWO different groups -> conflict, descriptive, and NO mutation.
    fake = _HostsSDK(hosts=[_Host("h1", iqns=["iqn.a:1"], host_group="HG1"),
                            _Host("h2", iqns=["iqn.b:2"], host_group="HG2")])
    c = _client(fake, monkeypatch)
    res = await c.register_host_group(
        "Req", [{"name": "x", "iqns": ["iqn.a:1"]}, {"name": "y", "iqns": ["iqn.b:2"]}])
    assert res["conflict"]
    assert "HG1" in res["conflict"] and "HG2" in res["conflict"]
    # Nothing was created/grouped.
    assert not any(x[0] in ("post_hosts", "post_host_groups", "post_host_groups_hosts")
                   for x in fake.calls)


async def test_register_host_group_creates_when_none_exist(monkeypatch):
    fake = _HostsSDK(hosts=[])
    c = _client(fake, monkeypatch)

    async def _fake_create(name, *, iqns=None, wwns=None, nqns=None):
        fake._hosts.append(_Host(name, iqns=iqns or []))
        return {"name": name}

    monkeypatch.setattr(c, "create_host", _fake_create)
    res = await c.register_host_group("NewHG", [{"name": "n1", "iqns": ["iqn.fresh:1"]}])
    assert res["conflict"] is None
    assert res["host_group"] == "NewHG"
    assert res["adopted"] is False
    assert res["created"] == ["n1"]


async def test_create_host_group_adds_each_member_individually(monkeypatch):
    fake = _FakeSDK()
    c = _client(fake, monkeypatch)
    await c.create_host_group("hg", ["h1", "h2", "h3"])
    member_calls = [x for x in fake.calls if x[0] == "post_host_groups_hosts"]
    # One add per host (not a single atomic batch), so an already-member host
    # can't abort adding the others.
    assert len(member_calls) == 3
    assert [x[2] for x in member_calls] == [("h1",), ("h2",), ("h3",)]


async def test_create_host_group_tolerates_already_member(monkeypatch):
    # Membership add reports "already exists" -> swallowed, group still converges.
    fake = _FakeSDK(fail={"post_host_groups_hosts": "Member already exists in group"})
    c = _client(fake, monkeypatch)
    # Must not raise even though every per-host add returns "already exists".
    await c.create_host_group("hg", ["h1", "h2"])


async def test_call_idempotent_reraises_does_not_exist(monkeypatch):
    # A genuine "does not exist" must NOT be swallowed (regression: a bare "exist"
    # substring match used to hide it, so members silently weren't added).
    fake = _FakeSDK(fail={"post_host_groups_hosts": "Host h1 does not exist"})
    c = _client(fake, monkeypatch)
    with pytest.raises(FlashArrayApiError):
        await c.create_host_group("hg", ["h1"])


# --------------------------------------------------------------------------- #
# vVol resolution (PURE_VVOL_ID tag in the VASA namespace)
# --------------------------------------------------------------------------- #
class _Tag:
    def __init__(self, key, value, resource_name):
        self.key = key
        self.value = value
        self.resource = _Ref(resource_name)


class _Vol:
    def __init__(self, name, serial, provisioned=0, destroyed=False):
        self.name = name
        self.serial = serial
        self.provisioned = provisioned
        self.destroyed = destroyed


class _VvolSDK(_FakeSDK):
    """Volume + tag inventory shaped like a real array carrying vVols."""

    def __init__(self, tags=None, vols=None, **kw):
        super().__init__(**kw)
        self._tags = tags or []
        self._vols = vols or []

    def get_volumes_tags(self, namespaces=None, filter=None, **kw):
        self.calls.append(("get_volumes_tags", tuple(namespaces or []), filter))
        # Mirror the real API: tags are only returned for requested namespaces,
        # so a caller that forgets `namespaces` sees nothing useful.
        if not namespaces:
            return _Resp(items=[])
        items = self._tags
        # Honour the same key/value filter the real endpoint supports, so the
        # test fails if the caller stops narrowing server-side.
        if filter:
            key = re.search(r"key='([^']*)'", filter)
            val = re.search(r"value='([^']*)'", filter)
            if key:
                items = [t for t in items if t.key == key.group(1)]
            if val:
                items = [t for t in items if t.value == val.group(1)]
        return _Resp(items=items)

    def get_volumes(self, names=None, **kw):
        self.calls.append(("get_volumes", tuple(names or [])))
        items = self._vols
        if names:
            items = [v for v in self._vols if v.name in names]
            if not items:
                return _Resp(status_code=400, errors="Volume does not exist")
        return _Resp(items=items)


# Synthetic values in the exact shapes a live array/vCenter produces: a
# pod-scoped vVol volume group, a Data vVol member, and a 24-hex FA serial.
VVOL_ID = "rfc4122.00000000-1111-2222-3333-444444444444"
VVOL_VOL = "ds-example::vvol-example-vm-0a1b2c3d-vg/Data-4e5f6a7b"
VVOL_SERIAL = "ABCDEF0123456789ABCDEF01"


def _vvol_sdk(**kw):
    return _VvolSDK(
        tags=[_Tag("PURE_VVOL_ID", VVOL_ID, VVOL_VOL),
              _Tag("VMW_VVolType", "Data", VVOL_VOL)],
        vols=[_Vol(VVOL_VOL, VVOL_SERIAL, provisioned=107374182400)],
        **kw,
    )


async def test_find_volume_by_vvol_id_resolves_exactly(monkeypatch):
    c = _client(_vvol_sdk(), monkeypatch)
    got = await c.find_volume_by_vvol_id(VVOL_ID)
    assert got == {"name": VVOL_VOL, "serial": VVOL_SERIAL}


async def test_find_volume_by_vvol_id_requests_the_vasa_namespace(monkeypatch):
    """Regression: an unnamespaced tags query returns only user tags, which
    makes the VASA mapping look like it doesn't exist."""
    fake = _vvol_sdk()
    c = _client(fake, monkeypatch)
    await c.find_volume_by_vvol_id(VVOL_ID)
    tag_calls = [x for x in fake.calls if x[0] == "get_volumes_tags"]
    assert tag_calls, "must query volume tags"
    assert all("vasa-integration.purestorage.com" in x[1] for x in tag_calls)


async def test_find_volume_by_vvol_id_narrows_server_side(monkeypatch):
    """The namespace holds thousands of entries on a real array, so the match
    must be pushed into `filter` rather than scanned client-side."""
    fake = _vvol_sdk()
    c = _client(fake, monkeypatch)
    await c.find_volume_by_vvol_id(VVOL_ID)
    flt = [x[2] for x in fake.calls if x[0] == "get_volumes_tags"][0]
    assert flt, "tags query must pass a filter"
    # Filtering on key as well as value matters: PURE_VVOL_ID2 carries the same
    # value, so a value-only filter returns duplicate rows.
    assert "PURE_VVOL_ID'" in flt
    assert VVOL_ID in flt


async def test_find_volume_by_vvol_id_rejects_non_vvol_ids(monkeypatch):
    """Only 'rfc4122.<uuid>' can match; anything else short-circuits without
    reaching the array (and never lands in a filter expression)."""
    fake = _vvol_sdk()
    c = _client(fake, monkeypatch)
    for bad in ("naa.624a9370f269", "rfc4122.not-a-uuid", "' or '1'='1",
                "[vvol-datastore] rfc4122.bf01/disk.vmdk"):
        assert await c.find_volume_by_vvol_id(bad) is None
    assert not fake.calls


async def test_find_volume_by_vvol_id_unknown_id_is_none(monkeypatch):
    c = _client(_vvol_sdk(), monkeypatch)
    assert await c.find_volume_by_vvol_id("rfc4122.00000000-0000-0000-0000-000000000000") is None


async def test_find_volume_by_vvol_id_ignores_other_tag_keys(monkeypatch):
    """Only PURE_VVOL_ID is authoritative; other keys collide across volumes."""
    fake = _VvolSDK(
        # VMW_VVolName is not unique - it must never be used to resolve.
        tags=[_Tag("VMW_VVolName", VVOL_ID, VVOL_VOL)],
        vols=[_Vol(VVOL_VOL, VVOL_SERIAL)],
    )
    c = _client(fake, monkeypatch)
    assert await c.find_volume_by_vvol_id(VVOL_ID) is None


async def test_find_volume_by_vvol_id_skips_destroyed_volume(monkeypatch):
    """A tag can outlive its volume (destroyed, not yet eradicated)."""
    fake = _VvolSDK(
        tags=[_Tag("PURE_VVOL_ID", VVOL_ID, VVOL_VOL)],
        vols=[_Vol(VVOL_VOL, VVOL_SERIAL, destroyed=True)],
    )
    c = _client(fake, monkeypatch)
    assert await c.find_volume_by_vvol_id(VVOL_ID) is None


async def test_find_volume_by_vvol_id_blank_input(monkeypatch):
    fake = _vvol_sdk()
    c = _client(fake, monkeypatch)
    assert await c.find_volume_by_vvol_id("") is None
    assert await c.find_volume_by_vvol_id(None) is None
    # Cheap guard: no API traffic for an input that cannot match.
    assert not fake.calls


# --------------------------------------------------------------------------- #
# Pod/realm-scoped volume-name resolution
# --------------------------------------------------------------------------- #
class _ScopedSDK(_FakeSDK):
    """Volume inventory where names may be pod- or realm-scoped."""

    def __init__(self, vols=None, **kw):
        super().__init__(**kw)
        self._vols = vols or []

    def get_volumes(self, names=None, filter=None, **kw):
        self.calls.append(("get_volumes", tuple(names or []), filter))
        if names:
            items = [v for v in self._vols if v.name in names]
            if not items:
                return _Resp(status_code=400, errors="Volume does not exist")
            return _Resp(items=items)
        if filter:
            # Mirror the server's "name='*::<leaf>'" wildcard behaviour.
            m = re.search(r"name='\*::([^']*)'", filter)
            if m:
                leaf = m.group(1)
                return _Resp(items=[v for v in self._vols
                                    if v.name.split("::")[-1] == leaf
                                    and "::" in v.name])
        return _Resp(items=self._vols)


LEAF = "nx-1234567890123456789-51-dt"
SCOPED = f"FSA76::AHV76::{LEAF}"


async def test_resolve_volume_name_exact_match_wins(monkeypatch):
    fake = _ScopedSDK(vols=[_Vol(LEAF, "AAA")])
    c = _client(fake, monkeypatch)
    assert await c.resolve_volume_name(LEAF) == LEAF
    # An exact hit must not trigger a wildcard search.
    assert all(x[2] is None for x in fake.calls if x[0] == "get_volumes")


async def test_resolve_volume_name_finds_pod_scoped(monkeypatch):
    """Nutanix reports the leaf name while the array holds pod::realm::leaf."""
    fake = _ScopedSDK(vols=[_Vol(SCOPED, "BBB")])
    c = _client(fake, monkeypatch)
    assert await c.resolve_volume_name(LEAF) == SCOPED


async def test_resolve_volume_name_ambiguous_raises(monkeypatch):
    """The same leaf in two pods cannot be chosen between — picking one could
    attach another tenant's data."""
    fake = _ScopedSDK(vols=[_Vol(f"podA::{LEAF}", "AAA"),
                            _Vol(f"podB::{LEAF}", "BBB")])
    c = _client(fake, monkeypatch)
    with pytest.raises(FlashArrayApiError) as e:
        await c.resolve_volume_name(LEAF)
    assert "ambiguous" in str(e.value).lower()


async def test_resolve_volume_name_skips_destroyed(monkeypatch):
    fake = _ScopedSDK(vols=[_Vol(SCOPED, "BBB", destroyed=True)])
    c = _client(fake, monkeypatch)
    assert await c.resolve_volume_name(LEAF) is None


async def test_resolve_volume_name_already_qualified_not_found(monkeypatch):
    """A fully-qualified name that misses should not fall back to a suffix
    search, which could only match the same volume."""
    fake = _ScopedSDK(vols=[])
    c = _client(fake, monkeypatch)
    assert await c.resolve_volume_name(SCOPED) is None
    assert not [x for x in fake.calls if x[0] == "get_volumes" and x[2]]


async def test_resolve_volume_name_rejects_quote(monkeypatch):
    """A quote cannot occur in an FA volume name and would break the filter
    expression, so it is treated as unmatchable rather than escaped."""
    fake = _ScopedSDK(vols=[_Vol(SCOPED, "BBB")])
    c = _client(fake, monkeypatch)
    assert await c.resolve_volume_name("nx-1' or name='*") is None


async def test_resolve_volume_name_blank(monkeypatch):
    fake = _ScopedSDK(vols=[])
    c = _client(fake, monkeypatch)
    assert await c.resolve_volume_name("") is None
    assert await c.resolve_volume_name(None) is None
    assert not fake.calls
