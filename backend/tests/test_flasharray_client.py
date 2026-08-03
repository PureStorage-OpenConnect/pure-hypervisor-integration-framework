"""Tests for the real PureFlashArrayClient call/idempotency behavior.

These don't touch a real array: a fake pypureclient-style SDK object records calls
and returns canned responses (with status_code/errors/items) the way the SDK does.
"""

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
