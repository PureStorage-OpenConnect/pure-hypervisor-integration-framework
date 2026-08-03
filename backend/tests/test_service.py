"""Tests for phif.api.service helpers (host-group persistence)."""

from types import SimpleNamespace

from phif.api.service import _persist_host_group
from phif.connectors.base import OpResult


def test_persist_host_group_writes_adopted_group():
    hv = SimpleNamespace(connection={"host_group": "Requested", "protocol": "iscsi"})
    result = OpResult.ok("ok", artifacts={"host_group": "AdoptedHG",
                                          "adopted_host_group": True})
    changed = _persist_host_group(hv, result)
    assert changed == "AdoptedHG"
    assert hv.connection["host_group"] == "AdoptedHG"
    # other connection keys are preserved
    assert hv.connection["protocol"] == "iscsi"


def test_persist_host_group_noop_when_unchanged():
    hv = SimpleNamespace(connection={"host_group": "Same"})
    result = OpResult.ok("ok", artifacts={"host_group": "Same"})
    assert _persist_host_group(hv, result) is None
    assert hv.connection == {"host_group": "Same"}


def test_persist_host_group_noop_when_no_artifact():
    hv = SimpleNamespace(connection={"host_group": "Keep"})
    result = OpResult.ok("ok", artifacts={"protocol": "fc"})
    assert _persist_host_group(hv, result) is None
    assert hv.connection["host_group"] == "Keep"


def test_persist_host_group_handles_empty_connection():
    hv = SimpleNamespace(connection=None)
    result = OpResult.ok("ok", artifacts={"host_group": "NewHG"})
    assert _persist_host_group(hv, result) == "NewHG"
    assert hv.connection == {"host_group": "NewHG"}
