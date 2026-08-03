"""Background cluster-monitoring loop (pure DETECTOR — never changes anything).

When enabled (Settings ``monitoring.enabled``), periodically calls each deployed
hypervisor's read-only ``assess_cluster`` and stores the result in
``state["cluster_assessment"]`` — which new hosts are present (with a readiness
verdict) and which hosts have departed. It does NOT configure or remove anything:
the admin applies changes from the UI via the "Deploy to new hosts" button (which
runs ``reconcile_cluster``) and a separate removal confirmation.

Disabled by default; toggled via ``PATCH /api/settings/monitoring``. A hypervisor
can opt out individually via ``state["monitoring_disabled"]``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select

from phif.api import service
from phif.connectors.base import Capability
from phif.connectors.registry import get_connector_class
from phif.db.models import Hypervisor
from phif.db.session import SessionLocal
from phif.db.settings_store import get_monitoring

log = logging.getLogger(__name__)

# Created in start_monitor() so it binds to the running loop (a module-level
# Event would bind to the import-time loop and break under per-test loops).
_stop: asyncio.Event | None = None
_task: asyncio.Task | None = None


async def _noop_emit(_line: str) -> None:
    return None


async def _assess(session, hv: Hypervisor) -> dict:
    """Run the connector's read-only assess_cluster and return its data dict."""
    connector = await service.build_connector(session, hv, _noop_emit)
    result = await connector.assess_cluster()
    data = dict(result.data or {})
    data["ok"] = result.success
    data["checked_at"] = datetime.now(timezone.utc).isoformat()
    return data


async def run_monitor_cycle(session) -> int:
    """One read-only pass over eligible hypervisors. Returns how many showed drift.

    Persists each cluster's assessment to ``state["cluster_assessment"]`` so the UI
    can surface new/departed hosts. Changes nothing on the array or the hosts.
    """
    flagged = 0
    res = await session.execute(select(Hypervisor))
    for hv in res.scalars().all():
        if hv.status != "deployed":
            continue
        if (hv.state or {}).get("monitoring_disabled"):
            continue
        cls = get_connector_class(hv.connector_key)
        if cls is None or Capability.RECONCILE_CLUSTER not in cls.CAPABILITIES:
            continue
        try:
            assessment = await _assess(session, hv)
        except Exception as exc:  # noqa: BLE001 - never let one host break the loop
            log.warning("monitor: assess failed for %s: %s", hv.name, exc)
            continue
        has_drift = bool(assessment.get("new_hosts") or assessment.get("departed_hosts"))
        # Persist only the relevant fields; skip a write if nothing changed.
        prev = (hv.state or {}).get("cluster_assessment") or {}
        changed = (prev.get("new_hosts") != assessment.get("new_hosts")
                   or prev.get("departed_hosts") != assessment.get("departed_hosts"))
        if changed:
            state = dict(hv.state or {})
            state["cluster_assessment"] = assessment
            hv.state = state
            await session.commit()
            if has_drift:
                log.info("monitor: %s drift -> new=%s departed=%s", hv.name,
                         [h.get("node") for h in assessment.get("new_hosts", [])],
                         assessment.get("departed_hosts"))
        if has_drift:
            flagged += 1
    return flagged


async def monitor_loop() -> None:
    """Run cycles forever (until stopped), sleeping the configured interval."""
    log.info("cluster monitor loop started")
    assert _stop is not None
    while not _stop.is_set():
        interval = 300
        try:
            async with SessionLocal() as session:
                cfg = await get_monitoring(session)
                interval = int(cfg.get("interval_seconds", 300))
                if cfg.get("enabled"):
                    n = await run_monitor_cycle(session)
                    if n:
                        log.info("monitor: launched %d reconcile job(s)", n)
        except Exception:  # noqa: BLE001 - keep the loop alive across transient errors
            log.exception("monitor: cycle failed")
        try:
            await asyncio.wait_for(_stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
    log.info("cluster monitor loop stopped")


def start_monitor() -> None:
    global _task, _stop
    if _task is None or _task.done():
        _stop = asyncio.Event()  # bind to the currently-running loop
        _task = asyncio.create_task(monitor_loop())


async def stop_monitor() -> None:
    global _task
    if _stop is not None:
        _stop.set()
    if _task is not None:
        try:
            await asyncio.wait_for(_task, timeout=5)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            _task.cancel()
        _task = None
