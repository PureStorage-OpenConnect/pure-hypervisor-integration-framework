"""Background scheduler for migration groups.

Fires migration groups whose ``scheduled_at`` has arrived, and resumes groups
left ``pending``/``running`` by a backend restart (the group runner is resumable
— it skips members that already reached a terminal state). Mirrors the
``monitor.py`` start/stop pattern.

A group is run by an in-process asyncio task (``run_migration_group``); the
scheduler tracks live tasks in ``_active`` so a group is never double-launched.
``kick()`` is the single launch entrypoint (used by the API for run-now /
immediate groups and by the polling loop for due/interrupted groups).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from sqlalchemy import select

from phif.db.models import MigrationGroup
from phif.db.session import SessionLocal

log = logging.getLogger(__name__)

POLL_SECONDS = 15

_stop: asyncio.Event | None = None
_task: asyncio.Task | None = None
# group_id -> running runner task (so we never launch a second runner for one group)
_active: dict[str, asyncio.Task] = {}


def _aware(dt: datetime) -> datetime:
    """Treat a naive datetime (some DB backends drop tzinfo) as UTC."""
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def kick(group_id: str) -> bool:
    """Launch the group runner for ``group_id`` unless one is already live.

    Returns True if a new runner task was started. The single launch path for
    both the API (run-now / immediate) and the polling loop."""
    from phif.migrate.service import run_migration_group

    existing = _active.get(group_id)
    if existing is not None and not existing.done():
        return False
    task = asyncio.create_task(run_migration_group(group_id))
    _active[group_id] = task
    task.add_done_callback(lambda _t, gid=group_id: _active.pop(gid, None))
    return True


async def run_scheduler_cycle(session) -> int:
    """One pass: launch scheduled groups that are due, and resume any
    pending/running group with no live runner (e.g. after a restart). Returns the
    number of runners launched this pass."""
    now = datetime.now(timezone.utc)
    res = await session.execute(
        select(MigrationGroup).where(
            MigrationGroup.status.in_(("scheduled", "pending", "running"))))
    to_launch: list[str] = []
    for g in res.scalars().all():
        if g.id in _active and not _active[g.id].done():
            continue  # already running in this process
        if g.status == "scheduled":
            if g.scheduled_at is None or _aware(g.scheduled_at) > now:
                continue  # not due yet
            g.status = "pending"
            g.updated_at = now
        # pending or running (interrupted) -> (re)launch the resumable runner
        to_launch.append(g.id)
    await session.commit()
    launched = 0
    for gid in to_launch:
        if kick(gid):
            launched += 1
    return launched


async def scheduler_loop() -> None:
    log.info("migration scheduler loop started")
    assert _stop is not None
    while not _stop.is_set():
        try:
            async with SessionLocal() as session:
                n = await run_scheduler_cycle(session)
                if n:
                    log.info("scheduler: launched %d migration group(s)", n)
        except Exception:  # noqa: BLE001 — keep the loop alive across transient errors
            log.exception("scheduler: cycle failed")
        try:
            await asyncio.wait_for(_stop.wait(), timeout=POLL_SECONDS)
        except asyncio.TimeoutError:
            pass
    log.info("migration scheduler loop stopped")


def start_scheduler() -> None:
    global _task, _stop
    if _task is None or _task.done():
        _stop = asyncio.Event()
        _task = asyncio.create_task(scheduler_loop())


async def stop_scheduler() -> None:
    global _task
    if _stop is not None:
        _stop.set()
    if _task is not None:
        try:
            await asyncio.wait_for(_task, timeout=5)
        except (asyncio.TimeoutError, asyncio.CancelledError):
            _task.cancel()
        _task = None
