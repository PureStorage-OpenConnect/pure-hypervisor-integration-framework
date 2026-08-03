"""Tests for migration groups + the scheduler (DB-backed, no real connectors).

`run_migration` is monkeypatched to a fake that writes a terminal `Migration`
row keyed off the vm_ref (`fail-*` → failed, else succeeded) and returns
immediately, so the group runner's launch → poll → aggregate logic and the
scheduler's due/resume logic are exercised against the sqlite test DB without the
job manager or any connector.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from phif.db.models import Base, Migration, MigrationGroup
from phif.db.session import SessionLocal, engine


async def _ensure_tables() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


def _member(vm_ref: str) -> dict:
    return {"source_hypervisor_id": "src", "dest_hypervisor_id": "dst",
            "vm_ref": vm_ref, "network_map": {}, "options": {},
            "migration_id": None, "status": "pending"}


async def _mk_group(**kw) -> str:
    async with SessionLocal() as s:
        g = MigrationGroup(**kw)
        s.add(g)
        await s.commit()
        return g.id


@pytest.fixture
def fake_run_migration(monkeypatch):
    launches: list[tuple[str, datetime]] = []

    async def fake(session, *, source_hv_id, dest_hv_id, vm_ref, network_map, options):
        async with SessionLocal() as s:
            m = Migration(
                source_hypervisor_id=source_hv_id, dest_hypervisor_id=dest_hv_id,
                vm_ref=vm_ref, phase="done",
                status=("failed" if "fail" in vm_ref else "succeeded"))
            s.add(m)
            await s.commit()
            mid = m.id
        launches.append((vm_ref, datetime.now(timezone.utc)))
        return mid, f"job-{mid}"

    # patch where the group runner looks it up (same module)
    monkeypatch.setattr("phif.migrate.service.run_migration", fake)
    return launches


async def test_group_runs_members_and_reports_partial(fake_run_migration):
    await _ensure_tables()
    from phif.migrate.service import run_migration_group

    gid = await _mk_group(name="g1", status="pending", concurrency=1,
                          continue_on_error=True,
                          members=[_member("ok-1"), _member("fail-2"), _member("ok-3")])
    await run_migration_group(gid)

    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, gid)
    assert [m["status"] for m in g.members] == ["succeeded", "failed", "succeeded"]
    assert all(m["migration_id"] for m in g.members)
    assert g.status == "partial"            # mix of success + failure
    assert g.finished_at is not None
    assert len(fake_run_migration) == 3     # all members launched


async def test_group_all_succeed(fake_run_migration):
    await _ensure_tables()
    from phif.migrate.service import run_migration_group
    gid = await _mk_group(name="ok", status="pending", concurrency=2,
                          continue_on_error=True,
                          members=[_member("ok-1"), _member("ok-2")])
    await run_migration_group(gid)
    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, gid)
    assert g.status == "succeeded"


async def test_group_stop_on_error_skips_remaining(fake_run_migration):
    await _ensure_tables()
    from phif.migrate.service import run_migration_group
    # concurrency=1 + continue_on_error=False: first member fails → rest skipped.
    gid = await _mk_group(name="stop", status="pending", concurrency=1,
                          continue_on_error=False,
                          members=[_member("fail-1"), _member("ok-2"), _member("ok-3")])
    await run_migration_group(gid)
    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, gid)
    sts = [m["status"] for m in g.members]
    assert sts[0] == "failed"
    assert sts[1:] == ["skipped", "skipped"]   # not launched after the failure
    assert g.status == "failed"
    assert len(fake_run_migration) == 1        # only the first member launched


async def test_canceled_group_is_not_run(fake_run_migration):
    await _ensure_tables()
    from phif.migrate.service import run_migration_group
    gid = await _mk_group(name="canceled", status="canceled", concurrency=1,
                          continue_on_error=True, members=[_member("ok-1")])
    await run_migration_group(gid)
    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, gid)
    assert g.status == "canceled"
    assert len(fake_run_migration) == 0        # nothing launched


async def test_rerun_partial_group_resets_only_failed_members(fake_run_migration):
    """A partial group re-run resets non-succeeded members to pending (and clears
    their ids/error) while leaving succeeded members untouched — mirrors the API's
    run-now path so the group runner re-attempts only the failures."""
    await _ensure_tables()
    from phif.migrate.service import run_migration_group

    members = [
        {**_member("ok-1"), "status": "succeeded", "migration_id": "m1"},
        {**_member("fail-2"), "status": "rolled_back", "migration_id": "m2",
         "error": "boom"},
        {**_member("ok-3"), "status": "skipped"},
    ]
    gid = await _mk_group(name="p", status="partial", concurrency=1,
                          continue_on_error=True, members=members)

    # Emulate the run-now endpoint's reset, then re-run.
    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, gid)
        reset = []
        for m in g.members:
            m = dict(m)
            if (m.get("status") or "pending") != "succeeded":
                m.update(status="pending", error=None, migration_id=None, job_id=None)
            reset.append(m)
        g.members = reset
        g.status = "pending"
        g.finished_at = None
        await s.commit()

    fake_run_migration.clear()
    await run_migration_group(gid)

    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, gid)
    # Succeeded member kept its original id and was NOT re-run; the others re-ran.
    assert g.members[0]["status"] == "succeeded" and g.members[0]["migration_id"] == "m1"
    assert g.members[1]["status"] == "failed"      # fail-2 re-attempted, failed again
    assert g.members[2]["status"] == "succeeded"   # ok-3 re-attempted, succeeded
    launched = {ref for ref, _ in fake_run_migration}
    assert launched == {"fail-2", "ok-3"}          # ok-1 not re-run
    assert g.status == "partial"


async def test_scheduler_fires_due_group_only(fake_run_migration, monkeypatch):
    await _ensure_tables()
    import phif.jobs.scheduler as sched

    kicked: list[str] = []
    monkeypatch.setattr(sched, "kick", lambda gid: (kicked.append(gid), True)[1])

    now = datetime.now(timezone.utc)
    due = await _mk_group(name="due", status="scheduled",
                          scheduled_at=now - timedelta(minutes=1), members=[_member("ok-1")])
    future = await _mk_group(name="future", status="scheduled",
                             scheduled_at=now + timedelta(hours=1), members=[_member("ok-2")])

    async with SessionLocal() as s:
        n = await sched.run_scheduler_cycle(s)

    assert due in kicked and future not in kicked
    async with SessionLocal() as s:
        gdue = await s.get(MigrationGroup, due)
        gfut = await s.get(MigrationGroup, future)
    assert gdue.status == "pending"            # flipped from scheduled → pending
    assert gfut.status == "scheduled"          # left alone (not due)
    assert n >= 1
