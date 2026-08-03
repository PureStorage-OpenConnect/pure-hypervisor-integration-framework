"""Startup reconciliation for interrupted jobs.

Job runners live in-process; there is no cross-restart resume. So any job still
``running``/``pending`` when the app starts was orphaned by the previous process
stopping and can never complete on its own. We mark such jobs ``failed`` at
startup so they don't linger forever (which otherwise makes the UI look hung).
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phif.db.models import Job, Migration

_ORPHAN_NOTE = "\n=== failed: interrupted by a backend restart (orphaned job) ==="


async def reconcile_orphaned_jobs(session: AsyncSession) -> int:
    """Mark all running/pending jobs as failed, and likewise any Migration still
    pending/running (its driving job is gone). Returns the count of jobs reconciled.

    A stuck Migration row otherwise lingers as "running" forever AND blocks
    re-migrating that VM (the in-progress guard), so it must be reconciled too.
    """
    rows = (await session.execute(
        select(Job).where(Job.status.in_(("running", "pending"))))).scalars().all()
    for job in rows:
        job.status = "failed"
        job.logs = (job.logs or "") + _ORPHAN_NOTE

    migs = (await session.execute(
        select(Migration).where(
            Migration.status.in_(("running", "pending"))))).scalars().all()
    for mig in migs:
        mig.status = "failed"
        mig.phase = "interrupted"

    if rows or migs:
        await session.commit()
    return len(rows)
