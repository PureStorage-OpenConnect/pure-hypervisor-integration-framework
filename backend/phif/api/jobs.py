"""Job status + live log streaming (WebSocket)."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phif.api.schemas import JobOut
from phif.db.models import Job
from phif.db.session import SessionLocal, get_session
from phif.jobs.manager import get_job_manager

router = APIRouter(prefix="/jobs", tags=["jobs"])


def _to_out(j: Job) -> JobOut:
    return JobOut(id=j.id, action=j.action, hypervisor_id=j.hypervisor_id,
                  connector_key=j.connector_key, status=j.status,
                  result=j.result or {}, logs=j.logs or "")


@router.get("", response_model=list[JobOut])
async def list_jobs(session: AsyncSession = Depends(get_session)):
    res = await session.execute(select(Job).order_by(Job.created_at.desc()).limit(100))
    return [_to_out(j) for j in res.scalars()]


@router.get("/{job_id}", response_model=JobOut)
async def get_job(job_id: str, session: AsyncSession = Depends(get_session)):
    j = await session.get(Job, job_id)
    if j is None:
        raise HTTPException(404, "Job not found")
    return _to_out(j)


@router.websocket("/{job_id}/logs")
async def stream_logs(websocket: WebSocket, job_id: str):
    """Stream a job's logs live. Replays buffered logs, then tails new lines."""
    await websocket.accept()
    manager = get_job_manager()

    # Replay whatever is already persisted (job may have started/finished).
    async with SessionLocal() as session:
        job = await session.get(Job, job_id)
        if job is None:
            await websocket.send_text("__ERROR__ job not found")
            await websocket.close()
            return
        if job.logs:
            for line in job.logs.splitlines():
                await websocket.send_text(line)
        if job.status in ("succeeded", "failed", "canceled"):
            await websocket.send_text("__END__")
            await websocket.close()
            return

    queue = manager.subscribe(job_id)
    try:
        while True:
            line = await queue.get()
            await websocket.send_text(line)
            if line == "__END__":
                break
    except WebSocketDisconnect:
        pass
    finally:
        manager.unsubscribe(job_id, queue)
        await websocket.close()
