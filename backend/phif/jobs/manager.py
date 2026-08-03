"""Job manager: runs connector operations as tracked, log-streaming jobs.

The manager:
* creates a :class:`~phif.db.models.Job` row,
* runs the supplied coroutine factory with a log emitter that appends to the job
  and fans out to any live WebSocket subscribers,
* records final status/result.

Log streaming uses per-job in-memory asyncio queues (pub/sub). For multi-replica
deployments this would move to Redis; the interface here stays the same.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy import update

from phif.config import get_settings
from phif.db.models import Job
from phif.db.session import SessionLocal
from phif.connectors.base import OpResult

log = logging.getLogger(__name__)

# A coroutine factory receiving the log emitter and returning an OpResult.
OpFactory = Callable[[Callable[[str], Awaitable[None]]], Awaitable[OpResult]]


class JobManager:
    def __init__(self) -> None:
        self._subscribers: dict[str, list[asyncio.Queue]] = defaultdict(list)
        self._semaphore = asyncio.Semaphore(get_settings().max_concurrent_jobs)
        self._tasks: dict[str, asyncio.Task] = {}

    # ----------------------------------------------------------- pub/sub ---
    def subscribe(self, job_id: str) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers[job_id].append(q)
        return q

    def unsubscribe(self, job_id: str, q: asyncio.Queue) -> None:
        subs = self._subscribers.get(job_id)
        if subs and q in subs:
            subs.remove(q)

    async def _publish(self, job_id: str, line: str) -> None:
        for q in list(self._subscribers.get(job_id, [])):
            await q.put(line)

    # ------------------------------------------------------------- run ---
    async def create_and_run(self, *, action: str, op_factory: OpFactory,
                             hypervisor_id: str | None = None,
                             connector_key: str | None = None,
                             params: dict[str, Any] | None = None) -> str:
        """Create a job row and launch it in the background. Returns the job id."""
        async with SessionLocal() as session:
            job = Job(
                action=action,
                hypervisor_id=hypervisor_id,
                connector_key=connector_key,
                params=_strip_secrets(params or {}),
                status="pending",
            )
            session.add(job)
            await session.commit()
            job_id = job.id

        self._tasks[job_id] = asyncio.create_task(self._run(job_id, op_factory))
        return job_id

    async def _run(self, job_id: str, op_factory: OpFactory) -> None:
        buffer: list[str] = []

        async def emit(line: str) -> None:
            buffer.append(line)
            await self._publish(job_id, line)

        async with self._semaphore:
            await self._set_status(job_id, "running", started=True)
            await emit(f"=== job {job_id} started ===")
            try:
                result = await op_factory(emit)
                status = "succeeded" if result.success else "failed"
                await emit(f"=== {status}: {result.message} ===")
                await self._finalize(job_id, status, result, buffer)
            except Exception as exc:  # noqa: BLE001 — surface any failure to the job
                log.exception("Job %s failed", job_id)
                await emit(f"=== failed: {exc} ===")
                await self._finalize(
                    job_id, "failed", OpResult.fail(str(exc)), buffer
                )
            finally:
                await self._publish(job_id, "__END__")

    # ---------------------------------------------------------- db helpers ---
    async def _set_status(self, job_id: str, status: str, *, started: bool = False) -> None:
        values: dict[str, Any] = {"status": status}
        if started:
            values["started_at"] = datetime.now(timezone.utc)
        async with SessionLocal() as session:
            await session.execute(update(Job).where(Job.id == job_id).values(**values))
            await session.commit()

    async def _finalize(self, job_id: str, status: str, result: OpResult,
                        buffer: list[str]) -> None:
        async with SessionLocal() as session:
            await session.execute(
                update(Job).where(Job.id == job_id).values(
                    status=status,
                    result={"success": result.success, "message": result.message,
                            "data": result.data, "artifacts": result.artifacts},
                    logs="\n".join(buffer),
                    finished_at=datetime.now(timezone.utc),
                )
            )
            await session.commit()


def _strip_secrets(params: dict[str, Any]) -> dict[str, Any]:
    redacted = {}
    for k, v in params.items():
        if any(s in k.lower() for s in ("token", "password", "secret", "key")):
            redacted[k] = "***"
        else:
            redacted[k] = v
    return redacted


_manager: JobManager | None = None


def get_job_manager() -> JobManager:
    global _manager
    if _manager is None:
        _manager = JobManager()
    return _manager
