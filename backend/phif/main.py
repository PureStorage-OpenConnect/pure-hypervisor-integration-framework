"""FastAPI application entrypoint."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from phif import __version__
from phif.api import (
    apikeys,
    arrays,
    connectors,
    hypervisors,
    jobs,
    migrations,
    settings as settings_api,
    vsphere_plugin,
)
from phif.config import get_settings
from phif.db.models import Base
from phif.db.session import engine

logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup (Alembic migrations are the production path; this
    # keeps dev/compose/test friction-free).
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Reconcile orphaned jobs interrupted by the previous process stopping.
    from phif.db.session import SessionLocal
    from phif.jobs.reconcile import reconcile_orphaned_jobs

    async with SessionLocal() as session:
        n = await reconcile_orphaned_jobs(session)
        if n:
            logging.info("Reconciled %d orphaned job(s) to failed on startup", n)

    # Start the background cluster-monitoring loop (no-op until enabled in settings).
    from phif.jobs.monitor import start_monitor, stop_monitor
    # Start the migration-group scheduler (fires scheduled groups + resumes any
    # group interrupted by a restart).
    from phif.jobs.scheduler import start_scheduler, stop_scheduler

    start_monitor()
    start_scheduler()
    try:
        yield
    finally:
        await stop_monitor()
        await stop_scheduler()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title="PHIF", version=__version__, lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    prefix = settings.api_prefix
    for module in (arrays, apikeys, connectors, hypervisors, jobs, migrations,
                   settings_api, vsphere_plugin):
        app.include_router(module.router, prefix=prefix)

    @app.get("/healthz")
    async def healthz():
        return {"status": "ok", "version": __version__, "mock_mode": settings.mock_mode}

    return app


app = create_app()
