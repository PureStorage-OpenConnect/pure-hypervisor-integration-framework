"""Async SQLAlchemy engine + session factory."""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from phif.config import get_settings


def _async_url(url: str) -> str:
    # Accept a sync-style URL and upgrade the driver to the async psycopg driver.
    if url.startswith("postgresql+psycopg://") or url.startswith("postgresql+asyncpg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    if url.startswith("sqlite://") and "+aiosqlite" not in url:
        return url.replace("sqlite://", "sqlite+aiosqlite://", 1)
    return url


_settings = get_settings()
_url = _async_url(_settings.database_url)
engine = create_async_engine(_url, pool_pre_ping=True, future=True)

# For SQLite (dev/tests), enable WAL + a busy timeout so concurrent background
# jobs share one file database cleanly (concurrent readers + a serialized writer
# that waits for the lock instead of erroring). No-op for Postgres (production).
if _url.startswith("sqlite"):
    from sqlalchemy import event

    @event.listens_for(engine.sync_engine, "connect")
    def _sqlite_pragmas(dbapi_conn, _record):  # pragma: no cover - thin glue
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=5000")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.close()
SessionLocal = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency yielding a session."""
    async with SessionLocal() as session:
        yield session
