"""Shared test fixtures. Forces mock mode + a SQLite DB so no real I/O occurs.

Uses a temp FILE (not ``:memory:``): under the suite's concurrent background jobs
the in-memory + connection-pool combination opens several independent ``:memory:``
databases, so writes on one connection are invisible on another (lost writes /
starvation flakes). A file is one shared database across all connections; WAL +
busy_timeout (set in db/session.py) give clean concurrent access. The file is
removed at session start so each run is clean (tests reuse fixed names)."""

import os
import tempfile

_TEST_DB = os.path.join(tempfile.gettempdir(), "phif-pytest.sqlite")
for _ext in ("", "-wal", "-shm", "-journal"):
    try:
        os.remove(_TEST_DB + _ext)
    except OSError:
        pass

os.environ.setdefault("PHIF_MOCK_MODE", "1")
os.environ.setdefault(
    "PHIF_DATABASE_URL", "sqlite+aiosqlite:///" + _TEST_DB.replace(os.sep, "/"))
os.environ.setdefault("PHIF_VAULT_MASTER_KEY", "")  # dev key

import pytest

from phif.connectors.base import ConnectorContext, HypervisorTarget
from phif.flasharray.client import MockFlashArrayClient
from phif.jobs.runner import JobRunner


@pytest.fixture
def captured_logs() -> list[str]:
    return []


@pytest.fixture
def log_emitter(captured_logs):
    async def _emit(line: str) -> None:
        captured_logs.append(line)

    return _emit


@pytest.fixture
def mock_array() -> MockFlashArrayClient:
    return MockFlashArrayClient()


@pytest.fixture
def make_context(log_emitter, mock_array):
    def _make(connector_key="example", connection=None, secrets=None, with_array=True):
        target = HypervisorTarget(
            id="t1",
            connector_key=connector_key,
            name="test-target",
            connection=connection or {"host": "mgr.test.local", "username": "admin"},
            secrets=secrets or {"password": "s3cret"},
        )
        return ConnectorContext(
            target=target,
            log=log_emitter,
            runner=JobRunner(log_emitter),
            array=mock_array if with_array else None,
        )

    return _make
