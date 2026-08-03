"""Runtime application settings (the ``settings`` table).

Currently holds the cluster-monitoring configuration. Defaults are returned when
a key is absent so the app works before anything is written.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phif.db.models import Setting

MONITORING_KEY = "monitoring"
MONITORING_DEFAULTS: dict[str, Any] = {
    # Global on/off for the background cluster-monitoring loop (off by default).
    "enabled": False,
    # How often the loop checks each deployed cluster for membership drift.
    "interval_seconds": 300,
}


async def get_setting(session: AsyncSession, key: str, default: dict | None = None) -> dict:
    row = await session.get(Setting, key)
    return dict(row.value) if row and row.value is not None else dict(default or {})


async def set_setting(session: AsyncSession, key: str, value: dict) -> dict:
    row = await session.get(Setting, key)
    if row is None:
        row = Setting(key=key, value=value)
        session.add(row)
    else:
        row.value = value
        row.updated_at = datetime.now(timezone.utc)
    await session.commit()
    return value


async def get_monitoring(session: AsyncSession) -> dict:
    """Return the monitoring config merged over defaults."""
    stored = await get_setting(session, MONITORING_KEY, {})
    return {**MONITORING_DEFAULTS, **stored}


async def set_monitoring(session: AsyncSession, **patch: Any) -> dict:
    """Patch the monitoring config (only known keys), persist, and return it."""
    current = await get_monitoring(session)
    if "enabled" in patch and patch["enabled"] is not None:
        current["enabled"] = bool(patch["enabled"])
    if patch.get("interval_seconds") is not None:
        # Clamp to a sane floor so the loop can't busy-spin.
        current["interval_seconds"] = max(30, int(patch["interval_seconds"]))
    return await set_monitoring_value(session, current)


async def set_monitoring_value(session: AsyncSession, value: dict) -> dict:
    return await set_setting(session, MONITORING_KEY, value)
