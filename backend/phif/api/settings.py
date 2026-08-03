"""Runtime application settings (monitoring toggle, etc.)."""

from __future__ import annotations

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from phif.db.session import get_session
from phif.db.settings_store import get_monitoring, set_monitoring

router = APIRouter(prefix="/settings", tags=["settings"])


class MonitoringOut(BaseModel):
    enabled: bool
    interval_seconds: int


class MonitoringPatch(BaseModel):
    enabled: bool | None = None
    interval_seconds: int | None = None


class SettingsOut(BaseModel):
    monitoring: MonitoringOut


@router.get("", response_model=SettingsOut)
async def get_settings_view(session: AsyncSession = Depends(get_session)):
    mon = await get_monitoring(session)
    return SettingsOut(monitoring=MonitoringOut(**mon))


@router.patch("/monitoring", response_model=MonitoringOut)
async def patch_monitoring(body: MonitoringPatch,
                           session: AsyncSession = Depends(get_session)):
    mon = await set_monitoring(session, enabled=body.enabled,
                               interval_seconds=body.interval_seconds)
    return MonitoringOut(**mon)
