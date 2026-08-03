"""Connector catalog — what hypervisors are available and what they can do.

Drives the capability-aware UI: the frontend renders the 'Add hypervisor' form
from ``target_schema`` and the day-2 action panel from ``actions``.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from phif.connectors.registry import discover, list_descriptors

router = APIRouter(prefix="/connectors", tags=["connectors"])


@router.get("")
async def list_connectors():
    return list_descriptors()


@router.get("/{key}")
async def get_connector(key: str):
    cls = discover().get(key)
    if cls is None:
        raise HTTPException(404, f"Unknown connector {key!r}")
    return cls.descriptor()
