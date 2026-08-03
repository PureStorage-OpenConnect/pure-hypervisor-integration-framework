"""FlashArray connection management."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phif.api.schemas import ArrayCreate, ArrayOut
from phif.db.models import FlashArray
from phif.db.session import get_session
from phif.flasharray.client import build_client
from phif.vault import get_vault

router = APIRouter(prefix="/arrays", tags=["arrays"])


def _to_out(a: FlashArray) -> ArrayOut:
    return ArrayOut(id=a.id, name=a.name, mgmt_endpoint=a.mgmt_endpoint,
                    verify_ssl=a.verify_ssl, info=a.info or {})


@router.get("", response_model=list[ArrayOut])
async def list_arrays(session: AsyncSession = Depends(get_session)):
    res = await session.execute(select(FlashArray))
    return [_to_out(a) for a in res.scalars()]


@router.post("", response_model=ArrayOut, status_code=201)
async def add_array(body: ArrayCreate, session: AsyncSession = Depends(get_session)):
    if not body.api_token and not (body.username and body.password):
        raise HTTPException(400, "Provide either api_token or username+password")

    # Validate connectivity before persisting.
    client = build_client(body.mgmt_endpoint, api_token=body.api_token,
                          username=body.username, password=body.password,
                          verify_ssl=body.verify_ssl)
    try:
        info = await client.connect()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Could not connect to array: {exc}") from exc
    finally:
        await client.close()

    secrets = {k: v for k, v in {
        "api_token": body.api_token,
        "username": body.username,
        "password": body.password,
    }.items() if v}

    array = FlashArray(
        name=body.name,
        mgmt_endpoint=body.mgmt_endpoint,
        secret_blob=get_vault().encrypt(secrets),
        verify_ssl=body.verify_ssl,
        info=info,
    )
    session.add(array)
    await session.commit()
    return _to_out(array)


@router.get("/{array_id}", response_model=ArrayOut)
async def get_array(array_id: str, session: AsyncSession = Depends(get_session)):
    a = await session.get(FlashArray, array_id)
    if a is None:
        raise HTTPException(404, "Array not found")
    return _to_out(a)


@router.post("/{array_id}/validate", response_model=ArrayOut)
async def validate_array(array_id: str, session: AsyncSession = Depends(get_session)):
    a = await session.get(FlashArray, array_id)
    if a is None:
        raise HTTPException(404, "Array not found")
    secrets = get_vault().decrypt(a.secret_blob)
    client = build_client(a.mgmt_endpoint, api_token=secrets.get("api_token"),
                          username=secrets.get("username"), password=secrets.get("password"),
                          verify_ssl=a.verify_ssl)
    try:
        a.info = await client.info()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Validation failed: {exc}") from exc
    finally:
        await client.close()
    await session.commit()
    return _to_out(a)


@router.delete("/{array_id}", status_code=204)
async def delete_array(array_id: str, session: AsyncSession = Depends(get_session)):
    a = await session.get(FlashArray, array_id)
    if a is None:
        raise HTTPException(404, "Array not found")
    await session.delete(a)
    await session.commit()
