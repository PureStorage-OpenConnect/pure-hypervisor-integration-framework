"""API-key minting for integrations that authenticate to the FlashArray.

Generates (and rotates) FlashArray API tokens on demand — the CSI driver, Cinder
driver, Proxmox plugin and vSphere plugin all consume one. The token value is
returned exactly once at creation; afterward only metadata (which integration
uses it) is exposed.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phif.api.schemas import ApiKeyCreate, ApiKeyOut
from phif.db.models import ApiKey, FlashArray
from phif.db.session import get_session
from phif.flasharray.client import build_client
from phif.vault import get_vault

router = APIRouter(prefix="/api-keys", tags=["api-keys"])


@router.get("", response_model=list[ApiKeyOut])
async def list_api_keys(session: AsyncSession = Depends(get_session)):
    res = await session.execute(select(ApiKey))
    return [
        ApiKeyOut(id=k.id, array_id=k.array_id, array_user=k.array_user, purpose=k.purpose)
        for k in res.scalars()
    ]


async def _array_client(session: AsyncSession, array_id: str) -> tuple[FlashArray, object]:
    array = await session.get(FlashArray, array_id)
    if array is None:
        raise HTTPException(404, "Array not found")
    secrets = get_vault().decrypt(array.secret_blob)
    client = build_client(array.mgmt_endpoint, api_token=secrets.get("api_token"),
                          username=secrets.get("username"), password=secrets.get("password"),
                          verify_ssl=array.verify_ssl)
    return array, client


@router.post("", response_model=ApiKeyOut, status_code=201)
async def mint_api_key(body: ApiKeyCreate, session: AsyncSession = Depends(get_session)):
    array = await session.get(FlashArray, body.array_id)
    if array is None:
        raise HTTPException(404, "Array not found")

    if body.use_existing:
        # Reuse the token the array was connected with — no minting (works when
        # you can't create new array users/tokens).
        existing = get_vault().decrypt(array.secret_blob).get("api_token")
        if not existing:
            raise HTTPException(
                400,
                "This array was not connected with an API token, so there is no "
                "existing token to reuse. Reconnect it with a token, or mint one.",
            )
        token = existing
        array_user = body.array_user or "existing-array-token"
    else:
        secrets = get_vault().decrypt(array.secret_blob)
        client = build_client(array.mgmt_endpoint, api_token=secrets.get("api_token"),
                              username=secrets.get("username"), password=secrets.get("password"),
                              verify_ssl=array.verify_ssl)
        if not body.array_user:
            raise HTTPException(400, "array_user is required when minting a new token")
        try:
            token = await client.create_api_token(body.array_user)
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(400, f"Failed to mint API token: {exc}") from exc
        finally:
            await client.close()
        array_user = body.array_user

    key = ApiKey(
        array_id=array.id,
        array_user=array_user,
        purpose=body.purpose,
        secret_blob=get_vault().encrypt({"api_token": token}),
    )
    session.add(key)
    await session.commit()
    return ApiKeyOut(id=key.id, array_id=key.array_id, array_user=key.array_user,
                     purpose=key.purpose, token=token)


@router.post("/{key_id}/rotate", response_model=ApiKeyOut)
async def rotate_api_key(key_id: str, session: AsyncSession = Depends(get_session)):
    key = await session.get(ApiKey, key_id)
    if key is None:
        raise HTTPException(404, "API key not found")
    _, client = await _array_client(session, key.array_id)
    try:
        # Re-minting a token for the same user rotates it on the array.
        await client.delete_api_token(key.array_user)
        token = await client.create_api_token(key.array_user)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Failed to rotate API token: {exc}") from exc
    finally:
        await client.close()
    key.secret_blob = get_vault().encrypt({"api_token": token})
    await session.commit()
    return ApiKeyOut(id=key.id, array_id=key.array_id, array_user=key.array_user,
                     purpose=key.purpose, token=token)


@router.delete("/{key_id}", status_code=204)
async def delete_api_key(key_id: str, session: AsyncSession = Depends(get_session)):
    key = await session.get(ApiKey, key_id)
    if key is None:
        raise HTTPException(404, "API key not found")
    await session.delete(key)
    await session.commit()
