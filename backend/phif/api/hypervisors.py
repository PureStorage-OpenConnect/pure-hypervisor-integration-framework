"""Hypervisor target management + operation dispatch."""

from __future__ import annotations

from fastapi import APIRouter, Body, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from phif.api import service
from phif.api.schemas import (
    HypervisorCreate,
    HypervisorOut,
    HypervisorUpdate,
    OperationRequest,
    WizardPrepareRequest,
    WizardRequest,
    WizardRunRequest,
)
from phif.connectors.base import ConnectionValidationError
from phif.connectors.registry import get_connector_class
from phif.db.models import Hypervisor
from phif.db.session import get_session
from phif.vault import get_vault

router = APIRouter(prefix="/hypervisors", tags=["hypervisors"])


def _to_out(h: Hypervisor) -> HypervisorOut:
    return HypervisorOut(id=h.id, name=h.name, connector_key=h.connector_key,
                         connection=h.connection or {}, array_id=h.array_id,
                         status=h.status, state=h.state or {})


@router.get("", response_model=list[HypervisorOut])
async def list_hvs(session: AsyncSession = Depends(get_session)):
    return [_to_out(h) for h in await service.list_hypervisors(session)]


@router.post("/wizard", status_code=202)
async def wizard(body: WizardRequest, session: AsyncSession = Depends(get_session)):
    """One-shot deployment: create the hypervisor and run validate + the full
    connector wizard sequence across the cluster (or a single node) as one job."""
    if get_connector_class(body.connector_key) is None:
        raise HTTPException(400, f"Unknown connector {body.connector_key!r}")
    hv_id, job_id = await service.run_wizard(
        session, name=body.name, connector_key=body.connector_key,
        connection=body.connection, secrets=body.secrets, array_id=body.array_id,
        scope=body.scope, params=body.params,
    )
    return {"hypervisor_id": hv_id, "job_id": job_id}


@router.post("/wizard/prepare", status_code=201)
async def wizard_prepare(body: WizardPrepareRequest,
                         session: AsyncSession = Depends(get_session)):
    """Wizard page 1: create the hypervisor + validate the connection. The UI then
    discovers nodes (GET /{id}/nodes) and interfaces (GET /{id}/discover/{kind})
    for the interface-selection page, then calls /{id}/wizard/run to deploy."""
    if get_connector_class(body.connector_key) is None:
        raise HTTPException(400, f"Unknown connector {body.connector_key!r}")
    try:
        hv = await service.prepare_wizard_hypervisor(
            session, name=body.name, connector_key=body.connector_key,
            connection=body.connection, secrets=body.secrets, array_id=body.array_id)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"hypervisor_id": hv.id}


@router.post("/{hv_id}/wizard/run", status_code=202)
async def wizard_run(hv_id: str, body: WizardRunRequest,
                     session: AsyncSession = Depends(get_session)):
    """Wizard page 2: deploy a prepared hypervisor with the selected interfaces."""
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")
    job_id = await service.run_wizard_job(session, h, scope=body.scope, params=body.params)
    return {"job_id": job_id}


@router.get("/{hv_id}/nodes")
async def list_nodes(hv_id: str, session: AsyncSession = Depends(get_session)):
    """Discover the cluster/pool member nodes for this hypervisor."""
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")

    async def _noop(_: str) -> None:
        return None

    connector = await service.build_connector(session, h, _noop)
    try:
        nodes = await connector.list_nodes()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Node discovery failed: {exc}") from exc
    return {"nodes": [n.to_dict() for n in nodes]}


@router.post("", response_model=HypervisorOut, status_code=201)
async def add_hv(body: HypervisorCreate, session: AsyncSession = Depends(get_session)):
    if get_connector_class(body.connector_key) is None:
        raise HTTPException(400, f"Unknown connector {body.connector_key!r}")
    h = Hypervisor(
        name=body.name,
        connector_key=body.connector_key,
        connection=body.connection,
        secret_blob=get_vault().encrypt(body.secrets) if body.secrets else "",
        array_id=body.array_id,
    )
    session.add(h)
    await session.commit()
    return _to_out(h)


@router.patch("/{hv_id}", response_model=HypervisorOut)
async def update_hv(hv_id: str, body: HypervisorUpdate,
                    session: AsyncSession = Depends(get_session)):
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")

    data = body.model_dump(exclude_unset=True)  # only fields the client sent
    if "name" in data and data["name"]:
        h.name = data["name"]
    if "connection" in data and data["connection"] is not None:
        h.connection = data["connection"]
    if "array_id" in data:
        h.array_id = data["array_id"] or None
    if "secrets" in data and data["secrets"]:
        # Merge: a provided non-empty value overwrites; blank means "keep existing".
        existing = get_vault().decrypt(h.secret_blob) if h.secret_blob else {}
        for k, v in data["secrets"].items():
            if v in ("", None):
                continue
            existing[k] = v
        h.secret_blob = get_vault().encrypt(existing)
    await session.commit()
    await session.refresh(h)
    return _to_out(h)


@router.patch("/{hv_id}/monitoring", response_model=HypervisorOut)
async def set_hv_monitoring(hv_id: str, enabled: bool = Body(..., embed=True),
                            session: AsyncSession = Depends(get_session)):
    """Per-hypervisor opt-out for the cluster-monitoring loop.

    ``enabled=false`` sets ``state["monitoring_disabled"]`` so the global monitor
    skips this hypervisor; ``enabled=true`` clears it.
    """
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")
    state = dict(h.state or {})
    state["monitoring_disabled"] = not enabled
    h.state = state
    await session.commit()
    await session.refresh(h)
    return _to_out(h)


@router.post("/{hv_id}/validate")
async def validate_hv(hv_id: str, session: AsyncSession = Depends(get_session)):
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")

    lines: list[str] = []

    async def log(line: str) -> None:
        lines.append(line)

    connector = await service.build_connector(session, h, log)
    try:
        result = await connector.validate_connection()
    except ConnectionValidationError as exc:
        # A bad/incomplete credential, unreachable endpoint, etc. is a NORMAL
        # validation outcome — return it as a clean failure the UI can show,
        # not an HTTP 500. (e.g. vCenter InvalidLogin when the username is blank.)
        return {"success": False, "message": str(exc), "data": {}, "logs": lines}

    data = dict(result.data or {})
    # A connector may return a freshly-minted durable kubeconfig (e.g. the
    # OpenShift connector promoting a username/password login into a
    # ServiceAccount token). Persist it as the target's kubeconfig secret so
    # future operations use the durable token, and strip it from the response
    # rather than returning the token over the API.
    sa_kubeconfig = data.pop("service_account_kubeconfig", None)
    if result.success and sa_kubeconfig:
        existing = get_vault().decrypt(h.secret_blob) if h.secret_blob else {}
        existing["kubeconfig"] = sa_kubeconfig
        h.secret_blob = get_vault().encrypt(existing)
        await session.commit()
        data["service_account_provisioned"] = True

    # Auto-recover a STALE error: if the integration is flagged "error" (e.g. a
    # deploy/wizard step failed once while a cluster node was transiently down)
    # but the connection now validates cleanly, it is working again — clear it so
    # the badge doesn't stick forever.
    if result.success and h.status == "error":
        h.status = "deployed"
        await session.commit()

    return {"success": result.success, "message": result.message, "data": data,
            "logs": lines}


@router.get("/{hv_id}/discover/{kind}")
async def discover_options(hv_id: str, kind: str, session: AsyncSession = Depends(get_session)):
    """Enumerate dynamic field choices (NICs / NVMe sources / FC HBAs) for the UI.

    Backs discoverable dropdowns: the frontend calls this for a form field whose
    ``options_source`` matches ``kind``. Runs synchronously (read-only); logs are
    discarded.
    """
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")

    async def _noop(_: str) -> None:
        return None

    connector = await service.build_connector(session, h, _noop)
    try:
        options = await connector.discover_options(kind)
    except Exception as exc:  # noqa: BLE001 — surface discovery failures to the UI
        raise HTTPException(400, f"Discovery failed: {exc}") from exc
    return {"kind": kind, "options": options}


@router.post("/{hv_id}/operations", status_code=202)
async def run_operation(hv_id: str, body: OperationRequest,
                        session: AsyncSession = Depends(get_session)):
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")
    connector_cls = get_connector_class(h.connector_key)
    valid_actions = {a.id for a in connector_cls.action_schemas()}
    if body.action_id not in valid_actions:
        raise HTTPException(400, f"Action {body.action_id!r} not supported by {h.connector_key!r}")
    job_id = await service.run_operation(session, h, body.action_id, body.params,
                                         dry_run=body.dry_run)
    return {"job_id": job_id}


@router.delete("/{hv_id}", status_code=204)
async def delete_hv(hv_id: str, session: AsyncSession = Depends(get_session)):
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")
    await session.delete(h)
    await session.commit()
