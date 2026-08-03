"""Cross-hypervisor VM migration API.

Read endpoints back the migration wizard (list VMs / networks, preview a VM's
captured spec) and follow the same build-connector-with-noop-logger pattern as
``/hypervisors/{id}/nodes``. ``POST /migrations`` launches a tracked migration
job via :func:`phif.migrate.service.run_migration`.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phif.api import service
from phif.api.schemas import (
    MigrationCreate,
    MigrationGroupCreate,
    MigrationGroupOut,
    MigrationOut,
    NetworkSummary,
    VmSummary,
)
from phif.connectors.base import Capability
from phif.connectors.registry import get_connector_class
from phif.db.models import Hypervisor, Migration, MigrationGroup
from phif.db.session import get_session

router = APIRouter(tags=["migrations"])


async def _noop(_: str) -> None:
    return None


async def _get_hv(session: AsyncSession, hv_id: str) -> Hypervisor:
    h = await session.get(Hypervisor, hv_id)
    if h is None:
        raise HTTPException(404, "Hypervisor not found")
    return h


def _supports(connector_key: str, capability: Capability) -> bool:
    cls = get_connector_class(connector_key)
    return cls is not None and cls.supports(capability)


async def _array_connection_status(session, source, dest):
    """Return (connected, dest_array_name): whether the SOURCE array already has a
    replication connection to the DESTINATION array."""
    try:
        src_conn = await service.build_connector(session, source, _noop)
        dst_conn = await service.build_connector(session, dest, _noop)
        if src_conn.ctx.array is None or dst_conn.ctx.array is None:
            return False, ""
        dest_name = await dst_conn.ctx.array.array_name()
        conns = await src_conn.ctx.array.list_array_connections()
        return any((c.get("name") or "") == dest_name for c in conns), dest_name
    except Exception:  # noqa: BLE001 — surface as "not connected / unknown"
        return False, ""


def _mig_out(m: Migration) -> MigrationOut:
    return MigrationOut(
        id=m.id, source_hypervisor_id=m.source_hypervisor_id,
        dest_hypervisor_id=m.dest_hypervisor_id, vm_ref=m.vm_ref,
        dest_vm_ref=m.dest_vm_ref, network_map=m.network_map or {},
        spec=m.spec or {}, phase=m.phase, status=m.status, job_id=m.job_id,
        result=m.result or {})


# --------------------------------------------------------- inventory reads ---
@router.get("/hypervisors/{hv_id}/vms", response_model=list[VmSummary])
async def list_vms(hv_id: str, session: AsyncSession = Depends(get_session)):
    h = await _get_hv(session, hv_id)
    if not _supports(h.connector_key, Capability.VM_INVENTORY):
        raise HTTPException(400, f"{h.connector_key!r} does not support VM inventory")
    connector = await service.build_connector(session, h, _noop)
    try:
        return [VmSummary(**v) for v in await connector.list_vms()]
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Listing VMs failed: {exc}") from exc


@router.get("/hypervisors/{hv_id}/networks", response_model=list[NetworkSummary])
async def list_networks(hv_id: str, session: AsyncSession = Depends(get_session)):
    h = await _get_hv(session, hv_id)
    if not _supports(h.connector_key, Capability.VM_INVENTORY):
        raise HTTPException(400, f"{h.connector_key!r} does not support VM inventory")
    connector = await service.build_connector(session, h, _noop)
    try:
        return [NetworkSummary(**n) for n in await connector.list_networks()]
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Listing networks failed: {exc}") from exc


# vm_ref uses a :path converter: some connectors (e.g. OpenShift/KubeVirt) use
# "<namespace>/<name>" refs that contain a slash, which a plain {vm_ref} segment
# can't match (the encoded %2F decodes to / and breaks routing → 404).
@router.get("/hypervisors/{hv_id}/vms/{vm_ref:path}/spec")
async def get_vm_spec(hv_id: str, vm_ref: str,
                      session: AsyncSession = Depends(get_session)):
    h = await _get_hv(session, hv_id)
    if not _supports(h.connector_key, Capability.VM_INVENTORY):
        raise HTTPException(400, f"{h.connector_key!r} does not support VM inventory")
    connector = await service.build_connector(session, h, _noop)
    try:
        spec = await connector.capture_vm_spec(vm_ref)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Capturing VM spec failed: {exc}") from exc
    return spec.to_dict()


@router.get("/hypervisors/{hv_id}/placements")
async def list_placements(hv_id: str, session: AsyncSession = Depends(get_session)):
    """Everpure-connected clusters + their Everpure storage for the migration destination
    picker. Empty list => the connector auto-places (no selectors shown)."""
    h = await _get_hv(session, hv_id)
    if not _supports(h.connector_key, Capability.VM_INVENTORY):
        return []
    connector = await service.build_connector(session, h, _noop)
    try:
        return await connector.list_placements()
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Listing placements failed: {exc}") from exc


@router.get("/migrations/precheck")
async def migration_precheck(source_hypervisor_id: str, dest_hypervisor_id: str,
                             session: AsyncSession = Depends(get_session)):
    """Tell the wizard whether this pair is cross-array and, if so, whether an
    array connection already exists (so it can prompt for authorization)."""
    source = await _get_hv(session, source_hypervisor_id)
    dest = await _get_hv(session, dest_hypervisor_id)
    cross = bool(source.array_id and dest.array_id and source.array_id != dest.array_id)
    connected, dest_name = (False, "")
    if cross:
        connected, dest_name = await _array_connection_status(session, source, dest)
    return {"cross_array": cross, "connection_exists": connected,
            "dest_array_name": dest_name,
            "needs_authorization": cross and not connected}


# ---------------------------------------------------- array connections (repl) ---
async def _array_client(session, array_id):
    from phif.api.service import _load_array

    client, _ = await _load_array(session, array_id)
    return client


@router.get("/arrays/{array_id}/connections")
async def list_array_connections(array_id: str,
                                 session: AsyncSession = Depends(get_session)):
    client = await _array_client(session, array_id)
    if client is None:
        raise HTTPException(404, "Array not found")
    try:
        return {"connections": await client.list_array_connections()}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Listing array connections failed: {exc}") from exc


@router.delete("/arrays/{array_id}/connections/{name}", status_code=204)
async def delete_array_connection(array_id: str, name: str,
                                  session: AsyncSession = Depends(get_session)):
    client = await _array_client(session, array_id)
    if client is None:
        raise HTTPException(404, "Array not found")
    try:
        await client.delete_array_connection(name)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, f"Removing array connection failed: {exc}") from exc


# -------------------------------------------------------------- migrations ---
@router.post("/migrations", status_code=202)
async def create_migration(body: MigrationCreate,
                           session: AsyncSession = Depends(get_session)):
    source = await _get_hv(session, body.source_hypervisor_id)
    dest = await _get_hv(session, body.dest_hypervisor_id)

    if source.id == dest.id:
        raise HTTPException(400, "Source and destination must differ")
    if not _supports(source.connector_key, Capability.MIGRATE):
        raise HTTPException(400, f"{source.connector_key!r} cannot be a migration source")
    if not _supports(dest.connector_key, Capability.MIGRATE):
        raise HTTPException(400, f"{dest.connector_key!r} cannot be a migration destination")
    if not source.array_id or not dest.array_id:
        raise HTTPException(
            400, "Both source and destination must be associated with a FlashArray")

    # Cross-array (different FlashArrays) requires a replication connection. If one
    # doesn't already exist, configuring it needs explicit authorization
    # (allow_array_connect) — the user is notified and approves on the UI.
    if source.array_id != dest.array_id and not (body.options or {}).get("allow_array_connect"):
        connected, dest_name = await _array_connection_status(session, source, dest)
        if not connected:
            raise HTTPException(
                409,
                f"Source and destination are on different FlashArrays. An array "
                f"connection to {dest_name or 'the destination array'!r} is required "
                f"to send the volume; re-submit with authorization to configure it.")

    # Reject a VM already mid-migration.
    existing = await session.execute(
        select(Migration).where(
            Migration.source_hypervisor_id == source.id,
            Migration.vm_ref == body.vm_ref,
            Migration.status.in_(("pending", "running"))))
    if existing.scalars().first() is not None:
        raise HTTPException(409, "This VM already has a migration in progress")

    from phif.migrate.service import run_migration

    try:
        migration_id, job_id = await run_migration(
            session, source_hv_id=source.id, dest_hv_id=dest.id,
            vm_ref=body.vm_ref, network_map=body.network_map, options=body.options)
    except ValueError as exc:
        raise HTTPException(400, str(exc)) from exc
    return {"migration_id": migration_id, "job_id": job_id}


@router.get("/migrations", response_model=list[MigrationOut])
async def list_migrations(session: AsyncSession = Depends(get_session)):
    res = await session.execute(select(Migration).order_by(Migration.created_at.desc()))
    return [_mig_out(m) for m in res.scalars()]


@router.get("/migrations/{migration_id}", response_model=MigrationOut)
async def get_migration(migration_id: str,
                        session: AsyncSession = Depends(get_session)):
    m = await session.get(Migration, migration_id)
    if m is None:
        raise HTTPException(404, "Migration not found")
    return _mig_out(m)


# --------------------------------------------------------- migration groups ---
async def _validate_pair(session, source: Hypervisor, dest: Hypervisor,
                         options: dict) -> None:
    """Validate one source→dest migration pair; raise HTTPException if invalid.
    Shared by single-migration and migration-group creation."""
    if source.id == dest.id:
        raise HTTPException(400, "Source and destination must differ")
    if not _supports(source.connector_key, Capability.MIGRATE):
        raise HTTPException(400, f"{source.connector_key!r} cannot be a migration source")
    if not _supports(dest.connector_key, Capability.MIGRATE):
        raise HTTPException(400, f"{dest.connector_key!r} cannot be a migration destination")
    if not source.array_id or not dest.array_id:
        raise HTTPException(
            400, "Both source and destination must be associated with a FlashArray")
    if source.array_id != dest.array_id and not (options or {}).get("allow_array_connect"):
        connected, dest_name = await _array_connection_status(session, source, dest)
        if not connected:
            raise HTTPException(
                409,
                f"Source and destination are on different FlashArrays. An array "
                f"connection to {dest_name or 'the destination array'!r} is required; "
                f"re-submit with authorization to configure it.")


def _grp_out(g: MigrationGroup) -> MigrationGroupOut:
    return MigrationGroupOut(
        id=g.id, name=g.name, status=g.status, scheduled_at=g.scheduled_at,
        concurrency=g.concurrency, continue_on_error=g.continue_on_error,
        options=g.options or {}, members=g.members or [],
        created_at=g.created_at, started_at=g.started_at, finished_at=g.finished_at)


@router.post("/migration-groups", status_code=202)
async def create_migration_group(body: MigrationGroupCreate,
                                 session: AsyncSession = Depends(get_session)):
    """Create a migration group of N VM migrations, run together (up to
    ``concurrency`` at once) either now or at ``scheduled_at``."""
    if not body.members:
        raise HTTPException(400, "A migration group needs at least one member")

    members: list[dict] = []
    for m in body.members:
        source = await _get_hv(session, m.source_hypervisor_id)
        dest = await _get_hv(session, m.dest_hypervisor_id)
        opts = {**(body.options or {}), **(m.options or {})}
        await _validate_pair(session, source, dest, opts)
        members.append({
            "source_hypervisor_id": source.id,
            "dest_hypervisor_id": dest.id,
            "vm_ref": m.vm_ref,
            "network_map": m.network_map or {},
            "options": m.options or {},
            "migration_id": None,
            "status": "pending",
        })

    now = datetime.now(timezone.utc)
    sched = body.scheduled_at
    if sched is not None and sched.tzinfo is None:
        sched = sched.replace(tzinfo=timezone.utc)
    run_now = sched is None or sched <= now
    g = MigrationGroup(
        name=body.name or f"group-{now.strftime('%Y%m%d-%H%M%S')}",
        status="pending" if run_now else "scheduled",
        scheduled_at=None if run_now else sched,
        concurrency=max(1, int(body.concurrency or 1)),
        continue_on_error=bool(body.continue_on_error),
        options=body.options or {},
        members=members,
    )
    session.add(g)
    await session.commit()

    if run_now:
        from phif.jobs.scheduler import kick
        kick(g.id)   # launch the runner in the background now
    return {"group_id": g.id, "name": g.name, "status": g.status,
            "scheduled_at": g.scheduled_at}


@router.get("/migration-groups", response_model=list[MigrationGroupOut])
async def list_migration_groups(session: AsyncSession = Depends(get_session)):
    res = await session.execute(
        select(MigrationGroup).order_by(MigrationGroup.created_at.desc()))
    return [_grp_out(g) for g in res.scalars()]


@router.get("/migration-groups/{group_id}", response_model=MigrationGroupOut)
async def get_migration_group(group_id: str,
                              session: AsyncSession = Depends(get_session)):
    g = await session.get(MigrationGroup, group_id)
    if g is None:
        raise HTTPException(404, "Migration group not found")
    return _grp_out(g)


@router.post("/migration-groups/{group_id}/run", status_code=202)
async def run_migration_group_now(group_id: str,
                                  session: AsyncSession = Depends(get_session)):
    """Run a group immediately. For a group that already ran (failed/partial/
    canceled), this re-runs only the members that did NOT succeed — succeeded
    members are left untouched, failed/rolled-back/skipped ones are reset to
    pending and launched again."""
    g = await session.get(MigrationGroup, group_id)
    if g is None:
        raise HTTPException(404, "Migration group not found")
    if g.status == "running":
        raise HTTPException(409, "Group is already running")
    if g.status == "succeeded":
        raise HTTPException(409, "Group already succeeded — nothing to re-run")
    # Reset every non-succeeded member back to pending so the resumable runner
    # picks them up again (rebuild the list so the JSON column is marked dirty).
    members = []
    for m in (g.members or []):
        m = dict(m)
        if (m.get("status") or "pending") != "succeeded":
            m["status"] = "pending"
            m["error"] = None
            m["migration_id"] = None
            m["job_id"] = None
        members.append(m)
    g.members = members
    g.status = "pending"
    g.scheduled_at = None
    g.finished_at = None
    g.updated_at = datetime.now(timezone.utc)
    await session.commit()
    from phif.jobs.scheduler import kick
    kick(g.id)
    return {"group_id": g.id, "status": "pending"}


@router.post("/migration-groups/{group_id}/cancel", status_code=202)
async def cancel_migration_group(group_id: str,
                                 session: AsyncSession = Depends(get_session)):
    """Cancel a group. Prevents launching not-yet-started members; migrations
    already in flight are NOT aborted (jobs run to completion/rollback)."""
    g = await session.get(MigrationGroup, group_id)
    if g is None:
        raise HTTPException(404, "Migration group not found")
    if g.status in ("succeeded", "failed", "partial", "canceled"):
        raise HTTPException(409, f"Group is already {g.status}")
    g.status = "canceled"
    g.updated_at = datetime.now(timezone.utc)
    await session.commit()
    return {"group_id": g.id, "status": "canceled"}


@router.delete("/migration-groups/{group_id}", status_code=204)
async def delete_migration_group(group_id: str,
                                 session: AsyncSession = Depends(get_session)):
    g = await session.get(MigrationGroup, group_id)
    if g is None:
        return
    if g.status == "running":
        raise HTTPException(409, "Cancel the group before deleting it")
    await session.delete(g)
    await session.commit()
