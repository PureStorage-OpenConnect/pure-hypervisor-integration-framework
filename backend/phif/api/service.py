"""Glue between the persistence layer and the connector runtime.

Builds a ready-to-use connector instance (with FlashArray client, decrypted
target secrets, log emitter, and job runner) for a stored hypervisor, and runs
connector operations through the job manager.
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from phif.connectors.base import (
    ConnectorContext,
    FieldType,
    HypervisorConnector,
    HypervisorTarget,
    OpResult,
)
from phif.connectors.registry import get_connector_class
from phif.db.models import FlashArray, Hypervisor
from phif.flasharray.client import build_client
from phif.jobs.manager import get_job_manager
from phif.jobs.runner import JobRunner
from phif.vault import get_vault

_SECRET_HINTS = ("token", "password", "secret", "key")


def _strip_secret_params(params: dict[str, Any]) -> dict[str, Any]:
    """Drop secret-ish params before persisting last-run state (kept in cleartext
    JSON; never store tokens/passwords there)."""
    return {
        k: v for k, v in params.items()
        if not any(h in k.lower() for h in _SECRET_HINTS)
    }


def _persist_host_group(hv: Hypervisor, result: OpResult) -> str | None:
    """If a connector action resolved/adopted a FlashArray host group different
    from the stored one, write it back to the hypervisor's connection config.

    Host registration may ADOPT a pre-existing array host group (when the hosts
    already belong to one) instead of the operator-typed value. Persisting it here
    means every later operation (provision, connect, reconcile) uses the same group
    that the array actually has the hosts in. Returns the new group if changed,
    else None. Caller commits.
    """
    hg = (result.artifacts or {}).get("host_group")
    if not hg:
        return None
    conn = dict(hv.connection or {})
    if conn.get("host_group") == hg:
        return None
    conn["host_group"] = hg
    hv.connection = conn
    return hg


async def _load_array(session: AsyncSession, array_id: str | None):
    """Return (client, original_api_token) for the associated array, or (None, None)."""
    if not array_id:
        return None, None
    array = await session.get(FlashArray, array_id)
    if array is None:
        return None, None
    secrets = get_vault().decrypt(array.secret_blob)
    client = build_client(
        array.mgmt_endpoint,
        api_token=secrets.get("api_token"),
        username=secrets.get("username"),
        password=secrets.get("password"),
        verify_ssl=array.verify_ssl,
    )
    return client, secrets.get("api_token")


async def build_connector(
    session: AsyncSession,
    hypervisor: Hypervisor,
    log: Callable[[str], Awaitable[None]],
    *,
    dry_run: bool = False,
) -> HypervisorConnector:
    cls = get_connector_class(hypervisor.connector_key)
    if cls is None:
        raise ValueError(f"Unknown connector {hypervisor.connector_key!r}")

    secrets = get_vault().decrypt(hypervisor.secret_blob) if hypervisor.secret_blob else {}
    connection = dict(hypervisor.connection or {})
    # Apply target-schema defaults for any non-secret field the stored connection
    # left blank. Form fields show their `default` in the UI, but that default is
    # only persisted if the operator actually edits the field — so a relied-upon
    # default (e.g. vcenter_user=administrator@vsphere.local, esxi_user=root) would
    # otherwise reach the connector empty and fail (vCenter InvalidLogin). This
    # also repairs existing records without needing a re-save.
    try:
        for field in cls.target_schema():
            if getattr(field, "type", None) == FieldType.SECRET:
                continue  # never inject secret defaults into the connection
            default = getattr(field, "default", None)
            if default not in (None, "") and not connection.get(field.name):
                connection[field.name] = default
    except Exception:  # noqa: BLE001 — defaults are best-effort, never fatal
        pass
    target = HypervisorTarget(
        id=hypervisor.id,
        connector_key=hypervisor.connector_key,
        name=hypervisor.name,
        connection=connection,
        secrets=secrets,
    )
    array_client, array_token = await _load_array(session, hypervisor.array_id)
    ctx = ConnectorContext(
        target=target,
        log=log,
        runner=JobRunner(log, dry_run=dry_run),
        array=array_client,
        array_token=array_token,
        dry_run=dry_run,
    )
    return cls(ctx)


async def run_operation(
    session: AsyncSession,
    hypervisor: Hypervisor,
    action_id: str,
    params: dict[str, Any],
    *,
    dry_run: bool = False,
) -> str:
    """Launch a connector action as a tracked job. Returns the job id.

    The connector is rebuilt inside the job's own DB session so it is not tied to
    the request's session lifetime.
    """
    hypervisor_id = hypervisor.id
    connector_key = hypervisor.connector_key

    async def op_factory(emit: Callable[[str], Awaitable[None]]) -> OpResult:
        from phif.db.session import SessionLocal

        async with SessionLocal() as job_session:
            hv = await job_session.get(Hypervisor, hypervisor_id)
            if hv is None:
                return OpResult.fail("Hypervisor was deleted before the job ran")
            connector = await build_connector(job_session, hv, emit, dry_run=dry_run)
            result = await connector.dispatch(action_id, params)
            # Persist the (non-secret) inputs of a successful run as this action's
            # last-run state, so the UI can pre-load the same values next time.
            if result.success and not dry_run:
                state = dict(hv.state or {})
                state[action_id] = _strip_secret_params(params)
                # Propagate the connector-reported deployment status to the
                # hypervisor record so the UI reflects deploy/teardown (the fix for
                # the status always showing "not_deployed").
                new_status = result.data.get("status")
                if new_status in ("deployed", "not_deployed", "error"):
                    hv.status = new_status
                elif action_id == "deploy":
                    # A successful deploy marks the integration deployed even when
                    # the connector didn't explicitly report a status (so a
                    # not_deployed/error hypervisor flips to deployed once fixed).
                    hv.status = "deployed"
                elif hv.status == "error":
                    # Auto-recover a STALE error: the integration previously errored
                    # (e.g. a failed deploy/wizard step while a cluster node was
                    # transiently down), but an operation has now completed
                    # successfully against it — so it is clearly working again.
                    # Without this, the "error" badge sticks forever because only a
                    # re-run deploy used to clear it (even though health_check /
                    # validate / reconcile all pass).
                    hv.status = "deployed"
                # reconcile_cluster reports hosts that have left the cluster but are
                # only FLAGGED for removal (apply_removals=False); surface them in
                # state so the UI can prompt before they're deleted from the array.
                if "pending_removals" in result.data:
                    state["pending_removals"] = result.data.get("pending_removals") or []
                # Surface cluster drift (new/departed hosts) for the UI, from either
                # the read-only assess_cluster or an applied reconcile_cluster.
                if action_id in ("assess_cluster", "reconcile_cluster") and (
                    "new_hosts" in result.data or "departed_hosts" in result.data
                ):
                    state["cluster_assessment"] = {
                        "nodes": result.data.get("nodes", []),
                        "new_hosts": result.data.get("new_hosts", []),
                        "departed_hosts": result.data.get("departed_hosts", []),
                        "not_ready": result.data.get("not_ready", []),
                    }
                # Record the reconciled cluster membership baseline.
                if action_id == "reconcile_cluster" and "nodes" in result.data:
                    state["known_nodes"] = result.data.get("nodes") or []
                # Persist an adopted FlashArray host group so future ops use it.
                adopted_hg = _persist_host_group(hv, result)
                if adopted_hg:
                    await emit(
                        f"Persisted FlashArray host group {adopted_hg!r} to the "
                        "hypervisor config (used by future operations).")
                hv.state = state
                await job_session.commit()
            elif not result.success and not dry_run and action_id == "deploy":
                # A failed deploy leaves the integration in an error state.
                hv.status = "error"
                await job_session.commit()
            return result

    return await get_job_manager().create_and_run(
        action=action_id,
        op_factory=op_factory,
        hypervisor_id=hypervisor_id,
        connector_key=connector_key,
        params=params,
    )


async def prepare_wizard_hypervisor(
    session: AsyncSession,
    *,
    name: str,
    connector_key: str,
    connection: dict[str, Any],
    secrets: dict[str, Any],
    array_id: str | None,
) -> Hypervisor:
    """Wizard page 1: create the hypervisor record (not yet deployed) and confirm
    the connection works, so the UI can then discover nodes + interfaces before
    asking the operator to pick the storage interfaces. Returns the Hypervisor.
    """
    cls = get_connector_class(connector_key)
    if cls is None:
        raise ValueError(f"Unknown connector {connector_key!r}")

    # Build a TRANSIENT hypervisor (not added to the session) and validate the
    # connection BEFORE persisting. This way a failed attempt never commits a
    # record — so it can't leave an orphan that later trips the unique-name
    # constraint, and the operator can safely retry with the same name.
    hv = Hypervisor(
        name=name,
        connector_key=connector_key,
        connection=connection,
        secret_blob=get_vault().encrypt(secrets) if secrets else "",
        array_id=array_id,
    )

    async def _noop(_: str) -> None:
        return None

    connector = await build_connector(session, hv, _noop)
    # validate_connection() normally returns OpResult, but the underlying SSH/HTTP
    # layer can raise (e.g. socket.gaierror when the host is not DNS-resolvable from
    # the backend, auth failures, timeouts). Treat ANY failure as a clean
    # validation error so the endpoint returns 400 with a useful message, not a 500.
    try:
        r = await connector.validate_connection()
        ok, message = r.success, r.message
    except Exception as exc:  # noqa: BLE001 — surface connection errors to the UI
        ok, message = False, f"{type(exc).__name__}: {exc}"
    if not ok:
        raise ValueError(f"Connection validation failed: {message}")

    # Connection is good — persist now. A duplicate name violates the unique
    # constraint; convert that to a clean ValueError (-> 400) instead of a 500.
    session.add(hv)
    try:
        await session.commit()
    except IntegrityError:
        await session.rollback()
        raise ValueError(
            f"A hypervisor named {name!r} already exists. Choose a different "
            f"name, or remove the existing one on the Hypervisors page."
        ) from None
    return hv


async def run_wizard_job(
    session: AsyncSession,
    hypervisor: Hypervisor,
    *,
    scope: str,
    params: dict[str, Any],
) -> str:
    """Wizard page 2: run the connector's full wizard sequence (validate ->
    [cluster check] -> steps) on an EXISTING hypervisor, with the operator's
    selected interfaces in ``params``. Returns the job id."""
    cls = get_connector_class(hypervisor.connector_key)
    hypervisor_id = hypervisor.id
    connector_key = hypervisor.connector_key
    valid_actions = {a.id for a in cls.action_schemas()}
    steps = [s for s in cls.wizard_steps() if s in valid_actions]

    async def op_factory(emit: Callable[[str], Awaitable[None]]) -> OpResult:
        from phif.db.session import SessionLocal

        async with SessionLocal() as js:
            hv2 = await js.get(Hypervisor, hypervisor_id)
            connector = await build_connector(js, hv2, emit)

            await emit("=== validating connection ===")
            r = await connector.validate_connection()
            if not r.success:
                return OpResult.fail(f"connection validation failed: {r.message}")

            # Pre-flight: validate the objects the install REQUIRES exist (array
            # reachable + connector-specific prerequisites) BEFORE creating anything.
            await emit("=== preflight: validating required objects ===")
            pf = await connector.preflight("wizard", params)
            await emit(("[preflight ok] " if pf.success
                        else "[preflight failed] ") + pf.message)
            if not pf.success:
                return OpResult.fail(f"preflight validation failed: {pf.message}")

            if scope == "cluster":
                await emit("=== validating cluster (storage interface consistency) ===")
                rc = await connector.validate_cluster(**params)
                # Advisory only: a mismatch is surfaced as a warning but never
                # blocks the deployment (nodes legitimately differ in VM taps /
                # bridges / unrelated VLANs; only storage NICs matter, and those
                # are enforced by subnet-filtered interface selection).
                await emit(("[ok] " if rc.success else "[warning] ") + rc.message)

            done: list[str] = []
            state = dict(hv2.state or {})
            for step in steps:
                await emit(f"=== step: {step} ===")
                res = await connector.dispatch(step, params)
                if not res.success:
                    # A step (e.g. deploy) failed mid-wizard → integration is in an
                    # error state; persist it so the UI reflects the failure.
                    hv2.status = "error"
                    hv2.state = state
                    await js.commit()
                    return OpResult.fail(f"step {step!r} failed: {res.message}",
                                         completed=done)
                done.append(step)
                # If a step adopted a different FlashArray host group, persist it
                # AND thread it into params so the remaining steps in THIS run use
                # the same (effective) group instead of the operator-typed one.
                adopted_hg = _persist_host_group(hv2, res)
                if adopted_hg:
                    params["host_group"] = adopted_hg
                    await emit(
                        f"Adopted FlashArray host group {adopted_hg!r} — persisted "
                        "to the hypervisor config and applied to the remaining steps.")
                state[step] = _strip_secret_params(params)
            hv2.state = state
            # Completing the wizard (deploy + configure + …) means the integration
            # is deployed — reflect it on the hypervisor record for the UI.
            hv2.status = "deployed"
            await js.commit()
            return OpResult.ok(f"wizard complete: {', '.join(done)}", steps=done)

    return await get_job_manager().create_and_run(
        action=f"wizard:{scope}",
        op_factory=op_factory,
        hypervisor_id=hypervisor_id,
        connector_key=connector_key,
        params=params,
    )


async def run_wizard(
    session: AsyncSession,
    *,
    name: str,
    connector_key: str,
    connection: dict[str, Any],
    secrets: dict[str, Any],
    array_id: str | None,
    scope: str,
    params: dict[str, Any],
) -> tuple[str, str]:
    """One-shot wizard: prepare (create + validate) then run as one call.

    Kept for the single-page / API path; the multi-step UI uses
    prepare_wizard_hypervisor + run_wizard_job so it can show node-discovered
    interface selection between the two.
    """
    hv = await prepare_wizard_hypervisor(
        session, name=name, connector_key=connector_key, connection=connection,
        secrets=secrets, array_id=array_id)
    job_id = await run_wizard_job(session, hv, scope=scope, params=params)
    return hv.id, job_id


async def get_hypervisor_or_none(session: AsyncSession, hypervisor_id: str) -> Hypervisor | None:
    return await session.get(Hypervisor, hypervisor_id)


async def list_hypervisors(session: AsyncSession) -> list[Hypervisor]:
    res = await session.execute(select(Hypervisor))
    return list(res.scalars())
