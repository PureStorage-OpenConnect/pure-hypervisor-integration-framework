"""Cross-hypervisor VM migration orchestrator.

A migration is a *cold* (reboot) cutover. Because every VM disk is already a
dedicated FlashArray volume on all supported hypervisors, no data moves: the
volume(s) are unmapped from the source host group and mapped to the destination
host group, a matching VM is created on the destination, and it is booted there.

The source VM is KEPT (powered off, disks detached, volumes unmapped) for
rollback. Per-NIC network mapping is applied and the source MAC is preserved.

Unlike ``run_operation`` (single hypervisor), this coordinates TWO connector
instances through ONE :class:`~phif.db.models.Job`, reusing
``phif.api.service.build_connector`` and ``JobManager.create_and_run`` and
mirroring ``run_wizard_job``'s op-factory shape.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession

from phif.connectors.base import Capability, HypervisorConnector, OpResult
from phif.migrate.spec import VmSpec
from phif.migrate.steps import MigrationError, RollbackStack

# An async callback the orchestrator calls as it advances, so the caller can
# persist the Migration row's phase/spec/dest_vm_ref after each step.
PhaseHook = Callable[..., Awaitable[None]]


async def _noop_phase(**_: Any) -> None:
    return None


class MigrationService:
    """Drives one VM's cold migration from ``src`` to ``dst``."""

    def __init__(
        self,
        src: HypervisorConnector,
        dst: HypervisorConnector,
        emit: Callable[[str], Awaitable[None]],
        *,
        vm_ref: str,
        network_map: dict[str, str],
        options: dict[str, Any] | None = None,
        on_phase: PhaseHook | None = None,
    ) -> None:
        self.src = src
        self.dst = dst
        self.emit = emit
        self.vm_ref = vm_ref
        self.network_map = network_map or {}
        self.options = options or {}
        self.on_phase = on_phase or _noop_phase
        self.rollback = RollbackStack(emit)
        self.spec: VmSpec | None = None
        self.dest_vm_ref: str | None = None
        # "move": reboot cutover re-pointing the SAME volume(s), source removed at
        # the end. "copy": clone the volume(s) on the array, build the destination
        # from the copies, leave the source fully intact (crash-consistent copy).
        self.mode = (self.options.get("mode") or "move").lower()
        self.dest_disks: list = []   # plugin-managed disks created on the destination
        self._source_was_running = False
        # Cross-array: source and destination hypervisors are on DIFFERENT
        # FlashArrays, so the volume must be sent to the destination array via
        # replication (then copied locally there) instead of re-mapped/cloned.
        self.cross_array = bool(self.options.get("cross_array"))

    # ----------------------------------------------------------- helpers ---
    def _opt(self, key: str, default: Any) -> Any:
        return self.options.get(key, default)

    @staticmethod
    def _is_mock(conn: HypervisorConnector) -> bool:
        """True when a connector does no real I/O (mock/dry-run), so power-state
        transitions cannot be observed and polling must be skipped."""
        return bool(conn.ctx.dry_run
                    or getattr(conn.ctx.runner, "mock", False)
                    or getattr(conn.ctx.runner, "dry_run", False))

    async def _phase(self, phase: str) -> None:
        await self.emit(f"=== {phase} ===")
        await self.on_phase(
            phase=phase,
            spec=self.spec.to_dict() if self.spec else None,
            dest_vm_ref=self.dest_vm_ref,
        )

    async def _await_power(self, conn: HypervisorConnector, vm_ref: str,
                           target: str) -> None:
        """Poll until ``vm_ref`` reaches ``target`` power state.

        ``unknown`` (e.g. a connector that can't introspect, or mock mode) is
        accepted after the action so the flow stays testable; a wrong concrete
        state past the attempt budget raises :class:`MigrationError`.
        """
        if self._is_mock(conn):
            await self.emit(f"[mock] skipping power-state poll for {vm_ref}")
            return
        attempts = int(self._opt("poll_attempts", 40))
        delay = float(self._opt("poll_delay", 3.0))
        last = "unknown"
        for i in range(max(1, attempts)):
            last = (await conn.power_state(vm_ref) or "unknown").lower()
            if last in (target, "unknown"):
                return
            if i + 1 < attempts and delay > 0:
                await asyncio.sleep(delay)
        raise MigrationError(
            f"VM {vm_ref!r} did not reach power state {target!r} (last={last!r})")

    # -------------------------------------------------------------- run ---
    async def run(self) -> OpResult:
        """Unified flow for move/copy, same-array and cross-array:

        1. Create the destination VM and, THROUGH its storage plugin, a managed disk
           per source disk (so the destination volume is correctly plugin-managed in
           its own namespace).
        2. FlashArray copy-with-overwrite the source volume's data onto that managed
           destination volume (same-array: direct; cross-array: replicate the source
           to the destination array first, then overwrite from the replica).
        3. Boot the destination. The SOURCE volume is only ever READ — it's left
           fully intact; for a *move* the source VM is removed afterward.
        """
        try:
            await self._preflight()
            await self._capture_spec()
            await self._prepare_source_disks()
            await self._resolve_volumes()
            await self._create_dest_vm()
            if self.cross_array:
                await self._ensure_array_connection()
            # Quiesce the source for a clean copy (always for move; for copy only
            # when shutdown_source is set). The source volume is never modified.
            await self._quiesce_source(force=(self.mode == "move"))
            await self._build_destination_disks()
            await self._finalize_dest_disks()
            # Convert RDMs to native VMFS VMDKs COLD (before power-on) when requested,
            # so the destination boots as a fully-native VM with no scratch storage.
            if self._opt("convert_to_vmfs", False):
                await self._convert_dest_disks()
            await self._set_boot(self.dest_disks)
            await self._power_on_dest()
            if self.cross_array:
                await self._cleanup_replication()
            if self.mode == "move":
                await self._finalize_move()
            else:
                await self._finalize_copy()
        except Exception as exc:  # noqa: BLE001 — convert to rollback + failure
            msg = f"{type(exc).__name__}: {exc}" if not isinstance(
                exc, MigrationError) else str(exc)
            await self.emit(f"[migration] FAILED: {msg}")
            failures = await self.rollback.unwind()
            await self._cleanup_scratch()
            data: dict[str, Any] = {"phase": "rolled_back",
                                    "dest_vm_ref": self.dest_vm_ref}
            if failures:
                data["rollback_failures"] = failures
            return OpResult.fail(
                f"migration failed and was rolled back: {msg}", **data)

        await self._cleanup_scratch()
        self.rollback.clear()
        consistency = ("clean (source shut down)"
                       if (self.mode == "move" or self._opt("shutdown_source", False))
                       else "crash-consistent (source left running)")
        power = "booted" if getattr(self, "_powered_on", False) else "left powered off"
        if self.mode == "move":
            msg = (f"move complete; destination built from a plugin-managed {consistency} "
                   f"copy and {power}; source VM removed, volume(s) deleted")
        else:
            msg = (f"copy complete; destination built from a plugin-managed {consistency} "
                   f"copy and {power}; source untouched")
        return OpResult.ok(
            msg,
            artifacts={"dest_vm_ref": self.dest_vm_ref,
                       "source_vm_ref": self.vm_ref, "mode": self.mode,
                       "cross_array": self.cross_array},
            phase="succeeded")

    # ------------------------------------------------------------ steps ---
    async def _preflight(self) -> None:
        await self._phase("preflight: source")
        if not self.src.supports(Capability.MIGRATE):
            raise MigrationError(
                f"source connector {self.src.key!r} cannot act as a migration source")
        if self.src.ctx.array is None:
            raise MigrationError("source hypervisor has no associated FlashArray")
        r = await self.src.validate_connection()
        if not r.success:
            raise MigrationError(f"source connection invalid: {r.message}")

        await self._phase("preflight: destination")
        if not self.dst.supports(Capability.MIGRATE):
            raise MigrationError(
                f"destination connector {self.dst.key!r} cannot act as a destination")
        if self.dst.ctx.array is None:
            raise MigrationError("destination hypervisor has no associated FlashArray")
        r = await self.dst.validate_connection()
        if not r.success:
            raise MigrationError(f"destination connection invalid: {r.message}")

        self.source_hg = self.src.migration_host_group()
        self.dest_hg = self.dst.migration_host_group()
        if not self.dest_hg:
            raise MigrationError("destination hypervisor has no FlashArray host group configured")

        # The destination host group must already have member hosts. In mock/
        # dry-run the array has no persisted state, so this is advisory only.
        pf = await self.dst.require_host_objects(self.dest_hg)
        if not pf.success:
            if self._is_mock(self.dst):
                await self.emit(f"[mock] skipping host-group readiness: {pf.message}")
            else:
                raise MigrationError(f"destination not ready: {pf.message}")

        # Every mapped destination network must resolve.
        nets = {n.get("id") for n in await self.dst.list_networks()}
        nets |= {n.get("name") for n in await self.dst.list_networks()}
        unknown = [v for v in self.network_map.values() if v and v not in nets]
        if unknown:
            raise MigrationError(
                f"destination network(s) not found: {', '.join(sorted(set(unknown)))}")

    async def _capture_spec(self) -> None:
        await self._phase("capture source VM spec")
        self.spec = await self.src.capture_vm_spec(self.vm_ref)
        if not self.spec.disks:
            raise MigrationError("source VM has no FlashArray-backed disks to migrate")
        # Confirm every source NIC has a destination mapping.
        for nic in self.spec.nics:
            if nic.source_network not in self.network_map:
                raise MigrationError(
                    f"no destination network mapping for source NIC on "
                    f"{nic.source_network!r} (mac {nic.mac})")
        await self.emit(
            f"[capture] {self.spec.name}: {self.spec.vcpus} vCPU, "
            f"{self.spec.memory_bytes // (1024*1024)} MiB, "
            f"{len(self.spec.disks)} disk(s), {len(self.spec.nics)} NIC(s), "
            f"firmware={self.spec.firmware}")

    async def _prepare_source_disks(self) -> None:
        """Allow the source connector to prepare disks that are not yet backed
        by their own FA volume (e.g. vSphere VMFS). The connector may perform
        Storage vMotion to RDM/vVol to give each disk a per-disk FA volume
        identity before _resolve_volumes tries to look them up on the array."""
        assert self.spec is not None
        updated = await self.src.prepare_source_disks(self.spec, self.options)
        if updated is not None and updated is not self.spec:
            self.spec = updated

    async def _cleanup_scratch(self) -> None:
        """Remove any temporary migration artifacts the source connector created
        (e.g. vSphere's phifmig-src clone volumes + RDM pointers). Best-effort and
        idempotent — runs on both success and rollback; never fails the migration."""
        if self.spec is None:
            return
        try:
            await self.src.cleanup_migration_scratch(self.spec)
        except Exception as exc:  # noqa: BLE001
            await self.emit(f"[migration] scratch cleanup warning: {exc}")

    async def _resolve_volumes(self) -> None:
        await self._phase("resolve FlashArray volumes")
        assert self.spec is not None
        array = self.src.ctx.array
        mock = self._is_mock(self.src)
        for disk in self.spec.disks:
            vol = await array.get_volume(disk.identity.fa_volume)
            if vol is None:
                if mock:  # fresh in-memory array has no persisted volumes
                    await self.emit(
                        f"[mock] skipping volume existence check for "
                        f"{disk.identity.fa_volume}")
                    continue
                raise MigrationError(
                    f"FlashArray volume {disk.identity.fa_volume!r} not found")
            if vol.get("destroyed"):
                raise MigrationError(
                    f"FlashArray volume {disk.identity.fa_volume!r} is destroyed")
            # Fill/confirm the serial used to match the device on the destination.
            disk.identity.serial = disk.identity.serial or vol.get("serial")
            disk.identity.size_bytes = disk.identity.size_bytes or vol.get("size")
            await self.emit(
                f"[resolve] {disk.identity.fa_volume} serial={disk.identity.serial}")

    async def _ensure_unique_dest_name(self) -> None:
        """Avoid clobbering an existing destination VM of the same name.

        Connector-agnostic: if a VM with the captured name already exists at the
        destination, append ``-N`` (smallest free) so we never overwrite it. Some
        platforms (e.g. KubeVirt ``oc apply``) upsert by name, so a name collision
        would silently replace the existing VM. If the destination can't be listed,
        fall back to a short unique suffix rather than risk an overwrite."""
        assert self.spec is not None
        base = self.spec.name
        try:
            existing = {(v.get("name") or "") for v in await self.dst.list_vms()}
        except Exception:  # noqa: BLE001 — listing is best-effort; stay safe
            suffix = self.migration_id[:8] if getattr(self, "migration_id", "") else "mig"
            self.spec.name = f"{base}-{suffix}"
            await self.emit(
                f"[dest] could not list destination VMs; using '{self.spec.name}' "
                f"to avoid overwriting an existing VM")
            return
        if base not in existing:
            return
        n = 1
        while f"{base}-{n}" in existing:
            n += 1
        self.spec.name = f"{base}-{n}"
        await self.emit(
            f"[dest] a VM named '{base}' already exists at the destination; "
            f"creating '{self.spec.name}' instead (not overwriting it)")

    async def _create_dest_vm(self) -> None:
        await self._phase("create destination VM")
        assert self.spec is not None
        await self._ensure_unique_dest_name()
        # Operator's destination placement (cluster + Everpure storage) from the wizard;
        # connectors that support it honor it, others auto-place.
        placement = {"cluster": self._opt("dest_cluster", None),
                     "storage": self._opt("dest_storage", None),
                     # Lets the destination connector self-provision scratch storage
                     # for the RDM intermediary when the disks will be converted to
                     # VMFS (vSphere puts the VM home on a temp FA-backed VMFS).
                     "convert_to_vmfs": self._opt("convert_to_vmfs", False),
                     "vmfs_datastore": self._opt("vmfs_datastore", None)}
        placement = {k: v for k, v in placement.items() if v}
        r = await self.dst.create_vm(self.spec, network_map=self.network_map,
                                     placement=placement or None)
        if not r.success:
            raise MigrationError(f"failed to create destination VM: {r.message}")
        self.dest_vm_ref = (r.artifacts or {}).get("vm_ref")
        if not self.dest_vm_ref:
            raise MigrationError("destination create_vm did not return a vm_ref")
        ref = self.dest_vm_ref
        # On rollback, remove the destination VM AND free its freshly-created
        # managed disks (throwaway copies) — keep_disks=False. The SOURCE volume is
        # never touched by this flow, so there is nothing to restore on the source.
        # Stop the dest first: a failed/partial _power_on_dest issues `start` before
        # timing out, so the dest may be powered ON, and some platforms (vSphere)
        # refuse to delete a powered-on VM. delete_vm itself stays move-semantics
        # (assumes powered-off); the power-off lives here in the rollback.
        self.rollback.push(
            f"stop + delete destination VM {ref} (free its new disks)",
            lambda: self._teardown_dest(ref))
        await self.emit(f"[dest] created VM {ref}")

    async def _teardown_dest(self, ref: str) -> None:
        """Rollback cleanup of a half-built destination VM: stop it, then delete it
        (freeing its new disks). The dest may be powered ON — a failed/partial
        ``_power_on_dest`` issues ``start`` before timing out — and some platforms
        (e.g. vSphere) refuse to delete a powered-on VM. The stop is best-effort (an
        already-stopped dest may error harmlessly); the delete still runs."""
        try:
            await self.dst.stop_vm(ref, force=True)
        except Exception as exc:  # noqa: BLE001 — best-effort; delete still attempted
            await self.emit(
                f"[rollback] stop {ref} before delete: {type(exc).__name__}: {exc}")
        await self.dst.delete_vm(ref, keep_disks=False)

    async def _quiesce_source(self, *, force: bool = False) -> None:
        """Power the source off before cloning/replicating so the copy is clean
        (cleanly-shut-down) rather than crash-consistent. ``force`` (cross-array
        move) always quiesces; otherwise it's the Copy ``shutdown_source`` option.
        The source is LEFT shut down on success; on FAILURE the rollback restarts
        it if it was running."""
        if not (force or self._opt("shutdown_source", False)):
            return
        await self._phase("shut down source for a clean copy")
        state = (await self.src.power_state(self.vm_ref) or "unknown").lower()
        self._source_was_running = state != "stopped"
        if not self._source_was_running:
            await self.emit("[copy] source already stopped")
            return
        r = await self.src.stop_vm(self.vm_ref, force=bool(self._opt("force_stop", False)))
        if not r.success:
            raise MigrationError(f"failed to stop source for a clean copy: {r.message}")
        # Restart only on ROLLBACK (failure). On success the source stays shut down.
        self.rollback.push("restart source VM",
                           lambda: self.src.start_vm(self.vm_ref))
        await self._await_power(self.src, self.vm_ref, "stopped")
        await self.emit("[copy] source shut down; it will be left shut down")

    async def _build_destination_disks(self) -> None:
        """Create a plugin-managed disk on the destination per source disk, then
        FlashArray copy-with-overwrite the source data onto it. The source volume is
        only READ — never modified or moved.

        * Same-array: copy source -> dest managed volume directly (instant + thin).
        * Cross-array: replicate the source to the destination array, then overwrite
          the dest managed volume from the replica.
        """
        await self._phase("create destination disk(s) + copy data")
        assert self.spec is not None and self.dest_vm_ref is not None
        from phif.migrate.spec import DiskIdentity, DiskSpec

        dst_arr = self.dst.ctx.array
        pg = None
        if self.cross_array:
            pg = f"phifmig-{self.dest_vm_ref}"
            self._repl_pgroup = pg
            self.rollback.push(
                "remove replication protection group",
                lambda: self.src.ctx.array.cleanup_replication_pgroup(pg))

        self.dest_disks = []
        for disk in self.spec.disks:
            size = disk.identity.size_bytes or 0
            # 1. The destination plugin creates + attaches a managed disk (its FA
            #    volume is named/owned in the plugin's namespace).
            dest_vol = await self.dst.create_managed_disk(
                self.dest_vm_ref, size_bytes=size, order=disk.order, boot=disk.boot)
            self.dest_disks.append(DiskSpec(
                identity=DiskIdentity(fa_volume=dest_vol), bus=disk.bus,
                order=disk.order, boot=disk.boot, source_ref=disk.source_ref))
            # 2. Resolve the copy SOURCE on the destination array.
            if self.cross_array:
                suffix = await self.src.ctx.array.replicate_volume_to(
                    disk.identity.fa_volume, self._dst_array_name, pg)
                await self._await_replication(
                    dst_arr, self._src_array_name, pg, suffix)
                copy_src = "%s:%s.%s.%s" % (
                    self._src_array_name, pg, suffix,
                    disk.identity.fa_volume.split("/")[-1])
            else:
                copy_src = disk.identity.fa_volume
            # 3. Overwrite the managed dest volume's DATA from the source (the dest
            #    volume keeps its identity, so the attached device stays valid).
            await dst_arr.copy_volume(copy_src, dest_vol, overwrite=True)
            await self.emit(
                f"[migrate] copied {copy_src} -> {dest_vol} "
                "(overwrite; source volume untouched)")

    async def _finalize_dest_disks(self) -> None:
        """Let the destination connector reconcile its attached disks with the
        backing volumes after the copy (e.g. vSphere re-creates RDM pointers whose
        geometry went stale when copy-with-overwrite resized the dest volume)."""
        assert self.dest_vm_ref is not None
        r = await self.dst.finalize_destination_disks(self.dest_vm_ref, self.dest_disks)
        if not r.success:
            raise MigrationError(f"failed to finalize destination disks: {r.message}")

    async def _convert_dest_disks(self) -> None:
        """Convert the destination's migration RDMs to native VMFS VMDKs and free the
        temporary FA volumes (+ scratch datastore). Done COLD before power-on: a
        failure leaves the VM on throwaway scratch storage, so it is FATAL — raise to
        trigger rollback (which tears down the VM and the scratch datastore) rather
        than booting a half-converted VM. The connector's verify-before-free gate
        guarantees no volume is eradicated unless its disk is confirmed converted."""
        assert self.dest_vm_ref is not None
        await self._phase("convert destination disks to VMFS (XCOPY) + free FA volumes")
        # Target VMFS: an explicit vmfs_datastore option, else the operator's wizard
        # "Storage" selection (dest_storage).
        ds = self._opt("vmfs_datastore", None) or self._opt("dest_storage", None)
        r = await self.dst.convert_disks_to_native(
            self.dest_vm_ref, self.dest_disks, datastore=ds)
        if not r.success:
            raise MigrationError(f"RDM→VMFS conversion failed: {r.message}")
        await self.emit(f"[convert] {r.message}")

    # ------------------------------------------------------- cross-array ---
    async def _ensure_array_connection(self) -> None:
        """Ensure the source array has a replication connection to the destination
        array. If not, it requires authorization (``allow_array_connect``) — the
        operator was notified and approved configuring it."""
        src_arr = self.src.ctx.array
        dst_arr = self.dst.ctx.array
        self._src_array_name = await src_arr.array_name()
        self._dst_array_name = await dst_arr.array_name()
        conns = await src_arr.list_array_connections()
        if any((c.get("name") or "") == self._dst_array_name for c in conns):
            await self.emit(
                f"[xarray] array connection to {self._dst_array_name!r} already exists")
            return
        if not self._opt("allow_array_connect", False):
            raise MigrationError(
                f"source array is not connected to destination array "
                f"{self._dst_array_name!r}; configuring an array connection requires "
                "authorization")
        await self._phase("configure array connection (replication)")
        key = await dst_arr.get_connection_key()
        repl_addrs = await dst_arr.get_replication_addresses()
        mgmt = getattr(dst_arr, "endpoint", "")
        await src_arr.connect_to_array(mgmt, key, repl_addrs)
        await self.emit(
            f"[xarray] connected source array {self._src_array_name!r} -> "
            f"{self._dst_array_name!r} (kept; manage on the FlashArrays page)")

    async def _await_replication(self, dst_arr, source_array: str, pgroup: str,
                                 suffix: str) -> None:
        """Wait for the replicated pgroup snapshot to land on the destination array.
        Skipped in mock/dry-run (no real transfer)."""
        if self._is_mock(self.dst):
            return
        # TODO(validate-on-hardware): poll the destination for the replicated
        # snapshot <source_array>:<pgroup>.<suffix>; a real transfer can take time.
        await asyncio.sleep(float(self._opt("replication_settle", 5.0)))

    async def _cleanup_replication(self) -> None:
        """Remove the temporary replication protection group/snapshot. The array
        CONNECTION is intentionally kept (reusable; managed on the FlashArrays page)."""
        pg = getattr(self, "_repl_pgroup", None)
        if not pg:
            return
        await self._phase("clean up replication artifacts (keep array connection)")
        try:
            await self.src.ctx.array.cleanup_replication_pgroup(pg)
        except Exception as exc:  # noqa: BLE001 — cleanup is best-effort
            await self.emit(f"[xarray] WARNING: pgroup cleanup failed: {exc}")

    async def _set_boot(self, disks: list) -> None:
        await self._phase("set destination boot order")
        assert self.dest_vm_ref is not None
        r = await self.dst.set_boot_order(self.dest_vm_ref, disks)
        if not r.success:
            raise MigrationError(f"failed to set boot order: {r.message}")

    def _should_power_on(self) -> bool:
        """Whether to power on the destination at the end of the migration.

        Explicit ``power_on`` option wins. Otherwise: a *move* defaults to the source
        VM's pre-migration power state (set by _quiesce_source, which always runs for
        move); a *copy* defaults to powered OFF (so it doesn't contend with the still-
        running source for IP/MAC)."""
        opt = self._opt("power_on", None)
        if opt is not None:
            return bool(opt)
        if self.mode == "move":
            return bool(getattr(self, "_source_was_running", False))
        return False

    async def _power_on_dest(self) -> None:
        assert self.dest_vm_ref is not None
        if not self._should_power_on():
            await self._phase("leave destination powered off")
            await self.emit(
                "[dest] leaving destination VM powered off (per power-state option)")
            self._powered_on = False
            return
        self._powered_on = True
        await self._phase("power on destination VM")
        # A freshly-mapped FlashArray volume can take a little while to appear as a
        # multipath device on the destination host (iSCSI rescan + multipath
        # assemble), so the first start can fail with "device not present". Each
        # start re-triggers the host-side rescan, so RETRY start + wait until the
        # VM reaches running (the device settles within a few attempts).
        if self._is_mock(self.dst):
            await self.dst.start_vm(self.dest_vm_ref)
            await self.emit(f"[mock] skipping power-on wait for {self.dest_vm_ref}")
            return
        attempts = int(self._opt("start_attempts", 6))
        poll_each = int(self._opt("start_poll_attempts", 8))
        delay = float(self._opt("start_poll_delay", 3.0))
        for i in range(attempts):
            r = await self.dst.start_vm(self.dest_vm_ref)
            if not r.success:
                await self.emit(f"[dest] start returned: {r.message}")
            for _ in range(poll_each):
                state = (await self.dst.power_state(self.dest_vm_ref) or "").lower()
                if state == "running":
                    if i:
                        await self.emit(
                            f"[dest] reached running on start attempt {i + 1}")
                    return
                await asyncio.sleep(delay)
            await self.emit(
                f"[dest] not running after start attempt {i + 1}/{attempts}; the "
                "storage device may still be assembling on the host — retrying")
        raise MigrationError(
            f"destination VM {self.dest_vm_ref} did not reach running after "
            f"{attempts} start attempts (storage device may not have assembled)")

    async def _finalize_move(self) -> None:
        """Remove the source VM and delete + eradicate its FlashArray volumes.
        This is the last step, after the dest is confirmed running, so we are
        committed; cleanup failures are warnings, not migration failures. NOT added
        to the rollback stack."""
        await self._phase("finalize: remove source VM and delete source volume(s)")
        assert self.spec is not None
        try:
            r = await self.src.delete_vm(self.vm_ref, keep_disks=True)
            if not r.success:
                await self.emit(
                    f"[move] WARNING: source VM removal failed: {r.message}")
        except Exception as exc:  # noqa: BLE001
            await self.emit(
                f"[move] WARNING: source VM removal error: {type(exc).__name__}: {exc}")

        arr = self.src.ctx.array
        for disk in self.spec.disks:
            vol = disk.identity.fa_volume
            if not vol or not arr:
                continue
            try:
                if self.source_hg:
                    await arr.disconnect_volume_from_group(self.source_hg, vol)
                await arr.delete_volume(vol, eradicate=True)
                await self.emit(f"[move] deleted source volume {vol}")
            except Exception as exc:  # noqa: BLE001
                await self.emit(
                    f"[move] WARNING: could not delete source volume {vol}: "
                    f"{type(exc).__name__}: {exc}")

    async def _finalize_copy(self) -> None:
        await self._phase("finalize: copy complete")
        if self._opt("shutdown_source", False):
            await self.emit(
                "[copy] destination has an independent CLEAN copy; source left "
                "shut down (it was powered off for the copy and NOT restarted)")
        else:
            await self.emit(
                "[copy] destination has an independent crash-consistent copy; "
                "source left running (untouched)")


# --------------------------------------------------------------------------- #
# Job entrypoint
# --------------------------------------------------------------------------- #
async def _update_migration(session: AsyncSession, migration_id: str,
                            **fields: Any) -> None:
    """Persist Migration row fields. Resilient to transient DB errors (e.g. a
    momentary lock under concurrent jobs): retry a few times with a tiny backoff,
    and never raise — progress bookkeeping must not abort the actual migration."""
    import asyncio

    from phif.db.models import Migration

    for _ in range(3):
        try:
            mig = await session.get(Migration, migration_id)
            if mig is None:
                return
            for k, v in fields.items():
                setattr(mig, k, v)
            mig.updated_at = datetime.now(timezone.utc)
            await session.commit()
            return
        except Exception:  # noqa: BLE001 — bookkeeping must not abort the migration
            try:
                await session.rollback()
            except Exception:  # noqa: BLE001
                pass
            await asyncio.sleep(0.01)


async def run_migration(
    session: AsyncSession,
    *,
    source_hv_id: str,
    dest_hv_id: str,
    vm_ref: str,
    network_map: dict[str, str],
    options: dict[str, Any] | None = None,
) -> tuple[str, str]:
    """Create a Migration record and launch the migration as a tracked job.

    Returns ``(migration_id, job_id)``. The connectors are rebuilt inside the
    job's own DB session so they outlive the request.
    """
    from phif.api.service import build_connector
    from phif.db.models import Hypervisor, Migration
    from phif.db.session import SessionLocal
    from phif.jobs.manager import get_job_manager

    source = await session.get(Hypervisor, source_hv_id)
    dest = await session.get(Hypervisor, dest_hv_id)
    if source is None or dest is None:
        raise ValueError("source or destination hypervisor not found")

    opts = dict(options or {})
    # Cross-array when the two hypervisors are on different FlashArrays — drives
    # the replication-based send instead of a same-array re-map/clone.
    opts["cross_array"] = bool(source.array_id and dest.array_id
                               and source.array_id != dest.array_id)

    mig = Migration(
        source_hypervisor_id=source_hv_id,
        dest_hypervisor_id=dest_hv_id,
        vm_ref=vm_ref,
        network_map=dict(network_map or {}),
        options=opts,
        status="pending",
        phase="pending",
    )
    session.add(mig)
    await session.commit()
    migration_id = mig.id
    connector_key = source.connector_key

    async def op_factory(emit: Callable[[str], Awaitable[None]]) -> OpResult:
        async with SessionLocal() as js:
            hv_src = await js.get(Hypervisor, source_hv_id)
            hv_dst = await js.get(Hypervisor, dest_hv_id)
            if hv_src is None or hv_dst is None:
                return OpResult.fail("a hypervisor was deleted before the migration ran")
            src_conn = await build_connector(js, hv_src, emit)
            dst_conn = await build_connector(js, hv_dst, emit)

            # Reuse the single job session for every Migration update so we never
            # open competing connections (which, on in-memory SQLite, can resolve
            # to separate databases and lose writes).
            async def on_phase(*, phase: str, spec: dict | None,
                               dest_vm_ref: str | None) -> None:
                fields: dict[str, Any] = {"phase": phase, "status": "running"}
                if spec is not None:
                    fields["spec"] = spec
                if dest_vm_ref is not None:
                    fields["dest_vm_ref"] = dest_vm_ref
                await _update_migration(js, migration_id, **fields)

            svc = MigrationService(
                src_conn, dst_conn, emit,
                vm_ref=vm_ref, network_map=network_map, options=opts,
                on_phase=on_phase)
            result = await svc.run()

            await _update_migration(
                js, migration_id,
                status="succeeded" if result.success else (
                    "rolled_back" if result.data.get("phase") == "rolled_back"
                    else "failed"),
                phase=result.data.get("phase", "done"),
                dest_vm_ref=svc.dest_vm_ref,
                spec=svc.spec.to_dict() if svc.spec else {},
                result={"success": result.success, "message": result.message,
                        "data": result.data, "artifacts": result.artifacts})
            return result

    job_id = await get_job_manager().create_and_run(
        action="migrate",
        op_factory=op_factory,
        hypervisor_id=source_hv_id,
        connector_key=connector_key,
        params={"vm_ref": vm_ref, "dest_hypervisor_id": dest_hv_id,
                "network_map": network_map},
    )

    await _update_migration(session, migration_id, job_id=job_id)
    return migration_id, job_id


# --------------------------------------------------------------------------- #
# Migration groups: run a batch of migrations (optionally scheduled), with a
# configurable number in flight (concurrency=1 => sequential).
# --------------------------------------------------------------------------- #
_GROUP_TERMINAL = ("succeeded", "failed", "rolled_back", "skipped")


# Per-group lock serializing the read-modify-write of the shared ``members`` JSON
# column. Without this, concurrent members (concurrency > 1) clobber each other's
# updates — e.g. a member's migration_id/job_id is lost to a stale-snapshot write.
# Keyed per group so unrelated groups don't serialize; created lazily inside the
# running loop (each group_id is unique, so locks are never reused across loops).
_member_locks: dict[str, asyncio.Lock] = {}


def _member_lock(group_id: str) -> asyncio.Lock:
    lk = _member_locks.get(group_id)
    if lk is None:
        lk = asyncio.Lock()
        _member_locks[group_id] = lk
    return lk


async def _group_update_member(group_id: str, index: int, **fields: Any) -> None:
    """Patch one member dict of a MigrationGroup (resilient, never raises).

    Serialized per group so concurrent members don't lose each other's writes to
    the shared ``members`` JSON list."""
    from phif.db.models import MigrationGroup
    from phif.db.session import SessionLocal

    async with _member_lock(group_id):
        for _ in range(3):
            try:
                async with SessionLocal() as s:
                    g = await s.get(MigrationGroup, group_id)
                    if g is None:
                        return
                    members = list(g.members or [])
                    if 0 <= index < len(members):
                        m = dict(members[index])
                        m.update(fields)
                        members[index] = m
                        g.members = members
                        g.updated_at = datetime.now(timezone.utc)
                        await s.commit()
                    return
            except Exception:  # noqa: BLE001 — bookkeeping must not abort the group
                await asyncio.sleep(0.02)


async def _group_is_canceled(group_id: str) -> bool:
    from phif.db.models import MigrationGroup
    from phif.db.session import SessionLocal
    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, group_id)
        return bool(g is not None and g.status == "canceled")


async def _launch_and_wait_member(group_id: str, index: int,
                                  poll_delay: float = 5.0,
                                  timeout_s: float = 7200.0) -> str:
    """Launch member ``index`` of the group as a Migration + job, then poll the
    Migration to a terminal state. Returns the member's final status."""
    from phif.db.models import Migration, MigrationGroup
    from phif.db.session import SessionLocal

    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, group_id)
        if g is None:
            return "failed"
        member = dict((g.members or [])[index])
        opts = dict(g.options or {})
        opts.update(member.get("options") or {})
        try:
            mig_id, job_id = await run_migration(
                s,
                source_hv_id=member["source_hypervisor_id"],
                dest_hv_id=member["dest_hypervisor_id"],
                vm_ref=member["vm_ref"],
                network_map=member.get("network_map") or {},
                options=opts,
            )
        except Exception as exc:  # noqa: BLE001 — record + continue with the group
            await _group_update_member(
                group_id, index, status="failed",
                error=f"{type(exc).__name__}: {exc}")
            return "failed"
    await _group_update_member(group_id, index, migration_id=mig_id,
                               job_id=job_id, status="running")

    # Poll the Migration row to terminal (check first, then sleep — so an already
    # finished migration is detected immediately).
    waited = 0.0
    while waited <= timeout_s:
        async with SessionLocal() as s:
            m = await s.get(Migration, mig_id)
            st = (m.status if m is not None else "failed")
            # Surface the migration's failure reason on the group member so it
            # shows in the UI's error column (not just the bare status).
            err = (m.result or {}).get("message") if (
                m is not None and st in ("failed", "rolled_back")) else None
        if st in ("succeeded", "failed", "rolled_back"):
            await _group_update_member(group_id, index, status=st, error=err)
            return st
        await asyncio.sleep(poll_delay)
        waited += poll_delay
    await _group_update_member(group_id, index, status="failed",
                               error="timed out waiting for migration to finish")
    return "failed"


async def run_migration_group(group_id: str) -> None:
    """Run all not-yet-terminal members of a group, up to ``concurrency`` in
    flight. Resumable: members already in a terminal state are skipped (so it can
    be re-launched after a restart). Updates the group's status as it goes."""
    import logging

    from phif.db.models import MigrationGroup
    from phif.db.session import SessionLocal

    log = logging.getLogger(__name__)
    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, group_id)
        if g is None:
            return
        if g.status in ("succeeded", "failed", "partial", "canceled"):
            return  # already finished
        g.status = "running"
        g.started_at = g.started_at or datetime.now(timezone.utc)
        g.updated_at = datetime.now(timezone.utc)
        members = list(g.members or [])
        concurrency = max(1, int(g.concurrency or 1))
        continue_on_error = bool(g.continue_on_error)
        await s.commit()

    sem = asyncio.Semaphore(concurrency)
    state = {"stop": False}  # set when a member fails and continue_on_error is off

    async def _run(i: int) -> None:
        async with sem:
            if state["stop"] or await _group_is_canceled(group_id):
                await _group_update_member(group_id, i, status="skipped")
                return
            try:
                st = await _launch_and_wait_member(group_id, i)
            except Exception as exc:  # noqa: BLE001
                log.warning("group %s member %d crashed: %s", group_id, i, exc)
                st = "failed"
                await _group_update_member(group_id, i, status="failed",
                                           error=f"{type(exc).__name__}: {exc}")
            if st != "succeeded" and not continue_on_error:
                state["stop"] = True

    pending = [i for i, m in enumerate(members)
               if (m.get("status") or "pending") not in _GROUP_TERMINAL]
    await asyncio.gather(*[_run(i) for i in pending], return_exceptions=True)

    # Aggregate final status.
    async with SessionLocal() as s:
        g = await s.get(MigrationGroup, group_id)
        if g is None:
            return
        if g.status == "canceled":
            g.finished_at = datetime.now(timezone.utc)
            await s.commit()
            return
        sts = [(m.get("status") or "pending") for m in (g.members or [])]
        if sts and all(x == "succeeded" for x in sts):
            final = "succeeded"
        elif all(x in ("failed", "rolled_back", "skipped") for x in sts):
            final = "failed"
        else:
            final = "partial"
        g.status = final
        g.finished_at = datetime.now(timezone.utc)
        g.updated_at = datetime.now(timezone.utc)
        await s.commit()
    _member_locks.pop(group_id, None)


def launch_group_task(group_id: str) -> None:
    """Fire-and-forget the group runner as a background asyncio task."""
    asyncio.create_task(run_migration_group(group_id))
