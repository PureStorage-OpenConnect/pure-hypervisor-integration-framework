"""A thin, async-friendly wrapper over the Everpure FlashArray REST v2 API.

Connectors use this for all array-side operations (host/host-group registration,
volume/snapshot/clone/resize, protection groups, QoS) and for **API-token
minting** — generating the tokens integrations like the CSI driver or Cinder
need to authenticate to the array.

The real implementation uses ``py-pure-client`` (``pypureclient.flasharray``).
A :class:`MockFlashArrayClient` provides an in-memory stand-in for tests, demos,
and ``PHIF_MOCK_MODE=1``.

The underlying SDK is synchronous; methods here are ``async`` and offload SDK
calls to a thread so they never block the event loop.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol

from phif.config import get_settings


class FlashArrayClient(Protocol):
    """Interface connectors depend on. Both real and mock clients satisfy it."""

    async def connect(self) -> dict[str, Any]: ...
    async def info(self) -> dict[str, Any]: ...
    # API token lifecycle
    async def create_api_token(self, username: str) -> str: ...
    async def delete_api_token(self, username: str) -> None: ...
    # Hosts / connectivity
    async def create_host(self, name: str, *, iqns=None, wwns=None, nqns=None) -> dict: ...
    async def find_host_by_initiator(self, *, iqns=None, wwns=None, nqns=None) -> str | None: ...
    async def ensure_host(self, name: str, *, iqns=None, wwns=None, nqns=None) -> str: ...
    async def get_host(self, name: str) -> dict | None: ...
    async def create_host_group(self, name: str, hosts: list[str]) -> dict: ...
    async def register_host_group(self, requested_group: str, hosts: list[dict]) -> dict: ...
    async def get_host_group_members(self, name: str) -> list[str]: ...
    async def remove_host_from_group(self, name: str, host: str) -> None: ...
    async def delete_host(self, name: str) -> None: ...
    async def delete_host_group(self, name: str) -> None: ...
    async def get_data_interfaces(self, service: str) -> list[str]: ...
    async def get_target_ports(self) -> dict: ...
    # File / NFS (Purity//FA File Services)
    async def create_filesystem(self, name: str) -> dict: ...
    async def delete_filesystem(self, name: str, *, eradicate: bool = False) -> None: ...
    async def create_nfs_export(self, name: str, filesystem: str, path: str,
                                policy: str | None = None) -> dict: ...
    async def get_nfs_exports(self, filesystem: str | None = None) -> list[dict]: ...
    async def delete_nfs_export(self, name: str) -> None: ...
    async def get_nfs_data_interfaces(self) -> list[str]: ...
    # Volumes / snapshots / clones
    async def create_volume(self, name: str, size: str | int) -> dict: ...
    async def get_volume(self, name: str) -> dict | None: ...
    async def find_volume_name_by_serial(self, serial: str) -> str | None: ...
    async def extend_volume(self, name: str, size: str | int) -> dict: ...
    async def delete_volume(self, name: str, *, eradicate: bool = False) -> None: ...
    async def create_snapshot(self, volume: str, suffix: str | None = None) -> dict: ...
    async def clone_volume(self, source: str, dest: str) -> dict: ...
    async def copy_volume(self, source: str, dest: str, *, overwrite: bool = False) -> dict: ...
    async def connect_volume(self, host_or_group: str, volume: str) -> dict: ...
    async def disconnect_volume(self, host_or_group: str, volume: str) -> None: ...
    async def connect_volume_to_group(self, group: str, volume: str) -> dict: ...
    async def disconnect_volume_from_group(self, group: str, volume: str) -> None: ...
    async def ensure_volume_group(self, name: str) -> None: ...
    async def rename_volume(self, old: str, new: str) -> None: ...
    async def volume_exists(self, name: str, *, include_destroyed: bool = True) -> bool: ...
    # --- cross-array replication (migration across arrays) ---
    async def array_name(self) -> str: ...
    async def list_array_connections(self) -> list[dict]: ...
    async def get_connection_key(self) -> str: ...
    async def get_replication_addresses(self) -> list[str]: ...
    async def connect_to_array(self, management_address: str, connection_key: str,
                               replication_addresses: list[str]) -> dict: ...
    async def delete_array_connection(self, name: str) -> None: ...
    async def replicate_volume_to(self, volume: str, target_array: str,
                                  pgroup: str) -> str: ...
    async def import_replicated_volume(self, source_array: str, pgroup: str,
                                       snapshot: str, member_volume: str,
                                       dest: str) -> dict: ...
    async def cleanup_replication_pgroup(self, pgroup: str) -> None: ...
    # QoS / protection
    async def set_qos(self, volume: str, *, iops_limit=None, bw_limit=None) -> dict: ...
    async def create_protection_group(self, name: str, volumes: list[str]) -> dict: ...
    async def close(self) -> None: ...


# --------------------------------------------------------------------------- #
# Real client
# --------------------------------------------------------------------------- #
class PureFlashArrayClient:
    def __init__(self, endpoint: str, *, api_token: str | None = None,
                 username: str | None = None, password: str | None = None,
                 verify_ssl: bool = False):
        self.endpoint = endpoint
        self._api_token = api_token
        self._username = username
        self._password = password
        self._verify_ssl = verify_ssl
        self._client = None  # pypureclient.flasharray.Client

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        from pypureclient import flasharray  # imported lazily

        kwargs: dict[str, Any] = {"verify_ssl": self._verify_ssl}
        if self._api_token:
            kwargs["api_token"] = self._api_token
        else:
            kwargs["username"] = self._username
            kwargs["password"] = self._password
        self._client = flasharray.Client(self.endpoint, **kwargs)
        return self._client

    async def _call(self, fn, *args, **kwargs):
        """Run a sync SDK call in a worker thread and raise on API error."""
        def _run():
            client = self._ensure_client()
            resp = fn(client, *args, **kwargs)
            status = getattr(resp, "status_code", 200)
            if status and status >= 400:
                errors = getattr(resp, "errors", None)
                raise FlashArrayApiError(f"FlashArray API error {status}: {errors}")
            items = getattr(resp, "items", None)
            if items is not None:
                return list(items)
            return resp

        return await asyncio.to_thread(_run)

    async def _call_idempotent(self, fn, *args, **kwargs):
        """Like :meth:`_call` but treats "already exists" errors as success.

        FlashArray POSTs (hosts, host groups, group membership) return a 4xx if
        the object already exists. For create-style operations we want that to be
        a no-op so the connectors can re-run host registration safely.
        """
        try:
            return await self._call(fn, *args, **kwargs)
        except FlashArrayApiError as exc:
            msg = str(exc).lower()
            # Only "already exists / already a member / already belongs" are benign
            # no-ops. Crucially do NOT swallow "does not exist" (a real failure that
            # a bare "exist" substring match used to hide).
            if "does not exist" in msg or "not exist" in msg:
                raise
            if "already" in msg:
                return None
            raise

    async def connect(self) -> dict[str, Any]:
        return await self.info()

    async def info(self) -> dict[str, Any]:
        arrays = await self._call(lambda c: c.get_arrays())
        a = arrays[0] if arrays else None
        # get_arrays() gives the array name (hostname) + Purity version, but NOT
        # the hardware model. The model lives on the controllers (e.g. "FA-X70R3"),
        # identical across CT0/CT1, so take the first non-empty one. Best-effort:
        # never let a missing/!=controller endpoint fail array validation.
        model = None
        try:
            controllers = await self._call(lambda c: c.get_controllers())
            for ctrl in controllers or []:
                m = getattr(ctrl, "model", None)
                if m:
                    model = m
                    break
        except Exception:  # noqa: BLE001 - model is informational only
            model = None
        return {
            "name": getattr(a, "name", None),
            "model": model,
            "version": getattr(a, "version", None),
            "id": getattr(a, "id", None),
        }

    async def create_api_token(self, username: str) -> str:
        # POST /admins/api-tokens creates (or rotates) the token for `username`.
        items = await self._call(lambda c: c.post_admins_api_tokens(names=[username]))
        item = items[0]
        token = getattr(getattr(item, "api_token", None), "token", None)
        if not token:
            raise FlashArrayApiError(f"No token returned for user {username!r}")
        return token

    async def delete_api_token(self, username: str) -> None:
        await self._call(lambda c: c.delete_admins_api_tokens(names=[username]))

    async def create_host(self, name, *, iqns=None, wwns=None, nqns=None) -> dict:
        from pypureclient.flasharray import HostPost

        # Create the host if absent (idempotent), then ensure its initiators via
        # PATCH so re-running registration converges to the desired WWN/IQN/NQN set.
        host = HostPost(iqns=iqns or [], wwns=wwns or [], nqns=nqns or [])
        await self._call_idempotent(lambda c: c.post_hosts(names=[name], host=host))
        if iqns or wwns or nqns:
            from pypureclient.flasharray import HostPatch

            patch = HostPatch(
                add_iqns=iqns or None, add_wwns=wwns or None, add_nqns=nqns or None
            )
            # add_* is itself idempotent on the array, but guard anyway.
            await self._call_idempotent(lambda c: c.patch_hosts(names=[name], host=patch))
        return {"name": name}

    async def find_host_by_initiator(self, *, iqns=None, wwns=None, nqns=None) -> str | None:
        """Return the name of an existing FA host that already owns ANY of the
        given initiators (IQN / NQN / WWN), or None.

        An initiator can belong to only one host on the array, so creating a new
        host with an initiator that's already registered (e.g. XCP/ESXi hosts a
        prior admin added by hand) fails. Connectors call this to REUSE the
        existing host instead. WWNs are matched case-insensitively with ':'
        separators stripped, since the array and operators format them differently.
        """
        want_iqn = {s.strip().lower() for s in (iqns or []) if s.strip()}
        want_nqn = {s.strip().lower() for s in (nqns or []) if s.strip()}
        want_wwn = {_norm_wwn(s) for s in (wwns or []) if s and s.strip()}
        if not (want_iqn or want_nqn or want_wwn):
            return None
        items = await self._call(lambda c: c.get_hosts())
        for h in items or []:
            hi = {s.lower() for s in (getattr(h, "iqns", None) or [])}
            hn = {s.lower() for s in (getattr(h, "nqns", None) or [])}
            hw = {_norm_wwn(s) for s in (getattr(h, "wwns", None) or [])}
            if (want_iqn & hi) or (want_nqn & hn) or (want_wwn & hw):
                return getattr(h, "name", None)
        return None

    async def ensure_host(self, name, *, iqns=None, wwns=None, nqns=None) -> str:
        """Return the FA host to use for these initiators, REUSING a pre-existing
        host that already owns any of them, else creating ``name``.

        Returns the resolved host name (the existing host's name when reused, else
        ``name``). This is the safe primitive for registration: it never tries to
        attach an in-use initiator to a second host.
        """
        existing = await self.find_host_by_initiator(iqns=iqns, wwns=wwns, nqns=nqns)
        if existing:
            return existing
        await self.create_host(name, iqns=iqns, wwns=wwns, nqns=nqns)
        return name

    async def get_host(self, name) -> dict | None:
        """Return ``{name, iqns, wwns, nqns, host_group}`` for an existing host, or
        None. ``host_group`` is the name of the host group the host currently
        belongs to (a FA host can be in at most one), or None — used to flag a host
        already claimed by a different host group during registration.
        """
        items = await self._call(lambda c: c.get_hosts(names=[name]))
        h = items[0] if items else None
        if h is None:
            return None
        hg = getattr(h, "host_group", None)
        return {
            "name": getattr(h, "name", name),
            "iqns": list(getattr(h, "iqns", None) or []),
            "wwns": list(getattr(h, "wwns", None) or []),
            "nqns": list(getattr(h, "nqns", None) or []),
            "host_group": getattr(hg, "name", None) if hg else None,
        }

    async def register_host_group(self, requested_group, hosts) -> dict:
        return await _register_host_group(self, requested_group, hosts)

    async def create_host_group(self, name, hosts) -> dict:
        # Idempotent: a pre-existing host group (or membership) is treated as
        # success, so connectors can re-run host registration safely.
        await self._call_idempotent(lambda c: c.post_host_groups(names=[name]))
        # Add members ONE AT A TIME. A single batch add is atomic on the array, so
        # if any one host is already a member (common on re-runs, or after a partial
        # prior run) the WHOLE request fails "already exists" and the remaining
        # hosts never get added. Per-host adds make each membership converge
        # independently, so every node ends up in the group.
        for host in hosts or []:
            await self._call_idempotent(
                lambda c, h=host: c.post_host_groups_hosts(
                    group_names=[name], member_names=[h])
            )
        return {"name": name, "hosts": hosts}

    async def get_host_group_members(self, name) -> list[str]:
        """Return the FA host names that are members of host group ``name``."""
        items = await self._call(
            lambda c: c.get_host_groups_hosts(group_names=[name]))
        members: list[str] = []
        for m in items or []:
            # The membership record carries the member host under `.member.name`.
            member = getattr(m, "member", None)
            mname = getattr(member, "name", None) if member else getattr(m, "name", None)
            if mname:
                members.append(mname)
        return members

    async def remove_host_from_group(self, name, host) -> None:
        """Remove a single host from a host group (idempotent)."""
        await self._call_idempotent(
            lambda c: c.delete_host_groups_hosts(group_names=[name], member_names=[host]))

    async def delete_host(self, name) -> None:
        """Delete a FA host (idempotent). Connections must be removed first."""
        await self._call_idempotent(lambda c: c.delete_hosts(names=[name]))

    async def delete_host_group(self, name) -> None:
        """Delete a FA host group (idempotent)."""
        await self._call_idempotent(lambda c: c.delete_host_groups(names=[name]))

    async def get_data_interfaces(self, service: str) -> list[str]:
        """Return enabled data-network IP addresses serving ``service``.

        ``service`` is the FlashArray network service, e.g. "iscsi" or "nvme-tcp".
        These are the portal IPs hosts connect to for that transport.
        """
        items = await self._call(lambda c: c.get_network_interfaces())
        addrs: list[str] = []
        for i in items:
            if getattr(i, "enabled", True) is False:
                continue
            services = getattr(i, "services", None) or []
            if service not in services:
                continue
            eth = getattr(i, "eth", None)
            addr = getattr(eth, "address", None) if eth else None
            if addr:
                addrs.append(addr)
        return addrs

    async def get_target_ports(self) -> dict[str, Any]:
        """Return the array's target identifiers: {iqn, nqn, wwns}.

        iqn = iSCSI target IQN, nqn = NVMe subsystem NQN, wwns = FC target WWNs.
        Used so connectors don't have to be told the array's target/portal.
        """
        items = await self._call(lambda c: c.get_ports())
        iqns: list[str] = []
        nqns: list[str] = []
        wwns: list[str] = []
        for p in items:
            if getattr(p, "iqn", None):
                iqns.append(p.iqn)
            if getattr(p, "nqn", None):
                nqns.append(p.nqn)
            if getattr(p, "wwn", None):
                wwns.append(p.wwn)
        return {
            "iqn": iqns[0] if iqns else None,
            "nqn": nqns[0] if nqns else None,
            "wwns": sorted(set(wwns)),
        }

    # ---- File / NFS (Purity//FA File Services) ---------------------------- #
    # TODO(doc-validate): confirm the exact pypureclient method names + model
    # classes for File Services on the targeted Purity//FA version. NFS support is
    # not yet hardware-validated; the mock client below drives the tests.
    async def create_filesystem(self, name) -> dict:
        from pypureclient.flasharray import FileSystemPost

        await self._call_idempotent(
            lambda c: c.post_file_systems(names=[name], file_system=FileSystemPost()))
        return {"name": name}

    async def delete_filesystem(self, name, *, eradicate=False) -> None:
        from pypureclient.flasharray import FileSystemPatch

        await self._call_idempotent(
            lambda c: c.patch_file_systems(
                names=[name], file_system=FileSystemPatch(destroyed=True)))
        if eradicate:
            await self._call_idempotent(lambda c: c.delete_file_systems(names=[name]))

    async def create_nfs_export(self, name, filesystem, path, policy=None) -> dict:
        from pypureclient.flasharray import (
            DirectoryPost, DirectoryExportPost, ReferenceNoId,
        )

        # A managed directory (<filesystem>:<name>) backs the export.
        dir_name = f"{filesystem}:{name}"
        await self._call_idempotent(
            lambda c: c.post_directories(
                names=[dir_name], directory=DirectoryPost(path=path)))
        pol = policy or "nfs-default"
        export = DirectoryExportPost(
            export_name=name, policy=ReferenceNoId(name=pol))
        await self._call_idempotent(
            lambda c: c.post_directory_exports(
                directory_names=[dir_name], directory_exports=export))
        return {"name": name, "filesystem": filesystem, "path": path, "policy": pol}

    async def get_nfs_exports(self, filesystem=None) -> list[dict]:
        items = await self._call(lambda c: c.get_directory_exports())
        out: list[dict] = []
        for e in items or []:
            fs = getattr(getattr(e, "directory", None), "name", "") or ""
            if filesystem and not fs.startswith(f"{filesystem}:"):
                continue
            out.append({"name": getattr(e, "export_name", None),
                        "directory": fs})
        return out

    async def delete_nfs_export(self, name) -> None:
        await self._call_idempotent(
            lambda c: c.delete_directory_exports(export_names=[name]))

    async def get_nfs_data_interfaces(self) -> list[str]:
        """Data-network IPs serving NFS (File Services), for client mounts."""
        items = await self._call(lambda c: c.get_network_interfaces())
        addrs: list[str] = []
        for i in items:
            if getattr(i, "enabled", True) is False:
                continue
            services = getattr(i, "services", None) or []
            if not any(s in ("nfs", "file") for s in services):
                continue
            eth = getattr(i, "eth", None)
            addr = getattr(eth, "address", None) if eth else None
            if addr:
                addrs.append(addr)
        return addrs

    async def create_volume(self, name, size) -> dict:
        from pypureclient.flasharray import VolumePost

        # Floor to the array's 1 MiB minimum so callers can provision tiny disks.
        vol = VolumePost(provisioned=max(_to_bytes(size), FA_MIN_VOLUME_BYTES))
        await self._call(lambda c: c.post_volumes(names=[name], volume=vol))
        return {"name": name, "size": size}

    async def get_volume(self, name) -> dict | None:
        """Return a live volume's {name, serial, size, destroyed} or None.

        The ``serial`` is the array-assigned 24-hex value used to derive the
        Linux SCSI multipath WWID (3 + 624a9370 + lc(serial)); connectors that
        attach a volume as a raw block device need it to resolve /dev/mapper/<wwid>.

        A name that does not exist makes FA return a 400 ("Volume does not exist"),
        which we map to ``None`` per this method's contract (callers — including
        ``volume_exists`` and name-collision checks — rely on None, not a raise).
        """
        try:
            items = await self._call(lambda c: c.get_volumes(names=[name]))
        except FlashArrayApiError as exc:
            if "does not exist" in str(exc).lower():
                return None
            raise
        v = items[0] if items else None
        if v is None:
            return None
        return {
            "name": getattr(v, "name", name),
            "serial": getattr(v, "serial", None),
            "size": getattr(v, "provisioned", None),
            "destroyed": getattr(v, "destroyed", False),
        }

    async def find_volume_name_by_serial(self, serial: str) -> str | None:
        """Return the live FA volume NAME whose serial matches ``serial`` (24-hex,
        case-insensitive), or None.

        Used when only a device WWID/serial is known (e.g. a disk the HPE VME Everpure
        plugin provisioned and reports as ``/dev/mapper/3624a9370<serial>``) and the
        caller needs the array volume name to copy/overwrite it.
        """
        want = (serial or "").strip().lower()
        if not want:
            return None
        items = await self._call(lambda c: c.get_volumes())
        for v in items or []:
            if (getattr(v, "serial", "") or "").lower() == want:
                if not getattr(v, "destroyed", False):
                    return getattr(v, "name", None)
        return None

    async def extend_volume(self, name, size) -> dict:
        from pypureclient.flasharray import VolumePatch

        patch = VolumePatch(provisioned=_to_bytes(size))
        await self._call(lambda c: c.patch_volumes(names=[name], volume=patch))
        return {"name": name, "size": size}

    async def delete_volume(self, name, *, eradicate=False) -> None:
        from pypureclient.flasharray import VolumePatch

        await self._call(lambda c: c.patch_volumes(names=[name], volume=VolumePatch(destroyed=True)))
        if eradicate:
            await self._call(lambda c: c.delete_volumes(names=[name]))

    async def create_snapshot(self, volume, suffix=None) -> dict:
        await self._call(lambda c: c.post_volume_snapshots(source_names=[volume], suffix=suffix))
        return {"volume": volume, "suffix": suffix}

    async def clone_volume(self, source, dest) -> dict:
        from pypureclient.flasharray import VolumePost, Reference

        vol = VolumePost(source=Reference(name=source))
        await self._call(lambda c: c.post_volumes(names=[dest], volume=vol))
        return {"source": source, "dest": dest}

    async def copy_volume(self, source, dest, *, overwrite=False) -> dict:
        """Copy ``source`` onto ``dest``. With ``overwrite`` the (existing) target's
        DATA is replaced while its identity (name + serial/WWID) is preserved — so a
        device already attached on a host stays valid, just with new contents. This
        is how migration fills a destination-plugin-managed disk from the source
        volume (the source is read-only and untouched)."""
        from pypureclient.flasharray import VolumePost, Reference

        vol = VolumePost(source=Reference(name=source))
        await self._call(lambda c: c.post_volumes(
            names=[dest], volume=vol, overwrite=overwrite))
        return {"source": source, "dest": dest, "overwrite": overwrite}

    async def connect_volume(self, host_or_group, volume) -> dict:
        await self._call(lambda c: c.post_connections(host_names=[host_or_group], volume_names=[volume]))
        return {"host": host_or_group, "volume": volume}

    async def disconnect_volume(self, host_or_group, volume) -> None:
        # Idempotent: a volume that is already disconnected (or never connected)
        # is treated as success so teardown/detach can re-run safely.
        await self._call_idempotent(
            lambda c: c.delete_connections(
                host_names=[host_or_group], volume_names=[volume]))

    async def connect_volume_to_group(self, group, volume) -> dict:
        # Group-level connection: the per-disk volumes provisioned by the storage
        # plugins are attached to the hypervisor's HOST GROUP, so migration maps/
        # unmaps at the group level (host_group_names, not host_names).
        await self._call(lambda c: c.post_connections(
            host_group_names=[group], volume_names=[volume]))
        return {"host_group": group, "volume": volume}

    async def disconnect_volume_from_group(self, group, volume) -> None:
        await self._call_idempotent(
            lambda c: c.delete_connections(
                host_group_names=[group], volume_names=[volume]))

    async def ensure_volume_group(self, name) -> None:
        # Idempotent: a vgroup that already exists is success (migration Copy mode
        # clones each disk into a per-dest-VM vgroup so the destination plugin can
        # attach it under its own namespace). post_volume_groups REQUIRES a body
        # argument (volume_group=) — an empty VolumeGroupPost just creates the group.
        from pypureclient.flasharray import VolumeGroupPost

        await self._call_idempotent(
            lambda c: c.post_volume_groups(names=[name], volume_group=VolumeGroupPost()))

    async def rename_volume(self, old, new) -> None:
        """Rename a volume (also MOVES it between volume groups, since group
        membership is encoded in the name ``<vg>/<vol>``). Used by Move-to-Proxmox
        to re-home a volume into the destination VM's namespace so the purefa
        plugin resolves it regardless of which VMID the destination landed on."""
        from pypureclient.flasharray import VolumePatch

        await self._call(lambda c: c.patch_volumes(
            names=[old], volume=VolumePatch(name=new)))

    async def volume_exists(self, name, *, include_destroyed=True) -> bool:
        """True if a volume ``name`` exists. With ``include_destroyed`` (default),
        also treats a soft-deleted (pending-eradication) volume as existing — its
        name is still reserved on the array, so renaming onto it would fail."""
        if await self.get_volume(name) is not None:
            return True
        if not include_destroyed:
            return False
        try:
            items = await self._call(
                lambda c: c.get_volumes(names=[name], destroyed=True))
            return bool(items)
        except Exception:  # noqa: BLE001 — unknown name / transient: treat as absent
            return False

    # ----------------------------------------------------- cross-array repl ---
    # NOTE: these power cross-array migration (volume send + copy). The Purity REST
    # 2.x shapes below are the documented async-replication flow; mark as
    # TODO(validate-on-hardware) — exercised against a single array in CI via the
    # mock client, not yet against a real replication pair.
    async def array_name(self) -> str:
        info = await self.info()
        return info.get("name", "")

    async def list_array_connections(self) -> list[dict]:
        items = await self._call(lambda c: c.get_array_connections())
        out = []
        for it in items or []:
            out.append({
                "name": getattr(it, "remote", None) and getattr(it.remote, "name", None)
                        or getattr(it, "name", ""),
                "management_address": getattr(it, "management_address", ""),
                "status": getattr(it, "status", ""),
                "type": getattr(it, "type", ""),
            })
        return out

    async def get_connection_key(self) -> str:
        # This array's connection key, handed to a REMOTE array so it can connect here.
        items = await self._call(lambda c: c.get_array_connections_connection_key())
        if items:
            return getattr(items[0], "connection_key", "") or ""
        return ""

    async def get_replication_addresses(self) -> list[str]:
        # Data interfaces whose services include 'replication'.
        items = await self._call(lambda c: c.get_network_interfaces())
        addrs = []
        for it in items or []:
            services = getattr(it, "services", None) or []
            eth = getattr(it, "eth", None)
            address = getattr(eth, "address", None) if eth else getattr(it, "address", None)
            if address and any("replication" in str(s).lower() for s in services):
                addrs.append(address)
        return addrs

    async def connect_to_array(self, management_address, connection_key,
                               replication_addresses) -> dict:
        from pypureclient.flasharray import ArrayConnectionPost

        body = ArrayConnectionPost(
            management_address=management_address,
            connection_key=connection_key,
            replication_addresses=replication_addresses or None,
            type="async-replication")
        await self._call_idempotent(
            lambda c: c.post_array_connections(array_connection=body))
        return {"management_address": management_address}

    async def delete_array_connection(self, name) -> None:
        await self._call_idempotent(
            lambda c: c.delete_array_connections(names=[name]))

    async def replicate_volume_to(self, volume, target_array, pgroup) -> str:
        """Put ``volume`` in a protection group targeting ``target_array`` and
        replicate a snapshot now. Returns the snapshot suffix used (so the target
        can find the replicated copy as ``<thisarray>:<pgroup>.<suffix>``)."""
        from pypureclient.flasharray import ProtectionGroupSnapshotPost

        await self._call_idempotent(lambda c: c.post_protection_groups(names=[pgroup]))
        await self._call_idempotent(
            lambda c: c.post_protection_groups_volumes(
                group_names=[pgroup], member_names=[volume]))
        await self._call_idempotent(
            lambda c: c.post_protection_groups_targets(
                group_names=[pgroup], member_names=[target_array]))
        suffix = "mig"
        await self._call(lambda c: c.post_protection_group_snapshots(
            source_names=[pgroup],
            protection_group_snapshot=ProtectionGroupSnapshotPost(replicate_now=True),
            suffix=suffix))
        return suffix

    async def import_replicated_volume(self, source_array, pgroup, snapshot,
                                       member_volume, dest) -> dict:
        """On the TARGET array: copy a replicated pgroup-snapshot member into a new
        local volume ``dest``. The replicated member snapshot is named
        ``<source_array>:<pgroup>.<snapshot>.<member_volume>``."""
        member = "%s:%s.%s.%s" % (source_array, pgroup, snapshot, member_volume)
        return await self.clone_volume(member, dest)

    async def cleanup_replication_pgroup(self, pgroup) -> None:
        from pypureclient.flasharray import ProtectionGroupPatch

        await self._call_idempotent(
            lambda c: c.patch_protection_groups(
                names=[pgroup], protection_group=ProtectionGroupPatch(destroyed=True)))
        await self._call_idempotent(
            lambda c: c.delete_protection_groups(names=[pgroup]))

    async def set_qos(self, volume, *, iops_limit=None, bw_limit=None) -> dict:
        from pypureclient.flasharray import VolumePatch, Qos

        qos = Qos(iops_limit=iops_limit, bandwidth_limit=bw_limit)
        await self._call(lambda c: c.patch_volumes(names=[volume], volume=VolumePatch(qos=qos)))
        return {"volume": volume, "iops_limit": iops_limit, "bw_limit": bw_limit}

    async def create_protection_group(self, name, volumes) -> dict:
        await self._call(lambda c: c.post_protection_groups(names=[name]))
        if volumes:
            await self._call(
                lambda c: c.post_protection_groups_volumes(group_names=[name], member_names=volumes)
            )
        return {"name": name, "volumes": volumes}

    async def close(self) -> None:
        self._client = None


class FlashArrayApiError(Exception):
    pass


# --------------------------------------------------------------------------- #
# Mock client
# --------------------------------------------------------------------------- #
class MockFlashArrayClient:
    """In-memory FlashArray for tests/demo. Records calls; never touches network."""

    def __init__(self, endpoint: str = "mock", **_: Any):
        self.endpoint = endpoint
        self.volumes: dict[str, dict] = {}
        self.hosts: dict[str, dict] = {}
        self.host_groups: dict[str, dict] = {}
        self.snapshots: list[dict] = []
        self.api_tokens: dict[str, str] = {}
        self.protection_groups: dict[str, dict] = {}
        self.filesystems: dict[str, dict] = {}
        self.nfs_exports: dict[str, dict] = {}
        self.calls: list[tuple[str, dict]] = []

    def _rec(self, op: str, **kw: Any) -> None:
        self.calls.append((op, kw))

    async def connect(self):
        return await self.info()

    async def info(self):
        return {"name": "mock-array", "model": "FA-X70R3",
                "version": "6.5.0", "id": "mock-0001"}

    async def create_api_token(self, username):
        token = f"mock-token-{username}-{len(self.api_tokens) + 1}"
        self.api_tokens[username] = token
        self._rec("create_api_token", username=username)
        return token

    async def delete_api_token(self, username):
        self.api_tokens.pop(username, None)
        self._rec("delete_api_token", username=username)

    async def create_host(self, name, *, iqns=None, wwns=None, nqns=None):
        # Idempotent + additive: merge initiators if the host already exists.
        h = self.hosts.setdefault(name, {"iqns": [], "wwns": [], "nqns": []})
        for key, vals in (("iqns", iqns), ("wwns", wwns), ("nqns", nqns)):
            for v in vals or []:
                if v not in h[key]:
                    h[key].append(v)
        self._rec("create_host", name=name, iqns=iqns, wwns=wwns, nqns=nqns)
        return {"name": name}

    async def find_host_by_initiator(self, *, iqns=None, wwns=None, nqns=None):
        want_iqn = {s.strip().lower() for s in (iqns or []) if s.strip()}
        want_nqn = {s.strip().lower() for s in (nqns or []) if s.strip()}
        want_wwn = {_norm_wwn(s) for s in (wwns or []) if s and s.strip()}
        self._rec("find_host_by_initiator", iqns=iqns, wwns=wwns, nqns=nqns)
        if not (want_iqn or want_nqn or want_wwn):
            return None
        for hname, h in self.hosts.items():
            hi = {s.lower() for s in h.get("iqns", [])}
            hn = {s.lower() for s in h.get("nqns", [])}
            hw = {_norm_wwn(s) for s in h.get("wwns", [])}
            if (want_iqn & hi) or (want_nqn & hn) or (want_wwn & hw):
                return hname
        return None

    async def ensure_host(self, name, *, iqns=None, wwns=None, nqns=None):
        existing = await self.find_host_by_initiator(iqns=iqns, wwns=wwns, nqns=nqns)
        if existing:
            self._rec("ensure_host", name=name, reused=existing)
            return existing
        await self.create_host(name, iqns=iqns, wwns=wwns, nqns=nqns)
        self._rec("ensure_host", name=name, reused=None)
        return name

    async def get_host(self, name):
        h = self.hosts.get(name)
        self._rec("get_host", name=name)
        if h is None:
            return None
        hg = None
        for gname, g in self.host_groups.items():
            if name in g.get("hosts", []):
                hg = gname
                break
        return {"name": name, "iqns": list(h.get("iqns", [])),
                "wwns": list(h.get("wwns", [])), "nqns": list(h.get("nqns", [])),
                "host_group": hg}

    async def register_host_group(self, requested_group, hosts):
        return await _register_host_group(self, requested_group, hosts)

    async def create_host_group(self, name, hosts):
        # Idempotent + additive: merge members if the group already exists.
        hg = self.host_groups.setdefault(name, {"hosts": []})
        for member in hosts or []:
            if member not in hg["hosts"]:
                hg["hosts"].append(member)
        self._rec("create_host_group", name=name, hosts=hosts)
        return {"name": name, "hosts": hg["hosts"]}

    async def get_host_group_members(self, name):
        self._rec("get_host_group_members", name=name)
        return list(self.host_groups.get(name, {}).get("hosts", []))

    async def remove_host_from_group(self, name, host):
        hg = self.host_groups.get(name)
        if hg and host in hg["hosts"]:
            hg["hosts"].remove(host)
        self._rec("remove_host_from_group", name=name, host=host)

    async def delete_host(self, name):
        self.hosts.pop(name, None)
        self._rec("delete_host", name=name)

    async def delete_host_group(self, name):
        self.host_groups.pop(name, None)
        self._rec("delete_host_group", name=name)

    async def create_filesystem(self, name):
        self.filesystems.setdefault(name, {})
        self._rec("create_filesystem", name=name)
        return {"name": name}

    async def delete_filesystem(self, name, *, eradicate=False):
        self.filesystems.pop(name, None)
        self._rec("delete_filesystem", name=name, eradicate=eradicate)

    async def create_nfs_export(self, name, filesystem, path, policy=None):
        self.filesystems.setdefault(filesystem, {})
        exp = {"name": name, "filesystem": filesystem, "path": path,
               "policy": policy or "nfs-default"}
        self.nfs_exports[name] = exp
        self._rec("create_nfs_export", **exp)
        return exp

    async def get_nfs_exports(self, filesystem=None):
        self._rec("get_nfs_exports", filesystem=filesystem)
        return [e for e in self.nfs_exports.values()
                if filesystem is None or e["filesystem"] == filesystem]

    async def delete_nfs_export(self, name):
        self.nfs_exports.pop(name, None)
        self._rec("delete_nfs_export", name=name)

    async def get_nfs_data_interfaces(self):
        self._rec("get_nfs_data_interfaces")
        return ["10.30.30.30", "10.30.31.30"]

    async def get_data_interfaces(self, service):
        self._rec("get_data_interfaces", service=service)
        return {
            "iscsi": ["10.10.10.10", "10.10.11.10"],
            "nvme-tcp": ["10.20.20.20", "10.20.21.20"],
        }.get(service, [])

    async def get_target_ports(self):
        self._rec("get_target_ports")
        return {
            "iqn": "iqn.2010-06.com.purestorage:flasharray.mock0001",
            "nqn": "nqn.2014-08.com.purestorage:nvme-subsystem.mock0001",
            "wwns": ["524a937000000001", "524a937000000002"],
        }

    async def create_volume(self, name, size):
        import hashlib

        # Deterministic synthetic 24-hex serial so WWID derivation
        # (3 + 624a9370 + lc(serial)) is stable and testable in mock mode.
        serial = hashlib.sha1(name.encode()).hexdigest()[:24]
        self.volumes[name] = {"size": size, "serial": serial}
        self._rec("create_volume", name=name, size=size)
        return {"name": name, "size": size, "serial": serial}

    async def get_volume(self, name):
        self._rec("get_volume", name=name)
        v = self.volumes.get(name)
        if v is None:
            return None
        return {"name": name, "serial": v.get("serial"), "size": v.get("size"),
                "destroyed": False}

    async def find_volume_name_by_serial(self, serial):
        self._rec("find_volume_name_by_serial", serial=serial)
        want = (serial or "").strip().lower()
        for name, v in self.volumes.items():
            if (v.get("serial") or "").lower() == want:
                return name
        return None

    async def extend_volume(self, name, size):
        self.volumes.setdefault(name, {})["size"] = size
        self._rec("extend_volume", name=name, size=size)
        return {"name": name, "size": size}

    async def delete_volume(self, name, *, eradicate=False):
        self.volumes.pop(name, None)
        if not hasattr(self, "destroyed_volumes"):
            self.destroyed_volumes = set()
        if eradicate:
            self.destroyed_volumes.discard(name)  # name freed
        else:
            self.destroyed_volumes.add(name)      # soft-deleted: name still reserved
        self._rec("delete_volume", name=name, eradicate=eradicate)

    async def create_snapshot(self, volume, suffix=None):
        snap = {"volume": volume, "suffix": suffix}
        self.snapshots.append(snap)
        self._rec("create_snapshot", **snap)
        return snap

    async def clone_volume(self, source, dest):
        import hashlib

        self.volumes[dest] = dict(self.volumes.get(source, {}))
        # A clone is a distinct volume with its own serial/WWID.
        self.volumes[dest]["serial"] = hashlib.sha1(dest.encode()).hexdigest()[:24]
        self._rec("clone_volume", source=source, dest=dest)
        return {"source": source, "dest": dest}

    async def copy_volume(self, source, dest, *, overwrite=False):
        import hashlib

        existing = self.volumes.get(dest)
        self.volumes[dest] = dict(self.volumes.get(source, {}))
        if overwrite and existing and existing.get("serial"):
            # Overwrite preserves the TARGET's identity (serial/WWID).
            self.volumes[dest]["serial"] = existing["serial"]
        elif "serial" not in self.volumes[dest]:
            self.volumes[dest]["serial"] = hashlib.sha1(dest.encode()).hexdigest()[:24]
        self._rec("copy_volume", source=source, dest=dest, overwrite=overwrite)
        return {"source": source, "dest": dest, "overwrite": overwrite}

    async def connect_volume(self, host_or_group, volume):
        self._rec("connect_volume", host=host_or_group, volume=volume)
        return {"host": host_or_group, "volume": volume}

    async def disconnect_volume(self, host_or_group, volume):
        self._rec("disconnect_volume", host=host_or_group, volume=volume)

    async def connect_volume_to_group(self, group, volume):
        self._rec("connect_volume_to_group", group=group, volume=volume)
        return {"host_group": group, "volume": volume}

    async def disconnect_volume_from_group(self, group, volume):
        self._rec("disconnect_volume_from_group", group=group, volume=volume)

    async def ensure_volume_group(self, name):
        if not hasattr(self, "volume_groups_set"):
            self.volume_groups_set = set()
        self.volume_groups_set.add(name)
        self._rec("ensure_volume_group", name=name)

    async def rename_volume(self, old, new):
        if old in self.volumes:
            self.volumes[new] = self.volumes.pop(old)
        self._rec("rename_volume", old=old, new=new)

    async def volume_exists(self, name, *, include_destroyed=True):
        if name in self.volumes:
            return True
        if include_destroyed and name in getattr(self, "destroyed_volumes", set()):
            return True
        return False

    async def set_qos(self, volume, *, iops_limit=None, bw_limit=None):
        self._rec("set_qos", volume=volume, iops_limit=iops_limit, bw_limit=bw_limit)
        return {"volume": volume, "iops_limit": iops_limit, "bw_limit": bw_limit}

    async def create_protection_group(self, name, volumes):
        self.protection_groups[name] = {"volumes": volumes}
        self._rec("create_protection_group", name=name, volumes=volumes)
        return {"name": name, "volumes": volumes}

    # --- cross-array replication (mock: in-memory, no network) ---
    async def array_name(self):
        return getattr(self, "name", self.endpoint)

    async def list_array_connections(self):
        return list(getattr(self, "array_connections", []))

    async def get_connection_key(self):
        return "mock-connection-key"

    async def get_replication_addresses(self):
        return ["10.99.0.1"]

    async def connect_to_array(self, management_address, connection_key,
                               replication_addresses):
        if not hasattr(self, "array_connections"):
            self.array_connections = []
        self.array_connections.append({
            "name": getattr(self, "_peer_name", management_address),
            "management_address": management_address,
            "status": "connected", "type": "async-replication"})
        self._rec("connect_to_array", management_address=management_address)
        return {"management_address": management_address}

    async def delete_array_connection(self, name):
        self.array_connections = [
            c for c in getattr(self, "array_connections", []) if c.get("name") != name]
        self._rec("delete_array_connection", name=name)

    async def replicate_volume_to(self, volume, target_array, pgroup):
        self._rec("replicate_volume_to", volume=volume, target_array=target_array,
                  pgroup=pgroup)
        return "mig"

    async def import_replicated_volume(self, source_array, pgroup, snapshot,
                                       member_volume, dest):
        import hashlib

        # Simulate the replicated copy landing as a local volume on THIS array.
        self.volumes[dest] = {"size": "0",
                              "serial": hashlib.sha1(dest.encode()).hexdigest()[:24]}
        self._rec("import_replicated_volume", source_array=source_array, dest=dest)
        return {"source": member_volume, "dest": dest}

    async def cleanup_replication_pgroup(self, pgroup):
        self._rec("cleanup_replication_pgroup", pgroup=pgroup)

    async def close(self):
        pass


async def _register_host_group(client, requested_group, hosts):
    """Reuse/create FA hosts and group them, adopting a pre-existing host group.

    Shared by the real + mock clients so EVERY connector gets identical behavior.
    ``hosts`` is a list of ``{"name": <intended>, "iqns": [...], "wwns": [...],
    "nqns": [...]}`` — one entry per pool node.

    Two phases:
      1. READ-ONLY: match each host to an existing FA host by initiator (reuse) and
         read which host group it currently belongs to.
      2. DECIDE + MUTATE: if the reused hosts already belong to a host group, ADOPT
         that group as the effective group (so we don't fight the array's existing
         topology). Hosts spanning MULTIPLE different groups is a genuine conflict —
         we return ``conflict`` (a descriptive message) and make NO changes. Else we
         create the genuinely-new hosts and ensure the effective group contains all.

    Returns ``{"host_group", "hosts", "adopted", "created", "reused", "conflict"}``.
    ``conflict`` is None on success, or a descriptive string (then nothing was
    mutated and the caller should fail the install with it).
    """
    plan = []                       # (intended_name, existing_or_None, spec)
    groups_seen: dict[str, list[str]] = {}
    for spec in hosts or []:
        existing = await client.find_host_by_initiator(
            iqns=spec.get("iqns"), wwns=spec.get("wwns"), nqns=spec.get("nqns"))
        if existing:
            info = await client.get_host(existing) or {}
            cur = info.get("host_group")
            if cur:
                groups_seen.setdefault(cur, []).append(existing)
        plan.append((spec.get("name"), existing, spec))

    distinct = sorted(groups_seen)
    if len(distinct) > 1:
        detail = "; ".join(
            "%s in host group %r" % (", ".join(groups_seen[g]), g) for g in distinct)
        return {"host_group": requested_group, "hosts": [], "adopted": False,
                "created": [], "reused": [],
                "conflict": (
                    "Pool hosts are spread across multiple FlashArray host groups "
                    "(%s); cannot choose one automatically. Consolidate them into a "
                    "single host group on the array (or clear the extra memberships), "
                    "then retry." % detail)}

    effective = distinct[0] if distinct else requested_group
    adopted = bool(distinct) and effective != requested_group

    names: list[str] = []
    created: list[str] = []
    reused: list[str] = []
    for name, existing, spec in plan:
        if existing:
            reused.append(existing)
            names.append(existing)
        else:
            await client.create_host(
                name, iqns=spec.get("iqns"), wwns=spec.get("wwns"),
                nqns=spec.get("nqns"))
            created.append(name)
            names.append(name)
    await client.create_host_group(effective, names)
    return {"host_group": effective, "hosts": names, "adopted": adopted,
            "created": created, "reused": reused, "conflict": None}


def _norm_wwn(wwn: str) -> str:
    """Normalize an FC WWN for comparison: lowercase, ':' / '-' / spaces stripped.

    The array reports WWNs and operators enter them in different formats
    (``52:4A:93…`` vs ``524a93…``), so we canonicalize before matching.
    """
    return "".join(c for c in str(wwn).lower() if c not in ":- ")


# FlashArray rejects volumes smaller than 1 MiB ("Volume size must be between
# 1 MB and 4 PB"). Connectors legitimately provision tiny disks (e.g. a UEFI
# EFI-vars disk, ~528 KiB); create_volume floors to this so every connector that
# provisions through this client inherits the guard.
FA_MIN_VOLUME_BYTES = 1048576


def _to_bytes(size: str | int) -> int:
    """Convert '1T'/'500G'/'10M' or an int to bytes."""
    if isinstance(size, int):
        return size
    s = str(size).strip().upper()
    units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}
    if s and s[-1] in units:
        return int(float(s[:-1]) * units[s[-1]])
    return int(s)


def build_client(endpoint: str, *, api_token=None, username=None, password=None,
                 verify_ssl=False) -> FlashArrayClient:
    """Factory honoring ``PHIF_MOCK_MODE``."""
    if get_settings().mock_mode:
        return MockFlashArrayClient(endpoint)
    return PureFlashArrayClient(
        endpoint, api_token=api_token, username=username, password=password, verify_ssl=verify_ssl
    )
