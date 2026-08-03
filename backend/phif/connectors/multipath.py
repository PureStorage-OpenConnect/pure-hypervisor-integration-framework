"""Shared host-side block-device hygiene for migration attach.

When a FlashArray volume is (re)mapped to a host group, the destination host must
make the multipath device appear before the disk is attached to a VM. Doing this
safely requires two things on EVERY hypervisor, not just one:

1. **Flush any STALE multipath map first.** A volume that was previously mapped
   and unmapped can leave a dm-multipath map with dead paths. Activating such a
   map drives uninterruptible (D-state) I/O that can wedge the whole host. We
   ``multipath -f <wwid>`` before rescanning so the map is rebuilt from live paths.
2. **Bound every command with a timeout.** A stuck transport/multipath command
   must fail fast (so the migration rolls back) instead of hanging — and hammering
   a struggling host — for the default 10-minute SSH timeout.

``run`` is an ``async (command: str, timeout: float) -> Any`` callable the caller
wires to its own SSH transport (single host or fanned out across a pool). NVMe-oF
uses native NVMe multipath (not dm-multipath), so the dm flush/assemble steps are
skipped for NVMe transports.
"""

from __future__ import annotations

from typing import Awaitable, Callable

from phif.migrate.spec import scsi_wwid

RunCmd = Callable[[str, float], Awaitable]

_NVME_PROTOCOLS = ("nvme-tcp", "nvme-fc", "nvme-roce")


def wwids_for(serials: "list[str]") -> list[str]:
    """SCSI multipath WWIDs for a list of volume serials (skips empties)."""
    return [scsi_wwid(s) for s in serials if s]


async def refresh_block_devices(run: RunCmd, *, protocol: str,
                                wwids: "list[str]") -> None:
    """Flush stale dm-multipath maps for ``wwids``, rescan the transport, and wait
    (bounded) for each device node to (re)appear. All commands are time-bounded."""
    proto = (protocol or "iscsi").lower()
    is_nvme = proto in _NVME_PROTOCOLS

    # 1. Flush stale maps BEFORE rescanning (SCSI/dm-multipath only).
    if not is_nvme:
        for wwid in wwids:
            await run(f"multipath -f {wwid} 2>/dev/null || true", 30)

    # 2. Rescan the transport for the freshly-mapped LUN(s).
    if proto == "nvme-tcp":
        await run("nvme connect-all", 60)
    elif proto in ("iscsi",):
        await run("iscsiadm -m session --rescan", 60)
    else:  # fc / nvme-fc fabric rescan
        await run("for h in /sys/class/scsi_host/host*/scan; "
                  "do echo '- - -' > $h; done", 60)

    # 3. Reassemble + wait (bounded) for each dm device node (SCSI only).
    if not is_nvme:
        await run("multipath -r || true", 60)
        for wwid in wwids:
            await run(
                f"for i in $(seq 1 20); do [ -e /dev/mapper/{wwid} ] && break; "
                f"multipath -r >/dev/null 2>&1; sleep 1; done", 40)
