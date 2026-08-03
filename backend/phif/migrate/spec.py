"""Normalized, cross-hypervisor VM model used by migration.

A connector's ``capture_vm_spec`` reads a VM's *logical* hardware on the source
platform and returns a :class:`VmSpec`; the destination connector's
``create_vm`` / ``attach_existing_volumes`` recreate that logical spec, mapping
each field to the platform's best-fit device model. Identical device models are
not possible across QEMU (Proxmox), Xen (XCP-ng), and KVM/libvirt (HPE VME), so
only *logical* parity is modeled (vCPU, RAM, per-disk size, NIC count).

The cross-hypervisor match key for a disk is the **FlashArray volume serial**
(24-hex). From it both the Linux SCSI multipath WWID (``3`` + ``624a9370`` +
``lc(serial)``) and the NVMe namespace EUI (``eui.<lc(serial)>``) are derived, so
the destination can find the freshly-mapped device regardless of transport.

This module is intentionally dependency-light (like ``connectors/base.py``): no
FastAPI / SQLAlchemy imports, so it can round-trip through Job params and the DB
JSON columns via :meth:`to_dict` / :meth:`from_dict`.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any

# Everpure Data SCSI vendor OUI used to build the dm-multipath WWID from a volume
# serial. Mirrors the derivation already used by the proxmox/hpevme connectors.
PURE_SCSI_OUI = "624a9370"


def scsi_wwid(serial: str) -> str:
    """Return the Linux dm-multipath WWID for a FlashArray volume serial.

    ``wwid = "3" + "624a9370" + lc(serial24)``. Tolerates a serial that is given
    with or without the ``624a937`` prefix, and strips a leading ``0x``.
    """
    s = (serial or "").strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    if s.startswith("624a937"):
        return f"3{s}"
    return f"3{PURE_SCSI_OUI}{s}"


def nvme_eui(serial: str) -> str:
    """Return the NVMe namespace EUI id (``eui.<lc(serial)>``) for a volume serial."""
    s = (serial or "").strip().lower()
    if s.startswith("0x"):
        s = s[2:]
    return f"eui.{s}"


@dataclass
class DiskIdentity:
    """Identifies a single VM disk by the FlashArray volume that backs it.

    ``fa_volume`` is the array volume NAME (the connect/disconnect key);
    ``serial`` is the authoritative match key across hypervisors. ``wwid`` is the
    derived SCSI multipath id (informational/convenience; recomputed on demand).
    """

    fa_volume: str
    serial: str | None = None
    wwid: str | None = None
    size_bytes: int | None = None

    def derived_wwid(self) -> str | None:
        if self.wwid:
            return self.wwid
        return scsi_wwid(self.serial) if self.serial else None

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DiskIdentity":
        return cls(
            fa_volume=d.get("fa_volume", ""),
            serial=d.get("serial"),
            wwid=d.get("wwid"),
            size_bytes=d.get("size_bytes"),
        )


@dataclass
class DiskSpec:
    """A VM disk: which FlashArray volume backs it + its logical attachment."""

    identity: DiskIdentity
    bus: str = "scsi"      # logical family: scsi|virtio|sata|ide|nvme
    order: int = 0         # position in the disk list (preserved across migration)
    boot: bool = False     # whether the guest boots from this disk
    source_ref: str = ""   # pve slot "scsi0" / xcpng "vbd:<uuid>" / VME disk id

    def to_dict(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d["identity"] = self.identity.to_dict()
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "DiskSpec":
        return cls(
            identity=DiskIdentity.from_dict(d.get("identity", {})),
            bus=d.get("bus", "scsi"),
            order=int(d.get("order", 0)),
            boot=bool(d.get("boot", False)),
            source_ref=d.get("source_ref", ""),
        )


@dataclass
class NicSpec:
    """A VM NIC. ``mac`` is PRESERVED across migration; ``source_network`` is the
    source-side network identifier the operator maps to a destination network."""

    mac: str
    source_network: str
    model: str = "virtio"
    order: int = 0

    def to_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "NicSpec":
        return cls(
            mac=d.get("mac", ""),
            source_network=d.get("source_network", ""),
            model=d.get("model", "virtio"),
            order=int(d.get("order", 0)),
        )


@dataclass
class VmSpec:
    """The normalized logical hardware of a VM, captured on the source platform."""

    name: str
    source_ref: str            # pve vmid / xcpng VM uuid / VME instance id
    vcpus: int = 1
    memory_bytes: int = 0
    firmware: str = "bios"     # bios|uefi
    secure_boot: bool = False
    disks: list[DiskSpec] = field(default_factory=list)
    nics: list[NicSpec] = field(default_factory=list)
    guest_os_hint: str = ""
    # Connector-specific extras (e.g. PVE scsihw, machine type) for round-tripping.
    raw: dict[str, Any] = field(default_factory=dict)

    def boot_disk(self) -> "DiskSpec | None":
        for d in self.disks:
            if d.boot:
                return d
        return self.disks[0] if self.disks else None

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "source_ref": self.source_ref,
            "vcpus": self.vcpus,
            "memory_bytes": self.memory_bytes,
            "firmware": self.firmware,
            "secure_boot": self.secure_boot,
            "disks": [d.to_dict() for d in self.disks],
            "nics": [n.to_dict() for n in self.nics],
            "guest_os_hint": self.guest_os_hint,
            "raw": self.raw,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "VmSpec":
        return cls(
            name=d.get("name", ""),
            source_ref=d.get("source_ref", ""),
            vcpus=int(d.get("vcpus", 1)),
            memory_bytes=int(d.get("memory_bytes", 0)),
            firmware=d.get("firmware", "bios"),
            secure_boot=bool(d.get("secure_boot", False)),
            disks=[DiskSpec.from_dict(x) for x in d.get("disks", [])],
            nics=[NicSpec.from_dict(x) for x in d.get("nics", [])],
            guest_os_hint=d.get("guest_os_hint", ""),
            raw=dict(d.get("raw", {})),
        )
