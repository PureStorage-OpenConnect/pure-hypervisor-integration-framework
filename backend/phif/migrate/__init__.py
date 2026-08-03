"""Cross-hypervisor VM migration.

A migration is a *cold* (reboot) cutover that moves a VM from one hypervisor
platform to another by re-pointing the same FlashArray volume(s) — the data
never moves. See :mod:`phif.migrate.service` for the orchestrator and
:mod:`phif.migrate.spec` for the normalized, cross-hypervisor VM model.
"""

from __future__ import annotations

from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec

__all__ = ["DiskIdentity", "DiskSpec", "NicSpec", "VmSpec"]
