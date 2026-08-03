"""Rollback bookkeeping for the migration orchestrator.

A migration is a sequence of mutating steps. After each mutating step succeeds it
pushes a compensating action onto a LIFO stack; on any later failure the stack is
unwound in reverse (best-effort) so the source VM is left bootable and the
FlashArray volumes are returned to the source host group. Compensations NEVER
delete/eradicate FlashArray volumes or the source VM.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Awaitable, Callable

# A compensating action: an async no-arg callable that undoes a completed step.
Undo = Callable[[], Awaitable[None]]


@dataclass
class _Entry:
    label: str
    undo: Undo


class RollbackStack:
    """LIFO stack of compensating actions for completed migration steps."""

    def __init__(self, emit: Callable[[str], Awaitable[None]]):
        self._entries: list[_Entry] = []
        self._emit = emit

    def push(self, label: str, undo: Undo) -> None:
        self._entries.append(_Entry(label, undo))

    def clear(self) -> None:
        """Drop all compensations — call once the migration has fully succeeded."""
        self._entries.clear()

    @property
    def pending(self) -> list[str]:
        return [e.label for e in reversed(self._entries)]

    async def unwind(self) -> list[str]:
        """Run every compensation in reverse order, best-effort.

        Returns the labels of compensations that themselves failed (logged, not
        raised) so the caller can surface them in the migration result.
        """
        failures: list[str] = []
        while self._entries:
            entry = self._entries.pop()
            await self._emit(f"[rollback] {entry.label}")
            try:
                await entry.undo()
            except Exception as exc:  # noqa: BLE001 — rollback is best-effort
                failures.append(entry.label)
                await self._emit(
                    f"[rollback] WARNING: compensation {entry.label!r} failed: "
                    f"{type(exc).__name__}: {exc}")
        return failures


class MigrationError(Exception):
    """Raised by a migration step to trigger rollback with a clear message."""
