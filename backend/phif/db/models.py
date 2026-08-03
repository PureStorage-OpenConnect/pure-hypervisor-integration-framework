"""ORM models.

Secret material is never stored in plain columns. ``secret_blob`` columns hold a
Fernet token produced by :mod:`phif.vault`; non-secret connection details live in
JSON columns.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, DateTime, ForeignKey, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


def _uuid() -> str:
    return str(uuid.uuid4())


def _now() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class FlashArray(Base):
    """A connected Everpure FlashArray."""

    __tablename__ = "flasharrays"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    mgmt_endpoint: Mapped[str] = mapped_column(String(255))  # IP or FQDN
    # Encrypted bundle: {"api_token": "..."} or {"username":..,"password":..}
    secret_blob: Mapped[str] = mapped_column(Text)
    verify_ssl: Mapped[bool] = mapped_column(default=False)
    # Cached, non-secret facts from the last validate (model, version, capacity).
    info: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    api_keys: Mapped[list["ApiKey"]] = relationship(back_populates="array", cascade="all, delete")


class ApiKey(Base):
    """An API token minted on a FlashArray for a specific integration to consume.

    The token value itself lives encrypted in ``secret_blob``. We track which
    integration/user it belongs to so the UI can show consumption + rotate it.
    """

    __tablename__ = "api_keys"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    array_id: Mapped[str] = mapped_column(ForeignKey("flasharrays.id", ondelete="CASCADE"))
    # FlashArray user the token authenticates as.
    array_user: Mapped[str] = mapped_column(String(255))
    # Which integration consumes it, e.g. "openshift-csi", "openstack-cinder".
    purpose: Mapped[str] = mapped_column(String(128))
    secret_blob: Mapped[str] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)

    array: Mapped[FlashArray] = relationship(back_populates="api_keys")


class Hypervisor(Base):
    """A connected hypervisor target managed via a connector."""

    __tablename__ = "hypervisors"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    connector_key: Mapped[str] = mapped_column(String(64))  # e.g. "vsphere"
    # Non-secret connection details (host, port, options).
    connection: Mapped[dict] = mapped_column(JSON, default=dict)
    # Encrypted credentials bundle.
    secret_blob: Mapped[str] = mapped_column(Text)
    # Optional association to an array for array-side operations.
    array_id: Mapped[str | None] = mapped_column(
        ForeignKey("flasharrays.id", ondelete="SET NULL"), nullable=True
    )
    # Deployment/integration state: "not_deployed" | "deployed" | "error".
    status: Mapped[str] = mapped_column(String(32), default="not_deployed")
    state: Mapped[dict] = mapped_column(JSON, default=dict)  # connector-managed facts
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class Migration(Base):
    """A cross-hypervisor VM migration (cold/reboot cutover).

    Long-lived and richly stateful, so it is NOT overloaded onto :class:`Job`;
    instead it links to the Job that streams its logs. ``spec`` holds the captured
    :class:`~phif.migrate.spec.VmSpec` (as a dict); ``network_map`` is the
    per-source-network → destination-network mapping.
    """

    __tablename__ = "migrations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    source_hypervisor_id: Mapped[str] = mapped_column(String(36))
    dest_hypervisor_id: Mapped[str] = mapped_column(String(36))
    vm_ref: Mapped[str] = mapped_column(String(255))  # source VM native reference
    dest_vm_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    network_map: Mapped[dict] = mapped_column(JSON, default=dict)
    options: Mapped[dict] = mapped_column(JSON, default=dict)
    spec: Mapped[dict] = mapped_column(JSON, default=dict)  # captured VmSpec
    phase: Mapped[str] = mapped_column(String(64), default="pending")  # current step
    # "pending" | "running" | "succeeded" | "failed" | "rolled_back"
    status: Mapped[str] = mapped_column(String(32), default="pending")
    job_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class MigrationGroup(Base):
    """A batch of VM migrations run together, optionally at a scheduled time.

    Members are stored as a JSON list (so no schema change to ``migrations``);
    each member is launched as its own :class:`Migration` (+ job) and the member's
    ``migration_id`` is recorded back here. The group runs up to ``concurrency``
    members in flight at once (1 = sequential). ``scheduled_at`` (when set in the
    future) holds the group in ``status="scheduled"`` until the scheduler loop
    fires it.

    member dict shape:
      ``{source_hypervisor_id, dest_hypervisor_id, vm_ref, network_map, options,
         migration_id, status}``  (migration_id/status filled as it runs)
    """

    __tablename__ = "migration_groups"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    name: Mapped[str] = mapped_column(String(255), default="")
    # "scheduled" | "pending" | "running" | "succeeded" | "failed" | "partial" | "canceled"
    status: Mapped[str] = mapped_column(String(32), default="pending")
    scheduled_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True)
    concurrency: Mapped[int] = mapped_column(default=1)       # 1 = sequential
    continue_on_error: Mapped[bool] = mapped_column(default=True)
    options: Mapped[dict] = mapped_column(JSON, default=dict)  # shared migration opts
    members: Mapped[list] = mapped_column(JSON, default=list)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    started_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True)


class Setting(Base):
    """A small key/value store for runtime application settings.

    Used for toggles that must be changeable from the UI (which env-based
    :class:`~phif.config.Settings` can't be), e.g. the cluster-monitoring loop.
    Value is JSON so a setting can hold a small object.
    """

    __tablename__ = "settings"

    key: Mapped[str] = mapped_column(String(128), primary_key=True)
    value: Mapped[dict] = mapped_column(JSON, default=dict)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)


class Job(Base):
    """A tracked, log-streaming operation (deploy or day-2 action)."""

    __tablename__ = "jobs"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_uuid)
    hypervisor_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    connector_key: Mapped[str | None] = mapped_column(String(64), nullable=True)
    action: Mapped[str] = mapped_column(String(128))
    params: Mapped[dict] = mapped_column(JSON, default=dict)  # secrets stripped
    # "pending" | "running" | "succeeded" | "failed" | "canceled"
    status: Mapped[str] = mapped_column(String(32), default="pending")
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    logs: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=_now)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
