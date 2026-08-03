"""Pydantic request/response models for the API."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# --- Arrays ---
class ArrayCreate(BaseModel):
    name: str
    mgmt_endpoint: str
    api_token: str | None = None
    username: str | None = None
    password: str | None = None
    verify_ssl: bool = False


class ArrayOut(BaseModel):
    id: str
    name: str
    mgmt_endpoint: str
    verify_ssl: bool
    info: dict[str, Any] = {}


# --- API keys ---
class ApiKeyCreate(BaseModel):
    array_id: str
    purpose: str = Field(..., description="Integration that will consume the token")
    # When true, reuse the token the array was connected with instead of minting a
    # new one (for environments that can't create new array users/tokens).
    use_existing: bool = False
    array_user: str = Field(
        "", description="FlashArray user the minted token authenticates as (mint mode only)"
    )


class ApiKeyOut(BaseModel):
    id: str
    array_id: str
    array_user: str
    purpose: str
    # The token value is only returned once, at creation time.
    token: str | None = None


# --- Hypervisors ---
class HypervisorCreate(BaseModel):
    name: str
    connector_key: str
    connection: dict[str, Any] = {}
    secrets: dict[str, Any] = {}
    array_id: str | None = None


class HypervisorUpdate(BaseModel):
    """Partial update. Only fields that are sent are changed (uses exclude_unset).

    ``secrets`` are MERGED into the existing bundle — a provided key overwrites,
    a blank/empty value is ignored (so the operator doesn't have to re-enter
    secrets just to change a connection setting). ``connector_key`` is immutable.
    """

    name: str | None = None
    connection: dict[str, Any] | None = None
    secrets: dict[str, Any] | None = None
    array_id: str | None = None  # "" or null detaches the array


class WizardRequest(BaseModel):
    """One-shot deployment across a cluster (or single node)."""

    name: str
    connector_key: str
    connection: dict[str, Any] = {}
    secrets: dict[str, Any] = {}
    array_id: str | None = None
    scope: str = "cluster"  # "cluster" | "node"
    params: dict[str, Any] = {}  # protocol, interfaces, host_group, etc.


class WizardPrepareRequest(BaseModel):
    """Wizard page 1: create the hypervisor + validate (before interface picking)."""

    name: str
    connector_key: str
    connection: dict[str, Any] = {}
    secrets: dict[str, Any] = {}
    array_id: str | None = None


class WizardRunRequest(BaseModel):
    """Wizard page 2: deploy an already-prepared hypervisor with chosen params."""

    scope: str = "cluster"  # "cluster" | "node"
    params: dict[str, Any] = {}


class HypervisorOut(BaseModel):
    id: str
    name: str
    connector_key: str
    connection: dict[str, Any] = {}
    array_id: str | None = None
    status: str
    state: dict[str, Any] = {}


# --- Operations ---
class OperationRequest(BaseModel):
    action_id: str
    params: dict[str, Any] = {}
    dry_run: bool = False


class JobOut(BaseModel):
    id: str
    action: str
    hypervisor_id: str | None = None
    connector_key: str | None = None
    status: str
    result: dict[str, Any] = {}
    logs: str = ""


# --- Migration ---
class VmSummary(BaseModel):
    id: str
    name: str
    power_state: str = "unknown"
    vcpus: int | None = None
    memory_bytes: int | None = None
    disk_count: int | None = None
    nic_count: int | None = None


class NetworkSummary(BaseModel):
    id: str
    name: str
    kind: str = ""


class MigrationCreate(BaseModel):
    source_hypervisor_id: str
    dest_hypervisor_id: str
    vm_ref: str
    # source-network identifier -> destination-network id
    network_map: dict[str, str] = {}
    options: dict[str, Any] = {}


class MigrationOut(BaseModel):
    id: str
    source_hypervisor_id: str
    dest_hypervisor_id: str
    vm_ref: str
    dest_vm_ref: str | None = None
    network_map: dict[str, str] = {}
    spec: dict[str, Any] = {}
    phase: str
    status: str
    job_id: str | None = None
    result: dict[str, Any] = {}


class MigrationGroupMemberIn(BaseModel):
    source_hypervisor_id: str
    dest_hypervisor_id: str
    vm_ref: str
    network_map: dict[str, str] = {}
    options: dict[str, Any] = {}      # per-member overrides of the group options


class MigrationGroupCreate(BaseModel):
    name: str = ""
    members: list[MigrationGroupMemberIn]
    # ISO-8601 datetime to run at; null/past => run immediately
    scheduled_at: datetime | None = None
    concurrency: int = 1              # how many members run at once (1 = sequential)
    continue_on_error: bool = True    # keep going if a member fails
    options: dict[str, Any] = {}      # shared migration options (mode, power_on, ...)


class MigrationGroupOut(BaseModel):
    id: str
    name: str
    status: str
    scheduled_at: datetime | None = None
    concurrency: int = 1
    continue_on_error: bool = True
    options: dict[str, Any] = {}
    members: list[dict[str, Any]] = []
    created_at: datetime | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None
