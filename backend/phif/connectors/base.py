"""The hypervisor connector contract.

This module is the single source of truth that every per-hypervisor connector
implements. It is intentionally dependency-light (no FastAPI, no SQLAlchemy) so
that connectors and their unit tests can import it in isolation.

Design overview
---------------
* A connector is a subclass of :class:`HypervisorConnector`.
* It declares static metadata as class attributes: ``key``, ``name``,
  ``description``, ``CAPABILITIES``, ``SUPPORTED_PROTOCOLS``.
* It declares *what inputs it needs* in two places, both consumed by the web UI:
    - :meth:`target_schema` — fields required to connect to the hypervisor.
    - :meth:`action_schemas` — one :class:`ActionSpec` per supported operation,
      describing the operation's parameters so the UI can render a form.
* It implements the lifecycle/operation methods it supports. Unsupported methods
  inherit a default that raises :class:`CapabilityNotSupported`, so a connector
  only writes the methods that match its declared ``CAPABILITIES``.

Every operation method is ``async``, receives validated ``params`` (already typed
per its :class:`ActionSpec`), uses ``self.ctx`` for the array client / target
credentials / log emitter / executors, and returns an :class:`OpResult`.
"""

from __future__ import annotations

import dataclasses
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Awaitable, Callable, ClassVar

if TYPE_CHECKING:  # avoid hard import cycles; these are injected at runtime
    from phif.flasharray.client import FlashArrayClient
    from phif.jobs.runner import JobRunner
    from phif.migrate.spec import DiskSpec, VmSpec


# --------------------------------------------------------------------------- #
# Capability + protocol vocabulary (shared across all connectors)
# --------------------------------------------------------------------------- #
class Capability(str, Enum):
    """The union of operations any hypervisor connector may expose.

    A connector lists the subset it supports in ``CAPABILITIES``. The web UI uses
    this to enable/grey-out actions, so the set MUST stay honest with what the
    connector actually implements.
    """

    CONNECT = "connect"  # validate_connection — implicitly required of all
    DEPLOY_PLUGIN = "deploy_plugin"  # install the Everpure integration onto the target
    CONFIGURE = "configure"  # post-deploy config (storage classes, backends, ...)
    PROVISION_DATASTORE = "provision_datastore"  # datastore/SR/StorageClass creation
    PROVISION_VOLUME = "provision_volume"  # individual volume/PV/VDI/disk creation
    SNAPSHOT = "snapshot"
    CLONE = "clone"
    RESIZE = "resize"
    DELETE = "delete"
    HOST_REGISTER = "host_register"  # register hypervisor hosts on the FlashArray
    CONNECTIVITY = "connectivity"  # set up iSCSI/FC/NVMe-oF transport + multipath
    QOS = "qos"
    REPLICATION = "replication"
    RECOVERY = "recovery"
    UPGRADE = "upgrade"
    ROTATE_CREDENTIALS = "rotate_credentials"
    HEALTH = "health"
    REMOVE = "remove"  # tear down / unregister the integration
    RECONCILE_CLUSTER = "reconcile_cluster"  # add new cluster hosts, prune departed ones
    # --- VM management (for cross-hypervisor migration) ---
    VM_INVENTORY = "vm_inventory"  # list VMs, read a VM's logical spec, list networks (read-only)
    VM_LIFECYCLE = "vm_lifecycle"  # create VM, power on/off, attach/detach existing volumes, NICs
    MIGRATE = "migrate"  # connector can act as a migration source/destination


class Protocol(str, Enum):
    ISCSI = "iscsi"
    FC = "fc"
    NVME_TCP = "nvme-tcp"
    NVME_FC = "nvme-fc"
    NVME_ROCE = "nvme-roce"
    NFS = "nfs"


# --------------------------------------------------------------------------- #
# UI-facing input schemas (rendered as forms by the frontend)
# --------------------------------------------------------------------------- #
class FieldType(str, Enum):
    STRING = "string"
    SECRET = "secret"  # masked in UI, stored in vault
    INT = "int"
    BOOL = "bool"
    ENUM = "enum"  # single choice from `options`
    MULTISELECT = "multiselect"  # multiple choices; value is a list
    SIZE = "size"  # human size like "1T", "500G"
    TEXT = "text"  # multi-line (e.g. kubeconfig)


# Well-known kinds a connector can enumerate for a dynamically-populated field.
class DiscoveryKind(str, Enum):
    NICS = "nics"  # network interfaces (iSCSI iface binding)
    NVME_SOURCES = "nvme_sources"  # NVMe-TCP source interfaces/addresses (host-traddr)
    FC_HBAS = "fc_hbas"  # Fibre Channel host bus adapters


@dataclass
class FormField:
    name: str
    label: str
    type: FieldType = FieldType.STRING
    required: bool = True
    default: Any = None
    options: list[str] | None = None  # static choices for ENUM/MULTISELECT
    # When set, the UI populates this field's choices dynamically by calling the
    # connector's discover_options(kind) via GET /hypervisors/{id}/discover/{kind}.
    # Value is one of DiscoveryKind (e.g. "nics", "nvme_sources", "fc_hbas").
    options_source: str | None = None
    help: str = ""
    placeholder: str = ""

    def to_dict(self) -> dict[str, Any]:
        d = dataclasses.asdict(self)
        d["type"] = self.type.value
        return d


@dataclass
class ActionSpec:
    """Describes one invokable operation for the capability-driven UI."""

    capability: Capability
    id: str  # stable action id, unique within a connector (e.g. "create_datastore")
    label: str
    description: str = ""
    fields: list[FormField] = field(default_factory=list)
    destructive: bool = False  # UI shows a confirm dialog
    long_running: bool = True  # runs as a tracked job with streamed logs

    def to_dict(self) -> dict[str, Any]:
        return {
            "capability": self.capability.value,
            "id": self.id,
            "label": self.label,
            "description": self.description,
            "fields": [f.to_dict() for f in self.fields],
            "destructive": self.destructive,
            "long_running": self.long_running,
        }


# --------------------------------------------------------------------------- #
# Result + error types
# --------------------------------------------------------------------------- #
@dataclass
class OpResult:
    """Uniform return type for every connector operation."""

    success: bool
    message: str = ""
    data: dict[str, Any] = field(default_factory=dict)
    # Identifiers created/affected, for the UI to display & for later day-2 ops.
    artifacts: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def ok(cls, message: str = "", *, artifacts: dict[str, Any] | None = None,
           **data: Any) -> "OpResult":
        return cls(success=True, message=message, data=data, artifacts=artifacts or {})

    @classmethod
    def fail(cls, message: str, *, artifacts: dict[str, Any] | None = None,
             **data: Any) -> "OpResult":
        return cls(success=False, message=message, data=data, artifacts=artifacts or {})


class ConnectorError(Exception):
    """Base class for connector-raised errors."""


class CapabilityNotSupported(ConnectorError):
    def __init__(self, connector_key: str, capability: Capability):
        super().__init__(f"{connector_key!r} does not support capability {capability.value!r}")
        self.connector_key = connector_key
        self.capability = capability


class ConnectionValidationError(ConnectorError):
    """Raised when a target cannot be reached / authenticated."""


# Async callback used to stream a log line to the job record / WebSocket.
LogEmitter = Callable[[str], Awaitable[None]]


@dataclass
class HypervisorTarget:
    """A connected hypervisor instance and its (decrypted) credentials.

    ``secrets`` is populated from the vault at call time and is never persisted
    here. ``connection`` holds non-secret connection details (host, port, etc.).
    """

    id: str
    connector_key: str
    name: str
    connection: dict[str, Any] = field(default_factory=dict)
    secrets: dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        return self.secrets.get(key, self.connection.get(key, default))


@dataclass
class ClusterNode:
    """A member node of a hypervisor cluster/pool the connector manages.

    ``host`` is the SSH/management address used to act on that node. ``info``
    carries connector-specific facts (e.g. node IQN, version, role).
    """

    name: str
    host: str
    info: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {"name": self.name, "host": self.host, "info": self.info}


def nics_on_array_subnets(nics: list[dict[str, Any]],
                          portal_ips: list[str]) -> list[dict[str, Any]]:
    """Keep only NICs whose network contains one of the array's storage portal IPs.

    ``nics`` are interface dicts from ``runner.discover_interfaces("nics")`` (each
    with a ``cidr`` like ``192.0.2.5/24``). ``portal_ips`` are the array's
    iSCSI/NVMe data IPs (from ``ctx.array.get_data_interfaces``). This restricts
    interface selection to the ones actually on the storage network, filtering
    out mgmt NICs, VM taps/bridges, and unrelated VLANs. If nothing matches (or no
    portals known), returns the input unfiltered so selection still works.
    """
    import ipaddress

    portals = []
    for p in portal_ips or []:
        try:
            portals.append(ipaddress.ip_address(p))
        except ValueError:
            continue
    if not portals:
        return nics
    matched: list[dict[str, Any]] = []
    for nic in nics:
        cidr = nic.get("cidr")
        if not cidr:
            continue
        try:
            net = ipaddress.ip_network(cidr, strict=False)
        except ValueError:
            continue
        if any(ip in net for ip in portals):
            matched.append(nic)
    return matched or nics


def _nic_network(nic: dict[str, Any]):
    """Return the ipaddress network for a NIC dict (from ``cidr``), or None."""
    import ipaddress

    cidr = nic.get("cidr")
    if not cidr:
        return None
    try:
        return ipaddress.ip_network(cidr, strict=False)
    except ValueError:
        return None


def nics_on_common_subnets(
    host_nics: list[dict[str, Any]],
    per_node_nics: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    """Keep only ``host_nics`` whose subnet is configured on EVERY cluster node.

    ``host_nics`` are the candidate interface dicts (each with a ``cidr``) shown
    in the UI — normally the connection host's NICs. ``per_node_nics`` maps each
    node name to that node's discovered NIC dicts. A candidate is kept only if its
    network (e.g. ``192.0.2.0/24``) is present on *all* nodes, so the chosen
    binding's subnet exists cluster-wide and connectivity binds uniformly.

    This is stricter than (and complements) :func:`nics_on_array_subnets`: that
    one keeps NICs that can reach the array's portals; this one additionally
    requires the subnet to be configured consistently across the hosts. With a
    single node (or no usable subnet data), returns ``host_nics`` unchanged so
    selection still works; an empty match also falls back to ``host_nics``.
    """
    node_subnet_sets: list[set] = []
    for nics in per_node_nics.values():
        nets = {n for n in (_nic_network(nic) for nic in nics) if n is not None}
        node_subnet_sets.append(nets)
    # Nothing to constrain by: <=1 node, or some node yielded no subnet data.
    if len(node_subnet_sets) <= 1 or any(not s for s in node_subnet_sets):
        return host_nics
    common = set.intersection(*node_subnet_sets)
    if not common:
        return host_nics
    matched = [nic for nic in host_nics if _nic_network(nic) in common]
    return matched or host_nics


def compare_node_interfaces(per_node: dict[str, list[str]]) -> tuple[bool, str]:
    """Check that every node exposes the same set of interface values.

    ``per_node`` maps node name -> list of interface identifiers (NIC names, WWPNs,
    NVMe source addrs). Returns ``(consistent, detail)``. Used by cluster
    validation so storage connectivity binds to a uniform set across the cluster.
    """
    if len(per_node) <= 1:
        return True, "single node"
    sets = {n: set(v) for n, v in per_node.items()}
    common = set.intersection(*sets.values()) if sets else set()
    union = set.union(*sets.values()) if sets else set()
    extra = union - common
    if not extra:
        return True, f"all {len(per_node)} nodes share {sorted(common)}"
    lines = [f"{n}: {sorted(s)}" for n, s in sets.items()]
    return False, "interfaces differ across nodes -> " + "; ".join(lines)


@dataclass
class ConnectorContext:
    """Everything a connector needs at runtime, injected by the API layer.

    * ``array`` — a connected FlashArray client (volumes, snapshots, hosts, pgroups,
      API-token minting). ``None`` only for connectors/operations that do not touch
      the array.
    * ``target`` — the hypervisor connection + decrypted secrets.
    * ``log`` — await ``ctx.log("...")`` to stream a line to the job/UI.
    * ``runner`` — execute Ansible playbooks, SSH commands, or HTTP calls.
    * ``dry_run`` — when True, connectors must validate & plan but make no changes.
    """

    target: HypervisorTarget
    log: LogEmitter
    runner: "JobRunner"
    array: "FlashArrayClient | None" = None
    # The original API token the associated FlashArray was connected with. Lets
    # connectors reuse the single existing token instead of minting a new one
    # (for environments that can't create new array users/tokens).
    array_token: str | None = None
    dry_run: bool = False

    async def emit(self, message: str) -> None:
        await self.log(message)

    def resolve_token(self, explicit: str | None = None) -> str | None:
        """Return the API token an integration should use.

        Preference: an explicitly-supplied token, else the original token the
        array was connected with (``array_token``). Connectors should call this
        instead of minting when they need a token to hand to a hypervisor, so a
        single shared array token works everywhere. Minting remains available via
        ``ctx.array.create_api_token(...)`` where the environment allows it.
        """
        return explicit or self.array_token


# --------------------------------------------------------------------------- #
# The connector base class
# --------------------------------------------------------------------------- #
class HypervisorConnector(ABC):
    """Base class for all hypervisor connectors.

    Subclasses MUST set the class attributes below and implement
    :meth:`validate_connection`. They implement additional operation methods to
    match each entry in ``CAPABILITIES``; unimplemented ones raise
    :class:`CapabilityNotSupported` via the defaults here.
    """

    # --- static metadata (override in subclass) ---
    key: ClassVar[str] = ""  # stable slug, e.g. "vsphere"
    name: ClassVar[str] = ""  # display name, e.g. "VMware vSphere"
    description: ClassVar[str] = ""
    CAPABILITIES: ClassVar[set[Capability]] = set()
    SUPPORTED_PROTOCOLS: ClassVar[set[Protocol]] = set()
    # Maturity flag surfaced in the UI: "ga" | "preview" | "scaffold"
    maturity: ClassVar[str] = "ga"

    def __init__(self, ctx: ConnectorContext):
        self.ctx = ctx

    # ---- class-level introspection (no instance / no I/O) ----
    @classmethod
    def capabilities(cls) -> set[Capability]:
        return set(cls.CAPABILITIES) | {Capability.CONNECT}

    @classmethod
    def supports(cls, capability: Capability) -> bool:
        return capability in cls.capabilities()

    @classmethod
    def target_schema(cls) -> list[FormField]:
        """Fields needed to connect to this hypervisor (rendered in 'Add hypervisor')."""
        return []

    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        """Operations exposed in the day-2 panel. Override in subclass."""
        return []

    @classmethod
    def descriptor(cls) -> dict[str, Any]:
        """Serializable metadata for the API/UI."""
        return {
            "key": cls.key,
            "name": cls.name,
            "description": cls.description,
            "maturity": cls.maturity,
            "capabilities": sorted(c.value for c in cls.capabilities()),
            "protocols": sorted(p.value for p in cls.SUPPORTED_PROTOCOLS),
            "target_schema": [f.to_dict() for f in cls.target_schema()],
            "actions": [a.to_dict() for a in cls.action_schemas()],
            "wizard_steps": cls.wizard_steps(),
        }

    # ---- helper for default implementations ----
    def _unsupported(self, capability: Capability) -> OpResult:
        raise CapabilityNotSupported(self.key, capability)

    # ---- dynamic field options (for discoverable dropdowns) ----
    async def discover_options(self, kind: str) -> list[dict[str, Any]]:
        """Enumerate choices for a dynamically-populated form field.

        ``kind`` is a :class:`DiscoveryKind` value (e.g. ``"nics"``, ``"nvme_sources"``,
        ``"fc_hbas"``). Returns a list of ``{"value": ..., "label": ...}`` dicts the
        UI renders as a (multi-)select. Used to let operators pick which NICs/HBAs
        the storage path binds to without typing them. Default: nothing to offer.
        """
        return []

    # ---- cluster awareness ----
    async def list_nodes(self) -> list["ClusterNode"]:
        """Return the cluster/pool member nodes this target manages.

        Node-specific operations (host registration, connectivity/multipath) fan
        out across these; cluster-wide operations (driver install, datastore/SR/
        storage definition) run once. Default: the single configured host, so
        non-clustered connectors work unchanged. Clustered connectors override to
        discover members (e.g. `pvecm nodes`, `xe host-list`, vCenter, `oc nodes`).
        """
        host = (self.ctx.target.get("node_host") or self.ctx.target.get("host")
                or self.ctx.target.get("pool_master_host")
                or self.ctx.target.get("controller_host") or self.ctx.target.name)
        return [ClusterNode(name=str(host), host=str(host))]

    async def validate_cluster(self, **params: Any) -> OpResult:
        """Validate the cluster is uniformly configured for storage connectivity.

        Checks that every node exposes the same storage interfaces (NICs / FC HBAs
        / NVMe sources) so the chosen binding is valid cluster-wide. Default: a
        single node always validates. Clustered connectors override using
        :func:`compare_node_interfaces`.
        """
        nodes = await self.list_nodes()
        return OpResult.ok(f"{len(nodes)} node(s); no cluster checks for {self.key}",
                           nodes=[n.to_dict() for n in nodes])

    @classmethod
    def wizard_steps(cls) -> list[str]:
        """Ordered action ids the deployment wizard runs end-to-end.

        Default covers the common flow; connectors override to match what they
        support (e.g. CSI/Cinder skip host registration / connectivity since the
        driver auto-manages those). Steps not in the connector's actions are
        skipped by the orchestrator.
        """
        return ["deploy", "register_hosts", "setup_connectivity", "configure"]

    # ---- lifecycle / operations (override the ones you support) ----
    @abstractmethod
    async def validate_connection(self) -> OpResult:
        """Reach the target and confirm credentials. Required of every connector."""
        ...

    async def preflight(self, action_id: str, params: dict[str, Any]) -> OpResult:
        """Validate the objects this action REQUIRES before any are created.

        Run by the deployment wizard (and before mutating day-2 operations) so a
        missing prerequisite fails fast with a clear, actionable error instead of an
        opaque array/SSH error partway through. Read-only — never mutates.

        Default: for array-backed connectors, confirm the associated FlashArray is
        reachable (the fundamental required object). Connectors override to add
        their own required-object checks (e.g. host group present/creatable, source
        volume exists for a clone, image present). Return :meth:`OpResult.fail` with
        a descriptive message to block the operation.
        """
        if self.ctx.array is None:
            return OpResult.ok("No FlashArray prerequisites for this connector")
        try:
            info = await self.ctx.array.info()
        except Exception as exc:  # noqa: BLE001 — surface as a clean preflight failure
            return OpResult.fail(
                "Required FlashArray is not reachable — cannot create storage "
                f"objects: {type(exc).__name__}: {exc}")
        return OpResult.ok("FlashArray reachable", array=info)

    async def require_host_objects(self, host_group: str) -> OpResult:
        """Validate that FlashArray host objects exist BEFORE creating a datastore /
        SR that depends on them.

        A datastore is only usable once the hypervisor's hosts are registered on the
        array (in a populated host group) so volumes can be connected to them.
        Connectors call this at the start of their datastore-creating action so the
        operator gets a clear "register hosts first" error instead of a datastore
        that silently can't attach volumes. Read-only.
        """
        if self.ctx.array is None:
            return OpResult.ok("No FlashArray prerequisites")
        if not host_group:
            return OpResult.fail(
                "No FlashArray host group is configured — register the hypervisor's "
                "hosts on the array before creating the datastore.")
        try:
            members = await self.ctx.array.get_host_group_members(host_group)
        except Exception as exc:  # noqa: BLE001
            return OpResult.fail(
                f"Could not verify FlashArray host group {host_group!r}: "
                f"{type(exc).__name__}: {exc}")
        if not members:
            return OpResult.fail(
                f"FlashArray host group {host_group!r} has no member hosts — register "
                "the hypervisor's hosts on the array before creating the datastore.")
        return OpResult.ok(
            f"host group {host_group!r} has {len(members)} host(s)", hosts=members)

    async def deploy_integration(self, **params: Any) -> OpResult:
        """Install the Everpure integration (plugin/driver/SR/StorageClass) on the target."""
        return self._unsupported(Capability.DEPLOY_PLUGIN)

    async def configure(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.CONFIGURE)

    async def provision(self, **params: Any) -> OpResult:
        """Create a datastore/StorageClass/SR or an individual volume/PV/disk.

        ``params['kind']`` distinguishes datastore vs volume when a connector
        supports both PROVISION_DATASTORE and PROVISION_VOLUME.
        """
        return self._unsupported(Capability.PROVISION_VOLUME)

    async def snapshot(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.SNAPSHOT)

    async def clone(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.CLONE)

    async def resize(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.RESIZE)

    async def delete(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.DELETE)

    async def register_hosts(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.HOST_REGISTER)

    async def apply_host_group(self, host_group: str,
                               specs: list[dict[str, Any]]) -> dict[str, Any]:
        """Register FlashArray hosts for ``specs`` into a host group, reusing hosts
        that already exist (matched by initiator) and ADOPTING a pre-existing host
        group when the pool hosts already belong to one.

        Shared by all connectors so host registration behaves identically: an
        initiator can belong to only one FA host (so pre-registered hosts are
        reused, never duplicated), and a FA host can be in only one host group (so
        an existing group is adopted rather than fought).

        ``specs`` is a list of ``{"name", "iqns", "wwns", "nqns"}`` (intended FA host
        name + that node's initiators). Returns the client result dict with keys
        ``host_group`` (the EFFECTIVE group — adopted or requested), ``hosts``,
        ``adopted``, ``created``, ``reused``, and ``conflict``. When ``conflict`` is
        set (hosts span multiple existing groups), NOTHING was mutated and the
        caller must fail the operation with that descriptive message. Emits
        reuse / adoption notes for visibility.
        """
        if self.ctx.array is None:
            return {"host_group": host_group, "hosts": [], "adopted": False,
                    "created": [], "reused": [],
                    "conflict": "No FlashArray associated with this hypervisor."}
        res = await self.ctx.array.register_host_group(host_group, specs)
        if res.get("conflict"):
            await self.ctx.emit(f"[host-register] BLOCKED: {res['conflict']}")
            return res
        if res.get("reused"):
            await self.ctx.emit(
                "[host-register] Reusing existing FlashArray host(s): "
                + ", ".join(res["reused"]))
        if res.get("adopted"):
            await self.ctx.emit(
                f"[host-register] Pool hosts already belong to FlashArray host group "
                f"{res['host_group']!r}; adopting it as the integration's host group "
                f"(instead of {host_group!r}).")
        return res

    async def setup_connectivity(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.CONNECTIVITY)

    async def set_qos(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.QOS)

    async def configure_replication(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.REPLICATION)

    async def recover(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.RECOVERY)

    async def upgrade(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.UPGRADE)

    async def rotate_credentials(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.ROTATE_CREDENTIALS)

    async def health_check(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.HEALTH)

    async def teardown(self, **params: Any) -> OpResult:
        return self._unsupported(Capability.REMOVE)

    async def assess_cluster(self, **params: Any) -> OpResult:
        """READ-ONLY: report cluster membership drift WITHOUT changing anything.

        Returns ``data`` with:
        * ``nodes`` -- current cluster node names (from :meth:`list_nodes`);
        * ``new_hosts`` -- nodes not yet configured on the array, each with a
          **readiness verdict** (``{"node","host","ready","reasons"}``) from the
          preflight (reachable + initiators + storage NIC on the array subnet +
          matches existing nodes), so the UI can show which are deployable;
        * ``departed_hosts`` -- FA host-group members with no matching current node.

        The cluster monitor calls this every cycle and stores the result in state;
        the "Deploy to new hosts" button then runs :meth:`reconcile_cluster`.
        Clustered connectors override; the default reports nodes only.
        """
        nodes = await self.list_nodes()
        return OpResult.ok(
            f"{len(nodes)} node(s); no cluster assessment for {self.key}",
            nodes=[n.name for n in nodes], new_hosts=[], departed_hosts=[])

    async def reconcile_cluster(self, apply_removals: bool = False,
                                **params: Any) -> OpResult:
        """Apply cluster changes: configure READY new hosts; optionally remove departed.

        This is the "Deploy to new hosts" action. It:

        1. Assesses the cluster (:meth:`assess_cluster`).
        2. Configures the new hosts that **pass the readiness preflight** (per-node
           install + ``register_hosts`` + ``setup_connectivity``); new hosts that
           FAIL preflight are **skipped and flagged** (``data["not_ready"]``) -- never
           half-configured.
        3. Reports departed FA hosts. With ``apply_removals=False`` (default) they are
           only flagged (``data["pending_removals"]``); with ``apply_removals=True``
           they are removed from the host group and deleted from the array.

        Returns ``data["nodes"]`` (current membership) so the monitor's baseline can
        update. Clustered connectors override; the default is unsupported.
        """
        return self._unsupported(Capability.RECONCILE_CLUSTER)

    # ---- VM management (override the ones you support) ----
    #
    # These power cross-hypervisor migration (see phif.migrate). A connector that
    # can be a migration source/destination declares VM_INVENTORY + VM_LIFECYCLE +
    # MIGRATE and implements all of them. The split mirrors the existing pattern:
    # read-only inventory methods back the wizard's pick/review steps; mutating
    # lifecycle methods are driven by the MigrationService orchestrator.

    async def list_vms(self) -> list[dict[str, Any]]:
        """READ-ONLY: enumerate VMs on this hypervisor for the migration picker.

        Returns dicts with at least ``{id, name, power_state}`` and, when cheap to
        gather, ``{vcpus, memory_bytes, disk_count, nic_count}``. ``id`` is the
        connector-native VM reference passed back to :meth:`capture_vm_spec` etc.
        """
        return self._unsupported(Capability.VM_INVENTORY)

    async def list_networks(self) -> list[dict[str, Any]]:
        """READ-ONLY: enumerate the destination networks a NIC can map to.

        Returns dicts with ``{id, name}`` (optionally ``kind``). ``id`` is what the
        operator's per-NIC ``network_map`` references and what :meth:`create_vm`
        wires each NIC to.
        """
        return self._unsupported(Capability.VM_INVENTORY)

    async def list_placements(self) -> list[dict[str, Any]]:
        """READ-ONLY: enumerate destination placements for the migration wizard.

        Returns a list of ``{"cluster": {"id","name"}, "storage": [{"id","name",
        "kind"}]}``. ONLY clusters that have FlashArray-backed storage are returned,
        and ``storage`` lists ONLY the Everpure-connected datastores/SRs/pools — so the
        UI can require the operator to land the migrated VM on Everpure storage in a
        cluster that can reach it. Connectors with a single implicit cluster/store
        may return one entry; those without the concept return ``[]`` (the wizard
        then omits the selectors and the connector auto-places).
        """
        return []

    async def capture_vm_spec(self, vm_ref: str) -> "VmSpec":
        """READ-ONLY: read a VM's logical hardware into a normalized VmSpec.

        Resolves each disk to its backing FlashArray volume (name + serial via
        ``ctx.array.get_volume``) so the destination can match by serial. A disk
        not backed by a FlashArray volume should raise — this migration method
        moves no data and cannot relocate such a disk.
        """
        return self._unsupported(Capability.VM_INVENTORY)

    async def power_state(self, vm_ref: str) -> str:
        """READ-ONLY: ``running`` | ``stopped`` | ``unknown`` for one VM."""
        return self._unsupported(Capability.VM_INVENTORY)

    async def stop_vm(self, vm_ref: str, *, force: bool = False) -> OpResult:
        """Power the VM off (graceful unless ``force``). Idempotent if already off."""
        return self._unsupported(Capability.VM_LIFECYCLE)

    async def start_vm(self, vm_ref: str) -> OpResult:
        """Power the VM on. Idempotent if already running."""
        return self._unsupported(Capability.VM_LIFECYCLE)

    async def detach_volumes(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        """Remove the given disks from the VM's CONFIG (the FlashArray volume and
        its data are left intact). The inverse of :meth:`attach_existing_volumes`."""
        return self._unsupported(Capability.VM_LIFECYCLE)

    async def create_vm(self, spec: "VmSpec", *,
                        network_map: dict[str, str],
                        placement: dict[str, Any] | None = None) -> OpResult:
        """Create a VM matching ``spec``'s logical hardware (NO disks created here
        beyond firmware/EFI scaffolding); wire each NIC to ``network_map[source]``
        with the source MAC PRESERVED. Returns ``artifacts={'vm_ref': ...}``.

        ``placement`` (optional) carries the operator's destination choices from
        :meth:`list_placements`: ``{"cluster": <id>, "storage": <id>}``. Connectors
        that support placement honor it; others auto-place and ignore it.
        """
        return self._unsupported(Capability.VM_LIFECYCLE)

    async def attach_existing_volumes(self, vm_ref: str,
                                      disks: "list[DiskSpec]") -> OpResult:
        """Attach EXISTING FlashArray volumes (matched by serial -> WWID/EUI) to the
        VM as raw block devices. Never provisions a new volume."""
        return self._unsupported(Capability.VM_LIFECYCLE)

    async def create_managed_disk(self, vm_ref: str, *, size_bytes: int,
                                  order: int, boot: bool) -> str:
        """Provision a NEW disk of ``size_bytes`` on ``vm_ref`` THROUGH this
        hypervisor's storage plugin (so the backing FlashArray volume is created
        and managed in the plugin's own namespace) and attach it. Returns the
        backing FlashArray volume NAME.

        Migration creates the destination disk this way and then does a FlashArray
        copy-with-overwrite from the source volume onto it — so the destination disk
        is correctly plugin-managed while the source volume is left untouched.
        """
        return self._unsupported(Capability.VM_LIFECYCLE)

    async def set_boot_order(self, vm_ref: str,
                             disks: "list[DiskSpec]") -> OpResult:
        """Set the VM to boot from the disk marked ``boot`` (first by order)."""
        return self._unsupported(Capability.VM_LIFECYCLE)

    async def finalize_destination_disks(self, vm_ref: str,
                                         disks: "list[DiskSpec]") -> OpResult:
        """Post-copy hook: reconcile the destination VM's attached disks with the
        backing FlashArray volumes AFTER the data copy has completed.

        Called once after all ``create_managed_disk`` + copy-with-overwrite steps
        and BEFORE set_boot/power-on. The default does nothing (most connectors'
        attached devices stay valid across an array-level overwrite); it exists as a
        general hook for connectors that must reconcile attached devices with their
        backing volumes once the data is in place. Must be idempotent. Returns an
        OpResult (a failure aborts → rollback)."""
        return OpResult.ok("no destination-disk finalization needed")

    async def convert_disks_to_native(self, vm_ref: str,
                                      disks: "list[DiskSpec]", *,
                                      datastore: str | None = None) -> OpResult:
        """Optional post-migration step: convert the migration's RDM/raw-device disks
        into native datastore-backed disks and FREE the temporary per-disk FA volumes.

        Runs only when the migration's ``convert_to_vmfs`` option is set, AFTER the
        destination VM is up. vSphere overrides it to Storage-vMotion each
        virtual-mode RDM onto a VMFS datastore (converting it to a VMDK), then
        disconnect + eradicate the now-orphaned ``phifmig-dst-*`` FA volumes. The
        default is a no-op (most destinations have no separate raw-device stage).
        Best-effort: the VM is already running on its RDMs, so a failure here is a
        WARNING, not a migration failure — and the connector must only delete an FA
        volume AFTER its disk has successfully converted (never orphan live data)."""
        return OpResult.ok("no disk conversion needed")

    async def delete_vm(self, vm_ref: str, *, keep_disks: bool = True) -> OpResult:
        """Delete the VM definition. With ``keep_disks`` (the only mode migration
        uses) the backing FlashArray volumes are NEVER deleted/eradicated. Used by
        migration rollback to remove a half-created destination VM."""
        return self._unsupported(Capability.VM_LIFECYCLE)

    async def prepare_source_disks(self, spec: "VmSpec",
                                   options: dict[str, Any]) -> "VmSpec":
        """Pre-migration hook: prepare source VM disks for migration.

        Called after ``capture_vm_spec`` and before ``_resolve_volumes``. The
        default does nothing (most connectors expose every disk as its own FA
        volume already). Connectors where some disk types are not directly backed
        by a per-disk FA volume (e.g. vSphere VMFS) override this to perform the
        storage preparation needed before a FlashArray vol-copy can proceed —
        e.g. Storage vMotion a VMFS VMDK to an RDM or vVol so each disk gains its
        own FA volume identity. Must return the (possibly updated) ``VmSpec``.
        """
        return spec

    async def cleanup_migration_scratch(self, spec: "VmSpec") -> None:
        """Post-migration hook: remove any TEMPORARY artifacts this connector
        created for the migration (scratch FA volumes, RDM pointer files, host
        mappings, etc.) — but NEVER the real source VM/disks or the destination's
        volumes. Called on BOTH successful completion and rollback, so it must be
        idempotent. Default does nothing (most connectors create no scratch:
        their disks are already per-disk FA volumes). vSphere overrides it to
        delete the ``phifmig-src-*`` clone volumes + RDM pointer vmdks it made.
        """
        return None

    async def prepare_copy_target(self, *, base_name: str, order: int,
                                  dest_vm_ref: str) -> str:
        """Return the FlashArray volume NAME a source disk should be CLONED into so
        that this connector (acting as a migration *Copy* destination) can attach
        it under its own storage-plugin namespace, creating any volume group needed.

        Copy mode clones the source volume on the array (data stays put on the
        source); the clone must be named where the destination's plugin will find
        it (e.g. an XCP-ng vgroup-member name the SMAPIv3 plugin resolves, or a
        Proxmox per-VM vgroup member the purefa plugin resolves). Default: a flat
        migration-scoped name.
        """
        return f"phifmig-{dest_vm_ref}-disk{order}-{base_name}"

    async def plan_volume_adoption(
        self, disks: "list[DiskSpec]", dest_vm_ref: str,
    ) -> "list[tuple[DiskSpec, str]]":
        """For a MOVE, return ``[(disk, new_fa_volume_name)]`` for disks whose array
        volume must be RENAMED into this destination's storage namespace so its
        plugin can attach it (e.g. Proxmox resolves disks only within the
        destination VM's ``vm-<vmid>/…`` group, so a volume carrying a different
        VMID's group name won't resolve). Default: no rename needed (e.g. XCP-ng
        attaches an existing volume by location, HPE by REST reference).
        """
        return []

    async def _disk_wwids(self, disks: "list[DiskSpec]") -> list[str]:
        """Resolve each disk's FlashArray volume to its SCSI multipath WWID (via the
        array-assigned serial). Shared by connectors' attach paths to flush stale
        multipath maps before activating a freshly-mapped device. Best-effort: a
        volume that can't be resolved is skipped (the rescan still runs)."""
        from phif.connectors.multipath import wwids_for

        serials: list[str] = []
        if self.ctx.array is not None:
            for d in disks:
                try:
                    vol = await self.ctx.array.get_volume(d.identity.fa_volume)
                    serial = (vol or {}).get("serial")
                    if serial:
                        serials.append(serial)
                except Exception:  # noqa: BLE001 — best-effort
                    pass
        return wwids_for(serials)

    def migration_host_group(self) -> str:
        """The FlashArray host group this hypervisor's hosts belong to.

        Migration unmaps volumes from the source group and maps them to the
        destination group. Defaults to ``connection['host_group']``.
        """
        return self.ctx.target.get("host_group") or ""

    @staticmethod
    def score_host_readiness(
        protocol: str, *, reachable: bool, initiators: dict[str, Any],
        host_nics: list[dict[str, Any]], array_portals: list[str],
        baseline_subnets: "set[str] | None" = None,
    ) -> dict[str, Any]:
        """Everpure readiness scorer for a candidate cluster host (no I/O; testable).

        The connector gathers the inputs (it knows the SSH creds + array) and calls
        this. Returns ``{"ready": bool, "reasons": [str, ...]}`` covering: host
        reachable, the protocol-relevant initiator was discovered, a storage NIC
        sits on the array's portal subnet (IP transports), and that subnet matches
        the existing nodes' (``baseline_subnets``).
        """
        if not reachable:
            return {"ready": False, "reasons": ["host unreachable over SSH"]}
        reasons: list[str] = []
        proto = (protocol or "iscsi").lower()
        is_nvme = proto in ("nvme-tcp", "nvme-fc", "nvme-roce")
        if is_nvme:
            if not (initiators or {}).get("nqn"):
                reasons.append("no NVMe NQN discovered on the host")
        elif proto in ("fc", "nvme-fc"):
            if not (initiators or {}).get("wwns"):
                reasons.append("no FC WWNs discovered on the host")
        else:  # iscsi
            if not (initiators or {}).get("iqn"):
                reasons.append("no iSCSI IQN discovered on the host")
        # IP transports must have a NIC on the array's storage subnet. (We can only
        # check this when the array's portals are known; skip otherwise.)
        if proto in ("iscsi", "nvme-tcp") and array_portals:
            import ipaddress

            portals = []
            for p in array_portals:
                try:
                    portals.append(ipaddress.ip_address(p))
                except ValueError:
                    continue
            matched_subnets: set[str] = set()
            for nic in host_nics or []:
                net = _nic_network(nic)
                if net and any(ip in net for ip in portals):
                    matched_subnets.add(str(net))
            if not matched_subnets:
                reasons.append("no storage NIC on the array's portal subnet")
            elif baseline_subnets and not (matched_subnets & set(baseline_subnets)):
                reasons.append("storage NIC subnet doesn't match existing nodes")
        return {"ready": not reasons, "reasons": reasons}

    async def _prune_departed_fa_hosts(
        self, host_group: str, expected_hosts: "set[str] | list[str]",
        *, apply_removals: bool,
    ) -> dict[str, Any]:
        """Compare the FA host group's members to ``expected_hosts`` and act on drift.

        Shared by clustered connectors' ``reconcile_cluster``. Returns a summary
        ``{"members", "expected", "departed", "removed"}``. When ``apply_removals``,
        each departed host is removed from the group and the FA host is deleted;
        otherwise departed hosts are only reported (flagged).
        """
        expected = {h for h in expected_hosts if h}
        result: dict[str, Any] = {"members": [], "expected": sorted(expected),
                                  "departed": [], "removed": []}
        if self.ctx.array is None or not host_group:
            return result
        members = await self.ctx.array.get_host_group_members(host_group)
        result["members"] = sorted(members)
        departed = [m for m in members if m not in expected]
        result["departed"] = sorted(departed)
        if not apply_removals:
            return result
        for host in departed:
            await self.ctx.emit(f"Removing departed host {host!r} from group "
                                f"{host_group!r} and the array")
            await self.ctx.array.remove_host_from_group(host_group, host)
            await self.ctx.array.delete_host(host)
            result["removed"].append(host)
        return result

    # ---- dispatch table: action id -> bound method ----
    # Connectors with custom action ids should override ``dispatch``.
    _DEFAULT_DISPATCH: ClassVar[dict[str, str]] = {
        "deploy": "deploy_integration",
        "configure": "configure",
        "provision": "provision",
        "snapshot": "snapshot",
        "clone": "clone",
        "resize": "resize",
        "delete": "delete",
        "register_hosts": "register_hosts",
        "setup_connectivity": "setup_connectivity",
        "set_qos": "set_qos",
        "configure_replication": "configure_replication",
        "recover": "recover",
        "upgrade": "upgrade",
        "rotate_credentials": "rotate_credentials",
        "health_check": "health_check",
        "teardown": "teardown",
        "assess_cluster": "assess_cluster",
        "reconcile_cluster": "reconcile_cluster",
    }

    async def dispatch(self, action_id: str, params: dict[str, Any]) -> OpResult:
        """Route an action id from the API/UI to the right method.

        Default routing maps known ids to the standard methods. Connectors with
        bespoke action ids should override and fall back to ``super().dispatch``.
        """
        method_name = self._DEFAULT_DISPATCH.get(action_id)
        if method_name is None:
            return OpResult.fail(f"Unknown action {action_id!r} for connector {self.key!r}")
        method = getattr(self, method_name)
        return await method(**params)
