"""Red Hat OpenShift / Kubernetes connector backed by Portworx (px-csi).

Portworx (``pxd.portworx.com``) is the only supported CSI driver: the legacy Everpure
Service Orchestrator (``pure-csi``) Helm chart has been retired and no
longer functions, so it has been removed. ``deploy`` installs the Portworx
Operator (OLM) + a StorageCluster — either one generated in Portworx Central
(``central.portworx.com``) and supplied as a spec URL / YAML, or PHIF's generated
FlashArray Direct Access (FADA) spec.

The connector drives ``kubectl`` and ``oc`` on the management host via
``ctx.runner.run_local`` (which streams output and is mock-safe). All cluster
auth flows through a kubeconfig supplied as a target secret; each method that
talks to the cluster writes that kubeconfig to a temp file and points the CLI at
it with ``--kubeconfig``.

Storage manifests (StorageClass / VolumeSnapshotClass / PVC / VolumeSnapshot) and
the Portworx install objects are rendered in
:mod:`phif.connectors.openshift.manifests` and applied via ``oc apply -f <tmpfile>``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import re
import shlex
import tempfile
from typing import Any

from phif.connectors.base import (
    ActionSpec,
    Capability,
    ClusterNode,
    ConnectionValidationError,
    ConnectorContext,
    DiscoveryKind,
    FieldType,
    FormField,
    HypervisorConnector,
    OpResult,
    Protocol,
    compare_node_interfaces,
)
from phif.connectors import iscsi_net
from phif.connectors.openshift import manifests
from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec


class MigrationCreateError(Exception):
    """A KubeVirt migration step failed (VM/PVC create, capture, bind timeout).

    Raised by the migration methods so the orchestrator's run() catches it and
    triggers rollback (it treats any exception as a failure)."""

# Interfaces that are never iSCSI storage NICs on an OpenShift node: loopback and
# the OVN-Kubernetes / OVS / CNI overlay + bridge + virtual devices that show up
# in NodeNetworkState alongside the real physical / bond / VLAN NICs. We filter
# these out so the interface-binding form only offers selectable storage NICs.
_NON_STORAGE_NIC_PREFIXES = (
    "br-", "br0", "ovs", "ovn", "veth", "vxlan", "genev", "cni", "docker",
    "tun", "tap", "flannel", "antrea", "cali", "virbr", "dummy", "kube-ipvs",
    "nodelocaldns", "lxc", "gre", "sit", "ip6", "bonding_masters",
    # OVN-Kubernetes OVS patch ports (e.g. patch-br-ex_<node>-to-br-int) and the
    # OVN management / geneve / external ports — never storage NICs.
    "patch-", "patch_", "k8s-", "int-", "ext-", "mp0",
)


# A Linux network-interface name: starts alphanumeric, then alphanumerics with
# single ``.`` / ``-`` / ``_`` / ``:`` separators (covers eno1, ens1f0, enp3s0,
# eth0, em1, bond0, and dotted VLANs like ens1f0.2230). Used to reject stray
# tokens — e.g. words from an `oc` ERROR message ("error: the server doesn't have
# a resource type nns") when NMState isn't installed — that would otherwise be
# parsed as bogus NIC options. Real storage NIC names also carry a digit (the
# port/index), which the error words ("the", "server", "resource", ...) do not.
_INTERFACE_RE = re.compile(r"^[A-Za-z0-9]+([._:-][A-Za-z0-9]+)*$")

# Markers that an `oc`/`kubectl` invocation returned an error / no-data line on
# stdout+stderr instead of the requested data (the runner merges streams and we
# call with check=False, so failures arrive as text we must recognise).
_OC_ERROR_MARKERS = (
    "error:", "error from server", "the server doesn't have",
    "doesn't have a resource", "no resources found", "not found", "notfound",
    "forbidden", "unable to", "couldn't find", "could not find",
    "command not found", "unknown",
)


def _looks_like_oc_error(raw: str) -> bool:
    """Whether ``raw`` looks like an oc/kubectl error rather than real output."""
    low = (raw or "").lower()
    return any(m in low for m in _OC_ERROR_MARKERS)


def _is_storage_nic_candidate(name: str) -> bool:
    """Whether ``name`` is plausibly a real storage NIC (vs loopback/overlay/noise).

    Keeps physical / bond / VLAN interfaces (eno*, ens*, enp*, eth*, em*, bond*,
    and dotted VLANs like ens1f0.2230); drops ``lo``, the OVN/OVS/CNI virtual
    devices in :data:`_NON_STORAGE_NIC_PREFIXES`, anything that isn't a
    syntactically valid interface name, and digit-less tokens (which are almost
    always stray words from an error line, never real storage NIC names).
    """
    if not name or name == "lo":
        return False
    if not _INTERFACE_RE.match(name) or not any(c.isdigit() for c in name):
        return False
    low = name.lower()
    return not any(low.startswith(p) for p in _NON_STORAGE_NIC_PREFIXES)


class OpenShiftConnector(HypervisorConnector):
    # --- static metadata ---
    key = "openshift"
    name = "Red Hat OpenShift (Portworx)"
    description = (
        "Manage Everpure storage on Red Hat OpenShift / Kubernetes via Portworx "
        "(px-csi, pxd.portworx.com). Installs the Portworx Operator + a "
        "StorageCluster (from a Portworx Central spec or PHIF's generated "
        "FlashArray Direct Access spec), configures StorageClasses and "
        "VolumeSnapshotClasses, binds storage NICs via MachineConfig, and "
        "provisions / snapshots / clones / resizes PersistentVolumeClaims. The "
        "legacy Service Orchestrator (`pure-csi`) driver has been retired and is no "
        "longer supported."
    )
    maturity = "ga"
    CAPABILITIES = {
        Capability.DEPLOY_PLUGIN,
        Capability.CONFIGURE,
        Capability.PROVISION_VOLUME,
        Capability.SNAPSHOT,
        Capability.CLONE,
        Capability.RESIZE,
        Capability.ROTATE_CREDENTIALS,
        Capability.UPGRADE,
        Capability.HEALTH,
        Capability.REMOVE,
        # Migration: OpenShift Virtualization (KubeVirt) VMs to/from the other
        # hypervisors. Each VM disk is a px-csi (FADA) PVC = a FlashArray volume.
        Capability.VM_INVENTORY,
        Capability.VM_LIFECYCLE,
        Capability.MIGRATE,
    }
    SUPPORTED_PROTOCOLS = {Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP, Protocol.NFS}

    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)

    # ------------------------------------------------------------------ #
    # UI: how to connect to this target
    # ------------------------------------------------------------------ #
    @classmethod
    def target_schema(cls) -> list[FormField]:
        return [
            FormField("kubeconfig", "Kubeconfig", FieldType.TEXT, required=False,
                      help="Full kubeconfig granting cluster-admin for the Portworx "
                           "install / day-2 ops. Provide this OR the API URL + "
                           "username + password below.",
                      placeholder="apiVersion: v1\nclusters: ..."),
            FormField("api_url", "API server URL", FieldType.STRING, required=False,
                      placeholder="https://api.ocp.example.com:6443",
                      help="OpenShift API endpoint for username/password login "
                           "(`oc login`). Used only when no kubeconfig is supplied."),
            FormField("username", "Username", FieldType.STRING, required=False,
                      help="OpenShift user for `oc login` (when not using a kubeconfig)."),
            FormField("password", "Password", FieldType.SECRET, required=False,
                      help="Password for `oc login` (stored encrypted in the vault; "
                           "masked in job logs)."),
            FormField("insecure_skip_tls_verify", "Skip API TLS verify",
                      FieldType.BOOL, default=False, required=False,
                      help="Pass --insecure-skip-tls-verify to `oc login` for "
                           "clusters with a self-signed API certificate."),
            FormField("namespace", "Namespace", FieldType.STRING, default="portworx",
                      help="Namespace Portworx is installed into."),
            FormField("vm_namespace", "VM namespace (migration)", FieldType.STRING,
                      default="default", required=False,
                      help="Namespace OpenShift Virtualization (KubeVirt) VMs are "
                           "created in / discovered from for migration."),
            FormField("protocol", "Storage protocol", FieldType.ENUM, default="iscsi",
                      options=["iscsi", "fc", "nvme-tcp"],
                      help="FlashArray transport Portworx attaches block volumes "
                           "over. 'fc' optionally pre-registers worker-node HBAs on "
                           "the array (see node_wwns); Portworx otherwise "
                           "auto-registers the worker nodes as FlashArray hosts at "
                           "volume-attach time, so no manual WWN registration is "
                           "required."),
            FormField("node_wwns", "Worker node FC WWNs", FieldType.STRING, required=False,
                      placeholder="21:00:00:..., 21:00:00:...",
                      help="(optional — Portworx auto-registers worker nodes on the "
                           "array; only needed for pre-zoning) Comma-separated FC "
                           "HBA WWNs of the OpenShift worker nodes to pre-register as "
                           "FlashArray hosts when protocol=fc."),
            # The FlashArray management endpoint and API token Portworx authenticates
            # with (the px-pure-secret) are NOT operator-entered fields: they are
            # derived from the FlashArray associated with this hypervisor
            # (ctx.array.endpoint and ctx.resolve_token()). The deploy / rotate
            # methods accept optional overrides as kwargs for advanced use, but the
            # UI does not prompt.
        ]

    # ------------------------------------------------------------------ #
    # UI: day-2 actions
    # ------------------------------------------------------------------ #
    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        return [
            ActionSpec(
                Capability.DEPLOY_PLUGIN, "deploy", "Deploy Portworx (px-csi)",
                "Install Portworx (px-csi): the Portworx Operator (OLM) + a "
                "StorageCluster. The StorageCluster comes from a spec you generated "
                "in Portworx Central (paste its spec URL or YAML below) — which "
                "carries your license + console tuning — or, if you leave both "
                "blank, from PHIF's generated FlashArray Direct Access spec. The "
                "px-pure-secret (FlashArray endpoint + token) is derived from the "
                "associated array. The retired PSO (pure-csi) driver is no longer "
                "available.",
                fields=[
                    FormField("release", "StorageCluster name", FieldType.STRING,
                              default=manifests.PX_STORAGECLUSTER_DEFAULT, required=False,
                              help="Portworx StorageCluster name (used only for "
                                   "PHIF's generated spec; a Central spec names "
                                   "itself)."),
                    FormField("cluster_id", "Cluster ID", FieldType.STRING,
                              help="Unique id tagged onto this cluster's resources."),
                    FormField("chart_version", "Portworx version", FieldType.STRING,
                              required=False, placeholder="latest",
                              help="Portworx image version (generated spec only)."),
                    FormField("fa_direct_access", "Portworx: FlashArray Direct Access",
                              FieldType.BOOL, default=True, required=False,
                              help="Portworx only. On (default) = FADA: PVCs map "
                                   "1:1 to FlashArray volumes, no pooled SDS / "
                                   "PX-Enterprise license. Off = Enterprise "
                                   "cloud-drive pooling. Ignored when a Portworx "
                                   "Central spec (URL/YAML) is supplied — that spec "
                                   "decides the mode."),
                    FormField("px_spec_url", "Portworx: Central spec URL",
                              FieldType.STRING, required=False,
                              placeholder="https://install.portworx.com/...",
                              help="Portworx only. Paste the StorageCluster / PX-CSI "
                                   "spec URL generated in Portworx Central "
                                   "(central.portworx.com → Generate Spec). PHIF "
                                   "installs the operator + px-pure-secret, then "
                                   "applies THIS spec verbatim (oc apply -f <url>) — "
                                   "so your Central license / tuning is preserved. "
                                   "Leave blank to apply PHIF's generated FADA "
                                   "StorageCluster instead."),
                    FormField("px_spec_yaml", "Portworx: Central spec YAML",
                              FieldType.TEXT, required=False,
                              placeholder="apiVersion: core.libopenstorage.org/v1\n"
                                          "kind: StorageCluster\n...",
                              help="Portworx only. Alternative to the spec URL: paste "
                                   "the StorageCluster YAML downloaded from Portworx "
                                   "Central. Applied verbatim after the operator + "
                                   "px-pure-secret. Takes precedence over the URL if "
                                   "both are given."),
                    FormField("px_operator_url", "Portworx: Operator manifest URL",
                              FieldType.STRING, required=False,
                              placeholder="https://install.portworx.com/...?comp=pxoperator...",
                              help="Portworx only. The Portworx Operator install URL "
                                   "from Portworx Central (the `comp=pxoperator` "
                                   "manifest — applied with oc apply -f). Use this when "
                                   "the Portworx Operator is NOT in the cluster's "
                                   "OperatorHub (the common case). If left blank, PHIF "
                                   "derives it from the spec URL above; if neither is "
                                   "available it falls back to the OLM "
                                   "(OperatorHub) Subscription."),
                ],
            ),
            ActionSpec(
                Capability.CONFIGURE, "configure", "Configure StorageClass / SnapshotClass",
                "Apply a StorageClass and a VolumeSnapshotClass for the CSI driver.",
                fields=[
                    FormField("storage_class", "StorageClass name", FieldType.STRING,
                              default="pure-block"),
                    FormField("snapshot_class", "VolumeSnapshotClass name", FieldType.STRING,
                              default="pure-snapshotclass"),
                    FormField("backend", "Backend", FieldType.ENUM, default="block",
                              options=["block", "file"], required=False),
                    FormField("fs_type", "Filesystem type", FieldType.STRING,
                              default="xfs", required=False),
                    FormField("is_default", "Default StorageClass", FieldType.BOOL,
                              default=False, required=False),
                    FormField("force", "Force create (ignore existing)", FieldType.BOOL,
                              default=False, required=False,
                              help="Portworx: by default, if the cluster already has "
                                   "pxd.portworx.com StorageClasses they are reused and "
                                   "creation is skipped. Enable to create this "
                                   "StorageClass anyway."),
                ],
            ),
            ActionSpec(
                Capability.CONFIGURE, "configure_binding",
                "Configure interface binding (MachineConfig)",
                "Bind the storage data path to specific node interfaces by applying a "
                "MachineConfig (Ignition) to the selected machine-config-pool role. "
                "iSCSI: writes an iscsiadm iface file per selected NIC + the ARP-flux "
                "sysctls (arp_ignore/arp_announce). NVMe-TCP: writes host config + a "
                "oneshot 'nvme connect' unit per source. FC: records the selected HBAs "
                "(zoning is switch-side). WARNING: the Machine Config Operator applies "
                "this by DRAINING AND REBOOTING the role's nodes one at a time (a "
                "rolling reboot) — operators never SSH the nodes.",
                fields=[
                    FormField("iscsi_nics", "iSCSI NICs", FieldType.MULTISELECT,
                              required=False, options_source=DiscoveryKind.NICS.value,
                              help="Node NICs to bind iSCSI ifaces to."),
                    FormField("nvme_sources", "NVMe-TCP sources", FieldType.MULTISELECT,
                              required=False,
                              options_source=DiscoveryKind.NVME_SOURCES.value,
                              help="Host source interfaces/addresses (host-traddr) for NVMe-TCP."),
                    FormField("nvme_options", "NVMe connect options", FieldType.STRING,
                              required=False, placeholder="--nr-io-queues=8",
                              help="Extra args passed verbatim to 'nvme connect'."),
                    FormField("fc_hbas", "FC HBAs", FieldType.MULTISELECT,
                              required=False, options_source=DiscoveryKind.FC_HBAS.value,
                              help="FC HBAs to record (selection is zoning-driven)."),
                    FormField("machine_config_role", "MachineConfig role", FieldType.ENUM,
                              default="worker", required=False,
                              options=["worker", "master"],
                              help="machine-config-pool role label the MachineConfig targets."),
                ],
            ),
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision", "Provision volume (PVC)",
                "Create a PersistentVolumeClaim backed by the Portworx StorageClass.",
                fields=[
                    FormField("name", "PVC name", FieldType.STRING),
                    FormField("size", "Size", FieldType.SIZE, default="10Gi"),
                    FormField("storage_class", "StorageClass", FieldType.STRING,
                              default="pure-block"),
                    FormField("access_mode", "Access mode", FieldType.ENUM,
                              default="ReadWriteOnce", required=False,
                              options=["ReadWriteOnce", "ReadWriteMany", "ReadOnlyMany"]),
                ],
            ),
            ActionSpec(
                Capability.SNAPSHOT, "snapshot", "Snapshot PVC",
                "Create a VolumeSnapshot custom resource of a PVC.",
                fields=[
                    FormField("name", "Snapshot name", FieldType.STRING),
                    FormField("source_pvc", "Source PVC", FieldType.STRING),
                    FormField("snapshot_class", "VolumeSnapshotClass", FieldType.STRING,
                              default="pure-snapshotclass", required=False),
                ],
            ),
            ActionSpec(
                Capability.CLONE, "clone", "Clone PVC",
                "Create a new PVC whose dataSource is an existing PVC or VolumeSnapshot.",
                fields=[
                    FormField("name", "New PVC name", FieldType.STRING),
                    FormField("source", "Source PVC / snapshot name", FieldType.STRING),
                    FormField("source_kind", "Source kind", FieldType.ENUM,
                              default="PersistentVolumeClaim", required=False,
                              options=["PersistentVolumeClaim", "VolumeSnapshot"]),
                    FormField("size", "Size", FieldType.SIZE, default="10Gi"),
                    FormField("storage_class", "StorageClass", FieldType.STRING,
                              default="pure-block"),
                ],
            ),
            ActionSpec(
                Capability.RESIZE, "resize", "Resize PVC",
                "Patch a PVC's requested storage (StorageClass must allow expansion).",
                fields=[
                    FormField("name", "PVC name", FieldType.STRING),
                    FormField("size", "New size", FieldType.SIZE),
                ],
            ),
            ActionSpec(
                Capability.ROTATE_CREDENTIALS, "rotate_credentials", "Rotate FlashArray token",
                "Re-apply the px-pure-secret with a new (or freshly minted) "
                "FlashArray API token; Portworx picks up the updated credential.",
                fields=[
                    FormField("api_token", "New FlashArray API token", FieldType.SECRET,
                              required=False,
                              help="Leave blank to mint a fresh token from the "
                                   "associated array."),
                ],
            ),
            ActionSpec(
                Capability.UPGRADE, "upgrade", "Upgrade Portworx",
                "Patch the StorageCluster's Portworx image to a new version (the "
                "Portworx Operator performs the rolling upgrade).",
                fields=[
                    FormField("release", "StorageCluster name", FieldType.STRING,
                              default=manifests.PX_STORAGECLUSTER_DEFAULT, required=False),
                    FormField("chart_version", "Target Portworx version", FieldType.STRING,
                              required=False, placeholder="3.1.2"),
                ],
            ),
            ActionSpec(Capability.HEALTH, "health_check", "Health check",
                       "oc get pods + csidrivers for the Portworx namespace.",
                       long_running=False),
            ActionSpec(
                Capability.REMOVE, "teardown", "Remove Portworx",
                "Delete the StorageCluster, then the Portworx operator Subscription.",
                destructive=True,
                fields=[
                    FormField("release", "StorageCluster name", FieldType.STRING,
                              default=manifests.PX_STORAGECLUSTER_DEFAULT, required=False),
                ],
            ),
        ]

    # ------------------------------------------------------------------ #
    # kubeconfig / CLI helpers
    # ------------------------------------------------------------------ #
    @contextlib.asynccontextmanager
    async def _kubeconfig_file(self):
        """Yield a path to a kubeconfig for ``--kubeconfig`` CLI auth; clean up after.

        Two auth modes, selected by which target fields are set:

        * ``kubeconfig`` — a full kubeconfig is written verbatim to the temp file.
        * ``api_url`` + ``username`` + ``password`` — ``oc login`` mints a
          token-based session into a fresh temp kubeconfig. (OpenShift uses OAuth
          bearer tokens; kubeconfig HTTP basic-auth does not work against its API
          server, so we must actually log in.)

        Async because the username/password path runs ``oc login`` — callers use
        ``async with self._kubeconfig_file() as kubeconfig:``.
        """
        fd, path = tempfile.mkstemp(prefix="phif-kubeconfig-", suffix=".yaml")
        try:
            kubeconfig = self.ctx.target.get("kubeconfig")
            if kubeconfig:
                with os.fdopen(fd, "w") as fh:
                    fh.write(kubeconfig)
            else:
                # No static kubeconfig: oc login creates/populates `path`.
                os.close(fd)
                await self._oc_login(path)
            yield path
        finally:
            with contextlib.suppress(OSError):
                os.remove(path)

    async def _oc_login(self, kubeconfig_path: str) -> None:
        """Authenticate via ``oc login -u/-p``, writing a session to ``kubeconfig_path``.

        Used when the target carries no static kubeconfig. The password is passed
        on argv (oc has no password env var) but is redacted from the streamed job
        log via ``run_local(redact=...)``. Raises :class:`ConnectionValidationError`
        if neither a kubeconfig nor a complete username/password set is configured.
        Mock / dry-run: ``run_local`` is a no-op, leaving an empty temp kubeconfig
        that the (also no-op) downstream CLI calls never read.
        """
        api_url = self.ctx.target.get("api_url") or self.ctx.target.get("server") or ""
        username = self.ctx.target.get("username") or ""
        password = self.ctx.target.get("password") or ""
        if not (api_url and username and password):
            raise ConnectionValidationError(
                "OpenShift target needs either a 'kubeconfig' or "
                "'api_url' + 'username' + 'password' (for `oc login`)."
            )
        insecure = bool(self.ctx.target.get("insecure_skip_tls_verify"))
        tls = " --insecure-skip-tls-verify=true" if insecure else ""
        await self.ctx.emit(f"Logging in to {api_url} as {username!r} via oc login ...")
        from phif.jobs.runner import CommandError
        try:
            await self.ctx.runner.run_local(
                f"oc login {api_url} --username {username} --password {password}"
                f" --kubeconfig {kubeconfig_path}{tls}",
                redact=[password],
            )
        except CommandError as exc:
            detail = (getattr(exc, "output", "") or "").strip()
            hint = ""
            low = detail.lower()
            if ("x509" in low or "certificate" in low or "unknown authority" in low
                    or "tls" in low) and not insecure:
                hint = (" The API certificate is not trusted — enable 'Skip API "
                        "TLS verify' on the target if the cluster uses a "
                        "self-signed API cert.")
            elif "unauthorized" in low or "401" in low or "invalid" in low:
                hint = " Check the username / password."
            elif "no route to host" in low or "refused" in low or "timeout" in low:
                hint = " Check the API URL / network reachability from the backend."
            raise ConnectionValidationError(
                f"oc login to {api_url} as {username!r} failed: "
                f"{detail or exc}{hint}"
            ) from exc

    @contextlib.contextmanager
    def _manifest_file(self, yaml_text: str):
        """Write rendered manifest YAML to a temp file; clean up after."""
        fd, path = tempfile.mkstemp(prefix="phif-manifest-", suffix=".yaml")
        try:
            with os.fdopen(fd, "w") as fh:
                fh.write(yaml_text)
            yield path
        finally:
            with contextlib.suppress(OSError):
                os.remove(path)

    def _namespace(self) -> str:
        return self._px_namespace()

    def _protocol(self) -> str:
        return (self.ctx.target.get("protocol") or "iscsi").lower()

    def _array_endpoint(self, override: str = "") -> str:
        """Resolve the FlashArray management endpoint Portworx should connect to.

        Prefer an explicit override (advanced/programmatic use), else derive it
        from the FlashArray associated with this hypervisor (``ctx.array.endpoint``).
        Operators do not type this — it comes from the associated array, matching
        the consistent UX across connectors.
        """
        if override:
            return override
        if self.ctx.array is not None:
            return getattr(self.ctx.array, "endpoint", "") or ""
        return ""

    async def _kubectl(self, kubeconfig: str, args: str, *, check: bool = True) -> str:
        return await self.ctx.runner.run_local(
            f"kubectl --kubeconfig {kubeconfig} {args}", check=check
        )

    async def _apply(self, kubeconfig: str, yaml_text: str) -> str:
        """Apply rendered YAML by writing it to a temp file and kubectl apply -f."""
        with self._manifest_file(yaml_text) as mpath:
            return await self._kubectl(kubeconfig, f"apply -f {mpath}")

    async def _oc_apply(self, kubeconfig: str, yaml_text: str) -> str:
        """Apply rendered YAML via ``oc apply -f`` (used for OpenShift CRs like
        MachineConfig). Mock-safe: run_local is a no-op in mock/dry-run mode."""
        with self._manifest_file(yaml_text) as mpath:
            return await self.ctx.runner.run_local(
                f"oc --kubeconfig {kubeconfig} apply -f {mpath}"
            )

    # ------------------------------------------------------------------ #
    # cluster awareness
    # ------------------------------------------------------------------ #
    @classmethod
    def wizard_steps(cls) -> list[str]:
        """Ordered wizard action ids for the OpenShift connector.

        OpenShift is cluster-level: the Portworx (px-csi) driver install, the
        StorageClass, and the storage-NIC binding MachineConfig apply to the whole
        cluster (all worker nodes) in a single operation, and the CSI driver
        auto-registers the worker nodes as FlashArray hosts at volume-attach time.
        There is no per-node SSH and no per-node FlashArray host registration to
        fan out, so the wizard deliberately OMITS ``register_hosts`` /
        ``setup_connectivity`` (the base default includes them).

        ``configure_binding`` runs LAST and only does anything when the operator
        selected storage interfaces in the wizard (iSCSI NICs / NVMe-TCP sources /
        FC HBAs). It applies a MachineConfig (iscsiadm iface binding + Everpure
        multipath + the ARP-flux sysctls), which the Machine Config Operator rolls
        out by **draining and rebooting the nodes** — so it is sequenced after the
        API-only install steps, and the reboot happens after the wizard returns.
        With no interfaces selected it is a clean no-op (no MachineConfig, no
        reboot).
        """
        return ["deploy", "configure", "configure_binding"]

    async def list_nodes(self) -> list[ClusterNode]:
        """Discover the cluster's worker nodes via ``oc``/``kubectl get nodes``.

        OpenShift is already cluster-level (CSI install + MachineConfig apply to
        all nodes), so node discovery exists for visibility + cluster validation,
        not for per-node fan-out. We query the worker nodes (the role Portworx
        attaches volumes to) and return one :class:`ClusterNode` per node, carrying the
        node's role/internal-IP in ``info`` where available.

        Mock / dry-run mode (``run_local`` returns an empty string) and any query
        failure fall back to a synthetic 3-worker-node cluster so the UI / tests
        have a deterministic shape without touching a cluster. If the query
        succeeds but yields nothing parseable, we return a single logical
        ``"cluster"`` node so callers always get at least one member.
        """
        nodes: list[ClusterNode] = []
        with contextlib.suppress(Exception):
            async with self._kubeconfig_file() as kubeconfig:
                # -l node-role.kubernetes.io/worker selects worker nodes; -o json
                # gives us name + role labels + addresses in one shot.
                # TODO(doc-validate): confirm the worker-role label selector
                # (node-role.kubernetes.io/worker=) and that worker nodes are the
                # right set for storage-NIC validation on the target OCP release.
                raw = await self.ctx.runner.run_local(
                    f"oc --kubeconfig {kubeconfig} get nodes "
                    "-l node-role.kubernetes.io/worker "
                    "-o json",
                    check=False,
                )
            nodes = self._parse_nodes_json(raw)

        if nodes:
            return nodes
        if self.ctx.dry_run or not self._has_real_cluster_output():
            # Mock / dry-run / unreachable: synthesize a 3-worker cluster.
            return [
                ClusterNode(name=f"worker-{i}", host=f"worker-{i}",
                            info={"role": "worker", "synthetic": True})
                for i in range(1, 4)
            ]
        # Reachable but nothing parseable: a single logical cluster node.
        return [ClusterNode(name="cluster", host="cluster",
                            info={"role": "cluster", "logical": True})]

    def _has_real_cluster_output(self) -> bool:
        """Whether run_local actually executes (vs. a mock/dry-run no-op).

        ``JobRunner.run_local`` returns ``""`` without running anything in mock or
        dry-run mode, so an empty parse result is indistinguishable from a live
        empty cluster. We treat mock/dry-run as 'no real output' and synthesize.
        """
        return not (getattr(self.ctx.runner, "mock", False) or self.ctx.dry_run)

    @staticmethod
    def _parse_nodes_json(raw: str) -> list[ClusterNode]:
        """Parse ``oc/kubectl get nodes -o json`` output into ClusterNodes.

        Tolerant of empty / non-JSON input (returns ``[]``) so the caller can
        fall back to synthetic / logical nodes.
        """
        if not raw or not raw.strip():
            return []
        try:
            doc = json.loads(raw)
        except (ValueError, TypeError):
            return []
        items = doc.get("items") if isinstance(doc, dict) else None
        if not isinstance(items, list):
            return []
        nodes: list[ClusterNode] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            meta = item.get("metadata") or {}
            name = meta.get("name")
            if not name:
                continue
            labels = meta.get("labels") or {}
            role = "worker" if any(
                k.startswith("node-role.kubernetes.io/worker") for k in labels
            ) else "node"
            # internal IP, if present, is the most useful management address
            internal_ip = ""
            for addr in (item.get("status") or {}).get("addresses") or []:
                if isinstance(addr, dict) and addr.get("type") == "InternalIP":
                    internal_ip = addr.get("address") or ""
                    break
            host = internal_ip or name
            nodes.append(ClusterNode(name=name, host=host,
                                     info={"role": role,
                                           "internal_ip": internal_ip}))
        return nodes

    async def validate_cluster(self, **params: Any) -> OpResult:
        """Best-effort check that worker nodes share the same storage NICs.

        For iSCSI / NVMe-TCP, the interface-binding MachineConfig is applied to a
        machine-config-pool role and assumes every node in that role exposes the
        same storage NIC names. This validates that assumption by discovering each
        node's NICs and comparing them with :func:`compare_node_interfaces`, so an
        operator learns up-front if a node is missing the expected NIC.

        Per-node NIC discovery on OpenShift is not reliably available through a
        single, portable kube API call (it depends on whether the NMState operator
        is installed), so when we cannot discover per-node NICs we return ``ok``
        with an explanatory note rather than failing. Mock / dry-run returns a
        uniform synthetic set so the check passes deterministically.

        # TODO(doc-validate): confirm the exact per-node NIC source on OCP.
        #   Candidates:
        #   * NMState ``NodeNetworkState`` (``oc get nns <node> -o
        #     jsonpath='{.status.currentState.interfaces[*].name}'``) — only
        #     present when the NMState/Kubernetes-NMState operator is installed;
        #   * a debug DaemonSet reading node sysfs (``/sys/class/net``).
        #   Until pinned, this validation is advisory: it never blocks deploy.
        """
        nodes = await self.list_nodes()
        per_node = await self._discover_per_node_nics(nodes)
        if not per_node:
            return OpResult.ok(
                f"{len(nodes)} worker node(s); per-node storage NIC discovery not "
                "available on this cluster (NMState NodeNetworkState absent or "
                "unreadable) — skipping cluster NIC uniformity check. "
                "Validate manually that all worker nodes expose the same storage "
                "NICs before binding iSCSI/NVMe ifaces.",
                nodes=[n.to_dict() for n in nodes],
                checked=False,
            )
        consistent, detail = compare_node_interfaces(per_node)
        msg = (
            f"Worker storage NICs consistent across {len(per_node)} node(s): {detail}"
            if consistent else
            f"Worker storage NICs DIFFER across nodes: {detail}"
        )
        return (OpResult.ok if consistent else OpResult.fail)(
            msg,
            nodes=[n.to_dict() for n in nodes],
            per_node_nics={n: sorted(v) for n, v in per_node.items()},
            checked=True,
        )

    async def _discover_per_node_nics(
        self, nodes: list[ClusterNode]
    ) -> dict[str, list[str]]:
        """Discover each node's storage NIC names, or ``{}`` if not feasible.

        Prefers NMState ``NodeNetworkState`` (``oc get nns``) when its CRD is
        installed; otherwise falls back to ``oc debug node`` reading
        ``/sys/class/net``. In mock / dry-run the live queries are no-ops (return
        ``""``), so we synthesize a uniform NIC set across the discovered nodes (the
        common, consistent case). On a real cluster an empty / error result yields
        ``{}`` so the caller reports 'not feasible' instead of guessing — and an
        `oc` error line is never mis-parsed into bogus NIC names.
        """
        if not self._has_real_cluster_output():
            # Mock / dry-run: uniform synthetic NICs so the uniformity check passes.
            return {n.name: ["ens192", "ens224"] for n in nodes}
        per_node: dict[str, list[str]] = {}
        with contextlib.suppress(Exception):
            async with self._kubeconfig_file() as kubeconfig:
                # Preferred source: NMState NodeNetworkState (`nns`). If the
                # operator/CRD isn't installed, `oc get nns` errors with text like
                # "error: the server doesn't have a resource type nns" — which must
                # NOT be parsed as NIC names. Gate on the CRD existing first.
                if await self._nmstate_available(kubeconfig):
                    for node in nodes:
                        raw = await self.ctx.runner.run_local(
                            f"oc --kubeconfig {kubeconfig} get nns {node.name} "
                            "-o jsonpath='{.status.currentState.interfaces[*].name}'",
                            check=False,
                        )
                        if not raw or _looks_like_oc_error(raw):
                            continue
                        nics = [n for n in raw.replace("'", "").split()
                                if _is_storage_nic_candidate(n)]
                        if nics:
                            per_node[node.name] = nics
                    if per_node:
                        return per_node
                # Fallback (no NMState): read each node's /sys/class/net via a
                # short-lived `oc debug node/<name>` pod. Works on a vanilla cluster
                # with no extra operator installed.
                await self.ctx.emit(
                    "[discover] NMState NodeNetworkState unavailable; reading node "
                    "interfaces from /sys/class/net via `oc debug node` ..."
                )
                for node in nodes:
                    nics = await self._node_nics_via_debug(kubeconfig, node.name)
                    if nics:
                        per_node[node.name] = nics
        return per_node

    async def _node_nics_via_debug(self, kubeconfig: str, node: str) -> list[str]:
        """Best-effort: list a node's storage NICs from sysfs via ``oc debug node``.

        ``oc debug node/<name> -q -- chroot /host ls -1 /sys/class/net`` schedules a
        short-lived privileged pod on the node and lists its network interfaces.
        ``-q`` suppresses the "Starting/Removing pod" preamble; we still filter the
        output through :func:`_is_storage_nic_candidate` (valid iface name, carries
        a digit, not a loopback/overlay device) so any stray lines are dropped.
        Returns ``[]`` on any failure (debug blocked, node not schedulable, etc.).
        """
        raw = await self.ctx.runner.run_local(
            f"oc --kubeconfig {kubeconfig} debug node/{node} -q "
            "-- chroot /host ls -1 /sys/class/net",
            check=False,
        )
        if not raw or _looks_like_oc_error(raw):
            return []
        return [n for n in raw.split() if _is_storage_nic_candidate(n)]

    async def _nmstate_available(self, kubeconfig: str) -> bool:
        """Whether the Kubernetes-NMState ``nodenetworkstates`` CRD is installed.

        Per-node NIC discovery depends on NMState's ``NodeNetworkState`` (``nns``).
        We probe the CRD once (cheap) so we can cleanly report 'not feasible'
        instead of querying ``nns`` on every node and mis-parsing the resulting
        "no resource type" error as NIC names.
        """
        raw = await self.ctx.runner.run_local(
            f"oc --kubeconfig {kubeconfig} get crd "
            "nodenetworkstates.nmstate.io -o name",
            check=False,
        )
        return "nodenetworkstates" in (raw or "") and not _looks_like_oc_error(raw)

    # ------------------------------------------------------------------ #
    # operations
    # ------------------------------------------------------------------ #
    async def validate_connection(self) -> OpResult:
        await self.ctx.emit("Validating OpenShift / Kubernetes connection ...")
        # Whether we authenticated via username/password (oc login) vs a static
        # kubeconfig — only the former is a candidate for SA-token promotion.
        used_login = not self.ctx.target.get("kubeconfig")
        sa_kubeconfig: str | None = None
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.runner.run_local(
                f"kubectl --kubeconfig {kubeconfig} version --output=json", check=False
            )
            await self.ctx.runner.run_local(
                f"oc --kubeconfig {kubeconfig} whoami", check=False
            )
            # If we logged in with a user/password, try to promote that session
            # into a durable ServiceAccount token (best-effort; needs the user to
            # have permission to create the SA + cluster-role binding).
            if used_login and not self.ctx.dry_run:
                sa_kubeconfig = await self._mint_sa_kubeconfig(kubeconfig)
        info: dict[str, Any] = {}
        if self.ctx.array is not None:
            info = await self.ctx.array.info()
            await self.ctx.emit(f"FlashArray reachable: {info}")
        data: dict[str, Any] = {"driver": "portworx",
                                "namespace": self._namespace(), "array": info}
        if sa_kubeconfig:
            # The validate endpoint persists this as the target's kubeconfig so
            # subsequent day-2 ops use the durable token instead of re-logging in.
            data["service_account_kubeconfig"] = sa_kubeconfig
            await self.ctx.emit(
                f"Provisioned ServiceAccount {manifests.SA_NAMESPACE}/"
                f"{manifests.SA_NAME}; future operations will use its durable "
                "token (the password login is no longer required)."
            )
        return OpResult.ok("Connected to cluster", **data)

    async def _mint_sa_kubeconfig(self, kubeconfig: str) -> str | None:
        """Promote the logged-in user session into a durable ServiceAccount token.

        Using the just-established session (``kubeconfig``), create the
        ``phif-operator`` ServiceAccount + a cluster-admin ClusterRoleBinding and
        mint a token for it, then build a token-based kubeconfig. Returns that
        kubeconfig string, or ``None`` if the user lacks permission (or no
        ``api_url`` is known) — the caller then keeps using the login session.

        Best-effort and idempotent: SA / binding creation tolerate "already
        exists". The minted token is read with ``log_output=False`` so it never
        lands in the job log.
        """
        api_url = self.ctx.target.get("api_url") or self.ctx.target.get("server") or ""
        if not api_url:
            await self.ctx.emit(
                "[sa] No api_url configured; cannot build a ServiceAccount "
                "kubeconfig — keeping the login session."
            )
            return None
        sa, ns = manifests.SA_NAME, manifests.SA_NAMESPACE
        kc = f"--kubeconfig {kubeconfig}"
        await self.ctx.emit(
            f"[sa] Ensuring ServiceAccount {ns}/{sa} (+ scoped {manifests.SA_CLUSTERROLE} "
            "ClusterRole) to mint a durable token ..."
        )
        # Idempotent create (tolerate AlreadyExists via check=False).
        await self.ctx.runner.run_local(
            f"oc {kc} -n {ns} create serviceaccount {sa}", check=False
        )
        # Apply the least-privilege ClusterRole, then bind the SA to it (NOT
        # cluster-admin). apply is idempotent; the binding tolerates AlreadyExists.
        with self._manifest_file(manifests.service_account_clusterrole_yaml()) as crpath:
            await self.ctx.runner.run_local(f"oc {kc} apply -f {crpath}", check=False)
        await self.ctx.runner.run_local(
            f"oc {kc} create clusterrolebinding {manifests.SA_CLUSTERROLEBINDING} "
            f"--clusterrole={manifests.SA_CLUSTERROLE} --serviceaccount={ns}:{sa}",
            check=False,
        )
        # Mint a token (OCP 4.11+). Output is the token itself -> never log it.
        token = (await self.ctx.runner.run_local(
            f"oc {kc} -n {ns} create token {sa} --duration=87600h",
            check=False, log_output=False,
        ) or "").strip()
        if not token or " " in token or "\n" in token.strip():
            # Fallback for clusters without `oc create token`: apply a long-lived
            # token Secret and read the token the controller writes into it.
            await self.ctx.emit(
                "[sa] `oc create token` unavailable/blocked; falling back to a "
                "long-lived token Secret."
            )
            token = await self._mint_sa_token_via_secret(kubeconfig, ns)
        if not token:
            await self.ctx.emit(
                "[sa] Could not mint a ServiceAccount token (the logged-in user "
                "likely lacks permission to create the SA / binding). Keeping the "
                "username/password login session."
            )
            return None
        ca_data = await self._cluster_ca_data(kubeconfig)
        insecure = bool(self.ctx.target.get("insecure_skip_tls_verify"))
        return manifests.service_account_kubeconfig_yaml(
            api_url=api_url, token=token, ca_data=ca_data, insecure=insecure,
        )

    async def _mint_sa_token_via_secret(self, kubeconfig: str, ns: str) -> str:
        """Apply a long-lived token Secret for the SA and return its decoded token."""
        kc = f"--kubeconfig {kubeconfig}"
        secret_yaml = manifests.service_account_token_secret_yaml(namespace=ns)
        with self._manifest_file(secret_yaml) as spath:
            await self.ctx.runner.run_local(f"oc {kc} apply -f {spath}", check=False)
        # The controller populates .data.token (base64) asynchronously; read it.
        b64 = (await self.ctx.runner.run_local(
            f"oc {kc} -n {ns} get secret {manifests.SA_NAME}-token "
            "-o jsonpath='{.data.token}'",
            check=False, log_output=False,
        ) or "").strip().strip("'")
        if not b64:
            return ""
        import base64
        try:
            return base64.b64decode(b64).decode("utf-8", errors="replace").strip()
        except (ValueError, UnicodeError):
            return ""

    async def _cluster_ca_data(self, kubeconfig: str) -> str:
        """Read the cluster CA bundle (base64) from the session kubeconfig, if any.

        CA data is not secret, but we keep it off the log for tidiness. Returns ""
        when unavailable (then the SA kubeconfig falls back to insecure / the
        operator-set skip-TLS flag).
        """
        out = (await self.ctx.runner.run_local(
            f"oc --kubeconfig {kubeconfig} config view --raw --minify "
            "-o jsonpath='{.clusters[0].cluster.certificate-authority-data}'",
            check=False, log_output=False,
        ) or "").strip().strip("'")
        return out

    async def deploy_integration(self, release: str = manifests.PX_STORAGECLUSTER_DEFAULT,
                                 cluster_id: str = "", chart_version: str = "",
                                 array_endpoint: str = "", api_token: str = "",
                                 fa_direct_access: bool = True,
                                 px_spec_url: str = "", px_spec_yaml: str = "",
                                 px_operator_url: str = "",
                                 **_: Any) -> OpResult:
        """Install Portworx (px-csi) — the only supported OpenShift CSI driver.

        The legacy Service Orchestrator (`pure-csi`) Helm chart has been retired and
        no longer functions, so deploy always drives the Portworx path: the
        Portworx Operator (OLM) + a StorageCluster. The StorageCluster either comes
        from a spec the customer generated in Portworx Central (``px_spec_url`` /
        ``px_spec_yaml``) or, when none is supplied, from PHIF's generated FADA
        spec.
        """
        cluster_id = cluster_id or self.ctx.target.id
        return await self._deploy_portworx(
            release=release, cluster_id=cluster_id, version=chart_version,
            array_endpoint=array_endpoint, api_token=api_token,
            direct_access=bool(fa_direct_access),
            spec_url=px_spec_url or "", spec_yaml=px_spec_yaml or "",
            operator_url=px_operator_url or "",
        )

    def _px_namespace(self) -> str:
        """Namespace Portworx installs into.

        Falls back to the Portworx default ("portworx") when the target still
        carries the legacy "pure-csi" default (left over from the retired PSO
        driver), so a Portworx deploy lands in a sensibly-named namespace without
        the operator having to retype it.
        """
        ns = self.ctx.target.get("namespace")
        return ns if ns and ns != "pure-csi" else manifests.PX_NAMESPACE_DEFAULT

    async def _portworx_present(self, kubeconfig: str) -> tuple[bool, str]:
        """Detect an existing Portworx install so deploy can adopt it.

        Returns ``(present, "<ns>/<name>")`` where present is True if the
        ``pxd.portworx.com`` CSIDriver is registered (the operator + driver are
        already there). The second element is the first existing StorageCluster
        found (``""`` if none). We check BEFORE creating ours, so any StorageCluster
        seen is pre-existing — installing a second one on the same nodes only
        produces a Degraded cluster, so we adopt instead.
        """
        drv = await self.ctx.runner.run_local(
            f"oc --kubeconfig {kubeconfig} get csidriver pxd.portworx.com -o name",
            check=False,
        )
        # IMPORTANT: a NotFound error echoes the resource name back —
        # `Error from server (NotFound): csidrivers.storage.k8s.io
        # "pxd.portworx.com" not found` — which CONTAINS "pxd.portworx.com". A
        # naive substring check would then mis-read "not installed" as "present"
        # and skip the whole operator + StorageCluster install. Reject error
        # output first, then require the `-o name` success form
        # (csidriver.storage.k8s.io/pxd.portworx.com).
        if (not drv or _looks_like_oc_error(drv)
                or "csidriver" not in drv.lower()
                or "pxd.portworx.com" not in drv):
            return False, ""
        raw = await self.ctx.runner.run_local(
            f"oc --kubeconfig {kubeconfig} get storagecluster -A -o json", check=False,
        )
        try:
            items = (json.loads(raw).get("items", [])
                     if raw and not _looks_like_oc_error(raw) else [])
        except (ValueError, TypeError):
            items = []
        for it in items:
            meta = it.get("metadata") or {}
            return True, f"{meta.get('namespace','')}/{meta.get('name','')}"
        return True, ""

    async def _existing_px_storageclasses(self, kubeconfig: str) -> tuple[list[str], str]:
        """Return ``(names, default)`` of existing pxd.portworx.com StorageClasses.

        Lets ``configure`` reuse the cluster's tuned Portworx StorageClasses (e.g.
        the KubeVirt RWX-block classes) instead of stamping a generic ``pure-block``
        that competes with them. ``default`` is the SC marked default, if any.
        """
        raw = await self.ctx.runner.run_local(
            f"oc --kubeconfig {kubeconfig} get sc -o json", check=False,
        )
        try:
            items = json.loads(raw).get("items", []) if raw else []
        except (ValueError, TypeError):
            items = []
        names: list[str] = []
        default = ""
        for it in items:
            if it.get("provisioner") != "pxd.portworx.com":
                continue
            meta = it.get("metadata") or {}
            name = meta.get("name", "")
            if not name:
                continue
            names.append(name)
            ann = meta.get("annotations") or {}
            if ann.get("storageclass.kubernetes.io/is-default-class") == "true":
                default = name
        return names, default

    async def _wait_for_crd(self, kubeconfig: str, crd: str, *,
                            timeout: int = 300, interval: int = 5) -> bool:
        """Poll until ``crd`` exists and is Established, or ``timeout`` elapses.

        ``oc wait`` errors immediately on a resource that doesn't exist yet, so it
        can't be used to wait for an OLM-installed CRD to *appear*. We poll
        ``oc get crd`` until it shows up, then ``oc wait --for=condition=established``.
        Returns True once established, False on timeout. Mock / dry-run short-circuit
        to True (nothing real to wait for) so tests don't sleep.
        """
        if getattr(self.ctx.runner, "mock", False) or self.ctx.dry_run:
            return True
        attempts = max(1, timeout // max(1, interval))
        for _ in range(attempts):
            out = await self.ctx.runner.run_local(
                f"oc --kubeconfig {kubeconfig} get crd {crd} -o name", check=False)
            if crd in (out or "") and not _looks_like_oc_error(out):
                # CRD registered — wait for it to be Established before using it.
                await self.ctx.runner.run_local(
                    f"oc --kubeconfig {kubeconfig} wait --for=condition=established "
                    f"crd/{crd} --timeout=60s", check=False)
                return True
            await asyncio.sleep(interval)
        return False

    @staticmethod
    def _derive_pxoperator_url(spec_url: str) -> str:
        """Derive the Portworx Operator manifest URL from a Central spec URL.

        Portworx Central hosts the operator install at the SAME base + version as
        the StorageCluster spec, selected by ``comp=pxoperator``. We carry over
        ``kbver`` (Kubernetes version) and ``ns`` (namespace) and set ``osft=true``
        (OpenShift), dropping the StorageCluster-only params (operator/csi/ce/oem/
        c/...). Applying this manifest installs the px-operator (Deployment + RBAC)
        WITHOUT needing the Portworx Operator to be present in the cluster's
        OperatorHub — the operator then registers the StorageCluster CRD at
        runtime. Returns "" if ``spec_url`` isn't an install.portworx.com URL.
        """
        from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
        if not spec_url:
            return ""
        try:
            parts = urlsplit(spec_url)
            if "install.portworx.com" not in (parts.netloc or ""):
                return ""
            q = parse_qs(parts.query)
            newq = {"comp": "pxoperator", "osft": "true"}
            for k in ("kbver", "ns"):
                if q.get(k):
                    newq[k] = q[k][0]
            return urlunsplit(
                (parts.scheme, parts.netloc, parts.path, urlencode(newq), ""))
        except Exception:  # noqa: BLE001 — derivation is best-effort
            return ""

    async def _deploy_portworx(self, *, release: str, cluster_id: str,
                               version: str = "", array_endpoint: str = "",
                               api_token: str = "",
                               direct_access: bool = True,
                               spec_url: str = "", spec_yaml: str = "",
                               operator_url: str = "") -> OpResult:
        """Install Portworx (px-csi) via the Portworx Operator (OLM) + StorageCluster.

        Steps: namespace -> OLM OperatorGroup + Subscription (portworx-certified)
        -> wait for the StorageCluster CRD -> px-pure-secret (FlashArray endpoint +
        token, derived from the associated array) -> StorageCluster CR. The
        StorageClass/SnapshotClass are applied separately by ``configure`` (the
        pxd.portworx.com provisioner). Honours ``ctx.dry_run``.

        **StorageCluster source** — the operator install + px-pure-secret are always
        automated, but the StorageCluster itself comes from one of:

        * ``spec_yaml`` — a StorageCluster spec the customer downloaded from
          Portworx Central (``central.portworx.com`` → Generate Spec), applied
          verbatim. Wins over ``spec_url`` if both are given.
        * ``spec_url`` — the Central-hosted spec URL, applied with ``oc apply -f
          <url>`` (exactly the command Central's UI hands you).
        * neither — PHIF's generated FADA / cloud-drive StorageCluster (below).

        The Central spec is the recommended path: it carries the customer's license
        and any tuning chosen in the console (which PHIF can't synthesize). PHIF
        still does everything around it — operator, secret, namespace, CRD wait.

        For the generated fallback, ``direct_access=True`` (default) deploys
        FlashArray Direct Access (FADA): the StorageCluster carries no pooled cloud
        drives, so PVCs map 1:1 to FlashArray volumes — the lightweight "just CSI"
        mode, no PX-Enterprise SDS pooling. ``direct_access=False`` deploys
        Enterprise cloud-drive pooling.
        """
        ns = self._px_namespace()
        release = release or manifests.PX_STORAGECLUSTER_DEFAULT
        version = version or manifests.PX_DEFAULT_VERSION
        spec_yaml = (spec_yaml or "").strip()
        spec_url = (spec_url or "").strip()
        if spec_yaml:
            spec_source = "Portworx Central spec (pasted YAML)"
        elif spec_url:
            spec_source = f"Portworx Central spec URL ({spec_url})"
        else:
            spec_source = ("PHIF-generated " + (
                "FlashArray Direct Access (no pooling)" if direct_access
                else "Enterprise cloud-drive pooling"))
        if self.ctx.dry_run:
            await self.ctx.emit(
                f"[dry-run] Portworx: would adopt an existing Portworx install if "
                "present, else apply Namespace + OperatorGroup + Subscription + "
                f"px-pure-secret + StorageCluster from {spec_source}. No changes made."
            )
            return OpResult.ok(
                "Dry-run: Portworx deploy planned",
                artifacts={"driver": "portworx", "namespace": ns,
                           "storagecluster": release, "version": version,
                           "direct_access": direct_access,
                           "spec_source": spec_source},
            )
        async with self._kubeconfig_file() as kubeconfig:
            # Adopt an existing Portworx rather than stamping a second, conflicting
            # StorageCluster (two Portworx clusters can't share the same nodes —
            # the duplicate just goes Degraded).
            present, existing_sc = await self._portworx_present(kubeconfig)
            if present:
                await self.ctx.emit(
                    "Existing Portworx detected (pxd.portworx.com CSIDriver"
                    + (f" + StorageCluster {existing_sc}" if existing_sc else "")
                    + "). Adopting it — skipping operator + StorageCluster install. "
                    "Run 'configure' to (re)use its StorageClasses."
                )
                return OpResult.ok(
                    "Adopted existing Portworx install (no changes made)",
                    artifacts={"driver": "portworx", "adopted": True,
                               "storagecluster": existing_sc},
                )
            # Fresh install: now we need the FlashArray to build px-pure-secret.
            if self.ctx.array is None:
                return OpResult.fail(
                    "No FlashArray associated with this hypervisor; cannot build "
                    "the px-pure-secret for Portworx. Associate a FlashArray and retry."
                )
            api_token = self.ctx.resolve_token(api_token) or ""
            array_endpoint = self._array_endpoint(array_endpoint)
            if not api_token:
                await self.ctx.emit("No token available; minting a FlashArray API token for Portworx ...")
                api_token = await self.ctx.array.create_api_token("portworx")
            # FC: optionally pre-register the worker-node HBAs on the array (pre-
            # zoning). Skipped cleanly when node_wwns is unset — Portworx
            # auto-registers the nodes at attach time either way.
            if self._protocol() == "fc":
                await self._register_fc_hosts(cluster_id)
            await self.ctx.emit(
                f"Portworx: operator={manifests.PX_OPERATOR_PACKAGE} (channel "
                f"{manifests.PX_OPERATOR_CHANNEL}) -> StorageCluster from "
                f"{spec_source} in {ns!r}; FlashArray backing {array_endpoint!r}."
            )
            await self._oc_apply(kubeconfig, manifests.portworx_namespace_yaml(namespace=ns))
            # Install the Portworx Operator. Preferred: the Portworx-hosted operator
            # MANIFEST (comp=pxoperator) — supplied explicitly or derived from the
            # Central spec URL — which works even when the Portworx Operator is NOT
            # in the cluster's OperatorHub (the common case). Fallback: the OLM
            # OperatorGroup + Subscription (only works on clusters whose OperatorHub
            # actually carries the portworx-certified package).
            op_url = (operator_url or "").strip() or self._derive_pxoperator_url(spec_url)
            if op_url:
                await self.ctx.emit(
                    f"Installing the Portworx Operator from manifest: "
                    f"oc apply -f {op_url}")
                await self.ctx.runner.run_local(
                    f"oc --kubeconfig {kubeconfig} apply -f {shlex.quote(op_url)}")
            else:
                await self.ctx.emit(
                    "Installing the Portworx Operator via OLM (OperatorGroup + "
                    "Subscription). NOTE: this requires the Portworx Operator to be "
                    "present in the cluster's OperatorHub.")
                await self._oc_apply(kubeconfig, manifests.portworx_operatorgroup_yaml(namespace=ns))
                await self._oc_apply(kubeconfig, manifests.portworx_subscription_yaml(namespace=ns))
            # Wait for the operator to register the StorageCluster CRD before we
            # apply the CR. The operator install is asynchronous (the manifest's
            # px-operator pod registers the CRD once running; OLM goes Subscription
            # -> InstallPlan -> CSV -> CRD). `oc wait` ERRORS immediately
            # ("NotFound") on a resource that doesn't exist rather than waiting for
            # it to appear, so we POLL until the CRD shows up, then wait for it to
            # be Established, before applying the StorageCluster.
            await self.ctx.emit(
                "Waiting for the Portworx Operator to register the StorageCluster "
                "CRD (up to 5 min) ...")
            if not await self._wait_for_crd(
                kubeconfig, "storageclusters.core.libopenstorage.org"
            ):
                hint = (
                    f"the px-operator pod in {ns!r} (`oc -n {ns} get pods`)"
                    if op_url else
                    f"the OLM install (`oc -n {ns} get subscription,installplan,csv` "
                    "— the Portworx Operator may not be in this cluster's "
                    "OperatorHub; supply the Portworx Central operator manifest URL "
                    "instead)")
                return OpResult.fail(
                    "The Portworx Operator did not register the StorageCluster CRD "
                    "(storageclusters.core.libopenstorage.org) within 5 minutes. "
                    f"Check {hint}."
                )
            # FlashArray creds (token stays in the secret's stringData, not argv).
            await self._oc_apply(
                kubeconfig,
                manifests.portworx_pure_secret_yaml(
                    array_endpoint=array_endpoint, api_token=api_token, namespace=ns),
            )
            # StorageCluster: the customer's Central spec wins (it carries their
            # license + console tuning); otherwise apply PHIF's generated spec.
            if spec_yaml:
                await self.ctx.emit(
                    "Applying the StorageCluster from the pasted Portworx Central "
                    "spec (verbatim) ...")
                await self._oc_apply(kubeconfig, spec_yaml)
            elif spec_url:
                await self.ctx.emit(
                    f"Applying the StorageCluster from the Portworx Central spec "
                    f"URL: oc apply -f {spec_url}")
                await self.ctx.runner.run_local(
                    f"oc --kubeconfig {kubeconfig} apply -f {shlex.quote(spec_url)}")
            else:
                await self.ctx.emit(
                    f"Applying PHIF-generated StorageCluster {release} ...")
                await self._oc_apply(
                    kubeconfig,
                    manifests.portworx_storagecluster_yaml(
                        name=release, namespace=ns, version=version,
                        cluster_id=cluster_id, direct_access=direct_access),
                )
                if direct_access:
                    await self.ctx.emit(
                        "[fada] Direct Access: PVCs map 1:1 to FlashArray volumes. "
                        "Use a StorageClass with backend=pure_block (run the "
                        "'configure' action) — no pooled Portworx storage is created."
                    )
        if spec_yaml or spec_url:
            summary = f"Deployed Portworx in {ns!r} from {spec_source}"
        else:
            summary = (
                f"Deployed Portworx StorageCluster {release!r} in {ns!r} "
                + ("(FlashArray Direct Access — 1 PVC = 1 FA volume)" if direct_access
                   else "(Enterprise cloud-drive pooling)"))
        return OpResult.ok(
            summary,
            artifacts={"driver": "portworx", "namespace": ns,
                       "storagecluster": release, "version": version,
                       "direct_access": direct_access, "spec_source": spec_source},
        )

    async def _register_fc_hosts(self, cluster_id: str) -> None:
        """Optionally pre-register OpenShift worker-node FC HBAs as FlashArray hosts.

        Portworx (px-csi) auto-registers the worker nodes as FlashArray hosts
        (creating host objects by initiator) at volume-attach time, so this manual
        registration is an OPTIONAL pre-stage — useful when the fabric is pre-zoned
        by WWN ahead of the first attach. WWNs come from the optional ``node_wwns``
        target field; when none are supplied we skip cleanly and let Portworx
        register the nodes itself. Registration goes through the idempotent
        ``ctx.array.create_host_group`` / ``create_host`` so re-running a deploy is
        always safe.
        """
        raw = self.ctx.target.get("node_wwns", "") or ""
        wwns = [w.strip() for w in raw.split(",") if w.strip()]
        if not wwns:
            await self.ctx.emit(
                "[fc] No node_wwns supplied; skipping manual FlashArray host "
                "registration. Portworx will auto-register the worker nodes as "
                "hosts at volume-attach time."
            )
            return
        if self.ctx.array is None:
            await self.ctx.emit(
                "[fc] node_wwns supplied but no FlashArray associated; skipping "
                "pre-registration. Portworx will auto-register the worker nodes "
                "at attach time."
            )
            return
        group = f"{cluster_id}-ocp"
        await self.ctx.emit(
            f"[fc] Pre-registering {len(wwns)} worker WWN(s) on FlashArray host "
            f"group {group} (optional pre-zoning; Portworx auto-registers too)."
        )
        if self.ctx.dry_run:
            await self.ctx.emit("[dry-run] would create FC host group + host by WWN.")
            return
        # Reuse a pre-existing FA host that already owns these WWNs, and adopt an
        # existing host group if the worker is already in one (shared
        # apply_host_group). Best-effort pre-zoning step — log a conflict but don't
        # abort the deploy (Portworx auto-registers nodes at attach time).
        res = await self.apply_host_group(group, [{"name": f"{group}-h1", "wwns": wwns}])
        if res.get("conflict"):
            await self.ctx.emit(
                f"[fc] Skipping optional WWN pre-registration: {res['conflict']}")

    async def configure(self, storage_class: str = "pure-block",
                         snapshot_class: str = "pure-snapshotclass",
                         backend: str = "block", fs_type: str = "xfs",
                         is_default: bool = False, force: bool = False,
                         **_: Any) -> OpResult:
        driver = "portworx"
        # Portworx FlashArray Direct Access uses pure_block / pure_file as the
        # StorageClass backend; map the operator's block/file choice onto the FADA
        # value so PVCs go directly to the array.
        backend = {"block": "pure_block", "file": "pure_file"}.get(backend, backend)
        if self.ctx.dry_run:
            await self.ctx.emit("[dry-run] would apply StorageClass + VolumeSnapshotClass.")
            return OpResult.ok("Dry-run: configure planned",
                               artifacts={"storage_class": storage_class,
                                          "snapshot_class": snapshot_class})
        sc_yaml = manifests.storage_class_yaml(
            name=storage_class, driver=driver, fs_type=fs_type, backend=backend,
            is_default=is_default,
        )
        vsc_yaml = manifests.volume_snapshot_class_yaml(
            name=snapshot_class, driver=driver,
        )
        async with self._kubeconfig_file() as kubeconfig:
            # Portworx: if the cluster already has pxd.portworx.com StorageClasses
            # (e.g. tuned KubeVirt RWX-block classes), reuse them instead of
            # stamping a generic 'pure-block' that competes with them. force=true
            # creates ours anyway.
            if not force:
                existing, default = await self._existing_px_storageclasses(kubeconfig)
                if existing:
                    rec = default or existing[0]
                    await self.ctx.emit(
                        "Existing Portworx StorageClasses found: "
                        f"{', '.join(existing)}"
                        + (f" (default: {default})" if default else "")
                        + f". Reusing — recommended: {rec!r}. Skipping creation of "
                        f"{storage_class!r}; pass force=true to create it anyway."
                    )
                    return OpResult.ok(
                        "Reusing existing Portworx StorageClass(es)",
                        artifacts={"reused": True, "storage_classes": existing,
                                   "default_storage_class": default,
                                   "recommended_storage_class": rec},
                    )
            await self.ctx.emit(f"Applying StorageClass {storage_class} ...")
            await self._apply(kubeconfig, sc_yaml)
            await self.ctx.emit(f"Applying VolumeSnapshotClass {snapshot_class} ...")
            await self._apply(kubeconfig, vsc_yaml)
        return OpResult.ok(
            f"Configured StorageClass {storage_class!r} and VolumeSnapshotClass {snapshot_class!r}",
            artifacts={"storage_class": storage_class, "snapshot_class": snapshot_class},
        )

    # ------------------------------------------------------------------ #
    # Interface binding (discoverable options + MachineConfig)
    # ------------------------------------------------------------------ #
    # Synthetic options for mock / dry-run only, so the UI + tests have choices
    # without a real cluster. These are NEVER returned for a reachable cluster —
    # doing so would offer NICs/HBAs that don't exist on the nodes.
    _SYNTHETIC_OPTIONS = {
        "nics": [("ens192", "ens192 (node NIC)"),
                 ("ens224", "ens224 (node NIC)"),
                 ("bond0", "bond0 (node bond)")],
        "nvme_sources": [("ens192", "ens192 (NVMe-TCP source)"),
                         ("192.0.2.10", "192.0.2.10 (host-traddr)")],
        "fc_hbas": [("21:00:00:aa", "21:00:00:aa (host0 HBA)"),
                    ("21:00:00:bb", "21:00:00:bb (host1 HBA)")],
    }

    async def discover_options(self, kind: str) -> list[dict[str, Any]]:
        """Enumerate candidate node interfaces for the interface-binding form.

        For a **reachable** cluster, NIC options come from the actual nodes —
        preferentially via NMState NodeNetworkState (``oc get nns``), falling back
        to ``oc debug node`` reading ``/sys/class/net`` when NMState isn't
        installed. Results are filtered to real storage NICs (loopback / OVN / OVS /
        CNI overlay devices are dropped; an `oc` error line is never parsed as NIC
        names) and annotated with how many worker nodes expose each. We return ONLY
        what is really on the nodes; if neither source yields interfaces we return
        an empty list (the operator types the NIC name) rather than inventing them.

        NVMe-TCP sources and FC HBAs have no portable kube-API source, so on a real
        cluster they return empty (enter manually). Mock / dry-run returns the
        synthetic set so the UI / tests have deterministic choices.
        """
        try:
            valid = {k.value for k in DiscoveryKind}
        except Exception:  # pragma: no cover - defensive
            valid = {"nics", "nvme_sources", "fc_hbas"}
        if kind not in valid:
            return []

        # Mock / dry-run: synthetic options (no live cluster to query).
        if not self._has_real_cluster_output():
            return [{"value": v, "label": label}
                    for v, label in self._SYNTHETIC_OPTIONS.get(kind, [])]

        # Real cluster: only NICs are discoverable declaratively (via NMState).
        if kind != DiscoveryKind.NICS.value:
            await self.ctx.emit(
                f"[discover] No declarative cluster source for {kind!r}; "
                "enter the values manually."
            )
            return []

        nodes = await self.list_nodes()
        per_node = await self._discover_per_node_nics(nodes)
        if not per_node:
            await self.ctx.emit(
                "[discover] Could not enumerate node NICs — neither NMState "
                "NodeNetworkState nor `oc debug node` (sysfs) returned interfaces. "
                "Enter the storage NIC name manually, or install the "
                "Kubernetes-NMState operator for automatic discovery."
            )
            return []

        # Union of real NICs across the worker nodes, annotated with coverage so
        # the operator can see which NICs exist on every node (the safe ones to
        # bind cluster-wide) vs only some.
        total = len(per_node)
        counts: dict[str, int] = {}
        for nics in per_node.values():
            for nic in set(nics):
                counts[nic] = counts.get(nic, 0) + 1
        options: list[dict[str, Any]] = []
        for nic in sorted(counts):
            cov = counts[nic]
            label = (f"{nic} (all {total} nodes)" if cov == total
                     else f"{nic} (on {cov}/{total} nodes)")
            options.append({"value": nic, "label": label})
        return options

    async def configure_binding(self, iscsi_nics: list[str] | None = None,
                                nvme_sources: list[str] | None = None,
                                nvme_options: str = "",
                                fc_hbas: list[str] | None = None,
                                machine_config_role: str = "worker",
                                **_: Any) -> OpResult:
        """Bind storage-path interfaces on the nodes via a MachineConfig.

        Renders a ``machineconfiguration.openshift.io/v1`` MachineConfig whose
        Ignition writes iscsiadm iface files (per NIC), an NVMe-TCP host config +
        connect unit, and records the FC HBA selection (zoning is switch-side).
        It is applied with ``oc apply`` so the Machine Config Operator rolls it
        out to the role's nodes — operators never SSH the nodes. Honours
        ``ctx.dry_run`` (renders + prints only).
        """
        iscsi_nics = list(iscsi_nics or [])
        nvme_sources = list(nvme_sources or [])
        fc_hbas = list(fc_hbas or [])
        role = (machine_config_role or "worker").lower()

        # Nothing selected -> nothing to bind. Skip cleanly WITHOUT applying a
        # MachineConfig: an empty MachineConfig still changes the pool's rendered
        # config and would trigger a needless rolling reboot of the nodes. This is
        # the common wizard path when the operator doesn't pick storage NICs.
        if not (iscsi_nics or nvme_sources or fc_hbas):
            await self.ctx.emit(
                "[binding] No storage interfaces selected — skipping the "
                "interface-binding MachineConfig (no node reboot)."
            )
            return OpResult.ok(
                "No interface binding requested (skipped)",
                artifacts={"machine_config": None, "skipped": True, "role": role},
            )

        # dm-multipath covers the SCSI transports (iSCSI + FC); NVMe-oF uses
        # native NVMe multipath, so multipathd / multipath.conf are skipped there.
        want_multipath = bool(iscsi_nics or fc_hbas)

        yaml_text = manifests.interface_binding_machineconfig_yaml(
            role=role, iscsi_nics=iscsi_nics, nvme_sources=nvme_sources,
            nvme_options=nvme_options, fc_hbas=fc_hbas,
        )
        artifacts = {
            "machine_config": "phif-interface-binding", "role": role,
            "iscsi_nics": iscsi_nics, "nvme_sources": nvme_sources,
            "fc_hbas": fc_hbas, "multipath": want_multipath,
        }
        await self.ctx.emit(
            f"Interface binding: role={role} iscsi_nics={iscsi_nics} "
            f"nvme_sources={nvme_sources} fc_hbas={fc_hbas}"
        )
        if want_multipath:
            await self.ctx.emit(
                "[multipath] Writing Everpure FlashArray /etc/multipath.conf and "
                "enabling multipathd.service on the nodes (the Machine Config "
                "Operator restarts multipathd as it rolls the change out)."
            )
        if iscsi_nics:
            await self.ctx.emit(
                f"[iscsi] Binding iSCSI ifaces to {iscsi_nics} and enabling "
                "iscsid.service (one /etc/iscsi/ifaces/phif_<nic> per NIC)."
            )
            await self.ctx.emit(
                f"[arp] Writing {iscsi_net.ARP_SYSCTL_FILE} with arp_ignore=2 / "
                f"arp_announce=2 for {iscsi_nics} (multi-NIC iSCSI ARP-flux "
                "prevention; the Machine Config Operator applies it on the node)."
            )
        if fc_hbas:
            await self.ctx.emit(
                "[fc] HBA selection recorded on the MachineConfig; FC pathing is "
                "zoning-driven (switch-side) — no node file is written."
            )
        if self.ctx.dry_run:
            await self.ctx.emit(
                "[dry-run] would oc apply the interface-binding MachineConfig; "
                "no changes made. Rendered MachineConfig:\n" + yaml_text
            )
            return OpResult.ok("Dry-run: interface binding planned",
                               artifacts=artifacts)
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.emit(
                f"Applying interface-binding MachineConfig to role {role} ..."
            )
            await self._oc_apply(kubeconfig, yaml_text)
        return OpResult.ok(
            f"Applied interface-binding MachineConfig to {role!r} nodes "
            "(Machine Config Operator will roll it out)",
            artifacts=artifacts,
        )

    async def provision(self, name: str, size: str = "10Gi",
                        storage_class: str = "pure-block",
                        access_mode: str = "ReadWriteOnce", **_: Any) -> OpResult:
        namespace = self._namespace()
        if self.ctx.dry_run:
            await self.ctx.emit(f"[dry-run] would create PVC {name} ({size}).")
            return OpResult.ok("Dry-run: provision planned", artifacts={"pvc": name})
        yaml_text = manifests.pvc_yaml(
            name=name, namespace=namespace, size=size, storage_class=storage_class,
            access_mode=access_mode,
        )
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.emit(f"Creating PVC {name} ({size}) in {namespace} ...")
            await self._apply(kubeconfig, yaml_text)
        return OpResult.ok(f"Provisioned PVC {name!r}",
                           artifacts={"pvc": name, "namespace": namespace})

    async def snapshot(self, name: str, source_pvc: str,
                       snapshot_class: str = "pure-snapshotclass", **_: Any) -> OpResult:
        namespace = self._namespace()
        if self.ctx.dry_run:
            await self.ctx.emit(f"[dry-run] would snapshot PVC {source_pvc} as {name}.")
            return OpResult.ok("Dry-run: snapshot planned", artifacts={"snapshot": name})
        yaml_text = manifests.volume_snapshot_yaml(
            name=name, namespace=namespace, source_pvc=source_pvc,
            snapshot_class=snapshot_class,
        )
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.emit(f"Creating VolumeSnapshot {name} of {source_pvc} ...")
            await self._apply(kubeconfig, yaml_text)
        return OpResult.ok(f"Created VolumeSnapshot {name!r}",
                           artifacts={"snapshot": name, "source_pvc": source_pvc})

    async def clone(self, name: str, source: str, size: str = "10Gi",
                    storage_class: str = "pure-block",
                    source_kind: str = "PersistentVolumeClaim", **_: Any) -> OpResult:
        namespace = self._namespace()
        if self.ctx.dry_run:
            await self.ctx.emit(f"[dry-run] would clone {source} -> PVC {name}.")
            return OpResult.ok("Dry-run: clone planned", artifacts={"pvc": name})
        yaml_text = manifests.pvc_yaml(
            name=name, namespace=namespace, size=size, storage_class=storage_class,
            data_source=source, data_source_kind=source_kind,
        )
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.emit(f"Cloning {source} ({source_kind}) -> PVC {name} ...")
            await self._apply(kubeconfig, yaml_text)
        return OpResult.ok(f"Cloned {source!r} -> PVC {name!r}",
                           artifacts={"pvc": name, "source": source})

    async def resize(self, name: str, size: str, **_: Any) -> OpResult:
        namespace = self._namespace()
        if self.ctx.dry_run:
            await self.ctx.emit(f"[dry-run] would resize PVC {name} to {size}.")
            return OpResult.ok("Dry-run: resize planned", artifacts={"pvc": name})
        patch = f'{{"spec":{{"resources":{{"requests":{{"storage":"{size}"}}}}}}}}'
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.emit(f"Patching PVC {name} to {size} ...")
            await self._kubectl(
                kubeconfig,
                f"patch pvc {name} -n {namespace} --type merge -p {patch!r}",
            )
        return OpResult.ok(f"Resized PVC {name!r} to {size}",
                           artifacts={"pvc": name, "size": size})

    async def rotate_credentials(self, api_token: str = "", **_: Any) -> OpResult:
        """Re-apply the px-pure-secret with a new / freshly minted FlashArray token.

        Portworx reads the FlashArray credential from the ``px-pure-secret``
        (``pure.json``), so rotation is just re-applying that secret with the new
        token; the operator's nodes pick up the updated credential. The endpoint
        comes from the associated array (not an operator field).
        """
        ns = self._px_namespace()
        array_endpoint = self._array_endpoint()
        if not api_token and self.ctx.array is not None:
            await self.ctx.emit("Minting fresh FlashArray API token for Portworx ...")
            api_token = await self.ctx.array.create_api_token("portworx")
        if not api_token:
            return OpResult.fail("No new API token provided and no array available to mint one")
        if self.ctx.dry_run:
            await self.ctx.emit("[dry-run] would re-apply px-pure-secret with new token.")
            return OpResult.ok("Dry-run: credential rotation planned",
                               artifacts={"secret": manifests.PX_PURE_SECRET})
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.emit(
                f"Re-applying {manifests.PX_PURE_SECRET} in {ns} with the rotated "
                "FlashArray token ...")
            await self._oc_apply(
                kubeconfig,
                manifests.portworx_pure_secret_yaml(
                    array_endpoint=array_endpoint, api_token=api_token, namespace=ns),
            )
        return OpResult.ok("Rotated FlashArray API token (px-pure-secret updated)",
                           artifacts={"secret": manifests.PX_PURE_SECRET, "namespace": ns})

    async def upgrade(self, release: str = manifests.PX_STORAGECLUSTER_DEFAULT,
                      chart_version: str = "", **_: Any) -> OpResult:
        """Upgrade Portworx by patching the StorageCluster image version.

        The Portworx Operator watches the StorageCluster and performs the rolling
        node upgrade once ``spec.image`` changes.
        """
        ns = self._px_namespace()
        version = chart_version or manifests.PX_DEFAULT_VERSION
        image = f"portworx/oci-monitor:{version}"
        if self.ctx.dry_run:
            await self.ctx.emit(
                f"[dry-run] would patch StorageCluster {release} image -> {image}.")
            return OpResult.ok("Dry-run: upgrade planned",
                               artifacts={"storagecluster": release, "version": version})
        patch = f'{{"spec":{{"image":"{image}"}}}}'
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.emit(f"Patching StorageCluster {release} image -> {image} ...")
            await self._kubectl(
                kubeconfig,
                f"-n {ns} patch storagecluster {release} --type merge -p {patch!r}",
            )
        return OpResult.ok(f"Upgraded Portworx StorageCluster {release!r} to {version}",
                           artifacts={"storagecluster": release, "version": version})

    async def health_check(self, **_: Any) -> OpResult:
        namespace = self._namespace()
        async with self._kubeconfig_file() as kubeconfig:
            await self.ctx.emit(f"Checking CSI pods in {namespace} ...")
            pods = await self._kubectl(kubeconfig, f"get pods -n {namespace}", check=False)
            drivers = await self._kubectl(kubeconfig, "get csidrivers", check=False)
        array = await self.ctx.array.info() if self.ctx.array else {}
        return OpResult.ok("Health check complete",
                           pods=pods, csidrivers=drivers, array=array)

    async def dispatch(self, action_id: str, params: dict[str, Any]) -> OpResult:
        """Route bespoke action ids, else fall back to the base dispatch table."""
        if action_id == "configure_binding":
            return await self.configure_binding(**params)
        return await super().dispatch(action_id, params)

    async def teardown(self, release: str = manifests.PX_STORAGECLUSTER_DEFAULT,
                       **_: Any) -> OpResult:
        return await self._teardown_portworx(release=release)

    async def _teardown_portworx(self, *, release: str) -> OpResult:
        """Remove Portworx: delete the StorageCluster, then the operator Subscription.

        The StorageCluster is deleted first so the operator can run its node
        decommission/cleanup before the operator itself goes away. The
        px-pure-secret is left in place (it carries no Portworx state and is
        cheap to recreate). Best-effort (check=False).
        """
        ns = self._px_namespace()
        sc = release or manifests.PX_STORAGECLUSTER_DEFAULT
        if self.ctx.dry_run:
            await self.ctx.emit(
                f"[dry-run] would delete StorageCluster {sc} + Portworx operator "
                f"Subscription in {ns}."
            )
            return OpResult.ok("Dry-run: Portworx teardown planned",
                               artifacts={"driver": "portworx", "storagecluster": sc})
        async with self._kubeconfig_file() as kubeconfig:
            kc = f"--kubeconfig {kubeconfig}"
            await self.ctx.emit(f"Deleting StorageCluster {sc} in {ns} ...")
            await self.ctx.runner.run_local(
                f"oc {kc} -n {ns} delete storagecluster {sc} --ignore-not-found",
                check=False,
            )
            await self.ctx.emit("Removing the Portworx operator Subscription ...")
            await self.ctx.runner.run_local(
                f"oc {kc} -n {ns} delete subscription {manifests.PX_OPERATOR_PACKAGE} "
                "--ignore-not-found", check=False,
            )
        return OpResult.ok(f"Removed Portworx StorageCluster {sc!r}",
                           artifacts={"driver": "portworx", "storagecluster": sc})

    # ================================================================== #
    # Migration: KubeVirt (OpenShift Virtualization) VMs <-> FlashArray
    # ================================================================== #
    # Each KubeVirt VM disk is a px-csi (FADA) PVC whose backing FlashArray volume
    # is px_<clusterUid[:8]>-<pvName>. So the migration model mirrors OpenStack
    # (Cinder volume == FA volume): create_managed_disk provisions a PVC and hands
    # back its FA volume for the orchestrator to copy into; capture maps each VM
    # disk's PVC back to its FA volume. vm_ref is "<namespace>/<name>".

    _BUS_MAP = {"scsi": "scsi", "virtio": "virtio", "sata": "sata",
                "ide": "sata", "nvme": "virtio", "usb": "usb"}

    def _vm_namespace(self) -> str:
        return self.ctx.target.get("vm_namespace") or "default"

    def _migration_sc(self) -> str:
        return (self.ctx.target.get("migration_storage_class")
                or manifests.MIGRATION_STORAGE_CLASS_DEFAULT)

    @staticmethod
    def _parse_ref(vm_ref: str, default_ns: str) -> tuple[str, str]:
        if vm_ref and "/" in vm_ref:
            ns, name = vm_ref.split("/", 1)
            return ns or default_ns, name
        return default_ns, vm_ref

    @staticmethod
    def _rfc1123(name: str) -> str:
        s = re.sub(r"[^a-z0-9-]", "-", (name or "vm").lower()).strip("-")
        return (s or "vm")[:53]

    @staticmethod
    def _qty_bytes(q: str) -> int:
        """Parse a Kubernetes quantity (e.g. '2Gi', '512Mi', '1000000') to bytes."""
        q = (q or "").strip()
        if not q:
            return 0
        units = {"Ki": 1024, "Mi": 1024**2, "Gi": 1024**3, "Ti": 1024**4,
                 "K": 1000, "M": 1000**2, "G": 1000**3, "T": 1000**4}
        for u, mult in units.items():
            if q.endswith(u):
                try:
                    return int(float(q[:-len(u)]) * mult)
                except ValueError:
                    return 0
        try:
            return int(float(q))
        except ValueError:
            return 0

    async def _cluster_uid(self, kubeconfig: str) -> str:
        """The Portworx StorageCluster clusterUid (cached) -- its first 8 chars are
        the prefix of every px-csi FADA FlashArray volume name."""
        cached = getattr(self, "_px_cluster_uid", None)
        if cached:
            return cached
        out = await self._kubectl(
            kubeconfig,
            "get storagecluster -A -o jsonpath={.items[0].status.clusterUid}",
            check=False)
        self._px_cluster_uid = "" if _looks_like_oc_error(out) else (out or "").strip()
        return self._px_cluster_uid

    async def _fa_volume_for_pvc(self, kubeconfig: str, ns: str, pvc: str):
        """Map a bound PVC to its px-csi FADA FlashArray volume (name, serial, size)."""
        pv = (await self._kubectl(
            kubeconfig, f"get pvc {pvc} -n {ns} -o jsonpath={{.spec.volumeName}}",
            check=False) or "").strip()
        if not pv or _looks_like_oc_error(pv):
            return "", None, None
        uid = await self._cluster_uid(kubeconfig)
        name = manifests.fada_fa_volume_name(uid, pv)
        serial = size = None
        if self.ctx.array is not None:
            with contextlib.suppress(Exception):
                vol = await self.ctx.array.get_volume(name)
                if vol:
                    serial, size = vol.get("serial"), vol.get("size")
        return name, serial, size

    async def _await_pvc_bound(self, kubeconfig: str, ns: str, pvc: str, *,
                               timeout: int = 120, interval: int = 5) -> None:
        if getattr(self.ctx.runner, "mock", False) or self.ctx.dry_run:
            return
        for _ in range(max(1, timeout // interval)):
            ph = (await self._kubectl(
                kubeconfig, f"get pvc {pvc} -n {ns} -o jsonpath={{.status.phase}}",
                check=False) or "").strip()
            if ph == "Bound":
                return
            await asyncio.sleep(interval)
        raise MigrationCreateError(f"PVC {ns}/{pvc} did not bind within {timeout}s")

    # ----------------------------------------------------- migration meta ---
    def migration_host_group(self) -> str:
        # Portworx (px-csi) owns the FlashArray host objects + attaches volumes at
        # pod scheduling time; there is no PHIF-managed host group. A non-empty
        # sentinel satisfies the orchestrator's "destination has a host group" gate.
        return "portworx-fada"

    async def require_host_objects(self, host_group: str) -> OpResult:
        return OpResult.ok(
            "Portworx (px-csi) manages FlashArray host objects automatically; no "
            "PHIF host group to validate")

    async def list_networks(self) -> list[dict[str, Any]]:
        """KubeVirt VM networks: the pod network + any Multus NetworkAttachmentDefinitions."""
        nets: list[dict[str, Any]] = [{"id": "pod", "name": "Pod network (default)"}]
        if not self._has_real_cluster_output():
            nets.append({"id": "default/bridge-net", "name": "bridge-net (NAD, default)"})
            return nets
        with contextlib.suppress(Exception):
            async with self._kubeconfig_file() as kc:
                raw = await self._kubectl(kc, "get net-attach-def -A -o json", check=False)
            items = json.loads(raw).get("items", []) if raw and not _looks_like_oc_error(raw) else []
            for it in items:
                meta = it.get("metadata") or {}
                ns, nm = meta.get("namespace", ""), meta.get("name", "")
                if nm:
                    nets.append({"id": f"{ns}/{nm}", "name": f"{nm} ({ns})"})
        return nets

    async def list_vms(self) -> list[dict[str, Any]]:
        """List KubeVirt VirtualMachines (migration sources). ref = '<ns>/<name>'."""
        # Shape matches VmSummary: id = "<ns>/<name>" (the migration vm_ref),
        # power_state lower-cased ("running"/"stopped"/...).
        if not self._has_real_cluster_output():
            return [{"id": f"default/mock-vm-{i}", "name": f"mock-vm-{i}",
                     "namespace": "default", "power_state": "stopped"} for i in (1, 2)]
        out: list[dict[str, Any]] = []
        with contextlib.suppress(Exception):
            async with self._kubeconfig_file() as kc:
                raw = await self._kubectl(kc, "get vm -A -o json", check=False)
            items = json.loads(raw).get("items", []) if raw and not _looks_like_oc_error(raw) else []
            for it in items:
                meta = it.get("metadata") or {}
                ns, nm = meta.get("namespace", ""), meta.get("name", "")
                st = (it.get("status") or {}).get("printableStatus", "") or "unknown"
                out.append({"id": f"{ns}/{nm}", "name": nm, "namespace": ns,
                            "power_state": st.lower()})
        return out

    # ----------------------------------------------------- capture (source) ---
    async def _resolve_compute(self, kc: str, ns: str, doc: dict, dom: dict):
        """vCPU (sockets*cores*threads) + RAM bytes, from inline domain or an
        instancetype reference."""
        cpu = dom.get("cpu") or {}
        cores, sockets, threads = (int(cpu.get("cores") or 0),
                                   int(cpu.get("sockets") or 0),
                                   int(cpu.get("threads") or 0))
        vcpus = ((cores or 1) * (sockets or 1) * (threads or 1)
                 if (cores or sockets or threads) else 0)
        mem = self._qty_bytes((dom.get("memory") or {}).get("guest") or "")
        if not mem:
            mem = self._qty_bytes(
                ((dom.get("resources") or {}).get("requests") or {}).get("memory") or "")
        it = (doc.get("spec") or {}).get("instancetype") or {}
        if (not vcpus or not mem) and it.get("name"):
            kind = it.get("kind", "VirtualMachineClusterInstancetype")
            cluster = "Cluster" in kind
            res = ("virtualmachineclusterinstancetypes" if cluster
                   else "virtualmachineinstancetypes")
            nsflag = "" if cluster else f"-n {ns} "
            c = await self._kubectl(
                kc, f"get {res} {nsflag}{it['name']} -o jsonpath={{.spec.cpu.guest}}",
                check=False)
            m = await self._kubectl(
                kc, f"get {res} {nsflag}{it['name']} -o jsonpath={{.spec.memory.guest}}",
                check=False)
            with contextlib.suppress(ValueError):
                vcpus = vcpus or int((c or "0").strip() or 0)
            mem = mem or self._qty_bytes((m or "").strip())
        return vcpus or 1, mem or 512 * 1024 * 1024

    async def _vmi_macs(self, kc: str, ns: str, name: str) -> dict:
        """MACs assigned to a running VMI's interfaces (preserved across migration)."""
        raw = await self._kubectl(kc, f"get vmi {name} -n {ns} -o json", check=False)
        if not raw or _looks_like_oc_error(raw):
            return {}
        try:
            doc = json.loads(raw)
        except (ValueError, TypeError):
            return {}
        out = {}
        for i in (doc.get("status") or {}).get("interfaces") or []:
            if i.get("name") and i.get("mac"):
                out[i["name"]] = i["mac"]
        return out

    async def _vmi_interfaces(self, kc: str, ns: str, name: str) -> tuple[list, list]:
        """The running VMI's resolved (interfaces, networks) from its spec —
        KubeVirt materializes auto-attached defaults here. Empty if VM is stopped."""
        raw = await self._kubectl(kc, f"get vmi {name} -n {ns} -o json", check=False)
        if not raw or _looks_like_oc_error(raw):
            return [], []
        try:
            doc = json.loads(raw)
        except (ValueError, TypeError):
            return [], []
        spec = doc.get("spec") or {}
        dom = spec.get("domain") or {}
        return (list((dom.get("devices") or {}).get("interfaces") or []),
                list(spec.get("networks") or []))

    async def capture_vm_spec(self, vm_ref: str) -> "VmSpec":
        ns, name = self._parse_ref(vm_ref, self._vm_namespace())
        if not self._has_real_cluster_output():
            return VmSpec(
                name=name, source_ref=vm_ref, vcpus=2, memory_bytes=2 * 1024**3,
                firmware="bios",
                disks=[DiskSpec(identity=DiskIdentity(
                    fa_volume=f"px_mock-{name}", serial="0123456700mock0000000001",
                    size_bytes=10 * 1024**3), bus="virtio", order=0, boot=True)],
                nics=[NicSpec(mac="52:54:00:00:00:01", source_network="pod", order=0)],
                raw={"namespace": ns})
        async with self._kubeconfig_file() as kc:
            raw = await self._kubectl(kc, f"get vm {name} -n {ns} -o json", check=False)
            if not raw or _looks_like_oc_error(raw):
                raise MigrationCreateError(f"VM {vm_ref!r} not found")
            doc = json.loads(raw)
            tmpl = (doc.get("spec") or {}).get("template", {}).get("spec", {})
            dom = tmpl.get("domain") or {}
            vcpus, mem = await self._resolve_compute(kc, ns, doc, dom)
            efi = ((dom.get("firmware") or {}).get("bootloader") or {}).get("efi")
            firmware = "uefi" if efi is not None else "bios"
            secure = bool((efi or {}).get("secureBoot")) if isinstance(efi, dict) else False
            devs = dom.get("devices") or {}
            disk_bus = {d.get("name"): ((d.get("disk") or {}).get("bus") or "virtio")
                        for d in (devs.get("disks") or [])}
            disk_boot = {d.get("name"): d.get("bootOrder")
                         for d in (devs.get("disks") or [])}
            disks: list[DiskSpec] = []
            order = 0
            for vol in tmpl.get("volumes") or []:
                pvc = None
                if vol.get("persistentVolumeClaim"):
                    pvc = (vol["persistentVolumeClaim"] or {}).get("claimName")
                elif vol.get("dataVolume"):
                    pvc = (vol["dataVolume"] or {}).get("name")  # CDI PVC == DV name
                if not pvc:
                    continue  # cloudInitNoCloud / containerDisk -- not FA-backed
                fa, serial, size = await self._fa_volume_for_pvc(kc, ns, pvc)
                if not fa:
                    await self.ctx.emit(
                        f"[capture] skipping volume {vol.get('name')!r}: PVC {pvc!r} "
                        "has no resolvable FlashArray volume")
                    continue
                vn = vol.get("name")
                bo = disk_boot.get(vn)
                disks.append(DiskSpec(
                    identity=DiskIdentity(fa_volume=fa, serial=serial, size_bytes=size),
                    bus=disk_bus.get(vn, "virtio"), order=order,
                    boot=(bo == 1), source_ref=f"{vn}:pvc/{pvc}"))
                order += 1
            if disks and not any(d.boot for d in disks):
                disks[0].boot = True
            macs = await self._vmi_macs(kc, ns, name)
            # Interfaces/networks come from the VM template, but KubeVirt also
            # auto-attaches a default pod interface when a VM declares none
            # (autoattachPodInterface defaults true) — those only appear on the
            # running VMI. Fall back to the VMI's resolved interfaces, then
            # synthesize the default pod NIC, so such VMs still migrate with a NIC
            # instead of reporting "no network interfaces".
            ifaces = list(devs.get("interfaces") or [])
            networks = list(tmpl.get("networks") or [])
            if not ifaces:
                ifaces, networks = await self._vmi_interfaces(kc, ns, name)
            nets = {n.get("name"): n for n in networks}
            nics: list[NicSpec] = []
            for i, iface in enumerate(ifaces):
                nm = iface.get("name")
                net = nets.get(nm, {})
                src = ("pod" if net.get("pod") is not None
                       else (net.get("multus") or {}).get("networkName") or nm or "pod")
                mac = macs.get(nm) or iface.get("macAddress") or ""
                nics.append(NicSpec(mac=mac, source_network=src, model="virtio", order=i))
            if not nics and devs.get("autoattachPodInterface", True) is not False:
                nics.append(NicSpec(mac=macs.get("default", ""), source_network="pod",
                                    model="virtio", order=0))
            return VmSpec(name=name, source_ref=vm_ref, vcpus=vcpus, memory_bytes=mem,
                          firmware=firmware, secure_boot=secure, disks=disks,
                          nics=nics, raw={"namespace": ns})

    # ------------------------------------------------------ power lifecycle ---
    async def power_state(self, vm_ref: str) -> str:
        ns, name = self._parse_ref(vm_ref, self._vm_namespace())
        if not self._has_real_cluster_output():
            return "unknown"
        async with self._kubeconfig_file() as kc:
            st = (await self._kubectl(
                kc, f"get vm {name} -n {ns} -o jsonpath={{.status.printableStatus}}",
                check=False) or "").strip().lower()
        if not st or _looks_like_oc_error(st):
            return "unknown"
        if st in ("running", "migrating"):
            return "running"
        if st in ("stopped", "halted"):
            return "stopped"
        return "unknown"

    async def _set_run(self, kc: str, ns: str, name: str, run: bool) -> None:
        """Start/stop a VM, honoring whichever lifecycle field it uses (a VM has
        EITHER spec.running OR spec.runStrategy -- patching the wrong one errors)."""
        rs = (await self._kubectl(
            kc, f"get vm {name} -n {ns} -o jsonpath={{.spec.runStrategy}}",
            check=False) or "").strip()
        if rs and not _looks_like_oc_error(rs):
            patch = '{"spec":{"runStrategy":"%s"}}' % ("Always" if run else "Halted")
        else:
            patch = '{"spec":{"running":%s}}' % ("true" if run else "false")
        await self._kubectl(
            kc, f"patch vm {name} -n {ns} --type merge -p {shlex.quote(patch)}")

    async def stop_vm(self, vm_ref: str, *, force: bool = False) -> OpResult:
        ns, name = self._parse_ref(vm_ref, self._vm_namespace())
        if self.ctx.dry_run:
            await self.ctx.emit(f"[dry-run] would stop VM {vm_ref}")
            return OpResult.ok("Dry-run: stop planned")
        async with self._kubeconfig_file() as kc:
            await self._set_run(kc, ns, name, False)
        return OpResult.ok(f"Stopping VM {vm_ref}")

    async def start_vm(self, vm_ref: str) -> OpResult:
        ns, name = self._parse_ref(vm_ref, self._vm_namespace())
        if self.ctx.dry_run:
            await self.ctx.emit(f"[dry-run] would start VM {vm_ref}")
            return OpResult.ok("Dry-run: start planned")
        async with self._kubeconfig_file() as kc:
            await self._set_run(kc, ns, name, True)
        return OpResult.ok(f"Starting VM {vm_ref}")

    # ------------------------------------------------ create (destination) ---
    async def create_vm(self, spec: "VmSpec", *, network_map: dict,
                        placement: dict | None = None) -> OpResult:
        ns = self._vm_namespace()
        vmname = self._rfc1123(spec.name)
        ref = f"{ns}/{vmname}"
        if self.ctx.dry_run or not self._has_real_cluster_output():
            # Dry-run / mock: synthesize the per-disk FA volume names so
            # create_managed_disk has something to return (no real cluster I/O).
            self._mig_disks = getattr(self, "_mig_disks", {})
            self._mig_disks[ref] = {d.order: f"px_dryrun-{vmname}-{d.order}"
                                    for d in spec.disks}
            await self.ctx.emit(f"[dry-run] would create KubeVirt VM {ref}")
            return OpResult.ok("Dry-run: create_vm planned", artifacts={"vm_ref": ref})
        try:
            return await self._create_vm_inner(ns, vmname, ref, spec, network_map)
        except Exception as exc:  # noqa: BLE001 -- clean up partial artifacts
            await self.ctx.emit(
                f"[create_vm] failed ({type(exc).__name__}: {exc}); cleaning up partial "
                "VM + PVCs")
            with contextlib.suppress(Exception):
                await self._cleanup_vm_artifacts(ns, vmname)
            return OpResult.fail(f"create_vm failed: {type(exc).__name__}: {exc}")

    async def _create_vm_inner(self, ns: str, vmname: str, ref: str, spec: "VmSpec",
                               network_map: dict) -> OpResult:
        cache: dict = {}
        disks_meta: list = []
        async with self._kubeconfig_file() as kc:
            for disk in spec.disks:
                pvc = f"{vmname}-disk{disk.order}"
                gib = max(1, -(-(disk.identity.size_bytes or 1) // (1024**3)))
                await self._apply(kc, manifests.migration_pvc_yaml(
                    name=pvc, namespace=ns, size=f"{gib}Gi",
                    storage_class=self._migration_sc()))
                await self._await_pvc_bound(kc, ns, pvc)
                fa, _, _ = await self._fa_volume_for_pvc(kc, ns, pvc)
                cache[disk.order] = fa
                disks_meta.append({
                    "name": f"disk{disk.order}", "claim": pvc,
                    "bus": self._BUS_MAP.get((disk.bus or "virtio").lower(), "virtio"),
                    "boot_order": 1 if disk.boot else None})
                await self.ctx.emit(
                    f"[dest] PVC {ns}/{pvc} bound -> FlashArray volume {fa}")
            if disks_meta and not any(d["boot_order"] for d in disks_meta):
                disks_meta[0]["boot_order"] = 1
            nics_meta: list = []
            for nic in spec.nics:
                dest = network_map.get(nic.source_network, "")
                pod = dest in ("", "pod", "pod network", "default")
                nics_meta.append({"name": f"nic{nic.order}", "mac": nic.mac,
                                  "dest": dest, "pod": pod})
            vm_yaml = manifests.virtual_machine_yaml(
                name=vmname, namespace=ns, vcpus=spec.vcpus,
                memory_bytes=spec.memory_bytes, firmware=spec.firmware,
                secure_boot=spec.secure_boot,
                disks=disks_meta, nics=nics_meta, running=False)
            await self._apply(kc, vm_yaml)
        self._mig_disks = getattr(self, "_mig_disks", {})
        self._mig_disks[ref] = cache
        await self.ctx.emit(f"[dest] created KubeVirt VM {ref} (stopped)")
        return OpResult.ok(f"Created VM {ref}", artifacts={"vm_ref": ref})

    async def create_managed_disk(self, vm_ref: str, *, size_bytes: int,
                                  order: int, boot: bool) -> str:
        """Return the FlashArray volume backing this VM's disk PVC (created by
        create_vm). The orchestrator then copy-with-overwrites the source data onto
        it; the VM boots from the now-populated PVC."""
        fa = getattr(self, "_mig_disks", {}).get(vm_ref, {}).get(order)
        if not fa:
            raise MigrationCreateError(
                f"no managed disk cached for {vm_ref} order={order} "
                "(create_vm must run first)")
        return fa

    async def set_boot_order(self, vm_ref: str, disks: "list[DiskSpec]") -> OpResult:
        # Boot order is stamped into the VM CR at create time (bootOrder: 1 on the
        # boot disk), so nothing to do here.
        return OpResult.ok("boot order set at VM creation")

    async def delete_vm(self, vm_ref: str, *, keep_disks: bool = True) -> OpResult:
        ns, name = self._parse_ref(vm_ref, self._vm_namespace())
        if self.ctx.dry_run:
            await self.ctx.emit(f"[dry-run] would delete VM {vm_ref}")
            return OpResult.ok("Dry-run: delete planned")
        async with self._kubeconfig_file() as kc:
            await self._kubectl(kc, f"delete vm {name} -n {ns} --ignore-not-found",
                                check=False)
            if not keep_disks:
                orders = getattr(self, "_mig_disks", {}).get(vm_ref, {})
                for order in (orders or {}):
                    await self._kubectl(
                        kc, f"delete pvc {name}-disk{order} -n {ns} --ignore-not-found",
                        check=False)
                if not orders:
                    await self._kubectl(
                        kc, f"delete pvc -n {ns} -l phif.purestorage.com/migration=true "
                        "--ignore-not-found", check=False)
        return OpResult.ok(f"Deleted VM {vm_ref}",
                           artifacts={"vm_ref": vm_ref, "kept_disks": keep_disks})

    async def _cleanup_vm_artifacts(self, ns: str, vmname: str) -> None:
        """Best-effort removal of a partially-created VM + its migration PVCs."""
        async with self._kubeconfig_file() as kc:
            await self._kubectl(kc, f"delete vm {vmname} -n {ns} --ignore-not-found",
                                check=False)
            await self._kubectl(
                kc, f"delete pvc -n {ns} -l phif.purestorage.com/migration=true "
                "--ignore-not-found", check=False)
