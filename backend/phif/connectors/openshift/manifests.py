"""In-code generators for the Kubernetes/OpenShift manifests this connector applies.

Everything here is pure string templating (no SDK, no I/O) so it is trivially
unit-testable and safe to import in mock mode. The connector writes the rendered
YAML to a temp file and applies it via ``oc apply -f <file>``.

Portworx (``pxd.portworx.com``) is the only supported CSI driver — the legacy Everpure
Service Orchestrator (``pure-csi``) chart has been retired and its
generators have been removed.
"""

from __future__ import annotations

from phif.connectors.iscsi_net import ARP_SYSCTL_FILE, arp_sysctl_content

# Map a connector "driver" choice to the CSI provisioner string used in
# StorageClass / VolumeSnapshotClass / CSIDriver objects. Portworx only.
DRIVER_PROVISIONER = {
    "portworx": "pxd.portworx.com",
}


def provisioner_for(driver: str) -> str:
    """Return the CSI provisioner string for a driver choice (defaults to Portworx)."""
    return DRIVER_PROVISIONER.get(driver, "pxd.portworx.com")


# --------------------------------------------------------------------------- #
# Portworx (px-csi) — Everpure's current supported CSI on OpenShift
# --------------------------------------------------------------------------- #
# Portworx Enterprise is installed via the Portworx Operator (OLM) + a
# StorageCluster CR. FlashArray backing (Portworx "FlashArray Cloud Drives") is supplied
# through a `px-pure-secret` carrying the FlashArray management endpoint + API
# token; Portworx then carves cloud drives off the array over iSCSI/FC. We derive
# the endpoint + token from the FlashArray associated with this hypervisor —
# operators don't re-enter them.
#
# TODO(doc-validate): pin against the current Portworx on OpenShift install guide:
#   * operator package/channel/catalog (portworx-certified in certified-operators);
#   * StorageCluster apiVersion (core.libopenstorage.org/v1) + the cloudStorage /
#     FlashArray cloud-drive fields and how SAN type (iSCSI vs FC) is selected;
#   * the default px image/version tag.
PX_NAMESPACE_DEFAULT = "portworx"
PX_OPERATOR_PACKAGE = "portworx-certified"
PX_OPERATOR_CHANNEL = "stable"
PX_OPERATOR_CATALOG = "certified-operators"
PX_OPERATOR_CATALOG_NAMESPACE = "openshift-marketplace"
PX_STORAGECLUSTER_DEFAULT = "px-cluster"
# px (oci-monitor) image version. TODO(doc-validate): track the current stable tag.
PX_DEFAULT_VERSION = "3.1.2"
PX_PURE_SECRET = "px-pure-secret"


def portworx_namespace_yaml(*, namespace: str = PX_NAMESPACE_DEFAULT) -> str:
    """Render the Portworx install Namespace."""
    return (
        "apiVersion: v1\n"
        "kind: Namespace\n"
        "metadata:\n"
        f"  name: {namespace}\n"
    )


def portworx_operatorgroup_yaml(*, namespace: str = PX_NAMESPACE_DEFAULT) -> str:
    """Render an OLM OperatorGroup scoping the Portworx operator to its namespace."""
    return (
        "apiVersion: operators.coreos.com/v1\n"
        "kind: OperatorGroup\n"
        "metadata:\n"
        f"  name: portworx\n"
        f"  namespace: {namespace}\n"
        "spec:\n"
        "  targetNamespaces:\n"
        f"    - {namespace}\n"
    )


def portworx_subscription_yaml(*, namespace: str = PX_NAMESPACE_DEFAULT,
                               channel: str = PX_OPERATOR_CHANNEL) -> str:
    """Render the OLM Subscription that installs the Portworx operator."""
    return (
        "apiVersion: operators.coreos.com/v1alpha1\n"
        "kind: Subscription\n"
        "metadata:\n"
        f"  name: {PX_OPERATOR_PACKAGE}\n"
        f"  namespace: {namespace}\n"
        "spec:\n"
        f"  channel: {channel}\n"
        f"  name: {PX_OPERATOR_PACKAGE}\n"
        f"  source: {PX_OPERATOR_CATALOG}\n"
        f"  sourceNamespace: {PX_OPERATOR_CATALOG_NAMESPACE}\n"
        "  installPlanApproval: Automatic\n"
    )


def portworx_pure_secret_yaml(*, array_endpoint: str, api_token: str,
                              namespace: str = PX_NAMESPACE_DEFAULT) -> str:
    """Render the ``px-pure-secret`` carrying the FlashArray endpoint + API token.

    Portworx reads ``pure.json`` from this secret to provision FlashArray cloud
    drives. The token lives in the secret's ``stringData`` (server-side), not on
    any command line, so it never reaches the job log.
    """
    import json
    pure_json = json.dumps(
        {"FlashArrays": [{"MgmtEndPoint": array_endpoint, "APIToken": api_token}]},
        indent=2,
    )
    indented = "".join(f"    {line}\n" for line in pure_json.splitlines())
    return (
        "apiVersion: v1\n"
        "kind: Secret\n"
        "metadata:\n"
        f"  name: {PX_PURE_SECRET}\n"
        f"  namespace: {namespace}\n"
        "type: Opaque\n"
        "stringData:\n"
        "  pure.json: |\n"
        f"{indented}"
    )


def portworx_storagecluster_yaml(*, name: str = PX_STORAGECLUSTER_DEFAULT,
                                 namespace: str = PX_NAMESPACE_DEFAULT,
                                 version: str = PX_DEFAULT_VERSION,
                                 device_size_gi: int = 150,
                                 cluster_id: str = "",
                                 direct_access: bool = True) -> str:
    """Render a StorageCluster CR with CSI enabled, in one of two modes.

    * ``direct_access=True`` (FlashArray Direct Access / FADA — the "just CSI"
      mode): NO ``cloudStorage`` block, so Portworx does NOT carve/pool cloud
      drives off the array. PVCs map 1:1 to FlashArray volumes via the
      ``backend: pure_block`` StorageClass (see :func:`storage_class_yaml`). This
      is the lightweight PSO-style behavior — no pooled SDS data plane.
    * ``direct_access=False`` (Portworx Enterprise pooling): adds
      ``cloudStorage.deviceSpecs`` so Portworx aggregates FlashArray cloud drives
      into a pooled storage layer (needs a PX-Enterprise license).

    Either way ``csi.enabled`` turns on the pxd.portworx.com CSI driver and
    ``portworx.io/is-openshift`` puts the operator in OpenShift mode.

    # TODO(doc-validate): confirm the FADA StorageCluster shape for the target
    # Portworx version — in particular whether internal KVDB needs a dedicated
    # metadata/KVDB device when no cloudStorage data drives are defined, and how
    # to pin SAN type (iSCSI vs FC).
    """
    cid = f"\n    portworx.io/cluster-id: {cluster_id}" if cluster_id else ""
    cloud_storage = "" if direct_access else (
        "  cloudStorage:\n"
        "    deviceSpecs:\n"
        f"      - \"size={device_size_gi}\"\n"
    )
    return (
        "apiVersion: core.libopenstorage.org/v1\n"
        "kind: StorageCluster\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"  namespace: {namespace}\n"
        "  annotations:\n"
        f"    portworx.io/is-openshift: \"true\"{cid}\n"
        "spec:\n"
        f"  image: portworx/oci-monitor:{version}\n"
        "  imagePullPolicy: Always\n"
        "  kvdb:\n"
        "    internal: true\n"
        f"{cloud_storage}"
        "  secretsProvider: k8s\n"
        "  stork:\n"
        "    enabled: true\n"
        "  csi:\n"
        "    enabled: true\n"
        "  monitoring:\n"
        "    telemetry:\n"
        "      enabled: false\n"
    )


# Bootstrap ServiceAccount used to mint a durable, token-based kubeconfig from a
# username/password login. Lives in kube-system (always present) and is granted a
# dedicated, least-privilege ClusterRole (NOT cluster-admin) scoped to exactly the
# resources PHIF touches: Portworx (px-csi) install, storage classes / snapshots,
# PVC lifecycle, and the interface-binding MachineConfig.
SA_NAME = "phif-operator"
SA_NAMESPACE = "kube-system"
SA_CLUSTERROLE = "phif-operator"
SA_CLUSTERROLEBINDING = "phif-operator"


def service_account_clusterrole_yaml(*, name: str = SA_CLUSTERROLE) -> str:
    """Render the least-privilege ClusterRole for the PHIF operator SA.

    Scoped to the API groups/resources PHIF actually uses rather than
    cluster-admin:

    * **Portworx (px-csi) install** — apps workloads,
      services/configmaps/secrets/SAs, CRDs, and the RBAC objects the operator and
      its components create (the ``bind`` + ``escalate`` verbs on
      roles/clusterroles are required to create that RBAC without tripping the RBAC
      escalation check); plus the OLM OperatorGroup / Subscription
      (``operators.coreos.com``) and the Portworx ``StorageCluster``
      (``core.libopenstorage.org``) the operator reconciles.
    * **Storage day-2** — storageclasses / csidrivers, the snapshot CRs, and the
      full PVC lifecycle (provision / clone / resize / delete); PVs read-only.
    * **Interface binding** — machineconfigs / machineconfigpools, plus read-only
      nodes + NMState node-network state for discovery / validation.

    This is intentionally broad enough to install a CSI driver but is enumerated
    and auditable, and cannot touch arbitrary resource types the way cluster-admin
    can. Created by the (admin) bootstrap user, so the RBAC escalation rules are
    satisfied at creation time.
    """
    return (
        "apiVersion: rbac.authorization.k8s.io/v1\n"
        "kind: ClusterRole\n"
        "metadata:\n"
        f"  name: {name}\n"
        "rules:\n"
        # Cluster + storage discovery / validation.
        "- apiGroups: [\"\"]\n"
        "  resources: [\"nodes\", \"events\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\"]\n"
        "- apiGroups: [\"\"]\n"
        "  resources: [\"namespaces\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\"]\n"
        # PVC lifecycle (provision / clone / resize / delete); PVs read-only.
        "- apiGroups: [\"\"]\n"
        "  resources: [\"persistentvolumeclaims\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        "- apiGroups: [\"\"]\n"
        "  resources: [\"persistentvolumes\", \"pods\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\"]\n"
        # Core resources the Portworx operator + components manage.
        "- apiGroups: [\"\"]\n"
        "  resources: [\"secrets\", \"configmaps\", \"serviceaccounts\", \"services\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        "- apiGroups: [\"apps\"]\n"
        "  resources: [\"deployments\", \"daemonsets\", \"statefulsets\", \"replicasets\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        # StorageClass / CSIDriver / snapshot CRs.
        "- apiGroups: [\"storage.k8s.io\"]\n"
        "  resources: [\"storageclasses\", \"csidrivers\", \"csinodes\", \"volumeattachments\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        "- apiGroups: [\"snapshot.storage.k8s.io\"]\n"
        "  resources: [\"volumesnapshotclasses\", \"volumesnapshots\", \"volumesnapshotcontents\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        # CRDs the Portworx operator / snapshot stack registers.
        "- apiGroups: [\"apiextensions.k8s.io\"]\n"
        "  resources: [\"customresourcedefinitions\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        # RBAC the operator installs for its components. bind + escalate are required
        # to create these without tripping the RBAC escalation check.
        "- apiGroups: [\"rbac.authorization.k8s.io\"]\n"
        "  resources: [\"roles\", \"rolebindings\", \"clusterroles\", \"clusterrolebindings\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\", \"bind\", \"escalate\"]\n"
        # OLM: install the Portworx operator (OperatorGroup + Subscription).
        "- apiGroups: [\"operators.coreos.com\"]\n"
        "  resources: [\"operatorgroups\", \"subscriptions\", \"clusterserviceversions\", \"installplans\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        # The Portworx StorageCluster the operator reconciles.
        "- apiGroups: [\"core.libopenstorage.org\"]\n"
        "  resources: [\"storageclusters\", \"storagenodes\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        # OpenShift MachineConfig (storage interface binding).
        "- apiGroups: [\"machineconfiguration.openshift.io\"]\n"
        "  resources: [\"machineconfigs\", \"machineconfigpools\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\", \"create\", \"update\", \"patch\", \"delete\"]\n"
        # NMState node-network discovery (read-only, optional).
        "- apiGroups: [\"nmstate.io\"]\n"
        "  resources: [\"nodenetworkstates\", \"nodenetworkconfigurationpolicies\"]\n"
        "  verbs: [\"get\", \"list\", \"watch\"]\n"
    )


def service_account_token_secret_yaml(*, name: str = f"{SA_NAME}-token",
                                      sa_name: str = SA_NAME,
                                      namespace: str = SA_NAMESPACE) -> str:
    """Render a long-lived ``kubernetes.io/service-account-token`` Secret.

    On OCP 4.11+ ServiceAccounts no longer get an auto-generated token Secret, and
    ``oc create token`` only mints time-bounded tokens. Applying this annotated
    Secret makes the controller populate it with a NON-expiring token for the SA —
    the durable fallback when ``oc create token`` is unavailable / undesirable.
    """
    return (
        "apiVersion: v1\n"
        "kind: Secret\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"  namespace: {namespace}\n"
        "  annotations:\n"
        f"    kubernetes.io/service-account.name: {sa_name}\n"
        "type: kubernetes.io/service-account-token\n"
    )


def service_account_kubeconfig_yaml(*, api_url: str, token: str,
                                    ca_data: str = "",
                                    insecure: bool = False,
                                    cluster_name: str = "phif",
                                    user_name: str = SA_NAME) -> str:
    """Render a token-based kubeconfig pointing at ``api_url``.

    Embeds the ServiceAccount bearer ``token``. TLS trust is established by the
    cluster's ``ca_data`` (base64 CA bundle) when available; otherwise
    ``insecure`` toggles ``insecure-skip-tls-verify``. This is the durable
    kubeconfig we persist so day-2 ops stop depending on the user's login session.
    """
    if ca_data:
        tls_line = f"    certificate-authority-data: {ca_data}\n"
    else:
        tls_line = f"    insecure-skip-tls-verify: {str(bool(insecure)).lower()}\n"
    return (
        "apiVersion: v1\n"
        "kind: Config\n"
        "clusters:\n"
        f"- name: {cluster_name}\n"
        "  cluster:\n"
        f"    server: {api_url}\n"
        f"{tls_line}"
        "contexts:\n"
        f"- name: {cluster_name}\n"
        "  context:\n"
        f"    cluster: {cluster_name}\n"
        f"    user: {user_name}\n"
        f"current-context: {cluster_name}\n"
        "users:\n"
        f"- name: {user_name}\n"
        "  user:\n"
        f"    token: {token}\n"
    )


def storage_class_yaml(*, name: str, driver: str, fs_type: str = "xfs",
                       backend: str = "block", reclaim_policy: str = "Delete",
                       allow_expansion: bool = True, is_default: bool = False) -> str:
    """Render a StorageClass for the Portworx (pxd.portworx.com) driver.

    ``backend`` selects the provisioning backend for Portworx FlashArray Direct
    Access (FADA): "pure_block" / "pure_file" — the ``backend: pure_block``
    parameter is what makes the pxd.portworx.com CSI driver provision the volume
    *directly* on the FlashArray (1 PVC = 1 FA volume) instead of from a pooled
    Portworx storage layer.

    The same block StorageClass is valid for FC: the transport (iSCSI vs FC) is
    selected at the driver/array level, not per-StorageClass.
    # TODO(doc-validate): confirm the FADA StorageClass parameter set (e.g.
    # pure_export_rules for file, any iSCSI/FC selector) for the target Portworx.
    """
    provisioner = provisioner_for(driver)
    annotations = ""
    if is_default:
        annotations = (
            "  annotations:\n"
            "    storageclass.kubernetes.io/is-default-class: \"true\"\n"
        )
    params = f"  csi.storage.k8s.io/fstype: {fs_type}\n"
    # Portworx FADA uses pure_block/pure_file as the StorageClass backend param.
    if backend:
        params += f"  backend: {backend}\n"
    return (
        "apiVersion: storage.k8s.io/v1\n"
        "kind: StorageClass\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"{annotations}"
        f"provisioner: {provisioner}\n"
        f"reclaimPolicy: {reclaim_policy}\n"
        f"allowVolumeExpansion: {str(allow_expansion).lower()}\n"
        "parameters:\n"
        f"{params}"
    )


def volume_snapshot_class_yaml(*, name: str, driver: str,
                               deletion_policy: str = "Delete") -> str:
    """Render a VolumeSnapshotClass for the given driver."""
    provisioner = provisioner_for(driver)
    return (
        "apiVersion: snapshot.storage.k8s.io/v1\n"
        "kind: VolumeSnapshotClass\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"driver: {provisioner}\n"
        f"deletionPolicy: {deletion_policy}\n"
    )


def pvc_yaml(*, name: str, namespace: str, size: str, storage_class: str,
             access_mode: str = "ReadWriteOnce",
             data_source: str | None = None,
             data_source_kind: str = "PersistentVolumeClaim") -> str:
    """Render a PersistentVolumeClaim, optionally cloning from a dataSource.

    ``data_source`` set with ``data_source_kind="PersistentVolumeClaim"`` clones
    an existing PVC; with ``data_source_kind="VolumeSnapshot"`` it restores a
    snapshot.
    """
    ds = ""
    if data_source:
        api_group = (
            "snapshot.storage.k8s.io"
            if data_source_kind == "VolumeSnapshot"
            else ""
        )
        group_line = f"    apiGroup: {api_group}\n" if api_group else ""
        ds = (
            "  dataSource:\n"
            f"    name: {data_source}\n"
            f"    kind: {data_source_kind}\n"
            f"{group_line}"
        )
    return (
        "apiVersion: v1\n"
        "kind: PersistentVolumeClaim\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"  namespace: {namespace}\n"
        "spec:\n"
        f"  accessModes:\n    - {access_mode}\n"
        f"  storageClassName: {storage_class}\n"
        f"{ds}"
        "  resources:\n"
        "    requests:\n"
        f"      storage: {size}\n"
    )


def volume_snapshot_yaml(*, name: str, namespace: str, source_pvc: str,
                         snapshot_class: str) -> str:
    """Render a VolumeSnapshot custom resource referencing a source PVC."""
    return (
        "apiVersion: snapshot.storage.k8s.io/v1\n"
        "kind: VolumeSnapshot\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"  namespace: {namespace}\n"
        "spec:\n"
        f"  volumeSnapshotClassName: {snapshot_class}\n"
        "  source:\n"
        f"    persistentVolumeClaimName: {source_pvc}\n"
    )


# --------------------------------------------------------------------------- #
# Interface binding via MachineConfig (Ignition)
# --------------------------------------------------------------------------- #
# OpenShift worker nodes (RHCOS) are configured *declaratively*: operators do not
# SSH the nodes. Storage-path interface binding — iSCSI iface binding to specific
# NICs, NVMe-TCP host config, FC HBA selection — is therefore delivered as a
# MachineConfig (machineconfiguration.openshift.io/v1) whose embedded Ignition
# config writes the relevant files / systemd units to every node carrying the
# target machine-config-pool role label. The Machine Config Operator (MCO) rolls
# the change out, draining/rebooting nodes as needed.
#
# Everything below is pure string templating (no SDK / no I/O) so it is trivially
# unit-testable. The connector renders the MachineConfig and applies it with
# ``oc apply -f <tmpfile>``.
#
# TODO(doc-validate): confirm the exact on-RHCOS file paths / units against Everpure +
# RHCOS docs:
#   * iscsiadm iface files: this generator writes /etc/iscsi/ifaces/phif_<nic>
#     with ``iface.net_ifacename = <nic>`` (the iscsiadm iface-file format). Verify
#     whether RHCOS expects the iface under /etc/iscsi/ifaces/ and whether a
#     ``iscsiadm -m iface`` oneshot is preferred over a static file.
#   * NVMe-TCP: writes /etc/nvme/phif-connect.conf and a oneshot unit running
#     ``nvme connect``. Verify the canonical RHCOS path (/etc/nvme/hostnqn,
#     /etc/nvme/discovery.conf) and whether nvme-cli is present in RHCOS.
#   * FC: HBA selection is zoning-driven (switch-side) — no per-HBA node file is
#     written; the selection is recorded as a MachineConfig annotation only.
#   * Multipath (iSCSI + FC): writes the Everpure FlashArray /etc/multipath.conf and
#     enables multipathd.service (RHCOS ships device-mapper-multipath but does not
#     enable it). This reuses the same Everpure stanza as the Proxmox / HPE / XCP-ng
#     connectors. NVMe-oF uses native NVMe multipath, so it is skipped there.

# Ignition spec version embedded in a MachineConfig on modern OCP (4.x).
# TODO(doc-validate): confirm the Ignition version for the target OCP release
# (3.2.0 is used by OCP 4.6+; older clusters may need 2.2.0).
IGNITION_VERSION = "3.2.0"

# Mode 0644 expressed as a decimal int (Ignition file ``mode`` is decimal).
_MODE_0644 = 420

# Everpure FlashArray dm-multipath configuration for SCSI transports (iSCSI / FC).
# This is the SAME Everpure-recommended stanza the Proxmox / HPE VME / XCP-ng
# connectors install — ALUA prio, group_by_prio, find_multipaths no so every
# (non-blacklisted) Everpure LUN is auto-assembled, user_friendly_names no so the map
# is always named by its WWID. RHCOS ships device-mapper-multipath but does NOT
# enable multipathd and ships no /etc/multipath.conf, so — unlike the SSH-driven
# connectors that drop a /etc/multipath/conf.d/*.conf snippet beside an operator's
# existing file — we write the whole /etc/multipath.conf here and enable
# multipathd.service via the MachineConfig. (NVMe-oF uses native NVMe multipath.)
MULTIPATH_CONF_PATH = "/etc/multipath.conf"
MULTIPATH_CONF = """\
# Managed by PHIF — Everpure FlashArray multipath settings.
defaults {
    polling_interval 10
    find_multipaths no
    user_friendly_names no
}

devices {
    device {
        vendor "PURE"
        product "FlashArray"
        path_selector "service-time 0"
        path_grouping_policy group_by_prio
        prio alua
        hardware_handler "1 alua"
        path_checker tur
        failback immediate
        fast_io_fail_tmo 10
        dev_loss_tmo 60
        no_path_retry 0
    }
}
"""


def _ignition_inline_file(path: str, content: str, *, mode: int = _MODE_0644) -> str:
    """Render one Ignition ``storage.files[]`` entry using an inline data: URI.

    Ignition inline file contents are carried as a URL-encoded ``data:`` source.
    We percent-encode the content so arbitrary text (newlines, ``=``, spaces) is
    embedded safely and deterministically — keeping the renderer pure + testable.
    """
    from urllib.parse import quote

    encoded = quote(content, safe="")
    return (
        f"      - path: {path}\n"
        "        overwrite: true\n"
        f"        mode: {mode}\n"
        "        contents:\n"
        f"          source: data:text/plain;charset=utf-8,{encoded}\n"
    )


def _ignition_enable_unit(name: str) -> str:
    """Render an Ignition ``systemd.units[]`` entry that ENABLES an existing unit.

    With no ``contents`` key, Ignition enables the unit already shipped in the
    RHCOS image (e.g. ``multipathd.service``, ``iscsid.service``) rather than
    defining a new one — exactly how the Machine Config Operator turns on
    multipathd / the iSCSI daemon on the nodes.
    """
    return (
        f"      - name: {name}\n"
        "        enabled: true\n"
    )


def _ignition_contents_unit(name: str, body: str) -> str:
    """Render an Ignition ``systemd.units[]`` entry that DEFINES + enables a unit.

    ``body`` is the full unit file text; it is indented under ``contents: |``.
    """
    indented = "".join(
        f"            {line}\n" if line else "\n"
        for line in body.splitlines()
    )
    return (
        f"      - name: {name}\n"
        "        enabled: true\n"
        "        contents: |\n"
        f"{indented}"
    )


def iscsi_iface_file_content(nic: str) -> str:
    """Render the body of an iscsiadm iface file bound to a single NIC.

    Mirrors the on-disk format iscsiadm uses for /etc/iscsi/ifaces/<name>: the
    key line is ``iface.net_ifacename = <nic>`` which binds the iSCSI iface to a
    specific host network interface for iface-based multipath login.
    """
    return (
        "# Managed by PHIF — iSCSI iface binding\n"
        f"iface.iscsi_ifacename = phif_{nic}\n"
        "iface.transport_name = tcp\n"
        f"iface.net_ifacename = {nic}\n"
    )


def nvme_connect_conf_content(sources: list[str], options: str = "") -> str:
    """Render an /etc/nvme host-config style file describing NVMe-TCP sources.

    Each selected source interface/address becomes a ``host-traddr`` line; extra
    ``options`` (e.g. ``--nr-io-queues=8``) are recorded verbatim for the connect
    unit to pass to ``nvme connect``.
    """
    lines = ["# Managed by PHIF — NVMe-TCP source binding"]
    for src in sources:
        lines.append(f"host-traddr={src}")
    if options:
        lines.append(f"options={options}")
    return "\n".join(lines) + "\n"


def nvme_connect_unit_content(sources: list[str], options: str = "") -> str:
    """Render a oneshot systemd unit that runs ``nvme connect -w <source>``.

    One ``ExecStart`` per source binds the NVMe-TCP host interface (``-w``/
    ``--host-traddr``); ``options`` is appended verbatim. Discovery/target args
    are intentionally left to Portworx/CSI at attach time — this unit only pins the
    host-side source binding.
    """
    execs = []
    for src in sources:
        opt = f" {options}" if options else ""
        execs.append(f"ExecStart=/usr/sbin/nvme connect-all -w {src}{opt}")
    if not execs:
        execs.append("ExecStart=/bin/true")
    body = "\n".join(execs)
    return (
        "[Unit]\n"
        "Description=PHIF NVMe-TCP source binding\n"
        "After=network-online.target\n"
        "Wants=network-online.target\n"
        "[Service]\n"
        "Type=oneshot\n"
        "RemainAfterExit=yes\n"
        f"{body}\n"
        "[Install]\n"
        "WantedBy=multi-user.target\n"
    )


def interface_binding_machineconfig_yaml(
    *,
    name: str = "phif-interface-binding",
    role: str = "worker",
    iscsi_nics: list[str] | None = None,
    nvme_sources: list[str] | None = None,
    nvme_options: str = "",
    fc_hbas: list[str] | None = None,
) -> str:
    """Render a MachineConfig that binds storage-path interfaces on RHCOS nodes.

    The returned object is ``machineconfiguration.openshift.io/v1 / MachineConfig``
    labelled for the given machine-config-pool ``role`` ("worker"/"master"). Its
    embedded Ignition config:

    * **iSCSI** — writes one iscsiadm iface file per selected NIC at
      ``/etc/iscsi/ifaces/phif_<nic>`` with ``iface.net_ifacename=<nic>`` (binding
      iSCSI sessions to that host NIC for multi-interface IP storage), enables
      ``iscsid.service``, and writes the ARP-flux sysctl drop-in
      (``/etc/sysctl.d/99-phif-iscsi-arp.conf`` with ``arp_ignore=2`` /
      ``arp_announce=2`` per selected NIC) so the node doesn't answer/announce ARP
      on the wrong storage NIC — the same fix the SSH-driven Linux connectors
      apply via :func:`phif.connectors.iscsi_net.arp_flux_cmd`.
    * **Multipath (iSCSI + FC)** — writes the Everpure FlashArray
      ``/etc/multipath.conf`` (ALUA, ``group_by_prio``, ``find_multipaths no``) and
      enables ``multipathd.service`` so the multiple portal/HBA paths assemble into
      one WWID-named ``/dev/mapper`` device. Skipped for NVMe-only (native NVMe
      multipath).
    * **NVMe-TCP** — writes ``/etc/nvme/phif-connect.conf`` and ships a oneshot
      ``phif-nvme-connect.service`` that runs ``nvme connect`` per source with the
      supplied ``nvme_options``.
    * **FC** — HBA selection is zoning-driven (switch-side), so no per-HBA node
      file is written; the selected HBAs are recorded in a MachineConfig
      annotation for auditability, and the multipath.conf above applies.

    Plain string templating — no SDK, no I/O — so it is unit-testable.
    """
    iscsi_nics = iscsi_nics or []
    nvme_sources = nvme_sources or []
    fc_hbas = fc_hbas or []

    # dm-multipath covers the SCSI transports (iSCSI + FC); NVMe-oF uses native
    # NVMe multipath, so it needs no /etc/multipath.conf or multipathd.
    want_multipath = bool(iscsi_nics or fc_hbas)

    files = ""
    unit_entries: list[str] = []

    # iSCSI: one iscsiadm iface file per NIC binds sessions to that host NIC, and
    # iscsid must be running for login. (RHCOS ships iscsi-initiator-utils but
    # leaves iscsid.service disabled.)
    for nic in iscsi_nics:
        files += _ignition_inline_file(
            f"/etc/iscsi/ifaces/phif_{nic}", iscsi_iface_file_content(nic),
        )
    if iscsi_nics:
        unit_entries.append(_ignition_enable_unit("iscsid.service"))

    # ARP-flux prevention: when iSCSI sessions are bound to specific NICs (the
    # dual-port FlashArray layout where multiple storage NICs sit on the same
    # subnet), the node must set arp_ignore=2 / arp_announce=2 per storage NIC or
    # it answers/announces ARP on the wrong interface and sessions bind to the
    # wrong path. This is the SAME fix the SSH-driven Linux connectors apply via
    # iscsi_net.arp_flux_cmd; here we write its declarative drop-in
    # (/etc/sysctl.d/99-phif-iscsi-arp.conf) so systemd-sysctl applies it on the
    # node (the MCO reboots the node rolling the MachineConfig out, so it takes
    # effect). NVMe-TCP / FC don't need it (FC isn't IP; NVMe-TCP uses its own
    # host-traddr source binding), so it's gated on iscsi_nics.
    arp_body = arp_sysctl_content(iscsi_nics)
    if arp_body:
        files += _ignition_inline_file(ARP_SYSCTL_FILE, arp_body)

    # Multipath: install the Everpure FlashArray /etc/multipath.conf and enable
    # multipathd so the several portal/HBA paths group to one WWID. Needed for
    # iSCSI and FC; RHCOS does not enable multipathd by default.
    if want_multipath:
        files += _ignition_inline_file(MULTIPATH_CONF_PATH, MULTIPATH_CONF)
        unit_entries.append(_ignition_enable_unit("multipathd.service"))

    # NVMe-TCP: host source-binding config + a oneshot connect unit.
    if nvme_sources:
        files += _ignition_inline_file(
            "/etc/nvme/phif-connect.conf",
            nvme_connect_conf_content(nvme_sources, nvme_options),
        )
        unit_entries.append(_ignition_contents_unit(
            "phif-nvme-connect.service",
            nvme_connect_unit_content(nvme_sources, nvme_options),
        ))

    units = ""
    if unit_entries:
        units = "    systemd:\n      units:\n" + "".join(unit_entries)

    # FC HBA selection is recorded as an annotation only (zoning-driven, no-op on
    # the node). Empty selections still yield a valid (no-op file) MachineConfig.
    fc_annotation = ""
    if fc_hbas:
        fc_annotation = (
            "    phif.purestorage.com/fc-hbas: \"" + ",".join(fc_hbas) + "\"\n"
        )

    storage_block = ""
    if files:
        storage_block = "    storage:\n      files:\n" + files

    return (
        "apiVersion: machineconfiguration.openshift.io/v1\n"
        "kind: MachineConfig\n"
        "metadata:\n"
        f"  name: {name}\n"
        "  labels:\n"
        f"    machineconfiguration.openshift.io/role: {role}\n"
        f"  annotations:\n"
        "    phif.purestorage.com/managed: \"true\"\n"
        f"{fc_annotation}"
        "spec:\n"
        "  config:\n"
        "    ignition:\n"
        f"      version: {IGNITION_VERSION}\n"
        f"{storage_block}"
        f"{units}"
    )


# --------------------------------------------------------------------------- #
# KubeVirt (OpenShift Virtualization) — VM + migration disk PVC
# --------------------------------------------------------------------------- #
# Migration to/from OpenShift treats each KubeVirt VM disk as a px-csi (FADA) PVC
# whose backing FlashArray volume is named ``px_<clusterUid[:8]>-<pvName>`` (the
# Portworx FADA convention). The migration disk PVC is created with the FADA
# StorageClass so its data lives 1:1 on a FlashArray volume the migration
# orchestrator can copy into / out of (exactly like a Cinder volume on OpenStack).
#
# These are pure string templating (no SDK / no I/O) so they are unit-testable.

# Default StorageClass for migration-created disk PVCs (px-csi FADA, 1 PVC = 1 FA
# volume). Overridable by the connector.
MIGRATION_STORAGE_CLASS_DEFAULT = "px-fa-direct-access"

# How a px-csi FADA PVC's backing FlashArray volume name is derived. The Portworx
# operator names FADA volumes ``px_<first 8 of the StorageCluster clusterUid>-<the
# PV name>`` (verified on hardware). The PV name is ``pvc-<uid>``.
def fada_fa_volume_name(cluster_uid: str, pv_name: str) -> str:
    """Return the FlashArray volume name px-csi (FADA) creates for a bound PVC.

    ``cluster_uid`` is the Portworx StorageCluster ``status.clusterUid``; only its
    first 8 characters are used. ``pv_name`` is the PVC's ``spec.volumeName``
    (``pvc-<uid>``). Example: ``px_da86dbd5-pvc-b5c2c73a-...``.
    """
    return f"px_{(cluster_uid or '')[:8]}-{pv_name}"


def migration_pvc_yaml(*, name: str, namespace: str, size: str,
                       storage_class: str = MIGRATION_STORAGE_CLASS_DEFAULT) -> str:
    """Render a RWX **Block** PVC for a migration disk (1 PVC = 1 FlashArray volume).

    * ``volumeMode: Block`` is REQUIRED: the migration copy-with-overwrite fills the
      backing FlashArray volume with the source VM's whole-disk image (GPT + FS). A
      Filesystem-mode PVC makes px-csi try to mkfs/mount the device at
      NodeStageVolume and fail ("device already formatted with gpt filesystem"), so
      the VMI never schedules. Block hands KubeVirt the raw device.
    * ``accessModes: ReadWriteMany`` (RWX): KubeVirt **LiveMigration / HA** and
      node mobility require the disk to be attachable on more than one node at once.
      px-csi FADA supports RWX block on the FlashArray. (RWO would pin the VM to a
      single node and block live migration.)
    """
    return (
        "apiVersion: v1\n"
        "kind: PersistentVolumeClaim\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"  namespace: {namespace}\n"
        "  labels:\n"
        "    phif.purestorage.com/migration: \"true\"\n"
        "spec:\n"
        "  accessModes:\n    - ReadWriteMany\n"
        "  volumeMode: Block\n"
        f"  storageClassName: {storage_class}\n"
        "  resources:\n    requests:\n"
        f"      storage: {size}\n"
    )


def _vm_disk_entry(name: str, bus: str, boot_order: int | None) -> str:
    # bootOrder is a field of the DISK ENTRY (sibling of name/disk at 14 spaces),
    # NOT a field of disk: — KubeVirt strict-decode rejects disk.bootOrder.
    bo = f"              bootOrder: {boot_order}\n" if boot_order else ""
    return (
        f"            - name: {name}\n"
        f"              disk:\n                bus: {bus}\n"
        f"{bo}"
    )


def _vm_volume_entry(name: str, claim: str) -> str:
    return (
        f"        - name: {name}\n"
        f"          persistentVolumeClaim:\n            claimName: {claim}\n"
    )


def _vm_iface_entry(name: str, mac: str, *, pod: bool) -> str:
    binding = "              masquerade: {}\n" if pod else "              bridge: {}\n"
    mac_line = f"              macAddress: \"{mac}\"\n" if mac else ""
    return f"            - name: {name}\n{binding}{mac_line}"


def _vm_network_entry(name: str, dest: str, *, pod: bool) -> str:
    if pod:
        return f"        - name: {name}\n          pod: {{}}\n"
    # dest is a Multus NetworkAttachmentDefinition, "<namespace>/<nad>" or "<nad>".
    return f"        - name: {name}\n          multus:\n            networkName: {dest}\n"


def virtual_machine_yaml(*, name: str, namespace: str, vcpus: int,
                         memory_bytes: int, firmware: str,
                         disks: list[dict], nics: list[dict],
                         running: bool = False, secure_boot: bool = False) -> str:
    """Render a KubeVirt ``VirtualMachine`` CR for migration.

    ``disks`` items: ``{name, claim, bus, boot_order}``. ``nics`` items:
    ``{name, mac, dest, pod}`` (``pod`` True → pod network / masquerade, else a
    Multus network named ``dest``). vCPU is set inline as ``domain.cpu.cores`` and
    RAM as ``domain.memory.guest`` (no instancetype dependency).

    ``firmware == "uefi"`` adds ``domain.firmware.bootloader.efi``. KubeVirt
    defaults EFI ``secureBoot: true``, which REQUIRES the SMM feature — so we set
    ``secureBoot: false`` for plain UEFI, and only enable SecureBoot (+ the
    required ``features.smm``) when ``secure_boot`` is requested. Created stopped
    (``running: false``) by default — migration starts it after the data copy.
    """
    mem_mib = max(1, int(memory_bytes) // (1024 * 1024)) if memory_bytes else 512
    efi = ""
    features = ""
    if (firmware or "").lower() == "uefi":
        sb = "true" if secure_boot else "false"
        efi = ("        firmware:\n          bootloader:\n            efi:\n"
               f"              secureBoot: {sb}\n")
        if secure_boot:  # SecureBoot needs SMM enabled
            features = "        features:\n          smm:\n            enabled: true\n"
    disk_block = "".join(
        _vm_disk_entry(d["name"], d.get("bus", "virtio"), d.get("boot_order"))
        for d in disks) or ""
    vol_block = "".join(
        _vm_volume_entry(d["name"], d["claim"]) for d in disks) or ""
    iface_block = "".join(
        _vm_iface_entry(n["name"], n.get("mac", ""), pod=bool(n.get("pod")))
        for n in nics) or ""
    net_block = "".join(
        _vm_network_entry(n["name"], n.get("dest", ""), pod=bool(n.get("pod")))
        for n in nics) or ""
    return (
        "apiVersion: kubevirt.io/v1\n"
        "kind: VirtualMachine\n"
        "metadata:\n"
        f"  name: {name}\n"
        f"  namespace: {namespace}\n"
        "  labels:\n"
        "    phif.purestorage.com/migrated: \"true\"\n"
        "spec:\n"
        f"  running: {str(bool(running)).lower()}\n"
        "  template:\n"
        "    metadata:\n"
        "      labels:\n"
        f"        kubevirt.io/domain: {name}\n"
        "    spec:\n"
        "      domain:\n"
        "        cpu:\n"
        f"          cores: {max(1, int(vcpus))}\n"
        "        memory:\n"
        f"          guest: {mem_mib}Mi\n"
        f"{efi}"
        f"{features}"
        "        devices:\n"
        "          disks:\n"
        f"{disk_block}"
        "          interfaces:\n"
        f"{iface_block}"
        "      networks:\n"
        f"{net_block}"
        "      volumes:\n"
        f"{vol_block}"
    )
