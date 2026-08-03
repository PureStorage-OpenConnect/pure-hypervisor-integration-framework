"""Unit tests for the OpenShift (Portworx / px-csi) connector.

All tests run against the mock FlashArray + mock JobRunner provided by the
``make_context`` fixture (PHIF_MOCK_MODE=1), so nothing touches a real cluster
or array.
"""

from __future__ import annotations

import dataclasses

import pytest

from phif.connectors.base import Capability, FieldType, OpResult, Protocol
from phif.connectors.openshift import OpenShiftConnector, manifests

KUBECONFIG = (
    "apiVersion: v1\nclusters:\n- cluster:\n    server: https://api.test:6443\n"
    "  name: test\ncurrent-context: test\n"
)


def _ctx(make_context, *, dry_run=False, with_array=True, secrets=None,
         array_token="tok-123"):
    # The FlashArray endpoint + token are derived from the associated array, not
    # from operator-entered target fields, so the connection carries neither
    # array_endpoint nor api_token. The array's original token is supplied via
    # ctx.array_token (what resolve_token() returns).
    ctx = make_context(
        connector_key="openshift",
        connection={"namespace": "portworx"},
        secrets=secrets if secrets is not None else {"kubeconfig": KUBECONFIG},
        with_array=with_array,
    )
    return dataclasses.replace(ctx, dry_run=dry_run, array_token=array_token)


# --------------------------------------------------------------------------- #
# metadata / descriptor
# --------------------------------------------------------------------------- #
def test_descriptor_metadata():
    d = OpenShiftConnector.descriptor()
    assert d["key"] == "openshift"
    assert d["name"] == "Red Hat OpenShift (Portworx)"
    assert d["maturity"] == "ga"


def test_capabilities_declared():
    caps = OpenShiftConnector.capabilities()
    expected = {
        Capability.CONNECT, Capability.DEPLOY_PLUGIN, Capability.CONFIGURE,
        Capability.PROVISION_VOLUME, Capability.SNAPSHOT, Capability.CLONE,
        Capability.RESIZE, Capability.ROTATE_CREDENTIALS, Capability.UPGRADE,
        Capability.HEALTH, Capability.REMOVE,
        # Migration (KubeVirt VMs <-> FlashArray)
        Capability.VM_INVENTORY, Capability.VM_LIFECYCLE, Capability.MIGRATE,
    }
    assert caps == expected


def test_supported_protocols():
    assert OpenShiftConnector.SUPPORTED_PROTOCOLS == {
        Protocol.ISCSI, Protocol.FC, Protocol.NVME_TCP, Protocol.NFS}


def test_target_schema_has_kubeconfig_secret():
    fields = {f.name: f for f in OpenShiftConnector.target_schema()}
    assert fields["kubeconfig"].type == FieldType.TEXT
    # The retired PSO driver is gone — there is no driver choice; Portworx only.
    assert "driver" not in fields
    assert fields["namespace"].default == "portworx"


def test_target_schema_omits_array_endpoint_and_api_token():
    # The FlashArray endpoint + token come from the associated array, not from
    # operator-entered form fields — so they must NOT be in the target schema.
    fields = {f.name for f in OpenShiftConnector.target_schema()}
    assert "array_endpoint" not in fields
    assert "api_token" not in fields
    # the OpenShift-side fields are still present (driver is gone — Portworx only)
    assert {"kubeconfig", "namespace", "protocol"} <= fields


def test_target_schema_supports_username_password_auth():
    fields = {f.name: f for f in OpenShiftConnector.target_schema()}
    # kubeconfig is now optional — kubeconfig OR (api_url + username + password)
    assert fields["kubeconfig"].required is False
    assert fields["api_url"].required is False
    assert fields["username"].type == FieldType.STRING
    # password is a vault-stored secret, masked in the UI + job logs
    assert fields["password"].type == FieldType.SECRET
    assert fields["insecure_skip_tls_verify"].type == FieldType.BOOL


def test_action_schemas_cover_every_capability():
    actions = OpenShiftConnector.action_schemas()
    action_caps = {a.capability for a in actions}
    declared = OpenShiftConnector.CAPABILITIES
    # Migration caps (VM_INVENTORY/VM_LIFECYCLE/MIGRATE) are driven by the
    # migration framework, not day-2 ActionSpecs, so they have no action entries.
    migration_caps = {Capability.VM_INVENTORY, Capability.VM_LIFECYCLE,
                      Capability.MIGRATE}
    assert (declared - migration_caps) <= action_caps
    # every action capability is actually declared
    assert action_caps <= declared
    # action ids are unique
    ids = [a.id for a in actions]
    assert len(ids) == len(set(ids))


# --------------------------------------------------------------------------- #
# manifest helpers (pure functions)
# --------------------------------------------------------------------------- #
def test_storage_class_yaml_portworx_default():
    # Portworx is the only driver now; provisioner_for defaults to pxd.portworx.com.
    y = manifests.storage_class_yaml(name="pure-block", driver="portworx",
                                     backend="pure_block")
    assert "kind: StorageClass" in y
    assert "provisioner: pxd.portworx.com" in y
    assert "allowVolumeExpansion: true" in y
    assert "backend: pure_block" in y


def test_storage_class_yaml_portworx_provisioner():
    y = manifests.storage_class_yaml(name="px", driver="portworx",
                                     backend="pure_block")
    assert "provisioner: pxd.portworx.com" in y
    # Portworx FADA StorageClass carries the backend param (pure_block) so PVCs
    # provision directly on the FlashArray.
    assert "backend: pure_block" in y


def test_storage_class_default_annotation():
    y = manifests.storage_class_yaml(name="d", driver="portworx", is_default=True)
    assert "storageclass.kubernetes.io/is-default-class: \"true\"" in y


def test_volume_snapshot_class_yaml():
    y = manifests.volume_snapshot_class_yaml(name="snc", driver="portworx")
    assert "kind: VolumeSnapshotClass" in y
    assert "driver: pxd.portworx.com" in y
    assert "deletionPolicy: Delete" in y


def test_pvc_yaml_plain():
    y = manifests.pvc_yaml(name="data", namespace="apps", size="10Gi",
                           storage_class="pure-block")
    assert "kind: PersistentVolumeClaim" in y
    assert "storage: 10Gi" in y
    assert "dataSource" not in y


def test_pvc_yaml_clone_from_pvc():
    y = manifests.pvc_yaml(name="copy", namespace="apps", size="10Gi",
                           storage_class="pure-block", data_source="orig",
                           data_source_kind="PersistentVolumeClaim")
    assert "dataSource:" in y
    assert "name: orig" in y
    assert "kind: PersistentVolumeClaim" in y


def test_pvc_yaml_clone_from_snapshot_has_apigroup():
    y = manifests.pvc_yaml(name="r", namespace="apps", size="10Gi",
                           storage_class="pure-block", data_source="snap1",
                           data_source_kind="VolumeSnapshot")
    assert "apiGroup: snapshot.storage.k8s.io" in y
    assert "kind: VolumeSnapshot" in y


def test_volume_snapshot_yaml():
    y = manifests.volume_snapshot_yaml(name="s1", namespace="apps",
                                       source_pvc="data", snapshot_class="snc")
    assert "kind: VolumeSnapshot" in y
    assert "persistentVolumeClaimName: data" in y


def test_target_schema_protocol_enum_includes_fc():
    fields = {f.name: f for f in OpenShiftConnector.target_schema()}
    assert fields["protocol"].type == FieldType.ENUM
    assert "fc" in fields["protocol"].options
    assert fields["protocol"].default == "iscsi"
    # node_wwns field exists for FC host registration
    assert "node_wwns" in fields


# --------------------------------------------------------------------------- #
# Fibre Channel: optional WWN host registration on the Portworx deploy
# --------------------------------------------------------------------------- #
def _fc_ctx(make_context, *, dry_run=False, with_array=True, node_wwns="21:00:00:aa,21:00:00:bb"):
    # No array_endpoint / api_token in the connection — they're derived from the
    # associated array (endpoint) and ctx.array_token (token).
    conn = {"namespace": "portworx", "protocol": "fc"}
    if node_wwns is not None:
        conn["node_wwns"] = node_wwns
    ctx = make_context(
        connector_key="openshift",
        connection=conn,
        secrets={"kubeconfig": KUBECONFIG},
        with_array=with_array,
    )
    return dataclasses.replace(ctx, dry_run=dry_run, array_token="tok-123")


@pytest.mark.asyncio
async def test_deploy_fc_registers_hosts_by_wwn(make_context, mock_array):
    conn = OpenShiftConnector(_fc_ctx(make_context))
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    # a host group + host were created on the array, host carries the node WWNs
    assert "c1-ocp" in mock_array.host_groups
    host = mock_array.hosts["c1-ocp-h1"]
    assert host["wwns"] == ["21:00:00:aa", "21:00:00:bb"]
    # FC does not register iSCSI IQNs
    assert host["iqns"] == []


@pytest.mark.asyncio
async def test_deploy_fc_no_wwns_skips_registration(make_context, mock_array, captured_logs):
    # FC deploy WITHOUT node_wwns must succeed (not fail for missing WWNs) and
    # cleanly skip manual registration, explaining Portworx auto-registers.
    conn = OpenShiftConnector(_fc_ctx(make_context, node_wwns=""))
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    # no manual host / host group created on the array
    assert mock_array.host_groups == {}
    assert mock_array.hosts == {}
    logs = "\n".join(captured_logs).lower()
    assert "no node_wwns" in logs
    assert "skipping" in logs
    # messaging explains Portworx auto-registers the worker nodes
    assert "auto-register" in logs
    assert any("portworx" in l.lower() for l in captured_logs)


@pytest.mark.asyncio
async def test_deploy_fc_no_wwns_field_absent_succeeds(make_context, mock_array):
    # node_wwns entirely absent from the target (not just empty) still succeeds.
    conn = OpenShiftConnector(_fc_ctx(make_context, node_wwns=None))
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    assert mock_array.host_groups == {}
    assert mock_array.hosts == {}


@pytest.mark.asyncio
async def test_deploy_fc_with_wwns_is_idempotent(make_context, mock_array):
    # Re-running deploy WITH node_wwns must be safe (idempotent + additive) and
    # converge to the same single host / host group with the same WWN set.
    conn = OpenShiftConnector(_fc_ctx(make_context))
    assert (await conn.deploy_integration(cluster_id="c1")).success
    assert (await conn.deploy_integration(cluster_id="c1")).success
    assert list(mock_array.host_groups) == ["c1-ocp"]
    host = mock_array.hosts["c1-ocp-h1"]
    # WWNs not duplicated on re-run
    assert host["wwns"] == ["21:00:00:aa", "21:00:00:bb"]


@pytest.mark.asyncio
async def test_deploy_fc_dry_run_no_array_changes(make_context, mock_array, captured_logs):
    conn = OpenShiftConnector(_fc_ctx(make_context, dry_run=True))
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    assert res.artifacts["driver"] == "portworx"
    # dry-run makes no real array host registration
    assert mock_array.host_groups == {}
    assert not any("apply -f" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_deploy_iscsi_no_fc_registration(make_context, mock_array):
    # default protocol (iscsi) path must not do FC host registration
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    assert res.artifacts["driver"] == "portworx"
    assert mock_array.host_groups == {}


# --------------------------------------------------------------------------- #
# Endpoint + token derived from the associated array (no operator fields)
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_deploy_derives_endpoint_and_token_from_array(make_context, mock_array,
                                                            captured_logs):
    # No array_endpoint / api_token are supplied (neither in the connection nor
    # as method kwargs); the connector must derive the endpoint from
    # ctx.array.endpoint and the token from ctx.resolve_token() (array_token) to
    # build the px-pure-secret.
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    # endpoint came from the associated array (MockFlashArrayClient.endpoint)
    assert any(mock_array.endpoint in l for l in captured_logs)
    # the array's existing token was reused, not minted
    assert "portworx" not in mock_array.api_tokens
    # the rendered px-pure-secret would carry the derived endpoint + token
    y = manifests.portworx_pure_secret_yaml(array_endpoint=mock_array.endpoint,
                                            api_token="tok-123", namespace="portworx")
    assert mock_array.endpoint in y
    assert "tok-123" in y


@pytest.mark.asyncio
async def test_deploy_no_array_fails_clearly(make_context):
    # With no FlashArray associated there's no endpoint/token to derive — deploy
    # must fail with a clear message rather than silently producing empty values.
    conn = OpenShiftConnector(_ctx(make_context, with_array=False, array_token=None))
    res = await conn.deploy_integration(cluster_id="c1")
    assert not res.success
    assert "no flasharray" in res.message.lower()


@pytest.mark.asyncio
async def test_deploy_explicit_overrides_win(make_context, mock_array, captured_logs):
    # An explicit endpoint/token kwarg still overrides the array-derived values.
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.deploy_integration(cluster_id="c1",
                                        array_endpoint="explicit.endpoint",
                                        api_token="explicit-tok")
    assert res.success
    assert any("explicit.endpoint" in l for l in captured_logs)
    assert "portworx" not in mock_array.api_tokens


@pytest.mark.asyncio
async def test_deploy_applies_central_spec_url(make_context, captured_logs):
    # A Portworx Central spec URL: PHIF installs the operator + px-pure-secret,
    # then applies the spec verbatim via `oc apply -f <url>` (no generated SC).
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.deploy_integration(
        cluster_id="c1", px_spec_url="https://install.portworx.com/3.1?abc=1")
    assert res.success
    assert "URL" in res.artifacts["spec_source"]
    logs = "\n".join(captured_logs)
    assert "apply -f https://install.portworx.com/3.1?abc=1" in logs
    # the generated StorageCluster was NOT used
    assert "PHIF-generated StorageCluster" not in logs


@pytest.mark.asyncio
async def test_deploy_applies_central_spec_yaml(make_context, captured_logs):
    # Pasted Central StorageCluster YAML is applied verbatim and wins over the URL
    # for the StorageCluster source (spec_source reflects the YAML).
    spec = ("apiVersion: core.libopenstorage.org/v1\nkind: StorageCluster\n"
            "metadata:\n  name: px-from-central\n")
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.deploy_integration(cluster_id="c1", px_spec_yaml=spec)
    assert res.success
    assert "pasted YAML" in res.artifacts["spec_source"]
    logs = "\n".join(captured_logs)
    # The StorageCluster came from the pasted YAML (applied from a temp manifest).
    assert "pasted Portworx Central spec" in logs
    assert "PHIF-generated StorageCluster" not in logs


# --------------------------------------------------------------------------- #
# operations (mock runner + mock array)
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_validate_connection(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.validate_connection()
    assert res.success
    assert res.data["driver"] == "portworx"
    # kubectl version + oc whoami were streamed
    assert any("kubectl" in line and "version" in line for line in captured_logs)
    assert any("oc" in line and "whoami" in line for line in captured_logs)


@pytest.mark.asyncio
async def test_validate_connection_no_kubeconfig_raises(make_context):
    from phif.connectors.base import ConnectionValidationError

    conn = OpenShiftConnector(_ctx(make_context, secrets={}))
    with pytest.raises(ConnectionValidationError):
        await conn.validate_connection()


@pytest.mark.asyncio
async def test_oc_login_used_when_no_kubeconfig(make_context, captured_logs):
    ctx = make_context(
        connector_key="openshift",
        connection={"namespace": "portworx",
                    "api_url": "https://api.ocp.example.com:6443",
                    "username": "kubeadmin",
                    "insecure_skip_tls_verify": True},
        secrets={"password": "s3cr3t-pw"},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    res = await conn.validate_connection()
    assert res.success
    logs = "\n".join(captured_logs)
    # oc login was invoked with the API URL + user + tls-skip ...
    assert "oc login https://api.ocp.example.com:6443" in logs
    assert "--username kubeadmin" in logs
    assert "--insecure-skip-tls-verify=true" in logs
    # ... and the password is redacted, never printed in the job log
    assert "s3cr3t-pw" not in logs
    assert "--password ***" in logs


def test_service_account_kubeconfig_yaml_with_ca():
    kc = manifests.service_account_kubeconfig_yaml(
        api_url="https://api.ocp:6443", token="eyJ.tok.en", ca_data="Q0FEQVRB")
    assert "server: https://api.ocp:6443" in kc
    assert "token: eyJ.tok.en" in kc
    assert "certificate-authority-data: Q0FEQVRB" in kc
    assert "insecure-skip-tls-verify" not in kc


def test_service_account_kubeconfig_yaml_insecure_when_no_ca():
    kc = manifests.service_account_kubeconfig_yaml(
        api_url="https://api.ocp:6443", token="t", insecure=True)
    assert "insecure-skip-tls-verify: true" in kc
    assert "certificate-authority-data" not in kc


def test_service_account_token_secret_yaml():
    y = manifests.service_account_token_secret_yaml()
    assert "type: kubernetes.io/service-account-token" in y
    assert f"kubernetes.io/service-account.name: {manifests.SA_NAME}" in y


def test_service_account_clusterrole_is_scoped_not_admin():
    y = manifests.service_account_clusterrole_yaml()
    assert "kind: ClusterRole" in y
    assert f"name: {manifests.SA_CLUSTERROLE}" in y
    # Scoped to the resources PHIF touches — never a cluster-admin wildcard.
    assert "resources: [\"*\"]" not in y
    assert "verbs: [\"*\"]" not in y
    for res in ("storageclasses", "volumesnapshots", "persistentvolumeclaims",
                "machineconfigs", "customresourcedefinitions", "daemonsets"):
        assert res in y, res
    # bind + escalate are present so the Portworx operator's RBAC can be installed.
    assert "bind" in y and "escalate" in y


class _StubRunner:
    """Minimal runner that scripts oc-command outputs for SA-token tests."""

    def __init__(self, token="eyJ.sa.token", ca="Q0FEQVRB"):
        self.mock = False
        self.dry_run = False
        self.token = token
        self.ca = ca
        self.commands: list[str] = []

    async def log(self, _line):
        return None

    async def run_local(self, command, *, check=True, redact=None, log_output=True):
        self.commands.append(command)
        if "create token" in command:
            return self.token
        if "certificate-authority-data" in command:
            return self.ca
        return ""


@pytest.mark.asyncio
async def test_mint_sa_kubeconfig_builds_token_kubeconfig(make_context):
    ctx = make_context(
        connector_key="openshift",
        connection={"api_url": "https://api.ocp.example.com:6443"},
        secrets={"username": "kubeadmin", "password": "pw"},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    conn.ctx.runner = _StubRunner()
    kc = await conn._mint_sa_kubeconfig("/tmp/session-kc")
    assert kc is not None
    # Built a token-based kubeconfig pointing at the API URL with the SA token.
    assert "server: https://api.ocp.example.com:6443" in kc
    assert "token: eyJ.sa.token" in kc
    assert "certificate-authority-data: Q0FEQVRB" in kc
    # Created the SA + bound it to the scoped ClusterRole (NOT cluster-admin).
    joined = "\n".join(conn.ctx.runner.commands)
    assert "create serviceaccount phif-operator" in joined
    assert "clusterrolebinding phif-operator" in joined
    assert "--clusterrole=phif-operator" in joined
    assert "cluster-admin" not in joined


@pytest.mark.asyncio
async def test_mint_sa_kubeconfig_none_without_api_url(make_context):
    ctx = make_context(
        connector_key="openshift",
        connection={},  # no api_url
        secrets={"username": "kubeadmin", "password": "pw"},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    conn.ctx.runner = _StubRunner()
    assert await conn._mint_sa_kubeconfig("/tmp/session-kc") is None


@pytest.mark.asyncio
async def test_mint_sa_kubeconfig_none_when_token_blank(make_context):
    ctx = make_context(
        connector_key="openshift",
        connection={"api_url": "https://api.ocp:6443"},
        secrets={"username": "kubeadmin", "password": "pw"},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    # Empty token from create-token AND the secret fallback -> no kubeconfig.
    conn.ctx.runner = _StubRunner(token="")
    assert await conn._mint_sa_kubeconfig("/tmp/session-kc") is None


@pytest.mark.asyncio
async def test_validate_with_login_attempts_sa_but_degrades_in_mock(make_context,
                                                                    captured_logs):
    # Mock runner returns "" for create-token, so SA promotion can't complete;
    # validate must still succeed and NOT include a service_account_kubeconfig.
    ctx = make_context(
        connector_key="openshift",
        connection={"namespace": "portworx",
                    "api_url": "https://api.ocp:6443", "username": "kubeadmin"},
        secrets={"password": "pw"},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    res = await conn.validate_connection()
    assert res.success
    assert "service_account_kubeconfig" not in res.data
    logs = "\n".join(captured_logs)
    assert "[sa]" in logs  # it attempted the promotion


@pytest.mark.asyncio
async def test_no_kubeconfig_and_incomplete_creds_raises(make_context):
    from phif.connectors.base import ConnectionValidationError

    ctx = make_context(
        connector_key="openshift",
        connection={"namespace": "portworx", "username": "kubeadmin"},  # no api_url/pw
        secrets={},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    with pytest.raises(ConnectionValidationError):
        await conn.validate_connection()


@pytest.mark.asyncio
async def test_deploy_mints_token_when_absent(make_context, mock_array):
    ctx = make_context(
        connector_key="openshift",
        connection={"namespace": "portworx"},
        secrets={"kubeconfig": KUBECONFIG},  # no api_token -> should mint
    )
    conn = OpenShiftConnector(ctx)
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    assert "portworx" in mock_array.api_tokens


@pytest.mark.asyncio
async def test_deploy_dry_run_makes_no_changes(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context, dry_run=True))
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    assert any("dry-run" in line.lower() for line in captured_logs)
    # in dry-run no helm install command is actually executed via run_local
    assert not any("[local] $ helm install" in line for line in captured_logs)


@pytest.mark.asyncio
async def test_deploy_portworx_installs_operator_and_storagecluster(make_context,
                                                                    captured_logs):
    ctx = make_context(
        connector_key="openshift",
        connection={"driver": "portworx", "namespace": "px"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    res = await OpenShiftConnector(ctx).deploy_integration(cluster_id="c1")
    assert res.success
    assert res.artifacts["driver"] == "portworx"
    assert res.artifacts["namespace"] == "px"
    logs = "\n".join(captured_logs).lower()
    # No spec/operator URL -> OLM operator fallback + StorageCluster, never helm.
    assert "portworx operator via olm" in logs
    assert "storagecluster" in logs
    assert "helm install" not in logs


def test_derive_pxoperator_url():
    spec = ("https://install.portworx.com/26.2?oem=px-csi&operator=true&ce=pure"
            "&csi=true&stork=false&kbver=1.31.0&ns=portworx&osft=true"
            "&c=px-cluster-abc&pureSanType=ISCSI&tel=false")
    url = OpenShiftConnector._derive_pxoperator_url(spec)
    # Same base + version, comp=pxoperator, carries kbver/ns, OpenShift flag.
    assert url.startswith("https://install.portworx.com/26.2?")
    assert "comp=pxoperator" in url
    assert "kbver=1.31.0" in url
    assert "ns=portworx" in url
    assert "osft=true" in url
    # StorageCluster-only params are dropped.
    assert "oem=px-csi" not in url and "c=px-cluster-abc" not in url
    # Non-Portworx / empty inputs derive nothing.
    assert OpenShiftConnector._derive_pxoperator_url("") == ""
    assert OpenShiftConnector._derive_pxoperator_url("https://example.com/x") == ""


@pytest.mark.asyncio
async def test_deploy_portworx_installs_operator_from_manifest_url(make_context, captured_logs):
    # With a Central spec URL, the operator is installed from the derived
    # comp=pxoperator MANIFEST (oc apply -f <url>), NOT via OLM — so it works on a
    # cluster whose OperatorHub lacks the Portworx Operator.
    ctx = make_context(
        connector_key="openshift",
        connection={"namespace": "portworx"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    spec = ("https://install.portworx.com/26.2?oem=px-csi&operator=true&ce=pure"
            "&kbver=1.31.0&ns=portworx&c=px-cluster-abc")
    res = await OpenShiftConnector(ctx).deploy_integration(cluster_id="c1", px_spec_url=spec)
    assert res.success
    logs = "\n".join(captured_logs)
    assert "Installing the Portworx Operator from manifest" in logs
    assert "comp=pxoperator" in logs
    # The OLM Subscription path is NOT used.
    assert "via OLM" not in logs


class _PxPresentRunner:
    """Real-cluster (mock=False) runner that reports an existing Portworx install."""

    def __init__(self, csidriver=True, storagecluster="portworx/px-cluster-abc",
                 storageclasses=(("flasharray-fada", True),
                                 ("px-rwx-block-kubevirt", False))):
        self.mock = False
        self.dry_run = False
        self.csidriver = csidriver
        self.storagecluster = storagecluster
        self.storageclasses = storageclasses
        self.commands = []

    async def log(self, _):
        return None

    async def run_local(self, command, *, check=True, redact=None, log_output=True):
        self.commands.append(command)
        # The fresh-install path polls for the StorageCluster CRD; report it
        # present so _wait_for_crd returns immediately (no 300s poll in tests).
        if "get crd storageclusters.core.libopenstorage.org" in command:
            return "customresourcedefinition.apiextensions.k8s.io/storageclusters.core.libopenstorage.org"
        if "get csidriver pxd.portworx.com" in command:
            if self.csidriver:
                return "csidriver.storage.k8s.io/pxd.portworx.com"
            # Realistic NotFound: the error text echoes the resource name back
            # (contains "pxd.portworx.com") — the regression that fooled the old
            # substring check into thinking Portworx was installed.
            return ('Error from server (NotFound): csidrivers.storage.k8s.io '
                    '"pxd.portworx.com" not found')
        if "get storagecluster -A -o json" in command:
            import json
            if not self.storagecluster:
                return json.dumps({"items": []})
            ns, name = self.storagecluster.split("/")
            return json.dumps({"items": [{"metadata": {"namespace": ns, "name": name}}]})
        if "get sc -o json" in command:
            import json
            items = [{"provisioner": "pxd.portworx.com",
                      "metadata": {"name": n, "annotations":
                          {"storageclass.kubernetes.io/is-default-class": "true"} if d else {}}}
                     for n, d in self.storageclasses]
            return json.dumps({"items": items})
        return ""


@pytest.mark.asyncio
async def test_deploy_portworx_adopts_existing_install(make_context, captured_logs):
    ctx = make_context(
        connector_key="openshift",
        connection={"driver": "portworx", "namespace": "portworx-csi"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    conn.ctx.runner = _PxPresentRunner()
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    assert res.artifacts.get("adopted") is True
    assert res.artifacts.get("storagecluster") == "portworx/px-cluster-abc"
    logs = "\n".join(captured_logs)
    assert "Existing Portworx detected" in logs
    # Adoption must NOT create a second StorageCluster / Subscription.
    joined = "\n".join(conn.ctx.runner.commands)
    assert "apply" not in joined


@pytest.mark.asyncio
async def test_deploy_portworx_notfound_error_not_adopted(make_context, captured_logs):
    # Regression: `get csidriver pxd.portworx.com` returning a NotFound error
    # (whose text contains "pxd.portworx.com") must NOT be read as "present" — the
    # deploy must proceed to actually install the operator + StorageCluster.
    ctx = make_context(
        connector_key="openshift",
        connection={"namespace": "portworx"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    conn.ctx.runner = _PxPresentRunner(csidriver=False)
    res = await conn.deploy_integration(cluster_id="c1")
    assert res.success
    assert res.artifacts.get("adopted") is not True
    assert "Existing Portworx detected" not in "\n".join(captured_logs)
    # It proceeded to install: operator/secret/StorageCluster were applied.
    assert any("apply -f" in c for c in conn.ctx.runner.commands)


@pytest.mark.asyncio
async def test_configure_portworx_reuses_existing_storageclasses(make_context, captured_logs):
    ctx = make_context(
        connector_key="openshift",
        connection={"driver": "portworx", "namespace": "portworx-csi"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    conn.ctx.runner = _PxPresentRunner()
    res = await conn.configure(storage_class="pure-block")
    assert res.success
    assert res.artifacts.get("reused") is True
    assert "flasharray-fada" in res.artifacts["storage_classes"]
    assert res.artifacts["recommended_storage_class"] == "flasharray-fada"
    # It must not have applied a competing pure-block StorageClass.
    assert not any("apply -f" in c for c in conn.ctx.runner.commands)


@pytest.mark.asyncio
async def test_configure_portworx_force_creates_storageclass(make_context):
    ctx = make_context(
        connector_key="openshift",
        connection={"driver": "portworx", "namespace": "portworx-csi"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    conn = OpenShiftConnector(ctx)
    conn.ctx.runner = _PxPresentRunner()
    res = await conn.configure(storage_class="pure-block", force=True)
    assert res.success
    assert res.artifacts.get("reused") is None  # created, not reused
    assert any("apply -f" in c for c in conn.ctx.runner.commands)


@pytest.mark.asyncio
async def test_deploy_portworx_no_array_fails(make_context):
    ctx = make_context(
        connector_key="openshift",
        connection={"driver": "portworx", "namespace": "px"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=False,
    )
    res = await OpenShiftConnector(ctx).deploy_integration(cluster_id="c1")
    assert not res.success
    assert "FlashArray" in res.message


@pytest.mark.asyncio
async def test_deploy_portworx_dry_run_no_changes(make_context, captured_logs):
    ctx = make_context(
        connector_key="openshift",
        connection={"driver": "portworx", "namespace": "px"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    res = await OpenShiftConnector(dataclasses.replace(ctx, dry_run=True)
                                   ).deploy_integration(cluster_id="c1")
    assert res.success
    logs = "\n".join(captured_logs)
    assert "[dry-run]" in logs
    assert not any("apply -f" in line for line in captured_logs)


@pytest.mark.asyncio
async def test_teardown_portworx_deletes_storagecluster(make_context, captured_logs):
    ctx = make_context(
        connector_key="openshift",
        connection={"driver": "portworx", "namespace": "px"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    res = await OpenShiftConnector(ctx).teardown(release="px-cluster")
    assert res.success
    assert res.artifacts["driver"] == "portworx"
    logs = "\n".join(captured_logs)
    assert "delete storagecluster px-cluster" in logs


def test_portworx_pure_secret_yaml_carries_array_creds():
    y = manifests.portworx_pure_secret_yaml(
        array_endpoint="10.0.0.10", api_token="tok-xyz", namespace="px")
    assert "name: px-pure-secret" in y
    assert "namespace: px" in y
    assert "pure.json:" in y
    assert "10.0.0.10" in y and "tok-xyz" in y
    assert "MgmtEndPoint" in y and "APIToken" in y


def test_portworx_subscription_and_storagecluster_yaml():
    sub = manifests.portworx_subscription_yaml(namespace="px")
    assert "kind: Subscription" in sub
    assert "name: portworx-certified" in sub
    assert "source: certified-operators" in sub
    # Default mode is FADA Direct Access: NO pooled cloud drives.
    sc = manifests.portworx_storagecluster_yaml(name="px-cluster", namespace="px",
                                                version="3.1.2", cluster_id="c1")
    assert "apiVersion: core.libopenstorage.org/v1" in sc
    assert "kind: StorageCluster" in sc
    assert "portworx/oci-monitor:3.1.2" in sc
    assert "portworx.io/is-openshift: \"true\"" in sc
    assert "csi:" in sc and "enabled: true" in sc
    assert "cloudStorage:" not in sc  # FADA = no pooling


def test_portworx_storagecluster_enterprise_mode_has_cloud_drives():
    sc = manifests.portworx_storagecluster_yaml(name="px", namespace="px",
                                                direct_access=False)
    assert "cloudStorage:" in sc
    assert "deviceSpecs:" in sc


def test_storage_class_yaml_portworx_fada_backend():
    # FADA StorageClass carries the pure_block backend that sends PVCs straight
    # to the FlashArray via the pxd.portworx.com CSI driver.
    y = manifests.storage_class_yaml(name="px", driver="portworx",
                                     backend="pure_block")
    assert "provisioner: pxd.portworx.com" in y
    assert "backend: pure_block" in y


@pytest.mark.asyncio
async def test_configure_portworx_maps_backend_to_pure_block(make_context, captured_logs):
    ctx = make_context(
        connector_key="openshift",
        connection={"driver": "portworx", "namespace": "px"},
        secrets={"kubeconfig": KUBECONFIG},
        with_array=True,
    )
    res = await OpenShiftConnector(ctx).configure(storage_class="px-block",
                                                  backend="block")
    assert res.success
    # The applied StorageClass used the FADA pure_block backend, not raw 'block'.
    assert any("apply" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_configure_applies_sc_and_vsc(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.configure(storage_class="pure-block",
                               snapshot_class="pure-snapshotclass")
    assert res.success
    assert res.artifacts["storage_class"] == "pure-block"
    # two kubectl apply calls streamed
    applies = [l for l in captured_logs if "kubectl" in l and "apply -f" in l]
    assert len(applies) == 2


@pytest.mark.asyncio
async def test_provision_creates_pvc(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.provision(name="mydata", size="20Gi")
    assert res.success
    assert res.artifacts["pvc"] == "mydata"
    assert any("apply -f" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_snapshot(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.snapshot(name="snap1", source_pvc="mydata")
    assert res.success
    assert res.artifacts["snapshot"] == "snap1"


@pytest.mark.asyncio
async def test_clone(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.clone(name="copy", source="mydata")
    assert res.success
    assert res.artifacts["pvc"] == "copy"


@pytest.mark.asyncio
async def test_resize_patches_pvc(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.resize(name="mydata", size="50Gi")
    assert res.success
    assert any("patch pvc mydata" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_rotate_credentials_reapplies_secret(make_context, captured_logs):
    # Rotation re-applies the px-pure-secret (oc apply) with the new token — no helm.
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.rotate_credentials(api_token="new-tok")
    assert res.success
    assert res.artifacts["secret"] == manifests.PX_PURE_SECRET
    assert any("apply -f" in l for l in captured_logs)
    assert not any("helm" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_rotate_credentials_mints_when_no_token(make_context, mock_array):
    ctx = make_context(
        connector_key="openshift",
        connection={"namespace": "portworx"},
        secrets={"kubeconfig": KUBECONFIG},
    )
    res = await OpenShiftConnector(ctx).rotate_credentials()
    assert res.success
    assert "portworx" in mock_array.api_tokens


@pytest.mark.asyncio
async def test_upgrade_patches_storagecluster(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.upgrade(chart_version="3.2.0")
    assert res.success
    assert res.artifacts["version"] == "3.2.0"
    # patches the StorageCluster image, not a helm chart
    assert any("patch storagecluster" in l and "oci-monitor:3.2.0" in l
               for l in captured_logs)


@pytest.mark.asyncio
async def test_health_check(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.health_check()
    assert res.success
    assert "array" in res.data
    assert any("get pods" in l for l in captured_logs)
    assert any("get csidrivers" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_teardown(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.teardown(release="px-cluster")
    assert res.success
    assert res.artifacts["driver"] == "portworx"
    # Portworx teardown deletes the StorageCluster (+ operator Subscription), no helm.
    assert any("delete storagecluster px-cluster" in l for l in captured_logs)
    assert not any("helm uninstall" in l for l in captured_logs)


# --------------------------------------------------------------------------- #
# dispatch routing
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_dispatch_routes_provision(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.dispatch("provision", {"name": "viadispatch", "size": "5Gi"})
    assert isinstance(res, OpResult)
    assert res.success
    assert res.artifacts["pvc"] == "viadispatch"


@pytest.mark.asyncio
async def test_dispatch_unknown_action(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.dispatch("nope", {})
    assert not res.success


# --------------------------------------------------------------------------- #
# Interface binding via MachineConfig
# --------------------------------------------------------------------------- #
def test_configure_binding_action_schema():
    actions = {a.id: a for a in OpenShiftConnector.action_schemas()}
    assert "configure_binding" in actions
    act = actions["configure_binding"]
    assert act.capability == Capability.CONFIGURE
    fields = {f.name: f for f in act.fields}
    assert fields["iscsi_nics"].type == FieldType.MULTISELECT
    assert fields["iscsi_nics"].options_source == "nics"
    assert fields["nvme_sources"].options_source == "nvme_sources"
    assert fields["nvme_options"].type == FieldType.STRING
    assert fields["fc_hbas"].options_source == "fc_hbas"
    assert fields["machine_config_role"].type == FieldType.ENUM
    assert fields["machine_config_role"].default == "worker"
    assert fields["machine_config_role"].options == ["worker", "master"]
    # all binding fields are optional
    assert all(not f.required for f in act.fields)


# --- discover_options ---
@pytest.mark.asyncio
async def test_discover_options_nics(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    opts = await conn.discover_options("nics")
    assert opts and all("value" in o and "label" in o for o in opts)
    assert any(o["value"] == "ens192" for o in opts)


@pytest.mark.asyncio
async def test_discover_options_nvme_and_fc(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    assert await conn.discover_options("nvme_sources")
    assert await conn.discover_options("fc_hbas")


@pytest.mark.asyncio
async def test_discover_options_unknown_kind(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    assert await conn.discover_options("bogus") == []


class _NicStubRunner:
    """Runner that scripts a real (mock=False) cluster's node + NMState output.

    ``nmstate=True`` (default) reports the ``nodenetworkstates`` CRD present so the
    NMState path is taken and ``nns_by_node`` is used. ``nmstate=False`` makes the
    CRD probe return an error (NMState not installed); ``debug_nics_by_node`` then
    scripts the `oc debug node` /sys/class/net fallback per node.
    """

    def __init__(self, nns_by_node=None, *, nmstate=True, debug_nics_by_node=None,
                 nns_error=None):
        self.mock = False
        self.dry_run = False
        self.nns = nns_by_node or {}
        self.nmstate = nmstate
        self.debug = debug_nics_by_node or {}
        self.nns_error = nns_error
        nodes = list(self.nns) or list(self.debug)
        self.nodes = nodes

    async def log(self, _line):
        return None

    async def run_local(self, command, *, check=True, redact=None, log_output=True):
        import json
        if "get crd nodenetworkstates" in command:
            return ("customresourcedefinition.apiextensions.k8s.io/"
                    "nodenetworkstates.nmstate.io" if self.nmstate
                    else "error: the server doesn't have a resource type "
                         '"nodenetworkstates"')
        if "get nodes" in command:
            return json.dumps({"items": [
                {"metadata": {"name": n,
                              "labels": {"node-role.kubernetes.io/worker": ""}},
                 "status": {"addresses": [{"type": "InternalIP",
                                           "address": f"10.0.0.{i}"}]}}
                for i, n in enumerate(self.nodes, start=1)
            ]})
        if "get nns " in command:
            if self.nns_error is not None:
                return self.nns_error
            for node, names in self.nns.items():
                if f"get nns {node} " in command:
                    return " ".join(names)
        if "debug node/" in command:
            for node, names in self.debug.items():
                if f"debug node/{node} " in command:
                    return "\n".join(names)
        return ""


@pytest.mark.asyncio
async def test_discover_options_nics_real_cluster_filters_virtual(make_context):
    # Real cluster: NICs come from NMState; lo / br-ex / ovn overlay are dropped,
    # and only the actual node NICs are offered (annotated with coverage).
    conn = OpenShiftConnector(_ctx(make_context))
    conn.ctx.runner = _NicStubRunner({
        "w1": ["ens192", "ens224", "lo", "br-ex", "ovn-k8s-mp0"],
        "w2": ["ens192", "lo", "br-ex"],
    })
    opts = await conn.discover_options("nics")
    values = {o["value"] for o in opts}
    assert values == {"ens192", "ens224"}  # no lo / br-ex / ovn-*
    labels = {o["value"]: o["label"] for o in opts}
    assert "all 2 nodes" in labels["ens192"]
    assert "1/2" in labels["ens224"]


@pytest.mark.asyncio
async def test_discover_options_nics_no_nmstate_no_debug_is_empty(make_context):
    # NMState absent AND oc-debug yields nothing -> empty (NOT synthetic, NOT the
    # error words that used to leak through). The CRD probe returns an error.
    conn = OpenShiftConnector(_ctx(make_context))
    conn.ctx.runner = _NicStubRunner(nmstate=False, debug_nics_by_node={"w1": []})
    assert await conn.discover_options("nics") == []


@pytest.mark.asyncio
async def test_discover_options_nns_error_not_parsed_as_nics(make_context):
    # Regression: when `oc get nns` returns the "no resource type nns" error, its
    # words ("error:", "the", "server", "resource", ...) must NOT become NICs.
    conn = OpenShiftConnector(_ctx(make_context))
    conn.ctx.runner = _NicStubRunner(
        {"w1": []},  # node exists
        nns_error="error: the server doesn't have a resource type \"nns\"",
    )
    opts = await conn.discover_options("nics")
    # No garbage options; with no debug fallback configured, the list is empty.
    assert opts == []


@pytest.mark.asyncio
async def test_discover_options_nics_debug_fallback_when_no_nmstate(make_context):
    # No NMState -> fall back to `oc debug node` reading /sys/class/net. Virtual
    # devices (lo, br-ex, ovn-*, veth*) are still filtered out.
    conn = OpenShiftConnector(_ctx(make_context))
    conn.ctx.runner = _NicStubRunner(
        nmstate=False,
        debug_nics_by_node={
            "w1": ["lo", "ens192", "ens224", "br-ex", "ovn-k8s-mp0", "veth1a2b"],
            "w2": ["lo", "ens192", "br-ex"],
        },
    )
    opts = await conn.discover_options("nics")
    values = {o["value"] for o in opts}
    assert values == {"ens192", "ens224"}
    labels = {o["value"]: o["label"] for o in opts}
    assert "all 2 nodes" in labels["ens192"]
    assert "1/2" in labels["ens224"]


@pytest.mark.asyncio
async def test_discover_options_nvme_fc_empty_on_real_cluster(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    conn.ctx.runner = _NicStubRunner({"w1": ["ens192"]})
    assert await conn.discover_options("nvme_sources") == []
    assert await conn.discover_options("fc_hbas") == []


def test_is_storage_nic_candidate():
    from phif.connectors.openshift.connector import _is_storage_nic_candidate
    assert _is_storage_nic_candidate("ens1f0np0")
    assert _is_storage_nic_candidate("bond0")
    assert _is_storage_nic_candidate("ens1f0.2230")  # VLAN subinterface
    for virt in ("lo", "br-ex", "br-int", "ovn-k8s-mp0", "ovs-system", "veth123",
                 "genev_sys_6081", "tun0",
                 # OVN OVS patch ports, which embed the node MAC and so carry
                 # digits/dashes — the digit/charset guard alone wouldn't drop them.
                 "patch-br-ex_00-00-5e-00-53-01-to-br-int",
                 "patch-br-int-to-br-ex_00-00-5e-00-53-01"):
        assert not _is_storage_nic_candidate(virt), virt
    # Words from an `oc` error line ("error: the server doesn't have a resource
    # type nns") must be rejected: they aren't valid interface names / carry no
    # digit. This is the regression that produced bogus "nns/a/doesnt/..." options.
    for word in ("error:", "the", "server", "doesn't", "have", "a", "resource",
                 "type", "nns"):
        assert not _is_storage_nic_candidate(word), word


def test_looks_like_oc_error():
    from phif.connectors.openshift.connector import _looks_like_oc_error
    assert _looks_like_oc_error("error: the server doesn't have a resource type")
    assert _looks_like_oc_error("No resources found")
    assert _looks_like_oc_error("Error from server (NotFound)")
    assert not _looks_like_oc_error("ens192 ens224 lo")
    assert not _looks_like_oc_error("")


# --- MachineConfig generator (pure function) ---
def test_machineconfig_yaml_kind_and_role():
    y = manifests.interface_binding_machineconfig_yaml(role="worker")
    assert "apiVersion: machineconfiguration.openshift.io/v1" in y
    assert "kind: MachineConfig" in y
    assert "machineconfiguration.openshift.io/role: worker" in y
    assert "version: 3.2.0" in y


def test_machineconfig_yaml_embeds_iscsi_nics():
    y = manifests.interface_binding_machineconfig_yaml(
        iscsi_nics=["ens192", "ens224"])
    # an iface file per NIC at the iscsiadm ifaces path
    assert "/etc/iscsi/ifaces/phif_ens192" in y
    assert "/etc/iscsi/ifaces/phif_ens224" in y
    # content is percent-encoded but the NIC name + iface key survive
    from urllib.parse import unquote
    decoded = unquote(y)
    assert "iface.net_ifacename = ens192" in decoded
    assert "iface.net_ifacename = ens224" in decoded


def test_machineconfig_yaml_embeds_nvme_unit():
    y = manifests.interface_binding_machineconfig_yaml(
        nvme_sources=["ens192"], nvme_options="--nr-io-queues=8")
    assert "/etc/nvme/phif-connect.conf" in y
    assert "phif-nvme-connect.service" in y
    assert "nvme connect-all -w ens192 --nr-io-queues=8" in y


def test_machineconfig_yaml_records_fc_hbas_as_annotation():
    y = manifests.interface_binding_machineconfig_yaml(
        fc_hbas=["21:00:00:aa", "21:00:00:bb"])
    assert "phif.purestorage.com/fc-hbas: \"21:00:00:aa,21:00:00:bb\"" in y
    # FC writes no per-HBA iSCSI/NVMe node file ...
    assert "/etc/iscsi/ifaces" not in y
    assert "/etc/nvme" not in y
    # ... but FC IS a dm-multipath transport, so multipath.conf + multipathd
    # are still configured.
    assert "/etc/multipath.conf" in y
    assert "multipathd.service" in y


def test_machineconfig_yaml_iscsi_enables_multipath_and_iscsid():
    y = manifests.interface_binding_machineconfig_yaml(iscsi_nics=["ens192"])
    # Everpure FlashArray multipath.conf written + multipathd enabled.
    assert "/etc/multipath.conf" in y
    assert "multipathd.service" in y
    from urllib.parse import unquote
    decoded = unquote(y)
    assert "vendor \"PURE\"" in decoded
    assert "product \"FlashArray\"" in decoded
    assert "find_multipaths no" in decoded
    # iscsid enabled so iSCSI login works.
    assert "iscsid.service" in y
    assert "enabled: true" in y


def test_machineconfig_yaml_nvme_only_skips_multipath():
    # NVMe-oF uses native NVMe multipath — no dm-multipath.conf / multipathd.
    y = manifests.interface_binding_machineconfig_yaml(nvme_sources=["ens192"])
    assert "/etc/multipath.conf" not in y
    assert "multipathd.service" not in y
    assert "iscsid.service" not in y


def test_machineconfig_yaml_iscsi_pushes_arp_sysctl():
    # iSCSI NIC binding must also push the ARP-flux sysctl drop-in (arp_ignore=2 /
    # arp_announce=2 per selected NIC) — the same fix the SSH-driven Linux
    # connectors apply, delivered declaratively via Ignition on RHCOS.
    from urllib.parse import unquote
    y = manifests.interface_binding_machineconfig_yaml(iscsi_nics=["ens192", "ens224"])
    assert "/etc/sysctl.d/99-phif-iscsi-arp.conf" in y
    decoded = unquote(y)
    assert "net.ipv4.conf.ens192.arp_ignore = 2" in decoded
    assert "net.ipv4.conf.ens192.arp_announce = 2" in decoded
    assert "net.ipv4.conf.ens224.arp_ignore = 2" in decoded
    assert "net.ipv4.conf.ens224.arp_announce = 2" in decoded


def test_machineconfig_yaml_no_arp_without_iscsi():
    # NVMe-only / FC selections don't write the iSCSI ARP drop-in.
    y_nvme = manifests.interface_binding_machineconfig_yaml(nvme_sources=["ens192"])
    assert "99-phif-iscsi-arp.conf" not in y_nvme
    y_fc = manifests.interface_binding_machineconfig_yaml(fc_hbas=["21:00:00:aa"])
    assert "99-phif-iscsi-arp.conf" not in y_fc


def test_arp_sysctl_content_shared_helper():
    # The declarative file body and the SSH snippet set the same keys/values.
    from phif.connectors.iscsi_net import arp_sysctl_content, arp_flux_cmd
    body = arp_sysctl_content(["ens1f0", "ens1f1"])
    assert "net.ipv4.conf.ens1f0.arp_ignore = 2" in body
    assert "net.ipv4.conf.ens1f1.arp_announce = 2" in body
    assert arp_sysctl_content([]) == ""  # no NICs -> empty (skip writing)
    # the SSH form sets the same NICs (live sysctl -w + persisted file)
    cmd = arp_flux_cmd(["ens1f0"])
    assert "net.ipv4.conf.ens1f0.arp_ignore=2" in cmd


def test_multipath_conf_matches_pure_stanza():
    # The OpenShift multipath.conf reuses the same Everpure-recommended stanza as the
    # Proxmox / HPE / XCP-ng connectors.
    c = manifests.MULTIPATH_CONF
    for token in ('vendor "PURE"', 'product "FlashArray"', "prio alua",
                  "path_grouping_policy group_by_prio", "hardware_handler \"1 alua\"",
                  "find_multipaths no", "user_friendly_names no", "no_path_retry 0"):
        assert token in c, token


def test_machineconfig_yaml_empty_is_valid_noop():
    y = manifests.interface_binding_machineconfig_yaml()
    assert "kind: MachineConfig" in y
    assert "ignition" in y


def test_iscsi_iface_file_content():
    c = manifests.iscsi_iface_file_content("bond0")
    assert "iface.net_ifacename = bond0" in c


# --- configure_binding action (apply path / dry-run) ---
@pytest.mark.asyncio
async def test_configure_binding_applies_machineconfig(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.configure_binding(iscsi_nics=["ens192"],
                                       nvme_sources=["ens224"],
                                       nvme_options="--nr-io-queues=8",
                                       machine_config_role="worker")
    assert res.success
    assert res.artifacts["machine_config"] == "phif-interface-binding"
    assert res.artifacts["role"] == "worker"
    assert res.artifacts["iscsi_nics"] == ["ens192"]
    # applied via `oc ... apply -f`
    assert any("oc" in l and "apply -f" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_configure_binding_dry_run_no_apply(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context, dry_run=True))
    res = await conn.configure_binding(iscsi_nics=["ens192"])
    assert res.success
    logs = "\n".join(captured_logs)
    assert "[dry-run]" in logs
    # rendered MachineConfig is shown but never applied
    assert "kind: MachineConfig" in logs
    assert not any("apply -f" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_configure_binding_fc_records_selection(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.configure_binding(fc_hbas=["21:00:00:aa"],
                                       machine_config_role="worker")
    assert res.success
    assert res.artifacts["fc_hbas"] == ["21:00:00:aa"]
    assert any("zoning-driven" in l for l in captured_logs)


@pytest.mark.asyncio
async def test_dispatch_routes_configure_binding(make_context, captured_logs):
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.dispatch("configure_binding", {"iscsi_nics": ["ens192"]})
    assert isinstance(res, OpResult)
    assert res.success
    assert res.artifacts["iscsi_nics"] == ["ens192"]


# --------------------------------------------------------------------------- #
# Cluster awareness: wizard_steps / list_nodes / validate_cluster
# --------------------------------------------------------------------------- #
def test_wizard_steps_cluster_level():
    # OpenShift is cluster-level: CSI install + StorageClass + the interface-
    # binding MachineConfig apply to all nodes, and the CSI driver auto-registers
    # worker nodes. The wizard omits register_hosts / setup_connectivity (base
    # defaults) and ends with configure_binding so any node reboot is last.
    steps = OpenShiftConnector.wizard_steps()
    assert steps == ["deploy", "configure", "configure_binding"]
    assert steps[-1] == "configure_binding"  # reboot-causing step runs last
    assert "register_hosts" not in steps
    assert "setup_connectivity" not in steps


def test_descriptor_includes_wizard_steps():
    d = OpenShiftConnector.descriptor()
    assert d["wizard_steps"] == ["deploy", "configure", "configure_binding"]


@pytest.mark.asyncio
async def test_configure_binding_noop_when_nothing_selected(make_context, captured_logs):
    # The wizard always runs configure_binding; with no interfaces selected it must
    # be a clean no-op — NO MachineConfig applied (an empty one would needlessly
    # reboot the nodes).
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.configure_binding()
    assert res.success
    assert res.artifacts.get("skipped") is True
    assert res.artifacts.get("machine_config") is None
    assert not any("apply -f" in l for l in captured_logs)
    assert any("skipping" in l.lower() for l in captured_logs)


@pytest.mark.asyncio
async def test_list_nodes_mock_returns_three_workers(make_context):
    # In mock mode run_local is a no-op, so list_nodes synthesizes 3 worker nodes.
    conn = OpenShiftConnector(_ctx(make_context))
    nodes = await conn.list_nodes()
    assert len(nodes) == 3
    assert {n.name for n in nodes} == {"worker-1", "worker-2", "worker-3"}
    assert all(n.info.get("role") == "worker" for n in nodes)
    # ClusterNode is serializable for the API/UI
    assert all(set(n.to_dict()) == {"name", "host", "info"} for n in nodes)


@pytest.mark.asyncio
async def test_list_nodes_dry_run_returns_three_workers(make_context):
    conn = OpenShiftConnector(_ctx(make_context, dry_run=True))
    nodes = await conn.list_nodes()
    assert len(nodes) == 3


def test_parse_nodes_json_extracts_workers():
    raw = (
        '{"items": [{"metadata": {"name": "w1", "labels": '
        '{"node-role.kubernetes.io/worker": ""}}, "status": {"addresses": '
        '[{"type": "InternalIP", "address": "10.0.0.5"}]}},'
        '{"metadata": {"name": "w2", "labels": {}}, "status": {}}]}'
    )
    nodes = OpenShiftConnector._parse_nodes_json(raw)
    assert [n.name for n in nodes] == ["w1", "w2"]
    # InternalIP becomes the host where present; otherwise the name
    assert nodes[0].host == "10.0.0.5"
    assert nodes[0].info["role"] == "worker"
    assert nodes[1].host == "w2"


def test_parse_nodes_json_empty_and_garbage():
    assert OpenShiftConnector._parse_nodes_json("") == []
    assert OpenShiftConnector._parse_nodes_json("   ") == []
    assert OpenShiftConnector._parse_nodes_json("not json") == []
    assert OpenShiftConnector._parse_nodes_json('{"items": "nope"}') == []


@pytest.mark.asyncio
async def test_validate_cluster_mock_consistent(make_context):
    # Mock mode synthesizes a uniform NIC set across the 3 worker nodes, so the
    # cluster NIC uniformity check passes.
    conn = OpenShiftConnector(_ctx(make_context))
    res = await conn.validate_cluster()
    assert res.success
    assert res.data["checked"] is True
    assert len(res.data["nodes"]) == 3
    # all nodes share the same NICs
    per_node = res.data["per_node_nics"]
    assert all(v == per_node["worker-1"] for v in per_node.values())


@pytest.mark.asyncio
async def test_validate_cluster_uses_compare_node_interfaces(monkeypatch, make_context):
    # When per-node NICs differ, validate_cluster fails via compare_node_interfaces.
    conn = OpenShiftConnector(_ctx(make_context))

    async def _fake_nics(nodes):
        return {"worker-1": ["ens192"], "worker-2": ["ens224"]}

    monkeypatch.setattr(conn, "_discover_per_node_nics", _fake_nics)
    res = await conn.validate_cluster()
    assert not res.success
    assert res.data["checked"] is True
    assert "differ" in res.message.lower()


@pytest.mark.asyncio
async def test_validate_cluster_not_feasible_returns_ok_note(monkeypatch, make_context):
    # When per-node NIC discovery is not feasible, return ok with a note (advisory).
    conn = OpenShiftConnector(_ctx(make_context))

    async def _no_nics(nodes):
        return {}

    monkeypatch.setattr(conn, "_discover_per_node_nics", _no_nics)
    res = await conn.validate_cluster()
    assert res.success
    assert res.data["checked"] is False
    assert "node" in res.message.lower()


# --------------------------------------------------------------------------- #
# Migration: KubeVirt VMs <-> FlashArray (capabilities, manifests, methods)
# --------------------------------------------------------------------------- #
def test_migration_capabilities_declared():
    caps = OpenShiftConnector.capabilities()
    assert Capability.MIGRATE in caps
    assert Capability.VM_INVENTORY in caps
    assert Capability.VM_LIFECYCLE in caps


def test_fada_fa_volume_name():
    # px_<clusterUid[:8]>-<pvName>
    n = manifests.fada_fa_volume_name("00000000-0000-4000-8000-000000000001",
                                      "pvc-11111111-2222-4333-8444-555555555555")
    assert n == "px_00000000-pvc-11111111-2222-4333-8444-555555555555"


def test_migration_pvc_yaml():
    y = manifests.migration_pvc_yaml(name="vm1-disk0", namespace="default", size="10Gi")
    assert "kind: PersistentVolumeClaim" in y
    assert "name: vm1-disk0" in y
    assert "storageClassName: px-fa-direct-access" in y
    assert "storage: 10Gi" in y
    assert "phif.purestorage.com/migration" in y
    # VM disks must be raw block (the copied image carries its own GPT/FS) and RWX
    # (KubeVirt LiveMigration/HA + node mobility need multi-node attach).
    assert "volumeMode: Block" in y
    assert "ReadWriteMany" in y


def test_virtual_machine_yaml_shape():
    y = manifests.virtual_machine_yaml(
        name="vm1", namespace="default", vcpus=4, memory_bytes=4 * 1024**3,
        firmware="uefi",
        disks=[{"name": "disk0", "claim": "vm1-disk0", "bus": "virtio", "boot_order": 1}],
        nics=[{"name": "nic0", "mac": "52:54:00:ab:cd:ef", "dest": "pod", "pod": True}],
        running=False)
    assert "apiVersion: kubevirt.io/v1" in y
    assert "kind: VirtualMachine" in y
    assert "running: false" in y
    assert "cores: 4" in y
    assert "guest: 4096Mi" in y
    # uefi firmware: SecureBoot off by default (avoids the SMM requirement)
    assert "efi:" in y and "secureBoot: false" in y
    assert "claimName: vm1-disk0" in y
    assert "bootOrder: 1" in y
    assert "macAddress: \"52:54:00:ab:cd:ef\"" in y
    assert "masquerade: {}" in y     # pod network


def test_virtual_machine_yaml_bios_multus():
    y = manifests.virtual_machine_yaml(
        name="vm2", namespace="vms", vcpus=1, memory_bytes=1024**3, firmware="bios",
        disks=[{"name": "disk0", "claim": "vm2-disk0", "bus": "scsi", "boot_order": 1}],
        nics=[{"name": "nic0", "mac": "", "dest": "vms/bridge-net", "pod": False}],
        running=False)
    assert "efi: {}" not in y         # bios -> no EFI
    assert "bus: scsi" in y
    assert "multus:" in y and "networkName: vms/bridge-net" in y


def test_qty_bytes():
    q = OpenShiftConnector._qty_bytes
    assert q("2Gi") == 2 * 1024**3
    assert q("512Mi") == 512 * 1024**2
    assert q("1000000") == 1000000
    assert q("1G") == 1000**3
    assert q("") == 0


def test_parse_ref_and_rfc1123():
    assert OpenShiftConnector._parse_ref("ns1/vmA", "default") == ("ns1", "vmA")
    assert OpenShiftConnector._parse_ref("vmA", "default") == ("default", "vmA")
    assert OpenShiftConnector._rfc1123("My VM_01!") == "my-vm-01"


def test_migration_host_group_sentinel(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    assert conn.migration_host_group() == "portworx-fada"


@pytest.mark.asyncio
async def test_capture_vm_spec_mock(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    spec = await conn.capture_vm_spec("default/red-possum")
    assert spec.name == "red-possum"
    assert spec.vcpus >= 1 and spec.memory_bytes > 0
    assert len(spec.disks) == 1 and spec.disks[0].boot
    assert spec.disks[0].identity.fa_volume.startswith("px_")
    assert len(spec.nics) == 1 and spec.nics[0].source_network == "pod"


@pytest.mark.asyncio
async def test_create_vm_and_managed_disk_dry_run(make_context):
    from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec
    conn = OpenShiftConnector(_ctx(make_context, dry_run=True))
    spec = VmSpec(name="webvm", source_ref="x", vcpus=2, memory_bytes=2 * 1024**3,
                  disks=[DiskSpec(identity=DiskIdentity(fa_volume="src-vol",
                         serial="s1", size_bytes=10 * 1024**3), order=0, boot=True)],
                  nics=[NicSpec(mac="52:54:00:00:00:09", source_network="pod", order=0)])
    res = await conn.create_vm(spec, network_map={"pod": "pod"})
    assert res.success
    ref = res.artifacts["vm_ref"]
    assert ref == "default/webvm"
    fa = await conn.create_managed_disk(ref, size_bytes=10 * 1024**3, order=0, boot=True)
    assert fa.startswith("px_dryrun-webvm")
    # set_boot_order is a no-op success; delete_vm dry-run succeeds.
    assert (await conn.set_boot_order(ref, spec.disks)).success
    assert (await conn.delete_vm(ref, keep_disks=False)).success


@pytest.mark.asyncio
async def test_list_vms_and_networks_mock(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    vms = await conn.list_vms()
    # Shape must match VmSummary (id + power_state), not the old ref/status keys.
    assert vms and all("id" in v and "power_state" in v for v in vms)
    assert all("/" in v["id"] for v in vms)  # id == "<ns>/<name>"
    nets = await conn.list_networks()
    assert any(n["id"] == "pod" for n in nets)


@pytest.mark.asyncio
async def test_require_host_objects_ok(make_context):
    conn = OpenShiftConnector(_ctx(make_context))
    assert (await conn.require_host_objects("portworx-fada")).success
