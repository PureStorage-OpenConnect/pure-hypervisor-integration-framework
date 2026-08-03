"""OpenStack (Cinder / Everpure FlashArray volume driver) connector.

This connector wraps the **upstream Cinder FlashArray volume driver**
(``cinder.volume.drivers.pure.PureISCSIDriver`` / ``PureFCDriver`` /
``PureNVMEDriver``). The driver ships with Cinder itself, so "deploying the
plugin" means configuring a backend stanza in ``cinder.conf``, adding it to
``enabled_backends`` and restarting ``cinder-volume``. Day-2 operations
(volume types, provisioning, snapshots, clones, resize, QoS, replication,
health) are driven over SSH to the OpenStack controller using the
``openstack``/``cinder`` CLIs.

All work is driven through ``ctx.runner.run_ssh`` so it is fully mock-safe in
``PHIF_MOCK_MODE`` and honors ``ctx.dry_run``.

References (Everpure Data support docs, bundle ``m_openstack``):
  https://support.purestorage.com/bundle/m_openstack/page/Solutions/OpenStack/OpenStack_Reference/topics/concept/c_pure_storage_openstack_driver_best_practices.html
  https://support.purestorage.com/bundle/m_openstack/page/Solutions/OpenStack/OpenStack_Reference/topics/concept/c_pure_storage_flasharray_volume_driver_for_openstack_release.html
"""

from __future__ import annotations

import asyncio
import json
import math
import shlex
from typing import Any

from phif.connectors.base import (
    ActionSpec,
    Capability,
    ClusterNode,
    ConnectionValidationError,
    DiscoveryKind,
    FieldType,
    FormField,
    HypervisorConnector,
    OpResult,
    Protocol,
)
from phif.connectors.iscsi_net import arp_flux_cmd
from phif.connectors.openstack import pure
from phif.migrate.spec import DiskIdentity, DiskSpec, NicSpec, VmSpec


class OpenStackConnector(HypervisorConnector):
    # --- static metadata ---
    key = "openstack"
    name = "OpenStack (Cinder / Everpure driver)"
    description = (
        "Wraps the upstream Cinder FlashArray volume driver "
        "(cinder.volume.drivers.pure). Configures a backend stanza in "
        "cinder.conf over SSH to the controller, manages volume types, and "
        "drives day-2 volume operations via the openstack/cinder CLIs."
    )
    maturity = "ga"
    CAPABILITIES = {
        Capability.DEPLOY_PLUGIN,
        Capability.CONFIGURE,
        Capability.CONNECTIVITY,
        Capability.VM_INVENTORY,
        Capability.VM_LIFECYCLE,
        Capability.MIGRATE,
        Capability.PROVISION_VOLUME,
        Capability.SNAPSHOT,
        Capability.CLONE,
        Capability.RESIZE,
        Capability.QOS,
        Capability.REPLICATION,
        Capability.HEALTH,
        Capability.REMOVE,
    }
    SUPPORTED_PROTOCOLS = {
        Protocol.ISCSI,
        Protocol.FC,
        Protocol.NVME_TCP,
        Protocol.NVME_ROCE,
    }

    # --- UI: how to connect to this hypervisor ---
    @classmethod
    def target_schema(cls) -> list[FormField]:
        return [
            FormField("controller_host", "Controller host / IP", FieldType.STRING,
                      placeholder="controller.openstack.local",
                      help="SSH-reachable OpenStack controller running cinder-volume."),
            FormField("ssh_user", "SSH user", FieldType.STRING, default="ubuntu",
                      help="A sudo-capable login on the controller (e.g. 'ubuntu' "
                           "or 'stack'). Privileged ops (cinder.conf, systemctl, "
                           "pip) run via passwordless sudo unless this user is root."),
            FormField("ssh_password", "SSH password", FieldType.SECRET, required=False,
                      help="Provide either an SSH password or an SSH private key."),
            FormField("ssh_key", "SSH private key", FieldType.TEXT, required=False,
                      help="PEM private key; alternative to SSH password."),
            FormField("cinder_conf_path", "cinder.conf path", FieldType.STRING,
                      default="/etc/cinder/cinder.conf"),
            FormField("protocol", "Storage protocol", FieldType.ENUM, default="iscsi",
                      options=["iscsi", "fc", "nvme-tcp", "nvme-roce"]),
            # NOTE: san_ip and pure_api_token are NOT operator-entered fields.
            # They are derived from the FlashArray associated with this
            # hypervisor (ctx.array.endpoint -> san_ip, ctx.resolve_token ->
            # pure_api_token), matching the consistent PHIF UX where array
            # credentials come from the associated array, not the connector form.
            FormField("backend_name", "Cinder backend name", FieldType.STRING,
                      default="pure"),
            FormField("volume_service", "cinder-volume service unit", FieldType.STRING,
                      default="cinder-volume",
                      help="systemd unit (or devstack@c-vol) restarted after a "
                           "backend change. Tried first, then sensible fallbacks."),
            FormField("compute_hosts", "Compute hosts (SSH-reachable)",
                      FieldType.STRING, required=False,
                      placeholder="192.0.2.11,192.0.2.12",
                      help="Comma-separated nova-compute hosts the appliance can "
                           "SSH to (same ssh_user/key), for the compute-side "
                           "multipath / iSCSI / NIC setup. Defaults to the "
                           "controller host (all-in-one / single node)."),
            # OpenStack admin credentials for the openstack/cinder CLIs. The SSH
            # login (e.g. 'ubuntu') typically has no OS_* env, so day-2 CLI ops
            # (create volume type, provision, etc.) must carry admin auth.
            FormField("os_auth_url", "Keystone auth URL", FieldType.STRING,
                      required=False,
                      placeholder="http://controller:5000/v3",
                      help="Defaults to http://<controller_host>:5000/v3."),
            FormField("os_username", "OpenStack admin user", FieldType.STRING,
                      default="admin"),
            FormField("os_password", "OpenStack admin password", FieldType.SECRET,
                      required=False),
            FormField("os_project_name", "OpenStack project", FieldType.STRING,
                      default="admin"),
            FormField("os_user_domain_name", "User domain", FieldType.STRING,
                      default="Default"),
            FormField("os_project_domain_name", "Project domain", FieldType.STRING,
                      default="Default"),
            FormField("os_region_name", "Region (optional)", FieldType.STRING,
                      required=False),
        ]

    # --- UI: day-2 actions ---
    @classmethod
    def action_schemas(cls) -> list[ActionSpec]:
        return [
            ActionSpec(
                Capability.DEPLOY_PLUGIN, "deploy", "Deploy Cinder backend",
                "Write/append the Everpure backend stanza to cinder.conf, add it to "
                "enabled_backends, and restart cinder-volume (shows a diff first).",
                fields=[
                    FormField("pure_iscsi_cidr", "iSCSI target CIDR",
                              FieldType.STRING, required=False,
                              placeholder="192.0.2.0/24",
                              help="Restrict iSCSI to this array target CIDR, "
                                   "e.g. 192.0.2.0/24. iSCSI only; ignored for "
                                   "FC/NVMe."),
                    FormField("pure_iscsi_cidr_list", "iSCSI target CIDR list",
                              FieldType.STRING, required=False,
                              placeholder="192.0.2.0/24,198.51.100.0/24",
                              help="Comma-separated list of array iSCSI target "
                                   "CIDRs (alternative to a single CIDR). iSCSI only."),
                    FormField("nvme_options", "Extra NVMe options",
                              FieldType.STRING, required=False,
                              help="Free-form 'key = value' lines appended to an "
                                   "NVMe stanza. TODO(doc-validate) transport "
                                   "option names."),
                    FormField("nics", "Compute-host NICs (documentation)",
                              FieldType.MULTISELECT, required=False,
                              options_source="nics",
                              help="Controller NICs discovered for reference. "
                                   "Compute-host iscsiadm iface binding is "
                                   "host-managed and separate. TODO(doc-validate)."),
                ],
            ),
            ActionSpec(
                Capability.CONFIGURE, "configure", "Create volume type",
                "Create a Cinder volume type bound to the Everpure backend "
                "(volume_backend_name) and optionally a QoS spec.",
                fields=[
                    FormField("volume_type", "Volume type name", FieldType.STRING,
                              default="pure"),
                    FormField("qos_name", "QoS spec name (optional)", FieldType.STRING,
                              required=False),
                    FormField("max_iops", "Max IOPS (optional)", FieldType.INT,
                              required=False),
                    FormField("max_bw", "Max bandwidth bytes/s (optional)",
                              FieldType.INT, required=False),
                ],
            ),
            ActionSpec(
                Capability.CONNECTIVITY, "setup_connectivity",
                "Set up compute-host data path",
                "Configure the iSCSI/multipath data path on each nova-compute "
                "host: lay down the Everpure multipath drop-in + enable multipathd, "
                "set [libvirt] volume_use_multipath=true in nova.conf and restart "
                "nova-compute, bind iSCSI ifaces to the selected NICs, and apply "
                "the multi-NIC ARP-flux sysctls. Required for multipathed "
                "attachments; the Cinder driver does NOT do host-side setup.",
                fields=[
                    FormField("iscsi_nics", "iSCSI NICs", FieldType.MULTISELECT,
                              required=False, options_source="nics",
                              help="Storage NICs on each compute host to bind iSCSI "
                                   "to (iscsiadm iface) and apply ARP-flux sysctls. "
                                   "Leave empty to enable multipath without "
                                   "per-NIC iface binding."),
                ],
                long_running=True,
            ),
            ActionSpec(
                Capability.PROVISION_VOLUME, "provision", "Provision volume",
                "Create a Cinder volume via `openstack volume create`.",
                fields=[
                    FormField("name", "Volume name", FieldType.STRING),
                    FormField("size", "Size (GiB)", FieldType.INT, default=10),
                    FormField("volume_type", "Volume type", FieldType.STRING,
                              default="pure", required=False),
                ],
            ),
            ActionSpec(
                Capability.SNAPSHOT, "snapshot", "Snapshot volume",
                fields=[
                    FormField("volume", "Source volume name/ID", FieldType.STRING),
                    FormField("name", "Snapshot name", FieldType.STRING),
                ],
            ),
            ActionSpec(
                Capability.CLONE, "clone", "Clone volume",
                fields=[
                    FormField("source", "Source volume name/ID", FieldType.STRING),
                    FormField("dest", "New volume name", FieldType.STRING),
                    FormField("size", "Size (GiB, optional)", FieldType.INT,
                              required=False),
                ],
            ),
            ActionSpec(
                Capability.RESIZE, "resize", "Resize volume",
                fields=[
                    FormField("volume", "Volume name/ID", FieldType.STRING),
                    FormField("size", "New size (GiB)", FieldType.INT),
                ],
            ),
            ActionSpec(
                Capability.QOS, "set_qos", "Create / associate QoS spec",
                fields=[
                    FormField("qos_name", "QoS spec name", FieldType.STRING),
                    FormField("volume_type", "Volume type to associate",
                              FieldType.STRING),
                    FormField("max_iops", "Max IOPS (optional)", FieldType.INT,
                              required=False),
                    FormField("max_bw", "Max bandwidth bytes/s (optional)",
                              FieldType.INT, required=False),
                ],
            ),
            ActionSpec(
                Capability.REPLICATION, "configure_replication",
                "Enable replication on volume type",
                "Set the replication_enabled extra spec on a volume type "
                "(requires a replication_device in cinder.conf).",
                fields=[
                    FormField("volume_type", "Volume type", FieldType.STRING),
                ],
            ),
            ActionSpec(Capability.HEALTH, "health_check", "Health check",
                       "Run `openstack volume service list`.", long_running=False),
            ActionSpec(Capability.REMOVE, "teardown", "Remove Cinder backend",
                       "Remove the backend stanza, drop it from enabled_backends, "
                       "and restart cinder-volume.",
                       destructive=True),
        ]

    # --- wizard: ordered steps the deployment wizard runs ---
    @classmethod
    def wizard_steps(cls) -> list[str]:
        """Deploy the Cinder backend, create the volume type, set up compute hosts.

        The Everpure Cinder driver (on the controller) auto-manages FlashArray host
        objects at attach time and os-brick performs the iSCSI/NVMe/FC attach on
        the Nova compute hosts -- but the host-side *data path* (multipath
        enablement + tuning, iSCSI iface/NIC binding, ARP-flux sysctls) is NOT
        done by the driver. So the flow is ``deploy`` (cinder.conf backend +
        restart cinder-volume) -> ``configure`` (volume type) -> ``setup_connectivity``
        (per-compute-host multipath / iSCSI / NIC tuning).
        """
        return ["deploy", "configure", "setup_connectivity"]

    # ----------------------------------------------------------------- utils ---
    def _conf_path(self) -> str:
        return self.ctx.target.get("cinder_conf_path", "/etc/cinder/cinder.conf")

    def _backend_name(self) -> str:
        return self.ctx.target.get("backend_name", "pure")

    def _ssh_user(self) -> str:
        return self.ctx.target.get("ssh_user", "ubuntu")

    def _mock_or_dry(self) -> bool:
        """True when no real controller is reachable (mock mode or dry-run)."""
        return bool(self.ctx.dry_run or getattr(self.ctx.runner, "mock", False)
                    or getattr(self.ctx.runner, "dry_run", False))

    def _use_sudo(self) -> bool:
        """Privileged shell ops need sudo unless the SSH login is already root.

        cinder.conf is typically ``root:cinder 0640`` and systemctl/pip require
        root, so a non-root login (e.g. ``ubuntu``/``stack``) must use passwordless
        sudo. ``runner.run_ssh(sudo=True)`` wraps the whole command in
        ``sudo -n sh -c '<cmd>'`` (``-n`` fails fast if sudo would prompt).
        """
        return self._ssh_user() != "root"

    async def _ssh(self, command: str, *, check: bool = True,
                   sudo: bool | None = None,
                   redact: list[str] | None = None,
                   host: str | None = None) -> str:
        """Run a command over SSH (mock/dry-run safe).

        ``host`` defaults to the controller; pass it to target a specific
        compute host (same ssh_user/key). ``sudo`` defaults to :meth:`_use_sudo`;
        pass ``sudo=False`` for commands that must run as the login user (e.g. the
        openstack/cinder CLIs, which carry their own OS_* admin env rather than
        running as root). ``redact`` masks secret substrings in the log.
        """
        host = host or self.ctx.target.get("controller_host") or self.ctx.target.get("host")
        if not host:
            raise ConnectionValidationError("No controller_host configured")
        return await self.ctx.runner.run_ssh(
            host,
            command,
            username=self._ssh_user(),
            password=self.ctx.target.get("ssh_password"),
            key=self.ctx.target.get("ssh_key"),
            check=check,
            sudo=self._use_sudo() if sudo is None else sudo,
            redact=redact,
        )

    def _os_env(self) -> str:
        """Build the ``OS_*`` admin-auth env prefix for openstack/cinder CLIs.

        The SSH login usually has no OpenStack RC sourced, so every CLI call is
        prefixed with explicit admin credentials. auth_url defaults to
        ``http://<controller_host>:5000/v3``.
        """
        host = self.ctx.target.get("controller_host") or self.ctx.target.get("host") or ""
        auth_url = (self.ctx.target.get("os_auth_url")
                    or f"http://{host}:5000/v3")
        pairs = {
            "OS_AUTH_URL": auth_url,
            "OS_USERNAME": self.ctx.target.get("os_username", "admin"),
            "OS_PASSWORD": self.ctx.target.get("os_password", ""),
            "OS_PROJECT_NAME": self.ctx.target.get("os_project_name", "admin"),
            "OS_USER_DOMAIN_NAME": self.ctx.target.get("os_user_domain_name", "Default"),
            "OS_PROJECT_DOMAIN_NAME": self.ctx.target.get("os_project_domain_name", "Default"),
            "OS_IDENTITY_API_VERSION": "3",
        }
        region = self.ctx.target.get("os_region_name")
        if region:
            pairs["OS_REGION_NAME"] = region
        return " ".join(f"{k}={shlex.quote(str(v))}" for k, v in pairs.items())

    # Transient API error signatures worth retrying — the all-in-one controller's
    # apache-hosted services (keystone/neutron/nova/placement) and OVN occasionally
    # 5xx under the burst of CLI calls a migration fires.
    _TRANSIENT_MARKERS = (
        "(HTTP 503)", "(HTTP 500)", "(HTTP 502)", "(HTTP 504)", "ServiceUnavailable",
        "Service Unavailable", "temporarily unavailable", "Unexpected API Error",
        "QueuePool limit", "Connection refused", "Gateway Time-out",
        "Unable to establish connection",
    )

    @classmethod
    def _is_transient(cls, text: str) -> bool:
        return any(m in (text or "") for m in cls._TRANSIENT_MARKERS)

    async def _cli(self, command: str, *, check: bool = True, retries: int = 5) -> str:
        """Run an ``openstack``/``cinder`` CLI on the controller as the login user,
        retrying transient API 5xx with backoff.

        Prefixed with the admin OS_* env (NOT sudo — the CLI authenticates to
        keystone, it does not need root). The admin password (carried on the argv
        via ``OS_PASSWORD=...``) is passed to ``redact`` to mask it in the log.
        stderr is intentionally NOT suppressed so real errors are visible in the
        job log (and detectable for retry).
        """
        from phif.jobs.runner import CommandError
        password = self.ctx.target.get("os_password")
        redact = [password] if password else None
        full = f"{self._os_env()} {command}"
        out = ""
        for attempt in range(max(1, retries)):
            try:
                out = await self._ssh(full, check=check, sudo=False, redact=redact)
            except CommandError as exc:
                out = getattr(exc, "output", "") or str(exc)
                if attempt < retries - 1 and self._is_transient(out):
                    await self.ctx.emit(
                        f"[openstack] transient API error; retry {attempt + 1}/{retries}")
                    await asyncio.sleep(3 * (attempt + 1))
                    continue
                raise
            if attempt < retries - 1 and not check and self._is_transient(out):
                await self.ctx.emit(
                    f"[openstack] transient API error; retry {attempt + 1}/{retries}")
                await asyncio.sleep(3 * (attempt + 1))
                continue
            return out
        return out

    async def _read_cinder_conf(self) -> str:
        """Best-effort read of the remote cinder.conf (empty string if absent)."""
        path = self._conf_path()
        return await self._ssh(f"cat {shlex.quote(path)} 2>/dev/null || true",
                               check=False)

    async def _restart_volume_service(self) -> None:
        """Restart cinder-volume so it picks up the backend change.

        Tries the configured unit first, then common fallbacks across packaging
        (systemd ``cinder-volume``, devstack ``devstack@c-vol``, RDO/RHOSP
        ``openstack-cinder-volume``). A new backend doesn't require an API
        restart — cinder-volume loads it and the scheduler learns it over RPC.
        """
        unit = self.ctx.target.get("volume_service", "cinder-volume")
        candidates = [unit, "cinder-volume", "devstack@c-vol",
                      "openstack-cinder-volume"]
        # De-dup while preserving order.
        seen: set[str] = set()
        ordered = [c for c in candidates if not (c in seen or seen.add(c))]
        chain = " || ".join(f"systemctl restart {shlex.quote(c)} 2>/dev/null"
                            for c in ordered)
        await self.ctx.emit(f"Restarting cinder-volume (trying: {', '.join(ordered)})")
        await self._ssh(f"{chain} || (echo 'no cinder-volume unit matched' >&2; false)")

    @staticmethod
    def _pkg_install_cmd(*packages: str) -> str:
        """A portable, run-at-root shell snippet to install OS packages.

        OpenStack controllers span distros — Debian/Ubuntu (apt), RHEL/CentOS/
        Rocky/Alma and RDO/RHOSP (dnf/yum), SUSE (zypper). Rather than assume a
        family, detect the available package manager at runtime and use its
        non-interactive install form. Runs under sudo via the caller.
        """
        pkgs = " ".join(shlex.quote(p) for p in packages)
        return (
            'if command -v apt-get >/dev/null 2>&1; then '
            f'DEBIAN_FRONTEND=noninteractive apt-get install -y -q {pkgs}; '
            'elif command -v dnf >/dev/null 2>&1; then '
            f'dnf install -y {pkgs}; '
            'elif command -v yum >/dev/null 2>&1; then '
            f'yum install -y {pkgs}; '
            'elif command -v zypper >/dev/null 2>&1; then '
            f'zypper --non-interactive install {pkgs}; '
            'else echo "no supported package manager (apt/dnf/yum/zypper)" >&2; exit 1; fi'
        )

    async def _ensure_py_pure_client(self) -> str:
        """Ensure the Everpure driver's ``pypureclient`` dependency is importable.

        ``py-pure-client`` is the one Everpure-specific runtime dependency of the
        Cinder FlashArray driver and is not bundled with cinder. Installs it into
        the same interpreter cinder runs under (system python3 / dist-packages on
        a packaged install). PEP-668 environments need ``--break-system-packages``.
        Idempotent: a successful import short-circuits.
        """
        check = "python3 -c 'import pypureclient' 2>/dev/null && echo present || echo missing"
        if self._mock_or_dry():
            # run_ssh produces no output in mock/dry-run, so the import probe can
            # never report "present" and the verify step below would always fail.
            await self.ctx.emit(
                "[mock/dry-run] skipping py-pure-client install check")
            return "already_present"
        state = (await self._ssh(check, check=False, sudo=False)).strip()
        if state.endswith("present"):
            await self.ctx.emit("py-pure-client already installed")
            return "already_present"
        await self.ctx.emit("Installing py-pure-client (Everpure Cinder driver dependency) ...")
        # cinder runs under the system python3; ensure pip is available there.
        has_pip = (await self._ssh(
            "python3 -m pip --version >/dev/null 2>&1 && echo yes || echo no",
            check=False, sudo=False)).strip()
        if has_pip.endswith("no"):
            await self.ctx.emit("pip not present; installing python3-pip via the host package manager ...")
            await self._ssh(self._pkg_install_cmd("python3-pip"))
        await self._ssh(
            "python3 -m pip install --upgrade --break-system-packages py-pure-client "
            "|| python3 -m pip install --upgrade py-pure-client")
        verify = (await self._ssh(check, check=False, sudo=False)).strip()
        if not verify.endswith("present"):
            raise ConnectionValidationError(
                "py-pure-client install did not result in an importable pypureclient")
        return "installed"

    @staticmethod
    def _strip_scheme(endpoint: str) -> str:
        """Reduce a FlashArray management endpoint to a bare host (san_ip).

        ``ctx.array.endpoint`` may be a bare host/IP (``192.0.2.10``) or a URL
        (``https://192.0.2.10``); Cinder's ``san_ip`` wants only the host portion,
        so strip any scheme and trailing path/port.
        """
        host = (endpoint or "").strip()
        if "://" in host:
            host = host.split("://", 1)[1]
        # Drop any path component, then a trailing :port if present.
        host = host.split("/", 1)[0]
        if host.count(":") == 1:  # host:port (leave IPv6 ::-forms alone)
            host = host.split(":", 1)[0]
        return host

    def _resolve_san_ip(self, explicit: str = "") -> str:
        """Resolve the Cinder ``san_ip``.

        Prefers an explicit override (method kwarg or, for back-compat, a target
        value), otherwise derives the host from the associated array's management
        endpoint. san_ip is no longer an operator-entered form field — it comes
        from the hypervisor's associated FlashArray.
        """
        explicit = explicit or self.ctx.target.get("san_ip", "")
        if explicit:
            return self._strip_scheme(explicit)
        if self.ctx.array is not None:
            return self._strip_scheme(self.ctx.array.endpoint)
        return ""

    async def _resolve_api_token(self, explicit: str = "") -> str:
        """Use an explicit token, else the array's original token, else mint one.

        The token is no longer an operator-entered form field; it comes from the
        associated FlashArray via ``ctx.resolve_token``. An explicit override
        (method kwarg) still wins for advanced/manual use.
        """
        # Prefer an explicit override, else reuse the array's original token.
        token = self.ctx.resolve_token(explicit or None)
        if token:
            return token
        if self.ctx.array is not None:
            await self.ctx.emit("No token available; minting one from the FlashArray")
            return await self.ctx.array.create_api_token("openstack-cinder")
        # No token and no array: fall back to a placeholder so dry-runs still plan.
        return "REPLACE_WITH_API_TOKEN"

    def _ssh_kwargs(self) -> dict[str, Any]:
        """SSH connection kwargs for runner.discover_interfaces (mock-safe)."""
        return {
            "username": self.ctx.target.get("ssh_user", "stack"),
            "password": self.ctx.target.get("ssh_password"),
            "key": self.ctx.target.get("ssh_key"),
        }

    # ---------------------------------------------------- discovery (dropdowns) ---
    async def discover_options(self, kind: str) -> list[dict[str, Any]]:
        """Enumerate bindable interfaces/HBAs for a dynamic form field.

        These are discovered on the OpenStack *controller* over SSH (best-effort,
        mock-safe). Note that the compute-host iscsiadm/NVMe iface binding that
        actually carries data-path traffic is host-managed and is *separate* from
        what is enumerated here. TODO(doc-validate): surface compute-host NICs.
        """
        host = self.ctx.target.get("controller_host") or self.ctx.target.get("host")
        if not host:
            return []
        if kind in (DiscoveryKind.NICS.value, DiscoveryKind.NVME_SOURCES.value,
                    DiscoveryKind.FC_HBAS.value):
            try:
                return await self.ctx.runner.discover_interfaces(
                    host, kind, **self._ssh_kwargs())
            except Exception as exc:  # best-effort; never block the form
                await self.ctx.emit(f"discover_options({kind}) failed: {exc}")
                return []
        return []

    # ----------------------------------------------------- cluster awareness ---
    def _controller_host(self) -> str:
        return (self.ctx.target.get("controller_host")
                or self.ctx.target.get("host") or self.ctx.target.name)

    @staticmethod
    def _short_host(host: str) -> str:
        """A Cinder service ``host`` is ``hostname@backend``; take the hostname."""
        return (host or "").split("@", 1)[0].strip()

    async def list_nodes(self) -> list[ClusterNode]:
        """Enumerate the OpenStack "cluster": controller(s) + compute hosts.

        For OpenStack the cluster is the controller node(s) that run
        ``cinder-volume`` (where the Everpure backend config lives) plus the Nova
        ``compute`` hosts (where os-brick performs the actual iSCSI/NVMe/FC
        attach). We discover them over SSH to the controller from the service
        catalogs:

        * ``openstack volume service list -f json`` -> ``cinder-volume`` hosts
          (role ``controller``)
        * ``openstack compute service list -f json`` -> ``nova-compute`` hosts
          (role ``compute``)

        In mock/dry-run ``run_ssh`` returns no output, so we synthesize a
        controller + two compute hosts so wizard/cluster flows stay exercisable.
        If discovery yields nothing (e.g. the CLIs are unavailable), we fall back
        to the single configured ``controller_host``.
        """
        controller = self._controller_host()

        # In mock/dry-run, run_ssh returns no output, so synthesize a cluster.
        if self.ctx.dry_run or getattr(self.ctx.runner, "mock", False):
            return self._synthetic_nodes(controller)

        nodes: dict[str, ClusterNode] = {}
        # cinder-volume hosts -> controller role.
        vol_out = await self._cli(
            "openstack volume service list -f json 2>/dev/null || true", check=False)
        for svc in self._parse_service_list(vol_out):
            if svc.get("Binary") != "cinder-volume":
                continue
            name = self._short_host(svc.get("Host", ""))
            if not name:
                continue
            nodes.setdefault(name, ClusterNode(
                name=name, host=name,
                info={"role": "controller", "binary": "cinder-volume",
                      "status": svc.get("Status"), "state": svc.get("State")}))
        # nova-compute hosts -> compute role.
        comp_out = await self._cli(
            "openstack compute service list -f json 2>/dev/null || true", check=False)
        for svc in self._parse_service_list(comp_out):
            if svc.get("Binary") != "nova-compute":
                continue
            name = self._short_host(svc.get("Host", ""))
            if not name or name in nodes:
                continue
            nodes[name] = ClusterNode(
                name=name, host=name,
                info={"role": "compute", "binary": "nova-compute",
                      "status": svc.get("Status"), "state": svc.get("State")})

        if not nodes:
            # Discovery produced nothing: fall back to the single controller.
            return [ClusterNode(name=str(controller), host=str(controller),
                                info={"role": "controller"})]
        return list(nodes.values())

    @staticmethod
    def _synthetic_nodes(controller: str) -> list[ClusterNode]:
        """Mock/dry-run cluster: one controller + two compute hosts."""
        return [
            ClusterNode(name=str(controller), host=str(controller),
                        info={"role": "controller", "binary": "cinder-volume"}),
            ClusterNode(name="compute-0", host="compute-0",
                        info={"role": "compute", "binary": "nova-compute"}),
            ClusterNode(name="compute-1", host="compute-1",
                        info={"role": "compute", "binary": "nova-compute"}),
        ]

    @staticmethod
    def _parse_service_list(out: str) -> list[dict[str, Any]]:
        """Parse ``openstack ... service list -f json`` output (best-effort)."""
        out = (out or "").strip()
        if not out:
            return []
        try:
            data = json.loads(out)
        except (ValueError, TypeError):
            return []
        if isinstance(data, list):
            return [d for d in data if isinstance(d, dict)]
        return []

    async def validate_cluster(self, **params: Any) -> OpResult:
        """Validate the OpenStack cluster for storage connectivity.

        Unlike host-managed hypervisors (Proxmox/XCP-ng), OpenStack interface
        binding is *host-managed*: os-brick performs the iSCSI/NVMe/FC attach on
        each Nova compute host using that host's own iscsiadm ifaces / NVMe
        host-traddr / FC HBAs. The array-side control is the iSCSI target subnet
        (``pure_iscsi_cidr``) rendered into ``cinder.conf`` on the controller. A
        strict per-node NIC comparison (as :func:`compare_node_interfaces` does
        for clustered host hypervisors) is therefore not generally available from
        the controller, so this connector does not attempt one.

        We list the nodes (controller(s) + compute hosts) and confirm the
        array-side iSCSI CIDR is set/reachable, returning ``OpResult.ok`` with a
        clear note that compute-host iSCSI/NVMe iface config is host-managed.

        TODO(doc-validate): surfacing per-compute-host NICs (and validating their
        reachability to the array iSCSI CIDR) would require SSH to each compute
        host; the controller CLIs do not expose this. Confirm the desired UX.
        """
        nodes = await self.list_nodes()
        controllers = [n for n in nodes if n.info.get("role") == "controller"]
        computes = [n for n in nodes if n.info.get("role") == "compute"]

        iscsi_cidr = (params.get("pure_iscsi_cidr")
                      or self.ctx.target.get("pure_iscsi_cidr"))
        iscsi_cidr_list = (params.get("pure_iscsi_cidr_list")
                           or self.ctx.target.get("pure_iscsi_cidr_list"))
        protocol = self.ctx.target.get("protocol", "iscsi")

        await self.ctx.emit(
            f"Cluster: {len(controllers)} controller(s) running cinder-volume, "
            f"{len(computes)} compute host(s).")
        await self.ctx.emit(
            "Compute-host iSCSI/NVMe iface binding is host-managed (os-brick + "
            "host networking/multipath); not validated per-node from the controller.")
        if protocol == "iscsi":
            cidr = iscsi_cidr or iscsi_cidr_list
            if cidr:
                await self.ctx.emit(
                    f"Array iSCSI target CIDR (array-side control): {cidr}. "
                    "Ensure each compute host can reach this subnet.")
            else:
                await self.ctx.emit(
                    "No pure_iscsi_cidr set: the driver advertises all array "
                    "iSCSI target ports. Confirm compute-host reachability.")

        return OpResult.ok(
            f"{len(nodes)} node(s): {len(controllers)} controller / "
            f"{len(computes)} compute. Compute-host iSCSI/NVMe iface config is "
            "host-managed; confirm the array iSCSI CIDR is reachable. "
            "TODO(doc-validate): per-compute-host NIC validation not available "
            "from the controller.",
            nodes=[n.to_dict() for n in nodes],
            controllers=[n.name for n in controllers],
            computes=[n.name for n in computes],
            iscsi_cidr=iscsi_cidr or iscsi_cidr_list,
            iface_binding="host-managed")

    # ----------------------------------------------------------- operations ---
    async def validate_connection(self) -> OpResult:
        host = self.ctx.target.get("controller_host") or self.ctx.target.get("host")
        await self.ctx.emit(f"Validating connection to OpenStack controller {host} ...")
        if not host:
            raise ConnectionValidationError("No controller_host configured")
        if not (self.ctx.target.get("ssh_password") or self.ctx.target.get("ssh_key")):
            # Not fatal in mock mode, but warn — real SSH needs one.
            await self.ctx.emit("Warning: neither ssh_password nor ssh_key set")
        # Confirm the CLI is reachable and cinder.conf exists.
        await self._ssh("openstack --version || true", check=False)
        await self._ssh(
            f"test -f {shlex.quote(self._conf_path())} && echo cinder.conf-present || true",
            check=False,
        )
        return OpResult.ok(f"Connected to {host}", host=host)

    async def deploy_integration(self, **params: Any) -> OpResult:
        backend = self._backend_name()
        protocol = self.ctx.target.get("protocol", "iscsi")
        path = self._conf_path()
        await self.ctx.emit(f"Deploying Cinder backend {backend!r} ({protocol}) on controller")

        existing = await self._read_cinder_conf()
        if pure.stanza_exists(existing, backend):
            return OpResult.ok(
                f"Backend stanza [{backend}] already present in {path} (no-op)",
                artifacts={"backend": backend}, status="already_present")

        # san_ip and the API token are derived from the associated FlashArray
        # (not operator-entered). Method kwargs remain optional overrides.
        san_ip = self._resolve_san_ip(params.get("san_ip", ""))
        token = await self._resolve_api_token(params.get("pure_api_token", ""))
        await self.ctx.emit(f"Using FlashArray management IP (san_ip): {san_ip or '(unset)'}")
        # Without an associated array, neither value can be derived. Fail clearly
        # outside dry-run; in dry-run we keep planning with the placeholder token.
        if not self.ctx.dry_run and (not san_ip or token == "REPLACE_WITH_API_TOKEN"):
            return OpResult.fail(
                "No FlashArray is associated with this hypervisor, so san_ip / "
                "pure_api_token cannot be derived. Associate an array, or pass "
                "san_ip and pure_api_token explicitly to this action.")
        # Interface-binding inputs may come from the action form or the target.
        iscsi_cidr = (params.get("pure_iscsi_cidr")
                      or self.ctx.target.get("pure_iscsi_cidr"))
        iscsi_cidr_list = (params.get("pure_iscsi_cidr_list")
                           or self.ctx.target.get("pure_iscsi_cidr_list"))
        nvme_options = (params.get("nvme_options")
                        or self.ctx.target.get("nvme_options"))
        if protocol == "iscsi" and iscsi_cidr:
            await self.ctx.emit(f"Binding iSCSI target subnet: pure_iscsi_cidr = {iscsi_cidr}")
        stanza = pure.render_stanza(
            backend_name=backend,
            protocol=protocol,
            san_ip=san_ip,
            pure_api_token=token,
            eradicate_on_delete=bool(params.get("eradicate_on_delete", False)),
            pure_iscsi_cidr=iscsi_cidr,
            pure_iscsi_cidr_list=iscsi_cidr_list,
            nvme_options=nvme_options,
        )
        enabled = pure.build_enabled_backends(existing, backend)

        # Show the planned diff before touching anything.
        await self.ctx.emit("--- planned cinder.conf changes ---")
        for line in stanza.splitlines():
            await self.ctx.emit(f"+ {line}")
        if enabled is not None:
            await self.ctx.emit(f"~ enabled_backends = {enabled}")
        await self.ctx.emit("-----------------------------------")

        if self.ctx.dry_run:
            return OpResult.ok("Dry run: cinder.conf not modified",
                               artifacts={"backend": backend},
                               stanza=stanza, enabled_backends=enabled,
                               status="planned")

        # Install the Everpure-specific driver dependency before enabling the backend
        # (cinder-volume fails to load the driver without pypureclient).
        pure_client = await self._ensure_py_pure_client()
        # Append the stanza.
        heredoc = f"cat >> {shlex.quote(path)} <<'PHIF_EOF'\n\n{stanza}PHIF_EOF"
        await self._ssh(heredoc)
        if enabled is not None:
            # crudini is the standard INI editor on OpenStack controllers; ensure
            # it's present (OS-aware) rather than assuming the base install has it.
            await self._ssh(
                "command -v crudini >/dev/null 2>&1 || { "
                + self._pkg_install_cmd("crudini") + "; }")
            await self._ssh(
                f"crudini --set {shlex.quote(path)} DEFAULT enabled_backends "
                f"{shlex.quote(enabled)}")
        await self._restart_volume_service()
        return OpResult.ok(f"Backend {backend!r} deployed and cinder-volume restarted",
                           artifacts={"backend": backend},
                           enabled_backends=enabled,
                           py_pure_client=pure_client)

    async def configure(self, volume_type: str = "pure", qos_name: str = "",
                        max_iops: int | None = None, max_bw: int | None = None,
                        **_: Any) -> OpResult:
        backend = self._backend_name()
        await self.ctx.emit(f"Creating Cinder volume type {volume_type!r} -> backend {backend!r}")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry run: would create volume type {volume_type}",
                               status="planned")
        await self._cli(
            f"openstack volume type create {shlex.quote(volume_type)} || true",
            check=False)
        await self._cli(
            f"openstack volume type set --property volume_backend_name={shlex.quote(backend)} "
            f"{shlex.quote(volume_type)}")
        artifacts: dict[str, Any] = {"volume_type": volume_type}
        if qos_name:
            qos = await self.set_qos(qos_name=qos_name, volume_type=volume_type,
                                     max_iops=max_iops, max_bw=max_bw)
            artifacts.update(qos.artifacts)
        return OpResult.ok(f"Volume type {volume_type!r} configured", artifacts=artifacts)

    # ------------------------------------------------ compute-host data path ---
    @staticmethod
    def _as_list(val: Any) -> list[str]:
        """Coerce a multiselect/CSV/list value into a clean list of strings."""
        if val is None:
            return []
        if isinstance(val, str):
            return [v.strip() for v in val.split(",") if v.strip()]
        if isinstance(val, (list, tuple, set)):
            return [str(v).strip() for v in val if str(v).strip()]
        return [str(val).strip()] if str(val).strip() else []

    def _compute_hosts(self) -> list[str]:
        """SSH-reachable nova-compute hosts to configure (data path).

        Prefers the operator-supplied ``compute_hosts`` (the appliance must reach
        these with the same ssh_user/key); falls back to the controller host for
        the all-in-one / single-node case.
        """
        explicit = self._as_list(self.ctx.target.get("compute_hosts"))
        if explicit:
            return explicit
        host = self.ctx.target.get("controller_host") or self.ctx.target.get("host")
        return [host] if host else []

    @staticmethod
    def _ensure_iscsi_multipath_cmd() -> str:
        """OS-aware install + enable of open-iscsi + multipath-tools.

        Package names differ by family (Debian: open-iscsi/multipath-tools;
        RHEL: iscsi-initiator-utils/device-mapper-multipath). Idempotent.
        """
        return (
            'if command -v apt-get >/dev/null 2>&1; then '
            'DEBIAN_FRONTEND=noninteractive apt-get install -y -q open-iscsi multipath-tools || true; '
            'elif command -v dnf >/dev/null 2>&1; then '
            'dnf install -y iscsi-initiator-utils device-mapper-multipath || true; '
            'elif command -v yum >/dev/null 2>&1; then '
            'yum install -y iscsi-initiator-utils device-mapper-multipath || true; '
            'elif command -v zypper >/dev/null 2>&1; then '
            'zypper --non-interactive install open-iscsi multipath-tools || true; fi; '
            # mpathconf (RHEL) seeds /etc/multipath.conf; on Debian create a minimal
            # one if absent so multipathd starts and includes conf.d drop-ins.
            'command -v mpathconf >/dev/null 2>&1 && mpathconf --enable 2>/dev/null || true; '
            "[ -f /etc/multipath.conf ] || printf 'defaults {\\n    user_friendly_names no\\n}\\n' > /etc/multipath.conf; "
            'systemctl enable --now iscsid multipathd 2>/dev/null || '
            'systemctl enable --now open-iscsi multipathd 2>/dev/null || true'
        )

    async def _restart_nova_compute(self, host: str) -> None:
        """Restart nova-compute on ``host`` across packaging variants."""
        units = ["nova-compute", "devstack@n-cpu", "openstack-nova-compute"]
        chain = " || ".join(f"systemctl restart {shlex.quote(u)} 2>/dev/null"
                            for u in units)
        await self._ssh(f"{chain} || (echo 'no nova-compute unit matched' >&2; false)",
                        host=host)

    async def setup_connectivity(self, iscsi_nics: Any = None, **params: Any) -> OpResult:
        """Configure the iSCSI/multipath data path on each nova-compute host.

        The Cinder Everpure driver (on the controller) never touches compute-host
        block plumbing; os-brick does the attach but relies on host config. This
        step, per compute host: installs+enables open-iscsi/multipathd, drops in
        the Everpure multipath device stanza, enables Nova multipath
        (``[libvirt] volume_use_multipath=true``) and restarts nova-compute, binds
        iSCSI ifaces to the selected NICs, and applies the multi-NIC ARP-flux
        sysctls (``arp_ignore``/``arp_announce=2``) on those NICs.
        """
        nics = self._as_list(iscsi_nics) or self._as_list(self.ctx.target.get("iscsi_nics"))
        nova_conf = params.get("nova_conf_path") or self.ctx.target.get(
            "nova_conf_path", "/etc/nova/nova.conf")
        hosts = self._compute_hosts()
        if not hosts:
            return OpResult.fail(
                "No compute hosts to configure. Set 'compute_hosts' (or a "
                "controller_host for all-in-one).")
        await self.ctx.emit(
            f"Configuring data path on {len(hosts)} compute host(s): {hosts}; "
            f"iSCSI NICs={nics or '(none — multipath only)'}")

        dropin = pure.render_pure_multipath_dropin()
        if self.ctx.dry_run:
            await self.ctx.emit("--- planned (dry run): per compute host ---")
            await self.ctx.emit("+ install/enable open-iscsi + multipathd")
            await self.ctx.emit("+ /etc/multipath/conf.d/99-pure.conf (Everpure device stanza)")
            await self.ctx.emit(f"~ {nova_conf} [libvirt] volume_use_multipath = true")
            for nic in nics:
                await self.ctx.emit(f"+ iscsiadm iface bind {nic}; arp_ignore/arp_announce=2")
            return OpResult.ok("Dry run: compute hosts not modified", status="planned",
                               compute_hosts=hosts, iscsi_nics=nics)

        configured: list[str] = []
        for host in hosts:
            await self.ctx.emit(f"[{host}] ensuring open-iscsi + multipathd ...")
            await self._ssh(self._ensure_iscsi_multipath_cmd(), host=host, check=False)

            await self.ctx.emit(f"[{host}] writing Everpure multipath drop-in ...")
            heredoc = ("mkdir -p /etc/multipath/conf.d && "
                       "cat > /etc/multipath/conf.d/99-pure.conf <<'PHIF_MP_EOF'\n"
                       f"{dropin}PHIF_MP_EOF")
            await self._ssh(heredoc, host=host)

            await self.ctx.emit(f"[{host}] enabling Nova multipath in {nova_conf} ...")
            await self._ssh(
                "command -v crudini >/dev/null 2>&1 || { "
                + self._pkg_install_cmd("crudini") + "; }", host=host, check=False)
            await self._ssh(
                f"crudini --set {shlex.quote(nova_conf)} libvirt volume_use_multipath true",
                host=host)

            if nics:
                await self.ctx.emit(f"[{host}] binding iSCSI ifaces to {nics} ...")
                for nic in nics:
                    iface = f"phif-{nic}"
                    await self._ssh(
                        f"iscsiadm -m iface -I {shlex.quote(iface)} -o new 2>/dev/null || true; "
                        f"iscsiadm -m iface -I {shlex.quote(iface)} -o update "
                        f"-n iface.net_ifacename -v {shlex.quote(nic)}",
                        host=host, check=False)
                await self.ctx.emit(f"[{host}] applying iSCSI ARP-flux sysctls for {nics} ...")
                await self._ssh(arp_flux_cmd(nics), host=host, check=False)

            await self.ctx.emit(f"[{host}] reloading multipathd + restarting nova-compute ...")
            await self._ssh("systemctl reload multipathd 2>/dev/null || "
                            "systemctl restart multipathd 2>/dev/null || true",
                            host=host, check=False)
            await self._restart_nova_compute(host)
            configured.append(host)

        return OpResult.ok(
            f"Data path configured on {len(configured)} compute host(s): {configured}",
            compute_hosts=configured, iscsi_nics=nics, nova_multipath=True)

    # ============================================================= migration ===
    # OpenStack as a migration DESTINATION (Nova + Cinder on the shared FlashArray).
    # Model (mirrors HPE VME's "provision, then hand back FA volume names"):
    #   create_vm           -> a Cinder volume (Everpure backend) per source disk, Neutron
    #                          ports (MAC preserved), a matching flavor, then boot a
    #                          Nova instance from those volumes and STOP it (Nova has
    #                          no create-stopped for boot-from-volume; the volumes are
    #                          still empty, so the brief boot writes nothing).
    #   create_managed_disk -> hand back the FA volume name (volume-<uuid>) the Everpure
    #                          Cinder driver created, for the array copy-overwrite.
    #   start_vm            -> power on (boots the now-populated boot volume).
    # Cinder/Nova manage the FlashArray host objects + LUN attach at boot, so there
    # is no PHIF-driven host-group connect here.

    _GIB = 1 << 30

    def _migrate_volume_type(self) -> str:
        """Cinder volume type for migration disks (the deployed Everpure backend's type)."""
        return (self.ctx.target.get("migration_volume_type")
                or self.ctx.target.get("volume_type")
                or self._backend_name())

    async def _cli_json(self, command: str, *, check: bool = True,
                        retries: int = 5) -> Any:
        """Run an openstack CLI with JSON output and parse it, retrying transient 5xx.

        stderr is merged into the stream (CLI deprecation warnings etc.), so the
        JSON is located by its first ``[``/``{`` and decoded with ``raw_decode``
        (trailing noise ignored). Returns None when the command produced no JSON
        (a non-transient failure — its error text is visible in the job log)."""
        for attempt in range(max(1, retries)):
            out = (await self._cli(f"{command} -f json", check=False, retries=1) or "")
            starts = [i for i in (out.find("["), out.find("{")) if i != -1]
            if starts:
                try:
                    return json.JSONDecoder().raw_decode(out[min(starts):])[0]
                except ValueError:
                    pass
            if attempt < retries - 1 and self._is_transient(out):
                await self.ctx.emit(
                    f"[openstack] transient API error (json); retry {attempt + 1}/{retries}")
                await asyncio.sleep(3 * (attempt + 1))
                continue
            return None

    async def _server_status(self, vm_ref: str) -> str:
        out = (await self._cli(
            f"openstack server show {shlex.quote(vm_ref)} -f value -c status 2>/dev/null",
            check=False) or "").strip()
        return out.upper()

    async def _await_server(self, vm_ref: str, target: str, *, attempts: int = 30,
                            delay: float = 4.0) -> None:
        # ``target`` may be a "|"-separated set of acceptable states.
        targets = {t for t in target.upper().split("|") if t}
        last = ""
        for _ in range(attempts):
            last = await self._server_status(vm_ref)
            if last in targets:
                return
            if last == "ERROR":
                raise ConnectionValidationError(f"server {vm_ref} entered ERROR state")
            await asyncio.sleep(delay)
        raise ConnectionValidationError(
            f"server {vm_ref} did not reach {target} (last={last or 'unknown'})")

    async def _await_volume(self, vol_id: str, target: str = "available", *,
                            attempts: int = 30, delay: float = 3.0) -> None:
        for _ in range(attempts):
            st = (await self._cli(
                f"openstack volume show {shlex.quote(vol_id)} -f value -c status 2>/dev/null",
                check=False) or "").strip().lower()
            if st == target:
                return
            if st == "error":
                raise ConnectionValidationError(f"volume {vol_id} entered error state")
            await asyncio.sleep(delay)
        raise ConnectionValidationError(f"volume {vol_id} did not reach {target}")

    async def list_networks(self) -> list[dict[str, Any]]:
        data = await self._cli_json("openstack network list", check=False) or []
        out: list[dict[str, Any]] = []
        for n in data:
            nid = n.get("ID") or n.get("id")
            if nid:
                out.append({"id": nid, "name": n.get("Name") or n.get("name") or nid})
        return out

    async def list_placements(self) -> list[dict[str, Any]]:
        """Compute availability zones as 'clusters' + the Everpure volume type as storage.

        Advisory for OpenStack (Nova schedules; Cinder routes to the Everpure backend by
        volume type). Returns [] when there's no Everpure volume type so the wizard just
        auto-places."""
        vt = self._migrate_volume_type()
        types = await self._cli_json("openstack volume type list", check=False) or []
        if not any((t.get("Name") or t.get("name")) == vt for t in types):
            return []
        storage = [{"id": vt, "name": vt, "kind": "volume_type"}]
        azs = await self._cli_json("openstack availability zone list --compute",
                                   check=False) or []
        names: list[str] = []
        for a in azs:
            zn = a.get("Zone Name") or a.get("zoneName") or a.get("Name")
            if zn and zn not in names and zn.lower() != "internal":
                names.append(zn)
        return [{"cluster": {"id": c, "name": c}, "storage": storage}
                for c in (names or ["nova"])]

    async def list_vms(self) -> list[dict[str, Any]]:
        data = await self._cli_json("openstack server list", check=False) or []
        out: list[dict[str, Any]] = []
        for s in data:
            sid = s.get("ID") or s.get("id")
            if not sid:
                continue
            status = (s.get("Status") or s.get("status") or "").upper()
            out.append({"id": sid, "name": s.get("Name") or s.get("name") or sid,
                        "power_state": {"ACTIVE": "running",
                                        "SHUTOFF": "stopped"}.get(status, "unknown")})
        return out

    async def power_state(self, vm_ref: str) -> str:
        return {"ACTIVE": "running", "SHUTOFF": "stopped"}.get(
            await self._server_status(vm_ref), "unknown")

    @staticmethod
    def _device_order(device: str) -> int:
        """Order a disk by its attach device (…/vda=0, vdb=1, …). Unknown → large."""
        tail = (device or "").rstrip("0123456789").rstrip("/").split("/")[-1]
        last = (device or "").rsplit("/", 1)[-1]
        # take the trailing alpha run (e.g. 'vda' -> 'a', 'sdb' -> 'b')
        letters = "".join(c for c in last if c.isalpha())
        return (ord(letters[-1]) - ord("a")) if letters else 99

    async def capture_vm_spec(self, vm_ref: str) -> VmSpec:
        """READ-ONLY: read a Nova instance into a VmSpec for migration OUT.

        Each attached Cinder volume is on the Everpure backend as ``volume-<uuid>`` on
        the FlashArray; the migrate service's resolve_volumes fills serial/size from
        the array. Flavor → vcpus/memory; the boot volume's image metadata →
        firmware (UEFI vs BIOS); Neutron ports → NIC MAC + source network.
        """
        srv = await self._cli_json(f"openstack server show {shlex.quote(vm_ref)}")
        if not srv:
            raise ConnectionValidationError(f"OpenStack server {vm_ref!r} not found")
        name = srv.get("name") or srv.get("Name") or vm_ref

        # Flavor → vcpus/memory (server show may inline a dict, or give a name/id).
        vcpus, ram_mb = 1, 0
        flavor = srv.get("flavor")
        if isinstance(flavor, dict):
            vcpus = int(flavor.get("vcpus") or flavor.get("VCPUs") or 1)
            ram_mb = int(flavor.get("ram") or flavor.get("RAM") or 0)
        elif flavor:
            fname = str(flavor).split("(")[0].strip()
            fl = await self._cli_json(f"openstack flavor show {shlex.quote(fname)}")
            if fl:
                vcpus = int(fl.get("vcpus") or 1)
                ram_mb = int(fl.get("ram") or 0)

        # Attached Cinder volumes → disks (resolve each to its FA volume-<uuid>).
        vols = (srv.get("volumes_attached")
                or srv.get("os-extended-volumes:volumes_attached") or [])
        disks: list[DiskSpec] = []
        firmware = "bios"
        for v in vols:
            vid = v.get("id") if isinstance(v, dict) else v
            if not vid:
                continue
            vshow = await self._cli_json(f"openstack volume show {shlex.quote(vid)}")
            if not vshow:
                raise ConnectionValidationError(f"could not read Cinder volume {vid}")
            device = ""
            for a in (vshow.get("attachments") or []):
                if a.get("server_id") in (vm_ref, None) or a.get("instance") == vm_ref:
                    device = a.get("device", "") or device
            vim = vshow.get("volume_image_metadata") or {}
            if str(vim.get("hw_firmware_type", "")).lower() == "uefi":
                firmware = "uefi"
            order = self._device_order(device)
            bootable = str(vshow.get("bootable", "")).lower() == "true"
            disks.append(DiskSpec(
                DiskIdentity(fa_volume=f"volume-{vid}",
                             size_bytes=int(vshow.get("size") or 0) * self._GIB),
                bus="scsi", order=order, boot=bootable and order == 0,
                source_ref=str(vid)))
        disks.sort(key=lambda d: d.order)
        # Renumber to contiguous 0..N and ensure exactly one boot disk.
        for i, d in enumerate(disks):
            d.order = i
        if disks and not any(d.boot for d in disks):
            disks[0].boot = True

        # Neutron ports → NICs (MAC + source network id).
        nics: list[NicSpec] = []
        ports = await self._cli_json(
            f"openstack port list --server {shlex.quote(vm_ref)}") or []
        for i, p in enumerate(ports):
            pid = p.get("ID") or p.get("id")
            mac = p.get("MAC Address") or p.get("mac_address")
            net = ""
            if pid:
                pshow = await self._cli_json(f"openstack port show {shlex.quote(pid)}")
                net = (pshow or {}).get("network_id") or ""
                mac = mac or (pshow or {}).get("mac_address")
            nics.append(NicSpec(mac=mac or "", source_network=net or "",
                                model="virtio", order=i))

        return VmSpec(
            name=name, source_ref=str(vm_ref), vcpus=vcpus,
            memory_bytes=ram_mb * (1 << 20), firmware=firmware,
            disks=disks, nics=nics, raw={"openstack_source": True})

    def migration_host_group(self) -> str:
        # Cinder/Nova manage the FlashArray host objects + attach at boot, so a PHIF
        # host group isn't used for the attach. Return a non-empty sentinel so the
        # migration preflight passes (require_host_objects is overridden to OK).
        return self.ctx.target.get("host_group") or "cinder-managed"

    async def require_host_objects(self, host_group: str) -> OpResult:
        return OpResult.ok("Cinder manages FlashArray host objects at attach time")

    async def _cleanup_artifacts(self) -> None:
        """Best-effort delete of the Nova instance + Cinder volumes + Neutron ports
        this migration created. Used on a create_vm partial failure (before a vm_ref
        exists, so the orchestrator's rollback can't see them) and on
        delete_vm(keep_disks=False)."""
        inst = getattr(self, "_mig_instance", None)
        if inst:
            await self._cli(
                f"openstack server delete --wait {shlex.quote(inst)} 2>/dev/null || true",
                check=False)
            self._mig_instance = None
        for d in getattr(self, "_mig_disks", {}).values():
            cid = d.get("cinder_id")
            if cid:
                await self._cli(
                    f"openstack volume delete {shlex.quote(cid)} 2>/dev/null || true",
                    check=False)
        for pid in getattr(self, "_mig_ports", []):
            await self._cli(f"openstack port delete {shlex.quote(pid)} 2>/dev/null || true",
                            check=False)

    async def _resolve_flavor(self, vcpus: int, memory_bytes: int) -> str:
        ram_mb = max(1, int(round(memory_bytes / (1 << 20)))) if memory_bytes else 512
        vcpus = max(1, int(vcpus or 1))
        flavors = await self._cli_json("openstack flavor list --long", check=False) or []
        for f in flavors:
            try:
                if int(f.get("VCPUs") or f.get("vcpus") or 0) == vcpus and \
                   int(f.get("RAM") or f.get("ram") or 0) == ram_mb:
                    return f.get("ID") or f.get("id") or f.get("Name") or f.get("name")
            except (TypeError, ValueError):
                continue
        name = f"phifmig-{vcpus}vcpu-{ram_mb}mb"
        await self.ctx.emit(f"[openstack] creating flavor {name} ({vcpus} vCPU / {ram_mb} MiB)")
        await self._cli(
            f"openstack flavor create --vcpus {vcpus} --ram {ram_mb} --disk 0 "
            f"{shlex.quote(name)} || true", check=False)
        return name

    async def create_vm(self, spec: VmSpec, *, network_map: dict[str, str],
                        placement: dict[str, Any] | None = None) -> OpResult:
        placement = placement or {}
        vt = self._migrate_volume_type()
        az = placement.get("cluster") or None
        await self.ctx.emit(
            f"[openstack] creating destination instance {spec.name!r} "
            f"({spec.vcpus} vCPU, {spec.memory_bytes // (1 << 20)} MiB, "
            f"{len(spec.disks)} disk(s))")
        if self.ctx.dry_run:
            return OpResult.ok("dry run: would create Nova instance",
                               artifacts={"vm_ref": f"phifmig-dryrun-{spec.source_ref}"})

        # Per-migration state (the connector instance persists across the flow).
        self._mig_disks: dict[int, dict[str, Any]] = {}
        self._mig_ports: list[str] = []
        try:
            return await self._create_vm_inner(spec, network_map, vt, az)
        except Exception:
            # create_vm failed before returning a vm_ref, so the orchestrator's
            # rollback has nothing to delete — clean up our own partial work.
            await self.ctx.emit("[openstack] create_vm failed; cleaning up partial artifacts")
            await self._cleanup_artifacts()
            raise

    async def _create_vm_inner(self, spec: VmSpec, network_map: dict[str, str],
                               vt: str, az: Any) -> OpResult:
        # 1. A Cinder volume (Everpure backend) per source disk.
        ordered = sorted(spec.disks, key=lambda d: d.order)
        for disk in ordered:
            gib = max(1, math.ceil((disk.identity.size_bytes or 0) / self._GIB))
            vname = f"phifmig-dst-{spec.source_ref}-disk{disk.order}"
            vol = await self._cli_json(
                f"openstack volume create --type {shlex.quote(vt)} --size {gib} "
                f"{shlex.quote(vname)}")
            vid = (vol or {}).get("id") or (vol or {}).get("ID")
            if not vid:
                raise ConnectionValidationError(f"failed to create Cinder volume {vname}")
            await self._await_volume(vid, "available")
            self._mig_disks[disk.order] = {"cinder_id": vid, "fa": f"volume-{vid}",
                                           "boot": disk.boot, "name": vname}
            await self.ctx.emit(
                f"[openstack] created Cinder volume {vname} ({gib} GiB) -> FA volume-{vid}")
        # The boot volume must be flagged bootable for Nova's boot_index 0. For a
        # UEFI source, stamp the firmware on the boot volume's image metadata —
        # Nova reads it for boot-from-volume to launch OVMF/UEFI (otherwise it
        # boots BIOS and a UEFI-partitioned disk won't boot).
        for d in self._mig_disks.values():
            if d["boot"]:
                await self._cli(
                    f"openstack volume set --bootable {shlex.quote(d['cinder_id'])}",
                    check=False)
                # Make the create-time stop of the (empty, no-OS) boot disk an
                # immediate power-off instead of waiting Nova's full
                # shutdown_timeout (~60s). REMOVED again in finalize_destination_disks
                # so the migrated VM keeps normal (graceful) shutdown behavior.
                await self._cli(
                    f"openstack volume set --image-property os_shutdown_timeout=0 "
                    f"{shlex.quote(d['cinder_id'])}", check=False)
                if (spec.firmware or "bios").lower() == "uefi":
                    await self.ctx.emit("[openstack] marking boot volume UEFI (hw_firmware_type=uefi)")
                    await self._cli(
                        f"openstack volume set --image-property hw_firmware_type=uefi "
                        f"--image-property hw_machine_type=q35 "
                        f"{shlex.quote(d['cinder_id'])}", check=False)
                    if spec.secure_boot:
                        await self._cli(
                            f"openstack volume set --image-property os_secure_boot=required "
                            f"{shlex.quote(d['cinder_id'])}", check=False)

        # 2. Neutron ports, preserving the source MAC where allowed.
        for nic in sorted(spec.nics, key=lambda n: n.order):
            net = network_map.get(nic.source_network) or nic.source_network
            if not net:
                continue
            pname = f"phifmig-{spec.source_ref}-nic{nic.order}"
            mac = f"--mac-address {shlex.quote(nic.mac)} " if nic.mac else ""
            port = await self._cli_json(
                f"openstack port create --network {shlex.quote(net)} {mac}{shlex.quote(pname)}",
                check=False)
            pid = (port or {}).get("id") or (port or {}).get("ID")
            if not pid and nic.mac:  # MAC rejected? retry without pinning it
                port = await self._cli_json(
                    f"openstack port create --network {shlex.quote(net)} {shlex.quote(pname)}",
                    check=False)
                pid = (port or {}).get("id") or (port or {}).get("ID")
            if pid:
                self._mig_ports.append(pid)
                await self.ctx.emit(
                    f"[openstack] created port {pname} (mac {nic.mac or 'auto'})")

        # 3. Flavor (matching vCPU/RAM; disk 0 -> boot-from-volume).
        flavor = await self._resolve_flavor(spec.vcpus, spec.memory_bytes)

        # 4. Boot the instance from the volumes (boot_index 0 = boot disk).
        parts = [f"openstack server create --flavor {shlex.quote(flavor)}"]
        for disk in ordered:
            d = self._mig_disks[disk.order]
            parts.append(
                f"--block-device uuid={d['cinder_id']},source_type=volume,"
                f"destination_type=volume,boot_index={0 if disk.boot else -1},"
                f"delete_on_termination=false")
        for pid in self._mig_ports:
            parts.append(f"--nic port-id={pid}")
        if az:
            parts.append(f"--availability-zone {shlex.quote(str(az))}")
        parts.append(shlex.quote(spec.name))
        # Create WITHOUT --wait so we capture the instance id immediately; if the
        # build later fails (ERROR), we still know the id and can delete it (the
        # except handler / _cleanup_artifacts), avoiding a leaked ERROR instance.
        srv = await self._cli_json(" ".join(parts))
        vm_ref = (srv or {}).get("id") or (srv or {}).get("ID")
        if not vm_ref:
            raise ConnectionValidationError("openstack server create did not return an id")
        self._mig_instance = vm_ref
        self._dest_vm_ref = vm_ref
        # Wait for the build to reach ACTIVE (empty disks → boots to no-OS but ACTIVE).
        await self._await_server(vm_ref, "ACTIVE")

        # 5. Stop it: the empty-disk boot has no OS to ACK an ACPI shutdown, so the
        #    boot volume's os_shutdown_timeout=0 (set above) makes this an immediate
        #    power-off (~instant instead of ~60s). That metadata is removed again in
        #    finalize_destination_disks so it never persists on the migrated VM.
        await self._cli(f"openstack server stop {shlex.quote(vm_ref)} 2>/dev/null || true",
                        check=False)
        await self._await_server(vm_ref, "SHUTOFF")
        await self.ctx.emit(f"[openstack] instance {spec.name} created and stopped ({vm_ref})")
        return OpResult.ok(f"created OpenStack instance {spec.name}",
                           artifacts={"vm_ref": vm_ref})

    async def create_managed_disk(self, vm_ref: str, *, size_bytes: int,
                                  order: int, boot: bool) -> str:
        # The Cinder volumes were created in create_vm; hand back the FA volume name
        # so the array copy-with-overwrite targets it (HPE VME pattern).
        if self.ctx.dry_run:
            return f"volume-phifmig-dryrun-{order}"
        disk = getattr(self, "_mig_disks", {}).get(order)
        if not disk:
            raise ConnectionValidationError(
                f"no Cinder volume provisioned for disk order {order}")
        return disk["fa"]

    async def set_boot_order(self, vm_ref: str, disks: list) -> OpResult:
        return OpResult.ok("boot device set via Nova block-device boot_index")

    async def finalize_destination_disks(self, vm_ref: str, disks: list) -> OpResult:
        # The boot volume got a temporary os_shutdown_timeout=0 so the create-time
        # power-off was instant; remove it now (post-copy, pre-boot) so the migrated
        # VM keeps normal graceful-shutdown behavior.
        for d in getattr(self, "_mig_disks", {}).values():
            if d.get("boot") and d.get("cinder_id"):
                await self._cli(
                    f"openstack volume unset --image-property os_shutdown_timeout "
                    f"{shlex.quote(d['cinder_id'])}", check=False)
        return OpResult.ok("destination disks finalized (temporary shutdown timeout removed)")

    async def start_vm(self, vm_ref: str) -> OpResult:
        if self.ctx.dry_run:
            return OpResult.ok("dry run: would start instance")
        await self._cli(f"openstack server start {shlex.quote(vm_ref)} 2>/dev/null || true",
                        check=False)
        return OpResult.ok(f"started instance {vm_ref}")

    async def stop_vm(self, vm_ref: str, *, force: bool = False) -> OpResult:
        if self.ctx.dry_run:
            return OpResult.ok("dry run: would stop instance")
        await self._cli(f"openstack server stop {shlex.quote(vm_ref)} 2>/dev/null || true",
                        check=False)
        return OpResult.ok(f"stopped instance {vm_ref}")

    async def delete_vm(self, vm_ref: str, *, keep_disks: bool = True) -> OpResult:
        if self.ctx.dry_run:
            return OpResult.ok("dry run: would delete instance")
        await self.ctx.emit(f"[openstack] deleting instance {vm_ref} (keep_disks={keep_disks})")
        await self._cli(
            f"openstack server delete --wait {shlex.quote(vm_ref)} 2>/dev/null || true",
            check=False)
        # Volumes use delete_on_termination=false, so server delete leaves them; free
        # them (and the ports) only when keep_disks is False (destination rollback).
        if not keep_disks:
            await self._cleanup_artifacts()
        return OpResult.ok(f"deleted instance {vm_ref}")

    async def provision(self, name: str, size: int = 10, volume_type: str = "pure",
                        **_: Any) -> OpResult:
        await self.ctx.emit(f"Creating Cinder volume {name!r} ({size} GiB, type {volume_type})")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry run: would create volume {name}", status="planned")
        cmd = f"openstack volume create --size {int(size)}"
        if volume_type:
            cmd += f" --type {shlex.quote(volume_type)}"
        cmd += f" {shlex.quote(name)}"
        await self._cli(cmd)
        return OpResult.ok(f"Provisioned volume {name!r}", artifacts={"volume": name})

    async def snapshot(self, volume: str, name: str = "", **_: Any) -> OpResult:
        snap_name = name or f"{volume}-snap"
        await self.ctx.emit(f"Snapshotting volume {volume!r} -> {snap_name!r}")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry run: would snapshot {volume}", status="planned")
        await self._cli(
            f"openstack volume snapshot create --volume {shlex.quote(volume)} "
            f"{shlex.quote(snap_name)}")
        return OpResult.ok(f"Snapshot {snap_name!r} created",
                           artifacts={"snapshot": snap_name})

    async def clone(self, source: str, dest: str, size: int | None = None,
                    **_: Any) -> OpResult:
        await self.ctx.emit(f"Cloning volume {source!r} -> {dest!r}")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry run: would clone {source} -> {dest}", status="planned")
        cmd = f"openstack volume create --source {shlex.quote(source)}"
        if size:
            cmd += f" --size {int(size)}"
        cmd += f" {shlex.quote(dest)}"
        await self._cli(cmd)
        return OpResult.ok(f"Cloned {source!r} -> {dest!r}", artifacts={"volume": dest})

    async def resize(self, volume: str, size: int, **_: Any) -> OpResult:
        await self.ctx.emit(f"Resizing volume {volume!r} to {size} GiB")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry run: would resize {volume} to {size}", status="planned")
        # `cinder extend` is the canonical CLI for growing a volume.
        await self._cli(f"cinder extend {shlex.quote(volume)} {int(size)}")
        return OpResult.ok(f"Resized {volume!r} to {size} GiB")

    async def set_qos(self, qos_name: str, volume_type: str = "",
                      max_iops: int | None = None, max_bw: int | None = None,
                      **_: Any) -> OpResult:
        await self.ctx.emit(f"Creating QoS spec {qos_name!r}")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry run: would create QoS {qos_name}", status="planned")
        props = []
        if max_iops is not None:
            props.append(f"--property maxIOPS={int(max_iops)}")
        if max_bw is not None:
            props.append(f"--property maxBWS={int(max_bw)}")
        prop_str = (" " + " ".join(props)) if props else ""
        await self._cli(
            f"openstack volume qos create{prop_str} {shlex.quote(qos_name)} || true",
            check=False)
        if volume_type:
            await self.ctx.emit(f"Associating QoS {qos_name!r} with type {volume_type!r}")
            await self._cli(
                f"openstack volume qos associate {shlex.quote(qos_name)} "
                f"{shlex.quote(volume_type)}")
        return OpResult.ok(f"QoS spec {qos_name!r} configured",
                           artifacts={"qos": qos_name})

    async def configure_replication(self, volume_type: str, **_: Any) -> OpResult:
        await self.ctx.emit(
            f"Enabling replication on volume type {volume_type!r}")
        await self.ctx.emit(
            "Note: requires a replication_device entry in the backend stanza of "
            "cinder.conf (secondary array backend_id/san_ip/api_token).")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry run: would enable replication on {volume_type}",
                               status="planned")
        await self._cli(
            f"openstack volume type set "
            f"--property replication_enabled='<is> True' {shlex.quote(volume_type)}")
        return OpResult.ok(f"Replication enabled on volume type {volume_type!r}",
                           artifacts={"volume_type": volume_type})

    async def health_check(self, **_: Any) -> OpResult:
        await self.ctx.emit("Checking Cinder volume services ...")
        out = await self._cli("openstack volume service list || true", check=False)
        return OpResult.ok("Cinder service status retrieved", output=out)

    async def teardown(self, **_: Any) -> OpResult:
        backend = self._backend_name()
        path = self._conf_path()
        await self.ctx.emit(f"Removing Cinder backend {backend!r} from {path}")
        existing = await self._read_cinder_conf()
        if not pure.stanza_exists(existing, backend):
            await self.ctx.emit(f"Backend [{backend}] not present; nothing to remove")
        if self.ctx.dry_run:
            return OpResult.ok(f"Dry run: would remove backend {backend}", status="planned")
        # crudini --del removes the whole section; then drop it from enabled_backends.
        await self._ssh(f"crudini --del {shlex.quote(path)} {shlex.quote(backend)} || true",
                        check=False)
        await self._ssh(
            f"crudini --get {shlex.quote(path)} DEFAULT enabled_backends 2>/dev/null | "
            f"tr ',' '\\n' | grep -vx {shlex.quote(backend)} | paste -sd, - | "
            f"xargs -r -I{{}} crudini --set {shlex.quote(path)} DEFAULT enabled_backends {{}} "
            f"|| true",
            check=False)
        await self._restart_volume_service()
        return OpResult.ok(f"Backend {backend!r} removed and cinder-volume restarted",
                           status="removed")
