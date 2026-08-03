"""Helpers for rendering and detecting the Everpure FlashArray Cinder backend stanza.

This module is intentionally free of any PHIF runtime imports so it can be unit
tested in isolation. It knows how to:

* map a PHIF ``protocol`` value to the correct Cinder ``volume_driver`` class,
* render a ``[<backend>]`` stanza for ``cinder.conf``,
* detect whether such a stanza already exists in an existing ``cinder.conf``
  (idempotency),
* build the ``enabled_backends`` line update.

References (Everpure Data support docs, OpenStack bundle ``m_openstack``):
  https://support.purestorage.com/bundle/m_openstack/page/Solutions/OpenStack/
  The FlashArray volume driver ships with upstream Cinder; integration is purely
  a backend stanza in ``cinder.conf`` plus a ``cinder-volume`` restart.
"""

from __future__ import annotations

import re

# protocol -> Cinder volume_driver class.
# cinder.volume.drivers.pure.{PureISCSIDriver,PureFCDriver,PureNVMEDriver}
# NVMe-TCP and NVMe-RoCE both use PureNVMEDriver; the transport is selected by
# the array/host config (pure_nvme_transport for newer releases).
DRIVER_CLASSES: dict[str, str] = {
    "iscsi": "cinder.volume.drivers.pure.PureISCSIDriver",
    "fc": "cinder.volume.drivers.pure.PureFCDriver",
    "nvme-tcp": "cinder.volume.drivers.pure.PureNVMEDriver",
    "nvme-roce": "cinder.volume.drivers.pure.PureNVMEDriver",
}

# protocol -> pure_nvme_transport value (only meaningful for the NVMe driver).
NVME_TRANSPORTS: dict[str, str] = {
    "nvme-tcp": "tcp",
    "nvme-roce": "roce",
}


def driver_class_for(protocol: str) -> str:
    """Return the Cinder ``volume_driver`` class for a PHIF protocol value."""
    try:
        return DRIVER_CLASSES[protocol]
    except KeyError:
        raise ValueError(
            f"Unsupported OpenStack/Cinder protocol {protocol!r}; "
            f"expected one of {sorted(DRIVER_CLASSES)}"
        ) from None


def render_stanza(
    *,
    backend_name: str,
    protocol: str,
    san_ip: str,
    pure_api_token: str,
    eradicate_on_delete: bool = False,
    use_multipath_for_image_xfer: bool = True,
    replication_device: str | None = None,
    pure_iscsi_cidr: str | None = None,
    pure_iscsi_cidr_list: str | None = None,
    nvme_options: str | None = None,
) -> str:
    """Render a ``[<backend>]`` stanza for ``cinder.conf``.

    ``backend_name`` is used both as the INI section name and as
    ``volume_backend_name`` (the value a Cinder volume-type maps to via the
    ``volume_backend_name`` extra spec).

    Interface binding is expressed at the driver level:

    * ``pure_iscsi_cidr`` / ``pure_iscsi_cidr_list`` restrict which array iSCSI
      target subnet(s) the driver advertises -- this is the driver-level "which
      interface/subnet" control. It is only meaningful for the iSCSI protocol;
      it is *never* rendered for FC or NVMe stanzas. (Compute-host iscsiadm
      iface binding is host-managed and is not expressed here.)
    * ``nvme_options`` is a free-form ``key = value`` block (one option per line)
      appended only for NVMe protocols, e.g. extra transport tuning.
      TODO(doc-validate): confirm the exact NVMe-TCP transport-selection option
      names against the shipping Everpure driver release.
    * FC HBA usage is zoning-driven and usually automatic; no iSCSI/NVMe keys
      are rendered for the FC stanza.
    """
    driver = driver_class_for(protocol)
    lines = [
        f"[{backend_name}]",
        f"volume_backend_name = {backend_name}",
        f"volume_driver = {driver}",
        f"san_ip = {san_ip}",
        f"pure_api_token = {pure_api_token}",
        f"use_multipath_for_image_xfer = {str(use_multipath_for_image_xfer).lower()}",
        f"pure_eradicate_on_delete = {str(eradicate_on_delete).lower()}",
    ]
    # iSCSI-only interface/subnet binding. Keep FC/NVMe stanzas free of these.
    if protocol == "iscsi":
        if pure_iscsi_cidr:
            lines.append(f"pure_iscsi_cidr = {pure_iscsi_cidr}")
        if pure_iscsi_cidr_list:
            lines.append(f"pure_iscsi_cidr_list = {pure_iscsi_cidr_list}")
    if protocol in NVME_TRANSPORTS:
        lines.append(f"pure_nvme_transport = {NVME_TRANSPORTS[protocol]}")
        if nvme_options:
            for opt in nvme_options.splitlines():
                opt = opt.strip()
                if opt:
                    lines.append(opt)
    if replication_device:
        # e.g. "backend_id:secondary,san_ip:1.2.3.4,api_token:xxxx"
        lines.append(f"replication_device = {replication_device}")
    return "\n".join(lines) + "\n"


def render_pure_multipath_dropin() -> str:
    """Render the Everpure FlashArray ``multipath`` device drop-in for compute hosts.

    Dropped in at ``/etc/multipath/conf.d/99-pure.conf`` (non-destructive — it
    does not clobber an operator's ``/etc/multipath.conf``). Values follow Everpure's
    Linux FlashArray best-practice multipath settings (ALUA, fast failover, no
    user-friendly names so the WWID is stable across hosts).
    """
    return (
        "# Managed by PHIF: Everpure FlashArray multipath settings\n"
        "devices {\n"
        '    device {\n'
        '        vendor "PURE"\n'
        '        product "FlashArray"\n'
        '        path_selector "service-time 0"\n'
        '        hardware_handler "1 alua"\n'
        "        path_grouping_policy group_by_prio\n"
        "        prio alua\n"
        "        failback immediate\n"
        "        path_checker tur\n"
        "        fast_io_fail_tmo 10\n"
        "        dev_loss_tmo 60\n"
        "        user_friendly_names no\n"
        "        no_path_retry 0\n"
        "    }\n"
        "}\n"
    )


def stanza_exists(cinder_conf_text: str, backend_name: str) -> bool:
    """True if a ``[<backend_name>]`` section already exists in the conf text."""
    pattern = re.compile(rf"^\s*\[{re.escape(backend_name)}\]\s*$", re.MULTILINE)
    return bool(pattern.search(cinder_conf_text or ""))


def build_enabled_backends(cinder_conf_text: str, backend_name: str) -> str | None:
    """Return the new ``enabled_backends`` value including ``backend_name``.

    Returns ``None`` if the backend is already present (idempotent no-op).
    If no ``enabled_backends`` line exists, returns just ``backend_name``.
    """
    pattern = re.compile(r"^\s*enabled_backends\s*=\s*(.*)$", re.MULTILINE)
    m = pattern.search(cinder_conf_text or "")
    if not m:
        return backend_name
    existing = [b.strip() for b in m.group(1).split(",") if b.strip()]
    if backend_name in existing:
        return None
    existing.append(backend_name)
    return ",".join(existing)
