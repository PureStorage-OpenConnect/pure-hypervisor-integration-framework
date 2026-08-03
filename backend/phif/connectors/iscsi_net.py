"""Shared host network tuning for multi-NIC iSCSI (ARP-flux prevention).

When a Linux host has more than one NIC on the same iSCSI target subnet -- the
common FlashArray dual-port iSCSI layout -- the kernel's default ARP behaviour
causes **ARP flux**: the host may answer ARP requests for an address on the
*wrong* interface and announce the wrong source address, so iSCSI logins/sessions
bind to the wrong path or fail intermittently. The standard fix is, per storage
NIC::

    net.ipv4.conf.<nic>.arp_ignore  = 2   # only answer ARP for IPs on the iface that received it
    net.ipv4.conf.<nic>.arp_announce = 2   # always use the best local source addr for the target

This applies to EVERY Linux hypervisor that does host-side iSCSI (Proxmox,
XCP-ng dom0, HPE VME KVM hosts) and to OpenStack compute hosts. This module is a
pure helper (no PHIF runtime imports) so each connector can drop the snippet into
its own privileged SSH path.
"""

from __future__ import annotations

import re
import shlex

# A PHIF-managed sysctl drop-in (full-rewrite each apply, so it's idempotent and
# the current NIC selection always wins). 99- prefix so it overrides defaults.
ARP_SYSCTL_FILE = "/etc/sysctl.d/99-phif-iscsi-arp.conf"

# Conservative interface-name allowlist (alnum, dot, dash, underscore, colon) so a
# NIC name can never inject shell or sysctl-key syntax. VLAN sub-interfaces with
# dots (e.g. ens1f0.100) keep working in the dotted key form for plain cases.
_NIC_RE = re.compile(r"^[A-Za-z0-9._:-]+$")


def _clean_nics(nics) -> list[str]:
    out: list[str] = []
    for n in nics or []:
        n = (n or "").strip()
        if n and _NIC_RE.match(n) and n not in out:
            out.append(n)
    return out


def _arp_sysctl_lines(clean: list[str]) -> list[str]:
    """Return the per-NIC ``arp_ignore``/``arp_announce`` sysctl assignment lines.

    Shared by both the SSH snippet (:func:`arp_flux_cmd`) and the declarative
    file body (:func:`arp_sysctl_content`) so the two delivery paths can never
    drift on which keys / values they set.
    """
    lines = ["# Managed by PHIF: multi-NIC iSCSI ARP-flux prevention "
             "(arp_ignore=2, arp_announce=2)"]
    for nic in clean:
        lines.append(f"net.ipv4.conf.{nic}.arp_ignore = 2")
        lines.append(f"net.ipv4.conf.{nic}.arp_announce = 2")
    return lines


def arp_sysctl_content(nics) -> str:
    """Return the body of the PHIF ARP-flux sysctl drop-in for the given NICs.

    This is the *declarative* form of :func:`arp_flux_cmd` -- the exact file
    contents to write to :data:`ARP_SYSCTL_FILE`. Used by connectors that
    configure nodes declaratively rather than over SSH (e.g. OpenShift RHCOS,
    where the file is embedded in a MachineConfig's Ignition and applied by the
    Machine Config Operator). Returns ``""`` when no usable NICs are given so the
    caller can skip writing an empty drop-in.
    """
    clean = _clean_nics(nics)
    if not clean:
        return ""
    return "\n".join(_arp_sysctl_lines(clean)) + "\n"


def arp_flux_cmd(nics) -> str:
    """Return a root shell snippet that persists + live-applies the ARP-flux fix.

    Heredoc-free and newline-free (uses ``printf`` + ``sysctl -w``) so it is safe
    to drop into any connector's SSH helper -- including ``&&``-joined script
    runners. Returns ``true`` (a no-op) when no usable NICs are given. Must run as
    root / via sudo (writes ``/etc/sysctl.d`` and calls ``sysctl``).
    """
    clean = _clean_nics(nics)
    if not clean:
        return "true"
    file_lines = _arp_sysctl_lines(clean)
    setvals: list[str] = []
    for nic in clean:
        setvals.append(f"net.ipv4.conf.{nic}.arp_ignore=2")
        setvals.append(f"net.ipv4.conf.{nic}.arp_announce=2")
    # printf '%s\n' <each line> writes one line per arg without any literal newline
    # in the command string.
    printf_args = " ".join(shlex.quote(l) for l in file_lines)
    persist = f"printf '%s\\n' {printf_args} > {ARP_SYSCTL_FILE}"
    live = "sysctl -w " + " ".join(shlex.quote(s) for s in setvals) + " 2>/dev/null || true"
    return f"{persist}; {live}"
