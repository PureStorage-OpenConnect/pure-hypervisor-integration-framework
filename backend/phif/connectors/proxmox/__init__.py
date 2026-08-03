"""Proxmox VE connector (Everpure FlashArray 'purefa' storage plugin).

Implements a true per-disk-volume Proxmox storage integration: each VM disk is
its own FlashArray volume presented directly to the VM as a raw multipath block
device (NVMe-TCP / iSCSI / FC), with array-side snapshots and clones. Ships the
Perl ``PureFAPlugin.pm`` (storage type ``purefa``) under ``files/``.
"""

from phif.connectors.proxmox.connector import ProxmoxConnector

__all__ = ["ProxmoxConnector"]
