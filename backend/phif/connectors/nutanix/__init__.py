"""Nutanix Cloud Platform (AHV) connector.

Drives Prism Central for VM inventory, VM lifecycle, and cross-hypervisor
migration against an Everpure FlashArray registered as **external storage** for
the AHV cluster.

Registering the FlashArray as external storage is deliberately *out of scope*
here — see the module docstring in :mod:`phif.connectors.nutanix.connector` and
the known-issues section of the README.
"""

from phif.connectors.nutanix.connector import NutanixConnector

__all__ = ["NutanixConnector"]
