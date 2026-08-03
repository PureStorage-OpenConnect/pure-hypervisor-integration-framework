"""VMware vSphere connector.

Wraps the Everpure Data vSphere Client Remote Plugin + VASA Provider (for vVols)
and the array-side host/volume/datastore lifecycle. vCenter operations go through
the vCenter REST API via ``ctx.runner.run_http`` (mock-safe); array-side host
registration uses the ``purestorage.flasharray`` Ansible collection
(``ansible/vsphere/``); all other FlashArray operations use ``ctx.array``.

Auto-discovery picks this up with no central registration.
"""

from phif.connectors.vsphere.connector import VSphereConnector

__all__ = ["VSphereConnector"]
