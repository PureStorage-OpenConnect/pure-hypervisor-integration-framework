"""HPE VM Essentials (VME) connector.

Targets the Everpure CSI / Cinder / Proxmox storage-plugin model: ONE FlashArray
volume per VM disk, presented directly to the VM as a raw multipathed block
device, with array-based snapshots / clones / resize -- rather than a shared
datastore pool.

maturity = "ga": HPE VM Essentials is a KVM/libvirt platform managed by a
Morpheus-lineage VME Manager. The native morpheus-plugin delivers per-VM-disk
raw-block array provisioning and array-backed snapshots, validated end-to-end on
a live VME appliance; a documented datastore fallback is retained. See
docs/connectors/hpevme.md.
"""

from phif.connectors.hpevme.connector import HpeVmeConnector

__all__ = ["HpeVmeConnector"]
