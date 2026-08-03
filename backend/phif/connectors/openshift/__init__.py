"""Red Hat OpenShift / Kubernetes connector (Portworx / px-csi).

Installs Portworx (``px-csi``, ``pxd.portworx.com``) — the Portworx Operator (OLM)
plus a StorageCluster (from a Portworx Central spec or PHIF's generated FlashArray
Direct Access spec) — and drives day-2 storage operations (StorageClass,
VolumeSnapshotClass, PVC, snapshot, clone, resize) plus storage-NIC binding
(MachineConfig) through ``kubectl``/``oc`` executed on the management host. The
legacy Service Orchestrator (`pure-csi`) driver has been retired and removed.

Auto-discovery picks this package up with no central registration.
"""

from phif.connectors.openshift.connector import OpenShiftConnector

__all__ = ["OpenShiftConnector"]
