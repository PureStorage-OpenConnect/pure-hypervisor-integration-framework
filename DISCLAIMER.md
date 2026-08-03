# Disclaimer — read this first

## PHIF is not a supported product

**PHIF is an independent, experimental project. It is not a product. It is not
supported. Use it at your own risk.**

Specifically:

- PHIF is **not** an officially released, sold, or supported product.
- It is **not** covered by any support agreement, warranty, SLA, or maintenance
  contract, and it carries no roadmap or compatibility commitment.
- **Support cases will not be accepted** for PHIF, for anything PHIF deploys or
  configures, or for problems arising from its use. Do not open a support case
  about this software.
- Nothing here should be read as an endorsement of, or a statement of support
  for, any configuration PHIF produces.
- The bundled storage plugins (the Proxmox `purefa` plugin, the XCP-ng SMAPIv3
  driver, and the HPE VM Essentials / Morpheus plugin) are **custom, unofficial
  integrations written for this project**. They are not vendor-shipped
  integrations, they are not certified by any hypervisor vendor, and installing
  them may place your hypervisor in a configuration its vendor does not support.

## What PHIF actually does to your infrastructure

PHIF is not a read-only or advisory tool. In normal operation it will:

- **Create, resize, snapshot, copy, and destroy volumes** on a FlashArray.
- **Create and delete host and host-group records** on the array, and change
  volume-to-host mappings.
- **Install software on your hypervisor hosts over SSH** — storage plugins,
  multipath configuration, iSCSI/NVMe settings — and restart services such as
  `pvedaemon`, `multipathd`, `iscsid`, and `cinder-volume`.
- **Apply Kubernetes manifests**, including operators and StorageCluster
  resources, to an OpenShift cluster.
- **Power off, create, modify, and delete virtual machines** during migrations.
- **Delete source VMs and their volumes** when a migration is run in `move` mode.

Any of these can cause **irreversible data loss, downtime, or an unbootable
system** if PHIF is pointed at the wrong target, if it contains a defect, or if
it encounters a state its authors did not anticipate.

## Before you run it

1. **Use a lab.** Do not run PHIF against production infrastructure, production
   data, or anything you cannot afford to lose and rebuild.
2. **Have backups you have actually tested restoring** — of your VMs, your array
   configuration, and your hypervisor configuration.
3. **Try mock mode first.** `PHIF_MOCK_MODE=1` stubs all array and hypervisor
   I/O, so you can walk the entire UI and every workflow without touching real
   hardware.
4. **Use a dedicated, least-privilege array and hypervisor account**, not your
   primary administrative credentials.
5. **Read the connector documentation** in `docs/connectors/` for the hypervisor
   you intend to use. Each one documents exactly what is changed on the host.
6. **Check the maturity badge.** Connectors vary widely in how much hardware
   validation they have had; see the connector status table in the
   [README](README.md) and the [Known issues](README.md#known-issues) section.

## Legal

PHIF is provided under the [Apache License 2.0](LICENSE). As stated in
Sections 7 and 8 of that license, the software is provided on an **"AS IS"
BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND**, either express or
implied, and **no contributor is liable for any damages** arising out of its
use — including, without limitation, damages for data loss, business
interruption, or any other commercial damages or losses.

All trademarks are the property of their respective owners; see [NOTICE](NOTICE).

By using PHIF you accept that you alone are responsible for what it does to
your systems.
