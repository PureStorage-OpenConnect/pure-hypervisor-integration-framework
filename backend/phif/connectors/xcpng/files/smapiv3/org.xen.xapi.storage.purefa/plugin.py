#!/usr/bin/env python
"""PureFA SMAPIv3 volume plugin -- Plugin interface (Query / diagnostics).

Per-VDI FlashArray volume plugin for XCP-ng 8.3 (SMAPIv3). Modeled on the XCP-ng
``org.xen.xapi.storage.raw-device`` reference plugin: a Plugin_skeleton subclass
dispatched by the invoked script's basename via Plugin_commandline.

Discovery: dropping this directory under
``/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.purefa/`` (with the
per-method symlinks created by link.sh) makes xapi-storage-script pick it up via
inotify; ``xe sm-list`` then shows the ``purefa`` plugin from this Query response.
"""

import os
import sys

import xapi.storage.api.v5.plugin
from xapi.storage import log


class Implementation(xapi.storage.api.v5.plugin.Plugin_skeleton):
    def diagnostics(self, dbg):
        return "PureFA SMAPIv3 plugin: no diagnostic data"

    def query(self, dbg):
        return {
            "plugin": "purefa",
            "name": "Everpure FlashArray (per-VDI volume)",
            "description": (
                "Everpure FlashArray SMAPIv3 SR. Each VDI is its own "
                "FlashArray volume presented as a raw multipath block device; "
                "snapshots and clones are array-native."),
            "vendor": "Everpure Data",
            "copyright": "(C) 2026 Everpure Data",
            "version": "1.0",
            "required_api_version": "5.0",
            "features": [
                "SR_ATTACH",
                "SR_DETACH",
                "SR_CREATE",
                "SR_PROBE",
                "VDI_CREATE",
                "VDI_DESTROY",
                "VDI_ATTACH",
                "VDI_ATTACH_OFFLINE",
                "VDI_DETACH",
                "VDI_ACTIVATE",
                "VDI_DEACTIVATE",
                "VDI_CLONE",
                "VDI_SNAPSHOT",
                "VDI_RESIZE",
                "VDI_UPDATE",
                "SR_METADATA",
            ],
            "configuration": {
                "endpoint": "FlashArray management endpoint (host or URL)",
                "token": "FlashArray REST API token",
                "hostgroup": "FlashArray host group containing the pool's hosts",
                "protocol": "Transport: iscsi | nvme-tcp | fc (default iscsi)",
            },
            "required_cluster_stack": [],
        }


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.plugin.Plugin_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Plugin.diagnostics":
        cmd.diagnostics()
    elif base == "Plugin.Query":
        cmd.query()
    else:
        raise xapi.storage.api.v5.plugin.Unimplemented(base)
