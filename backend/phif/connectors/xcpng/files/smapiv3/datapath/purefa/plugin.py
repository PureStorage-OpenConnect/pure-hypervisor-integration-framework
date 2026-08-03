#!/usr/bin/env python
"""PureFA SMAPIv3 datapath plugin -- Plugin interface (Query / diagnostics).

Registers the ``purefa`` datapath so xapi-storage-script routes ``purefa://`` VDI
URIs (emitted by the purefa volume plugin) to datapath.py.
"""

import os
import sys

import xapi.storage.api.v5.plugin
from xapi.storage import log


class Implementation(xapi.storage.api.v5.plugin.Plugin_skeleton):
    def diagnostics(self, dbg):
        return "PureFA datapath plugin: no diagnostic data"

    def query(self, dbg):
        return {
            "plugin": "purefa",
            "name": "Everpure FlashArray datapath",
            "description": (
                "Presents an Everpure FlashArray volume directly to the guest as a raw "
                "multipath block device (Blkback)."),
            "vendor": "Everpure Data",
            "copyright": "(C) 2026 Everpure Data",
            "version": "1.0",
            "required_api_version": "5.0",
            "features": [],
            "configuration": {},
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
