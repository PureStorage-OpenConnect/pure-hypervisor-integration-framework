#!/bin/sh
# Create the SMAPIv3 datapath per-method entrypoint symlinks.
set -e
cd "$(dirname "$0")"

chmod +x plugin.py datapath.py

for m in Plugin.Query Plugin.diagnostics; do
    ln -sf plugin.py "$m"
done
for m in Datapath.attach Datapath.activate Datapath.deactivate Datapath.detach \
         Datapath.open Datapath.close; do
    ln -sf datapath.py "$m"
done
echo "purefa datapath links created"
