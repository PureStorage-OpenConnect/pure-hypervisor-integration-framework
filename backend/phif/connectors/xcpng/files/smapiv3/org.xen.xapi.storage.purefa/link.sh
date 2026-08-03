#!/bin/sh
# Create the SMAPIv3 per-method entrypoint symlinks. xapi-storage-script invokes
# a program named "<Interface>.<method>" in the plugin dir; each is a symlink to
# the implementing module, which dispatches on its own basename.
set -e
cd "$(dirname "$0")"

chmod +x plugin.py sr.py volume.py

for m in Plugin.Query Plugin.diagnostics; do
    ln -sf plugin.py "$m"
done
for m in SR.probe SR.create SR.attach SR.detach SR.destroy SR.stat SR.ls \
         SR.set_name SR.set_description; do
    ln -sf sr.py "$m"
done
for m in Volume.create Volume.destroy Volume.stat Volume.snapshot Volume.clone \
         Volume.resize Volume.set Volume.unset Volume.set_name \
         Volume.set_description; do
    ln -sf volume.py "$m"
done
echo "purefa SMAPIv3 plugin links created"
