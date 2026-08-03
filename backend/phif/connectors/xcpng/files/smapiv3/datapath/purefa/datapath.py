#!/usr/bin/env python
"""PureFA SMAPIv3 DATAPATH plugin.

XCP-ng 8.3 xenopsd accepts only ``qdisk`` (qemu) or ``vbd3`` (tapdisk/tapback)
XenDisk backends -- there is no in-kernel raw passthrough -- and it REQUIRES a
XenDisk implementation ("Could not find XenDisk implementation"). So we front the
raw FlashArray multipath device with **tapdisk** (the ``aio`` driver) and hand the
guest a ``vbd3`` XenDisk, mirroring exactly how the stock blktap path wires a VBD
(confirmed from a live xenstore backend):

    backend_type = "vbd3"
    params       = /dev/sm/backend/purefa/<wwid>   (symlink -> /dev/xen/blktap-2/tapdev<minor>)

Per the xapi-storage spec, ``attach`` returns the backend struct
``{"implementations": [...]}`` (the missing ``implementations`` key was the
SR_BACKEND_FAILURE/KeyError on VM power-on).

VDI URI: ``purefa:///dev/mapper/3624a9370<serial>`` (NVMe: ``.../eui.<serial>``).
"""

import os
import subprocess
import sys
import time

# The daemon runs the per-method symlink without the plugin dir on sys.path.
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

import xapi.storage.api.v5.datapath
from xapi.storage import log

try:
    from urllib.parse import urlparse
except ImportError:                       # py2 fallback
    from urlparse import urlparse  # type: ignore

_BLKTAP_BASE = "/dev/xen/blktap-2"
_BACKEND_DIR = "/dev/sm/backend/purefa"


def _device(uri):
    """purefa:///dev/mapper/<wwid>  ->  /dev/mapper/<wwid>."""
    return urlparse(uri).path


def _run(args):
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, _ = p.communicate()
    return p.returncode, (out or b"").decode("utf-8", "replace")


def _ensure_device(dev):
    """Make sure the multipath device node exists (rescan transport + reassemble)."""
    if os.path.exists(dev):
        return True
    _run(["sh", "-c", "iscsiadm -m session --rescan >/dev/null 2>&1 || true"])
    _run(["sh", "-c", "nvme connect-all >/dev/null 2>&1 || true"])
    _run(["sh", "-c", "multipath -r >/dev/null 2>&1 || true"])
    for _ in range(30):
        if os.path.exists(dev):
            return True
        time.sleep(0.5)
        _run(["sh", "-c", "multipath -r >/dev/null 2>&1 || true"])
    return os.path.exists(dev)


def _tap_info_for(dev):
    """Return (pid, minor) of an existing tapdisk serving aio:<dev>, or (None, None)."""
    rc, out = _run(["tap-ctl", "list"])
    needle = "aio:" + dev
    for line in out.splitlines():
        if needle in line.split("args=")[-1]:
            pid = minor = None
            for tok in line.split():
                if tok.startswith("pid="):
                    pid = tok.split("=", 1)[1]
                elif tok.startswith("minor="):
                    minor = tok.split("=", 1)[1]
            return pid, minor
    return None, None


def _tap_create(dev):
    """Ensure a tapdisk serves aio:<dev>; return its minor (str) or None."""
    _, minor = _tap_info_for(dev)
    if minor is None:
        _run(["tap-ctl", "create", "-a", "aio:" + dev])
        _, minor = _tap_info_for(dev)
    return minor


def _flush_device(dev):
    """Flush the multipath map + remove the underlying SCSI paths for this volume.

    Run on detach so a deleted/detached VDI's device doesn't linger as a STALE
    multipath map. A leftover map (paths gone after the array disconnects the
    volume) wedges multipathd -- it stops tracking paths -- which then blocks NEW
    LUNs from ever assembling (the "new disks don't show up / VM won't start"
    symptom). ``dev`` is /dev/mapper/<wwid>, so its basename is the WWID.
    """
    wwid = os.path.basename(dev)
    _run(["multipath", "-f", wwid])         # remove the dm-multipath map
    # Remove the now-idle SCSI path devices so they don't ghost (and so a later
    # reattach rediscovers them cleanly via iscsiadm --rescan).
    try:
        for blk in os.listdir("/sys/block"):
            if not blk.startswith("sd"):
                continue
            rc, out = _run(["/lib/udev/scsi_id", "-g", "-u", "-d", "/dev/" + blk])
            if out.strip() == wwid:
                try:
                    with open("/sys/block/%s/device/delete" % blk, "w") as fh:
                        fh.write("1")
                except (IOError, OSError):
                    pass
    except OSError:
        pass


def _backend_link(dev, tapdev):
    """Create /dev/sm/backend/purefa/<wwid> -> tapdev (the vbd3 params path)."""
    try:
        if not os.path.isdir(_BACKEND_DIR):
            os.makedirs(_BACKEND_DIR)
        link = os.path.join(_BACKEND_DIR, os.path.basename(dev))
        if os.path.islink(link) or os.path.exists(link):
            os.remove(link)
        os.symlink(tapdev, link)
        return link
    except OSError as e:
        log.error("purefa-dp: backend symlink failed (%s); using tapdev" % e)
        return tapdev


class Implementation(xapi.storage.api.v5.datapath.Datapath_skeleton):
    def open(self, dbg, uri, persistent):
        return None

    def attach(self, dbg, uri, domain):
        dev = _device(uri)
        if not _ensure_device(dev):
            raise xapi.storage.api.v5.datapath.Unimplemented(
                "purefa: multipath device %s not present" % dev)
        minor = _tap_create(dev)
        if minor is None:
            raise xapi.storage.api.v5.datapath.Unimplemented(
                "purefa: failed to start tapdisk for %s" % dev)
        tapdev = "%s/tapdev%s" % (_BLKTAP_BASE, minor)
        params = _backend_link(dev, tapdev)
        log.debug("%s: purefa attach %s -> vbd3 params=%s (tapdev=%s)"
                  % (dbg, dev, params, tapdev))
        # attach returns the backend struct (xapi-storage spec): a list of
        # implementation variants. xenopsd uses the XenDisk for the guest ring.
        return {"implementations": [
            ["XenDisk", {"params": params, "extra": {}, "backend_type": "vbd3"}],
            ["BlockDevice", {"path": tapdev}],
        ]}

    def activate(self, dbg, uri, domain):
        return None

    def deactivate(self, dbg, uri, domain):
        return None

    def detach(self, dbg, uri, domain):
        # Tear down the tapdisk, remove the params symlink, then flush the
        # multipath map + SCSI paths so a deleted/detached VDI leaves no stale map.
        dev = _device(uri)
        pid, minor = _tap_info_for(dev)
        if pid is not None and minor is not None:
            _run(["tap-ctl", "close", "-p", pid, "-m", minor, "-t", "120"])
            _run(["tap-ctl", "destroy", "-p", pid, "-m", minor])
        try:
            os.remove(os.path.join(_BACKEND_DIR, os.path.basename(dev)))
        except OSError:
            pass
        _flush_device(dev)
        return None

    def close(self, dbg, uri):
        return None


if __name__ == "__main__":
    log.log_call_argv()
    CMD = xapi.storage.api.v5.datapath.Datapath_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Datapath.attach":
        CMD.attach()
    elif base == "Datapath.activate":
        CMD.activate()
    elif base == "Datapath.deactivate":
        CMD.deactivate()
    elif base == "Datapath.detach":
        CMD.detach()
    elif base == "Datapath.open":
        CMD.open()
    elif base == "Datapath.close":
        CMD.close()
    else:
        raise xapi.storage.api.v5.datapath.Unimplemented(base)
