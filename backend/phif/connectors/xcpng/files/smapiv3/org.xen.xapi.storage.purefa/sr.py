#!/usr/bin/env python
"""PureFA SMAPIv3 volume plugin -- SR interface.

Implements the SMAPIv3 SR.* methods. The SR has no shared backing store (per-VDI
model): "creating" the SR just validates the FlashArray is reachable and persists
the connection config; VDIs are individual FA volumes created on demand.

The SR is namespaced on the array by a deterministic prefix derived from the
endpoint + host group, so Volume.* (which only receives the SR handle) can map
back to the array without a local database.

Validated on XCP-ng 8.3 (xapi-storage-script, xapi 25.6): the SR.attach
return-handle convention and the SR.stat field names used here are confirmed
on-host -- `xe sr-create`/`pbd-plug` and `sr-list`/`sr-param-list` round-trip
the plugin's responses correctly.
"""

import hashlib
import os
import sys

# The SMAPIv3 daemon invokes the per-method symlink (e.g. SR.create) without the
# plugin's own directory on sys.path, so sibling imports (purefa_fa) fail at load
# time -- before any logging -- giving an opaque "non-zero exit / bad json". Add
# the real module dir (resolve the symlink) so sibling imports work.
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

# The SR interface lives in the v5 *volume* module (there is no separate v5.sr).
import xapi.storage.api.v5.volume
from xapi.storage import log

import purefa_fa as fa


def _sr_id(conf):
    """Deterministic SR id from endpoint + host group (stable across attaches)."""
    raw = ("%s|%s" % (conf.get("endpoint", ""), conf.get("hostgroup", ""))).encode()
    return "phif-" + hashlib.sha1(raw).hexdigest()[:8]


def _prefix(sr_id):
    return sr_id + "-"


def vgroup_name(sr_id):
    """The per-VM FlashArray volume-group name for this SR.

    The SMAPIv3 Volume interface does not hand the plugin a VM uuid at create
    time, so we use the SR-scoped grouping the plugin already derives (``sr_id``)
    as the vgroup. Members are named ``<vg>/<sr_id>-<vdi_uuid>`` so a multi-disk
    VM's disks (all on this SR) snapshot consistently as a group.
    """
    return sr_id


def member_name(sr_id, vdi_uuid):
    """Full FA name of a NEW vgroup member volume: ``<vg>/<sr_id>-<vdi_uuid>``."""
    return "%s/%s%s" % (vgroup_name(sr_id), _prefix(sr_id), vdi_uuid)


def pgroup_name(vg):
    """The protection-group name for a volume group: ``<vgroup>-pg``.

    A vgroup is a namespace, not a consistency group, so crash-consistent
    snapshots are taken on a protection group whose members are the vgroup's
    member VOLUMES (a pgroup cannot contain a vgroup directly).
    """
    return "%s-pg" % vg if vg else ""


def is_snapshot_key(key):
    """A snapshot VDI's key is a per-volume FlashArray snapshot name "<volume>.<sfx>".

    The volume may itself be a vgroup member "<vg>/<vol>", so the snapshot is
    "<vg>/<vol>.<sfx>" and the suffix '.' is in the LAST '/'-segment. A plain
    volume key ("<vg>/<vol>" or legacy "<uuid>" / "<sr_id>-<uuid>") has no '.' in
    its last segment (SR/VDI uuids contain no dots), so it is NOT a snapshot.
    """
    return "." in key.rsplit("/", 1)[-1]


def is_vgroup_key(key):
    """True when a VDI key is a vgroup-member name ``<vg>/<vol>`` (has a '/').

    ADDITIVE SAFETY: a legacy standalone VDI key has no '/', so it follows the
    original ``<sr_id>-<key>`` code path unchanged.
    """
    return "/" in key


def vgroup_of(key):
    """The vgroup name embedded in a vgroup-member key ``<vg>/<vol>``."""
    return key.split("/", 1)[0] if is_vgroup_key(key) else ""


def obj_name(sr_id, key):
    """Resolve a VDI key to its FlashArray object name (volume or snapshot).

    * snapshot key -> the full snapshot name as-is (legacy "<vol>.<sfx>" OR the
      pgroup-snapshot member "<pg>.<sfx>.<vg>/<vol>"; both are usable verbatim as
      the copy_volume source for clone).
    * vgroup-member key (has '/') -> the key as-is (it IS the full ``<vg>/<vol>``).
    * legacy bare-uuid key -> ``<sr_id>-<key>`` (the original standalone path).
    """
    if is_snapshot_key(key) or is_vgroup_key(key):
        return key
    return _prefix(sr_id) + key


def _snap_to_vdi(sr_id, snap):
    """Map a FlashArray volume-snapshot dict to a SMAPIv3 VDI dict.

    A snapshot is a thin, read-only point-in-time. It is NOT directly attachable
    (FA snapshots can't be connected to a host group), so uri is empty -- to use a
    snapshot's data, clone it (VDI.clone -> Volume.clone copies the snapshot into a
    new attachable volume). The key is the snapshot's full name so destroy/stat can
    act on it without a metadata DB.
    """
    name = snap.get("name", "")
    return {
        "key": name,
        "uuid": name,
        "name": name,
        "description": "snapshot",
        "read_write": False,
        "virtual_size": int(snap.get("provisioned", 0)),
        "physical_utilisation": 0,
        "uri": [],
        "sharable": False,
        "keys": {},
    }


def _vol_to_vdi(sr_id, vol, protocol):
    """Map a FlashArray volume dict to a SMAPIv3 VDI dict.

    The VDI ``key`` must round-trip back to the FA object name via ``obj_name``:
      * vgroup member ``<vg>/<sr_id>-<uuid>`` -> key is the FULL name (has '/').
      * legacy standalone ``<sr_id>-<uuid>``   -> key is the bare ``<uuid>``.
    """
    name = vol.get("name", "")
    if is_vgroup_key(name):
        # New vgroup-member volume: the whole "<vg>/<vol>" name IS the key.
        key = name
        tail = name.split("/", 1)[1]
        vdi_uuid = tail[len(_prefix(sr_id)):] if tail.startswith(_prefix(sr_id)) \
            else tail
    else:
        vdi_uuid = name[len(_prefix(sr_id)):] if name.startswith(_prefix(sr_id)) \
            else name
        key = vdi_uuid
    serial = vol.get("serial", "")
    tags = vol.get("tags") or {}
    uri = []
    if serial:
        # purefa:///dev/mapper/<wwid> -- handled by our custom 'purefa' datapath
        # plugin, which hands the raw multipath device to the guest via Blkback.
        # (The stock tapdisk/qdisk datapaths only serve VHD/qcow files.)
        uri = ["purefa://" + fa.device_path(serial, protocol)]
    return {
        "key": key,
        "uuid": vdi_uuid,
        "name": tags.get("vdi_name", vdi_uuid),
        "description": tags.get("vdi_desc", ""),
        "read_write": True,
        "virtual_size": int(vol.get("provisioned", 0)),
        "physical_utilisation": int(vol.get("provisioned", 0)),
        "uri": uri,
        "sharable": False,
        "keys": {},
    }


class Implementation(xapi.storage.api.v5.volume.SR_skeleton):
    def probe(self, dbg, configuration):
        conf = fa.normalize_config(configuration)
        client = fa.FlashArray(conf["endpoint"], conf["token"], timeout=10)
        client.array_space()  # raises if unreachable / bad token
        return {"srs": [], "uris": []}

    def create(self, dbg, sr_uuid, configuration, name, description):
        conf = fa.normalize_config(configuration)
        # Validate connectivity up front so a misconfig fails at create time.
        client = fa.FlashArray(conf["endpoint"], conf["token"], timeout=15)
        client.array_space()
        fa.save_sr_state(_sr_id(conf), conf)
        return configuration

    def attach(self, dbg, configuration):
        conf = fa.normalize_config(configuration)
        sr_id = _sr_id(conf)
        fa.save_sr_state(sr_id, conf)
        return sr_id

    def detach(self, dbg, sr):
        return None

    def destroy(self, dbg, sr):
        # Per-VDI model: nothing array-side to tear down here (VDIs are destroyed
        # individually). Drop the local state file.
        try:
            os.remove(fa.sr_state_path(sr))
        except OSError:
            pass

    def stat(self, dbg, sr):
        conf = fa.load_sr_state(sr)
        client = fa.FlashArray(conf["endpoint"], conf["token"], timeout=10)
        space = client.array_space()
        total = int(space.get("capacity", 0))
        used = int((space.get("space") or {}).get("total_physical", 0))
        return {
            "sr": sr,
            "name": "Everpure FlashArray",
            "description": "Per-VDI FlashArray volumes (host group %s)" % conf.get("hostgroup", ""),
            "total_space": total,
            "free_space": max(total - used, 0),
            "datasources": [],
            "clustered": True,
            "health": ["Healthy", ""],
        }

    def ls(self, dbg, sr):
        conf = fa.load_sr_state(sr)
        client = fa.FlashArray(conf["endpoint"], conf["token"])
        # Legacy standalone volumes ("<sr_id>-*") AND new per-VM vgroup members
        # ("<vg>/<sr_id>-*", where <vg> == sr_id). Both map to a VDI; the key
        # round-trips via obj_name (vgroup members carry the '/').
        out = [_vol_to_vdi(sr, v, conf["protocol"])
               for v in client.list_volumes(_prefix(sr))]
        try:
            out += [_vol_to_vdi(sr, v, conf["protocol"])
                    for v in client.list_vgroup_volumes(vgroup_name(sr))]
        except Exception as e:
            log.debug("%s: vgroup-member enumeration failed: %s" % (dbg, e))
        # Snapshots are VDIs too (read-only PIT). Include both standalone and
        # vgroup-member snapshots so the SR scan doesn't lose them.
        try:
            out += [_snap_to_vdi(sr, s)
                    for s in client.list_snapshots(_prefix(sr))]
            out += [_snap_to_vdi(sr, s)
                    for s in client.list_snapshots(vgroup_name(sr) + "/")]
        except Exception as e:
            log.debug("%s: snapshot enumeration failed: %s" % (dbg, e))
        return out

    def set_name(self, dbg, sr, new_name):
        return None

    def set_description(self, dbg, sr, new_description):
        return None


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.volume.SR_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "SR.probe":
        cmd.probe()
    elif base == "SR.create":
        cmd.create()
    elif base == "SR.attach":
        cmd.attach()
    elif base == "SR.detach":
        cmd.detach()
    elif base == "SR.destroy":
        cmd.destroy()
    elif base == "SR.stat":
        cmd.stat()
    elif base == "SR.ls":
        cmd.ls()
    elif base == "SR.set_name":
        cmd.set_name()
    elif base == "SR.set_description":
        cmd.set_description()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)
