#!/usr/bin/env python
"""PureFA SMAPIv3 volume plugin -- Volume interface (per-VDI FlashArray volumes).

Each VDI is its own FlashArray volume. NEW volumes are created as members of a
per-VM FlashArray volume group, named ``<vg>/<sr_id>-<vdi_uuid>`` (the vgroup is
created first, idempotently), so a multi-disk VM snapshots consistently; the
vgroup is torn down when its last member is destroyed. A pre-existing standalone
VDI (a bare ``<sr_id>-<vdi_uuid>`` name with no ``/``) keeps working unchanged.
Create makes the FA volume + connects it to the SR's host group; snapshot/clone
are array-native; the VDI's block device is the multipath node (keyed off the
volume serial, unaffected by vgroup membership), returned as a ``purefa://``
datapath URI for the custom datapath plugin to attach.

Validated on XCP-ng 8.3 (xapi 25.6): the purefa:// datapath URI emitted here is
accepted by the 8.3 datapath plugin, and the Volume.* return-dicts match what
xapi-storage-script expects -- VDI create/delete/snapshot/clone/resize and
guest attach all succeed end-to-end on the live pool.
"""

import os
import sys
import uuid

# See sr.py: the daemon runs the symlink without the plugin dir on sys.path, so
# add the real module dir before importing siblings (purefa_fa, sr).
sys.path.insert(0, os.path.dirname(os.path.realpath(__file__)))

import xapi.storage.api.v5.volume
from xapi.storage import log

import purefa_fa as fa
import sr as srlib


def _client(sr):
    return fa.load_sr_state(sr), None


def _fa(conf):
    return fa.FlashArray(conf["endpoint"], conf["token"])


class Implementation(xapi.storage.api.v5.volume.Volume_skeleton):
    def create(self, dbg, sr, name, description, size, sharable):
        conf = fa.load_sr_state(sr)
        client = _fa(conf)
        vdi_uuid = str(uuid.uuid4())
        # NEW volumes are created as members of the per-VM FlashArray vgroup so a
        # multi-disk VM snapshots consistently. The vgroup must exist before the
        # member volume is created (idempotent). Member name = "<vg>/<sr_id>-<uuid>".
        vg = srlib.vgroup_name(sr)
        client.create_vgroup(vg)
        volname = srlib.member_name(sr, vdi_uuid)
        client.create_volume(volname, size)
        if conf.get("hostgroup"):
            client.connect_volume(volname, conf["hostgroup"])
        # Persist VDI name/description as volume tags (best-effort).
        try:
            client.set_tag(volname, "vdi_name", name or vdi_uuid)
            client.set_tag(volname, "vdi_desc", description or "")
        except Exception as e:   # tags are non-essential
            log.debug("%s: tag set failed: %s" % (dbg, e))
        vol = client.volume(volname)
        return srlib._vol_to_vdi(sr, vol, conf["protocol"])

    def destroy(self, dbg, sr, key):
        conf = fa.load_sr_state(sr)
        client = _fa(conf)
        # A snapshot VDI's key is the per-volume FA snapshot name "<volume>.<sfx>"
        # (the volume may be a vgroup member "<vg>/<vol>") -> destroy that snapshot.
        if srlib.is_snapshot_key(key):
            client.destroy_snapshot(key, eradicate=bool(conf.get("eradicate")))
            return
        # Resolve the full FA name. A vgroup-member key is "<vg>/<vol>" (used
        # as-is); a legacy bare-uuid key maps to "<sr_id>-<key>".
        volname = srlib.obj_name(sr, key)
        vg = srlib.vgroup_of(key)   # "" for legacy standalone VDIs
        # Capture the serial BEFORE destroying so we can flush the multipath map.
        serial = ""
        try:
            vol = client.volume(volname)
            serial = (vol or {}).get("serial", "")
        except Exception as e:
            log.debug("%s: pre-destroy serial lookup failed: %s" % (dbg, e))
        # Disconnect from the host group (required) then destroy; eradicate too
        # when the SR was created with device-config:eradicate=true.
        client.destroy_volume(volname, hostgroup=conf.get("hostgroup"),
                              eradicate=bool(conf.get("eradicate")))
        # Last-member cleanup: once this vgroup's members are gone, destroy the
        # now-empty vgroup (idempotent/defensive -- no-op if members remain or the
        # group is absent). Only for vgroup members; legacy VDIs have no vgroup.
        if vg:
            try:
                client.destroy_vgroup(vg)
            except Exception as e:
                log.debug("%s: vgroup teardown failed: %s" % (dbg, e))
            # The crash-consistency pgroup "<vg>-pg" is now orphaned too; tear it
            # down best-effort (idempotent -- only matters once the vgroup is gone).
            if client.vgroup_member_count(vg) == 0:
                try:
                    client.destroy_pgroup(srlib.pgroup_name(vg))
                except Exception as e:
                    log.debug("%s: pgroup teardown failed: %s" % (dbg, e))
        # Pool-wide cleanup: the volume is now disconnected/destroyed, so its
        # dm-multipath map is stale on every host that mapped it. Flush them all
        # (SCSI transports; NVMe-TCP uses native nvme multipath, not dm-multipath).
        if serial and conf.get("protocol", "iscsi") != "nvme-tcp":
            fa.flush_multipath_poolwide(dbg, fa.scsi_wwid(serial))

    def stat(self, dbg, sr, key):
        conf = fa.load_sr_state(sr)
        client = _fa(conf)
        if srlib.is_snapshot_key(key):
            snap = client.get_snapshot(key)
            if not snap:
                raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)
            return srlib._snap_to_vdi(sr, snap)
        vol = client.volume(srlib.obj_name(sr, key))
        if not vol:
            raise xapi.storage.api.v5.volume.Volume_does_not_exist(key)
        return srlib._vol_to_vdi(sr, vol, conf["protocol"])

    def snapshot(self, dbg, sr, key):
        conf = fa.load_sr_state(sr)
        client = _fa(conf)
        src = srlib.obj_name(sr, key)
        # PER-VOLUME FlashArray snapshot of THIS VDI. xapi calls Volume.snapshot once
        # per VDI and orchestrates VM-level consistency itself (it snapshots every
        # VDI of the VM together), so a per-volume array snapshot is the right unit.
        #
        # NB we deliberately do NOT use a FlashArray protection group here: in this
        # SMAPIv3 driver the volume group is SR-SCOPED (Volume.create gets no VM
        # identity), so a pgroup snapshot would capture EVERY VDI in the SR, not just
        # this VM's disks. Per-VM crash-consistent pgroups are used by the Proxmox /
        # HPE-VME plugins, where the vgroup is genuinely per-VM.
        #
        # The snapshot's full name "<src>.<s>" (e.g. "<vg>/<vol>.<s>" for a member,
        # or "<vol>.<s>" for a legacy standalone VDI) becomes the snapshot VDI key.
        suffix = "s" + uuid.uuid4().hex[:10]
        resp = client.snapshot_volume(src, suffix)
        snap_name = (((resp or {}).get("items") or [{}])[0]).get("name") \
            or (src + "." + suffix)
        snap = client.get_snapshot(snap_name) or {"name": snap_name,
                                                  "provisioned": 0}
        return srlib._snap_to_vdi(sr, snap)

    def clone(self, dbg, sr, key):
        conf = fa.load_sr_state(sr)
        client = _fa(conf)
        # Source may be a volume OR a snapshot (clone-from-snapshot): copy it into
        # a new attachable volume (array-native, thin via dedup).
        src = srlib.obj_name(sr, key)
        # The clone is a NEW attachable volume -> make it a member of the per-VM
        # vgroup too (ensure the group exists first, idempotent).
        vg = srlib.vgroup_name(sr)
        client.create_vgroup(vg)
        dest = srlib.member_name(sr, str(uuid.uuid4()))
        client.copy_volume(src, dest)
        if conf.get("hostgroup"):
            client.connect_volume(dest, conf["hostgroup"])
        return srlib._vol_to_vdi(sr, client.volume(dest), conf["protocol"])

    def resize(self, dbg, sr, key, new_size):
        conf = fa.load_sr_state(sr)
        _fa(conf).resize_volume(srlib.obj_name(sr, key), new_size)

    def set_name(self, dbg, sr, key, new_name):
        conf = fa.load_sr_state(sr)
        try:
            _fa(conf).set_tag(srlib.obj_name(sr, key), "vdi_name", new_name)
        except Exception:
            pass

    def set_description(self, dbg, sr, key, new_description):
        conf = fa.load_sr_state(sr)
        try:
            _fa(conf).set_tag(srlib.obj_name(sr, key), "vdi_desc", new_description)
        except Exception:
            pass

    def set(self, dbg, sr, key, k, v):
        return None

    def unset(self, dbg, sr, key, k):
        return None


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v5.volume.Volume_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Volume.create":
        cmd.create()
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.stat":
        cmd.stat()
    elif base == "Volume.snapshot":
        cmd.snapshot()
    elif base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.resize":
        cmd.resize()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.unset":
        cmd.unset()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.set_description":
        cmd.set_description()
    else:
        raise xapi.storage.api.v5.volume.Unimplemented(base)
