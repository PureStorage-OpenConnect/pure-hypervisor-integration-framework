"""FlashArray REST helpers + SR-state persistence for the PureFA SMAPIv3 plugin.

This mirrors the proven logic in the Proxmox ``PureFAPlugin.pm`` (session/login +
per-VDI volume create/destroy/snapshot/copy/resize, connect to a host group, SCSI
WWID derivation) but in the shape the SMAPIv3 volume plugin needs.

Design (per-VDI volume, identical to the other Everpure storage plugins):
  * Each VDI is its OWN FlashArray volume.
  * NEW volumes are created as members of a per-VM FlashArray VOLUME GROUP, named
    ``<vg>/<sr_id>-<vdi_uuid>`` (the ``/`` is %2F-encoded in REST paths), so a
    multi-disk VM can be snapshotted consistently as a group. The vgroup is
    created first (idempotent) and torn down when its last member is destroyed.
  * ADDITIVE SAFETY: a stored volume name WITHOUT a ``<vg>/`` prefix (a bare
    ``<sr_id>-<vdi_uuid>`` -- a pre-existing standalone VDI) keeps working exactly
    as before. The vgroup behavior applies only to newly-created volumes.
  * The VDI's block device is the multipath node ``/dev/mapper/3624a9370<serial>``
    (SCSI WWID = NAA-6 + Everpure OUI + lowercased serial), keyed off the volume
    SERIAL and UNCHANGED by vgroup membership -- the host-side device path is
    unaffected; only the FA name changes. Presented to the guest as a raw block
    device via the custom ``purefa`` datapath plugin.
  * Snapshots/clones are array-native (volume snapshot / volume copy); a
    multi-disk VM additionally supports a group-consistent snapshot of its vgroup.

SR config (endpoint/token/host group/protocol) arrives in the ``configuration``
dict on ``SR.create``/``SR.attach``; we persist it under the SR's mount dir so the
``Volume.*`` methods (which only receive the SR path) can reach the array.

Validated on a live XCP-ng 8.3 pool (xapi 25.6): the Purity//FA REST field names
used here and the raw+block datapath URI consumed by the 8.3 datapath plugin are
confirmed end-to-end on-host (volume create/destroy/snapshot/copy/resize, host-group
connect, SCSI WWID derivation, and guest block-device attach all succeed).
"""

import json
import os
import ssl
import time

try:                                   # py3 (XCP-ng 8.3 dom0)
    from urllib.parse import quote as _urlquote
except ImportError:                    # py2 fallback
    from urllib import quote as _urlquote  # type: ignore


def _enc(name):
    """URL-encode a FlashArray object name for use in a REST path.

    A volume-group member volume is named ``<vg>/<vol>``; the ``/`` MUST be
    percent-encoded (%2F) so it is treated as part of the name, not a path
    separator. Plain (non-vgroup) names are unaffected. ``safe=""`` so the
    slash is encoded too.
    """
    return _urlquote(name, safe="")


def _name_filter(pattern):
    """A FlashArray REST v2 wildcard-name query fragment: ``filter=name='<pattern>'``.

    Purity does NOT interpret ``*`` in the ``names`` parameter (it is an exact-match
    list), so a wildcard like ``names=<prefix>*`` returns HTTP 400. Prefix/wildcard
    matching must go through ``filter`` with the Fusion query ``name='<prefix>*'``.
    ``pattern`` may contain ``*`` and ``/`` (a vgroup member); the whole expression
    is URL-encoded.
    """
    return "filter=" + _urlquote("name='%s'" % pattern, safe="")

try:                                   # py3 (XCP-ng 8.3 dom0)
    from urllib.request import Request, urlopen
    from urllib.error import HTTPError, URLError
except ImportError:                    # py2 fallback
    from urllib2 import Request, urlopen, HTTPError, URLError  # type: ignore

# Where we stash per-SR connection config so Volume.* can reach the array.
SR_STATE_ROOT = "/var/run/sr-ref/purefa"
PURE_OUI = "624a9370"   # FlashArray NAA-6 IEEE Registered Extended OUI


# --------------------------------------------------------------------------- #
# SR state (endpoint/token/host group/protocol) persistence
# --------------------------------------------------------------------------- #
def sr_state_path(sr_id):
    return os.path.join(SR_STATE_ROOT, "%s.json" % sr_id)


def save_sr_state(sr_id, conf):
    os.path.isdir(SR_STATE_ROOT) or os.makedirs(SR_STATE_ROOT)
    with open(sr_state_path(sr_id), "w") as fh:
        json.dump(conf, fh)


def load_sr_state(sr_id):
    with open(sr_state_path(sr_id)) as fh:
        return json.load(fh)


def normalize_config(configuration):
    """Pull the FA connection settings out of the device-config dict."""
    return {
        "endpoint": configuration.get("endpoint", ""),
        "token": configuration.get("token", ""),
        "hostgroup": configuration.get("hostgroup", ""),
        "protocol": (configuration.get("protocol") or "iscsi").lower(),
        # When true, a destroyed VDI's volume is hard-deleted (eradicated)
        # immediately instead of sitting in pending-eradication for 24h.
        "eradicate": str(configuration.get("eradicate", "")).lower()
        in ("1", "true", "yes", "on"),
    }


def flush_multipath_poolwide(dbg, wwid):
    """Flush the dm-multipath map for ``wwid`` on EVERY pool host.

    Called from Volume.destroy (which runs only on the SR master). With
    find_multipaths=no every host auto-maps a connected volume, and the datapath
    detach only cleans the resident host -- so a deleted volume's map would linger
    (stale) on the other hosts and wedge multipathd. We fan a flush out to all
    hosts via the purefa-mpath host plugin (xe host-call-plugin).
    """
    import subprocess

    if not wwid:
        return
    # CRITICAL: capture ALL subprocess output. This runs inside Volume.destroy,
    # whose ONLY stdout must be the JSON result -- any `xe` output leaking to
    # stdout corrupts it ("bad json on stdout") and fails the delete.
    devnull = open(os.devnull, "w")
    try:
        out = subprocess.check_output(["xe", "host-list", "--minimal"],
                                      stderr=devnull)
        hosts = [h.strip() for h in out.decode("utf-8").split(",") if h.strip()]
    except Exception:
        devnull.close()
        return
    for h in hosts:
        try:
            subprocess.call(["xe", "host-call-plugin", "host-uuid=" + h,
                             "plugin=purefa-mpath", "fn=flush",
                             "args:wwid=" + wwid],
                            stdout=devnull, stderr=devnull)
        except Exception:
            pass
    devnull.close()


def scsi_wwid(serial):
    """Build the dm-multipath WWID for a FA volume serial (NAA-6 + Everpure OUI)."""
    s = serial.lower()
    if s.startswith("0x"):
        s = s[2:]
    if s.startswith("624a937"):
        return "3" + s
    return "3" + PURE_OUI + s


def device_path(serial, protocol="iscsi"):
    s = serial.lower()
    if protocol in ("nvme-tcp", "nvme-fc"):
        return "/dev/mapper/eui." + s
    return "/dev/mapper/" + scsi_wwid(serial)


# --------------------------------------------------------------------------- #
# FlashArray REST v2 client (token login + version negotiation, short timeouts)
# --------------------------------------------------------------------------- #
class FlashArray(object):
    def __init__(self, endpoint, token, timeout=30):
        ep = endpoint or ""
        if not ep.startswith("http://") and not ep.startswith("https://"):
            ep = "https://" + ep
        self.endpoint = ep.rstrip("/")
        self.token = token
        self.timeout = timeout
        self._ctx = ssl.create_default_context()
        self._ctx.check_hostname = False
        self._ctx.verify_mode = ssl.CERT_NONE
        self._auth = None
        self._apiver = None

    def _http(self, method, url, body=None, headers=None):
        data = json.dumps(body).encode("utf-8") if body is not None else None
        req = Request(url, data=data)
        req.get_method = lambda: method
        req.add_header("Content-Type", "application/json")
        for k, v in (headers or {}).items():
            req.add_header(k, v)
        resp = urlopen(req, context=self._ctx, timeout=self.timeout)
        raw = resp.read().decode("utf-8")
        tok = resp.headers.get("x-auth-token")
        if tok:
            self._auth = tok
        return json.loads(raw) if raw else {}

    def _api_version(self):
        if self._apiver:
            return self._apiver
        ver = "2.0"
        try:
            data = self._http("GET", "%s/api/api_version" % self.endpoint)
            vs = data.get("version") or []
            if vs:
                ver = vs[-1]
        except Exception:
            pass
        self._apiver = ver
        return ver

    def _login(self):
        if self._auth:
            return
        url = "%s/api/%s/login" % (self.endpoint, self._api_version())
        self._http("POST", url, headers={"api-token": self.token})
        if not self._auth:
            raise IOError("purefa: login returned no x-auth-token")

    def request(self, method, path, body=None):
        self._login()
        url = "%s/api/%s/%s" % (self.endpoint, self._api_version(), path)
        # Read-only GETs (volume/snapshot listing for SR.ls etc.) are retried with
        # a fresh login on a transient array error. Right after a copy-overwrite +
        # host-group connect, the first `xe sr-scan` (SR.ls -> list_volumes) can hit
        # a transient HTTP 400/auth blip, which made the destination VM need a
        # second power-on attempt before its multipath device appeared. Retrying
        # here lets the first attempt succeed. Mutating calls are NOT retried (the
        # caller may have partially applied them).
        if method == "GET":
            last = None
            for attempt in range(3):
                try:
                    return self._http(method, url, body=body,
                                      headers={"x-auth-token": self._auth})
                except Exception as exc:  # transient: drop token, re-login, retry
                    last = exc
                    self._auth = None
                    time.sleep(0.5 * (attempt + 1))
                    self._login()
                    url = "%s/api/%s/%s" % (self.endpoint, self._api_version(), path)
            raise last
        return self._http(method, url, body=body,
                          headers={"x-auth-token": self._auth})

    # ---- volume-group operations (per-VM consistency group) -------------- #
    def create_vgroup(self, vg):
        """Create a FlashArray volume group (idempotent).

        Member volumes are created as ``<vg>/<vol>`` and the vgroup must exist
        first. Treat an already-exists response as success so re-provisioning a
        VM's disks into an existing group is a no-op.
        """
        if not vg:
            return
        try:
            self.request("POST", "volume-groups?names=%s" % _enc(vg))
        except Exception:
            # Already exists (HTTP 400 "already exists") -- not fatal.
            pass
        # RECOVER a soft-destroyed (pending-eradication) vgroup: after the last
        # member is destroyed the vgroup lingers ~24h as destroyed, and creating a
        # new member in it fails ("Volume group has been destroyed"). PATCH
        # destroyed=false restores it so the name is reusable immediately.
        # Best-effort (benign/no-op if already live).
        try:
            self.request("PATCH", "volume-groups?names=%s" % _enc(vg),
                         {"destroyed": False})
        except Exception:
            pass

    def vgroup(self, vg):
        """GET a volume group, or None if it doesn't exist."""
        try:
            data = self.request("GET", "volume-groups?names=%s" % _enc(vg))
        except Exception:
            return None
        items = data.get("items") or []
        return items[0] if items else None

    def vgroup_member_count(self, vg):
        """Count LIVE (non-destroyed) member volumes of a volume group."""
        if not vg:
            return 0
        try:
            data = self.request(
                "GET", "volumes?%s" % _name_filter(vg + "/*"))
        except Exception:
            return 0
        return len([v for v in (data.get("items") or [])
                    if not v.get("destroyed")])

    def destroy_vgroup(self, vg):
        """Destroy an EMPTY volume group (soft-delete + eradicate).

        Called on last-member destroy. Defensive/idempotent: only acts when the
        group has no live members; a missing group or a failed PATCH/DELETE is
        swallowed (the member volume is already gone, which is what matters).
        """
        if not vg:
            return
        if self.vgroup_member_count(vg) > 0:
            return  # still has members -- leave it
        try:
            self.request("PATCH", "volume-groups?names=%s" % _enc(vg),
                         {"destroyed": True})
        except Exception:
            pass
        try:
            self.request("DELETE", "volume-groups?names=%s" % _enc(vg))
        except Exception:
            pass

    # NB: FlashArray has NO volume-GROUP snapshot endpoint -- a volume group is a
    # namespace, not a consistency group (POST volume-group-snapshots 404s). For
    # crash-consistent snapshots we drive a PROTECTION GROUP (pgroup) instead: the
    # vgroup's member volumes are added to a "<vgroup>-pg" protection group and the
    # pgroup is snapshotted as a unit (see the protection-group ops below).

    # ---- protection-group operations (crash-consistent snapshots) -------- #
    #
    # A pgroup CANNOT contain a volume group; it holds member VOLUMES. We create a
    # "<vgroup>-pg" pgroup, add the vgroup's member volumes to it, then snapshot the
    # pgroup. A pgroup-snapshot "<pg>.<sfx>" yields one member snapshot per volume,
    # named EXACTLY "<pg>.<sfx>.<vg>/<vol>".
    def create_pgroup(self, pg):
        """Create a protection group (idempotent).

        Treat an already-exists response (HTTP 400/409) as success so re-running
        a snapshot of an existing VM's pgroup is a no-op.
        """
        if not pg:
            return
        try:
            self.request("POST", "protection-groups?names=%s" % _enc(pg))
        except Exception:
            # Already exists -- not fatal.
            pass

    def add_pgroup_volumes(self, pg, member_names):
        """Add member VOLUMES to a protection group (idempotent).

        ``member_names`` is a list of full "<vg>/<vol>" volume names. Adding a
        volume that is already a member is benign and swallowed.
        """
        if not pg or not member_names:
            return
        names = ",".join(_enc(n) for n in member_names)
        try:
            self.request(
                "POST",
                "protection-groups/volumes?group_names=%s&member_names=%s"
                % (_enc(pg), names))
        except Exception:
            # Already a member -- not fatal.
            pass

    def snapshot_pgroup(self, pg, suffix):
        """Snapshot a protection group -> creates "<pg>.<suffix>".

        Each member volume gets a snapshot named "<pg>.<suffix>.<vg>/<vol>".
        Treat an already-exists suffix as benign (idempotent per-VDI calls).
        """
        try:
            return self.request(
                "POST", "protection-group-snapshots?source_names=%s&suffix=%s"
                % (_enc(pg), _enc(suffix)))
        except Exception:
            # Snapshot with this suffix already exists -- not fatal.
            return {}

    def get_pgroup_snapshot(self, name):
        """GET a protection-group snapshot "<pg>.<sfx>", or None if absent."""
        try:
            data = self.request(
                "GET", "protection-group-snapshots?names=%s" % _enc(name))
        except Exception:
            return None
        items = data.get("items") or []
        return items[0] if items else None

    def destroy_pgroup_snapshot(self, name, eradicate=False):
        """Destroy a WHOLE protection-group snapshot "<pg>.<sfx>".

        A pgroup-snapshot MEMBER cannot be destroyed alone -- we PATCH the whole
        snapshot destroyed=true, then DELETE (eradicate) it. Idempotent: a missing
        / already-destroyed snapshot is swallowed so deleting an already-gone
        snapshot VDI doesn't 400.
        """
        if not name:
            return
        try:
            snap = self.get_pgroup_snapshot(name)
        except Exception:
            snap = None
        if snap is not None and not snap.get("destroyed"):
            try:
                self.request(
                    "PATCH", "protection-group-snapshots?names=%s" % _enc(name),
                    {"destroyed": True})
            except Exception:
                pass
        if eradicate:
            try:
                self.request(
                    "DELETE", "protection-group-snapshots?names=%s" % _enc(name))
            except Exception:
                pass

    def get_pgroup_snapshot_member(self, name):
        """GET a single pgroup-snapshot MEMBER volume-snapshot by its full name
        "<pg>.<sfx>.<vg>/<vol>" (used for stat). Returns the snapshot dict or
        None. Member snapshots surface as volume-snapshots on the array."""
        try:
            data = self.request("GET", "volume-snapshots?names=%s" % _enc(name))
        except Exception:
            return None
        items = data.get("items") or []
        return items[0] if items else None

    def destroy_pgroup(self, pg):
        """Destroy a protection group (soft-delete + eradicate), best-effort.

        Called on last-member vgroup teardown. Idempotent/defensive: a missing
        group or a failed PATCH/DELETE is swallowed.
        """
        if not pg:
            return
        try:
            self.request("PATCH", "protection-groups?names=%s" % _enc(pg),
                         {"destroyed": True})
        except Exception:
            pass
        try:
            self.request("DELETE", "protection-groups?names=%s" % _enc(pg))
        except Exception:
            pass

    # ---- volume operations (per-VDI) ------------------------------------- #
    def create_volume(self, name, size_bytes):
        # A vgroup member name is "<vg>/<vol>"; the vgroup must already exist
        # (callers create it first). The "/" is %2F-encoded in the path.
        return self.request("POST", "volumes?names=%s" % _enc(name),
                            {"provisioned": int(size_bytes)})

    def disconnect_volume(self, name, hostgroup):
        """Remove the volume's host-group connection (idempotent / best-effort)."""
        if not hostgroup:
            return
        try:
            self.request(
                "DELETE",
                "connections?host_group_names=%s&volume_names=%s"
                % (_enc(hostgroup), _enc(name)))
        except Exception:
            # Already disconnected / never connected -- not fatal for destroy.
            pass

    def destroy_volume(self, name, hostgroup=None, eradicate=False):
        # A FlashArray volume that is still CONNECTED to a host/host group cannot
        # be destroyed (the PATCH fails), so the VDI's volume would linger on the
        # array after a VM/disk delete. Disconnect from the host group FIRST, then
        # soft-delete (destroyed=true), and optionally eradicate (hard delete).
        #
        # IDEMPOTENT: a VDI delete may target a volume that's already gone (e.g. a
        # dangling VDI whose volume was destroyed by a prior partial run). GET it
        # first; if it's absent or already destroyed, there's nothing to PATCH --
        # treat as success so the XAPI VDI record can still be removed (otherwise
        # FA returns HTTP 400 and the delete fails forever).
        self.disconnect_volume(name, hostgroup)
        try:
            vol = self.volume(name)        # GET returns only LIVE volumes
        except Exception:
            vol = None
        if vol is not None and not vol.get("destroyed"):
            self.request("PATCH", "volumes?names=%s" % _enc(name),
                         {"destroyed": True})
        if eradicate:
            try:
                self.request("DELETE", "volumes?names=%s" % _enc(name))
            except Exception:
                pass               # already eradicated / pending -- not fatal

    def resize_volume(self, name, size_bytes):
        return self.request("PATCH", "volumes?names=%s" % _enc(name),
                            {"provisioned": int(size_bytes)})

    def snapshot_volume(self, name, suffix):
        # Create a real FlashArray volume-snapshot (thin point-in-time). Returns
        # the created snapshot object(s); its name is "<name>.<suffix>".
        return self.request(
            "POST", "volume-snapshots?source_names=%s&suffix=%s"
            % (_enc(name), _enc(suffix)))

    def get_snapshot(self, name):
        data = self.request("GET", "volume-snapshots?names=%s" % _enc(name))
        items = data.get("items") or []
        return items[0] if items else None

    def list_snapshots(self, name_prefix):
        # Live (non-destroyed) snapshots whose name starts with the SR prefix.
        # Covers both standalone "<sr_id>-*" and vgroup-member "<vg>/<sr_id>-*"
        # snapshots (the caller passes each prefix).
        data = self.request(
            "GET", "volume-snapshots?%s" % _name_filter(name_prefix + "*"))
        return [s for s in (data.get("items") or []) if not s.get("destroyed")]

    def destroy_snapshot(self, name, eradicate=False):
        # Idempotent: only PATCH destroyed=true if it still exists and is live, so
        # deleting an already-gone snapshot VDI doesn't 400. This is the fix for
        # snapshots persisting on the array after the VDI is deleted in XCP.
        try:
            snap = self.get_snapshot(name)
        except Exception:
            snap = None
        if snap is not None and not snap.get("destroyed"):
            self.request("PATCH", "volume-snapshots?names=%s" % _enc(name),
                         {"destroyed": True})
        if eradicate:
            try:
                self.request("DELETE", "volume-snapshots?names=%s" % _enc(name))
            except Exception:
                pass

    def copy_volume(self, source, dest):
        # Array-native clone: create dest from a source volume reference.
        return self.request("POST", "volumes?names=%s" % _enc(dest),
                            {"source": {"name": source}})

    def connect_volume(self, name, hostgroup):
        return self.request(
            "POST",
            "connections?host_group_names=%s&volume_names=%s"
            % (_enc(hostgroup), _enc(name)))

    def volume(self, name):
        data = self.request("GET", "volumes?names=%s" % _enc(name))
        items = data.get("items") or []
        return items[0] if items else None

    def list_volumes(self, name_prefix):
        # Live (non-destroyed) volumes whose name starts with the SR prefix.
        data = self.request("GET", "volumes?%s" % _name_filter(name_prefix + "*"))
        return [v for v in (data.get("items") or []) if not v.get("destroyed")]

    def list_vgroup_volumes(self, vg):
        # Live (non-destroyed) member volumes "<vg>/*" of the per-VM vgroup.
        if not vg:
            return []
        try:
            data = self.request("GET", "volumes?%s" % _name_filter(vg + "/*"))
        except Exception:
            return []
        return [v for v in (data.get("items") or []) if not v.get("destroyed")]

    def set_tag(self, name, key, value):
        # Store VDI name/description as volume tags (best-effort).
        return self.request(
            "PUT", "volumes/tags?resource_names=%s&keys=%s" % (_enc(name), key),
            {"value": value})

    def array_space(self):
        data = self.request("GET", "arrays/space")
        items = data.get("items") or []
        return items[0] if items else {}
