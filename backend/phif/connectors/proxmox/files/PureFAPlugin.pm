package PVE::Storage::Custom::PureFAPlugin;

# ---------------------------------------------------------------------------
# Everpure FlashArray storage plugin for Proxmox VE  (storage type "purefa")
#
# This is a TRUE Proxmox VE custom storage plugin, modelled on the way the Everpure
# CSI driver and the OpenStack Cinder Everpure driver work:
#
#   * EACH VM DISK IS ITS OWN FLASHARRAY VOLUME (one LUN per disk), named
#     vm-<vmid>-disk-<N>. There is NO LVM layer and NO shared LVM pool.
#   * The volume is presented DIRECTLY to the VM as a raw multipath block
#     device (e.g. /dev/mapper/<wwid>) over iSCSI, FC, or NVMe-TCP and attached
#     as a virtio-scsi disk.
#   * Snapshots and clones are performed ON THE ARRAY (FlashArray volume
#     snapshots and volume copy), NOT via LVM/qcow2.
#
# Install at: /usr/share/perl5/PVE/Storage/Custom/PureFAPlugin.pm
# then: systemctl reload pvedaemon pveproxy pvestatd
#
# storage.cfg stanza (added by the PHIF connector):
#
#   purefa: <storage_id>
#       pure_endpoint   https://flasharray.example.com
#       pure_api_token  <fa-api-token>
#       protocol        nvme-tcp          # iscsi | fc | nvme-tcp
#       host_group      pve-cluster       # FA host group the nodes belong to
#       eradicate       0                 # 1 = hard-delete on free_image
#       content         images,rootdir
#       shared          1
#
# Sources / model:
#   * Community reference plugin (per-volume model, multipath, FA REST):
#     https://github.com/kolesa-team/pve-purestorage-plugin
#   * Everpure "Proxmox with FlashArray" solution:
#     https://support.purestorage.com/bundle/m_proxmox/page/Solutions/Proxmox/
#   * PVE::Storage::Plugin API (Proxmox VE perl API)
#
# The FlashArray REST v2 request/response shapes and the multipath device-path
# discovery (serial -> wwid) below are confirmed against the live cluster's
# Purity//FA (validated over iSCSI and FC).
# ---------------------------------------------------------------------------

use strict;
use warnings;

use base qw(PVE::Storage::Plugin);

use PVE::Tools qw(run_command);
use PVE::Storage::Plugin;
use PVE::JSONSchema qw(get_standard_option);

use HTTP::Request;
use LWP::UserAgent;
use JSON;

# ---------------------------------------------------------------------------
# Plugin identity + configuration schema
# ---------------------------------------------------------------------------

sub api {
    # PVE storage plugin API version this plugin is written against.
    # PVE 9.x ships APIVER 14 (APIAGE 5). PVE uses this only as a load-gate
    # (version must be within [APIVER-APIAGE, APIVER]) and to warn on mismatch;
    # it does not change how the standard methods below are invoked.
    return 14;
}

sub type {
    return 'purefa';
}

sub plugindata {
    return {
        # Each disk is a raw block device presented straight to the guest.
        content => [ { images => 1, rootdir => 1 }, { images => 1 } ],
        format  => [ { raw => 1 }, 'raw' ],
    };
}

sub properties {
    return {
        pure_endpoint => {
            description => 'FlashArray REST endpoint, e.g. https://fa.example.com',
            type        => 'string',
        },
        pure_api_token => {
            description => 'FlashArray REST API token.',
            type        => 'string',
        },
        protocol => {
            # iscsi/fc/nvme-tcp are block transports handled by this per-disk
            # plugin. 'nfs' is a DIFFERENT storage model (FlashArray File export
            # mounted via PVE's native 'nfs' storage type) and is NOT served by
            # this plugin; it is accepted here only so the connector can record the
            # operator's protocol choice on the hypervisor.
            # NOTE: the nfs path is NOT yet hardware-validated against FlashArray File.
            description => 'Transport: iscsi, fc, nvme-tcp (block), or nfs (FlashArray File).',
            type        => 'string',
            enum        => [ 'iscsi', 'fc', 'nvme-tcp', 'nfs' ],
            default     => 'nvme-tcp',
        },
        host_group => {
            description => 'FlashArray host group the Proxmox nodes belong to.',
            type        => 'string',
        },
        fa_host => {
            description => 'FlashArray single host (when not using a host group).',
            type        => 'string',
        },
        eradicate => {
            description => 'Eradicate (hard-delete) volumes on free_image.',
            type        => 'boolean',
            default     => 0,
        },
        # Per-VM FlashArray volume groups. When enabled (the default), each NEWLY
        # provisioned VM gets its own FA volume group (vgroup) named vm-<vmid> and
        # every disk is created as a member volume <vgroup>/<vol> so a multi-disk
        # VM can be snapshotted crash-consistently as a group. This is STRICTLY
        # ADDITIVE: it only affects volumes created while enabled; volumes whose FA
        # name has no '<vg>/' prefix (existing standalone volumes) keep the exact
        # legacy code path. Set to 0 to provision new volumes standalone (legacy).
        volume_groups => {
            description => 'Provision each new VM into its own FA volume group (vgroup).',
            type        => 'boolean',
            default     => 1,
        },
        # PHIF interface-binding keys (written by the connector's
        # setup_connectivity). When set, the transport activation path binds to
        # the named interfaces/HBAs (see _binding_list + activate_storage/
        # activate_volume). When unset/empty the default transport path is used
        # unchanged.
        iscsi_nics => {
            description => 'PHIF: comma-separated NICs to bind iSCSI sessions to.',
            type        => 'string',
        },
        nvme_sources => {
            description => 'PHIF: comma-separated NVMe-TCP host source addresses.',
            type        => 'string',
        },
        fc_hbas => {
            description => 'PHIF: comma-separated FC HBA WWPNs to restrict to.',
            type        => 'string',
        },
    };
}

sub options {
    return {
        pure_endpoint  => { fixed    => 1 },
        pure_api_token => { fixed    => 1 },
        protocol       => { optional => 1 },
        host_group     => { optional => 1 },
        fa_host        => { optional => 1 },
        eradicate      => { optional => 1 },
        volume_groups  => { optional => 1 },
        content        => { optional => 1 },
        nodes          => { optional => 1 },
        shared         => { optional => 1 },
        disable        => { optional => 1 },
        # PHIF interface-binding device-config keys. The PHIF connector's
        # setup_connectivity action writes these into the storage.cfg stanza so
        # the transport path binds to the operator-selected interfaces/HBAs:
        #   iscsi_nics    comma-separated NIC names bound via open-iscsi ifaces
        #   nvme_sources  comma-separated host source addresses (nvme -w host-traddr)
        #   fc_hbas       comma-separated FC HBA WWPNs to restrict to
        # These are honored by activate_storage/activate_volume (see
        # _binding_list and the per-transport enforcement helpers). The
        # enforcement is strictly additive: when a key is absent/empty the
        # activation path is byte-for-byte the validated default.
        iscsi_nics     => { optional => 1 },
        nvme_sources   => { optional => 1 },
        fc_hbas        => { optional => 1 },
    };
}

# ---------------------------------------------------------------------------
# FlashArray REST v2 helpers
# ---------------------------------------------------------------------------

# Open a REST session: exchange the API token for an auth (x-auth-token) header.
sub _fa_session {
    my ($scfg, $timeout) = @_;
    my $endpoint = $scfg->{pure_endpoint}
        or die "purefa: pure_endpoint not configured\n";
    my $token = $scfg->{pure_api_token}
        or die "purefa: pure_api_token not configured\n";

    # The stored endpoint may be a bare host/IP (e.g. "192.0.2.10"); make it an
    # absolute URL or LWP rejects it ("URL must be absolute").
    $endpoint = "https://$endpoint" unless $endpoint =~ m!^https?://!i;
    $endpoint =~ s!/+$!!;

    # Callers on the hot path (status/pvestatd) pass a short timeout so an
    # unreachable management endpoint can't stall; provisioning paths use 30s.
    my $ua = LWP::UserAgent->new( timeout => $timeout // 30 );
    # check_ssl defaults off to match FlashArray self-signed deployments.
    $ua->ssl_opts( verify_hostname => 0, SSL_verify_mode => 0 );

    # Negotiate a concrete REST version: GET /api/api_version returns the list
    # of supported versions (e.g. ["2.0",...,"2.38"]); use the highest. Falls
    # back to 2.0 if the probe fails.
    my $apiver = '2.0';
    my $vresp = $ua->request( HTTP::Request->new( 'GET', "$endpoint/api/api_version" ) );
    if ( $vresp->is_success ) {
        my $vs = eval { decode_json( $vresp->decoded_content )->{version} };
        $apiver = $vs->[-1] if ref($vs) eq 'ARRAY' && @$vs;
    }

    # POST /api/<ver>/login with api-token header -> returns x-auth-token header.
    my $req = HTTP::Request->new( 'POST', "$endpoint/api/$apiver/login" );
    $req->header( 'api-token' => $token );
    my $resp = $ua->request($req);
    die "purefa: login failed: " . $resp->status_line . "\n"
        unless $resp->is_success;

    my $auth = $resp->header('x-auth-token')
        or die "purefa: no x-auth-token returned by array\n";

    return { ua => $ua, endpoint => $endpoint, apiver => $apiver, auth => $auth };
}

# URL-encode a FlashArray object NAME for use in a query string. Critically this
# MUST encode '/' as %2F: a volume-group MEMBER volume's name is "<vg>/<vol>" and
# an unencoded slash would be parsed as a path separator by the REST router. We
# percent-encode every byte that is not an RFC-3986 unreserved char, so legacy
# names (no slash) are byte-for-byte unchanged (they contain only [A-Za-z0-9._-]).
sub _fa_urlenc {
    my ($v) = @_;
    return '' unless defined $v;
    $v =~ s/([^A-Za-z0-9._~-])/sprintf('%%%02X', ord($1))/ge;
    return $v;
}

sub _fa_request {
    my ($sess, $method, $path, $body) = @_;
    my $req = HTTP::Request->new( $method, "$sess->{endpoint}/api/$sess->{apiver}/$path" );
    $req->header( 'x-auth-token' => $sess->{auth} );
    if ( defined $body ) {
        $req->header( 'Content-Type' => 'application/json' );
        $req->content( encode_json($body) );
    }
    my $resp = $sess->{ua}->request($req);
    die "purefa: $method $path failed: " . $resp->status_line
        . " body=" . $resp->decoded_content . "\n"
        unless $resp->is_success;
    my $content = $resp->decoded_content;
    return $content ? decode_json($content) : {};
}

# Target this volume operation at the configured host or host group.
sub _fa_target {
    my ($scfg) = @_;
    return ( 'host_group', $scfg->{host_group} ) if $scfg->{host_group};
    return ( 'host',       $scfg->{fa_host} )    if $scfg->{fa_host};
    die "purefa: neither host_group nor fa_host configured\n";
}

# ---------------------------------------------------------------------------
# Per-VM FlashArray volume groups (vgroups)
#
# When volume_groups is enabled (default), each VM is provisioned into its own FA
# volume group named vm-<vmid> and every disk is a MEMBER volume "<vg>/<vol>".
# On the array a member's group membership is encoded entirely in its NAME; the
# volume SERIAL (and therefore the host-side multipath WWID / device path) is
# UNCHANGED by membership, so the data path is unaffected -- only the FA name
# carries the "<vg>/" prefix.
#
# CRITICAL ADDITIVE SAFETY: PVE only ever stores/parses the plain PVE volname
# (vm-<vmid>-disk-<N>); parse_volname is UNCHANGED. The "<vg>/" prefix lives only
# on the FA side. Every array op resolves the FA name via _fa_name(), which probes
# the array: if the grouped name "<vg>/<vol>" exists it is used, otherwise the
# bare "<vol>" is used. So an EXISTING standalone volume (no "<vg>/" prefix on the
# array) always resolves to its bare name and follows the exact legacy code path;
# the grouped path triggers only for volumes that were actually created grouped.
# ---------------------------------------------------------------------------

# Are per-VM vgroups enabled? Default ON; honor an explicit 0/false to disable.
sub _vgroups_enabled {
    my ($scfg) = @_;
    my $v = $scfg->{volume_groups};
    return 1 unless defined $v;          # default enabled
    return ( $v && $v ne '0' ) ? 1 : 0;  # honor explicit disable
}

# Derive the (sanitized) vgroup name for a vmid: vm-<vmid>. FA group names allow
# [A-Za-z0-9-_], so strip anything else from the vmid defensively.
sub _vgroup_name {
    my ($vmid) = @_;
    my $id = defined $vmid ? "$vmid" : '';
    $id =~ s/[^A-Za-z0-9_-]//g;
    return length $id ? "vm-$id" : undef;
}

# Split an FA name into (vgroup, member) -- (undef, name) when it has no prefix.
sub _split_fa_name {
    my ($faname) = @_;
    return ( $1, $2 ) if $faname =~ m{^([^/]+)/(.+)$};
    return ( undef, $faname );
}

# Does an FA volume with this exact name exist (live OR pending-eradication)?
sub _fa_volume_exists {
    my ($sess, $faname) = @_;
    my $enc = _fa_urlenc($faname);
    for my $q ( "volumes?names=$enc", "volumes?destroyed=true&names=$enc" ) {
        my $data = eval { _fa_request( $sess, 'GET', $q ) };
        next unless $data;
        return 1 if @{ $data->{items} || [] };
    }
    return 0;
}

# Resolve the on-array FA name for a PVE volname. Prefers the grouped member name
# "vm-<vmid>/<volname>" when that volume actually exists on the array; otherwise
# falls back to the bare <volname> (the legacy / existing-volume path). VM-state
# volumes and any name we can't attribute to a vmid resolve to the bare name.
sub _fa_name {
    my ($class, $scfg, $sess, $volname) = @_;
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    my $vg = _vgroup_name($vmid);
    if ( defined $vg ) {
        my $grouped = "$vg/$name";
        $sess //= _fa_session($scfg);
        return $grouped if _fa_volume_exists( $sess, $grouped );
    }
    return $name;
}

# Ensure the vgroup exists (idempotent). A 400/409 "already exists" is success.
sub _fa_ensure_vgroup {
    my ($sess, $vg) = @_;
    my $enc = _fa_urlenc($vg);
    eval { _fa_request( $sess, 'POST', "volume-groups?names=$enc", {} ); };
    if ( my $err = $@ ) {
        die $err unless $err =~ /already exist|exists|in use|400|409/i;
    }
    # RECOVER a soft-destroyed (pending-eradication) vgroup: after a VM delete the
    # vgroup lingers ~24h as destroyed, and creating a member in it fails with
    # "Volume group has been destroyed". PATCH destroyed=false restores it so the
    # name is reusable immediately. Best-effort (no-op/benign if already live).
    eval { _fa_request( $sess, 'PATCH', "volume-groups?names=$enc",
                        { destroyed => JSON::false } ); };
    return 1;
}

# Count of LIVE member volumes currently in a vgroup (excludes destroyed).
sub _fa_vgroup_member_count {
    my ($sess, $vg) = @_;
    my $enc = _fa_urlenc($vg);
    my $data = eval { _fa_request( $sess, 'GET', "volumes?names=$enc" ) } || {};
    # GET volumes?names=<vg> returns the group's members (names "<vg>/<vol>").
    my $n = 0;
    for my $vol ( @{ $data->{items} || [] } ) {
        next if $vol->{destroyed};
        $n++;
    }
    return $n;
}

# Destroy (and optionally eradicate) a now-empty vgroup. Defensive: never fail
# teardown if the group is missing or the call 404s.
sub _fa_destroy_vgroup {
    my ($sess, $vg, $eradicate) = @_;
    my $enc = _fa_urlenc($vg);
    eval { _fa_request( $sess, 'PATCH', "volume-groups?names=$enc",
        { destroyed => JSON::true } ); };
    if ($eradicate) {
        eval { _fa_request( $sess, 'DELETE', "volume-groups?names=$enc" ); };
    }
    return 1;
}

# ---------------------------------------------------------------------------
# Per-VM protection groups (pgroups) -- crash-consistent group snapshots
#
# A FlashArray volume GROUP (vgroup) is a NAMESPACE, not a consistency group, so
# it CANNOT be snapshotted as a unit (and a pgroup CANNOT contain a vgroup). To
# take a crash-consistent snapshot across all of a VM's disks we maintain a
# per-VM PROTECTION GROUP named "<vgroup>-pg" whose MEMBERS are the vgroup's
# member VOLUMES ("<vg>/<vol>"). Snapshotting the pgroup yields a single group
# snapshot "<pg>.<snap>" whose per-disk members are named EXACTLY
# "<pg>.<snap>.<vg>/<vol>".
#
# PVE calls volume_snapshot / _rollback / _delete ONCE PER DISK, all with the
# SAME $snap for a given VM snapshot. So volume_snapshot is idempotent: it
# (re)creates the pgroup, (re)adds ALL current vgroup members, and takes the
# group snapshot -- the 2nd..Nth per-disk calls find "<pg>.<snap>" already
# created and treat that as benign. rollback/delete resolve the per-disk member
# name via _fa_snapshot_name().
# ---------------------------------------------------------------------------

# The protection-group name for a vgroup: "<vg>-pg".
sub _pgroup_name {
    my ($vg) = @_;
    return defined $vg ? "$vg-pg" : undef;
}

# Create the pgroup (idempotent). A 400/409 "already exists" is success.
sub _fa_ensure_pgroup {
    my ($sess, $pg) = @_;
    my $enc = _fa_urlenc($pg);
    eval { _fa_request( $sess, 'POST', "protection-groups?names=$enc", {} ); };
    if ( my $err = $@ ) {
        die $err unless $err =~ /already exist|exists|in use|400|409/i;
    }
    # Recover a soft-destroyed pgroup (pending eradication after a prior teardown)
    # so a new snapshot can reuse the name. Best-effort/benign if already live.
    eval { _fa_request( $sess, 'PATCH', "protection-groups?names=$enc",
                        { destroyed => JSON::false } ); };
    return 1;
}

# List the LIVE member volume names ("<vg>/<vol>") of a vgroup (skip destroyed).
# Uses the documented filter form: GET volumes?filter=volume_group.name='<vg>'.
sub _fa_vgroup_member_names {
    my ($sess, $vg) = @_;
    my $filter = _fa_urlenc("volume_group.name='$vg'");
    my $data = eval { _fa_request( $sess, 'GET', "volumes?filter=$filter" ) } || {};
    my @names;
    for my $vol ( @{ $data->{items} || [] } ) {
        next if $vol->{destroyed};
        my $n = $vol->{name} // next;
        push @names, $n;
    }
    return @names;
}

# Add member VOLUMES to the pgroup (idempotent; already-member is benign).
# POST protection-groups/volumes?group_names=<pg>&member_names=<vg>/<vol>[,...]
sub _fa_pgroup_add_volumes {
    my ($sess, $pg, @members) = @_;
    return 1 unless @members;
    my $genc = _fa_urlenc($pg);
    my $menc = join( ',', map { _fa_urlenc($_) } @members );
    eval { _fa_request( $sess, 'POST',
        "protection-groups/volumes?group_names=$genc&member_names=$menc", {} ); };
    if ( my $err = $@ ) {
        die $err
            unless $err =~ /already (a )?member|already exist|exists|in use|400|409/i;
    }
    return 1;
}

# Destroy (and optionally eradicate) a whole protection-group SNAPSHOT
# "<pg>.<snap>". A pgroup-snapshot MEMBER cannot be destroyed on its own, so we
# always target the group snapshot. Defensive/idempotent: PVE calls
# volume_snapshot_delete once per disk with the same $snap, so the first disk
# destroys the group snapshot and the rest are benign no-ops (404/not-found).
sub _fa_destroy_pgroup_snapshot {
    my ($sess, $pgsnap, $eradicate) = @_;
    my $enc = _fa_urlenc($pgsnap);
    eval { _fa_request( $sess, 'PATCH', "protection-group-snapshots?names=$enc",
        { destroyed => JSON::true } ); };
    if ($eradicate) {
        eval { _fa_request( $sess, 'DELETE',
            "protection-group-snapshots?names=$enc" ); };
    }
    return 1;
}

# Take a protection-group snapshot "<pg>.<snap>", handling SNAPSHOT-NAME REUSE.
#
# PVE reuses snapshot names, and deleting a snapshot only SOFT-destroys the pgroup
# snapshot (pending eradication ~24h). A naive POST then hits "already exists" for
# that destroyed snapshot; swallowing it would leave PVE recording a snapshot with
# NO live array backing (a silent desync). So on "already exists" we inspect the
# existing snapshot:
#   * LIVE   -> benign (the idempotent 2nd..Nth per-disk call in THIS operation).
#   * DESTROYED (stale, pending eradication from a prior same-named snapshot) ->
#     eradicate it and re-take a FRESH snapshot, so the new one is real.
sub _fa_pgroup_snapshot {
    my ($sess, $pg, $snap) = @_;
    my $post = sub {
        _fa_request( $sess, 'POST',
            "protection-group-snapshots?source_names=" . _fa_urlenc($pg)
            . "&suffix=" . _fa_urlenc($snap), {} );
    };
    eval { $post->(); };
    my $err = $@ or return 1;
    die $err unless $err =~ /already exist|exists|in use|400|409/i;
    my $pgsnap = "$pg.$snap";
    my $data = eval { _fa_request( $sess, 'GET',
        "protection-group-snapshots?names=" . _fa_urlenc($pgsnap) ) } || {};
    my $items = (ref $data eq 'HASH') ? $data->{items} : undef;
    my $destroyed = (ref $items eq 'ARRAY' && @$items) ? $items->[0]{destroyed} : 0;
    if ($destroyed) {
        # Eradicate the stale destroyed snapshot, then take a fresh one.
        eval { _fa_request( $sess, 'DELETE',
            "protection-group-snapshots?names=" . _fa_urlenc($pgsnap) ); };
        eval { $post->(); };
        die $@ if $@;
    }
    # else: a LIVE snapshot with this name already exists -> idempotent, benign.
    return 1;
}

# Destroy (and optionally eradicate) a now-empty pgroup. Defensive: never fail
# teardown if the group is missing or the call 404s.
sub _fa_destroy_pgroup {
    my ($sess, $pg, $eradicate) = @_;
    my $enc = _fa_urlenc($pg);
    eval { _fa_request( $sess, 'PATCH', "protection-groups?names=$enc",
        { destroyed => JSON::true } ); };
    if ($eradicate) {
        eval { _fa_request( $sess, 'DELETE', "protection-groups?names=$enc" ); };
    }
    return 1;
}

# ---------------------------------------------------------------------------
# PHIF interface binding (iscsi_nics / nvme_sources / fc_hbas)
#
# These optional storage.cfg keys let the operator pin each transport to a
# specific set of host interfaces/HBAs. They are STRICTLY ADDITIVE: when a key
# is absent or empty, every helper below is a no-op and the transport activation
# path is identical to the validated default. All helpers are defensive (wrapped
# at the call sites and tolerant of malformed input) so a bad value never blocks
# bringing a volume online.
# ---------------------------------------------------------------------------

# Parse a comma/whitespace-separated binding value into a de-duplicated list.
# Returns an empty list for undef/empty so callers can simply `return unless @x`.
sub _binding_list {
    my ($val) = @_;
    return () unless defined $val && length $val;
    my @out;
    my %seen;
    for my $tok ( split /[,\s]+/, $val ) {
        $tok =~ s/^\s+//;
        $tok =~ s/\s+$//;
        next unless length $tok;
        next if $seen{$tok}++;
        push @out, $tok;
    }
    return @out;
}

# iSCSI: when iscsi_nics is set, log the configured sessions in ONLY via the
# matching open-iscsi ifaces (phif_<nic>, created by the connector). Without a
# binding this is a no-op and the caller's plain `iscsiadm -m session --rescan`
# runs unchanged.
sub _iscsi_login_bound {
    my ($scfg) = @_;
    my @nics = _binding_list( $scfg->{iscsi_nics} );
    return 0 unless @nics;
    for my $nic (@nics) {
        my $iface = "phif_$nic";
        eval {
            run_command(
                [ 'iscsiadm', '-m', 'node', '-I', $iface, '--login' ],
                outfunc => sub { }, errfunc => sub { }, noerr => 1 );
        };
        eval {
            run_command(
                [ 'iscsiadm', '-m', 'session', '-R' ],
                outfunc => sub { }, errfunc => sub { }, noerr => 1 );
        };
    }
    return 1;
}

# NVMe-TCP: when nvme_sources is set, (re)connect one controller per host source
# address via `nvme connect-all ... --host-traddr <src>` so sessions are pinned
# to the selected source interfaces. No binding -> no-op (caller's plain
# `nvme connect-all` runs unchanged).
sub _nvme_connect_bound {
    my ($scfg) = @_;
    my @sources = _binding_list( $scfg->{nvme_sources} );
    return 0 unless @sources;
    for my $src (@sources) {
        eval {
            run_command(
                [ 'nvme', 'connect-all', '--host-traddr', $src ],
                outfunc => sub { }, errfunc => sub { }, noerr => 1 );
        };
    }
    return 1;
}

# FC: when fc_hbas is set, restrict the rescan to the SCSI hosts whose HBA port
# name (/sys/class/fc_host/host*/port_name) matches one of the selected WWPNs,
# instead of rescanning every host. No binding -> no-op (caller's all-host
# rescan runs unchanged).
#
# WWPNs are matched on their bare 16-hex-digit form so colon-delimited
# (21:00:..) and 0x-prefixed sysfs values (0x2100..) compare equal.
sub _fc_normalize_wwpn {
    my ($w) = @_;
    return '' unless defined $w;
    my $s = lc $w;
    $s =~ s/^0x//;
    $s =~ s/[^0-9a-f]//g;
    return $s;
}

sub _fc_rescan_bound {
    my ($scfg) = @_;
    my @hbas = map { _fc_normalize_wwpn($_) } _binding_list( $scfg->{fc_hbas} );
    @hbas = grep { length } @hbas;
    return 0 unless @hbas;
    my %want = map { $_ => 1 } @hbas;

    # Shell: for each fc_host, read its port_name; if it matches a selected WWPN,
    # rescan the SCSI hosts that belong to the same PCI device. Best-effort.
    my $list = join ' ', map { "'$_'" } @hbas;
    my $cmd = <<"SH";
for fh in /sys/class/fc_host/host*; do
    [ -r "\$fh/port_name" ] || continue
    pn=\$(cat "\$fh/port_name" 2>/dev/null | tr 'A-Z' 'a-z')
    pn=\${pn#0x}
    for w in $list; do
        if [ "\$pn" = "\$w" ]; then
            hn=\$(basename "\$fh")
            [ -w "\$fh/issue_lip" ] && echo 1 > "\$fh/issue_lip" 2>/dev/null || true
            for sh in /sys/class/scsi_host/\$hn/scan; do
                [ -w "\$sh" ] && echo '- - -' > "\$sh" 2>/dev/null || true
            done
        fi
    done
done
SH
    eval {
        run_command( [ 'sh', '-c', $cmd ],
            outfunc => sub { }, errfunc => sub { }, noerr => 1 );
    };
    return 1;
}

# Run a housekeeping command (iscsiadm/nvme/multipath rescans) but DISCARD its
# stdout/stderr. By default PVE::Tools::run_command echoes a child's output to the
# parent, so a rescan during activate_volume spams `qm start`'s console (e.g.
# "Rescanning session [sid: ...]"). Best-effort: never dies (wrapped + noerr).
sub _qrun {
    my ($cmd) = @_;
    eval {
        run_command( $cmd, outfunc => sub { }, errfunc => sub { }, noerr => 1 );
    };
}

# ---------------------------------------------------------------------------
# Volume name parsing  (vm-<vmid>-disk-<N>)
# ---------------------------------------------------------------------------

sub parse_volname {
    my ($class, $volname) = @_;

    # Regular VM/template disks: vm-<vmid>-disk-<N> / base-<vmid>-disk-<N>.
    if ( $volname =~ m/^((vm|base)-(\d+)-disk-(\d+))$/ ) {
        my $name  = $1;
        my $vtype = $2 eq 'base' ? 'base' : 'images';
        my $vmid  = $3;
        # (vtype, name, vmid, basename, basevmid, isBase, format)
        return ( $vtype, $name, $vmid, undef, undef, ( $2 eq 'base' ), 'raw' );
    }

    # VM state (RAM) volume created when snapshotting a RUNNING VM with memory:
    # vm-<vmid>-state-<snapname>. qemu writes the migration/vmstate stream to this
    # raw block device. Must parse so path()/activate_volume()/free_image() work.
    if ( $volname =~ m/^(vm-(\d+)-state-(\S+))$/ ) {
        my $name = $1;
        my $vmid = $2;
        return ( 'images', $name, $vmid, undef, undef, 0, 'raw' );
    }

    die "purefa: unable to parse volume name '$volname'\n";
}

sub filesystem_path {
    my ($class, $scfg, $volname, $snapname) = @_;
    die "purefa: snapshot is not a filesystem path\n" if defined $snapname;
    my ($vtype, $name, $vmid) = $class->parse_volname($volname);
    # The block device path is the multipath device resolved by WWID. Resolve the
    # on-array FA name first (grouped "<vg>/<vol>" for new VMs, bare for existing);
    # the WWID/serial is identical either way so the device node is unchanged.
    my $faname = eval { $class->_fa_name($scfg, undef, $volname) } // $name;
    my $path = _device_path($scfg, $name, $faname);
    return wantarray ? ( $path, $vmid, $vtype ) : $path;
}

# We MUST override path(): the base PVE::Storage::Plugin::path builds a path via
# get_subdir($scfg, $vtype), which dies "storage definition has no path" for a
# block storage with no `path` config. (PVE calls path() during, e.g., convert-to-
# template / `qm template`, which surfaced as a spurious "TASK ERROR: storage
# definition has no path" even though create_base succeeded.) Like the stock
# LVM/RBD/ZFS block plugins, we resolve the path ourselves -> the multipath device.
sub path {
    my ($class, $scfg, $volname, $storeid, $snapname) = @_;
    return $class->filesystem_path($scfg, $volname, $snapname);
}

# Fetch the FlashArray volume serial (used to build the SCSI/multipath WWID).
# The 'serial' field on GET /volumes is confirmed against the live cluster's
# Purity//FA REST version (used to assemble the SCSI WWID over iSCSI and FC).
sub _fa_volume_serial {
    my ($scfg, $name, $sess) = @_;
    $sess //= _fa_session($scfg);
    my $data = _fa_request( $sess, 'GET', "volumes?names=" . _fa_urlenc($name) );
    my $vol  = ( $data->{items} || [] )->[0] || {};
    return $vol->{serial};    # 24-hex volume serial, e.g. "0123456789ABCDEF0BB82813"
}

# Build the Linux SCSI multipath WWID for a FlashArray volume serial.
# Everpure volumes use NAA IEEE Registered Extended (type 6) with the Everpure OUI:
#   wwid = "3" . "624a9370" . lc(serial24)   e.g. 3624a93700123456789abcdef0bb82813
# REST returns the 24-hex serial WITHOUT the "624a9370" prefix, so prepend it
# (but tolerate a serial that already includes it).
sub _scsi_wwid {
    my ($serial) = @_;
    my $s = lc($serial);
    $s =~ s/^0x//;
    return "3$s"            if $s =~ /^624a937/;   # already has the OUI
    return "3624a9370$s";                          # prepend NAA-6 + Everpure OUI
}

# Resolve the multipath device path for a FA volume.
#
#   * iSCSI and FC are SCSI transports: Linux multipath names the device by its
#     SCSI WWID, which for a FlashArray volume is "3" . lc(serial)
#     (NAA-6 prefix), e.g. /dev/mapper/3624a937....   No login is needed for FC;
#     the LUN is presented by the fabric once zoned + connected on the array.
#   * NVMe-TCP/FC uses the NVMe namespace EUI/NGUID under /dev/disk/by-id/.
#
# We compute the deterministic path from the array serial so activate_volume can
# return it WITHOUT any session/login step. For FC specifically this is the whole
# point: there is no login, only a rescan, then map-by-WWID.
# Optional 3rd arg $faname: the resolved on-array FA name (may carry a "<vg>/"
# prefix). When omitted we look up by the bare $name (legacy path). The device
# path is derived from the SERIAL, which is identical whether or not the volume
# is a vgroup member -- so multipath is unaffected by membership.
sub _device_path {
    my ($scfg, $name, $faname) = @_;
    my $proto = $scfg->{protocol} // 'nvme-tcp';

    my $serial = eval { _fa_volume_serial($scfg, $faname // $name) };
    if ( $serial ) {
        my $s = lc($serial);
        if ( $proto eq 'nvme-tcp' || $proto eq 'nvme-fc' ) {
            # NVMe: namespace globally-unique id. NOTE: the live cluster was
            # validated over iSCSI + FC only; the NVMe by-id naming
            # (nvme-eui.<nguid>) is NOT yet hardware-validated -- confirm the
            # exact node emitted by your kernel/multipath before relying on it.
            return "/dev/mapper/eui.$s";
        }
        # iSCSI + FC: SCSI WWID = "3" + NAA-6 Everpure OUI (624a9370) + serial.
        return "/dev/mapper/" . _scsi_wwid($serial);
    }
    # Fallback (e.g. array not reachable at path-resolve time): symbolic name.
    return "/dev/mapper/$name";
}

# ---------------------------------------------------------------------------
# Multipath host cleanup -- mirrors the XCP-ng purefa-mpath host plugin.
#
# With find_multipaths=no, EVERY node auto-maps a connected volume. When a volume
# is destroyed/disconnected its dm-multipath map goes STALE (paths gone) on every
# node that mapped it; a leftover map wedges multipathd -- it stops tracking paths
# -- which then BLOCKS new LUNs from assembling and makes a reused LUN number keep
# the old device. So on delete we flush the map + drop the idle SCSI paths, and we
# do it POOL-WIDE (free_image runs on one node, but the stale map is everywhere).
# ---------------------------------------------------------------------------

# Shell that flushes the dm-multipath map for $wwid and deletes the underlying
# SCSI path devices. GUARD: only delete the sd* paths if the map was actually
# removed (i.e. it was NOT in use) -- `multipath -f` refuses to remove a map that
# is open, so a still-attached volume is never torn out from under a running VM.
sub _mpath_flush_cmd {
    my ($wwid) = @_;
    my $dm = "/dev/mapper/$wwid";
    return
        "multipath -f $wwid >/dev/null 2>&1 || true; "
      . "if [ ! -e $dm ]; then "
      .   'for b in /sys/block/sd*; do n=$(basename "$b"); '
      .   'w=$(/lib/udev/scsi_id -g -u -d /dev/$n 2>/dev/null); '
      .   "if [ \"\$w\" = \"$wwid\" ]; then echo 1 > \"\$b/device/delete\" 2>/dev/null || true; fi; done; "
      . "fi; "
      . "multipath -f $wwid >/dev/null 2>&1 || true";
}

# Cluster node names (corosync members); fall back to the local node so this also
# works on a standalone host.
sub _cluster_nodes {
    my @nodes;
    eval {
        require PVE::Cluster;
        my $nl = PVE::Cluster::get_nodelist();
        @nodes = @$nl if $nl;
    };
    if ( !@nodes ) {
        eval {
            my $m = decode_json( PVE::Tools::file_get_contents('/etc/pve/.members') );
            @nodes = keys %{ $m->{nodelist} || {} };
        };
    }
    if ( !@nodes ) { chomp( my $h = `hostname` ); @nodes = ($h) if $h; }
    return @nodes;
}

# Run a shell command on EVERY cluster node: locally on this node, via root SSH on
# the others (PVE sets up passwordless root SSH between cluster members). Best
# effort + quiet -- a delete must not be blocked or made noisy by one bad node.
sub _run_poolwide {
    my ($cmd) = @_;
    chomp( my $local = `hostname` );
    my %quiet = ( outfunc => sub { }, errfunc => sub { }, noerr => 1 );
    for my $node ( _cluster_nodes() ) {
        eval {
            if ( $node eq $local ) {
                run_command( [ 'sh', '-c', $cmd ], %quiet );
            }
            else {
                run_command(
                    [ 'ssh', '-o', 'BatchMode=yes', '-o', 'ConnectTimeout=5',
                      "root\@$node", $cmd ], %quiet );
            }
        };
        warn "purefa: multipath cleanup on $node failed: $@" if $@;
    }
}

# ---------------------------------------------------------------------------
# Allocation: create a FA volume + connect it to the host/host group.
# ---------------------------------------------------------------------------

# Return a hash{index => 1} of the vm-<vmid>-disk-<N> indices already taken on the
# array, for the given destroyed-state. FlashArray's GET volumes returns only LIVE
# volumes by default; destroyed (pending-eradication) volumes must be queried with
# ?destroyed=true. We consider BOTH so a name that still physically exists on the
# array -- even if soft-deleted and awaiting eradication -- is never reused (which
# would fail with a "...pending eradication" / already-exists error).
sub _vmdisk_indices {
    my ($sess, $vmid, $destroyed) = @_;
    my $path = $destroyed ? 'volumes?destroyed=true' : 'volumes';
    my $data = eval { _fa_request( $sess, 'GET', $path ) } || {};
    my %taken;
    for my $vol ( @{ $data->{items} || [] } ) {
        my $n = $vol->{name} // next;
        # A grouped member's name is "<vg>/<vol>"; strip the vgroup prefix so the
        # per-VM disk index is read from the member name (legacy bare names match
        # unchanged since they have no '/').
        ( undef, my $member ) = _split_fa_name($n);
        $taken{$1} = 1 if $member =~ /^(?:vm|base)-\Q$vmid\E-disk-(\d+)$/;
    }
    return %taken;
}

# Pick the lowest-free vm-<vmid>-disk-<N> name, skipping indices used by ANY volume
# on the array (live or pending-eradication). Overrides the PVE base method, which
# only consults list_images() (live volumes) and would therefore reuse the name of
# a just-destroyed disk that the array still holds pending eradication.
sub find_free_diskname {
    my ($class, $storeid, $scfg, $vmid, $fmt, $add_fmt_suffix) = @_;
    my $sess = _fa_session($scfg);
    my %taken = ( _vmdisk_indices( $sess, $vmid, 0 ),
                  _vmdisk_indices( $sess, $vmid, 1 ) );
    my $n = 0;
    $n++ while $taken{$n};
    return "vm-$vmid-disk-$n";
}

sub alloc_image {
    my ($class, $storeid, $scfg, $vmid, $fmt, $name, $size) = @_;

    die "purefa: only raw format is supported (got '$fmt')\n"
        if defined $fmt && $fmt ne 'raw';

    my $sess = _fa_session($scfg);
    my $bytes = $size * 1024;    # PVE passes size in KiB.
    # FlashArray rejects volumes smaller than 1 MiB ("Volume size must be between
    # 1 MB and 4 PB"). PVE legitimately requests tiny disks -- notably the UEFI
    # efidisk0 (~528 KiB) -- so floor to the 1 MiB array minimum (512-byte aligned,
    # as FA requires). Mirrors FA_MIN_VOLUME_BYTES in the HPE VME Morpheus plugin.
    my $FA_MIN_VOLUME_BYTES = 1048576;
    $bytes = $FA_MIN_VOLUME_BYTES if $bytes < $FA_MIN_VOLUME_BYTES;
    my $autoname = !$name;       # did PVE leave the name for us to choose?

    # Derive vm-<vmid>-disk-<N> if PVE did not pass an explicit name.
    $name = $class->find_free_diskname( $storeid, $scfg, $vmid ) if $autoname;

    # Per-VM vgroup: when enabled, create the disk as a member volume
    # "<vg>/<vol>" inside the VM's own volume group (creating the group first).
    # This is the ONLY place the "<vg>/" prefix is introduced; everywhere else
    # resolves it back from the array. PVE still receives the plain PVE volname.
    my $vg = _vgroups_enabled($scfg) ? _vgroup_name($vmid) : undef;
    _fa_ensure_vgroup( $sess, $vg ) if defined $vg;

    # The on-array name we create: grouped member when vgroups are on, else bare.
    my $faname = defined $vg ? "$vg/$name" : $name;

    # Create the volume. If the chosen name still collides on the array (e.g. a
    # concurrent alloc, or a name pending eradication that slipped past the scan),
    # the POST fails -- when WE picked the name, recompute a fresh free name and
    # retry a few times rather than surfacing the eradication error to the user.
    my $tries = $autoname ? 5 : 1;
    while (1) {
        my $ok = eval {
            _fa_request( $sess, 'POST', "volumes?names=" . _fa_urlenc($faname),
                { provisioned => $bytes } );
            1;
        };
        last if $ok;
        my $err = $@ || 'unknown error';
        die $err if !$autoname || --$tries <= 0
            || $err !~ /eradicat|already exist|in use|exists/i;
        $name   = $class->find_free_diskname( $storeid, $scfg, $vmid );
        $faname = defined $vg ? "$vg/$name" : $name;
    }

    # Connect the new volume to the cluster's host/host group (by full FA name).
    my ( $tkey, $tval ) = _fa_target($scfg);
    my $cparam = $tkey eq 'host_group' ? 'host_group_names' : 'host_names';
    _fa_request( $sess, 'POST',
        "connections?${cparam}=$tval&volume_names=" . _fa_urlenc($faname), {} );

    # PVE only ever sees/stores the plain PVE volname; the "<vg>/" prefix lives
    # exclusively on the array and is re-resolved by _fa_name() on later ops.
    return $name;
}

sub free_image {
    my ($class, $storeid, $scfg, $volname, $isBase, $format) = @_;

    my ($vtype, $name) = $class->parse_volname($volname);
    my $sess = _fa_session($scfg);

    # Resolve the on-array FA name (grouped "<vg>/<vol>" for new VMs, bare for
    # existing standalone volumes). All name-based ops below use the full FA name.
    my $faname = $class->_fa_name( $scfg, $sess, $volname );
    my ( $vg ) = _split_fa_name($faname);
    my $enc = _fa_urlenc($faname);

    # Capture the serial BEFORE destroying so we can flush the host multipath map.
    my $serial = eval { _fa_volume_serial( $scfg, $faname, $sess ) };

    # Disconnect from host/host group first.
    my ( $tkey, $tval ) = _fa_target($scfg);
    my $cparam = $tkey eq 'host_group' ? 'host_group_names' : 'host_names';
    eval {
        _fa_request( $sess, 'DELETE',
            "connections?${cparam}=$tval&volume_names=$enc" );
    };

    # Destroy the volume (soft delete). PATCH destroyed=true.
    _fa_request( $sess, 'PATCH', "volumes?names=$enc",
        { destroyed => JSON::true } );

    # Optionally eradicate (hard delete) immediately.
    if ( $scfg->{eradicate} ) {
        _fa_request( $sess, 'DELETE', "volumes?names=$enc" );
    }

    # If this was the LAST member of its vgroup, tear the (now-empty) group down
    # too -- soft-delete, and eradicate when configured. Defensive: never fail
    # teardown if the group is already gone or the call 404s. Only triggers for
    # grouped volumes; standalone volumes have no $vg and skip this entirely.
    if ( defined $vg ) {
        my $remaining = _fa_vgroup_member_count( $sess, $vg );
        if ( $remaining == 0 ) {
            # Also tear down the per-VM protection group "<vg>-pg" used for
            # crash-consistent group snapshots (best-effort/idempotent; ignore
            # 404 if it was never created or already gone).
            eval { _fa_destroy_pgroup( $sess, _pgroup_name($vg),
                $scfg->{eradicate} ); };
            _fa_destroy_vgroup( $sess, $vg, $scfg->{eradicate} );
        }
    }

    # The volume is now disconnected + destroyed, so its dm-multipath map is STALE
    # on every node that mapped it. Flush the map + drop the idle SCSI paths
    # POOL-WIDE so a deleted disk leaves nothing behind on ANY host and a reused
    # LUN number assembles cleanly. SCSI transports only (NVMe-TCP uses native
    # nvme multipath, not dm-multipath).
    my $proto = $scfg->{protocol} // 'nvme-tcp';
    if ( $serial && $proto ne 'nvme-tcp' && $proto ne 'nvme-fc' ) {
        _run_poolwide( _mpath_flush_cmd( _scsi_wwid($serial) ) );
    }
    return undef;
}

sub list_images {
    my ($class, $storeid, $scfg, $vmid, $vollist, $cache) = @_;

    my $sess = _fa_session($scfg);
    # GET /volumes returns provisioned size + name for vm-*-disk-* volumes.
    my $data = _fa_request( $sess, 'GET', 'volumes' );

    my $res = [];
    for my $vol ( @{ $data->{items} || [] } ) {
        # Strip any "<vg>/" prefix: PVE only knows the bare PVE volname, so a
        # grouped member volume "<vg>/<vol>" must surface to PVE as "<vol>".
        # Legacy standalone names have no '/' and are unchanged.
        ( undef, my $name ) = _split_fa_name( $vol->{name} // '' );
        # Skip soft-deleted (destroyed / pending-eradication) volumes so a
        # destroyed disk no longer lingers in `pvesm list` after a VM destroy.
        next if $vol->{destroyed};
        # Disks (vm/base-<vmid>-disk-<N>) and VM-state volumes (vm-<vmid>-state-<snap>).
        next unless $name =~ m/^(?:vm|base)-(\d+)-(?:disk-\d+|state-\S+)$/;
        my $owner = $1;
        next if defined $vmid && $owner != $vmid;
        my $volid = "$storeid:$name";
        next if $vollist && !grep { $_ eq $volid } @$vollist;
        push @$res, {
            volid  => $volid,
            format => 'raw',
            size   => $vol->{provisioned},
            vmid   => $owner,
        };
    }
    return $res;
}

sub status {
    my ($class, $storeid, $scfg, $cache) = @_;

    # Capacity is INFORMATIONAL ONLY. pvestatd polls status() constantly and the
    # PVE web UI/`pvesm status` block on it, so this must be fast and must never
    # die: a slow or unreachable FlashArray *management* endpoint must not hang
    # `pvesm status` or flip the storage inactive. Real usability is the DATA path
    # (multipath/iSCSI/NVMe) set up by activate_storage, not the REST mgmt API.
    #
    # Best-effort capacity probe: short HTTP timeout + a hard wall-clock cap via
    # alarm(); on ANY failure report active with capacity unknown (zeros).
    my ($total, $avail, $used, $active) = (0, 0, 0, 1);
    eval {
        local $SIG{ALRM} = sub { die "purefa: status capacity probe timed out\n" };
        alarm(10);
        my $sess  = _fa_session( $scfg, 6 );
        my $data  = _fa_request( $sess, 'GET', 'arrays/space' );
        my $space = ( $data->{items} || [] )->[0] || {};
        $total = $space->{capacity}              // 0;
        $used  = $space->{space}->{total_physical} // 0;
        $avail = $total - $used;
    };
    alarm(0);
    warn "purefa: capacity probe failed (storage stays active): $@" if $@;
    return ( $total, $avail, $used, $active );
}

# ---------------------------------------------------------------------------
# Activation: transport login + multipath, return the block device path.
# ---------------------------------------------------------------------------

sub activate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    # Ensure transport + multipath services are running on this node.
    my $proto = $scfg->{protocol} // 'nvme-tcp';
    if ( $proto eq 'iscsi' ) {
        eval { run_command([ 'systemctl', 'start', 'iscsid', 'multipathd' ]) };
    } elsif ( $proto eq 'nvme-tcp' ) {
        eval { run_command([ 'modprobe', 'nvme-tcp' ]) };
    } else {    # fc: no login daemon; just ensure multipath is running. Zoning is
                # configured on the SAN fabric (off-host), not here.
        eval { run_command([ 'systemctl', 'start', 'multipathd' ]) };
    }
    return 1;
}

sub deactivate_storage {
    my ($class, $storeid, $scfg, $cache) = @_;
    return 1;
}

sub activate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
    my ($vtype, $name) = $class->parse_volname($volname);
    my $proto = $scfg->{protocol} // 'nvme-tcp';

    # Make the freshly-connected LUN visible on this node, then let multipath
    # assemble the device. The guest is handed the raw /dev/mapper/<wwid> node.
    #
    # The connect path differs FUNDAMENTALLY by transport:
    #   * NVMe-TCP / iSCSI are IP transports with a SESSION LOGIN: we (re)connect
    #     the controller/session so the namespace/LUN appears.
    #   * FIBRE CHANNEL HAS NO LOGIN. The fabric presents LUNs once the HBA WWPNs
    #     are zoned to the array and the volume is connected to the host group on
    #     the array. Here we only RESCAN the SCSI bus (optionally issue an FC LIP)
    #     and then map the device by its WWID. Do NOT call iscsiadm/nvme here.
    if ( $proto eq 'nvme-tcp' ) {
        # NVMe-TCP: ensure the controller is connected (login/discovery). When
        # nvme_sources is set, pin the connect to the selected host source
        # interfaces; otherwise fall back to the validated plain connect-all.
        unless ( eval { _nvme_connect_bound($scfg) } ) {
            _qrun([ 'nvme', 'connect-all' ]);
        }
    } elsif ( $proto eq 'iscsi' ) {
        # iSCSI: rescan the existing logged-in sessions for the new LUN. When
        # iscsi_nics is set, additionally ensure the bound ifaces are logged in
        # (the plain session rescan below still runs for the default case).
        eval { _iscsi_login_bound($scfg) };
        _qrun([ 'iscsiadm', '-m', 'session', '--rescan' ]);
    } else {    # fc -- NO login; rescan + map by WWID only.
        # Re-scan the FC/SCSI hosts so the zoned LUN is enumerated. When fc_hbas
        # is set, restrict the LIP + scan to the matching HBAs; otherwise rescan
        # every host. Best-effort LIP nudges the fabric; rescan-scsi-bus.sh
        # (sg3-utils) is preferred when installed. This FC rescan sequence is
        # confirmed on the live cluster.
        unless ( eval { _fc_rescan_bound($scfg) } ) {
            _qrun([ 'sh', '-c',
                'for f in /sys/class/fc_host/host*/issue_lip; do '
              . '[ -w "$f" ] && echo 1 > "$f" || true; done' ]);
            _qrun([ 'sh', '-c',
                'for h in /sys/class/scsi_host/host*/scan; do echo "- - -" > $h; done' ]);
        }
        _qrun([ 'sh', '-c',
            'command -v rescan-scsi-bus.sh >/dev/null 2>&1 && '
          . 'rescan-scsi-bus.sh -a || true' ]);
    }
    _qrun([ 'multipath', '-r' ]);

    # Wait (up to ~30s) for the multipath device (by WWID) to settle, reloading
    # each pass. If it still hasn't appeared on a SCSI transport, the LUN number
    # was likely REUSED -- a prior volume at this LUN wasn't cleaned off this host,
    # so the kernel caches the old identity and a plain rescan won't re-map it.
    # Force a remove+add bus scan (rescan-scsi-bus.sh -r drops vanished LUNs and
    # adds new ones) to clear the stale identity, then keep polling. Self-heals
    # even if a prior free_image's cleanup was missed. (NVMe uses native multipath,
    # so there's no dm-by-wwid node to wait on.)
    my $faname = eval { $class->_fa_name($scfg, undef, $volname) } // $name;
    my $path = _device_path($scfg, $name, $faname);
    _qrun([ 'sh', '-c', "udevadm settle || true" ]);
    if ( $proto ne 'nvme-tcp' && $proto ne 'nvme-fc' ) {
        my $healed = 0;
        for my $i ( 1 .. 30 ) {
            last if -e $path;
            # Re-issue the transport rescan periodically (not once): a freshly
            # connected LUN can be missed by the first rescan (the array may not
            # have presented it yet, or a transient network blip dropped it), and
            # a plain `multipath -r` can't assemble a device the SCSI layer never
            # discovered. Re-scanning every few seconds lets a late LUN self-heal.
            if ( $i % 5 == 0 && $proto eq 'iscsi' ) {
                _qrun([ 'iscsiadm', '-m', 'node', '--rescan' ]);
                _qrun([ 'iscsiadm', '-m', 'session', '--rescan' ]);
            }
            if ( $i == 8 && !$healed ) {
                $healed = 1;
                _qrun([ 'sh', '-c',
                    'command -v rescan-scsi-bus.sh >/dev/null 2>&1 && '
                  . 'rescan-scsi-bus.sh -r >/dev/null 2>&1 || true' ]);
                _qrun([ 'iscsiadm', '-m', 'session', '--rescan' ])
                    if $proto eq 'iscsi';
            }
            _qrun([ 'multipath', '-r' ]);
            sleep 1;
        }
    }
    return 1;
}

sub deactivate_volume {
    my ($class, $storeid, $scfg, $volname, $snapname, $cache) = @_;
    # This node no longer needs the device (VM stopped / migrated away). Flush the
    # LOCAL dm-multipath map + idle SCSI paths so a stale map doesn't linger on a
    # migration-source node (with find_multipaths=no it would wedge multipathd and
    # block future LUNs). The _mpath_flush_cmd GUARD makes this safe: `multipath -f`
    # won't remove a map still in use, and the SCSI paths are only deleted if the
    # map was actually removed -- so a still-attached disk is never torn out. The
    # device re-assembles on the next activate_volume. SCSI transports only.
    return 1 if defined $snapname;    # a snapshot has no device of its own
    my $proto = $scfg->{protocol} // 'nvme-tcp';
    return 1 if $proto eq 'nvme-tcp' || $proto eq 'nvme-fc';
    my ( $vtype, $name ) = $class->parse_volname($volname);
    my $faname = eval { $class->_fa_name( $scfg, undef, $volname ) } // $name;
    my $serial = eval { _fa_volume_serial( $scfg, $faname ) };
    if ($serial) {
        eval {
            run_command(
                [ 'sh', '-c', _mpath_flush_cmd( _scsi_wwid($serial) ) ],
                outfunc => sub { }, errfunc => sub { }, noerr => 1 );
        };
    }
    return 1;
}

# ---------------------------------------------------------------------------
# Sizing + resize
# ---------------------------------------------------------------------------

sub volume_size_info {
    my ($class, $scfg, $storeid, $volname, $timeout) = @_;
    my ($vtype, $name) = $class->parse_volname($volname);
    my $sess = _fa_session($scfg);
    my $faname = $class->_fa_name( $scfg, $sess, $volname );
    my $data = _fa_request( $sess, 'GET', "volumes?names=" . _fa_urlenc($faname) );
    my $vol  = ( $data->{items} || [] )->[0] || {};
    return wantarray
        ? ( $vol->{provisioned}, 'raw', $vol->{provisioned}, undef )
        : $vol->{provisioned};
}

sub volume_resize {
    my ($class, $scfg, $storeid, $volname, $size, $running) = @_;
    my ($vtype, $name) = $class->parse_volname($volname);
    my $sess = _fa_session($scfg);
    my $faname = $class->_fa_name( $scfg, $sess, $volname );

    # FA extend: PATCH provisioned (bytes). PVE passes size in bytes here.
    _fa_request( $sess, 'PATCH', "volumes?names=" . _fa_urlenc($faname),
        { provisioned => $size } );

    # Rescan so the node sees the new capacity. FC has no login: just rescan.
    my $proto = $scfg->{protocol} // 'nvme-tcp';
    if ( $proto eq 'nvme-tcp' ) {
        _qrun([ 'nvme', 'connect-all' ]);
    } elsif ( $proto eq 'iscsi' ) {
        _qrun([ 'iscsiadm', '-m', 'session', '--rescan' ]);
    } else {    # fc
        _qrun([ 'sh', '-c',
            'for h in /sys/class/scsi_host/host*/scan; do echo "- - -" > $h; done' ]);
    }
    _qrun([ 'multipath', '-r' ]);
    return 1;
}

# ---------------------------------------------------------------------------
# Snapshots (ON THE ARRAY)
# ---------------------------------------------------------------------------

sub volume_snapshot {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    my ($vtype, $name) = $class->parse_volname($volname);
    my $sess = _fa_session($scfg);

    my $faname = $class->_fa_name( $scfg, $sess, $volname );
    my ( $vg ) = _split_fa_name($faname);

    if ( defined $vg ) {
        # GROUPED disk: take a CRASH-CONSISTENT protection-group snapshot.
        #
        # A vgroup is a NAMESPACE, not a consistency group (and a pgroup cannot
        # contain a vgroup), so we maintain a per-VM PROTECTION GROUP "<vg>-pg"
        # whose members are the vgroup's member VOLUMES, then snapshot the pgroup.
        # The group snapshot is "<pg>.<snap>" with per-disk members named
        # "<pg>.<snap>.<vg>/<vol>".
        #
        # PVE calls volume_snapshot ONCE PER DISK with the SAME $snap, so this is
        # idempotent: we (re)create the pgroup and (re)add ALL current vgroup
        # members BEFORE snapshotting -- never just the current disk -- so the
        # very first per-disk call already captures every disk. The 2nd..Nth
        # per-disk calls re-take the snapshot, but "<pg>.<snap>" already exists
        # and that "already exists" is treated as benign.
        my $pg = _pgroup_name($vg);
        _fa_ensure_pgroup( $sess, $pg );

        # Enumerate ALL current members of the vgroup and add them ALL.
        my @members = _fa_vgroup_member_names( $sess, $vg );
        # Defensive: ensure THIS disk is included even if the listing missed it.
        push @members, $faname unless grep { $_ eq $faname } @members;
        _fa_pgroup_add_volumes( $sess, $pg, @members );

        # Snapshot the pgroup (handles same-name reuse vs a destroyed snapshot).
        _fa_pgroup_snapshot( $sess, $pg, $snap );
        return 1;
    }

    # STANDALONE disk (no "<vg>/" prefix): per-volume snapshot "<faname>.<snap>".
    # Handle snapshot-name reuse: if a DESTROYED (pending-eradication) snapshot of
    # the same name lingers, eradicate it and retake a fresh one so PVE's snapshot
    # is really backed (mirrors _fa_pgroup_snapshot).
    my $vsnap = "$faname.$snap";
    eval { _fa_request( $sess, 'POST',
        "volume-snapshots?source_names=" . _fa_urlenc($faname)
        . "&suffix=" . _fa_urlenc($snap), {} ); };
    if ( my $err = $@ ) {
        die $err unless $err =~ /already exist|exists|in use|400|409/i;
        my $data = eval { _fa_request( $sess, 'GET',
            "volume-snapshots?names=" . _fa_urlenc($vsnap) ) } || {};
        my $items = (ref $data eq 'HASH') ? $data->{items} : undef;
        if ( ref $items eq 'ARRAY' && @$items && $items->[0]{destroyed} ) {
            eval { _fa_request( $sess, 'DELETE',
                "volume-snapshots?names=" . _fa_urlenc($vsnap) ); };
            _fa_request( $sess, 'POST',
                "volume-snapshots?source_names=" . _fa_urlenc($faname)
                . "&suffix=" . _fa_urlenc($snap), {} );
        }
    }
    return 1;
}

# The FA snapshot name for a (resolved) FA name + suffix.
#   * vgroup member "<vg>/<vol>": the snapshot lives inside the protection-group
#     snapshot, named EXACTLY "<vg>-pg.<snap>.<vg>/<vol>".
#   * standalone "<vol>": the per-volume snapshot "<vol>.<snap>".
sub _fa_snapshot_name {
    my ($faname, $snap) = @_;
    my ( $vg ) = _split_fa_name($faname);
    if ( defined $vg ) {
        my $pg = _pgroup_name($vg);
        return "$pg.$snap.$faname";
    }
    return "$faname.$snap";
}

sub volume_snapshot_rollback {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    my ($vtype, $name) = $class->parse_volname($volname);
    my $sess = _fa_session($scfg);
    my $faname = $class->_fa_name( $scfg, $sess, $volname );
    my $snapname = _fa_snapshot_name( $faname, $snap );
    # Overwrite the volume from its snapshot: POST /volumes with source = snapshot.
    # Works for BOTH forms: a grouped member's source is the pgroup-snapshot member
    # "<vg>-pg.<snap>.<vg>/<vol>"; a standalone's source is "<vol>.<snap>". The
    # overwrite=true semantics are confirmed against the live cluster's Purity.
    _fa_request( $sess, 'POST',
        "volumes?names=" . _fa_urlenc($faname) . "&overwrite=true",
        { source => { name => $snapname } } );
    return 1;
}

sub volume_snapshot_delete {
    my ($class, $scfg, $storeid, $volname, $snap) = @_;
    my ($vtype, $name) = $class->parse_volname($volname);
    my $sess = _fa_session($scfg);
    my $faname = $class->_fa_name( $scfg, $sess, $volname );
    my ( $vg ) = _split_fa_name($faname);

    if ( defined $vg ) {
        # GROUPED disk: a pgroup-snapshot MEMBER cannot be destroyed alone, so we
        # destroy the WHOLE protection-group snapshot "<vg>-pg.<snap>". PVE calls
        # this once per disk with the same $snap, so the first disk's delete
        # destroys (and eradicates when configured) the group snapshot and the
        # rest are benign no-ops. Wrap in eval + ignore not-found.
        my $pg     = _pgroup_name($vg);
        my $pgsnap = "$pg.$snap";
        eval { _fa_destroy_pgroup_snapshot( $sess, $pgsnap, $scfg->{eradicate} ); };
        return 1;
    }

    # STANDALONE disk: destroy the per-volume snapshot "<faname>.<snap>" as today.
    my $snapname = _fa_snapshot_name( $faname, $snap );
    _fa_request( $sess, 'PATCH',
        "volume-snapshots?names=" . _fa_urlenc($snapname),
        { destroyed => JSON::true } );
    eval { _fa_request( $sess, 'DELETE',
        "volume-snapshots?names=" . _fa_urlenc($snapname) ); };
    return 1;
}

# ---------------------------------------------------------------------------
# Template / base image (enables OFFLOADED linked clones)
# ---------------------------------------------------------------------------

# Proxmox only routes a clone through clone_image() -- our array-side volume copy
# -- for a LINKED clone, i.e. cloning a TEMPLATE. A plain `qm clone` of a normal
# VM is a FULL clone, which PVE performs with a host-side qemu-img / drive-mirror
# block copy (no storage offload hook exists for that). So to get array-offloaded
# clones, the source must first be converted to a template (`qm template`), which
# calls create_base(). Without this method PVE falls back to its file-based base
# class and fails on our block volumes.
#
# We convert in place by RENAMING the FlashArray volume vm-<vmid>-disk-<N> ->
# base-<vmid>-disk-<N> (PATCH /volumes name). The data, serial and host-group
# connection are preserved; the volume just becomes the read-only base that
# clone_image() copies on the array.
sub create_base {
    my ($class, $storeid, $scfg, $volname) = @_;

    my ($vtype, $name, $vmid, undef, undef, $isBase) =
        $class->parse_volname($volname);
    die "purefa: $volname is already a base image\n" if $isBase;

    my $newname = $name;
    $newname =~ s/^vm-/base-/
        or die "purefa: cannot derive a base name from '$name'\n";

    my $sess = _fa_session($scfg);
    # Resolve the on-array name; for a grouped member ("<vg>/vm-...") rename the
    # MEMBER part in place and keep it inside its vgroup ("<vg>/base-..."). The
    # connection, data and serial are preserved. PVE still gets the bare PVE name.
    my $faname = $class->_fa_name( $scfg, $sess, $volname );
    my ( $vg ) = _split_fa_name($faname);
    my $newfaname = defined $vg ? "$vg/$newname" : $newname;
    _fa_request( $sess, 'PATCH', "volumes?names=" . _fa_urlenc($faname),
        { name => $newfaname } );

    return $newname;
}

# ---------------------------------------------------------------------------
# Clone (ON THE ARRAY: volume copy)
# ---------------------------------------------------------------------------

sub clone_image {
    my ($class, $scfg, $storeid, $volname, $vmid, $snap) = @_;
    my ($vtype, $name) = $class->parse_volname($volname);

    my $sess  = _fa_session($scfg);
    my $clone = $class->find_free_diskname( $storeid, $scfg, $vmid );

    # Source: resolve the on-array source name (grouped or bare); for a snapshot
    # source use the snapshot's member name (group-snap "<vg>.<snap>/<vol>" or
    # per-volume "<vol>.<snap>").
    my $srcfa  = $class->_fa_name( $scfg, $sess, $volname );
    my $source = defined $snap ? _fa_snapshot_name( $srcfa, $snap ) : $srcfa;

    # Destination: the clone is a NEW disk for the target VM, so it lands in that
    # VM's own vgroup ("<vg>/<clone>") when vgroups are enabled, creating the
    # group first. Disabled or no vmid -> standalone "<clone>" (legacy path).
    my $vg = _vgroups_enabled($scfg) ? _vgroup_name($vmid) : undef;
    _fa_ensure_vgroup( $sess, $vg ) if defined $vg;
    my $clonefa = defined $vg ? "$vg/$clone" : $clone;

    # FA volume copy: POST /volumes?names=<clone> body { source: { name: <src> } }
    _fa_request( $sess, 'POST', "volumes?names=" . _fa_urlenc($clonefa),
        { source => { name => $source } } );

    # Connect the clone to the host/host group so the new VM can use it.
    my ( $tkey, $tval ) = _fa_target($scfg);
    my $cparam = $tkey eq 'host_group' ? 'host_group_names' : 'host_names';
    _fa_request( $sess, 'POST',
        "connections?${cparam}=$tval&volume_names=" . _fa_urlenc($clonefa), {} );

    return $clone;
}

# ---------------------------------------------------------------------------
# Feature advertisement
# ---------------------------------------------------------------------------

sub volume_has_feature {
    my ($class, $scfg, $feature, $storeid, $volname, $snapname, $running) = @_;

    my $features = {
        snapshot   => { current => 1, snap => 1 },
        clone      => { base    => 1, current => 1, snap => 1 },
        copy       => { base    => 1, current => 1, snap => 1 },
        sparseinit => { base    => 1, current => 1 },
        template   => { current => 1 },
    };

    my ($vtype, $name, $vmid, undef, undef, $isBase) =
        $class->parse_volname($volname);
    my $key = $isBase ? 'base' : 'current';
    $key = 'snap' if defined $snapname;
    return 1 if $features->{$feature}->{$key};
    return undef;
}

1;
