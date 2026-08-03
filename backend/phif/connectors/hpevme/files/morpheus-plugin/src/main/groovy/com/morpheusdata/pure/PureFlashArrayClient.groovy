package com.morpheusdata.pure

import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import groovy.util.logging.Slf4j

import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManager
import javax.net.ssl.X509TrustManager
import java.security.cert.X509Certificate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/**
 * Minimal FlashArray REST v2 client (token login + version negotiation),
 * mirroring the proven logic in the PHIF Proxmox {@code PureFAPlugin.pm} and the
 * XCP-ng SMAPIv3 {@code purefa_fa.py}: per-VDI/-disk volume
 * create/destroy/snapshot/copy/resize, connect/disconnect to a host group, and
 * SCSI WWID derivation (NAA-6 + Everpure OUI).
 *
 * Uses only {@code java.net} + {@code groovy.json} so the plugin JAR needs no
 * bundled HTTP dependency. TLS verification is disabled (FlashArray management
 * endpoints are typically self-signed), matching every other PHIF FA client.
 */
@Slf4j
class PureFlashArrayClient {

    static final String PURE_OUI = '624a9370'   // FlashArray NAA-6 IEEE Registered Extended OUI

    String endpoint
    String token
    int timeoutMs
    private String authToken
    private String apiVersion

    PureFlashArrayClient(String endpoint, String token, int timeoutMs = 30000) {
        String ep = endpoint ?: ''
        if (!ep.startsWith('http://') && !ep.startsWith('https://')) {
            ep = 'https://' + ep
        }
        this.endpoint = ep.replaceAll('/+$', '')
        this.token = token
        this.timeoutMs = timeoutMs
    }

    // ----------------------------------------------------------------- WWID --
    /** Build the dm-multipath WWID for a FA volume serial (NAA-6 + Everpure OUI). */
    static String scsiWwid(String serial) {
        String s = (serial ?: '').toLowerCase()
        if (s.startsWith('0x')) s = s.substring(2)
        if (s.startsWith('624a937')) return '3' + s
        return '3' + PURE_OUI + s
    }

    /** Host block-device path for a volume serial; NVMe uses the EUI namespace. */
    static String devicePath(String serial, String protocol = 'iscsi') {
        String s = (serial ?: '').toLowerCase()
        if (protocol in ['nvme-tcp', 'nvme-fc', 'nvme-roce']) {
            return '/dev/disk/by-id/nvme-eui.' + s
        }
        return '/dev/mapper/' + scsiWwid(serial)
    }

    // -------------------------------------------------------------- transport --
    private SSLContext trustAllContext() {
        def trustAll = [
            getAcceptedIssuers: { -> null },
            checkClientTrusted: { X509Certificate[] c, String a -> },
            checkServerTrusted: { X509Certificate[] c, String a -> }
        ] as X509TrustManager
        SSLContext ctx = SSLContext.getInstance('TLS')
        ctx.init(null, [trustAll] as TrustManager[], new java.security.SecureRandom())
        return ctx
    }

    static {
        // FlashArray management endpoints are self-signed and reached by IP, so
        // disable java.net.http endpoint (hostname) identification -- trustAllContext()
        // already accepts the cert chain; this allows the IP/CN mismatch.
        System.setProperty('jdk.internal.httpclient.disableHostnameVerification', 'true')
    }

    private Map http(String method, String url, Object body, Map headers) {
        // java.net.http.HttpClient (JDK 11) supports PATCH natively. The legacy
        // HttpURLConnection threw "Invalid HTTP method: PATCH", which broke volume
        // destroy/resize/snapshot (all PATCH). No bundled dependency -- still JDK only.
        HttpClient client = HttpClient.newBuilder()
            .sslContext(trustAllContext())
            .connectTimeout(Duration.ofMillis(timeoutMs))
            .build()
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url))
            .timeout(Duration.ofMillis(timeoutMs))
            .header('Content-Type', 'application/json')
        headers?.each { k, v -> b.header(k as String, v as String) }
        HttpRequest.BodyPublisher pub = (body != null) ?
            HttpRequest.BodyPublishers.ofString(JsonOutput.toJson(body)) :
            HttpRequest.BodyPublishers.noBody()
        b.method(method, pub)
        HttpResponse<String> resp = client.send(b.build(), HttpResponse.BodyHandlers.ofString())
        int code = resp.statusCode()
        String tok = resp.headers().firstValue('x-auth-token').orElse(null)
        if (tok) authToken = tok
        String raw = resp.body() ?: ''
        if (code < 200 || code >= 300) {
            throw new IOException("FlashArray ${method} ${url} -> HTTP ${code}: ${raw?.take(500)}")
        }
        return raw ? (Map) new JsonSlurper().parseText(raw) : [:]
    }

    private String version() {
        if (apiVersion) return apiVersion
        apiVersion = '2.0'
        try {
            Map data = http('GET', "${endpoint}/api/api_version", null, null)
            List vs = (data?.version ?: []) as List
            if (vs) apiVersion = vs[-1] as String
        } catch (ignored) { }
        return apiVersion
    }

    private void login() {
        if (authToken) return
        http('POST', "${endpoint}/api/${version()}/login", null, ['api-token': token])
        if (!authToken) throw new IOException('purefa: login returned no x-auth-token')
    }

    Map request(String method, String path, Object body = null) {
        login()
        return http(method, "${endpoint}/api/${version()}/${path}", body, ['x-auth-token': authToken])
    }

    // ----------------------------------------------------------- operations --
    Map arraySpace() {
        List items = (request('GET', 'arrays/space')?.items ?: []) as List
        return (items ? items[0] : [:]) as Map
    }

    Map createVolume(String name, long sizeBytes) {
        return request('POST', "volumes?names=${enc(name)}", [provisioned: sizeBytes])
    }

    Map volume(String name) {
        List items = (request('GET', "volumes?names=${enc(name)}")?.items ?: []) as List
        return items ? (Map) items[0] : null
    }

    void resizeVolume(String name, long sizeBytes) {
        request('PATCH', "volumes?names=${enc(name)}", [provisioned: sizeBytes])
    }

    void connectVolume(String name, String hostGroup) {
        request('POST', "connections?host_group_names=${enc(hostGroup)}&volume_names=${enc(name)}")
    }

    /** Member host names of a host group ([] if the group is absent/empty). */
    List hostGroupMembers(String hostGroup) {
        if (!hostGroup) return []
        List items
        try {
            items = (request('GET', "host-groups/hosts?group_names=${enc(hostGroup)}")?.items ?: []) as List
        } catch (ignored) {
            return []   // group does not exist yet
        }
        List names = []
        for (it in items) {
            def member = it?.member
            String n = (member?.name ?: it?.name) as String
            if (n) names << n
        }
        return names
    }

    void disconnectVolume(String name, String hostGroup) {
        if (!hostGroup) return
        try {
            request('DELETE', "connections?host_group_names=${enc(hostGroup)}&volume_names=${enc(name)}")
        } catch (ignored) {
            // already disconnected / never connected -- not fatal
        }
    }

    /** Disconnect (idempotent), soft-delete, optionally eradicate. */
    void destroyVolume(String name, String hostGroup = null, boolean eradicate = false) {
        disconnectVolume(name, hostGroup)
        Map vol = null
        try { vol = volume(name) } catch (ignored) { }
        if (vol != null && !vol.destroyed) {
            request('PATCH', "volumes?names=${enc(name)}", [destroyed: true])
        }
        if (eradicate) {
            try { request('DELETE', "volumes?names=${enc(name)}") } catch (ignored) { }
        }
    }

    Map copyVolume(String source, String dest, boolean overwrite = false) {
        // overwrite=true restores/copies onto an EXISTING volume (used by VM snapshot
        // revert: copy <vol>.<suffix> back onto <vol>). Without it FA 400s if dest exists.
        String q = "volumes?names=${enc(dest)}" + (overwrite ? "&overwrite=true" : "")
        return request('POST', q, [source: [name: source]])
    }

    Map snapshotVolume(String name, String suffix) {
        return request('POST', "volume-snapshots?source_names=${enc(name)}&suffix=${enc(suffix)}")
    }

    Map getSnapshot(String name) {
        List items = (request('GET', "volume-snapshots?names=${enc(name)}")?.items ?: []) as List
        return items ? (Map) items[0] : null
    }

    /** All (live) snapshots whose source is the given volume. */
    List listVolumeSnapshots(String volume) {
        try { return (request('GET', "volume-snapshots?source_names=${enc(volume)}")?.items ?: []) as List }
        catch (Exception e) { return [] }
    }

    void destroySnapshot(String name, boolean eradicate = false) {
        Map snap = null
        try { snap = getSnapshot(name) } catch (ignored) { }
        if (snap != null && !snap.destroyed) {
            request('PATCH', "volume-snapshots?names=${enc(name)}", [destroyed: true])
        }
        if (eradicate) {
            try { request('DELETE', "volume-snapshots?names=${enc(name)}") } catch (ignored) { }
        }
    }

    // ------------------------------------------------------ volume groups -----
    // Per-VM FlashArray volume group support. A VM's disks are created as MEMBER
    // volumes named "<vgroup>/<vol>" so a group-consistent snapshot
    // (volume-group-snapshots) captures every disk crash-consistently. All
    // name-based ops below accept the full "<vgroup>/<vol>" name unchanged --
    // enc() URL-encodes the '/' to %2F (URLEncoder behavior), which FA REST v2
    // requires for member-volume names in a query string.
    //
    // ADDITIVE SAFETY: standalone volumes (names with NO '/') keep using the
    // existing createVolume/volume/resizeVolume/... methods verbatim. These
    // vgroup helpers are only invoked for newly-created grouped volumes.

    /** Split a member volume name "<vg>/<vol>" into [vgroup, vol]; null vgroup
     *  for a standalone (slashless) name. */
    static List<String> splitMember(String name) {
        String n = name ?: ''
        int i = n.indexOf('/')
        if (i < 0) return [null, n]
        return [n.substring(0, i), n.substring(i + 1)]
    }

    /** True when the name is a volume-group member ("<vg>/<vol>"). */
    static boolean isMember(String name) {
        return (name ?: '').indexOf('/') >= 0
    }

    /**
     * Create a volume group (idempotent). FA returns HTTP 400-409 when the group
     * already exists -- treat that as success so re-provisioning onto the same VM
     * vgroup is safe. Any other failure propagates.
     */
    Map createVolumeGroup(String vgroup) {
        if (!vgroup) return [:]
        Map r = [:]
        try {
            r = request('POST', "volume-groups?names=${enc(vgroup)}")
        } catch (IOException e) {
            String m = e.message ?: ''
            // already-exists / conflict -> success (FA 400 "already exists" or 409).
            if (!(m.contains('HTTP 400') || m.contains('HTTP 409') ||
                  m.toLowerCase().contains('already exist'))) {
                throw e
            }
        }
        // RECOVER a soft-destroyed (pending-eradication) vgroup: after a VM delete
        // the vgroup lingers ~24h as destroyed and creating a member in it fails
        // ("Volume group has been destroyed"). PATCH destroyed=false restores it so
        // the name is reusable immediately. Best-effort (benign if already live).
        try { request('PATCH', "volume-groups?names=${enc(vgroup)}", [destroyed: false]) }
        catch (Exception ignored) { }
        return r
    }

    /**
     * Create a member volume "<vg>/<vol>" inside its volume group, creating the
     * group first (idempotent). The full member name is the volume's NAME for all
     * subsequent name-based ops.
     */
    Map createMemberVolume(String memberName, long sizeBytes) {
        String vg = splitMember(memberName)[0]
        if (vg) createVolumeGroup(vg)
        return createVolume(memberName, sizeBytes)   // POST volumes?names=<vg>%2F<vol>
    }

    // ------------------------------------------------ protection groups -------
    // FlashArray crash-consistency for a VM's disks is achieved with a PROTECTION
    // GROUP (pgroup), NOT a volume group: a vgroup is only a namespace, and FA has
    // no usable volume-group snapshot endpoint (POST volume-group-snapshots 404s).
    // A pgroup CANNOT contain a volume group -- it takes member VOLUMES. We name a
    // VM's pgroup "<vgroup>-pg", add the vgroup's member volumes to it, then
    // snapshot the pgroup so every disk is captured at one crash-consistent point.
    //
    // pgroup-snapshot naming (Purity-verified): POST protection-group-snapshots
    // with source_names=<pg>&suffix=<sfx> creates "<pg>.<sfx>"; each MEMBER volume
    // snapshot is named EXACTLY "<pg>.<sfx>.<vg>/<vol>". A member snapshot cannot be
    // destroyed alone -- the whole pgroup snapshot "<pg>.<sfx>" is destroyed at once.

    /**
     * Create a protection group (idempotent). FA returns HTTP 400/409 "already
     * exists" when the pgroup is present -- treat that as success. Any other
     * failure propagates.
     *   POST protection-groups?names=<pg>
     */
    Map createProtectionGroup(String pgroup) {
        if (!pgroup) return [:]
        Map r = [:]
        try {
            r = request('POST', "protection-groups?names=${enc(pgroup)}")
        } catch (IOException e) {
            String m = e.message ?: ''
            if (!(m.contains('HTTP 400') || m.contains('HTTP 409') ||
                  m.toLowerCase().contains('already exist'))) {
                throw e
            }
        }
        // Recover a soft-destroyed pgroup (pending eradication) so a new snapshot
        // can reuse the name. Best-effort/benign if already live.
        try { request('PATCH', "protection-groups?names=${enc(pgroup)}", [destroyed: false]) }
        catch (Exception ignored) { }
        return r
    }

    /**
     * Add member VOLUMES to a protection group (idempotent). Members are full
     * volume names "<vg>/<vol>"; already-a-member is benign (FA 400/409).
     *   POST protection-groups/volumes?group_names=<pg>&member_names=<vg>/<vol>[,...]
     */
    Map addVolumesToProtectionGroup(String pgroup, List<String> members) {
        if (!pgroup || !members) return [:]
        String csv = members.findAll { it }.collect { enc(it as String) }.join(',')
        if (!csv) return [:]
        try {
            return request('POST',
                "protection-groups/volumes?group_names=${enc(pgroup)}&member_names=${csv}")
        } catch (IOException e) {
            String m = e.message ?: ''
            if (m.contains('HTTP 400') || m.contains('HTTP 409') ||
                m.toLowerCase().contains('already exist') ||
                m.toLowerCase().contains('already a member')) {
                return [:]
            }
            throw e
        }
    }

    /**
     * Snapshot a protection group:
     *   POST protection-group-snapshots?source_names=<pg>&suffix=<sfx>
     * creates the pgroup snapshot "<pg>.<sfx>"; each member volume snapshot is
     * named EXACTLY "<pg>.<sfx>.<vg>/<vol>". Returns the created resource.
     */
    Map snapshotProtectionGroup(String pgroup, String suffix) {
        return request('POST',
            "protection-group-snapshots?source_names=${enc(pgroup)}&suffix=${enc(suffix)}")
    }

    /**
     * Destroy an ENTIRE pgroup snapshot "<pg>.<sfx>" (member snapshots cannot be
     * destroyed individually -- Purity refuses). PATCH destroyed:true then optional
     * DELETE (eradicate). Idempotent/defensive: a missing snapshot is not fatal.
     *   PATCH  protection-group-snapshots?names=<pg>.<sfx>  {destroyed:true}
     *   DELETE protection-group-snapshots?names=<pg>.<sfx>            (eradicate)
     */
    void destroyProtectionGroupSnapshot(String pgSnapName, boolean eradicate = false) {
        if (!pgSnapName) return
        try {
            request('PATCH', "protection-group-snapshots?names=${enc(pgSnapName)}", [destroyed: true])
        } catch (Exception ignored) {
            // not found / already destroyed -- not fatal
        }
        if (eradicate) {
            try { request('DELETE', "protection-group-snapshots?names=${enc(pgSnapName)}") }
            catch (Exception ignored) { }
        }
    }

    /**
     * Destroy a protection group "<vgroup>-pg" (best-effort/idempotent): PATCH
     * destroyed:true then optional DELETE (eradicate). A missing pgroup is not an
     * error. Called when the backing vgroup is torn down.
     */
    void destroyProtectionGroup(String pgroup, boolean eradicate = false) {
        if (!pgroup) return
        try {
            request('PATCH', "protection-groups?names=${enc(pgroup)}", [destroyed: true])
        } catch (Exception ignored) {
            // not found / already destroyed -- not fatal
        }
        if (eradicate) {
            try { request('DELETE', "protection-groups?names=${enc(pgroup)}") }
            catch (Exception ignored) { }
        }
    }

    /**
     * Destroy a (now-empty) volume group: PATCH destroyed:true then optional
     * DELETE (eradicate). Defensive/idempotent -- a missing group is not an error
     * (members are destroyed first by the caller). Only call once the group's
     * member volumes have been destroyed.
     */
    void destroyVolumeGroup(String vgroup, boolean eradicate = false) {
        if (!vgroup) return
        try {
            request('PATCH', "volume-groups?names=${enc(vgroup)}", [destroyed: true])
        } catch (Exception ignored) {
            // not found / already destroyed -- not fatal
        }
        if (eradicate) {
            try { request('DELETE', "volume-groups?names=${enc(vgroup)}") }
            catch (Exception ignored) { }
        }
    }

    /** Member volume names currently in a volume group ([] if absent/empty). */
    List volumeGroupMembers(String vgroup) {
        if (!vgroup) return []
        List items
        try {
            items = (request('GET', "volumes?filter=${enc("volume_group.name='" + vgroup + "'")}")?.items ?: []) as List
        } catch (Exception ignored) {
            return []
        }
        List names = []
        for (it in items) {
            String n = (it?.name) as String
            // skip an already-destroyed member so the empty-group check is accurate
            if (n && !it?.destroyed) names << n
        }
        return names
    }

    private static String enc(String v) {
        return URLEncoder.encode(v ?: '', 'UTF-8')
    }
}
