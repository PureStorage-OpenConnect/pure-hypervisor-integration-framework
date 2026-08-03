package com.morpheusdata.pure

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import com.morpheusdata.core.providers.DatastoreTypeProvider
import com.morpheusdata.model.StorageServer
import com.morpheusdata.model.ComputeServer
import com.morpheusdata.model.ComputeServerGroup
import com.morpheusdata.model.Datastore
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.Snapshot
import com.morpheusdata.model.StorageVolume
import com.morpheusdata.model.StorageVolumeType
import com.morpheusdata.model.VirtualImage
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

/**
 * Per-VM-disk FlashArray provisioning for HPE VM Essentials (VME).
 *
 * Each VM disk is its OWN FlashArray volume (named {@code phif-<uuid>}), connected
 * to the FA host group, and attached to the KVM VM as a raw multipathed block
 * device. Snapshot / clone / resize are offloaded to the array. The
 * {@link DatastoreTypeProvider.MvmProvisionFacet} hooks make VME orchestrate the
 * host-side presentation + the libvirt disk definition natively (no SSH/virsh
 * back door): {@code prepareHostForVolume} rescans multipath so the LUN appears,
 * {@code buildDiskConfig} returns the libvirt disk config, and
 * {@code releaseVolumeFromHost} flushes the stale map on detach.
 *
 * Host-side commands run via {@code MorpheusContext.executeCommandOnServer}, which
 * returns a LAZY RxJava {@code Single} -- {@link #runOnHost} subscribes with
 * {@code blockingGet()} so the command actually runs AND completes before we return
 * (without it the rescan/cleanup silently no-ops). Because FA volumes attach to the
 * host GROUP and a VM may run on / migrate to any host, attach/resize/remove ops run
 * on ALL hypervisor hosts in the cluster (see {@link #clusterHosts}).
 */
@Slf4j
class PureDatastoreProvider implements DatastoreTypeProvider,
        DatastoreTypeProvider.MvmProvisionFacet, DatastoreTypeProvider.SnapshotFacet,
        DatastoreTypeProvider.SnapshotFacet.SnapshotServerFacet,
        DatastoreTypeProvider.SnapshotFacet.SnapshotInstanceFacet {

    static final String PROVIDER_CODE = 'pure-flasharray-vme.datastore'
    // Provisioner this datastore type binds to. MUST be 'mvm' for HPE VME: that is
    // the MvmProvisionFacet's provision code, and it's what makes the datastore-add
    // "Cloud" dropdown populate (empirically confirmed working). NB: 'mvm' is NOT
    // listed by /api/provision-types (which shows the generic 'kvm') -- it's an
    // internal VME/MVM code. Setting this to 'kvm' left the Cloud field EMPTY.
    static final String MVM_PROVISION_TYPE_CODE = 'mvm'
    static final String VOL_PREFIX = 'phif-'
    // FlashArray rejects volumes smaller than 1 MiB ("Volume size must be between
    // 1 MB and 4 PB"). VME legitimately requests tiny disks -- notably the
    // cloud-init user-data ISO (a few hundred KB) -- so floor every FA volume at
    // 1 MiB (which is 512-byte aligned, as FA requires).
    static final long FA_MIN_VOLUME_BYTES = 1048576L

    protected MorpheusContext morpheusContext
    protected Plugin plugin

    PureDatastoreProvider(Plugin plugin, MorpheusContext morpheusContext) {
        this.plugin = plugin
        this.morpheusContext = morpheusContext
    }

    @Override String getCode() { return PROVIDER_CODE }
    @Override String getName() { return 'Everpure FlashArray Datastore' }
    @Override MorpheusContext getMorpheus() { return this.morpheusContext }
    @Override Plugin getPlugin() { return this.plugin }

    @Override String getProvisionTypeCode() { return MVM_PROVISION_TYPE_CODE }
    @Override String getStorageProviderCode() { return PureStorageProvider.PROVIDER_CODE }

    @Override
    List<OptionType> getOptionTypes() {
        // Storage-server selector shown on the datastore-create form: lets the
        // operator choose WHICH FlashArray a datastore provisions on when more than
        // one Everpure storage server is registered. Backed by the pureStorageServers
        // option source (PureOptionSourceProvider). createDatastore reads the chosen
        // id from config.storageServerId and links the datastore to it. (When only
        // one is registered the plugin auto-links, so this just confirms it.)
        return [
            new OptionType(
                name: 'FlashArray Storage Server',
                code: "${PROVIDER_CODE}.storageServerId",
                fieldName: 'storageServerId', fieldContext: 'config',
                fieldLabel: 'FlashArray Storage Server',
                inputType: OptionType.InputType.SELECT,
                optionSource: 'pureStorageServers',
                required: true, displayOrder: 1,
                helpText: 'The Everpure FlashArray storage server this datastore provisions volumes on.'),
        ]
    }
    @Override boolean getCreatable() { return true }
    @Override boolean getEditable() { return true }
    @Override boolean getRemovable() { return true }
    // Volumes live on the array and must be refreshed by this plugin, not core.
    @Override boolean getPluginManagedVolumeRefresh() { return true }
    // CRITICAL for image-based provisioning: when FALSE (the API default), VME does
    // NOT treat this datastore as a place it can deploy an OS image, so it writes the
    // bootable qcow2 into a generic directory pool and attaches our (empty) raw LUN
    // -> "disk has no image data / not bootable". TRUE tells VME the datastore can be
    // an image target; VME then drives the image onto the plugin's volume via
    // cloneVolume(volume, server, VirtualImage, CloudFileInterface), which we
    // implement to qemu-img convert the image onto the raw /dev/mapper/<wwid> device.
    @Override boolean getImageTargetCapable() { return true }

    @Override
    List<StorageVolumeType> getVolumeTypes() {
        return [
            new StorageVolumeType(
                code: 'pure-flasharray-vme.volume', name: 'Everpure FlashArray Volume',
                displayName: 'Everpure FlashArray Volume', volumeType: 'disk',
                volumeCategory: 'disk', customLabel: true, customSize: true,
                resizable: true, planResizable: true, deletable: true, editable: true,
                hasDatastore: true),
        ]
    }

    // -------------------------------------------------- volume lifecycle ------
    @Override
    ServiceResponse<StorageVolume> createVolume(StorageVolume volume, ComputeServer server) {
        try {
            def (client, ss) = clientFor(server, volume)
            // NEW VMs: name the disk as a per-VM vgroup member "<vg>/<base>" so all
            // of the VM's disks snapshot crash-consistently. Existing volumes keep
            // their stored (possibly standalone) name -- faMemberName defers to it.
            String name = faMemberName(volume, server)
            long size = (volume.maxStorage ?: 0L) as long
            if (size <= 0) return ServiceResponse.error('Volume size (maxStorage) is required')
            size = Math.max(size, FA_MIN_VOLUME_BYTES)   // FA 1 MiB floor (tiny disks/ISOs)
            String hg = PureConfig.hostGroup(ss)
            // Validate the configured host group BEFORE creating the volume so a
            // stale/missing group fails clearly (no orphan volume, no cryptic
            // "Host group does not exist" 400 on connect).
            if (hg && client.hostGroupMembers(hg).isEmpty()) {
                return ServiceResponse.error(
                    "FlashArray host group '${hg}' does not exist or has no member hosts. " +
                    "Set the storage server's host group to the group that contains the " +
                    "KVM hosts, or register the hosts on the array, then retry.")
            }
            // createMemberVolume creates the vgroup first (idempotent) for a
            // "<vg>/<vol>" name; for a standalone name it's a plain createVolume.
            client.createMemberVolume(name, size)
            if (hg) client.connectVolume(name, hg)
            // Resolve the array-assigned serial -> WWID so VME can find the device.
            Map vol = client.volume(name)
            String serial = (vol?.serial ?: '') as String
            String protocol = PureConfig.protocol(ss)
            // FA volume name lives in volumeName/uniqueId; externalId carries the
            // host device path (VME's libvirt <source dev>). See faVolumeName.
            volume.uniqueId = name
            volume.volumeName = name
            if (serial) {
                volume.wwn = PureFlashArrayClient.scsiWwid(serial)
                String dev = PureFlashArrayClient.devicePath(serial, protocol)
                volume.deviceName = dev
                volume.externalId = dev
            } else {
                volume.externalId = name   // no serial yet; resolved later
            }
            volume.status = 'provisioned'
            return ServiceResponse.success(volume)
        } catch (Exception e) {
            log.error("Everpure createVolume failed: ${e.message}", e)
            return ServiceResponse.error("createVolume failed: ${e.message}")
        }
    }

    @Override
    ServiceResponse removeVolume(StorageVolume volume, ComputeServer server, boolean removeSnapshots, boolean force) {
        try {
            def (client, ss) = clientFor(server, volume)
            String fa = faVolumeName(volume)
            boolean erad = PureConfig.eradicate(ss)
            log.info("Everpure removeVolume: ${fa} removeSnapshots=${removeSnapshots} force=${force}")
            // A FlashArray volume can't be eradicated while it still has snapshots, and
            // orphaned snapshots otherwise leak. When asked (VM/disk delete), destroy the
            // volume's snapshots first.
            if (removeSnapshots) {
                for (def snap : client.listVolumeSnapshots(fa)) {
                    try { client.destroySnapshot((snap.name as String), erad) }
                    catch (Exception e) { log.warn("Everpure removeVolume: snapshot ${snap?.name} cleanup failed: ${e.message}") }
                }
            }
            client.destroyVolume(fa, PureConfig.hostGroup(ss), erad)
            // If this disk was a per-VM vgroup member, tear the vgroup down once its
            // last member is gone (the VM's other disks were/will be removed the same
            // way). Defensive/idempotent: a non-empty or missing group is left alone.
            // Standalone (slashless) volumes have no vgroup -- unchanged behavior.
            String vg = vgroupOf(fa)
            if (vg) {
                List remaining = []
                try { remaining = client.volumeGroupMembers(vg) } catch (Exception ignored) { }
                if (remaining.isEmpty()) {
                    // Tear down the VM's protection group "<vg>-pg" first (best-effort/
                    // idempotent; a missing pgroup is fine), then the now-empty vgroup.
                    try { client.destroyProtectionGroup("${vg}-pg".toString(), erad) }
                    catch (Exception e) { log.warn("Everpure removeVolume: pgroup ${vg}-pg teardown failed: ${e.message}") }
                    try { client.destroyVolumeGroup(vg, erad) }
                    catch (Exception e) { log.warn("Everpure removeVolume: vgroup ${vg} teardown failed: ${e.message}") }
                }
            }
            return ServiceResponse.success()
        } catch (Exception e) {
            log.error("Everpure removeVolume failed: ${e.message}", e)
            return force ? ServiceResponse.success() : ServiceResponse.error("removeVolume failed: ${e.message}")
        }
    }

    @Override
    ServiceResponse<StorageVolume> resizeVolume(StorageVolume volume, ComputeServer server, Long newSize) {
        try {
            def (client, ss) = clientFor(server, volume)
            client.resizeVolume(faVolumeName(volume), Math.max((newSize ?: 0L) as long, FA_MIN_VOLUME_BYTES))
            volume.maxStorage = newSize
            // The shared LUN grew; every cluster host that has the multipath map needs
            // to pick up the new size (the VM may run on / migrate to any of them).
            runOnHosts(clusterHosts(null, server),
                "multipathd reconfigure 2>/dev/null || multipath -r 2>/dev/null || true")
            return ServiceResponse.success(volume)
        } catch (Exception e) {
            log.error("Everpure resizeVolume failed: ${e.message}", e)
            return ServiceResponse.error("resizeVolume failed: ${e.message}")
        }
    }

    @Override
    ServiceResponse<StorageVolume> cloneVolume(StorageVolume volume, ComputeServer server, StorageVolume sourceVolume) {
        try {
            def (client, ss) = clientFor(server, volume)
            // NEW VM disk -> per-VM vgroup member name; existing volume keeps its
            // stored name (faMemberName defers to it).
            String dest = faMemberName(volume, server)
            String src = faVolumeName(sourceVolume)
            String hg = PureConfig.hostGroup(ss)
            // Validate the configured host group BEFORE creating/cloning so a
            // stale/missing group fails clearly (no orphan volume).
            if (hg && client.hostGroupMembers(hg).isEmpty()) {
                return ServiceResponse.error(
                    "FlashArray host group '${hg}' does not exist or has no member hosts. " +
                    "Set the storage server's host group to the group that contains the " +
                    "KVM hosts, or register the hosts on the array, then retry.")
            }
            // VALIDATE the source object exists before array-cloning it. When a VM
            // is created from an OS image/template, the "source" disk lives in VME's
            // image store (NOT on the FlashArray), so an array clone of it returns
            // HTTP 400 "Volume does not exist". In that case provision a FRESH empty
            // volume sized for the target disk so VME streams the image onto the raw
            // device (per-disk model: each disk is its own FA volume).
            boolean srcExists = false
            try { srcExists = (client.volume(src) != null) } catch (ignored) { srcExists = false }
            boolean fromImage = false
            // Ensure the destination's per-VM vgroup exists before any volume is
            // created in it (idempotent; no-op for a standalone dest name).
            String destVg = vgroupOf(dest)
            if (destVg) client.createVolumeGroup(destVg)
            if (srcExists) {
                client.copyVolume(src, dest)          // array-native clone (FA source)
            } else {
                // The "source" is a VME-cached disk source, NOT an FA volume: the OS
                // image (when creating from a template/image) or the cloud-init ISO.
                // VME (because getImageTargetCapable()=true) delegates the populate to
                // us: provision a fresh volume, then qemu-img convert the cached source
                // onto the raw device below. (Previously we left it EMPTY -> the disk
                // had no image data and wasn't bootable.)
                long size = (volume.maxStorage ?: sourceVolume?.maxStorage ?: 0L) as long
                if (size <= 0L) {
                    return ServiceResponse.error(
                        "cloneVolume: source volume '${src}' does not exist on the " +
                        "FlashArray and no target size (maxStorage) is set — cannot " +
                        "provision the disk.")
                }
                size = Math.max(size, FA_MIN_VOLUME_BYTES)   // FA 1 MiB floor (e.g. cloud-init ISO)
                boolean exists = false
                try { exists = (client.volume(dest) != null) } catch (ignored) { }
                if (!exists) client.createVolume(dest, size)
                fromImage = true
            }
            if (hg) client.connectVolume(dest, hg)
            Map vol = client.volume(dest)
            String serial = (vol?.serial ?: '') as String
            String protocol = PureConfig.protocol(ss)
            // FA name in volumeName/uniqueId; externalId = host device path (VME's
            // libvirt <source dev>). See faVolumeName / createVolume.
            volume.uniqueId = dest
            volume.volumeName = dest
            String devPath = null
            if (serial) {
                volume.wwn = PureFlashArrayClient.scsiWwid(serial)
                devPath = PureFlashArrayClient.devicePath(serial, protocol)
                volume.deviceName = devPath
                volume.externalId = devPath
            } else {
                volume.externalId = dest
            }
            // Populate the raw device from the source so the disk is bootable. The
            // source is NOT on the Everpure datastore -- it's a system-downloaded image or
            // a disk on ANOTHER datastore -- so resolve its real on-host path from the
            // sourceVolume (VME sets volumePath/deviceName to the source file) and fall
            // back to the well-known VME caches.
            if (fromImage && devPath) {
                log.info("Everpure cloneVolume image source: name='${src}' " +
                         "volumePath=${safeGet(sourceVolume,'getVolumePath')} " +
                         "deviceName=${safeGet(sourceVolume,'getDeviceName')} " +
                         "externalId=${safeGet(sourceVolume,'getExternalId')} " +
                         "sourceImage=${safeGet(sourceVolume,'getSourceImage')} " +
                         "datastore=${safeGet(sourceVolume,'getDatastore')}")
                List<ComputeServer> hosts = clusterHosts(null, server)
                ensureDeviceOnHosts(hosts, protocol, devPath)
                // Collect source-location hints from the sourceVolume. Keep absolute
                // file paths (system image store) AND relative ones like
                // 'testvm/<disk>' (a disk on another datastore -- resolved under the
                // datastore mounts in convertSourceToDevice). Skip guest device nodes
                // (/dev/...), which are attach targets, not source files.
                List<String> candidates = []
                ['getVolumePath','getExternalId','getSourceImage','getDeviceName'].each { m ->
                    def v = safeGet(sourceVolume, m)
                    if (v) {
                        String sv = (v as String).trim()
                        if (sv && !sv.startsWith('/dev/')) candidates << sv
                    }
                }
                ServiceResponse conv = convertSourceToDevice(hosts, candidates, src, devPath)
                if (!conv.success) return conv
            }
            return ServiceResponse.success(volume)
        } catch (Exception e) {
            log.error("Everpure cloneVolume failed: ${e.message}", e)
            return ServiceResponse.error("cloneVolume failed: ${e.message}")
        }
    }

    @Override
    ServiceResponse<StorageVolume> cloneVolume(StorageVolume volume, ComputeServer server, VirtualImage virtualImage,
                                               com.bertramlabs.plugins.karman.CloudFileInterface cloudFile) {
        // Image-based provisioning onto a RAW BLOCK per-disk volume. VME only calls
        // this (instead of writing a qcow2 into a directory pool) because we set
        // getImageTargetCapable()=true. We: provision the FA LUN, assemble its device
        // on the host, then `qemu-img convert -O raw` the cached OS image directly
        // onto /dev/mapper/<wwid> so the disk is bootable.
        try {
            // DIAGNOSTIC: log exactly what VME hands us so we can locate the cached
            // image file on the host (path/accessors aren't documented for this API).
            log.info("Everpure image cloneVolume: dest='${faVolumeName(volume)}' size=${volume?.maxStorage} " +
                     "image[name=${virtualImage?.name}, internalId=${virtualImage?.internalId}, " +
                     "uniqueId=${virtualImage?.uniqueId}, remotePath=${virtualImage?.remotePath}, " +
                     "locations=${virtualImage?.locations}] cloudFile=${cloudFile}")
            def (client, ss) = clientFor(server, volume)
            // NEW VM disk -> per-VM vgroup member; existing keeps its stored name.
            String dest = faMemberName(volume, server)
            String hg = PureConfig.hostGroup(ss)
            if (hg && client.hostGroupMembers(hg).isEmpty())
                return ServiceResponse.error("FlashArray host group '${hg}' has no member hosts.")
            // 1) Provision the FA volume sized for the disk (>= image virtual size).
            long size = Math.max((volume.maxStorage ?: 0L) as long, FA_MIN_VOLUME_BYTES)
            String destVg = vgroupOf(dest)
            if (destVg) client.createVolumeGroup(destVg)   // idempotent; no-op if standalone
            boolean exists = false
            try { exists = (client.volume(dest) != null) } catch (ignored) { }
            if (!exists) client.createVolume(dest, size)
            if (hg) client.connectVolume(dest, hg)
            // 2) Resolve serial -> WWID -> device path; persist on the volume.
            Map vol = client.volume(dest)
            String serial = (vol?.serial ?: '') as String
            String protocol = PureConfig.protocol(ss)
            volume.uniqueId = dest; volume.volumeName = dest
            String devPath = null
            if (serial) {
                volume.wwn = PureFlashArrayClient.scsiWwid(serial)
                devPath = PureFlashArrayClient.devicePath(serial, protocol)
                volume.deviceName = devPath; volume.externalId = devPath
            } else {
                volume.externalId = dest
            }
            // 3) Assemble the device on the cluster hosts.
            List<ComputeServer> hosts = clusterHosts(null, server)
            ensureDeviceOnHosts(hosts, protocol, devPath)
            // 4) Convert the cached image onto the RAW device. VME caches the image on
            //    the KVM host(s) under /var/morpheus/kvm/images/<id>; try the known
            //    identifiers and convert the first that exists. The LUN is shared, so
            //    converting once (on the first host that has the image) populates it
            //    for all. qemu-img auto-detects the source format.
            if (devPath) {
                String iid = (virtualImage?.internalId ?: '') as String
                String uid = (virtualImage?.uniqueId ?: '') as String
                String rp  = (virtualImage?.remotePath ?: '') as String
                String D = '/var/morpheus/kvm/images'
                String convert =
                    "DEV='${devPath}'; SRC=''; " +
                    "for c in '${D}/${iid}' '${D}/${uid}' '${rp}' \$(ls -1d ${D}/${iid}* 2>/dev/null) \$(ls -1d ${D}/${uid}* 2>/dev/null); do " +
                    "[ -n \"\$c\" ] && [ -f \"\$c\" ] && { SRC=\"\$c\"; break; }; done; " +
                    "if [ -z \"\$SRC\" ]; then echo PURE_IMG_SRC_NOT_FOUND; ls -l ${D} 2>&1 | head; exit 3; fi; " +
                    "echo \"converting \$SRC -> \$DEV\"; qemu-img convert -O raw \"\$SRC\" \"\$DEV\" && echo PURE_IMG_CONVERT_OK"
                boolean converted = false
                for (ComputeServer hh : (hosts ?: [])) {
                    def r = runOnHost(hh, convert)
                    if (r != null && r.exitCode == 0) { converted = true; break }
                }
                if (!converted)
                    return ServiceResponse.error(
                        "image cloneVolume: could not convert image '${virtualImage?.name}' onto " +
                        "${devPath} (cached image not found on any host under /var/morpheus/kvm/images). " +
                        "Check the diagnostic log for the image identifiers.")
            }
            volume.status = 'provisioned'
            return ServiceResponse.success(volume)
        } catch (Exception e) {
            log.error("Everpure image cloneVolume failed: ${e.message}", e)
            return ServiceResponse.error("image cloneVolume failed: ${e.message}")
        }
    }

    // -------------------------------------------------- datastore lifecycle ---
    // Per-disk model: the "datastore" is the array itself, with no shared backing
    // store to create/destroy. Accept create/remove so VME can register the type.
    @Override
    ServiceResponse<Datastore> createDatastore(Datastore datastore) {
        try {
            // LINK the datastore to the Everpure storage server. The cluster-level add
            // doesn't offer a storage-server field, so auto-link when there's exactly
            // ONE registered (unambiguous). With several, require an explicit link
            // rather than guess the wrong array.
            StorageServer ss = datastore?.storageServer
            // 1) Honor the operator's selection from the datastore form (the
            //    'FlashArray Storage Server' SELECT -> config.storageServerId).
            if (ss == null) {
                def sid = datastoreConfigValue(datastore, 'storageServerId')
                if (sid) {
                    try { ss = (StorageServer) morpheusContext.services.storageServer.get(sid as Long) }
                    catch (Exception e) { log.warn("Everpure: storageServerId ${sid} lookup failed: ${e.message}") }
                    if (ss) datastore.storageServer = ss
                }
            }
            // 2) Else auto-link when exactly ONE is registered; refuse to guess when many.
            if (ss == null) {
                List servers = listPureStorageServers()
                if (servers.size() == 1) {
                    ss = (StorageServer) servers[0]
                    datastore.storageServer = ss
                } else if (servers.size() > 1) {
                    return ServiceResponse.error(
                        "Multiple Everpure storage servers are registered (${servers.size()}); " +
                        "choose one in the 'FlashArray Storage Server' field when adding the datastore.")
                }
            }
            // Validate the array is reachable + seed capacity so the UI/scheduler don't
            // show "unknown", and confirm host objects exist before provisioning.
            if (ss != null) {
                def client = PureConfig.client(ss, 15000)
                applyCapacity(datastore, client.arraySpace())
                String hg = PureConfig.hostGroup(ss)
                if (hg && client.hostGroupMembers(hg).isEmpty()) {
                    return ServiceResponse.error(
                        "FlashArray host group '${hg}' has no member hosts — register " +
                        "the VME KVM hosts on the array before creating this datastore.")
                }
            }
            datastore.online = true
            datastore.status = 'provisioned'
            return ServiceResponse.success(datastore)
        } catch (Exception e) {
            return ServiceResponse.error("createDatastore failed: ${e.message}")
        }
    }

    /** Read a value from the Datastore's config (accessor name varies by API version). */
    private Object datastoreConfigValue(Datastore datastore, String key) {
        if (datastore == null) return null
        try { return datastore.getConfigProperty(key) } catch (ignored) { }
        try { def c = datastore.getConfig(); if (c instanceof Map) return ((Map) c).get(key) } catch (ignored) { }
        try { def c = datastore.getConfigMap(); if (c instanceof Map) return ((Map) c).get(key) } catch (ignored) { }
        return null
    }

    /** Map FlashArray space (capacity / total_physical used) onto Datastore fields. */
    private void applyCapacity(Datastore datastore, Map space) {
        if (space == null) return
        Long capacity = (space?.capacity ?: 0L) as Long
        Long used = ((space?.space instanceof Map ? space.space.total_physical : 0L) ?: 0L) as Long
        if (capacity > 0L) {
            datastore.storageSize = capacity
            datastore.freeSpace = Math.max(0L, capacity - used)
        }
    }

    @Override
    ServiceResponse removeDatastore(Datastore datastore) {
        // Nothing array-side to tear down (volumes are removed individually).
        return ServiceResponse.success()
    }

    // -------------------------------------------------- MVM (VME) attach ------
    @Override
    ServiceResponse<StorageVolume> prepareHostForVolume(ComputeServerGroup cluster, ComputeServer server, StorageVolume volume) {
        try {
            def (client, ss) = clientFor(server, volume)
            String protocol = PureConfig.protocol(ss)
            // Resolve the expected host device path + WWID from the array serial FIRST
            // so we can wait for exactly that device node to appear.
            String devPath = volume.deviceName
            String wwid = volume.wwn
            if (!devPath || !devPath.startsWith('/dev/') || !wwid) {
                Map vol = client.volume(faVolumeName(volume))
                String serial = (vol?.serial ?: '') as String
                if (serial) {
                    wwid = PureFlashArrayClient.scsiWwid(serial)
                    devPath = PureFlashArrayClient.devicePath(serial, protocol)
                    volume.wwn = wwid
                    volume.deviceName = devPath
                    volume.externalId = devPath   // libvirt <source dev=.../>
                }
            }
            // The volume is connected to the host GROUP on the array, so make EVERY
            // hypervisor host in the cluster discover + assemble the new LUN and wait
            // for the multipath device node. VME may start (or later live-migrate) the
            // domain on any cluster host, and each needs /dev/mapper/<wwid> present or
            // start/migrate fails with "Cannot access storage file".
            ensureDeviceOnHosts(clusterHosts(cluster, server), protocol, devPath)
            return ServiceResponse.success(volume)
        } catch (Exception e) {
            log.error("Everpure prepareHostForVolume failed: ${e.message}", e)
            return ServiceResponse.error("prepareHostForVolume failed: ${e.message}")
        }
    }

    @Override
    ServiceResponse<MvmProvisionFacet.MvmDiskConfig> buildDiskConfig(ComputeServerGroup cluster, ComputeServer server, StorageVolume volume) {
        try {
            def (client, ss) = clientFor(server, volume)
            String protocol = PureConfig.protocol(ss)
            // VME builds the libvirt disk as:
            //   <disk type='block'><source dev='${volume.externalId}'/>
            //                       <target dev='${config.deviceName}' .../></disk>
            // So the HOST DEVICE PATH must be in volume.externalId (the <source dev>),
            // and config.deviceName is the guest TARGET name -- which must be slashless
            // (a /dev/... path there becomes an invalid target "mapper/<wwid>" and
            // libvirt rejects the domain with "Unknown disk name"). Resolve the
            // /dev/mapper path from the array serial and put it on externalId.
            String devPath = volume.deviceName
            if (!devPath || !devPath.startsWith('/dev/')) {
                Map vol = client.volume(faVolumeName(volume))
                String serial = (vol?.serial ?: '') as String
                if (serial) {
                    volume.wwn = PureFlashArrayClient.scsiWwid(serial)
                    devPath = PureFlashArrayClient.devicePath(serial, protocol)
                    volume.deviceName = devPath
                }
            }
            if (!devPath || !devPath.startsWith('/dev/')) {
                return ServiceResponse.error(
                    "buildDiskConfig: could not resolve the host block-device path for " +
                    "FlashArray volume '${faVolumeName(volume)}' (no serial). The volume " +
                    "must exist on the array before the disk can be attached.")
            }
            volume.externalId = devPath      // libvirt <source dev=.../>
            // Belt-and-suspenders: ensure the device is assembled on every cluster
            // host here too. buildDiskConfig provably runs right before VME builds the
            // domain XML + powers on, whereas prepareHostForVolume isn't always invoked
            // in the MVM flow. The per-host poll returns immediately where the device
            // already exists, so this is cheap on hosts prepareHostForVolume reached.
            ensureDeviceOnHosts(clusterHosts(cluster, server), protocol, devPath)
            MvmProvisionFacet.MvmDiskConfig config = new MvmProvisionFacet.MvmDiskConfig()
            config.diskMode = MvmProvisionFacet.MvmDiskConfig.DiskMode.VIRTIO
            config.deviceType = MvmProvisionFacet.MvmDiskConfig.DeviceType.DISK
            config.diskType = 'block'        // raw block device, not a file-backed disk
            // Guest target: a slashless name (VME assigns vdX ordering). Use the FA
            // volume name (slashless) -- it defines cleanly; never a /dev path.
            config.deviceName = volume.deviceDisplayName ?: faVolumeName(volume)
            return ServiceResponse.success(config)
        } catch (Exception e) {
            log.error("Everpure buildDiskConfig failed: ${e.message}", e)
            return ServiceResponse.error("buildDiskConfig failed: ${e.message}")
        }
    }

    @Override
    ServiceResponse<StorageVolume> releaseVolumeFromHost(ComputeServerGroup cluster, ComputeServer server, StorageVolume volume) {
        try {
            // Resolve the WWID even if volume.wwn isn't populated at delete time: the
            // host device path (/dev/mapper/<wwid>) is stored on externalId/deviceName.
            String wwid = volume.wwn
            if (!wwid) {
                String dev = (volume.externalId ?: volume.deviceName ?: '') as String
                if (dev.contains('/')) wwid = dev.substring(dev.lastIndexOf('/') + 1)
            }
            if (wwid) {
                // The FA volume is being disconnected/destroyed. Clean the host so it
                // doesn't keep a stale multipath map + faulted SCSI paths for a LUN
                // that no longer exists: (1) discover the underlying path devices via
                // the dm map's /sys slaves, (2) flush the multipath map, (3) delete
                // each /dev/sdX path, (4) rescan sessions so removed-LUN entries clear.
                // NB: runOnHost now BLOCKS (blockingGet), so this actually executes --
                // previously the lazy RxJava Single meant the flush never ran and the
                // host kept the stale device after the array volume was deleted.
                String cleanup = """\
                    WWID='${wwid}'; DM="/dev/mapper/\$WWID"; PATHS=""
                    if [ -e "\$DM" ]; then BASE=\$(basename \$(readlink -f "\$DM")); PATHS=\$(ls /sys/block/\$BASE/slaves/ 2>/dev/null); fi
                    multipath -f "\$WWID" 2>/dev/null || true
                    # Also catch raw SCSI paths still reporting this volume's id even when
                    # the multipath map was already flushed (map gone, paths lingering) --
                    # otherwise a reused LUN keeps the stale identity. Match the serial
                    # part of the WWID (strip the leading '3') against each sd*'s wwid.
                    SER="\${WWID#3}"
                    for b in \$(ls /sys/block 2>/dev/null | grep '^sd'); do
                        w=\$(cat /sys/block/\$b/device/wwid 2>/dev/null)
                        case "\$w" in *\$SER*) PATHS="\$PATHS \$b";; esac
                    done
                    for p in \$(echo \$PATHS | tr ' ' '\\n' | sort -u); do [ -e /sys/block/\$p/device/delete ] && echo 1 > /sys/block/\$p/device/delete 2>/dev/null || true; done
                    iscsiadm -m session --rescan >/dev/null 2>&1 || true
                    ls -l "\$DM" 2>&1 || echo "device removed"
                """.stripIndent()
                // The LUN was connected to the whole host group, so EVERY cluster host
                // may hold a stale map + faulted paths -- clean all of them, not just
                // the host that last ran the VM.
                runOnHosts(clusterHosts(cluster, server), cleanup)
            } else {
                log.warn("Everpure releaseVolumeFromHost: no WWID/device path on volume ${volume?.id}; skipping host cleanup")
            }
            return ServiceResponse.success(volume)
        } catch (Exception e) {
            log.error("Everpure releaseVolumeFromHost failed: ${e.message}", e)
            return ServiceResponse.error("releaseVolumeFromHost failed: ${e.message}")
        }
    }

    // -------------------------------------------------- snapshots (array) -----
    @Override
    ServiceResponse<Snapshot> createSnapshot(StorageVolume volume) {
        try {
            def (client, ss) = clientFor(null, volume)
            String suffix = 's' + UUID.randomUUID().toString().replace('-', '').substring(0, 10)
            Map resp = client.snapshotVolume(faVolumeName(volume), suffix)
            String snapName = ((resp?.items ?: [])[0]?.name) ?: "${faVolumeName(volume)}.${suffix}"
            Snapshot snap = new Snapshot(externalId: snapName, name: snapName)
            return ServiceResponse.success(snap)
        } catch (Exception e) {
            log.error("Everpure createSnapshot failed: ${e.message}", e)
            return ServiceResponse.error("createSnapshot failed: ${e.message}")
        }
    }

    @Override
    ServiceResponse removeSnapshot(StorageVolume volume) {
        // Without an explicit Snapshot handle here, destroy the most recent snapshot
        // for this volume's name. (VME calls the server-level facet with an explicit
        // snapshot for targeted deletes.)
        try {
            log.info("Everpure volume removeSnapshot: vol=${faVolumeName(volume)} sourceSnapshotId=${volume?.sourceSnapshotId}")
            def (client, ss) = clientFor(null, volume)
            String snapId = volume.sourceSnapshotId
            if (snapId) client.destroySnapshot(snapId, PureConfig.eradicate(ss))
            return ServiceResponse.success()
        } catch (Exception e) {
            log.error("Everpure removeSnapshot failed: ${e.message}", e)
            return ServiceResponse.error("removeSnapshot failed: ${e.message}")
        }
    }

    @Override
    ServiceResponse<Snapshot> listSnapshots(com.morpheusdata.model.StorageServer storageServer) {
        // Optional inventory hook; per-disk snapshots are enumerated lazily.
        return ServiceResponse.success()
    }

    // ----------------------------------- VM snapshots (Server + Instance facets)
    // A VM snapshot = an array snapshot of EVERY FlashArray volume backing the VM's
    // disks, tied together by a shared suffix on the returned Snapshot's externalId.
    //
    // CRITICAL: VME routes instance snapshot create/revert/DELETE through the
    // SnapshotInstanceFacet (Instance-level) -- NOT the SnapshotServerFacet. Without
    // SnapshotInstanceFacet, delete/revert are never delegated to the plugin (they
    // fall back to native KVM and the array snapshots leak). HPE's own Alletra MP
    // plugin implements BOTH facets; we mirror that. (SnapshotInstanceFacet was added
    // in plugin-api 1.3.0, hence the API bump.)

    // ---- SnapshotServerFacet (ComputeServer-level) ----
    @Override
    ServiceResponse<Snapshot> createSnapshot(ComputeServer server, Boolean flagA, Boolean flagB) {
        return doCreateVmSnapshot(faVolumesOf(server), "server:${server?.name}")
    }
    @Override
    ServiceResponse<Snapshot> revertSnapshot(ComputeServer server, Snapshot snapshot) {
        return doRevertVmSnapshot(faVolumesOf(server), snapshot, "server:${server?.name}")
    }
    @Override
    ServiceResponse removeSnapshot(ComputeServer server, Snapshot snapshot) {
        return doRemoveVmSnapshot(faVolumesOf(server), snapshot, "server:${server?.name}")
    }

    // ---- SnapshotInstanceFacet (Instance-level) -- the path VME's UI snapshot
    //      create/revert/delete actually uses for MVM ----
    @Override
    ServiceResponse<com.morpheusdata.model.Snapshot> createSnapshot(com.morpheusdata.model.Instance instance,
            com.morpheusdata.request.CreateSnapshotRequest request) {
        return doCreateVmSnapshot(faVolumesOfInstance(instance), "instance:${instance?.name}")
    }
    @Override
    ServiceResponse<com.morpheusdata.model.Snapshot> revertSnapshot(com.morpheusdata.model.Instance instance, Snapshot snapshot) {
        return doRevertVmSnapshot(faVolumesOfInstance(instance), snapshot, "instance:${instance?.name}")
    }
    @Override
    ServiceResponse removeSnapshot(com.morpheusdata.model.Instance instance, Snapshot snapshot) {
        return doRemoveVmSnapshot(faVolumesOfInstance(instance), snapshot, "instance:${instance?.name}")
    }

    // ---- shared snapshot implementation (array-offloaded, multi-disk) ----
    private ServiceResponse<Snapshot> doCreateVmSnapshot(List<StorageVolume> vols, String ctx) {
        try {
            if (!vols) return ServiceResponse.error('No FlashArray volumes found on this VM to snapshot')
            def (client, ss) = clientFor(null, vols[0])
            String suffix = 's' + UUID.randomUUID().toString().replace('-', '').substring(0, 10)
            log.info("Everpure createSnapshot [${ctx}]: suffix=${suffix} vols=${vols.collect { faVolumeName(it) }}")
            // externalId = the shared suffix so revert/remove rebuild each per-volume
            // snapshot name as <faVolume>.<suffix>.
            Snapshot snap = new Snapshot(externalId: suffix, name: suffix)
            List<com.morpheusdata.model.SnapshotFile> files = []

            // CRASH-CONSISTENT (common case): if EVERY disk is a member of ONE vgroup
            // <vg>, snapshot a PROTECTION GROUP "<vg>-pg" so all disks are captured at
            // one atomic point-in-time (a vgroup is only a namespace; FA has no usable
            // volume-group snapshot endpoint, and a pgroup cannot contain a vgroup --
            // it takes member VOLUMES). We create the pgroup, add the vgroup's CURRENT
            // members, then snapshot the pgroup. Each member snapshot is named EXACTLY
            // "<pg>.<sfx>.<vg>/<vol>" -- that is the SnapshotFile externalId revert/
            // remove key off. If disks are standalone/legacy/mixed (not all in one
            // vgroup), fall back to the per-volume snapshot path below.
            String sharedVg = soleVgroupOf(vols)
            if (sharedVg) {
                String pg = "${sharedVg}-pg".toString()
                client.createProtectionGroup(pg)            // idempotent
                List members = []
                try { members = client.volumeGroupMembers(sharedVg) } catch (Exception ignored) { }
                // Fall back to the VM's known member names if the live enumeration is empty.
                if (!members) members = vols.collect { faVolumeName(it) }.findAll { PureFlashArrayClient.isMember(it) }
                client.addVolumesToProtectionGroup(pg, members)   // idempotent; BEFORE snapshot
                client.snapshotProtectionGroup(pg, suffix)        // -> "<pg>.<sfx>"
                int idx = 0
                for (StorageVolume v : vols) {
                    String fa = faVolumeName(v)                     // "<vg>/<vol>"
                    String snapName = "${pg}.${suffix}.${fa}".toString()   // "<pg>.<sfx>.<vg>/<vol>"
                    files << buildSnapshotFile(snapName, v, snap, idx)
                    idx++
                }
                if (files.isEmpty()) return ServiceResponse.error('FlashArray protection-group snapshot produced no files')
                snap.setSnapshotFiles(files)
                return ServiceResponse.success(snap)
            }

            // FALLBACK (standalone/legacy/mixed): snapshot EACH disk with the SAME
            // suffix -> "<fa>.<suffix>". The SnapshotFile externalId MUST be that real
            // array snapshot name (revert/remove key off it). This is the historical
            // per-volume path, preserved verbatim for slashless/standalone volumes.
            int idx = 0
            for (StorageVolume v : vols) {
                String fa = faVolumeName(v)
                String snapName = memberSnapName(fa, suffix, false)   // "<fa>.<suffix>"
                try {
                    client.snapshotVolume(fa, suffix)
                    files << buildSnapshotFile(snapName, v, snap, idx)
                } catch (Exception e) {
                    log.warn("Everpure createSnapshot: ${snapName} failed: ${e.message}")
                }
                idx++
            }
            if (files.isEmpty()) return ServiceResponse.error('FlashArray snapshot failed for all VM volumes')
            snap.setSnapshotFiles(files)
            return ServiceResponse.success(snap)
        } catch (Exception e) {
            log.error("Everpure createSnapshot [${ctx}] failed: ${e.message}", e)
            return ServiceResponse.error("createSnapshot failed: ${e.message}")
        }
    }

    private ServiceResponse<Snapshot> doRevertVmSnapshot(List<StorageVolume> vols, Snapshot snapshot, String ctx) {
        try {
            // VME OVERWRITES the Snapshot.externalId we return, but it PERSISTS the
            // per-volume SnapshotFiles (each externalId = "<faVolume>.<suffix>"). So
            // drive revert from the snapshotFiles: the FA snapshot name -> overwrite
            // its source volume (the part before the last '.').
            List<String> snaps = faSnapNames(snapshot)
            log.info("Everpure revertSnapshot [${ctx}]: snapshotFiles=${snaps}")
            if (!snaps) return ServiceResponse.error('revertSnapshot: snapshot has no FlashArray snapshotFiles')
            def (client, ss) = clientFor(null, vols ? vols[0] : null)
            int reverted = 0
            for (String snapName : snaps) {
                String dest = snapSourceVolume(snapName)
                if (!dest) continue
                try { client.copyVolume(snapName, dest, true); reverted++ }
                catch (Exception e) { log.warn("Everpure revertSnapshot: ${dest} <- ${snapName} failed: ${e.message}") }
            }
            if (reverted == 0) return ServiceResponse.error("revertSnapshot: no volumes restored from ${snaps}")
            return ServiceResponse.success(snapshot)
        } catch (Exception e) {
            log.error("Everpure revertSnapshot [${ctx}] failed: ${e.message}", e)
            return ServiceResponse.error("revertSnapshot failed: ${e.message}")
        }
    }

    private ServiceResponse doRemoveVmSnapshot(List<StorageVolume> vols, Snapshot snapshot, String ctx) {
        try {
            List<String> snaps = faSnapNames(snapshot)
            log.info("Everpure removeSnapshot [${ctx}]: snapshotFiles=${snaps}")
            if (!snaps) return ServiceResponse.success()
            def (client, ss) = clientFor(null, vols ? vols[0] : null)
            boolean erad = PureConfig.eradicate(ss)
            // PGROUP-based members "<pg>.<sfx>.<vg>/<vol>" cannot be destroyed
            // individually -- Purity only destroys the WHOLE pgroup snapshot "<pg>.<sfx>".
            // Collapse all pgroup members to their distinct pgroup snapshot names and
            // destroy each once. Per-volume fallback names "<fa>.<suffix>" are destroyed
            // individually as before.
            Set<String> pgSnaps = new LinkedHashSet<>()
            List<String> perVolume = []
            for (String snapName : snaps) {
                String pgSnap = pgroupSnapOf(snapName)
                if (pgSnap) pgSnaps << pgSnap
                else perVolume << snapName
            }
            for (String pgSnap : pgSnaps) {
                try { client.destroyProtectionGroupSnapshot(pgSnap, erad) }
                catch (Exception e) { log.warn("Everpure removeSnapshot: pgroup snapshot ${pgSnap} failed: ${e.message}") }
            }
            for (String snapName : perVolume) {
                try { client.destroySnapshot(snapName, erad) }
                catch (Exception e) { log.warn("Everpure removeSnapshot: ${snapName} failed: ${e.message}") }
            }
            return ServiceResponse.success()
        } catch (Exception e) {
            log.error("Everpure removeSnapshot [${ctx}] failed: ${e.message}", e)
            return ServiceResponse.error("removeSnapshot failed: ${e.message}")
        }
    }

    /** The single vgroup shared by EVERY disk, or null if the disks are standalone,
     *  legacy, or spread across more than one vgroup (in which case the per-volume
     *  snapshot fallback applies). Crash-consistent pgroup snapshots require all of
     *  the VM's disks to live in exactly one vgroup. */
    private String soleVgroupOf(List<StorageVolume> vols) {
        if (!vols) return null
        String vg = null
        for (StorageVolume v : vols) {
            String g = vgroupOf(faVolumeName(v))
            if (!g) return null                 // a standalone disk -> not all grouped
            if (vg == null) vg = g
            else if (vg != g) return null        // disks span multiple vgroups
        }
        return vg
    }

    /** Build a STORAGE-BACKED SnapshotFile for one disk. The externalId is the real
     *  array snapshot name -- this is what tells VME the snapshot is array-offloaded
     *  (so revert/delete are delegated to our facet rather than handled natively by
     *  KVM, which would leak the array snapshots), and it is what revert/remove key
     *  off. Mirrors HPE's Alletra MP plugin. */
    private com.morpheusdata.model.SnapshotFile buildSnapshotFile(String snapName, StorageVolume v, Snapshot snap, int idx) {
        com.morpheusdata.model.SnapshotFile sf = new com.morpheusdata.model.SnapshotFile()
        sf.setName(snapName)
        sf.setExternalId(snapName)
        sf.setType('block')
        sf.setVolume(v)
        sf.setSnapshot(snap)
        String dev = (v.externalId ?: v.deviceName) as String
        if (dev) { sf.setPath(dev); sf.setExportPath(dev) }
        sf.setDiskIndex(idx)
        return sf
    }

    /** The real FlashArray member-snapshot name for a disk + suffix.
     *
     *  Per-volume snapshots of a name "<fa>" are "<fa>.<suffix>" (the only form this
     *  helper builds -- pgroup member names "<pg>.<sfx>.<vg>/<vol>" are constructed
     *  inline in doCreateVmSnapshot, since they need the pgroup name). For a
     *  standalone volume the per-volume form is the historical name. */
    private String memberSnapName(String fa, String suffix, boolean grouped) {
        return "${fa}.${suffix}".toString()
    }

    /** Recover the SOURCE volume name a member snapshot reverts/copies back onto.
     *
     *  Two snapshot-name forms are recognized:
     *    PGROUP member  "<pg>.<sfx>.<vg>/<vol>"  -> "<vg>/<vol>"
     *        ("<pg>" and "<sfx>" are dot-free FA tokens, so the source is everything
     *         after the SECOND '.'; the volume part "<vg>/<vol>" itself contains '/'.)
     *    PER-VOLUME     "<fa>.<suffix>"          -> "<fa>"
     *        ("<fa>" is standalone OR "<vg>/<vol>"; FA names are [A-Za-z0-9-/] with no
     *         '.', so the LAST '.' separates the suffix.)
     *  A pgroup member is distinguished by having a '/' with >=2 dots before it. */
    private String snapSourceVolume(String snapName) {
        if (!snapName) return null
        int slash = snapName.indexOf('/')
        if (slash >= 0) {
            String beforeSlash = snapName.substring(0, slash)
            int dots = beforeSlash.count('.')
            if (dots >= 2) {
                // PGROUP member "<pg>.<sfx>.<vg>/<vol>" -> strip the "<pg>.<sfx>." prefix.
                int firstDot = snapName.indexOf('.')
                int secondDot = snapName.indexOf('.', firstDot + 1)
                if (secondDot < 0) return null
                return snapName.substring(secondDot + 1)   // "<vg>/<vol>"
            }
            // Legacy vgroup form "<vg>.<suffix>/<vol>" -> strip ".<suffix>" from group part.
            String groupSnap = beforeSlash                 // "<vg>.<suffix>"
            String vol = snapName.substring(slash + 1)      // "<vol>"
            int dot = groupSnap.lastIndexOf('.')
            if (dot < 0) return null
            return "${groupSnap.substring(0, dot)}/${vol}".toString()   // "<vg>/<vol>"
        }
        // "<fa>.<suffix>" (fa standalone) -> drop ".<suffix>".
        int dot = snapName.lastIndexOf('.')
        if (dot < 0) return null
        return snapName.substring(0, dot)
    }

    /** Derive the PGROUP SNAPSHOT name "<pg>.<sfx>" from a pgroup member snapshot
     *  "<pg>.<sfx>.<vg>/<vol>" by stripping the trailing ".<vg>/<vol>". Returns null
     *  for a non-pgroup (per-volume) snapshot name. The whole pgroup snapshot is the
     *  unit of destruction -- members cannot be destroyed individually. */
    private String pgroupSnapOf(String snapName) {
        if (!snapName) return null
        int slash = snapName.indexOf('/')
        if (slash < 0) return null
        String beforeSlash = snapName.substring(0, slash)
        if (beforeSlash.count('.') < 2) return null   // not a pgroup member form
        // "<pg>.<sfx>.<vg>/<vol>": keep "<pg>.<sfx>" (the first two dot-free tokens).
        int firstDot = snapName.indexOf('.')
        int secondDot = snapName.indexOf('.', firstDot + 1)
        if (secondDot < 0) return null
        return snapName.substring(0, secondDot)   // "<pg>.<sfx>"
    }

    /** FlashArray snapshot names from a Snapshot's persisted SnapshotFiles -- the
     *  reliable handle, since VME overwrites Snapshot.externalId. Each is either
     *  "<faVolume>.<suffix>" (per-volume) or "<vg>.<suffix>/<vol>" (group member). */
    private List<String> faSnapNames(Snapshot snapshot) {
        List<String> names = []
        try {
            def files = safeGet(snapshot, 'getSnapshotFiles')
            if (files instanceof Collection) {
                for (def f : files) {
                    def n = safeGet(f, 'getExternalId') ?: safeGet(f, 'getName')
                    if (n) names << (n as String)
                }
            }
        } catch (Exception e) { log.warn("Everpure faSnapNames failed: ${e.message}") }
        return names
    }

    /** FA volumes for every ComputeServer backing an Instance's containers. */
    private List<StorageVolume> faVolumesOfInstance(com.morpheusdata.model.Instance instance) {
        List<StorageVolume> out = []
        try {
            def conts = safeGet(instance, 'getContainers')
            if (conts instanceof Collection) {
                for (def w : conts) {
                    def srv = safeGet(w, 'getServer')
                    if (srv instanceof ComputeServer) out.addAll(faVolumesOf((ComputeServer) srv))
                }
            }
        } catch (Exception e) {
            log.warn("Everpure faVolumesOfInstance(${instance?.name}) failed: ${e.message}")
        }
        return out.unique { it.id }
    }

    /** The VM's disks that are FlashArray volumes on this provider's array. */
    private List<StorageVolume> faVolumesOf(ComputeServer server) {
        List<StorageVolume> out = []
        try {
            def vols = safeGet(server, 'getVolumes')
            if (vols instanceof Collection) {
                for (def v : vols) {
                    if (v instanceof StorageVolume) {
                        // A disk backed by our datastore carries an FA device path on
                        // externalId/deviceName (/dev/mapper/<wwid>) or an FA volume name.
                        String ext = (safeGet(v, 'getExternalId') ?: '') as String
                        String dev = (safeGet(v, 'getDeviceName') ?: '') as String
                        if (ext.contains('/dev/mapper/') || dev.contains('/dev/mapper/') ||
                            (v.datastore != null && faVolumeName(v))) {
                            out << (StorageVolume) v
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Everpure faVolumesOf(${server?.name}) failed: ${e.message}")
        }
        return out
    }

    @Override
    ServiceResponse<StorageVolume> cloneVolume(StorageVolume volume, Snapshot sourceSnapshot) {
        try {
            def (client, ss) = clientFor(null, volume)
            String dest = faVolumeName(volume)
            // If the destination is a vgroup member, ensure its group exists first
            // (idempotent; no-op for a standalone name).
            String destVg = vgroupOf(dest)
            if (destVg) client.createVolumeGroup(destVg)
            client.copyVolume(sourceSnapshot.externalId, dest)
            String hg = PureConfig.hostGroup(ss)
            if (hg) client.connectVolume(dest, hg)
            volume.externalId = dest
            return ServiceResponse.success(volume)
        } catch (Exception e) {
            log.error("Everpure cloneVolume(from snapshot) failed: ${e.message}", e)
            return ServiceResponse.error("cloneVolume from snapshot failed: ${e.message}")
        }
    }

    // ----------------------------------------------------------- helpers ------
    /** Resolve the StorageServer for this volume/server and build a FA client. */
    private List clientFor(ComputeServer server, StorageVolume volume) {
        StorageServer ss = resolveStorageServer(volume)
        if (ss == null) {
            int n = listPureStorageServers().size()
            String why = n > 1 ?
                "${n} Everpure storage servers are registered -- this datastore must be " +
                "linked to a specific one" :
                "no Everpure storage server is registered -- add one under " +
                "Infrastructure > Storage > Storage Servers"
            throw new IllegalStateException("No FlashArray StorageServer for this volume: ${why}.")
        }
        return [PureConfig.client(ss), ss]
    }

    /** Resolve the Everpure StorageServer for a volume/datastore. Uses the explicit link
     *  if present; otherwise falls back to the registered Everpure storage server ONLY
     *  when there is exactly one (unambiguous). With several it returns null so the
     *  caller fails clearly rather than guessing the wrong array. */
    private StorageServer resolveStorageServer(StorageVolume volume) {
        StorageServer ss = volume?.storageServer
        if (ss == null && volume?.datastore != null) {
            try { ss = volume.datastore.storageServer } catch (ignored) { }
        }
        if (ss == null) {
            List servers = listPureStorageServers()
            if (servers.size() == 1) ss = (StorageServer) servers[0]
        }
        return ss
    }

    /** All registered Everpure FlashArray storage servers (by type code). */
    private List listPureStorageServers() {
        try {
            return (morpheusContext.services.storageServer.list(
                new DataQuery().withFilter('type.code', PureStorageProvider.PROVIDER_CODE))) ?: []
        } catch (Exception e) {
            log.warn("Everpure: storage server lookup failed: ${e.message}")
            return []
        }
    }

    /** Stable FlashArray volume name for a VM disk (sanitized to [A-Za-z0-9-]).
     *
     * The FA NAME is tracked in volumeName / uniqueId. NOTE: ``externalId`` holds
     * the host device path (/dev/mapper/<wwid>) because VME uses externalId as the
     * libvirt ``<source dev>``; so externalId must NOT be treated as the FA name
     * once it's been set to a /dev path.
     *
     * The returned name is used VERBATIM for every name-based array op. For an
     * EXISTING volume this is whatever was stored -- a standalone name like
     * ``phif-foo`` (no '/') OR a vgroup member ``<vg>/phif-foo``. We must NEVER
     * re-derive/alter a stored name (that would orphan existing volumes), so the
     * stored value always wins. Only a brand-new volume with nothing stored falls
     * through to {@link #faMemberName} (which adds the per-VM vgroup prefix). */
    private String faVolumeName(StorageVolume volume) {
        for (String v in [volume?.volumeName, volume?.uniqueId, volume?.externalId]) {
            if (v && !v.startsWith('/dev/')) return v
        }
        return faBaseName(volume)
    }

    /** Sanitized standalone (slashless) FA volume name for a disk -- the historical
     *  per-disk name, prefixed with VOL_PREFIX. No vgroup. */
    private String faBaseName(StorageVolume volume) {
        String base = volume?.name ?: ('vol-' + UUID.randomUUID().toString().substring(0, 8))
        String clean = base.replaceAll('[^A-Za-z0-9-]', '-').replaceAll('(^-+|-+$)', '')
        return clean.startsWith(VOL_PREFIX) ? clean : (VOL_PREFIX + clean)
    }

    /**
     * NEW-VM volume name: a volume-group MEMBER ``<vgroup>/<base>`` so every disk
     * of the VM lands in the VM's own FlashArray vgroup and snapshots
     * crash-consistently. Used ONLY when first creating a volume (createVolume /
     * the image+clone create paths) and only when a per-VM vgroup can be derived
     * from the server identity. If no vgroup can be derived (no server context),
     * falls back to the historical standalone name -- existing behavior.
     *
     * ADDITIVE SAFETY: if the volume already carries a stored FA name we DEFER to
     * it (faVolumeName) -- this is reached only for the very first create.
     */
    private String faMemberName(StorageVolume volume, ComputeServer server) {
        // Respect a name we ALREADY assigned as a vgroup MEMBER (re-create / retry):
        // it contains '/'. Do NOT defer to a *bare* stored name -- VME pre-populates
        // volumeName with the disk's name (e.g. "debian_34-disk-0-..") BEFORE this
        // hook runs, so deferring to it would skip the vgroup entirely (the disk
        // would land standalone). Treat a bare stored name as the BASE and wrap it
        // in the per-VM vgroup below.
        for (String v in [volume?.volumeName, volume?.uniqueId, volume?.externalId]) {
            if (v && !v.startsWith('/dev/') && v.contains('/')) return v
        }
        // Base: keep VME's pre-assigned disk name as-is (so the on-array member still
        // reflects the VM/disk), else our sanitized fallback.
        String base = null
        for (String v in [volume?.volumeName, volume?.uniqueId]) {
            if (v && !v.startsWith('/dev/') && !v.contains('/')) { base = v; break }
        }
        if (!base) base = faBaseName(volume)
        String vg = vmVgroup(server, volume)
        return vg ? ("${vg}/${base}".toString()) : base
    }

    /** The volume group for the VM owning this disk: ``phif-<vm-identity>`` derived
     *  from the server (or volume) identity used elsewhere to scope the VM. Returns
     *  null when no stable VM identity is resolvable (then we provision standalone,
     *  preserving the pre-vgroup behavior). FA group names allow only [A-Za-z0-9-]. */
    private String vmVgroup(ComputeServer server, StorageVolume volume) {
        String ident = null
        // Prefer a stable id, then the name; the server is the VM in the MVM flow.
        for (def src : [server, volume]) {
            if (src == null) continue
            def id = safeGet(src, 'getId')
            if (id) { ident = (src.is(server) ? 'srv' : 'vol') + id; break }
            def nm = safeGet(src, 'getName')
            if (nm) { ident = nm as String; break }
        }
        if (!ident) return null
        String clean = ident.replaceAll('[^A-Za-z0-9-]', '-').replaceAll('(^-+|-+$)', '')
        if (!clean) return null
        return clean.startsWith(VOL_PREFIX) ? clean : (VOL_PREFIX + clean)
    }

    /** The vgroup of a member name, or null for a standalone volume. */
    private String vgroupOf(String faName) {
        return PureFlashArrayClient.splitMember(faName)[0]
    }

    /**
     * Run a shell command on a KVM host through the Morpheus context and BLOCK until
     * it completes. VME orchestrates host access, so this avoids a separate SSH
     * credential.
     *
     * CRITICAL: {@code executeCommandOnServer(ComputeServer, String)} returns a lazy
     * RxJava {@code Single<TaskResult>} -- it does NOT dispatch the command until it
     * is subscribed. Calling it without {@code .blockingGet()} silently no-ops (the
     * host never runs the rescan, so /dev/mapper/<wwid> never appears and the libvirt
     * Power On fails with "Cannot access storage file"). blockingGet() both runs the
     * command AND waits for it, which is exactly the ordering guarantee callers need
     * (the device must exist before VME starts the domain).
     */
    /**
     * Every hypervisor host in the cluster. FlashArray volumes are connected to the
     * host GROUP (all hosts), and a VM can be started on or live-migrated to any of
     * them, so LUN discover/assemble (attach, resize) and map/path cleanup (remove)
     * must run on ALL hosts -- not just the one VME happens to pass. Falls back to
     * {@code [server]} if the cluster can't be enumerated. Guest VMs are also
     * ComputeServers, so filter to {@code vmHypervisor} hosts.
     */
    private List<ComputeServer> clusterHosts(ComputeServerGroup cluster, ComputeServer server) {
        Long cid = cluster?.id ?: server?.serverGroup?.id
        List<ComputeServer> hosts = []
        if (cid) {
            try {
                hosts = morpheusContext.services.computeServer.list(
                    new DataQuery().withFilter('serverGroup.id', cid)) ?: []
            } catch (Exception e) {
                log.warn("clusterHosts query failed for cluster ${cid}: ${e.message}")
            }
        }
        // Keep only hypervisor HOSTS. CRITICAL: the `server` passed to the MVM hooks
        // is the GUEST VM being provisioned (also a ComputeServer) -- it is NOT a host
        // and VME has no remote-exec path to a guest ("no path exists for remote
        // execution on <vm>"). So filter to vmHypervisor and NEVER add `server`.
        hosts = hosts.findAll { it?.computeServerType?.vmHypervisor }
        // FALLBACK: during INITIAL provisioning the guest's serverGroup isn't set yet,
        // so the serverGroup query yields nothing and the convert/rescan would run on
        // ZERO hosts ("could not locate source on any cluster host"). Enumerate ALL
        // hypervisor hosts instead (scoped to the guest's cloud when resolvable). The
        // FA LUN spans the host group and our host commands no-op where the device or
        // source is absent, so running on every KVM host is safe.
        if (!hosts) {
            try {
                List all = morpheusContext.services.computeServer.list(new DataQuery()) ?: []
                Long cloudId = null
                try { def cl = safeGet(server, 'getCloud'); cloudId = cl ? (safeGet(cl, 'getId') as Long) : null } catch (ignored) { }
                hosts = all.findAll { ch ->
                    if (!ch?.computeServerType?.vmHypervisor) return false
                    if (cloudId == null) return true
                    Long chCloud = null
                    try { def cc = safeGet(ch, 'getCloud'); chCloud = cc ? (safeGet(cc, 'getId') as Long) : null } catch (ignored) { }
                    return (chCloud == null || chCloud == cloudId)
                }
            } catch (Exception e) {
                log.warn("clusterHosts hypervisor-host fallback failed: ${e.message}")
            }
        }
        // Last resort: only if the server itself is a hypervisor host (direct host op).
        if (!hosts && server?.computeServerType?.vmHypervisor) hosts = [server]
        if (!hosts) log.warn("clusterHosts: no hypervisor hosts resolved (cluster ${cid})")
        else log.info("clusterHosts resolved ${hosts.size()} hypervisor host(s): ${hosts.collect { it.name }}")
        return hosts
    }

    /** Run a command on each host (blocking per host so ordering is deterministic). */
    private void runOnHosts(List<ComputeServer> hosts, String command, String op = 'cmd') {
        (hosts ?: []).each { runOnHost(it, command, op) }
    }

    /**
     * Make every given host discover + assemble the LUN, then wait (up to ~30s) for
     * the multipath device node. Idempotent: the poll returns immediately on hosts
     * that already have the device. Rescan method depends on transport.
     */
    private void ensureDeviceOnHosts(List<ComputeServer> hosts, String protocol, String devPath) {
        String rescan
        if (protocol == 'fc') {
            rescan = "rescan-scsi-bus.sh -a >/dev/null 2>&1 || for s in /sys/class/scsi_host/host*/scan; do echo '- - -' > \$s 2>/dev/null; done"
        } else if (PureConfig.isNvme(protocol)) {
            rescan = "nvme connect-all >/dev/null 2>&1 || true"
        } else {
            // Rescan ACTIVE SESSIONS -- `-m node --rescan` alone frequently misses a
            // newly-presented LUN on an already-established iSCSI session.
            rescan = "iscsiadm -m session --rescan >/dev/null 2>&1; iscsiadm -m node --rescan >/dev/null 2>&1 || true"
        }
        String cmd
        if (devPath) {
            // Poll up to ~30s for the device. If it hasn't appeared after ~8s the LUN
            // number was likely REUSED (a prior volume at this LUN wasn't cleaned off
            // the host, so the kernel still caches the old identity and a plain rescan
            // won't re-map it). Force a remove+add bus scan (rescan-scsi-bus.sh -r,
            // which drops vanished LUNs and adds new ones) to clear the stale identity,
            // then keep polling. This self-heals even if a previous delete's cleanup
            // was missed; the normal case never triggers it.
            cmd = "${rescan}; " +
                "for i in \$(seq 1 30); do " +
                "multipath -r >/dev/null 2>&1 || true; " +
                "[ -e '${devPath}' ] && break; " +
                "if [ \$i -eq 8 ]; then command -v rescan-scsi-bus.sh >/dev/null 2>&1 && rescan-scsi-bus.sh -r >/dev/null 2>&1 || true; ${rescan}; fi; " +
                "sleep 1; done; " +
                "ls -l '${devPath}' 2>&1"
        } else {
            cmd = "${rescan}; multipath -r >/dev/null 2>&1 || true"
        }
        runOnHosts(hosts, cmd, 'rescan')
    }

    /** Null-safe reflective getter (model accessors vary across API versions). */
    private Object safeGet(Object obj, String method) {
        if (obj == null) return null
        try { return obj.invokeMethod(method, null) } catch (ignored) { return null }
    }

    /**
     * TaskResult.exitCode is a STRING ("0", "3", ...), so a direct `== 0` (String vs
     * int) is ALWAYS false in Groovy -- which silently made every convert look failed
     * even when qemu-img succeeded. Parse it to an int (falling back to success flag
     * when exitCode is null/blank). Returns -1 on no result / unparseable.
     */
    private int rcOf(com.morpheusdata.model.TaskResult r) {
        if (r == null) return -1
        String ec = r.exitCode
        if (ec == null || ec.trim().isEmpty()) return (r.success ? 0 : -1)
        try { return ec.trim() as int } catch (Exception e) { return (r.success ? 0 : -1) }
    }

    /**
     * Write the SOURCE disk image onto the raw FlashArray device so the new volume is
     * bootable. The source is never on the Everpure datastore -- it is a system-downloaded
     * image or a disk on another datastore -- so we try, in order: the explicit paths
     * VME put on the sourceVolume (volumePath/deviceName/externalId), then the
     * well-known VME caches (/var/morpheus/kvm/images/<id> for OS images,
     * /var/morpheus/kvm/cloud-init/<name> for cloud-init ISOs). qemu-img convert
     * auto-detects the source format (qcow2 or raw) and writes RAW onto
     * /dev/mapper/<wwid>. The LUN is shared, so one convert (on whichever host has the
     * source) populates it for all; we try hosts until one reports the source present.
     */
    private ServiceResponse convertSourceToDevice(List<ComputeServer> hosts, List<String> explicitPaths,
                                                  String srcName, String devPath) {
        String I = '/var/morpheus/kvm/images'
        String C = '/var/morpheus/kvm/cloud-init'
        String g = (srcName ?: '').replaceAll("[^A-Za-z0-9._-]", "")   // safe for glob
        // Build the candidate list:
        //  - ABSOLUTE paths -> use as-is (system image store / explicit volumePath).
        //  - RELATIVE paths (e.g. 'testvm/<disk>' from a source disk on ANOTHER
        //    datastore) -> resolve under every datastore mount via /mnt/*/<rel> glob,
        //    plus the kvm caches.
        List<String> quoted = []
        StringBuilder globs = new StringBuilder("${I}/${g}* ${C}/${g}*")
        (explicitPaths ?: []).findAll { it }.each { p ->
            if (p.startsWith('/')) {
                quoted << ("'" + p.replace("'", "'\\''") + "'")
            } else {
                String rp = p.replaceAll("[^A-Za-z0-9._/-]", "")   // safe for glob
                globs.append(" /mnt/*/${rp} ${I}/${rp} ${C}/${rp}")
            }
        }
        quoted << "'${I}/${srcName}'".toString()
        quoted << "'${C}/${srcName}'".toString()
        globs.append(" /mnt/*/${g}")
        String forList = (quoted.join(' ') + ' ' + globs.toString())
        String wwid = (devPath && devPath.contains('/')) ? devPath.substring(devPath.lastIndexOf('/') + 1) : devPath

        // START: find the source, wait for the target LUN, then launch qemu-img as a
        // DETACHED background process (setsid, fds redirected) that writes its exit
        // code to a status file. Returns immediately, so NO single host command blocks
        // for the (possibly many-minute) convert -- avoiding the command-exec RPC
        // timeout for large images. Idempotent: won't relaunch if already running/done.
        // Values are passed to the detached qemu-img via exported env vars so the inner
        // single-quoted sh -c needs no fragile nested quoting.
        String start =
            "DEV=\"${devPath}\"; M=\"/var/tmp/pure-conv-${wwid}\"; SRC=\"\"; " +
            "for c in ${forList}; do [ -n \"\$c\" ] && [ -f \"\$c\" ] && { SRC=\"\$c\"; break; }; done; " +
            "if [ -z \"\$SRC\" ]; then echo PURE_SRC_NOT_FOUND; exit 3; fi; " +
            "for i in \$(seq 1 45); do [ -e \"\$DEV\" ] && break; iscsiadm -m session --rescan >/dev/null 2>&1; multipath -r >/dev/null 2>&1; sleep 1; done; " +
            "if [ ! -e \"\$DEV\" ]; then echo PURE_DEV_NOT_READY; exit 4; fi; " +
            "if [ -f \"\$M.status\" ] && [ \"\$(cat \$M.status 2>/dev/null)\" = 0 ]; then echo PURE_ALREADY_DONE; exit 0; fi; " +
            "if ! pgrep -f \"qemu-img convert.*${wwid}\" >/dev/null 2>&1; then " +
            "rm -f \"\$M.status\"; export PURE_SRC=\"\$SRC\" PURE_DEV=\"\$DEV\" PURE_STATUS=\"\$M.status\"; " +
            "if command -v setsid >/dev/null 2>&1; then setsid sh -c 'qemu-img convert -U -O raw \"\$PURE_SRC\" \"\$PURE_DEV\"; echo \$? > \"\$PURE_STATUS\"' </dev/null >\"\$M.log\" 2>&1 & " +
            "else nohup sh -c 'qemu-img convert -U -O raw \"\$PURE_SRC\" \"\$PURE_DEV\"; echo \$? > \"\$PURE_STATUS\"' </dev/null >\"\$M.log\" 2>&1 & fi; fi; " +
            "echo PURE_CONV_STARTED \"\$SRC\"; exit 0"

        // POLL: status file is the source of truth (rc=0 done-ok, else done-fail);
        // a live qemu-img for this wwid means still running. Quick command, well within
        // any RPC timeout.
        String poll =
            "M=\"/var/tmp/pure-conv-${wwid}\"; " +
            "if [ -f \"\$M.status\" ]; then S=\"\$(cat \$M.status 2>/dev/null)\"; echo \"PURE_CONV_DONE rc=\$S\"; [ \"\$S\" = 0 ] && exit 0 || exit 5; fi; " +
            "if pgrep -f \"qemu-img convert.*${wwid}\" >/dev/null 2>&1; then echo PURE_CONV_RUNNING; exit 6; fi; " +
            "echo PURE_CONV_UNKNOWN; tail -3 \"\$M.log\" 2>/dev/null; exit 7"

        log.info("Everpure convertSourceToDevice(bg): src='${srcName}' dev='${devPath}' hosts=${(hosts ?: []).collect { it.name }} explicit=${explicitPaths}")
        ComputeServer convHost = null
        for (ComputeServer hh : (hosts ?: [])) {
            def r = runOnHost(hh, start, 'convert-start')
            log.info("Everpure convert-start on ${hh?.name}: exit=${r?.exitCode}")
            if (rcOf(r) == 0) { convHost = hh; break }
        }
        if (convHost == null) {
            return ServiceResponse.error(
                "cloneVolume: no cluster host had source '${srcName}' + a ready device for ${devPath}. " +
                "Tried ${explicitPaths} + /var/morpheus/kvm/{images,cloud-init}. " +
                "If the image lives on another datastore, its mount must be reachable on the KVM hosts.")
        }
        log.info("Everpure convert started on ${convHost.name}; polling for completion -> ${devPath}")
        int maxPolls = 360, unknown = 0      // up to ~360 * 6s = 36 min for large images
        for (int p = 0; p < maxPolls; p++) {
            try { Thread.sleep(6000) } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break }
            def r = runOnHost(convHost, poll, 'convert-poll')
            int ec = rcOf(r)
            if (ec == 0) { log.info("Everpure convert SUCCEEDED on ${convHost.name} -> ${devPath}"); return ServiceResponse.success() }
            if (ec == 5) return ServiceResponse.error("cloneVolume: qemu-img convert FAILED on ${convHost.name} for ${devPath} (see ${convHost.name}:/var/tmp/pure-conv-${wwid}.log)")
            if (ec == 7) { if (++unknown >= 3) return ServiceResponse.error("cloneVolume: convert process vanished on ${convHost.name} before writing status for ${devPath} (see /var/tmp/pure-conv-${wwid}.log)") }
            else { unknown = 0 }   // ec==6 running, or null transient -> keep polling
        }
        return ServiceResponse.error("cloneVolume: convert did not finish within the poll window on ${convHost.name} for ${devPath}")
    }

    private com.morpheusdata.model.TaskResult runOnHost(ComputeServer server, String command) {
        return runOnHost(server, command, 'cmd')
    }

    private com.morpheusdata.model.TaskResult runOnHost(ComputeServer server, String command, String op) {
        if (server == null) return null
        try {
            // VME runs host commands as the UNPRIVILEGED 'morpheus-node' user, but
            // iscsiadm/multipath/nvme/rescan-scsi-bus.sh and writes to
            // /sys/.../device/delete all require root -- without it the rescan/cleanup
            // silently fails ("Permission denied", exit 2) so disks never assemble on
            // add and stale maps never clear on delete. morpheus-node is granted
            // passwordless sudo (NOPASSWD:ALL, !requiretty) by VME, so run the script
            // as root via `sudo -n sh`. The script is piped in base64-encoded to
            // sidestep ALL shell-quoting issues with the multi-statement command.
            String b64 = command.bytes.encodeBase64().toString()
            String wrapped = "echo ${b64} | base64 -d | sudo -n sh"
            com.morpheusdata.model.TaskResult result =
                morpheusContext.executeCommandOnServer(server, wrapped).blockingGet()
            log.info("Everpure runOnHost [${server.name}] op=${op} success=${result?.success} exit=${result?.exitCode} " +
                     "out=${((result?.output ?: '') as String).take(300)} err=${((result?.error ?: '') as String).take(200)}")
            return result
        } catch (Exception e) {
            log.warn("Everpure runOnHost [${server?.name}] op=${op} threw: ${e.message}", e)
            return null
        }
    }
}
