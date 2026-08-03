package com.morpheusdata.pure

import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.providers.AbstractStorageProvider
import com.morpheusdata.core.providers.StorageProvider
import com.morpheusdata.core.providers.StorageProviderVolumes
import com.morpheusdata.model.Icon
import com.morpheusdata.model.OptionType
import com.morpheusdata.model.StorageGroup
import com.morpheusdata.model.StorageServer
import com.morpheusdata.model.StorageServerType
import com.morpheusdata.model.StorageVolume
import com.morpheusdata.model.StorageVolumeType
import com.morpheusdata.response.ServiceResponse
import groovy.util.logging.Slf4j

/**
 * Registers an Everpure FlashArray as a Morpheus/VME StorageServerType so it
 * can be added under Infrastructure > Storage > Storage Servers, with connection
 * verification and a capacity refresh. Also implements StorageProviderVolumes so
 * volumes can be created/resized/deleted directly against the array.
 *
 * The per-VM-disk provisioning + KVM attach lives in {@link PureDatastoreProvider}
 * (whose {@code getStorageProviderCode()} points back at this provider's code).
 */
@Slf4j
class PureStorageProvider extends AbstractStorageProvider implements StorageProviderVolumes {

    static final String PROVIDER_CODE = 'pure-flasharray-vme.storage'

    protected MorpheusContext morpheusContext
    protected Plugin plugin

    PureStorageProvider(Plugin plugin, MorpheusContext morpheusContext) {
        this.plugin = plugin
        this.morpheusContext = morpheusContext
    }

    @Override String getCode() { return PROVIDER_CODE }
    @Override String getName() { return 'Everpure FlashArray' }
    @Override MorpheusContext getMorpheus() { return this.morpheusContext }
    @Override Plugin getPlugin() { return this.plugin }

    @Override String getDescription() { return 'Everpure FlashArray (per-VM-disk block volumes for VME)' }
    @Override Icon getIcon() { return new Icon(path: 'pure.svg', darkPath: 'pure.svg') }

    @Override
    StorageServerType getStorageServerType() {
        StorageServerType type = new StorageServerType(
            code: getCode(),
            name: getName(),
            description: getDescription(),
            hasBlock: true,      createBlock: true,    // FlashArray serves block LUNs
            hasObject: false,    createObject: false,
            hasFile: false,      createFile: false,
            hasDatastore: true,  createDatastore: true,  // exposed as a VME datastore type
            hasNamespaces: false, createNamespaces: false,
            hasGroups: false,    createGroup: false,
            hasDisks: true,      createDisk: true,       // one FA volume per VM disk
            hasHosts: false,     createHost: false,
            hasFileBrowser: false,
            enabled: true,
            creatable: true,
        )
        type.optionTypes = storageServerOptionTypes()
        type.volumeTypes = getStorageVolumeTypes()
        return type
    }

    /** Connection fields shown when adding the FlashArray as a storage server. */
    Collection<OptionType> storageServerOptionTypes() {
        return [
            new OptionType(
                name: 'Management endpoint', code: "${PROVIDER_CODE}.serviceUrl",
                fieldName: 'serviceUrl', fieldContext: 'domain', fieldLabel: 'Management Endpoint',
                inputType: OptionType.InputType.TEXT, required: true, displayOrder: 1,
                placeHolderText: 'https://flasharray.example.local',
                helpText: 'FlashArray management IP / DNS (REST v2).'),
            new OptionType(
                name: 'API token', code: "${PROVIDER_CODE}.serviceToken",
                fieldName: 'serviceToken', fieldContext: 'domain', fieldLabel: 'API Token',
                inputType: OptionType.InputType.PASSWORD, secretField: true, required: true,
                displayOrder: 2, helpText: 'FlashArray REST API token for an array user.'),
            new OptionType(
                name: 'Host group', code: "${PROVIDER_CODE}.hostGroup",
                fieldName: 'hostGroup', fieldContext: 'config', fieldLabel: 'Host Group',
                inputType: OptionType.InputType.TEXT, required: true, displayOrder: 3,
                helpText: 'FlashArray host group containing the VME KVM hosts; per-disk volumes are connected to it.'),
            // NB: plain TEXT, NOT a SELECT. A SELECT OptionType with a null
            // optionSource makes Morpheus NPE (tokenize() on null in
            // OptionSourcePluginService.pluginHasMethod) while rendering the form /
            // options endpoints -- which blanks the datastore "Cloud" dropdown for
            // EVERY type and 500s /api/options/zones. Static text avoids the
            // option-source machinery entirely.
            new OptionType(
                name: 'Protocol', code: "${PROVIDER_CODE}.protocol",
                fieldName: 'protocol', fieldContext: 'config', fieldLabel: 'Storage Protocol',
                inputType: OptionType.InputType.TEXT, required: true, defaultValue: 'iscsi',
                displayOrder: 4,
                helpText: 'Transport used to present volumes to the KVM hosts: iscsi | fc | nvme-tcp (default iscsi).'),
            new OptionType(
                name: 'Eradicate on delete', code: "${PROVIDER_CODE}.eradicate",
                fieldName: 'eradicate', fieldContext: 'config', fieldLabel: 'Eradicate on Delete',
                inputType: OptionType.InputType.CHECKBOX, defaultValue: 'false', displayOrder: 5,
                helpText: 'Hard-delete (eradicate) destroyed volumes immediately instead of leaving them in 24h pending-eradication.'),
        ]
    }

    @Override
    ServiceResponse verifyStorageServer(StorageServer storageServer, Map opts) {
        try {
            if (!PureConfig.endpoint(storageServer)) {
                return ServiceResponse.error('Management endpoint is required')
            }
            if (!PureConfig.token(storageServer)) {
                return ServiceResponse.error('API token is required')
            }
            // Token login + a lightweight call proves reachability + credentials.
            PureConfig.client(storageServer, 15000).arraySpace()
            return ServiceResponse.success()
        } catch (Exception e) {
            log.error("Everpure verifyStorageServer failed: ${e.message}", e)
            return ServiceResponse.error("Could not reach FlashArray: ${e.message}")
        }
    }

    @Override
    ServiceResponse initializeStorageServer(StorageServer storageServer, Map opts) {
        // Nothing array-side to initialize for the per-disk model; capacity is
        // populated by refreshStorageServer.
        return refreshStorageServer(storageServer, opts)
    }

    @Override
    ServiceResponse refreshStorageServer(StorageServer storageServer, Map opts) {
        try {
            Map space = PureConfig.client(storageServer, 15000).arraySpace()
            Long capacity = (space?.capacity ?: 0L) as Long
            Long used = ((space?.space instanceof Map ? space.space.total_physical : 0L) ?: 0L) as Long
            storageServer.setMaxStorage(capacity)
            storageServer.setUsedStorage(used)
            storageServer.setStatus('ok')
            storageServer.setStatusMessage(null)
            // Persist the refreshed capacity/status. Best-effort: a save failure
            // must never fail the refresh (the in-memory StorageServer returned in
            // the ServiceResponse still reflects the live values for the caller).
            persistStorageServer(storageServer)
            return ServiceResponse.success(storageServer)
        } catch (Exception e) {
            log.error("Everpure refreshStorageServer failed: ${e.message}", e)
            storageServer.setStatus('error')
            storageServer.setStatusMessage(e.message)
            persistStorageServer(storageServer)
            return ServiceResponse.error("FlashArray refresh failed: ${e.message}")
        }
    }

    /**
     * Persist the refreshed StorageServer (capacity / used / status) so the UI and
     * scheduler see the live values across requests. Best-effort by contract:
     * the reactive {@code async.storageServer.save(...)} is run to completion with
     * {@code blockingGet()} (an unsubscribed Single silently no-ops), but ANY
     * failure is swallowed and logged so it can never fail the refresh. Per-volume
     * StorageVolume inventory sync is intentionally out of scope for the
     * per-VM-disk model (volumes are owned by the datastore provider).
     */
    protected void persistStorageServer(StorageServer storageServer) {
        try {
            morpheusContext.async.storageServer.save(storageServer).blockingGet()
        } catch (Throwable t) {
            log.warn("Everpure: could not persist StorageServer ${storageServer?.name} capacity/status: ${t.message}")
        }
    }

    // ---------------- StorageProviderVolumes (direct array volume CRUD) -------
    // The per-VM-disk lifecycle runs through PureDatastoreProvider; these let an
    // operator also manage volumes directly on the storage server.
    @Override
    ServiceResponse<StorageVolume> createVolume(StorageGroup storageGroup, StorageVolume storageVolume, Map opts) {
        return ServiceResponse.error('Create volumes via the FlashArray datastore (per-VM disk), not the storage group')
    }

    @Override
    ServiceResponse<StorageVolume> resizeVolume(StorageGroup storageGroup, StorageVolume storageVolume, Map opts) {
        return ServiceResponse.error('Resize volumes via the FlashArray datastore (per-VM disk)')
    }

    // Added to StorageProviderVolumes in plugin-api 1.3.x.
    @Override
    ServiceResponse updateVolume(StorageGroup storageGroup, StorageVolume storageVolume, Map opts) {
        return ServiceResponse.error('Update volumes via the FlashArray datastore (per-VM disk)')
    }

    @Override
    ServiceResponse<StorageVolume> deleteVolume(StorageGroup storageGroup, StorageVolume storageVolume, Map opts) {
        return ServiceResponse.error('Delete volumes via the FlashArray datastore (per-VM disk)')
    }

    @Override
    Collection<StorageVolumeType> getStorageVolumeTypes() {
        return [
            new StorageVolumeType(
                code: 'pure-flasharray-vme.volume',
                name: 'Everpure FlashArray Volume',
                displayName: 'Everpure FlashArray Volume',
                volumeType: 'disk', volumeCategory: 'disk',
                customLabel: true, customSize: true,
                resizable: true, planResizable: true, deletable: true, editable: true,
                hasDatastore: true,
            ),
        ]
    }
}
