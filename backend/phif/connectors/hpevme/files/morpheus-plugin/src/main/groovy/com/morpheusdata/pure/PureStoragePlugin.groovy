package com.morpheusdata.pure

import com.morpheusdata.core.Plugin

/**
 * Everpure FlashArray plugin for HPE VM Essentials (VME) / Morpheus.
 *
 * Registers two providers:
 *   - {@link PureStorageProvider}   -- a StorageServerType so a FlashArray can be
 *     added under Infrastructure > Storage > Storage Servers (verify/init/refresh).
 *   - {@link PureDatastoreProvider} -- a DatastoreTypeProvider that creates one FA
 *     volume per VM disk, offloads snapshot/clone/resize to the array, and (via
 *     MvmProvisionFacet) presents the volume to the KVM host + emits the libvirt
 *     disk config so VME attaches it natively as a raw block device.
 *
 * Packaged via {@code ./gradlew shadowJar} and uploaded under
 * Administration > Integrations > Plugins.
 */
class PureStoragePlugin extends Plugin {

    @Override
    String getCode() {
        return 'pure-flasharray-vme'
    }

    @Override
    void initialize() {
        // NB: no parentheses / special chars in the name. The icon URL is built by
        // slugifying this name (spaces -> dashes), but the appliance's asset router
        // slugifies more strictly (strips '()'), so a name like "... (VME)" yields
        // /assets/plugin/...-(vme)/pure.svg on one side and ...-vme on the other ->
        // 404 broken icon. Keep it alphanumeric+spaces so both slugs agree.
        this.setName('Everpure FlashArray VME')
        PureStorageProvider storageProvider = new PureStorageProvider(this, morpheus)
        PureDatastoreProvider datastoreProvider = new PureDatastoreProvider(this, morpheus)
        PureOptionSourceProvider optionSourceProvider = new PureOptionSourceProvider(this, morpheus)
        this.registerProvider(storageProvider)
        this.registerProvider(datastoreProvider)
        // Supplies the 'pureStorageServers' option source for the datastore form's
        // storage-server selector (multi-array support).
        this.registerProvider(optionSourceProvider)
    }

    @Override
    void onDestroy() {
        // No background workers to stop.
    }
}
