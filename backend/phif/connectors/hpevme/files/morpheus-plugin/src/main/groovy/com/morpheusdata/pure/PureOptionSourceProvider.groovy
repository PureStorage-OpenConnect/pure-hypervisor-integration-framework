package com.morpheusdata.pure

import com.morpheusdata.core.AbstractOptionSourceProvider
import com.morpheusdata.core.MorpheusContext
import com.morpheusdata.core.Plugin
import com.morpheusdata.core.data.DataQuery
import groovy.util.logging.Slf4j

/**
 * Supplies dropdown data for the Everpure plugin's OptionTypes.
 *
 * The datastore-create form (cluster Storage > Add) shows a "FlashArray Storage
 * Server" SELECT whose {@code optionSource} is {@code pureStorageServers}; this
 * provider implements that method so an operator can pick WHICH FlashArray a
 * datastore provisions on when more than one is registered. (A SELECT must have a
 * real option source -- a null one makes Morpheus NPE rendering the form.)
 */
@Slf4j
class PureOptionSourceProvider extends AbstractOptionSourceProvider {

    static final String PROVIDER_CODE = 'pure-flasharray-vme.optionsource'

    protected MorpheusContext morpheusContext
    protected Plugin plugin

    PureOptionSourceProvider(Plugin plugin, MorpheusContext morpheusContext) {
        this.plugin = plugin
        this.morpheusContext = morpheusContext
    }

    @Override String getCode() { return PROVIDER_CODE }
    @Override String getName() { return 'Everpure FlashArray Option Source' }
    @Override MorpheusContext getMorpheus() { return this.morpheusContext }
    @Override Plugin getPlugin() { return this.plugin }

    @Override
    List<String> getMethodNames() { return ['pureStorageServers'] }

    /** Registered Everpure FlashArray storage servers as [{name, value: id}] for a SELECT. */
    def pureStorageServers(args) {
        try {
            List servers = morpheusContext.services.storageServer.list(
                new DataQuery().withFilter('type.code', PureStorageProvider.PROVIDER_CODE)) ?: []
            return servers.collect { [name: (it.name ?: ('Storage Server ' + it.id)), value: it.id] }
        } catch (Exception e) {
            log.error("Everpure pureStorageServers option source failed: ${e.message}", e)
            return []
        }
    }
}
