package com.morpheusdata.pure

import com.morpheusdata.model.StorageServer

/**
 * Extracts the FlashArray connection settings from a {@link StorageServer} and
 * builds a {@link PureFlashArrayClient}.
 *
 * Mapping (set on the StorageServerType's OptionTypes in {@link PureStorageProvider}):
 *   endpoint   -> StorageServer.serviceUrl   (model field)
 *   api token  -> StorageServer.serviceToken (model field, secret)
 *   host group -> config.hostGroup           (config map)
 *   protocol   -> config.protocol            (config map; iscsi|fc|nvme-tcp|nvme-fc)
 *   eradicate  -> config.eradicate           (config map; "true"/"false")
 */
class PureConfig {

    static String endpoint(StorageServer ss) {
        return (ss?.serviceUrl ?: ss?.serviceHost ?: '') as String
    }

    static String token(StorageServer ss) {
        return (ss?.serviceToken ?: '') as String
    }

    static String hostGroup(StorageServer ss) {
        return cfg(ss, 'hostGroup', '')
    }

    static String protocol(StorageServer ss) {
        return (cfg(ss, 'protocol', 'iscsi') ?: 'iscsi').toLowerCase()
    }

    static boolean eradicate(StorageServer ss) {
        return cfg(ss, 'eradicate', 'false')?.toLowerCase() in ['1', 'true', 'yes', 'on']
    }

    static PureFlashArrayClient client(StorageServer ss, int timeoutMs = 30000) {
        return new PureFlashArrayClient(endpoint(ss), token(ss), timeoutMs)
    }

    static boolean isNvme(String protocol) {
        return protocol in ['nvme-tcp', 'nvme-fc', 'nvme-roce']
    }

    /**
     * Read a custom config value (fieldContext="config" OptionTypes) off the model.
     * MorpheusModel exposes getConfigProperty on recent API versions; guard for it.
     */
    static String cfg(StorageServer ss, String key, String dflt = '') {
        if (ss == null) return dflt
        try {
            if (ss.metaClass.respondsTo(ss, 'getConfigProperty', String)) {
                def v = ss.getConfigProperty(key)
                if (v != null) return v as String
            }
        } catch (ignored) { }
        try {
            def cm = ss.metaClass.respondsTo(ss, 'getConfigMap') ? ss.getConfigMap() : null
            if (cm != null && cm[key] != null) return cm[key] as String
        } catch (ignored) { }
        return dflt
    }
}
