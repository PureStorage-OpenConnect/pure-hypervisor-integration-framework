# Everpure FlashArray plugin for HPE VM Essentials (VME)

A native **Morpheus / HPE VME storage provider plugin** (Java/Groovy) that makes a
Everpure FlashArray a first-class storage backend in VME: one FA volume per VM disk,
attached to the KVM VM as a raw multipathed block device, with snapshot / clone /
resize **offloaded to the array**.

This is the *supported-shaped* integration path (VME orchestrates host
presentation + the libvirt disk definition itself) — as opposed to PHIF's
fallback of attaching `/dev/mapper/<wwid>` over SSH, which goes behind VME's
control plane.

## Why a plugin

HPE VME is rebranded Morpheus and exposes the `morpheus-plugin-core` SDK. The SDK
provides exactly the contracts an Everpure integration needs:

| Contract | Used for |
|---|---|
| `StorageProvider` + `StorageServerType` | Register the FlashArray as a Storage Server (verify / refresh capacity). |
| `StorageProviderVolumes` | Direct volume CRUD on the array (secondary). |
| `DatastoreTypeProvider` | Per-VM-disk `createVolume` / `removeVolume` / `cloneVolume` / `resizeVolume`. |
| `DatastoreTypeProvider.SnapshotFacet` | Array-offloaded volume snapshots + clone-from-snapshot. |
| `DatastoreTypeProvider.MvmProvisionFacet` | **VME-native per-disk attach**: `prepareHostForVolume` (multipath rescan), `buildDiskConfig` → `MvmDiskConfig` (libvirt disk), `releaseVolumeFromHost` (multipath flush). |

## Layout

```
morpheus-plugin/
├─ build.gradle              # shadowJar; compileOnly morpheus-plugin-api 1.2.9 + groovy
├─ settings.gradle
├─ gradle.properties         # SDK / toolchain versions
└─ src/main/groovy/com/morpheusdata/pure/
   ├─ PureStoragePlugin.groovy     # Plugin entrypoint; registers both providers
   ├─ PureStorageProvider.groovy   # StorageServerType + connection OptionTypes + verify/refresh
   ├─ PureDatastoreProvider.groovy # volume CRUD + SnapshotFacet + MvmProvisionFacet
   ├─ PureConfig.groovy            # pull endpoint/token/host-group/protocol off the StorageServer
   └─ PureFlashArrayClient.groovy  # FlashArray REST v2 client (no external deps)
```

## Build

**The JAR is built automatically when the PHIF backend image is built** — a
`gradle:8.5-jdk11` stage in `backend/Dockerfile` runs `shadowJar` and copies the
result into the image at `build/libs/`, where the `hpevme` connector's `deploy`
action finds it. So `docker compose build` (what the deploy scripts run) produces
it; no manual step is needed.

To build it standalone (needs a JDK 11 + Gradle, or use the `gradle:8.5-jdk11`
image):

```bash
cd backend/phif/connectors/hpevme/files/morpheus-plugin
gradle shadowJar          # or ./gradlew shadowJar once a wrapper is committed
# -> build/libs/pure-flasharray-vme-plugin-0.1.0-all.jar
```

Targets **Java 11** (the published `morpheus-plugin-api` is a Java 11 artifact).
`morpheus-plugin-api`, Groovy, and Karman are `compileOnly` — the appliance
provides them at runtime, so the JAR stays lean.

## Install

Upload the shadow JAR in the VME Manager UI:
**Administration → Integrations → Plugins → Choose File**. It loads on every node
with no restart. Then add the array under **Infrastructure → Storage → Storage
Servers** (type *Everpure FlashArray*), supplying endpoint, API token, host
group, and protocol.

PHIF automates upload + storage-server registration via the `hpevme` connector
(`deploy` uploads the JAR through the Plugins API; `configure` registers the
storage server).

## Status — GA; iSCSI block path validated on a live VME appliance

The providers are written against the `morpheus-plugin-core` `rel-2.10.0` interface
signatures (`StorageProvider`/`AbstractStorageProvider`, `DatastoreTypeProvider` +
`MvmProvisionFacet` + `SnapshotFacet`, `ServiceResponse`, and the model classes) and
**compile cleanly against the real `morpheus-plugin-api:1.2.9`** (verified by the
Docker build stage).

**Validated on a live VME appliance over the iSCSI block path:**

- the **MVM provision type code** (`getProvisionTypeCode()`) and the per-VM-disk
  provision / attach / release lifecycle via `MvmProvisionFacet`;
- `MvmDiskConfig` + `StorageVolume.deviceName`/`wwn` mapping onto the libvirt
  `<disk><source dev=…/>` for a raw `/dev/mapper/<wwid>` multipath block device;
- host / initiator registration over iSCSI (one FA host per KVM node in a shared
  host group);
- the `MorpheusContext.executeCommandOnServer(...)` call used for the host-side
  multipath rescan / flush;
- image convert / clone (image-based deploy and clone-from-running-VM);
- snapshot create / revert / delete via `SnapshotFacet`;
- StorageServer **config persistence** in `refreshStorageServer` (capacity / status
  sync via `morpheusContext.async.storageServer.save(...)`, best-effort).

Full per-volume `StorageVolume` inventory sync is intentionally out of scope for the
per-VM-disk model (volumes are owned by the datastore provider).

Items still marked `TODO(validate-on-appliance)` are **not** yet hardware-validated
(only iSCSI block was): the **NVMe-TCP** and **FC** transports, the NVMe-oF
namespace device path, the guest-side resize rescan trigger, and the NFS /
FlashArray File datastore path.

Target SDK: `com.morpheusdata:morpheus-plugin-api:1.2.9` (Maven Central; source tag
`rel-2.10.0`). Newer HPE-published APIs add `prepareVolumeAttach` /
`filterStorageVolumeTypes` if you need them.
