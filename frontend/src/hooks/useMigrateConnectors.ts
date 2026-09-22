import { useMemo } from "react";
import { api } from "../api/client";
import type { Hypervisor } from "../api/client";
import { useAsync } from "../components";

// Which connectors can take part in a migration is decided by the BACKEND: a
// connector declares Capability.MIGRATE and the API surfaces it on the
// descriptor. Deriving the set here keeps the UI honest.
//
// This used to be a hardcoded list of connector keys, duplicated in
// MigrationPage and MigrationGroupsPanel. Adding the Nutanix connector made the
// drift visible: the backend happily accepted it as a migration destination
// while the UI silently omitted it from the picker, so it looked like a
// configuration problem rather than a stale constant.
const MIGRATE_CAPABILITY = "migrate";

export function useMigrateConnectors() {
  const connectors = useAsync(() => api.listConnectors());

  const migrateKeys = useMemo(() => {
    const keys = (connectors.data ?? [])
      .filter((c) => (c.capabilities ?? []).includes(MIGRATE_CAPABILITY))
      .map((c) => c.key);
    return new Set(keys);
  }, [connectors.data]);

  // While the descriptors are still loading `migrateKeys` is empty, which would
  // render an empty picker and a misleading "no eligible destinations" notice.
  // Callers use `ready` to tell "still loading" from "genuinely none".
  const ready = !connectors.loading && connectors.error == null;

  const filterMigratable = (hypervisors: Hypervisor[] | null | undefined) =>
    (hypervisors ?? []).filter((h) => migrateKeys.has(h.connector_key));

  return { migrateKeys, ready, filterMigratable, error: connectors.error };
}
