import type { Placement } from "../api/client";

// A connector could return a malformed placement. The Nutanix connector once
// returned a FLAT {id,name,kind} list instead of the contract's
// {cluster:{id,name}, storage:[...]}, and `p.cluster.id` then threw inside a
// useMemo — which React escalates into a full-page "Unexpected Application
// Error" the moment that destination is selected.
//
// The connector is the real fix (and a test now asserts the shape for every
// connector), but the wizard should degrade to an empty picker rather than take
// the whole UI down for one bad entry.
export function validPlacements(ps: Placement[] | null | undefined): Placement[] {
  return (ps ?? []).filter(
    (p): p is Placement =>
      !!p && typeof p === "object" && !!p.cluster && !!p.cluster.id,
  );
}
