import { useEffect, useMemo, useState } from "react";
import { api } from "../api/client";
import type {
  Hypervisor, MigrationGroup, NetworkSummary, Placement, VmSpec, VmSummary,
} from "../api/client";

// Batch + scheduled migrations. A group migrates several VMs from one source
// hypervisor to one destination (same FlashArray) with a shared network map and
// options, run together up to `concurrency` at once — now or at a scheduled time.
// The single-VM wizard lives in MigrationPage; this panel is the multi-VM path.

const MIGRATE_CONNECTORS = new Set(["proxmox", "xcpng", "hpevme", "vsphere", "openstack", "openshift"]);

function statusClass(s: string): string {
  if (s === "succeeded") return "ga";
  if (s === "running" || s === "partial") return "preview";
  if (s === "failed") return "error-badge";
  return "scaffold"; // scheduled / pending / canceled / skipped
}

// datetime-local value ("YYYY-MM-DDTHH:mm", local) -> ISO-8601 UTC, or null.
function localToIso(v: string): string | null {
  if (!v) return null;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? null : d.toISOString();
}

function GroupRow({ g, onChanged }: { g: MigrationGroup; onChanged: () => void }) {
  const [open, setOpen] = useState(false);
  const [busy, setBusy] = useState(false);
  const counts = useMemo(() => {
    const c: Record<string, number> = {};
    for (const m of g.members) c[m.status ?? "pending"] = (c[m.status ?? "pending"] ?? 0) + 1;
    return c;
  }, [g.members]);

  const act = async (fn: () => Promise<unknown>) => {
    setBusy(true);
    try { await fn(); } catch (e) { alert(String((e as Error).message)); }
    finally { setBusy(false); onChanged(); }
  };

  const canRun = ["scheduled", "pending", "failed", "partial", "canceled"].includes(g.status);
  const canCancel = ["scheduled", "pending", "running"].includes(g.status);
  const canDelete = g.status !== "running";

  return (
    <>
      <tr>
        <td>
          <button onClick={() => setOpen((o) => !o)}
                  style={{ background: "none", border: "none", padding: 0, cursor: "pointer",
                           textDecoration: "underline", color: "inherit", font: "inherit" }}>
            {open ? "▾" : "▸"} {g.name}
          </button>
        </td>
        <td><span className={`badge ${statusClass(g.status)}`}>{g.status}</span></td>
        <td>
          {g.members.length} VM{g.members.length === 1 ? "" : "s"}
          {Object.keys(counts).length > 0 && (
            <span className="muted" style={{ fontSize: 12 }}>
              {" "}({Object.entries(counts).map(([k, v]) => `${v} ${k}`).join(", ")})
            </span>
          )}
        </td>
        <td className="muted" style={{ fontSize: 12 }}>
          {g.scheduled_at ? new Date(g.scheduled_at).toLocaleString() : "—"}
        </td>
        <td style={{ whiteSpace: "nowrap" }}>
          {canRun && <button disabled={busy} onClick={() => act(() => api.runMigrationGroup(g.id))}>Run now</button>}{" "}
          {canCancel && <button className="secondary" disabled={busy} onClick={() => act(() => api.cancelMigrationGroup(g.id))}>Cancel</button>}{" "}
          {canDelete && <button className="danger" disabled={busy} onClick={() => { if (confirm(`Delete group "${g.name}"?`)) act(() => api.deleteMigrationGroup(g.id)); }}>Delete</button>}
        </td>
      </tr>
      {open && (
        <tr>
          <td colSpan={5} style={{ background: "var(--panel-2)" }}>
            <table style={{ margin: 0 }}>
              <thead><tr><th>VM</th><th>Status</th><th>Migration</th><th>Error</th></tr></thead>
              <tbody>
                {g.members.map((m, i) => (
                  <tr key={i}>
                    <td><code>{m.vm_ref}</code></td>
                    <td><span className={`badge ${statusClass(m.status ?? "pending")}`}>{m.status ?? "pending"}</span></td>
                    <td className="muted" style={{ fontSize: 12 }}>{m.migration_id ?? "—"}</td>
                    <td className="error" style={{ fontSize: 12 }}>{m.error ?? ""}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </td>
        </tr>
      )}
    </>
  );
}

export default function MigrationGroupsPanel({ hypervisors }: { hypervisors: Hypervisor[] }) {
  const [groups, setGroups] = useState<MigrationGroup[] | null>(null);
  const reload = async () => { try { setGroups(await api.listMigrationGroups()); } catch { /* ignore */ } };

  // Poll while any group is active so live status updates without a manual refresh.
  useEffect(() => {
    reload();
    const t = setInterval(reload, 5000);
    return () => clearInterval(t);
  }, []);

  // --- Builder state ---
  const [name, setName] = useState("");
  const [sourceId, setSourceId] = useState("");
  const [destId, setDestId] = useState("");
  const [vms, setVms] = useState<VmSummary[] | null>(null);
  const [selected, setSelected] = useState<Record<string, boolean>>({});
  const [placements, setPlacements] = useState<Placement[] | null>(null);
  const [destCluster, setDestCluster] = useState("");
  const [destStorage, setDestStorage] = useState("");
  const [destNets, setDestNets] = useState<NetworkSummary[] | null>(null);
  const [sourceNets, setSourceNets] = useState<string[] | null>(null);
  const [networkMap, setNetworkMap] = useState<Record<string, string>>({});
  const [mode, setMode] = useState<"move" | "copy">("move");
  const [forceStop, setForceStop] = useState(false);
  const [powerOn, setPowerOn] = useState(false);
  const [convertToVmfs, setConvertToVmfs] = useState(false);
  const [scheduledAt, setScheduledAt] = useState("");
  const [concurrency, setConcurrency] = useState(1);
  const [continueOnError, setContinueOnError] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [msg, setMsg] = useState<string | null>(null);

  const eligible = useMemo(
    () => hypervisors.filter((h) => MIGRATE_CONNECTORS.has(h.connector_key)),
    [hypervisors],
  );
  const source = eligible.find((h) => h.id === sourceId);
  const dest = eligible.find((h) => h.id === destId);
  const destIsVsphere = dest?.connector_key === "vsphere";
  const destChoices = eligible.filter((h) => h.id !== sourceId && source?.array_id && h.array_id);
  const clusterStorage = useMemo(
    () => placements?.find((p) => p.cluster.id === destCluster)?.storage ?? [],
    [placements, destCluster],
  );
  const placementReady = !placements || placements.length === 0 || (!!destCluster && !!destStorage);
  const selectedRefs = useMemo(() => Object.keys(selected).filter((k) => selected[k]), [selected]);

  const chooseSource = async (id: string) => {
    setSourceId(id); setVms(null); setSelected({}); setSourceNets(null); setNetworkMap({});
    setDestId(""); setPlacements(null); setDestCluster(""); setDestStorage(""); setDestNets(null);
    if (!id) return;
    try { setVms(await api.listVms(id)); } catch (e) { setErr(String((e as Error).message)); }
  };

  const chooseDest = async (id: string) => {
    setDestId(id); setDestCluster(""); setDestStorage(""); setPlacements(null); setDestNets(null);
    setSourceNets(null); setNetworkMap({});
    if (!id) return;
    try {
      const [ps, nets] = await Promise.all([api.listPlacements(id), api.listNetworks(id)]);
      setPlacements(ps); setDestNets(nets);
      if (ps.length === 1) {
        setDestCluster(ps[0].cluster.id);
        if (ps[0].storage.length === 1) setDestStorage(ps[0].storage[0].id);
      }
    } catch (e) { setErr(String((e as Error).message)); }
  };

  // Gather the union of source networks across the selected VMs, then seed the map.
  const loadSourceNetworks = async () => {
    if (!sourceId || selectedRefs.length === 0) return;
    setBusy(true); setErr(null);
    try {
      const specs = await Promise.all(
        selectedRefs.map((ref) => api.getVmSpec(sourceId, ref).catch(() => null)),
      );
      const nets = new Set<string>();
      for (const sp of specs as (VmSpec | null)[]) {
        for (const nic of sp?.nics ?? []) nets.add(nic.source_network);
      }
      const list = [...nets];
      setSourceNets(list);
      setNetworkMap((prev) => {
        const seed: Record<string, string> = {};
        for (const n of list) seed[n] = prev[n] ?? destNets?.[0]?.id ?? "";
        return seed;
      });
    } catch (e) { setErr(String((e as Error).message)); }
    finally { setBusy(false); }
  };

  // Auto-load source networks whenever the selection (or destination) changes, so
  // network mapping is always presented and can be enforced before submit.
  useEffect(() => {
    if (selectedRefs.length > 0 && destNets) {
      loadSourceNetworks();
    } else {
      setSourceNets(null);
      setNetworkMap({});
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedRefs.join(","), destNets]);

  const submit = async () => {
    setBusy(true); setErr(null); setMsg(null);
    try {
      const options: Record<string, unknown> = { mode, force_stop: forceStop, power_on: powerOn };
      if (destCluster) options.dest_cluster = destCluster;
      if (destStorage) options.dest_storage = destStorage;
      if (destIsVsphere && convertToVmfs) options.convert_to_vmfs = true;
      const members = selectedRefs.map((ref) => ({
        source_hypervisor_id: sourceId,
        dest_hypervisor_id: destId,
        vm_ref: ref,
        network_map: networkMap,
        options: {},
      }));
      const r = await api.createMigrationGroup({
        name: name || undefined,
        members,
        scheduled_at: localToIso(scheduledAt),
        concurrency: Math.max(1, concurrency),
        continue_on_error: continueOnError,
        options,
      });
      setMsg(r.scheduled_at
        ? `Scheduled "${r.name}" for ${new Date(r.scheduled_at).toLocaleString()}.`
        : `Group "${r.name}" started (${selectedRefs.length} VMs).`);
      // Reset the builder.
      setName(""); setSelected({}); setSourceNets(null); setNetworkMap({}); setScheduledAt("");
      reload();
    } catch (e) { setErr(String((e as Error).message)); }
    finally { setBusy(false); }
  };

  // Network mapping is required: source networks must be inspected and every one
  // mapped to a destination network before the group can be created.
  const networksMapped = sourceNets !== null && sourceNets.every((n) => !!networkMap[n]);
  const canSubmit = !busy && sourceId && destId && selectedRefs.length > 0 && placementReady
    && networksMapped;

  return (
    <div>
      <div className="card">
        <h2 style={{ marginTop: 0 }}>New migration group</h2>
        <p className="muted" style={{ fontSize: 13 }}>
          Migrate several VMs from one source to one destination (same FlashArray) with a
          shared network map and options. Run now, or schedule for later. Members run up to
          <b> {Math.max(1, concurrency)}</b> at a time.
        </p>

        <label>Group name (optional)</label>
        <input value={name} onChange={(e) => setName(e.target.value)} placeholder="auto-generated if blank" />

        <div style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}>
          <div style={{ flex: 1, minWidth: 240 }}>
            <label>Source hypervisor *</label>
            <select value={sourceId} onChange={(e) => chooseSource(e.target.value)}>
              <option value="">Select…</option>
              {eligible.map((h) => <option key={h.id} value={h.id}>{h.name} ({h.connector_key})</option>)}
            </select>
            {sourceId && !source?.array_id && (
              <div className="error">This hypervisor has no associated FlashArray.</div>
            )}
          </div>
          <div style={{ flex: 1, minWidth: 240 }}>
            <label>Destination hypervisor *</label>
            <select value={destId} onChange={(e) => chooseDest(e.target.value)} disabled={!sourceId}>
              <option value="">Select…</option>
              {destChoices.map((h) => <option key={h.id} value={h.id}>{h.name} ({h.connector_key})</option>)}
            </select>
          </div>
        </div>

        {placements && placements.length > 0 && (
          <div style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}>
            <div style={{ flex: 1, minWidth: 240 }}>
              <label>Cluster *</label>
              <select value={destCluster} onChange={(e) => { setDestCluster(e.target.value); setDestStorage(""); }}>
                <option value="">Select…</option>
                {placements.map((p) => <option key={p.cluster.id} value={p.cluster.id}>{p.cluster.name}</option>)}
              </select>
            </div>
            <div style={{ flex: 1, minWidth: 240 }}>
              <label>Storage *</label>
              <select value={destStorage} disabled={!destCluster} onChange={(e) => setDestStorage(e.target.value)}>
                <option value="">Select…</option>
                {clusterStorage.map((s) => <option key={s.id} value={s.id}>{s.name} ({s.kind})</option>)}
              </select>
            </div>
          </div>
        )}

        {sourceId && (
          <div style={{ marginTop: 12 }}>
            <label>VMs to migrate * ({selectedRefs.length} selected)</label>
            <div style={{ maxHeight: 220, overflow: "auto", border: "1px solid var(--border)", borderRadius: 6, padding: 8 }}>
              {vms === null && <div className="muted">Loading…</div>}
              {vms?.length === 0 && <div className="muted">No VMs found.</div>}
              {(vms ?? []).map((v) => (
                <label key={v.id} style={{ display: "flex", alignItems: "center", gap: 8, margin: "2px 0" }}>
                  <input type="checkbox" style={{ width: "auto" }}
                    checked={!!selected[v.id]}
                    onChange={(e) => setSelected((s) => ({ ...s, [v.id]: e.target.checked }))} />
                  <span>{v.name} <span className="muted">— {v.power_state} ({v.id})</span></span>
                </label>
              ))}
            </div>
          </div>
        )}

        {selectedRefs.length > 0 && destNets && (
          <div style={{ marginTop: 12 }}>
            <label>Map each source network to a destination network *</label>
            {sourceNets === null ? (
              <div className="muted">Inspecting selected VMs…</div>
            ) : sourceNets.length === 0 ? (
              <div className="muted" style={{ fontSize: 13 }}>
                The selected VMs have no network interfaces — nothing to map.
              </div>
            ) : (
              <>
                <table>
                  <thead><tr><th>Source network</th><th>Destination network</th></tr></thead>
                  <tbody>
                    {sourceNets.map((sn) => (
                      <tr key={sn}>
                        <td>{sn}</td>
                        <td>
                          <select value={networkMap[sn] ?? ""}
                            onChange={(e) => setNetworkMap((m) => ({ ...m, [sn]: e.target.value }))}>
                            <option value="">Select…</option>
                            {destNets.map((n) => <option key={n.id} value={n.id}>{n.name} ({n.id})</option>)}
                          </select>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
                {!networksMapped && (
                  <div className="muted" style={{ fontSize: 12 }}>
                    Every source network must be mapped before the group can be created.
                  </div>
                )}
              </>
            )}
          </div>
        )}

        <h2>Options</h2>
        <div style={{ display: "flex", gap: 6, marginBottom: 8 }}>
          <label style={{ display: "flex", alignItems: "center", gap: 6, margin: 0 }}>
            <input type="radio" name="gmode" style={{ width: "auto" }}
              checked={mode === "move"} onChange={() => setMode("move")} /> <span><b>Move</b> (re-point volumes, remove source)</span>
          </label>
          <label style={{ display: "flex", alignItems: "center", gap: 6, margin: "0 0 0 16px" }}>
            <input type="radio" name="gmode" style={{ width: "auto" }}
              checked={mode === "copy"} onChange={() => setMode("copy")} /> <span><b>Copy</b> (clone volumes, keep source)</span>
          </label>
        </div>
        {mode === "move" && (
          <label style={{ display: "flex", alignItems: "center", gap: 8, margin: "0 0 6px" }}>
            <input type="checkbox" style={{ width: "auto" }} checked={forceStop} onChange={(e) => setForceStop(e.target.checked)} />
            <span>Force power off the source if it won't shut down gracefully (move powers off the source first)</span>
          </label>
        )}
        <label style={{ display: "flex", alignItems: "center", gap: 8, margin: "0 0 6px" }}>
          <input type="checkbox" style={{ width: "auto" }} checked={powerOn} onChange={(e) => setPowerOn(e.target.checked)} />
          <span>Power on each destination VM when its migration completes</span>
        </label>
        {destIsVsphere && (
          <label style={{ display: "flex", alignItems: "center", gap: 8, margin: "0 0 6px" }}>
            <input type="checkbox" style={{ width: "auto" }} checked={convertToVmfs} onChange={(e) => setConvertToVmfs(e.target.checked)} />
            <span><b>Convert disks to VMFS VMDKs</b> — after copying, Storage vMotion each disk
              onto a FlashArray-backed VMFS datastore (converting the RDMs to native VMDKs) and
              clean up the temporary volumes. Leave unchecked to keep disks as RDMs.</span>
          </label>
        )}

        <div style={{ display: "flex", gap: 16, marginTop: 12, flexWrap: "wrap" }}>
          <div>
            <label>Schedule (optional — blank = run now)</label>
            <input type="datetime-local" value={scheduledAt} onChange={(e) => setScheduledAt(e.target.value)} />
          </div>
          <div>
            <label>Concurrency</label>
            <input type="number" min={1} max={16} value={concurrency}
              onChange={(e) => setConcurrency(parseInt(e.target.value, 10) || 1)} style={{ width: 80 }} />
          </div>
          <div style={{ display: "flex", alignItems: "flex-end", paddingBottom: 6 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <input type="checkbox" style={{ width: "auto" }} checked={continueOnError}
                onChange={(e) => setContinueOnError(e.target.checked)} />
              <span>Continue if a member fails</span>
            </label>
          </div>
        </div>

        <div style={{ marginTop: 14 }}>
          <button disabled={!canSubmit} onClick={submit}>
            {busy ? "Working…" : scheduledAt ? "Schedule group" : `Create & run group (${selectedRefs.length} VMs)`}
          </button>
        </div>
        {err && <div className="error">{err}</div>}
        {msg && <div className="muted" style={{ marginTop: 8, color: "var(--ok, #2e7d32)" }}>{msg}</div>}
      </div>

      <div className="card" style={{ marginTop: 16 }}>
        <h2 style={{ marginTop: 0 }}>Migration groups</h2>
        {groups === null && <div className="muted">Loading…</div>}
        {groups?.length === 0 && <div className="muted">No migration groups yet.</div>}
        {groups && groups.length > 0 && (
          <table>
            <thead>
              <tr><th>Name</th><th>Status</th><th>Members</th><th>Scheduled</th><th>Actions</th></tr>
            </thead>
            <tbody>
              {groups.map((g) => <GroupRow key={g.id} g={g} onChanged={reload} />)}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
