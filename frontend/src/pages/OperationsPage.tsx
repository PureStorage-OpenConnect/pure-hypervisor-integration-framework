import { useEffect, useMemo, useRef, useState } from "react";
import { useParams } from "react-router-dom";
import { api } from "../api/client";
import type { ActionSpec, ConnectorDescriptor } from "../api/client";
import { DynamicForm, useAsync } from "../components";

// Live log viewer: streams a job's logs over WebSocket.
function JobLogs({ jobId, onDone }: { jobId: string; onDone?: () => void }) {
  const [lines, setLines] = useState<string[]>([]);
  const [done, setDone] = useState(false);
  const boxRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setLines([]);
    setDone(false);
    const ws = api.jobLogSocket(jobId);
    ws.onmessage = (ev) => {
      if (ev.data === "__END__") {
        setDone(true);
        onDone?.(); // refresh so the action's persisted last-run state reloads
        ws.close();
        return;
      }
      setLines((prev) => [...prev, ev.data as string]);
    };
    ws.onerror = () => setDone(true);
    return () => ws.close();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [jobId]);

  useEffect(() => {
    boxRef.current?.scrollTo(0, boxRef.current.scrollHeight);
  }, [lines]);

  return (
    <div>
      <div className="toolbar">
        <h2 style={{ margin: 0 }}>Job log</h2>
        <span className="muted">{done ? "finished" : "streaming…"}</span>
      </div>
      <div className="logs" ref={boxRef}>
        {lines.join("\n") || "Waiting for output…"}
      </div>
    </div>
  );
}

export default function OperationsPage() {
  const { hypervisorId } = useParams();
  const hvs = useAsync(() => api.listHypervisors());
  const [selectedHv, setSelectedHv] = useState<string>(hypervisorId ?? "");
  const [descriptor, setDescriptor] = useState<ConnectorDescriptor | null>(null);
  const [activeAction, setActiveAction] = useState<ActionSpec | null>(null);
  const [params, setParams] = useState<Record<string, unknown>>({});
  const [jobId, setJobId] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    if (hypervisorId) setSelectedHv(hypervisorId);
  }, [hypervisorId]);

  const hv = useMemo(() => hvs.data?.find((h) => h.id === selectedHv), [hvs.data, selectedHv]);

  useEffect(() => {
    setDescriptor(null);
    setActiveAction(null);
    setJobId(null);
    if (hv) api.getConnector(hv.connector_key).then(setDescriptor).catch(() => setDescriptor(null));
    // Key on the hypervisor's id, NOT the hv object: when a job finishes its
    // onDone fires hvs.reload(), which hands back a new hv object (same id). If
    // this effect depended on `hv` it would re-run on that reload and clear the
    // active action + job log, hiding the results. Keying on id resets only when
    // the operator actually switches hypervisors.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [hv?.id]);

  const run = async () => {
    if (!hv || !activeAction) return;
    setErr(null);
    try {
      const { job_id } = await api.runOperation(hv.id, { action_id: activeAction.id, params });
      setJobId(job_id);
    } catch (e) {
      setErr(String((e as Error).message));
    }
  };

  // Pre-fill an action's params from the hypervisor's stored connection so values
  // like host_group / storage_id carry into the form instead of showing blank.
  const prefill = (a: ActionSpec): Record<string, unknown> => {
    const conn = (hv?.connection ?? {}) as Record<string, unknown>;
    const init: Record<string, unknown> = {};
    for (const f of a.fields) {
      const v = conn[f.name];
      if (v !== undefined && v !== null && v !== "") init[f.name] = v;
    }
    return init;
  };

  const selectAction = async (a: ActionSpec) => {
    setActiveAction(a);
    setJobId(null);
    // Pre-load order: connection defaults, then this action's last-run values
    // (persisted in hv.state) which win — so forms reflect what was configured
    // last time. Discoverable inputs then fill any still-empty fields.
    const lastRun = (hv?.state?.[a.id] as Record<string, unknown> | undefined) ?? {};
    const init = { ...prefill(a), ...lastRun };
    setParams(init);
    // If the action has node-initiator fields, discover them from the node and
    // display the values. (Array-side fields like iqn_target/subsystem_nqn are
    // NOT here — they auto-fill via their options_source discoverable input.)
    const NODE_INIT_FIELDS = new Set([
      "node_iqn", "node_nqn", "node_wwns", "iqns", "nqns", "wwns",
    ]);
    if (hv && a.fields.some((f) => NODE_INIT_FIELDS.has(f.name))) {
      try {
        const r = await api.discoverOptions(hv.id, "initiators");
        // Merge into the latest params functionally so we don't clobber values
        // that discoverable inputs (e.g. host_name) auto-filled meanwhile.
        setParams((prev) => {
          const merged = { ...prev };
          for (const o of r.options) {
            const field = o.field as string | undefined;
            if (field && a.fields.some((f) => f.name === field) && !merged[field]) {
              merged[field] = o.value;
            }
          }
          return merged;
        });
      } catch {
        /* discovery is best-effort; leave fields editable */
      }
    }
  };

  return (
    <div>
      <h1>Operations</h1>
      <p className="subtitle">Deploy integrations and run day-2 operations. Available actions are driven by each connector's declared capabilities.</p>

      <div className="card">
        <label>Hypervisor</label>
        <select value={selectedHv} onChange={(e) => { setSelectedHv(e.target.value); setActiveAction(null); setJobId(null); }}>
          <option value="">Select…</option>
          {(hvs.data ?? []).map((h) => (
            <option key={h.id} value={h.id}>{h.name} ({h.connector_key})</option>
          ))}
        </select>
      </div>

      {descriptor && hv && (
        <div className="card">
          <h2 style={{ marginTop: 0 }}>{descriptor.name} actions</h2>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 8 }}>
            {descriptor.actions.map((a) => (
              <button
                key={a.id}
                className={activeAction?.id === a.id ? "" : "secondary"}
                onClick={() => selectAction(a)}
              >
                {a.label}
              </button>
            ))}
          </div>

          {activeAction && (
            <div style={{ marginTop: 18 }}>
              <p className="muted">{activeAction.description}</p>
              <DynamicForm
                fields={activeAction.fields}
                values={params}
                onChange={setParams}
                onDiscover={(kind) => api.discoverOptions(hv.id, kind).then((r) => r.options)}
              />
              <div style={{ marginTop: 14 }}>
                <button className={activeAction.destructive ? "danger" : ""} onClick={run}>
                  {activeAction.destructive ? "Confirm: " : "Run: "}{activeAction.label}
                </button>
              </div>
              {err && <div className="error">{err}</div>}
              {/* The job log streams directly below the action being run. */}
              {jobId && (
                <div style={{ marginTop: 16, borderTop: "1px solid var(--border)", paddingTop: 12 }}>
                  <JobLogs jobId={jobId} onDone={() => hvs.reload()} />
                </div>
              )}
            </div>
          )}
        </div>
      )}

    </div>
  );
}
