import { Fragment, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type {
  ClusterAssessment,
  ConnectorDescriptor,
  Hypervisor,
} from "../api/client";
import { DynamicForm, LiveLog, StatusBadge, useAsync } from "../components";

// True if the connector advertises the cluster reconcile capability/actions.
function supportsReconcile(descriptor?: ConnectorDescriptor): boolean {
  return Boolean(
    descriptor?.actions.some((a) => a.id === "reconcile_cluster"),
  );
}

// Cluster membership panel: shows new/departed hosts from state.cluster_assessment
// and runs assess_cluster / reconcile_cluster, streaming the job log.
function ClusterPanel({ hv, onReload }: { hv: Hypervisor; onReload: () => void }) {
  const [jobId, setJobId] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);

  const assessment = (hv.state?.cluster_assessment ?? {}) as ClusterAssessment;
  const newHosts = assessment.new_hosts ?? [];
  const readyHosts = newHosts.filter((h) => h.ready);
  const notReady = [...(assessment.not_ready ?? []), ...newHosts.filter((h) => !h.ready)];
  const departed = assessment.departed_hosts ?? [];
  const pendingRemovals = (hv.state?.pending_removals as string[] | undefined) ?? [];
  const removable = Array.from(new Set([...departed, ...pendingRemovals]));

  const run = async (action_id: string, params: Record<string, unknown>, key: string) => {
    setErr(null);
    setBusy(key);
    try {
      const { job_id } = await api.runOperation(hv.id, { action_id, params });
      setJobId(job_id);
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(null);
    }
  };

  const removeDeparted = () => {
    if (
      !window.confirm(
        `Remove ${removable.length} departed host(s) from the FlashArray? ` +
          "This deletes the FA host(s) and host-group membership and cannot be undone.",
      )
    )
      return;
    run("reconcile_cluster", { apply_removals: true }, "remove");
  };

  return (
    <div style={{ background: "var(--panel-2)", borderRadius: 8, padding: 14, marginTop: 8 }}>
      <div className="toolbar">
        <strong>Cluster membership</strong>
        <button
          className="secondary"
          disabled={busy !== null}
          onClick={() => run("assess_cluster", {}, "assess")}
        >
          {busy === "assess" ? "Checking…" : "Check now"}
        </button>
      </div>

      <div style={{ marginTop: 10 }}>
        <div className="muted" style={{ fontSize: 12, marginBottom: 4 }}>New hosts</div>
        {newHosts.length === 0 && (
          <span className="muted" style={{ fontSize: 13 }}>None detected.</span>
        )}
        {newHosts.map((h) => (
          <div key={h.node} style={{ fontSize: 13, marginBottom: 4 }}>
            <span className={h.ready ? "status-deployed" : "status-error"}>
              {h.ready ? "✓ ready" : "✗ not ready"}
            </span>{" "}
            <strong>{h.node}</strong>{" "}
            <span className="muted">({h.host})</span>
            {!h.ready && h.reasons?.length > 0 && (
              <div className="muted" style={{ fontSize: 12, marginLeft: 18 }}>
                {h.reasons.join("; ")}
              </div>
            )}
          </div>
        ))}
        {readyHosts.length > 0 && (
          <button
            disabled={busy !== null}
            style={{ marginTop: 6 }}
            onClick={() => run("reconcile_cluster", { apply_removals: false }, "deploy")}
          >
            {busy === "deploy" ? "Deploying…" : `Deploy to new hosts (${readyHosts.length})`}
          </button>
        )}
        {readyHosts.length === 0 && notReady.length > 0 && (
          <p className="muted" style={{ fontSize: 12 }}>
            No ready hosts to deploy; resolve the readiness issues above first.
          </p>
        )}
      </div>

      {removable.length > 0 && (
        <div style={{ marginTop: 12 }}>
          <div className="muted" style={{ fontSize: 12, marginBottom: 4 }}>
            Departed hosts (flagged for removal)
          </div>
          {removable.map((h) => (
            <div key={h} style={{ fontSize: 13 }}>
              <span className="status-error">✗</span> {h}
            </div>
          ))}
          <button
            className="danger"
            disabled={busy !== null}
            style={{ marginTop: 6 }}
            onClick={removeDeparted}
          >
            {busy === "remove" ? "Removing…" : "Remove departed hosts"}
          </button>
        </div>
      )}

      {err && <div className="error">{err}</div>}
      {jobId && (
        <div style={{ marginTop: 12 }}>
          <LiveLog jobId={jobId} onDone={onReload} />
        </div>
      )}
    </div>
  );
}

export default function HypervisorsPage() {
  const connectors = useAsync(() => api.listConnectors());
  const arrays = useAsync(() => api.listArrays());
  const hvs = useAsync(() => api.listHypervisors());

  const [connectorKey, setConnectorKey] = useState("");
  const [name, setName] = useState("");
  const [arrayId, setArrayId] = useState("");
  const [secrets, setSecrets] = useState<Record<string, unknown>>({});
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [validateMsg, setValidateMsg] = useState<Record<string, string>>({});
  const [expanded, setExpanded] = useState<string | null>(null);

  // --- edit state ---
  const [editId, setEditId] = useState<string | null>(null);
  const [editName, setEditName] = useState("");
  const [editArrayId, setEditArrayId] = useState("");
  const [editValues, setEditValues] = useState<Record<string, unknown>>({});
  const [editBusy, setEditBusy] = useState(false);
  const [editErr, setEditErr] = useState<string | null>(null);

  const editingHv = hvs.data?.find((h) => h.id === editId);
  const editDescriptor = connectors.data?.find((c) => c.key === editingHv?.connector_key);

  const startEdit = (h: Hypervisor) => {
    setEditId(h.id);
    setEditName(h.name);
    setEditArrayId(h.array_id ?? "");
    setEditValues({ ...h.connection }); // non-secret fields prefilled; secrets stay blank
    setEditErr(null);
    setConnectorKey(""); // close the add panel if open
  };

  const removeHypervisor = (h: Hypervisor) => {
    if (
      !window.confirm(
        `Remove hypervisor "${h.name || h.id}" from PHIF?\n\n` +
          "This deletes its PHIF record and stored credentials. It does NOT " +
          "uninstall the deployed driver/plugin or touch any FlashArray data. " +
          "This cannot be undone.",
      )
    )
      return;
    api.deleteHypervisor(h.id).then(() => hvs.reload());
  };

  const saveEdit = async () => {
    if (!editingHv || !editDescriptor) return;
    setEditBusy(true);
    setEditErr(null);
    // Preserve any extra connection keys (e.g. interface_binding) the connector stored.
    const connection: Record<string, unknown> = { ...editingHv.connection };
    const secretBundle: Record<string, unknown> = {};
    for (const f of editDescriptor.target_schema) {
      const v = editValues[f.name];
      if (f.type === "secret") {
        if (v) secretBundle[f.name] = v; // blank = keep existing secret
      } else if (v !== undefined && v !== "") {
        connection[f.name] = v;
      }
    }
    try {
      await api.updateHypervisor(editingHv.id, {
        name: editName,
        connection,
        secrets: secretBundle,
        array_id: editArrayId || null,
      });
      setEditId(null);
      hvs.reload();
    } catch (e) {
      setEditErr(String((e as Error).message));
    } finally {
      setEditBusy(false);
    }
  };

  const selected: ConnectorDescriptor | undefined = useMemo(
    () => connectors.data?.find((c) => c.key === connectorKey),
    [connectors.data, connectorKey],
  );

  // Split target fields into non-secret "connection" and secret "secrets".
  const submit = async () => {
    if (!selected) return;
    setBusy(true);
    setErr(null);
    const connection: Record<string, unknown> = {};
    const secretBundle: Record<string, unknown> = {};
    for (const f of selected.target_schema) {
      const v = secrets[f.name];
      if (v === undefined || v === "") continue;
      if (f.type === "secret") secretBundle[f.name] = v;
      else connection[f.name] = v;
    }
    try {
      await api.addHypervisor({
        name,
        connector_key: connectorKey,
        connection,
        secrets: secretBundle,
        array_id: arrayId || null,
      });
      setName("");
      setSecrets({});
      setConnectorKey("");
      hvs.reload();
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  const validate = async (id: string) => {
    setValidateMsg({ ...validateMsg, [id]: "validating…" });
    try {
      const r = await api.validateHypervisor(id);
      setValidateMsg({ ...validateMsg, [id]: r.success ? `✓ ${r.message}` : `✗ ${r.message}` });
    } catch (e) {
      setValidateMsg({ ...validateMsg, [id]: `✗ ${(e as Error).message}` });
    }
    // Refresh the list so the status badge reflects any server-side change (e.g.
    // a deploy elsewhere flipped not_deployed/error → deployed).
    hvs.reload();
  };

  const toggleMonitoring = async (h: Hypervisor) => {
    const currentlyDisabled = Boolean(h.state?.monitoring_disabled);
    try {
      await api.setHypervisorMonitoring(h.id, currentlyDisabled); // enable if disabled
      hvs.reload();
    } catch (e) {
      setValidateMsg({ ...validateMsg, [h.id]: `✗ ${(e as Error).message}` });
    }
  };

  return (
    <div>
      <h1>Hypervisors</h1>
      <p className="subtitle">Connect a hypervisor, then install its Everpure integration and run day-2 operations.</p>

      <h2>Available integrations</h2>
      <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fill,minmax(250px,1fr))", gap: 12 }}>
        {(connectors.data ?? []).map((c) => (
          <div className="card" key={c.key} style={{ cursor: "pointer", margin: 0 }} onClick={() => setConnectorKey(c.key)}>
            <div className="toolbar">
              <strong>{c.name}</strong>
              <span className={`badge ${c.maturity}`}>{c.maturity}</span>
            </div>
            <p className="muted" style={{ fontSize: 13 }}>{c.description}</p>
            <div>
              {c.capabilities.slice(0, 6).map((cap) => (
                <span className="cap" key={cap}>{cap}</span>
              ))}
              {c.capabilities.length > 6 && <span className="cap">+{c.capabilities.length - 6}</span>}
            </div>
          </div>
        ))}
      </div>

      {selected && (
        <div className="card" style={{ marginTop: 20, borderColor: "var(--accent)" }}>
          <h2 style={{ marginTop: 0 }}>Connect {selected.name}</h2>
          <label>Name *</label>
          <input value={name} onChange={(e) => setName(e.target.value)} />
          <label>Associated FlashArray</label>
          <select value={arrayId} onChange={(e) => setArrayId(e.target.value)}>
            <option value="">None</option>
            {(arrays.data ?? []).map((a) => (
              <option key={a.id} value={a.id}>{a.name}</option>
            ))}
          </select>
          <DynamicForm fields={selected.target_schema} values={secrets} onChange={setSecrets} />
          <div style={{ marginTop: 14 }}>
            <button disabled={busy || !name} onClick={submit}>
              {busy ? "Adding…" : "Add hypervisor"}
            </button>{" "}
            <button className="secondary" onClick={() => setConnectorKey("")}>Cancel</button>
          </div>
          {err && <div className="error">{err}</div>}
        </div>
      )}

      {editingHv && editDescriptor && (
        <div className="card" style={{ marginTop: 20, borderColor: "var(--accent)" }}>
          <h2 style={{ marginTop: 0 }}>Edit {editingHv.name} ({editingHv.connector_key})</h2>
          <label>Name *</label>
          <input value={editName} onChange={(e) => setEditName(e.target.value)} />
          <label>Associated FlashArray</label>
          <select value={editArrayId} onChange={(e) => setEditArrayId(e.target.value)}>
            <option value="">None</option>
            {(arrays.data ?? []).map((a) => (
              <option key={a.id} value={a.id}>{a.name}</option>
            ))}
          </select>
          <DynamicForm fields={editDescriptor.target_schema} values={editValues} onChange={setEditValues} />
          <p className="muted" style={{ fontSize: 12 }}>Leave secret fields blank to keep the existing value.</p>
          <div style={{ marginTop: 14 }}>
            <button disabled={editBusy || !editName} onClick={saveEdit}>
              {editBusy ? "Saving…" : "Save changes"}
            </button>{" "}
            <button className="secondary" onClick={() => setEditId(null)}>Cancel</button>
          </div>
          {editErr && <div className="error">{editErr}</div>}
        </div>
      )}

      <div className="toolbar">
        <h2 style={{ marginBottom: 0 }}>Connected hypervisors</h2>
        <button className="secondary" onClick={() => hvs.reload()}>Refresh</button>
      </div>
      <div className="card" style={{ padding: 0 }}>
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Type</th>
              <th>Status</th>
              <th>Monitoring</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {(hvs.data ?? []).map((h) => {
              const descriptor = connectors.data?.find((c) => c.key === h.connector_key);
              const canReconcile = supportsReconcile(descriptor);
              const monitoringOn = !h.state?.monitoring_disabled;
              return (
                <Fragment key={h.id}>
                  <tr>
                    <td>{h.name}</td>
                    <td className="muted">{h.connector_key}</td>
                    <td>
                      <StatusBadge status={h.status} />
                      {validateMsg[h.id] && <div className="muted" style={{ fontSize: 12 }}>{validateMsg[h.id]}</div>}
                    </td>
                    <td>
                      <label style={{ display: "inline-flex", gap: 6, alignItems: "center", margin: 0, fontSize: 13, color: "var(--text)" }}>
                        <input
                          type="checkbox"
                          style={{ width: "auto" }}
                          checked={monitoringOn}
                          onChange={() => toggleMonitoring(h)}
                        />
                        {monitoringOn ? "on" : "off"}
                      </label>
                    </td>
                    <td style={{ textAlign: "right" }}>
                      {canReconcile && (
                        <>
                          <button
                            className="secondary"
                            onClick={() => setExpanded(expanded === h.id ? null : h.id)}
                          >
                            {expanded === h.id ? "Hide cluster" : "Cluster"}
                          </button>{" "}
                        </>
                      )}
                      <button className="secondary" onClick={() => validate(h.id)}>Validate</button>{" "}
                      <button className="secondary" onClick={() => startEdit(h)}>Edit</button>{" "}
                      <Link to={`/operations/${h.id}`}><button className="secondary">Operations</button></Link>{" "}
                      <button className="danger" onClick={() => removeHypervisor(h)}>
                        Remove
                      </button>
                    </td>
                  </tr>
                  {canReconcile && expanded === h.id && (
                    <tr>
                      <td colSpan={5} style={{ background: "var(--panel)" }}>
                        <ClusterPanel hv={h} onReload={() => hvs.reload()} />
                      </td>
                    </tr>
                  )}
                </Fragment>
              );
            })}
            {hvs.data?.length === 0 && (
              <tr><td colSpan={5} className="muted">No hypervisors connected yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
