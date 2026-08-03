import { useState } from "react";
import { api } from "../api/client";
import { useAsync } from "../components";

export default function ArraysPage() {
  const { data: arrays, error, reload } = useAsync(() => api.listArrays());
  const [form, setForm] = useState({
    name: "",
    mgmt_endpoint: "",
    api_token: "",
    verify_ssl: false,
  });
  const [busy, setBusy] = useState(false);
  const [formError, setFormError] = useState<string | null>(null);
  const [validateMsg, setValidateMsg] = useState<Record<string, string>>({});
  const [openArray, setOpenArray] = useState<string | null>(null);
  const [conns, setConns] = useState<Record<string, { name: string; management_address: string; status: string; type: string }[]>>({});
  const [connErr, setConnErr] = useState<Record<string, string>>({});

  const loadConns = async (id: string) => {
    setConnErr((e) => ({ ...e, [id]: "" }));
    try {
      const r = await api.listArrayConnections(id);
      setConns((c) => ({ ...c, [id]: r.connections }));
    } catch (e) {
      setConnErr((er) => ({ ...er, [id]: String((e as Error).message) }));
    }
  };

  const toggleConns = (id: string) => {
    if (openArray === id) { setOpenArray(null); return; }
    setOpenArray(id);
    loadConns(id);
  };

  const removeConn = async (id: string, name: string) => {
    try {
      await api.deleteArrayConnection(id, name);
      await loadConns(id);
    } catch (e) {
      setConnErr((er) => ({ ...er, [id]: String((e as Error).message) }));
    }
  };

  const validateArray = async (id: string) => {
    setValidateMsg((m) => ({ ...m, [id]: "validating…" }));
    try {
      const a = await api.validateArray(id);
      const info = a.info || {};
      const detail = [info.name, info.model, info.version].filter(Boolean).join(" ");
      setValidateMsg((m) => ({ ...m, [id]: `✓ connected${detail ? ` — ${detail}` : ""}` }));
      reload();
    } catch (e) {
      setValidateMsg((m) => ({ ...m, [id]: `✗ ${(e as Error).message}` }));
    }
  };

  const submit = async () => {
    setBusy(true);
    setFormError(null);
    try {
      await api.addArray(form);
      setForm({ name: "", mgmt_endpoint: "", api_token: "", verify_ssl: false });
      reload();
    } catch (e) {
      setFormError(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <h1>FlashArrays</h1>
      <p className="subtitle">Connect Everpure FlashArrays. Credentials are validated, then stored encrypted in the vault.</p>

      <div className="card">
        <h2 style={{ marginTop: 0 }}>Connect an array</h2>
        <div className="row">
          <div>
            <label>Name *</label>
            <input value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
          </div>
          <div>
            <label>Management IP / FQDN *</label>
            <input
              value={form.mgmt_endpoint}
              placeholder="192.0.2.10"
              onChange={(e) => setForm({ ...form, mgmt_endpoint: e.target.value })}
            />
          </div>
        </div>
        <label>API token *</label>
        <input
          type="password"
          value={form.api_token}
          onChange={(e) => setForm({ ...form, api_token: e.target.value })}
        />
        <label style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 12 }}>
          <input
            type="checkbox"
            style={{ width: "auto" }}
            checked={form.verify_ssl}
            onChange={(e) => setForm({ ...form, verify_ssl: e.target.checked })}
          />
          Verify TLS certificate
        </label>
        <div style={{ marginTop: 14 }}>
          <button disabled={busy || !form.name || !form.mgmt_endpoint} onClick={submit}>
            {busy ? "Connecting…" : "Connect & validate"}
          </button>
        </div>
        {formError && <div className="error">{formError}</div>}
      </div>

      <h2>Connected arrays</h2>
      {error && <div className="error">{error}</div>}
      <div className="card" style={{ padding: 0 }}>
        <table>
          <thead>
            <tr>
              <th>Name</th>
              <th>Endpoint</th>
              <th>Model / Version</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {(arrays ?? []).map((a) => (
              <tr key={a.id}>
                <td>{a.name}</td>
                <td>{a.mgmt_endpoint}</td>
                <td className="muted">
                  {String(a.info?.model ?? "—")} {String(a.info?.version ?? "")}
                </td>
                <td style={{ textAlign: "right" }}>
                  {validateMsg[a.id] && (
                    <span
                      className={validateMsg[a.id].startsWith("✗") ? "status-failed" : "status-succeeded"}
                      style={{ fontSize: 12, marginRight: 8 }}
                    >
                      {validateMsg[a.id]}
                    </span>
                  )}
                  <button className="secondary" onClick={() => validateArray(a.id)}>
                    Validate
                  </button>{" "}
                  <button className="secondary" onClick={() => toggleConns(a.id)}>
                    {openArray === a.id ? "Hide connections" : "Connections"}
                  </button>{" "}
                  <button className="danger" onClick={() => api.deleteArray(a.id).then(reload)}>
                    Remove
                  </button>
                </td>
              </tr>
            )).flatMap((row, i) => {
              const a = (arrays ?? [])[i];
              if (!a || openArray !== a.id) return [row];
              const list = conns[a.id] ?? [];
              return [row, (
                <tr key={a.id + "-conns"}>
                  <td colSpan={4} style={{ background: "var(--panel-2)" }}>
                    <b style={{ fontSize: 13 }}>Replication connections</b>
                    {connErr[a.id] && <div className="error">{connErr[a.id]}</div>}
                    {list.length === 0 && !connErr[a.id] && (
                      <div className="muted" style={{ fontSize: 13 }}>No replication connections.</div>
                    )}
                    {list.map((c) => (
                      <div key={c.name} style={{ display: "flex", alignItems: "center", gap: 10, padding: "4px 0" }}>
                        <code>{c.name || c.management_address}</code>
                        <span className="muted" style={{ fontSize: 12 }}>
                          {c.management_address} · {c.type} · {c.status}
                        </span>
                        <button className="danger" style={{ fontSize: 12, padding: "2px 8px" }}
                          onClick={() => removeConn(a.id, c.name)}>
                          Remove
                        </button>
                      </div>
                    ))}
                  </td>
                </tr>
              )];
            })}
            {arrays?.length === 0 && (
              <tr>
                <td colSpan={4} className="muted">
                  No arrays connected yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
