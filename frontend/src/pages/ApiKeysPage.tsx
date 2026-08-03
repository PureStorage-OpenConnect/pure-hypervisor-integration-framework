import { useState } from "react";
import { api } from "../api/client";
import { useAsync } from "../components";

const PURPOSES = [
  "openshift-csi",
  "openstack-cinder",
  "proxmox-plugin",
  "vsphere-plugin",
  "xcpng",
  "hpe-vme",
  "other",
];

export default function ApiKeysPage() {
  const arrays = useAsync(() => api.listArrays());
  const keys = useAsync(() => api.listApiKeys());
  const [form, setForm] = useState({
    array_id: "",
    array_user: "",
    purpose: PURPOSES[0],
    use_existing: true,
  });
  const [minted, setMinted] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const mint = async () => {
    setBusy(true);
    setErr(null);
    setMinted(null);
    try {
      const k = await api.mintApiKey(form);
      setMinted(k.token ?? null);
      keys.reload();
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <h1>API Keys</h1>
      <p className="subtitle">
        Generate FlashArray API tokens for integrations that authenticate to the array (CSI, Cinder, the
        Proxmox/vSphere plugins). The token is shown once, then stored encrypted.
      </p>

      <div className="card">
        <h2 style={{ marginTop: 0 }}>Provision a token</h2>
        <label style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 4 }}>
          <input
            type="checkbox"
            style={{ width: "auto" }}
            checked={form.use_existing}
            onChange={(e) => setForm({ ...form, use_existing: e.target.checked })}
          />
          Use the array's existing token (don't mint a new one)
        </label>
        <p className="muted" style={{ fontSize: 12, marginTop: 0 }}>
          {form.use_existing
            ? "Reuses the single token this array was connected with — no new array user/token is created."
            : "Mints a new token for the given array user (needs permission to create tokens on the array)."}
        </p>
        <div className="row">
          <div>
            <label>FlashArray *</label>
            <select value={form.array_id} onChange={(e) => setForm({ ...form, array_id: e.target.value })}>
              <option value="">Select…</option>
              {(arrays.data ?? []).map((a) => (
                <option key={a.id} value={a.id}>
                  {a.name}
                </option>
              ))}
            </select>
          </div>
          <div>
            <label>Array user {form.use_existing ? "" : "*"}</label>
            <input
              value={form.array_user}
              placeholder={form.use_existing ? "(not needed)" : "csi-user"}
              disabled={form.use_existing}
              onChange={(e) => setForm({ ...form, array_user: e.target.value })}
            />
          </div>
          <div>
            <label>Consumed by *</label>
            <select value={form.purpose} onChange={(e) => setForm({ ...form, purpose: e.target.value })}>
              {PURPOSES.map((p) => (
                <option key={p} value={p}>
                  {p}
                </option>
              ))}
            </select>
          </div>
        </div>
        <div style={{ marginTop: 14 }}>
          <button disabled={busy || !form.array_id || (!form.use_existing && !form.array_user)} onClick={mint}>
            {busy ? "Working…" : form.use_existing ? "Use existing token" : "Mint token"}
          </button>
        </div>
        {err && <div className="error">{err}</div>}
        {minted && (
          <div style={{ marginTop: 14 }}>
            <label>New token (copy it now — it won't be shown again)</label>
            <div className="token-box">{minted}</div>
          </div>
        )}
      </div>

      <h2>Issued tokens</h2>
      <div className="card" style={{ padding: 0 }}>
        <table>
          <thead>
            <tr>
              <th>Array user</th>
              <th>Consumed by</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {(keys.data ?? []).map((k) => (
              <tr key={k.id}>
                <td>{k.array_user}</td>
                <td>{k.purpose}</td>
                <td style={{ textAlign: "right" }}>
                  <button className="secondary" onClick={() => api.rotateApiKey(k.id).then(() => keys.reload())}>
                    Rotate
                  </button>{" "}
                  <button className="danger" onClick={() => api.deleteApiKey(k.id).then(() => keys.reload())}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
            {keys.data?.length === 0 && (
              <tr>
                <td colSpan={3} className="muted">
                  No tokens issued yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
