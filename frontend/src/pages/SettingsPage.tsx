import { useEffect, useState } from "react";
import { api } from "../api/client";
import { useAsync } from "../components";

export default function SettingsPage() {
  const settings = useAsync(() => api.getSettings());

  const [enabled, setEnabled] = useState(false);
  const [interval, setInterval] = useState(300);
  const [busy, setBusy] = useState(false);
  const [msg, setMsg] = useState<string | null>(null);
  const [err, setErr] = useState<string | null>(null);

  // Sync local form state when the settings load/reload.
  useEffect(() => {
    if (settings.data) {
      setEnabled(settings.data.monitoring.enabled);
      setInterval(settings.data.monitoring.interval_seconds);
    }
  }, [settings.data]);

  const save = async () => {
    setBusy(true);
    setErr(null);
    setMsg(null);
    try {
      await api.patchMonitoring({ enabled, interval_seconds: interval });
      setMsg("Saved.");
      settings.reload();
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  return (
    <div>
      <h1>Settings</h1>
      <p className="subtitle">Global PHIF configuration.</p>

      <div className="card">
        <h2 style={{ marginTop: 0 }}>Cluster monitoring</h2>
        <p className="muted" style={{ fontSize: 13 }}>
          When enabled, PHIF periodically assesses each hypervisor's cluster
          membership and records new/departed hosts so they can be reconciled.
          Individual hypervisors can opt out from the Hypervisors page.
        </p>
        {settings.error && <div className="error">{settings.error}</div>}
        <label style={{ display: "flex", gap: 8, alignItems: "center", marginTop: 12 }}>
          <input
            type="checkbox"
            style={{ width: "auto" }}
            checked={enabled}
            onChange={(e) => setEnabled(e.target.checked)}
          />
          Enable background monitoring
        </label>
        <label>Interval (seconds)</label>
        <input
          type="number"
          min={30}
          value={interval}
          onChange={(e) => setInterval(Number(e.target.value))}
          style={{ maxWidth: 200 }}
        />
        <div style={{ marginTop: 14 }}>
          <button disabled={busy} onClick={save}>
            {busy ? "Saving…" : "Save"}
          </button>
          {msg && (
            <span className="status-succeeded" style={{ fontSize: 13, marginLeft: 10 }}>
              {msg}
            </span>
          )}
        </div>
        {err && <div className="error">{err}</div>}
      </div>
    </div>
  );
}
