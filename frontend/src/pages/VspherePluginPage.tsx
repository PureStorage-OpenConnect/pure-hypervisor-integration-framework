import { useEffect, useRef, useState } from "react";
import { api, VspherePluginStatus } from "../api/client";

const POLL_MS = 5000;
// While a pull is running, poll faster so the progress bar actually moves.
const POLL_MS_INSTALLING = 1500;

type ContainerState = "running" | "exited" | "missing" | string;

function stateBadge(state: ContainerState) {
  const color =
    state === "running" ? "var(--green)" : state === "missing" ? "#888" : "var(--yellow)";
  return (
    <span style={{ color, fontWeight: 600 }}>
      {state === "missing" ? "not installed" : state}
    </span>
  );
}

function imageBadge(loaded: boolean) {
  return loaded ? (
    <span style={{ color: "var(--green)", fontWeight: 600 }}>present</span>
  ) : (
    <span style={{ color: "#888", fontWeight: 600 }}>not present</span>
  );
}

// pct: 0–100 = measured progress; null = indeterminate (no byte totals yet)
function ProgressBar({ pct }: { pct: number | null }) {
  const indeterminate = pct === null || pct >= 100;
  return (
    <div
      style={{
        marginTop: "0.75rem",
        background: "var(--panel-2)",
        borderRadius: 4,
        height: 8,
        overflow: "hidden",
        border: "1px solid var(--border)",
      }}
    >
      <div
        style={{
          height: "100%",
          background: "var(--accent)",
          borderRadius: 4,
          width: indeterminate ? "40%" : `${pct}%`,
          transition: indeterminate ? "none" : "width 0.2s ease",
          animation: indeterminate ? "progress-slide 1.4s ease-in-out infinite" : "none",
        }}
      />
    </div>
  );
}

export default function VspherePluginPage() {
  const [status, setStatus] = useState<VspherePluginStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState("");
  const [msg, setMsg] = useState("");
  const [showOffline, setShowOffline] = useState(false);

  // Upload state (offline install path)
  const [pluginFile, setPluginFile] = useState<File | null>(null);
  const [proxyFile, setProxyFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState(false);
  // null = indeterminate (docker loading phase), 0-100 = transfer %
  const [uploadPct, setUploadPct] = useState<number | null>(null);
  const [uploadLabel, setUploadLabel] = useState("");

  const pluginInputRef = useRef<HTMLInputElement>(null);
  const proxyInputRef = useRef<HTMLInputElement>(null);

  const install = status?.install;
  const installing = install?.active === true;

  const load = async () => {
    try {
      const s = await api.vspherePluginStatus();
      setStatus(s);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  // Re-arm the poll whenever the cadence changes so an in-flight install is
  // followed closely and an idle page stays cheap.
  useEffect(() => {
    load();
    const t = setInterval(load, installing ? POLL_MS_INSTALLING : POLL_MS);
    return () => clearInterval(t);
  }, [installing]);

  // Surface the outcome of a detached install once it finishes.
  const lastInstallDone = useRef<boolean>(false);
  useEffect(() => {
    if (!install) return;
    if (install.done && !lastInstallDone.current) {
      if (install.error) setErr(install.error);
      else setMsg("vSphere plugin installed and running.");
    }
    lastInstallDone.current = install.done;
  }, [install?.done, install?.error]);

  const run = async (action: () => Promise<VspherePluginStatus>, successMsg: string) => {
    setBusy(true);
    setErr("");
    setMsg("");
    try {
      const s = await action();
      setStatus(s);
      setMsg(successMsg);
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  const totalUploadBytes = (pluginFile?.size ?? 0) + (proxyFile?.size ?? 0);

  const handleUpload = async () => {
    if (!pluginFile && !proxyFile) {
      setErr("Select at least one tar file to upload.");
      return;
    }
    setUploading(true);
    setErr("");
    setMsg("");
    setUploadPct(0);
    setUploadLabel("Uploading…");
    try {
      const fd = new FormData();
      if (pluginFile) fd.append("plugin_tar", pluginFile);
      if (proxyFile) fd.append("proxy_tar", proxyFile);

      const result = await api.vspherePluginUpload(fd, (pct) => {
        setUploadPct(pct);
        if (pct < 100) {
          const mbSent = Math.round((pct / 100) * totalUploadBytes / (1024 * 1024));
          const mbTotal = Math.round(totalUploadBytes / (1024 * 1024));
          setUploadLabel(`Uploading… ${mbSent} / ${mbTotal} MB (${pct}%)`);
        } else {
          // Transfer done; server is now running docker image load (indeterminate)
          setUploadPct(null);
          setUploadLabel("Loading images into Docker…");
        }
      });

      if (result.errors.length > 0) {
        setErr("Errors: " + result.errors.join("; "));
      }
      if (result.loaded.length > 0) {
        setMsg("Loaded: " + result.loaded.join(", "));
      }
      setPluginFile(null);
      setProxyFile(null);
      if (pluginInputRef.current) pluginInputRef.current.value = "";
      if (proxyInputRef.current) proxyInputRef.current.value = "";
      await load();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setUploading(false);
      setUploadPct(null);
      setUploadLabel("");
    }
  };

  // Kick off a detached action, then let the status poll report progress.
  const beginInstall = async (action: () => Promise<unknown>) => {
    setBusy(true);
    setErr("");
    setMsg("");
    lastInstallDone.current = false;
    try {
      await action();
      await load();
    } catch (e) {
      setErr((e as Error).message);
    } finally {
      setBusy(false);
    }
  };

  if (loading) return <p>Loading…</p>;

  const dockerUnavailable = status && !status.available;
  const running = status?.server === "running" && status?.proxy === "running";
  const refs = status?.image_refs;

  return (
    <div>
      <h2>vSphere Plugin Containers</h2>
      <p style={{ color: "var(--muted)" }}>
        The Everpure Data vSphere Client Plugin runs as two Docker containers alongside PHIF.
        Set them up here before running the{" "}
        <strong>Deploy vSphere plugin</strong> action from the Operations page.
      </p>

      {dockerUnavailable && (
        <div className="error">
          Docker socket unavailable: {status?.error}
          <br />
          Ensure <code>/var/run/docker.sock</code> is mounted into the backend container.
        </div>
      )}

      {/* ── Status ─────────────────────────────────────────────────────── */}
      {status && !dockerUnavailable && (
        <section className="card" style={{ marginBottom: "1.5rem" }}>
          <h3 style={{ marginTop: 0 }}>Status</h3>
          <table style={{ borderCollapse: "collapse", width: "100%" }}>
            <tbody>
              <tr>
                <td style={{ padding: "4px 12px 4px 0", color: "var(--muted)" }}>Plugin server</td>
                <td>{stateBadge(status.server)}</td>
              </tr>
              <tr>
                <td style={{ padding: "4px 12px 4px 0", color: "var(--muted)" }}>Proxy (port 9443)</td>
                <td>{stateBadge(status.proxy)}</td>
              </tr>
              <tr>
                <td style={{ padding: "4px 12px 4px 0", color: "var(--muted)" }}>Plugin image</td>
                <td>
                  {imageBadge(status.images.server)}
                  {refs?.server && (
                    <code style={{ marginLeft: "0.5rem", fontSize: "0.8rem", color: "var(--muted)" }}>
                      {refs.server}
                    </code>
                  )}
                </td>
              </tr>
              <tr>
                <td style={{ padding: "4px 12px 4px 0", color: "var(--muted)" }}>Proxy image</td>
                <td>
                  {imageBadge(status.images.proxy)}
                  {refs?.proxy && (
                    <code style={{ marginLeft: "0.5rem", fontSize: "0.8rem", color: "var(--muted)" }}>
                      {refs.proxy}
                    </code>
                  )}
                </td>
              </tr>
              <tr>
                <td style={{ padding: "4px 12px 4px 0", color: "var(--muted)" }}>Plugin secret</td>
                <td>
                  {status.secret_exists ? (
                    <span style={{ color: "var(--green)", fontWeight: 600 }}>present</span>
                  ) : (
                    <span style={{ color: "#888", fontWeight: 600 }}>will be generated on start</span>
                  )}
                </td>
              </tr>
            </tbody>
          </table>

          <div style={{ display: "flex", gap: "0.75rem", marginTop: "1rem" }}>
            <button
              disabled={busy || uploading || installing || running}
              onClick={() => beginInstall(api.vspherePluginInstall)}
              title="Pull both images from Docker Hub and start the containers"
            >
              {installing ? "Installing…" : running ? "Installed" : "Install from Docker Hub"}
            </button>
            <button
              className="secondary"
              disabled={busy || installing || !status.can_start || running}
              onClick={() => run(api.vspherePluginStart, "Containers started")}
            >
              Start
            </button>
            <button
              className="secondary"
              disabled={
                busy || installing ||
                (status.server === "missing" && status.proxy === "missing")
              }
              onClick={() => run(api.vspherePluginStop, "Containers stopped")}
            >
              Stop
            </button>
          </div>

          {/* Install progress — driven by the status poll */}
          {installing && (
            <div style={{ marginTop: "1rem" }}>
              <div style={{ fontSize: "0.85rem", color: "var(--muted)", marginBottom: "4px" }}>
                {install?.phase}
                {install?.detail ? ` — ${install.detail}` : ""}
                {install?.percent != null ? ` (${install.percent}%)` : ""}
              </div>
              <ProgressBar pct={install?.percent ?? null} />
            </div>
          )}

          {!installing && !running && (
            <p style={{ margin: "0.75rem 0 0", color: "var(--muted)", fontSize: "0.85rem" }}>
              <strong>Install from Docker Hub</strong> pulls both images and starts the
              containers in one step — nothing to download by hand. Use{" "}
              <strong>Start</strong> on its own if the images are already present.
            </p>
          )}
        </section>
      )}

      {err && <div className="error">{err}</div>}
      {msg && <div className="success">{msg}</div>}

      {/* ── Offline / air-gapped install ───────────────────────────────── */}
      {status && !dockerUnavailable && (
        <section className="card">
          <h3 style={{ marginTop: 0 }}>
            <button
              className="secondary"
              style={{ marginRight: "0.75rem" }}
              onClick={() => setShowOffline((v) => !v)}
            >
              {showOffline ? "▾" : "▸"}
            </button>
            Offline install
          </h3>
          <p style={{ color: "var(--muted)", marginTop: 0, marginBottom: showOffline ? "1rem" : 0 }}>
            Only needed if this host cannot reach Docker Hub. Save the images on a
            connected machine with{" "}
            <code>docker save {refs?.server ?? "everpure/client-plugin-vsphere"} -o vsphere-plugin.tar</code>,
            copy them over, and upload them here.
          </p>

          {showOffline && (
            <>
              <div style={{ display: "flex", flexDirection: "column", gap: "0.75rem" }}>
                <label style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                  <span style={{ color: "var(--muted)", fontSize: "0.875rem" }}>
                    Plugin server image tar{" "}
                    {status.images.server && (
                      <span style={{ color: "var(--green)" }}>(already present)</span>
                    )}
                  </span>
                  <input
                    ref={pluginInputRef}
                    type="file"
                    accept=".tar"
                    disabled={uploading || installing}
                    onChange={(e) => setPluginFile(e.target.files?.[0] ?? null)}
                  />
                </label>

                <label style={{ display: "flex", flexDirection: "column", gap: "4px" }}>
                  <span style={{ color: "var(--muted)", fontSize: "0.875rem" }}>
                    Reverse-proxy image tar{" "}
                    {status.images.proxy && (
                      <span style={{ color: "var(--green)" }}>(already present)</span>
                    )}
                  </span>
                  <input
                    ref={proxyInputRef}
                    type="file"
                    accept=".tar"
                    disabled={uploading || installing}
                    onChange={(e) => setProxyFile(e.target.files?.[0] ?? null)}
                  />
                </label>
              </div>

              {/* Progress bar — shown during upload */}
              {uploading && (
                <div style={{ marginTop: "1rem" }}>
                  <div style={{ fontSize: "0.85rem", color: "var(--muted)", marginBottom: "4px" }}>
                    {uploadLabel}
                  </div>
                  <ProgressBar pct={uploadPct} />
                </div>
              )}

              <div style={{ display: "flex", gap: "0.75rem", marginTop: "1rem" }}>
                <button
                  className="secondary"
                  disabled={uploading || installing || (!pluginFile && !proxyFile)}
                  onClick={handleUpload}
                >
                  {uploading ? "Uploading…" : "Upload & Load"}
                </button>
                <button
                  className="secondary"
                  disabled={busy || uploading || installing}
                  onClick={() => beginInstall(api.vspherePluginPull)}
                  title="Pull the images from Docker Hub without starting the containers"
                >
                  Pull only
                </button>
              </div>
            </>
          )}
        </section>
      )}

      {/* ── Next step hint ─────────────────────────────────────────────── */}
      {running && (
        <section
          className="card"
          style={{ marginTop: "1.5rem", borderLeft: "4px solid var(--green)" }}
        >
          <h3 style={{ marginTop: 0 }}>Next step</h3>
          <p style={{ margin: 0 }}>
            Containers are running. Go to <strong>Operations</strong> for a vSphere
            hypervisor and run <strong>Deploy vSphere plugin + VASA</strong> to register
            the plugin with vCenter. Use{" "}
            <code>https://&lt;this-host&gt;:9443</code> as the PHIF server address.
          </p>
        </section>
      )}
    </div>
  );
}
