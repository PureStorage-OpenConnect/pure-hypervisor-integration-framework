import { useEffect, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { api } from "../api/client";
import type { ConnectorDescriptor, FormFieldSpec, VspherePluginStatus } from "../api/client";
import { DynamicForm, LiveLog, useAsync } from "../components";

// Map the chosen protocol to the interface-binding param + discovery kind.
const IFACE_BY_PROTOCOL: Record<string, { param: string; kind: string; label: string }> = {
  iscsi: { param: "iscsi_nics", kind: "nics", label: "iSCSI NICs (storage subnet)" },
  "nvme-tcp": { param: "nvme_sources", kind: "nvme_sources", label: "NVMe-TCP source interfaces" },
  fc: { param: "fc_hbas", kind: "fc_hbas", label: "Fibre Channel HBAs" },
};

type Step = "connect" | "select" | "deploy";

export default function WizardPage() {
  const connectors = useAsync(() => api.listConnectors());
  const arrays = useAsync(() => api.listArrays());

  const [step, setStep] = useState<Step>("connect");
  const [connectorKey, setConnectorKey] = useState("");
  const [name, setName] = useState("");
  const [arrayId, setArrayId] = useState("");
  const [scope, setScope] = useState<"cluster" | "node">("cluster");
  const [values, setValues] = useState<Record<string, unknown>>({});
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  const [vspherePluginStatus, setVspherePluginStatus] = useState<VspherePluginStatus | null>(null);

  // page 2 state
  const [hvId, setHvId] = useState<string | null>(null);
  const [nodes, setNodes] = useState<{ name: string; host: string }[]>([]);
  const [ifaceVals, setIfaceVals] = useState<Record<string, unknown>>({});
  const [deployVals, setDeployVals] = useState<Record<string, unknown>>({});
  const [jobId, setJobId] = useState<string | null>(null);

  // Fetch vsphere plugin status whenever the vsphere connector is selected.
  useEffect(() => {
    if (connectorKey !== "vsphere") { setVspherePluginStatus(null); return; }
    api.vspherePluginStatus().then(setVspherePluginStatus).catch(() => setVspherePluginStatus(null));
  }, [connectorKey]);

  const vspherePluginReady =
    vspherePluginStatus?.server === "running" && vspherePluginStatus?.proxy === "running";

  const selected: ConnectorDescriptor | undefined = useMemo(
    () => connectors.data?.find((c) => c.key === connectorKey),
    [connectors.data, connectorKey],
  );
  const protocol = String(values.protocol ?? "iscsi");
  const ifaceCfg = IFACE_BY_PROTOCOL[protocol] ?? IFACE_BY_PROTOCOL.iscsi;

  const splitConnSecrets = () => {
    const connection: Record<string, unknown> = {};
    const secrets: Record<string, unknown> = {};
    for (const f of selected!.target_schema) {
      const v = values[f.name] ?? f.default;
      if (v === undefined || v === null || v === "") continue;
      if (f.type === "secret") secrets[f.name] = v;
      else connection[f.name] = v;
    }
    return { connection, secrets };
  };

  // Page 1 -> create + validate the hypervisor, then move to interface selection.
  const prepare = async () => {
    if (!selected) return;
    setBusy(true);
    setErr(null);
    try {
      const { connection, secrets } = splitConnSecrets();
      const r = await api.wizardPrepare({
        name, connector_key: connectorKey, connection, secrets, array_id: arrayId || null,
      });
      setHvId(r.hypervisor_id);
      const n = await api.listNodes(r.hypervisor_id);
      setNodes(n.nodes);
      setDeployVals({ phif_host: window.location.hostname });
      setStep("select");
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  // Page 2 -> deploy with the selected interfaces.
  const deploy = async () => {
    if (!hvId) return;
    setBusy(true);
    setErr(null);
    try {
      // Forward the "Deployment options" the operator filled in for ANY wizard
      // step (deploy, configure, configure_binding — e.g. machine_config_role),
      // not just the deploy action. The shared params dict is passed to every step.
      const stepIds = selected?.wizard_steps ?? ["deploy"];
      const resolvedDeployVals: Record<string, unknown> = {};
      for (const id of stepIds) {
        const action = selected?.actions.find((a) => a.id === id);
        for (const f of action?.fields ?? []) {
          const v = deployVals[f.name] ?? f.default;
          if (v !== undefined && v !== null && v !== "") resolvedDeployVals[f.name] = v;
        }
      }
      // Iface selection wins over any same-named default from the step fields.
      const params: Record<string, unknown> = {
        ...resolvedDeployVals,
        [ifaceCfg.param]: ifaceVals[ifaceCfg.param] ?? [],
        rebind: Boolean(ifaceVals.rebind),
      };
      const r = await api.wizardRun(hvId, { scope, params });
      setJobId(r.job_id);
      setStep("deploy");
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  // Abandon a prepared-but-not-deployed hypervisor.
  const cancel = async () => {
    if (hvId) {
      try { await api.deleteHypervisor(hvId); } catch { /* ignore */ }
    }
    setHvId(null); setNodes([]); setIfaceVals({}); setJobId(null);
    setStep("connect");
  };

  const ifaceField: FormFieldSpec = {
    name: ifaceCfg.param, label: ifaceCfg.label, type: "multiselect", required: false,
    default: null, options: null, options_source: ifaceCfg.kind, help: "", placeholder: "",
  };
  const rebindField: FormFieldSpec = {
    name: "rebind", label: "Re-bind iSCSI ifaces on every node (make them uniform)",
    type: "bool", required: false, default: false, options: null, options_source: null,
    help: "", placeholder: "",
  };

  return (
    <div>
      <h1>Deployment Wizard</h1>
      <p className="subtitle">
        Deploy the complete Everpure integration to a cluster (or a single node) in three steps:
        connect → pick storage interfaces (after node discovery) → deploy.
      </p>

      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        {(["connect", "select", "deploy"] as Step[]).map((s, i) => (
          <span key={s} className={`badge ${step === s ? "ga" : "scaffold"}`}>
            {i + 1}. {s === "connect" ? "Connect" : s === "select" ? "Interfaces" : "Deploy"}
          </span>
        ))}
      </div>

      {step === "connect" && (
        <div className="card">
          <div className="row">
            <div>
              <label>Hypervisor type *</label>
              <select value={connectorKey} onChange={(e) => { setConnectorKey(e.target.value); setValues({}); }}>
                <option value="">Select…</option>
                {(connectors.data ?? []).map((c) => <option key={c.key} value={c.key}>{c.name}</option>)}
              </select>
            </div>
            <div>
              <label>FlashArray *</label>
              <select value={arrayId} onChange={(e) => setArrayId(e.target.value)}>
                <option value="">Select…</option>
                {(arrays.data ?? []).map((a) => <option key={a.id} value={a.id}>{a.name}</option>)}
              </select>
            </div>
          </div>
          {selected && (
            <>
              {/* vSphere plugin readiness gate */}
              {connectorKey === "vsphere" && !vspherePluginReady && (
                <div
                  style={{
                    margin: "12px 0",
                    padding: "10px 14px",
                    background: "rgba(210,153,34,0.12)",
                    border: "1px solid var(--yellow)",
                    borderRadius: 6,
                    fontSize: 13,
                    lineHeight: 1.5,
                  }}
                >
                  <strong style={{ color: "var(--yellow)" }}>
                    vSphere plugin containers are not running.
                  </strong>{" "}
                  The plugin must be set up before a vSphere integration can be deployed.{" "}
                  <Link to="/vsphere-plugin" style={{ color: "var(--accent)" }}>
                    Go to vSphere Plugin →
                  </Link>
                </div>
              )}
              <div className="row" style={{ marginTop: 4 }}>
                <div>
                  <label>Name *</label>
                  <input value={name} onChange={(e) => setName(e.target.value)} placeholder="prod-cluster" />
                </div>
                <div>
                  <label>Scope</label>
                  <select value={scope} onChange={(e) => setScope(e.target.value as "cluster" | "node")}>
                    <option value="cluster">Whole cluster (all nodes)</option>
                    <option value="node">Single node</option>
                  </select>
                </div>
              </div>
              <DynamicForm fields={selected.target_schema} values={values} onChange={setValues} />
              <div style={{ marginTop: 14 }}>
                <button
                  disabled={busy || !name || !arrayId || (connectorKey === "vsphere" && !vspherePluginReady)}
                  onClick={prepare}
                >
                  {busy ? "Discovering…" : "Next: discover nodes & interfaces →"}
                </button>
              </div>
            </>
          )}
          {err && <div className="error">{err}</div>}
        </div>
      )}

      {step === "select" && selected && (
        <div className="card">
          <h2 style={{ marginTop: 0 }}>Discovered {nodes.length} node(s)</h2>
          <div style={{ marginBottom: 12 }}>
            {nodes.map((n) => <span key={n.host} className="cap">{n.name} ({n.host})</span>)}
          </div>
          <p className="muted" style={{ fontSize: 13 }}>
            Select the storage interfaces to bind ({protocol}). Only interfaces on the array's
            storage subnet are offered; they should be consistent across all nodes.
          </p>
          {connectorKey === "openshift" && (
            <div
              style={{
                margin: "8px 0 12px",
                padding: "10px 14px",
                background: "rgba(210,153,34,0.12)",
                border: "1px solid var(--yellow)",
                borderRadius: 6,
                fontSize: 13,
                lineHeight: 1.5,
              }}
            >
              <strong style={{ color: "var(--yellow)" }}>Heads up:</strong>{" "}
              on OpenShift, binding storage interfaces is delivered as a{" "}
              <strong>MachineConfig</strong>. When you select one or more interfaces, the
              Machine Config Operator will <strong>drain and reboot the nodes one at a
              time (a rolling reboot)</strong> to apply the iSCSI iface binding, multipath,
              and ARP settings. Leave the selection empty to skip interface binding (no
              MachineConfig, no reboot) and configure it later.
            </div>
          )}
          <DynamicForm
            fields={protocol === "iscsi" ? [ifaceField, rebindField] : [ifaceField]}
            values={ifaceVals}
            onChange={setIfaceVals}
            onDiscover={(kind) => api.discoverOptions(hvId!, kind).then((r) => r.options)}
          />
          {(() => {
            // Fields the wizard handles itself — never show as extra form fields.
            const WIZARD_HANDLED = new Set([
              "protocol", "rebind", "nvme_options",
              // all transport iface selectors (only the active one is shown above)
              "iscsi_nics", "iscsi_vmknics", "fc_hbas", "nvme_sources", "nvme_adapters",
              // per-host data is auto-discovered from vCenter
              "hosts", "initiators", "iqns", "wwns", "nqns",
            ]);
            const stepIds = selected.wizard_steps ?? [];
            const seen = new Set<string>();
            const extraFields = stepIds.flatMap((id) => {
              const action = selected.actions.find((a) => a.id === id);
              return action?.fields ?? [];
            }).filter((f) => {
              if (WIZARD_HANDLED.has(f.name)) return false;
              if (seen.has(f.name)) return false;  // deduplicate (e.g. host_group from two steps)
              seen.add(f.name);
              return true;
            });
            return extraFields.length > 0 ? (
              <>
                <h3 style={{ marginTop: 18, marginBottom: 4, fontSize: 14 }}>Deployment options</h3>
                <DynamicForm fields={extraFields} values={deployVals} onChange={setDeployVals} />
              </>
            ) : null;
          })()}
          <div style={{ marginTop: 14 }}>
            <button disabled={busy} onClick={deploy}>{busy ? "Starting…" : `Deploy to ${scope}`}</button>{" "}
            <button className="secondary" onClick={cancel}>Cancel</button>
          </div>
          {err && <div className="error">{err}</div>}
        </div>
      )}

      {step === "deploy" && jobId && (
        <div className="card"><LiveLog jobId={jobId} /></div>
      )}
    </div>
  );
}
