import { useEffect, useMemo, useState } from "react";
import { api } from "../api/client";
import type { Hypervisor, NetworkSummary, Placement, VmSpec, VmSummary } from "../api/client";
import { LiveLog, useAsync } from "../components";
import MigrationGroupsPanel from "./MigrationGroupsPanel";

// Cross-hypervisor VM migration (cold/reboot cutover). The same FlashArray
// volume(s) are re-pointed from the source to the destination — no data moves.
// Steps: pick source HV + VM → pick destination HV (same array) → map each NIC
// to a destination network → review matched hardware → run with a live log.
type Step = "source" | "destination" | "network" | "run";

const MIGRATE_CONNECTORS = new Set(["proxmox", "xcpng", "hpevme", "vsphere", "openstack", "openshift"]);

function mib(bytes?: number | null): string {
  if (!bytes) return "—";
  return `${Math.round(bytes / (1024 * 1024))} MiB`;
}

export default function MigrationPage() {
  const hypervisors = useAsync(() => api.listHypervisors());
  const [tab, setTab] = useState<"single" | "groups">("single");

  const [step, setStep] = useState<Step>("source");
  const [sourceId, setSourceId] = useState("");
  const [vmRef, setVmRef] = useState("");
  const [destId, setDestId] = useState("");
  const [vms, setVms] = useState<VmSummary[] | null>(null);
  const [spec, setSpec] = useState<VmSpec | null>(null);
  const [destNets, setDestNets] = useState<NetworkSummary[] | null>(null);
  const [networkMap, setNetworkMap] = useState<Record<string, string>>({});
  // Destination placement: Everpure-connected clusters + their Everpure storage (HPE/XCP).
  const [placements, setPlacements] = useState<Placement[] | null>(null);
  const [destCluster, setDestCluster] = useState("");
  const [destStorage, setDestStorage] = useState("");
  const [mode, setMode] = useState<"move" | "copy">("move");
  const [shutdownSource, setShutdownSource] = useState(false);
  const [forceStop, setForceStop] = useState(false);
  const [convertToVmfs, setConvertToVmfs] = useState(false);
  // Destination end-of-migration power state. Default: copy → off; move → match the
  // source VM's current state. The user can override (powerOnTouched).
  const [powerOn, setPowerOn] = useState(false);
  const [powerOnTouched, setPowerOnTouched] = useState(false);
  const [precheck, setPrecheck] = useState<{
    cross_array: boolean; connection_exists: boolean; dest_array_name: string; needs_authorization: boolean;
  } | null>(null);
  const [authorizeConnect, setAuthorizeConnect] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  // Only migration-capable hypervisors are eligible.
  const eligible = useMemo<Hypervisor[]>(
    () => (hypervisors.data ?? []).filter((h) => MIGRATE_CONNECTORS.has(h.connector_key)),
    [hypervisors.data],
  );
  const source = eligible.find((h) => h.id === sourceId);
  const dest = eligible.find((h) => h.id === destId);
  // RDM→VMFS conversion (self-provisioned scratch VMFS + XCOPY) is vSphere-specific.
  const destIsVsphere = dest?.connector_key === "vsphere";

  // Current power state of the selected source VM (for the move power-on default).
  const sourceRunning = useMemo(
    () => ((vms ?? []).find((v) => v.id === vmRef)?.power_state ?? "")
      .toLowerCase() === "running",
    [vms, vmRef],
  );
  // Apply the default power state until the user overrides it: copy → off,
  // move → match the source.
  useEffect(() => {
    if (!powerOnTouched) setPowerOn(mode === "move" ? sourceRunning : false);
  }, [mode, sourceRunning, powerOnTouched]);
  // Destination must have a FlashArray. It may be the SAME array (re-map/clone) or
  // a DIFFERENT array (volume sent via replication — may need authorization).
  const destChoices = eligible.filter(
    (h) => h.id !== sourceId && source?.array_id && h.array_id,
  );

  const loadVms = async (id: string) => {
    setBusy(true);
    setErr(null);
    try {
      setVms(await api.listVms(id));
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  const chooseSource = (id: string) => {
    setSourceId(id);
    setVmRef("");
    setVms(null);
    setDestId("");
    if (id) loadVms(id);
  };

  // Source VM chosen → capture its spec; move to destination selection.
  const toDestination = async () => {
    if (!sourceId || !vmRef) return;
    setBusy(true);
    setErr(null);
    try {
      setSpec(await api.getVmSpec(sourceId, vmRef));
      setStep("destination");
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  // Destination hypervisor picked → load its Everpure-connected clusters + storage.
  const onDestChange = async (id: string) => {
    setDestId(id);
    setDestCluster("");
    setDestStorage("");
    setPlacements(null);
    if (!id) return;
    try {
      const ps = await api.listPlacements(id);
      setPlacements(ps);
      if (ps.length === 1) {
        setDestCluster(ps[0].cluster.id);
        if (ps[0].storage.length === 1) setDestStorage(ps[0].storage[0].id);
      }
    } catch (e) {
      setErr(String((e as Error).message));
    }
  };

  const clusterStorage = useMemo(
    () => placements?.find((p) => p.cluster.id === destCluster)?.storage ?? [],
    [placements, destCluster],
  );
  // Placement is required only when the destination exposes Everpure-connected clusters.
  const placementReady = !placements || placements.length === 0
    || (!!destCluster && !!destStorage);

  // Destination chosen → load its networks; seed each source NIC's mapping.
  const toNetwork = async () => {
    if (!destId) return;
    setBusy(true);
    setErr(null);
    try {
      setPrecheck(await api.migrationPrecheck(sourceId, destId));
      const nets = await api.listNetworks(destId);
      setDestNets(nets);
      const seed: Record<string, string> = {};
      for (const nic of spec?.nics ?? []) {
        seed[nic.source_network] = nets[0]?.id ?? "";
      }
      setNetworkMap(seed);
      setStep("network");
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  const start = async () => {
    if (!sourceId || !destId || !vmRef) return;
    setBusy(true);
    setErr(null);
    try {
      const r = await api.createMigration({
        source_hypervisor_id: sourceId,
        dest_hypervisor_id: destId,
        vm_ref: vmRef,
        network_map: networkMap,
        options: {
          mode,
          force_stop: forceStop,
          ...(mode === "copy" ? { shutdown_source: shutdownSource } : {}),
          ...(precheck?.cross_array ? { allow_array_connect: authorizeConnect } : {}),
          ...(destCluster ? { dest_cluster: destCluster } : {}),
          ...(destStorage ? { dest_storage: destStorage } : {}),
          ...(destIsVsphere && convertToVmfs ? { convert_to_vmfs: true } : {}),
          power_on: powerOn,
        },
      });
      setJobId(r.job_id);
      setStep("run");
    } catch (e) {
      setErr(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  const reset = () => {
    setStep("source");
    setSourceId(""); setVmRef(""); setDestId("");
    setVms(null); setSpec(null); setDestNets(null);
    setNetworkMap({}); setJobId(null); setErr(null);
    setPrecheck(null); setAuthorizeConnect(false);
    setConvertToVmfs(false);
    setPowerOn(false); setPowerOnTouched(false);
  };

  // Human-readable migration plan computed from the current selections. Shown on the
  // review step so the operator sees exactly what will happen before running.
  const planSteps = useMemo<string[]>(() => {
    const xa = !!precheck?.cross_array;
    const steps: string[] = [];
    steps.push("Create the destination VM shell (matching vCPU/RAM/firmware; each NIC's MAC preserved where the source provides one).");
    if (destIsVsphere && convertToVmfs) {
      steps.push("Self-provision a temporary FlashArray-backed VMFS scratch datastore and place the VM home + RDM pointers on it.");
    }
    if (xa) {
      steps.push("Replicate each source volume to the destination array, then create a destination-managed volume from the replica.");
    } else {
      steps.push("Create a destination-managed FlashArray volume per disk and array-copy the source data onto it (no host data movement).");
    }
    if (destIsVsphere) {
      steps.push("Attach each managed volume as a virtual-mode RDM (vRDM) and create its mapping pointer.");
    }
    if (destIsVsphere && convertToVmfs) {
      steps.push("Storage vMotion the whole VM onto a FlashArray-backed VMFS datastore — converting each vRDM to a native VMDK (one-time host-side copy).");
      steps.push("Verify each disk is now a native VMDK, then free the temporary RDM volumes and tear down the scratch datastore.");
    }
    steps.push(powerOn
      ? "Set the boot disk and power on the destination VM."
      : "Set the boot disk and leave the destination VM powered off.");
    if (mode === "move") {
      steps.push("Remove the source VM (its source volume is preserved, never erased).");
    } else {
      steps.push(shutdownSource
        ? "Leave the source shut down (it was powered off for a clean copy)."
        : "Leave the source running untouched (crash-consistent copy).");
    }
    return steps;
  }, [destIsVsphere, convertToVmfs, precheck, mode, shutdownSource, powerOn]);

  const sourceNetworks = useMemo(() => {
    const seen = new Set<string>();
    return (spec?.nics ?? []).filter((n) => {
      if (seen.has(n.source_network)) return false;
      seen.add(n.source_network);
      return true;
    });
  }, [spec]);

  const steps: Step[] = ["source", "destination", "network", "run"];
  const stepLabel: Record<Step, string> = {
    source: "Source VM", destination: "Destination", network: "Map NICs", run: "Run",
  };

  return (
    <div>
      <h1>Migrate VM</h1>
      <p className="subtitle">
        Move or copy a VM between Proxmox, XCP-ng, HPE VM Essentials, and VMware
        vSphere, backed by the same FlashArray. <b>Move</b> is a cold cutover that re-points the volume(s)
        to the destination and removes the source; <b>Copy</b> clones the volume(s)
        and leaves the source running. MAC addresses are preserved. (Choose the mode
        on the last step.)
      </p>

      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        <button className={tab === "single" ? "" : "secondary"} onClick={() => setTab("single")}>
          Single migration
        </button>
        <button className={tab === "groups" ? "" : "secondary"} onClick={() => setTab("groups")}>
          Migration groups &amp; scheduling
        </button>
      </div>

      {tab === "groups" && (
        <MigrationGroupsPanel hypervisors={hypervisors.data ?? []} />
      )}

      {tab === "single" && (
      <>
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        {steps.map((s, i) => (
          <span key={s} className={`badge ${step === s ? "ga" : "scaffold"}`}>
            {i + 1}. {stepLabel[s]}
          </span>
        ))}
      </div>

      {step === "source" && (
        <div className="card">
          <label>Source hypervisor *</label>
          <select value={sourceId} onChange={(e) => chooseSource(e.target.value)}>
            <option value="">Select…</option>
            {eligible.map((h) => (
              <option key={h.id} value={h.id}>{h.name} ({h.connector_key})</option>
            ))}
          </select>
          {sourceId && !source?.array_id && (
            <div className="error">
              This hypervisor has no associated FlashArray — migration requires one.
            </div>
          )}
          {sourceId && (
            <div style={{ marginTop: 12 }}>
              <label>VM to migrate *</label>
              <select value={vmRef} onChange={(e) => setVmRef(e.target.value)} disabled={busy}>
                <option value="">{busy ? "Loading…" : "Select…"}</option>
                {(vms ?? []).map((v) => (
                  <option key={v.id} value={v.id}>
                    {v.name} — {v.power_state} ({v.id})
                  </option>
                ))}
              </select>
            </div>
          )}
          <div style={{ marginTop: 14 }}>
            <button disabled={busy || !vmRef} onClick={toDestination}>
              {busy ? "Capturing…" : "Next: destination →"}
            </button>
          </div>
          {err && <div className="error">{err}</div>}
        </div>
      )}

      {step === "destination" && (
        <div className="card">
          <label>Destination hypervisor * (same array, or a different array via replication)</label>
          <select value={destId} onChange={(e) => onDestChange(e.target.value)}>
            <option value="">Select…</option>
            {destChoices.map((h) => (
              <option key={h.id} value={h.id}>{h.name} ({h.connector_key})</option>
            ))}
          </select>
          {destChoices.length === 0 && (
            <div className="muted" style={{ fontSize: 13, marginTop: 6 }}>
              No eligible destinations: you need another migration-capable hypervisor
              associated with a FlashArray.
            </div>
          )}
          {placements && placements.length > 0 && (
            <>
              <label style={{ marginTop: 12 }}>Cluster * (only clusters with an Everpure connection)</label>
              <select value={destCluster} onChange={(e) => { setDestCluster(e.target.value); setDestStorage(""); }}>
                <option value="">Select…</option>
                {placements.map((p) => (
                  <option key={p.cluster.id} value={p.cluster.id}>{p.cluster.name}</option>
                ))}
              </select>
              <label style={{ marginTop: 12 }}>Storage * (Everpure-backed only)</label>
              <select value={destStorage} disabled={!destCluster}
                      onChange={(e) => setDestStorage(e.target.value)}>
                <option value="">Select…</option>
                {clusterStorage.map((s) => (
                  <option key={s.id} value={s.id}>{s.name} ({s.kind})</option>
                ))}
              </select>
            </>
          )}
          {placements !== null && placements.length === 0 && destId && (
            <div className="muted" style={{ fontSize: 13, marginTop: 6 }}>
              This destination auto-selects its Everpure storage (no cluster choice).
            </div>
          )}
          <div style={{ marginTop: 14 }}>
            <button disabled={busy || !destId || !placementReady} onClick={toNetwork}>
              {busy ? "Loading networks…" : "Next: map NICs →"}
            </button>{" "}
            <button className="secondary" onClick={() => setStep("source")}>Back</button>
          </div>
          {err && <div className="error">{err}</div>}
        </div>
      )}

      {step === "network" && (
        <div className="card">
          <h2 style={{ marginTop: 0 }}>Map each NIC to a destination network</h2>
          <p className="muted" style={{ fontSize: 13 }}>
            Each source NIC's MAC address is preserved on the destination.
          </p>
          <table>
            <thead>
              <tr><th>Source network</th><th>MAC (preserved)</th><th>Destination network</th></tr>
            </thead>
            <tbody>
              {sourceNetworks.map((nic) => (
                <tr key={nic.source_network}>
                  <td>{nic.source_network}</td>
                  <td><code>{nic.mac}</code></td>
                  <td>
                    <select
                      value={networkMap[nic.source_network] ?? ""}
                      onChange={(e) =>
                        setNetworkMap((m) => ({ ...m, [nic.source_network]: e.target.value }))
                      }
                    >
                      <option value="">Select…</option>
                      {(destNets ?? []).map((n) => (
                        <option key={n.id} value={n.id}>{n.name} ({n.id})</option>
                      ))}
                    </select>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          <h2>Matched hardware</h2>
          {spec && (
            <div className="muted" style={{ fontSize: 13, marginBottom: 8 }}>
              {spec.vcpus} vCPU · {mib(spec.memory_bytes)} RAM · firmware {spec.firmware}
              {spec.secure_boot ? " (secure boot)" : ""}
            </div>
          )}
          <table>
            <thead>
              <tr><th>#</th><th>FlashArray volume</th><th>Bus</th><th>Boot</th></tr>
            </thead>
            <tbody>
              {(spec?.disks ?? []).map((d) => (
                <tr key={d.order}>
                  <td>{d.order}</td>
                  <td><code>{d.identity.fa_volume}</code></td>
                  <td>{d.bus}</td>
                  <td>{d.boot ? "yes" : ""}</td>
                </tr>
              ))}
            </tbody>
          </table>

          {precheck?.cross_array && (
            <div className="card" style={{ background: "var(--panel-2)", marginBottom: 12 }}>
              <h2 style={{ marginTop: 0 }}>Cross-array migration</h2>
              <p className="muted" style={{ fontSize: 13 }}>
                The source and destination are on <b>different FlashArrays</b>. The
                volume(s) will be <b>sent to the destination array via replication</b>,
                copied locally there, and attached to the new VM.
              </p>
              {precheck.connection_exists ? (
                <div className="muted" style={{ fontSize: 13 }}>
                  ✓ A replication connection to <code>{precheck.dest_array_name}</code> already
                  exists — no array configuration needed.
                </div>
              ) : (
                <>
                  <div className="error" style={{ marginBottom: 8 }}>
                    No replication connection to <code>{precheck.dest_array_name || "the destination array"}</code> exists
                    yet. Proceeding will <b>configure an array connection</b> between the
                    two FlashArrays (kept afterwards; removable on the FlashArrays page).
                  </div>
                  <label style={{ display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
                    <input type="checkbox" style={{ width: "auto" }}
                      checked={authorizeConnect}
                      onChange={(e) => setAuthorizeConnect(e.target.checked)} />
                    <span>I authorize PHIF to configure the replication connection between these arrays.</span>
                  </label>
                </>
              )}
            </div>
          )}

          <h2>Mode</h2>
          <div style={{ display: "flex", flexDirection: "column", gap: 6, marginBottom: 8 }}>
            <label style={{ display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <input type="radio" name="mode" style={{ width: "auto" }}
                checked={mode === "move"} onChange={() => setMode("move")} />
              <span><b>Move</b> — cold reboot: power off source, re-point the same
                FlashArray volume(s) to the destination, boot there. The source VM
                is removed afterwards (its volume is preserved).</span>
            </label>
            <label style={{ display: "flex", alignItems: "center", gap: 8, margin: 0 }}>
              <input type="radio" name="mode" style={{ width: "auto" }}
                checked={mode === "copy"} onChange={() => setMode("copy")} />
              <span><b>Copy</b> — clone the volume(s) on the array and build an
                independent destination VM. The source is preserved. Don't run both
                with the same IP/MAC.</span>
            </label>
          </div>
          {mode === "move" && (
            // Move always powers off the source first, so the force option is a
            // top-level move option (not a copy sub-item).
            <label style={{ display: "flex", alignItems: "center", gap: 8, margin: "0 0 8px" }}>
              <input type="checkbox" style={{ width: "auto" }}
                checked={forceStop} onChange={(e) => setForceStop(e.target.checked)} />
              <span><b>Force power off</b> the source if it doesn't shut down gracefully
                (no guest agent, or it ignores the request). Without this, a failed
                graceful shutdown aborts the migration.</span>
            </label>
          )}
          {mode === "copy" && (
            <>
              <label style={{ display: "flex", alignItems: "center", gap: 8, margin: "0 0 8px 24px" }}>
                <input type="checkbox" style={{ width: "auto" }}
                  checked={shutdownSource} onChange={(e) => setShutdownSource(e.target.checked)} />
                <span>Shut down the source first for a <b>clean</b> copy. The source is
                  <b> left shut down</b> (intended for a cutover). Otherwise the source
                  keeps running and the copy is crash-consistent (no power-off needed).</span>
              </label>
              {shutdownSource && (
                // Only a clean copy powers off the source — so the force option is a
                // sub-item of "shut down first", not of copy in general.
                <label style={{ display: "flex", alignItems: "center", gap: 8, margin: "0 0 8px 48px" }}>
                  <input type="checkbox" style={{ width: "auto" }}
                    checked={forceStop} onChange={(e) => setForceStop(e.target.checked)} />
                  <span><b>Force power off</b> if the guest doesn't shut down gracefully.</span>
                </label>
              )}
            </>
          )}
          {destIsVsphere && (
            <label style={{ display: "flex", alignItems: "center", gap: 8, margin: "8px 0 8px 0" }}>
              <input type="checkbox" style={{ width: "auto" }}
                checked={convertToVmfs} onChange={(e) => setConvertToVmfs(e.target.checked)} />
              <span><b>Convert disks to VMFS VMDKs</b> (optional) — after copying, Storage
                vMotion the disks onto a FlashArray-backed VMFS datastore (converting the
                raw device mappings to native VMDKs) and clean up the temporary FlashArray
                volumes. This is a <b>one-time host-side copy</b> during migration (a
                raw-LUN→VMFS copy can't be offloaded to the array via XCOPY). Leave
                unchecked (default) to keep the disks as <b>RDMs</b> — the array copy does
                everything with no host-side data movement.</span>
            </label>
          )}

          <label style={{ display: "flex", alignItems: "center", gap: 8, margin: "8px 0" }}>
            <input type="checkbox" style={{ width: "auto" }}
              checked={powerOn}
              onChange={(e) => { setPowerOnTouched(true); setPowerOn(e.target.checked); }} />
            <span><b>Power on the destination VM</b> when the migration completes.
              {" "}Default: {mode === "copy"
                ? "off for a copy (so it doesn't contend with the still-running source for IP/MAC)."
                : `match the source (currently ${sourceRunning ? "running → on" : "stopped → off"}).`}
            </span>
          </label>

          <h2>Migration plan</h2>
          <ol className="muted" style={{ fontSize: 13, marginTop: 0, paddingLeft: 20 }}>
            {planSteps.map((s, i) => <li key={i} style={{ marginBottom: 4 }}>{s}</li>)}
          </ol>
          <div style={{ marginTop: 14 }}>
            <button
              disabled={
                busy ||
                sourceNetworks.some((n) => !networkMap[n.source_network]) ||
                Boolean(precheck?.needs_authorization && !authorizeConnect)
              }
              onClick={start}
            >
              {busy ? "Starting…" : mode === "move" ? "Migrate (cold reboot)" : "Copy to destination"}
            </button>{" "}
            <button className="secondary" onClick={() => setStep("destination")}>Back</button>
          </div>
          {err && <div className="error">{err}</div>}
        </div>
      )}

      {step === "run" && jobId && (
        <div className="card">
          <LiveLog jobId={jobId} onDone={() => hypervisors.reload()} />
          <div style={{ marginTop: 14 }}>
            <button className="secondary" onClick={reset}>Start another migration</button>
          </div>
        </div>
      )}
      </>
      )}
    </div>
  );
}
