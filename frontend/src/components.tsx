import { useEffect, useRef, useState } from "react";
import { api } from "./api/client";
import type { DiscoveredOption, FormFieldSpec } from "./api/client";

// Multi-select that can pull its options dynamically (NICs / HBAs / NVMe sources).
function MultiSelect({
  field,
  value,
  onChange,
  onDiscover,
}: {
  field: FormFieldSpec;
  value: string[];
  onChange: (v: string[]) => void;
  onDiscover?: (kind: string) => Promise<DiscoveredOption[]>;
}) {
  const [opts, setOpts] = useState<DiscoveredOption[]>(
    (field.options ?? []).map((o) => ({ value: o, label: o })),
  );
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const load = () => {
    if (!field.options_source || !onDiscover) return;
    setLoading(true);
    setErr(null);
    onDiscover(field.options_source)
      .then(setOpts)
      .catch((e) => setErr(String(e.message ?? e)))
      .finally(() => setLoading(false));
  };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(load, [field.options_source]);

  const toggle = (v: string) =>
    onChange(value.includes(v) ? value.filter((x) => x !== v) : [...value, v]);

  return (
    <div>
      {field.options_source && (
        <button type="button" className="secondary" style={{ marginBottom: 6, fontSize: 12, padding: "4px 10px" }} onClick={load}>
          {loading ? "Discovering…" : "↻ Re-discover"}
        </button>
      )}
      <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
        {opts.length === 0 && !loading && <span className="muted" style={{ fontSize: 13 }}>No options discovered.</span>}
        {opts.map((o) => (
          <label
            key={o.value}
            style={{
              display: "inline-flex", alignItems: "center", gap: 6, margin: 0, padding: "5px 10px",
              borderRadius: 6, border: "1px solid var(--border)", cursor: "pointer",
              background: value.includes(o.value) ? "var(--accent)" : "var(--panel-2)",
              color: value.includes(o.value) ? "#1a1a1a" : "var(--text)",
            }}
          >
            <input type="checkbox" style={{ width: "auto" }} checked={value.includes(o.value)} onChange={() => toggle(o.value)} />
            {o.label}
          </label>
        ))}
      </div>
      {err && <div className="error">{err}</div>}
    </div>
  );
}

// A single-value text field that auto-fills from discovery (e.g. array portal
// IPs, target IQN/NQN, suggested host name). On mount it fetches the field's
// options_source and fills the value (joined) when empty; a Re-discover button
// refreshes. The value stays editable.
function DiscoverableInput({
  field,
  value,
  onChange,
  onDiscover,
}: {
  field: FormFieldSpec;
  value: string;
  onChange: (v: string) => void;
  onDiscover?: (kind: string) => Promise<DiscoveredOption[]>;
}) {
  const [loading, setLoading] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const load = () => {
    if (!field.options_source || !onDiscover) return;
    setLoading(true);
    setErr(null);
    onDiscover(field.options_source)
      .then((opts) => {
        const joined = opts.map((o) => String(o.value)).filter(Boolean).join(", ");
        if (joined) onChange(joined);
      })
      .catch((e) => setErr(String(e.message ?? e)))
      .finally(() => setLoading(false));
  };
  // Auto-discover on mount only when the field is empty (don't clobber input).
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(() => {
    if (!value) load();
  }, [field.options_source]);

  return (
    <div>
      <div style={{ display: "flex", gap: 8 }}>
        <input
          type="text"
          value={value}
          placeholder={loading ? "discovering…" : field.placeholder}
          onChange={(e) => onChange(e.target.value)}
          style={{ flex: 1 }}
        />
        {field.options_source && onDiscover && (
          <button type="button" className="secondary" style={{ whiteSpace: "nowrap" }} onClick={load}>
            {loading ? "…" : "↻"}
          </button>
        )}
      </div>
      {err && <div className="error">{err}</div>}
    </div>
  );
}

// Renders a dynamic form from a connector's field specs and tracks values.
// `onDiscover` (optional) lets fields with an `options_source` populate their
// choices dynamically from the connected target (NICs, FC HBAs, NVMe sources).
export function DynamicForm({
  fields,
  values,
  onChange,
  onDiscover,
}: {
  fields: FormFieldSpec[];
  values: Record<string, unknown>;
  // Accepts a value or a functional updater (so concurrent field updates — e.g.
  // several discoverable fields auto-loading at once — compose instead of
  // clobbering each other via a stale snapshot).
  onChange: (
    v: Record<string, unknown> | ((prev: Record<string, unknown>) => Record<string, unknown>),
  ) => void;
  onDiscover?: (kind: string) => Promise<DiscoveredOption[]>;
}) {
  const set = (name: string, value: unknown) =>
    onChange((prev) => ({ ...prev, [name]: value }));

  return (
    <>
      {fields.map((f) => {
        const val = (values[f.name] ?? f.default ?? "") as string;
        return (
          <div key={f.name}>
            <label>
              {f.label}
              {f.required ? " *" : ""}
            </label>
            {f.type === "multiselect" ? (
              <MultiSelect
                field={f}
                value={(values[f.name] as string[]) ?? []}
                onChange={(v) => set(f.name, v)}
                onDiscover={onDiscover}
              />
            ) : f.options_source ? (
              <DiscoverableInput
                field={f}
                value={val}
                onChange={(v) => set(f.name, v)}
                onDiscover={onDiscover}
              />
            ) : f.type === "enum" ? (
              <select value={val} onChange={(e) => set(f.name, e.target.value)}>
                {(f.options ?? []).map((o) => (
                  <option key={o} value={o}>
                    {o}
                  </option>
                ))}
              </select>
            ) : f.type === "bool" ? (
              <input
                type="checkbox"
                checked={Boolean(values[f.name] ?? f.default)}
                onChange={(e) => set(f.name, e.target.checked)}
                style={{ width: "auto" }}
              />
            ) : f.type === "text" ? (
              <textarea value={val} placeholder={f.placeholder} onChange={(e) => set(f.name, e.target.value)} />
            ) : (
              <input
                type={f.type === "secret" ? "password" : f.type === "int" ? "number" : "text"}
                value={val}
                placeholder={f.placeholder}
                onChange={(e) => set(f.name, e.target.value)}
              />
            )}
            {f.help && <div className="muted" style={{ fontSize: 12, marginTop: 2 }}>{f.help}</div>}
          </div>
        );
      })}
    </>
  );
}

// Tiny data-loading hook.
export function useAsync<T>(fn: () => Promise<T>, deps: unknown[] = []) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);

  const reload = () => {
    setLoading(true);
    fn()
      .then((d) => {
        setData(d);
        setError(null);
      })
      .catch((e) => setError(String(e.message ?? e)))
      .finally(() => setLoading(false));
  };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  useEffect(reload, deps);
  return { data, error, loading, reload };
}

// Streams a job's logs over WebSocket; calls onDone when the job ends.
export function LiveLog({ jobId, onDone }: { jobId: string; onDone?: () => void }) {
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
        onDone?.();
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
        <h2 style={{ margin: 0 }}>Deployment log</h2>
        <span className="muted">{done ? "finished" : "running…"}</span>
      </div>
      <div className="logs" ref={boxRef}>{lines.join("\n") || "Waiting for output…"}</div>
    </div>
  );
}

// Human-readable status labels for hypervisor deploy state and job status.
const STATUS_LABELS: Record<string, string> = {
  deployed: "Deployed",
  not_deployed: "Not deployed",
  error: "Error",
  succeeded: "Succeeded",
  failed: "Failed",
  running: "Running",
  pending: "Pending",
};

export function StatusBadge({ status }: { status: string }) {
  return (
    <span className={`status-${status}`}>{STATUS_LABELS[status] ?? status}</span>
  );
}
