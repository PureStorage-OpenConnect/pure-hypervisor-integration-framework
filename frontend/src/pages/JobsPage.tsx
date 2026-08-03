import { Fragment, useState } from "react";
import { api } from "../api/client";
import { useAsync, LiveLog } from "../components";

export default function JobsPage() {
  const { data: jobs, reload } = useAsync(() => api.listJobs());
  const [open, setOpen] = useState<string | null>(null);

  return (
    <div>
      <div className="toolbar">
        <h1>Jobs</h1>
        <button className="secondary" onClick={reload}>Refresh</button>
      </div>
      <p className="subtitle">History of deploy and day-2 operations.</p>

      <div className="card" style={{ padding: 0 }}>
        <table>
          <thead>
            <tr>
              <th>Action</th>
              <th>Connector</th>
              <th>Status</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {(jobs ?? []).map((j) => (
              <Fragment key={j.id}>
                <tr>
                  <td>{j.action}</td>
                  <td className="muted">{j.connector_key ?? "—"}</td>
                  <td><span className={`status-${j.status}`}>{j.status}</span></td>
                  <td style={{ textAlign: "right" }}>
                    <button
                      className="secondary"
                      onClick={() => setOpen(open === j.id ? null : j.id)}
                    >
                      {open === j.id ? "Hide log" : "View log"}
                    </button>
                  </td>
                </tr>
                {open === j.id && (
                  <tr>
                    <td colSpan={4} style={{ background: "var(--panel)" }}>
                      {/* Log streams inline directly below the row that was opened. */}
                      <LiveLog jobId={j.id} onDone={reload} />
                    </td>
                  </tr>
                )}
              </Fragment>
            ))}
            {jobs?.length === 0 && (
              <tr><td colSpan={4} className="muted">No jobs yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
