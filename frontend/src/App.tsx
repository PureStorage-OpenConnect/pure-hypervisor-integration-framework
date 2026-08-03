import { NavLink, Outlet } from "react-router-dom";

const nav = [
  { to: "/wizard", label: "Deploy Wizard" },
  { to: "/arrays", label: "FlashArrays" },
  { to: "/api-keys", label: "API Keys" },
  { to: "/hypervisors", label: "Hypervisors" },
  { to: "/operations", label: "Operations" },
  { to: "/migrate", label: "Migrate VM" },
  { to: "/jobs", label: "Jobs" },
  { to: "/vsphere-plugin", label: "vSphere Plugin" },
  { to: "/settings", label: "Settings" },
];

export default function App() {
  return (
    <div className="layout">
      <aside className="sidebar">
        <div className="brand">
          PHIF
          <span className="brand-sub">Everpure Hypervisor Integration</span>
        </div>
        <nav>
          {nav.map((n) => (
            <NavLink
              key={n.to}
              to={n.to}
              className={({ isActive }) => (isActive ? "nav-item active" : "nav-item")}
            >
              {n.label}
            </NavLink>
          ))}
        </nav>
        <div className="unsupported-note">
          <strong>Unsupported / experimental.</strong> Not a supported product and
          not covered by any support agreement. This tool creates and destroys
          array volumes, reconfigures hosts, and deletes VMs — use at your own
          risk, in a lab, on data you can afford to lose.
        </div>
      </aside>
      <main className="content">
        <Outlet />
      </main>
    </div>
  );
}
