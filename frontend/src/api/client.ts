// Typed API client + types shared across pages.

export interface FormFieldSpec {
  name: string;
  label: string;
  type: "string" | "secret" | "int" | "bool" | "enum" | "multiselect" | "size" | "text";
  required: boolean;
  default: unknown;
  options: string[] | null;
  options_source: string | null; // dynamic dropdown source: "nics" | "nvme_sources" | "fc_hbas"
  help: string;
  placeholder: string;
}

export interface DiscoveredOption {
  value: string;
  label: string;
  [k: string]: unknown;
}

export interface ActionSpec {
  capability: string;
  id: string;
  label: string;
  description: string;
  fields: FormFieldSpec[];
  destructive: boolean;
  long_running: boolean;
}

export interface ConnectorDescriptor {
  key: string;
  name: string;
  description: string;
  maturity: string;
  capabilities: string[];
  protocols: string[];
  target_schema: FormFieldSpec[];
  actions: ActionSpec[];
  wizard_steps: string[];
}

export interface Array_ {
  id: string;
  name: string;
  mgmt_endpoint: string;
  verify_ssl: boolean;
  info: Record<string, unknown>;
}

export interface ApiKey {
  id: string;
  array_id: string;
  array_user: string;
  purpose: string;
  token?: string | null;
}

export interface Hypervisor {
  id: string;
  name: string;
  connector_key: string;
  connection: Record<string, unknown>;
  array_id: string | null;
  status: string;
  state: Record<string, unknown>;
}

// Per-node cluster-membership readiness verdict from assess/reconcile.
export interface ClusterHost {
  node: string;
  host: string;
  ready: boolean;
  reasons: string[];
}

// state.cluster_assessment shape (populated by the monitor and assess action).
export interface ClusterAssessment {
  nodes?: string[];
  new_hosts?: ClusterHost[];
  departed_hosts?: string[];
  not_ready?: ClusterHost[];
}

export interface MonitoringSettings {
  enabled: boolean;
  interval_seconds: number;
}

export interface SettingsOut {
  monitoring: MonitoringSettings;
}

export interface Job {
  id: string;
  action: string;
  hypervisor_id: string | null;
  connector_key: string | null;
  status: string;
  result: Record<string, unknown>;
  logs: string;
}

// --- Migration ---
export interface VmSummary {
  id: string;
  name: string;
  power_state: string;
  vcpus?: number | null;
  memory_bytes?: number | null;
  disk_count?: number | null;
  nic_count?: number | null;
}

export interface NetworkSummary {
  id: string;
  name: string;
  kind?: string;
}

export interface PlacementStorage {
  id: string;
  name: string;
  kind: string;
}

export interface Placement {
  cluster: { id: string; name: string };
  storage: PlacementStorage[];
}

export interface DiskIdentity {
  fa_volume: string;
  serial?: string | null;
  wwid?: string | null;
  size_bytes?: number | null;
}

export interface DiskSpec {
  identity: DiskIdentity;
  bus: string;
  order: number;
  boot: boolean;
  source_ref: string;
}

export interface NicSpec {
  mac: string;
  source_network: string;
  model: string;
  order: number;
}

export interface VmSpec {
  name: string;
  source_ref: string;
  vcpus: number;
  memory_bytes: number;
  firmware: string;
  secure_boot: boolean;
  disks: DiskSpec[];
  nics: NicSpec[];
  guest_os_hint: string;
  raw: Record<string, unknown>;
}

export interface Migration {
  id: string;
  source_hypervisor_id: string;
  dest_hypervisor_id: string;
  vm_ref: string;
  dest_vm_ref: string | null;
  network_map: Record<string, string>;
  spec: Record<string, unknown>;
  phase: string;
  status: string;
  job_id: string | null;
  result: Record<string, unknown>;
}

export interface MigrationGroupMember {
  source_hypervisor_id: string;
  dest_hypervisor_id: string;
  vm_ref: string;
  network_map?: Record<string, string>;
  options?: Record<string, unknown>;
  status?: string;
  migration_id?: string | null;
  job_id?: string | null;
  error?: string | null;
}

export interface MigrationGroup {
  id: string;
  name: string;
  status: string;
  scheduled_at: string | null;
  concurrency: number;
  continue_on_error: boolean;
  options: Record<string, unknown>;
  members: MigrationGroupMember[];
  created_at: string | null;
  started_at: string | null;
  finished_at: string | null;
}

export interface VspherePluginImages {
  server: boolean;
  proxy: boolean;
}

export interface VspherePluginInstallProgress {
  active: boolean;
  phase: string;
  detail: string;
  percent: number | null;   // null = indeterminate
  image: string | null;     // "server" | "proxy"
  error: string | null;
  done: boolean;
}

export interface VspherePluginStatus {
  server: string;       // "running" | "exited" | "missing"
  proxy: string;
  images: VspherePluginImages;
  /** Configured image references, e.g. everpure/client-plugin-vsphere:latest */
  image_refs?: VspherePluginImageRefs;
  secret_exists: boolean;
  can_start: boolean;
  available: boolean;
  install?: VspherePluginInstallProgress;
  error?: string;
}

export interface VspherePluginImageRefs {
  server: string;
  proxy: string;
}

export interface VspherePluginUploadResult {
  loaded: string[];
  errors: string[];
}

const BASE = "/api";

// Hard ceiling so a stuck/half-sent response (e.g. a backend 500 that aborts
// mid-stream) surfaces as a visible error instead of an indefinite hang.
const REQUEST_TIMEOUT_MS = 90_000;

async function req<T>(method: string, path: string, body?: unknown): Promise<T> {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), REQUEST_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(BASE + path, {
      method,
      headers: body ? { "Content-Type": "application/json" } : {},
      body: body ? JSON.stringify(body) : undefined,
      signal: ctrl.signal,
    });
  } catch (e) {
    // Network error, CORS failure, or our own abort/timeout — never swallow it.
    if ((e as Error).name === "AbortError") {
      throw new Error(
        `Request to ${path} timed out after ${REQUEST_TIMEOUT_MS / 1000}s`,
      );
    }
    throw new Error(`Request to ${path} failed: ${(e as Error).message}`);
  } finally {
    clearTimeout(timer);
  }
  if (!res.ok) {
    let detail = res.statusText;
    try {
      detail = (await res.json()).detail ?? detail;
    } catch {
      // Non-JSON error body (e.g. a plain-text 500) — keep the status text.
    }
    throw new Error(detail || `HTTP ${res.status}`);
  }
  if (res.status === 204) return undefined as T;
  return res.json();
}

// Multipart upload with progress callback — uses XHR so onprogress fires.
// onProgress receives 0–100 (upload phase); after 100 the server is loading
// the image into Docker (indeterminate phase).
export function reqUpload<T>(
  path: string,
  body: FormData,
  onProgress?: (pct: number) => void,
): Promise<T> {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", BASE + path);
    xhr.timeout = 10 * 60 * 1000; // 10 minutes
    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable && onProgress) {
        onProgress(Math.round((e.loaded / e.total) * 100));
      }
    };
    xhr.ontimeout = () => reject(new Error(`Upload to ${path} timed out`));
    xhr.onerror = () => reject(new Error(`Upload to ${path} failed`));
    xhr.onload = () => {
      if (xhr.status === 204) { resolve(undefined as T); return; }
      let body: unknown;
      try { body = JSON.parse(xhr.responseText); } catch { body = null; }
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(body as T);
      } else {
        const detail =
          (body && typeof body === "object" && "detail" in body)
            ? String((body as { detail: unknown }).detail)
            : xhr.statusText || `HTTP ${xhr.status}`;
        reject(new Error(detail));
      }
    };
    xhr.send(body);
  });
}

export const api = {
  // Arrays
  listArrays: () => req<Array_[]>("GET", "/arrays"),
  addArray: (b: unknown) => req<Array_>("POST", "/arrays", b),
  validateArray: (id: string) => req<Array_>("POST", `/arrays/${id}/validate`),
  deleteArray: (id: string) => req<void>("DELETE", `/arrays/${id}`),
  // API keys
  listApiKeys: () => req<ApiKey[]>("GET", "/api-keys"),
  mintApiKey: (b: unknown) => req<ApiKey>("POST", "/api-keys", b),
  rotateApiKey: (id: string) => req<ApiKey>("POST", `/api-keys/${id}/rotate`),
  deleteApiKey: (id: string) => req<void>("DELETE", `/api-keys/${id}`),
  // Connectors
  listConnectors: () => req<ConnectorDescriptor[]>("GET", "/connectors"),
  getConnector: (k: string) => req<ConnectorDescriptor>("GET", `/connectors/${k}`),
  // Hypervisors
  listHypervisors: () => req<Hypervisor[]>("GET", "/hypervisors"),
  addHypervisor: (b: unknown) => req<Hypervisor>("POST", "/hypervisors", b),
  updateHypervisor: (id: string, b: unknown) =>
    req<Hypervisor>("PATCH", `/hypervisors/${id}`, b),
  validateHypervisor: (id: string) =>
    req<{ success: boolean; message: string; logs: string[] }>(
      "POST",
      `/hypervisors/${id}/validate`,
    ),
  setHypervisorMonitoring: (id: string, enabled: boolean) =>
    req<Hypervisor>("PATCH", `/hypervisors/${id}/monitoring`, { enabled }),
  runOperation: (id: string, b: unknown) =>
    req<{ job_id: string }>("POST", `/hypervisors/${id}/operations`, b),
  discoverOptions: (id: string, kind: string) =>
    req<{ kind: string; options: DiscoveredOption[] }>(
      "GET",
      `/hypervisors/${id}/discover/${kind}`,
    ),
  wizard: (b: unknown) =>
    req<{ hypervisor_id: string; job_id: string }>("POST", "/hypervisors/wizard", b),
  wizardPrepare: (b: unknown) =>
    req<{ hypervisor_id: string }>("POST", "/hypervisors/wizard/prepare", b),
  wizardRun: (id: string, b: unknown) =>
    req<{ job_id: string }>("POST", `/hypervisors/${id}/wizard/run`, b),
  listNodes: (id: string) =>
    req<{ nodes: { name: string; host: string; info: Record<string, unknown> }[] }>(
      "GET",
      `/hypervisors/${id}/nodes`,
    ),
  deleteHypervisor: (id: string) => req<void>("DELETE", `/hypervisors/${id}`),
  // Migration
  listVms: (id: string) => req<VmSummary[]>("GET", `/hypervisors/${id}/vms`),
  listNetworks: (id: string) => req<NetworkSummary[]>("GET", `/hypervisors/${id}/networks`),
  listPlacements: (id: string) => req<Placement[]>("GET", `/hypervisors/${id}/placements`),
  getVmSpec: (id: string, vmRef: string) =>
    req<VmSpec>("GET", `/hypervisors/${id}/vms/${encodeURIComponent(vmRef)}/spec`),
  createMigration: (b: unknown) =>
    req<{ migration_id: string; job_id: string }>("POST", "/migrations", b),
  listMigrations: () => req<Migration[]>("GET", "/migrations"),
  getMigration: (id: string) => req<Migration>("GET", `/migrations/${id}`),
  migrationPrecheck: (sourceId: string, destId: string) =>
    req<{ cross_array: boolean; connection_exists: boolean; dest_array_name: string; needs_authorization: boolean }>(
      "GET",
      `/migrations/precheck?source_hypervisor_id=${sourceId}&dest_hypervisor_id=${destId}`,
    ),
  // Migration groups (batch + scheduled migrations)
  createMigrationGroup: (b: unknown) =>
    req<{ group_id: string; name: string; status: string; scheduled_at: string | null }>(
      "POST", "/migration-groups", b),
  listMigrationGroups: () => req<MigrationGroup[]>("GET", "/migration-groups"),
  getMigrationGroup: (id: string) => req<MigrationGroup>("GET", `/migration-groups/${id}`),
  runMigrationGroup: (id: string) =>
    req<{ group_id: string; status: string }>("POST", `/migration-groups/${id}/run`),
  cancelMigrationGroup: (id: string) =>
    req<{ group_id: string; status: string }>("POST", `/migration-groups/${id}/cancel`),
  deleteMigrationGroup: (id: string) => req<void>("DELETE", `/migration-groups/${id}`),
  listArrayConnections: (arrayId: string) =>
    req<{ connections: { name: string; management_address: string; status: string; type: string }[] }>(
      "GET",
      `/arrays/${arrayId}/connections`,
    ),
  deleteArrayConnection: (arrayId: string, name: string) =>
    req<void>("DELETE", `/arrays/${arrayId}/connections/${encodeURIComponent(name)}`),
  // vSphere Plugin containers
  vspherePluginStatus: () => req<VspherePluginStatus>("GET", "/vsphere-plugin/status"),
  vspherePluginUpload: (formData: FormData, onProgress?: (pct: number) => void) =>
    reqUpload<VspherePluginUploadResult>("/vsphere-plugin/images", formData, onProgress),
  // Both return 202 at once; follow progress via vspherePluginStatus().install
  vspherePluginInstall: () =>
    req<{ install: VspherePluginInstallProgress }>("POST", "/vsphere-plugin/install"),
  vspherePluginPull: () =>
    req<{ install: VspherePluginInstallProgress }>("POST", "/vsphere-plugin/pull"),
  vspherePluginStart: () => req<VspherePluginStatus>("POST", "/vsphere-plugin/start"),
  vspherePluginStop: () => req<VspherePluginStatus>("POST", "/vsphere-plugin/stop"),
  // Settings
  getSettings: () => req<SettingsOut>("GET", "/settings"),
  patchMonitoring: (b: Partial<MonitoringSettings>) =>
    req<MonitoringSettings>("PATCH", "/settings/monitoring", b),
  // Jobs
  listJobs: () => req<Job[]>("GET", "/jobs"),
  getJob: (id: string) => req<Job>("GET", `/jobs/${id}`),
  jobLogSocket: (id: string) => {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    return new WebSocket(`${proto}://${location.host}${BASE}/jobs/${id}/logs`);
  },
};
