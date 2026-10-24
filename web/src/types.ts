export interface Resources {
  cpu_millis: number;
  memory_mb: number;
}

export interface ClusterNode {
  id: string;
  address: string;
  api_address?: string;
  capacity: Resources;
  labels?: Record<string, string>;
  status: "ready" | "down" | "draining";
  voter: boolean;
  runtime: string;
  joined_at: number;
  changed_at: number;
  down_reason?: string;
}

export interface Autoscale {
  min: number;
  max: number;
  target_cpu_percent: number;
}

export interface ServiceSpec {
  name: string;
  image: string;
  command?: string[];
  replicas: number;
  resources: Resources;
  port?: number;
  autoscale?: Autoscale;
  sim_demand_millis?: number;
}

export interface ServiceView {
  spec: ServiceSpec;
  created_at: number;
  updated_at: number;
  generation: number;
  running: number;
  pending: number;
}

export type TaskState = "pending" | "assigned" | "running" | "failed" | "lost";

export interface Task {
  id: string;
  service: string;
  node?: string;
  state: TaskState;
  message?: string;
  resources: Resources;
  created_at: number;
  assigned_at?: number;
  running_at?: number;
  restarts: number;
  replaced_node?: string;
}

export interface ClusterEvent {
  seq: number;
  time: number;
  kind: string;
  message: string;
  node?: string;
  service?: string;
  duration_ms?: number;
}

export interface RaftStatus {
  id: string;
  role: string;
  term: number;
  leader: string;
  commit_index: number;
  last_index: number;
  snapshot_index: number;
  voters: number;
  peers?: { id: string; address: string; match_index: number; next_index: number; last_contact_ms: number }[];
}

export interface Live {
  nodes: Record<string, { last_seen_ms: number; usage: Resources }>;
  tasks: Record<string, { cpu_millis: number; memory_mb: number; state: string; restarts: number }>;
  service_cpu_util: Record<string, number>;
  sched_p50_ms: number;
  sched_p99_ms: number;
  recoveries: { at: number; nodes: string[]; duration_ms: number }[];
  incident_open: boolean;
}

export interface ProcessInfo {
  id: string;
  alive: boolean;
  pid?: number;
  api: string;
  role?: string;
  term?: number;
  commit_index?: number;
  leader?: string;
  uptime_ms?: number;
  reachable: boolean;
}

export interface Overview {
  served_by?: string;
  leader?: string;
  raft?: RaftStatus;
  nodes: ClusterNode[] | null;
  services: ServiceView[] | null;
  tasks: Task[] | null;
  events: ClusterEvent[] | null;
  live?: Live;
  applied_index?: number;
  time?: number;
  extra?: { demo?: boolean; processes?: ProcessInfo[]; runtime?: string };
}

export interface Sample {
  t: number;
  running: number;
  desired: number;
  cpu: number;
  cpuCap: number;
  ready: number;
  total: number;
  perService: Record<string, number>;
}
