import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { api } from "./api";
import { CpuChart, ReplicaChart, Sparkline } from "./charts";
import type { ClusterEvent, ClusterNode, Overview, ProcessInfo, Sample, ServiceView, Task } from "./types";

const PALETTE = ["#a78bfa", "#22d3ee", "#fbbf24", "#34d399", "#f472b6", "#60a5fa", "#fb923c", "#a3e635"];
const HISTORY_MS = 90_000;

function useServiceColors(services: ServiceView[]) {
  return useMemo(() => {
    const m: Record<string, string> = {};
    [...services].map((s) => s.spec.name).sort().forEach((n, i) => (m[n] = PALETTE[i % PALETTE.length]));
    return m;
  }, [services]);
}

function fmtAgo(ms: number) {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

function fmtTime(ms: number) {
  const d = new Date(ms);
  return d.toLocaleTimeString([], { hour12: false }) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

type Health = { label: string; tone: "ok" | "warn" | "bad" | "info"; detail: string };

function health(o: Overview | null, err: boolean): Health {
  if (!o || err || !o.leader) return { label: "No leader", tone: "bad", detail: "Raft is electing a leader…" };
  const nodes = o.nodes ?? [];
  // The demo supervisor knows a process died before the cluster does; show
  // that window explicitly so you can watch detection happen.
  const procs = o.extra?.processes ?? [];
  const leaderProc = procs.find((p) => p.id === o.leader);
  if (leaderProc && !leaderProc.alive) return { label: "Leader lost", tone: "bad", detail: "followers will time out and elect a new one" };
  const silent = procs.filter((p) => !p.alive && nodes.find((n) => n.id === p.id)?.status !== "down").map((p) => p.id);
  if (silent.length) return { label: "Failure suspected", tone: "warn", detail: `${silent.join(", ")} silent · waiting for heartbeat timeout` };
  const down = nodes.filter((n) => n.status === "down").length;
  const desired = (o.services ?? []).reduce((a, s) => a + s.spec.replicas, 0);
  const running = (o.services ?? []).reduce((a, s) => a + s.running, 0);
  if (o.live?.incident_open || running < desired)
    return { label: "Self-healing", tone: "warn", detail: `${running}/${desired} replicas running` };
  if (down > 0) return { label: "Degraded", tone: "info", detail: `${down} node${down > 1 ? "s" : ""} down · all workloads healthy` };
  return { label: "Healthy", tone: "ok", detail: "all nodes and replicas up" };
}

// ---------------------------------------------------------------- app

export default function App() {
  const [ov, setOv] = useState<Overview | null>(null);
  const [err, setErr] = useState(false);
  const [samples, setSamples] = useState<Sample[]>([]);
  const [svcHist, setSvcHist] = useState<Record<string, number[]>>({});
  const [toast, setToast] = useState<{ msg: string; bad?: boolean } | null>(null);
  const toastTimer = useRef<number>();

  const notify = useCallback((msg: string, bad = false) => {
    setToast({ msg, bad });
    window.clearTimeout(toastTimer.current);
    toastTimer.current = window.setTimeout(() => setToast(null), 3200);
  }, []);

  useEffect(() => {
    let alive = true;
    const tick = async () => {
      const ctl = new AbortController();
      const timer = window.setTimeout(() => ctl.abort(), 1500);
      try {
        const o = await api.overview(ctl.signal);
        if (!alive) return;
        setOv(o);
        setErr(false);
        const nodes = o.nodes ?? [];
        const services = o.services ?? [];
        const now = Date.now();
        const s: Sample = {
          t: now,
          running: services.reduce((a, x) => a + x.running, 0),
          desired: services.reduce((a, x) => a + x.spec.replicas, 0),
          cpu: Object.values(o.live?.nodes ?? {}).reduce((a, n) => a + n.usage.cpu_millis, 0),
          cpuCap: nodes.filter((n) => n.status !== "down").reduce((a, n) => a + n.capacity.cpu_millis, 0),
          ready: nodes.filter((n) => n.status === "ready").length,
          total: nodes.length,
          perService: Object.fromEntries(services.map((x) => [x.spec.name, x.running])),
        };
        setSamples((prev) => [...prev.filter((p) => now - p.t < HISTORY_MS), s]);
        if (o.live) {
          setSvcHist((prev) => {
            const next: Record<string, number[]> = {};
            for (const x of services) {
              const v = (o.live?.service_cpu_util[x.spec.name] ?? 0) * 100;
              next[x.spec.name] = [...(prev[x.spec.name] ?? []), v].slice(-60);
            }
            return next;
          });
        }
      } catch {
        if (alive) setErr(true);
      } finally {
        window.clearTimeout(timer);
      }
    };
    tick();
    const id = window.setInterval(tick, 500);
    return () => {
      alive = false;
      window.clearInterval(id);
    };
  }, []);

  const run = useCallback(
    async (label: string, fn: () => Promise<Record<string, string>>) => {
      try {
        const r = await fn();
        notify(r.ok ?? label);
      } catch (e) {
        notify(`${label} failed: ${(e as Error).message}`, true);
      }
    },
    [notify],
  );

  const services = ov?.services ?? [];
  const nodes = ov?.nodes ?? [];
  const tasks = ov?.tasks ?? [];
  const events = ov?.events ?? [];
  const colors = useServiceColors(services);
  const h = health(ov, err);
  const demo = !!ov?.extra?.demo;
  const procs = new Map((ov?.extra?.processes ?? []).map((p) => [p.id, p]));

  // In demo mode, show processes that were killed before they ever
  // registered as well.
  const nodeIds = Array.from(new Set([...nodes.map((n) => n.id), ...procs.keys()])).sort((a, b) =>
    a.localeCompare(b, undefined, { numeric: true }),
  );

  return (
    <div className="app">
      <Header ov={ov} health={h} />
      {demo && <ChaosBar ov={ov} run={run} />}
      <main className="grid">
        <section className="panel nodes-panel">
          <div className="panel-head">
            <h2>Nodes</h2>
            <span className="muted">
              {nodes.filter((n) => n.status === "ready").length}/{nodeIds.length} ready · placement by scheduler score
            </span>
          </div>
          <div className="nodes">
            {nodeIds.map((id) => (
              <NodeCard
                key={id}
                id={id}
                node={nodes.find((n) => n.id === id)}
                proc={procs.get(id)}
                leader={ov?.leader}
                tasks={tasks.filter((t) => t.node === id && (t.state === "running" || t.state === "assigned"))}
                ov={ov}
                colors={colors}
                demo={demo}
                run={run}
              />
            ))}
          </div>
        </section>

        <section className="panel">
          <div className="panel-head">
            <h2>Replicas</h2>
            <span className="legend">
              <i className="sw cyan" /> running <i className="sw dashed" /> desired <i className="sw red" /> node down
              <i className="sw violet" /> election <i className="sw green" /> recovered
            </span>
          </div>
          <ReplicaChart samples={samples} events={events} />
        </section>

        <section className="panel">
          <div className="panel-head">
            <h2>Cluster CPU</h2>
            <span className="muted">used ÷ capacity of live nodes</span>
          </div>
          <CpuChart samples={samples} />
        </section>

        <section className="panel services-panel">
          <div className="panel-head">
            <h2>Services</h2>
            <span className="muted">{services.length} deployed</span>
          </div>
          <Services services={services} ov={ov} colors={colors} hist={svcHist} run={run} demo={demo} />
        </section>

        <section className="panel raft-panel">
          <div className="panel-head">
            <h2>Raft consensus</h2>
            <span className="muted">term {ov?.raft?.term ?? "–"}</span>
          </div>
          <RaftPanel ov={ov} procs={[...procs.values()]} />
        </section>

        <section className="panel events-panel">
          <div className="panel-head">
            <h2>Event log</h2>
            <span className="muted">replicated through Raft</span>
          </div>
          <Events events={events} />
        </section>

        <section className="panel recovery-panel">
          <div className="panel-head">
            <h2>Self-healing</h2>
            <span className="muted">node declared down → all replicas running</span>
          </div>
          <Recoveries ov={ov} />
        </section>
      </main>
      {toast && <div className={`toast ${toast.bad ? "bad" : ""}`}>{toast.msg}</div>}
    </div>
  );
}

// ---------------------------------------------------------------- header

function Header({ ov, health: h }: { ov: Overview | null; health: Health }) {
  const services = ov?.services ?? [];
  const desired = services.reduce((a, s) => a + s.spec.replicas, 0);
  const running = services.reduce((a, s) => a + s.running, 0);
  const nodes = ov?.nodes ?? [];
  const last = ov?.live?.recoveries?.at(-1);
  return (
    <header className="header">
      <div className="brand">
        <svg viewBox="0 0 32 32" className="logo" aria-hidden>
          <defs>
            <radialGradient id="lg" cx="50%" cy="45%" r="55%">
              <stop offset="0" stopColor="#c4b5fd" />
              <stop offset=".55" stopColor="#7c3aed" />
              <stop offset="1" stopColor="#1e1b4b" />
            </radialGradient>
          </defs>
          <circle cx="16" cy="16" r="13" fill="url(#lg)" />
          <ellipse cx="16" cy="16" rx="15" ry="5" fill="none" stroke="#67e8f9" strokeWidth="1.4" transform="rotate(-20 16 16)" />
        </svg>
        <div>
          <h1>NebulaOS</h1>
          <p>self-healing distributed cloud orchestrator</p>
        </div>
      </div>
      <div className={`health ${h.tone}`}>
        <span className="dot" />
        <div>
          <strong>{h.label}</strong>
          <small>{h.detail}</small>
        </div>
      </div>
      <div className="stats">
        <Stat label="Leader" value={ov?.leader || "—"} sub={ov?.raft ? `term ${ov.raft.term}` : ""} accent />
        <Stat label="Nodes ready" value={`${nodes.filter((n) => n.status === "ready").length}/${nodes.length}`} />
        <Stat label="Replicas" value={`${running}/${desired}`} />
        <Stat label="Commit index" value={String(ov?.raft?.commit_index ?? "—")} />
        <Stat
          label="Sched p50 / p99"
          value={ov?.live ? `${ov.live.sched_p50_ms.toFixed(0)} / ${ov.live.sched_p99_ms.toFixed(0)}` : "—"}
          sub="ms"
        />
        <Stat label="Last self-heal" value={last ? `${(last.duration_ms / 1000).toFixed(2)}s` : "—"} sub={last ? last.nodes.join(", ") : ""} />
      </div>
    </header>
  );
}

function Stat({ label, value, sub, accent }: { label: string; value: string; sub?: string; accent?: boolean }) {
  return (
    <div className={`stat ${accent ? "accent" : ""}`}>
      <span>{label}</span>
      <strong>{value}</strong>
      {sub ? <small>{sub}</small> : null}
    </div>
  );
}

// ---------------------------------------------------------------- chaos

function ChaosBar({ ov, run }: { ov: Overview | null; run: (l: string, f: () => Promise<Record<string, string>>) => void }) {
  const web = ov?.services?.find((s) => s.spec.name === "web");
  const [load, setLoad] = useState<number | null>(null);
  const server = web?.spec.sim_demand_millis ?? 0;
  const shown = load ?? server;
  // Drop the local override once the cluster has caught up with it.
  useEffect(() => {
    if (load !== null && load === server) setLoad(null);
  }, [load, server]);
  const commit = () => {
    if (load !== null && load !== server) run(`web traffic → ${(load / 1000).toFixed(1)} cores`, () => api.load("web", load));
  };
  return (
    <div className="chaos">
      <span className="chaos-title">Chaos lab</span>
      <button className="btn danger" onClick={() => run("kill leader", api.killLeader)}>
        ⚡ Kill leader
      </button>
      <button className="btn danger" onClick={() => run("kill two", api.killTwo)}>
        ⚡ Kill 2 of 5 nodes
      </button>
      <button className="btn" onClick={() => run("restart all", api.restartAll)}>
        ↻ Restart all
      </button>
      {web && ov?.extra?.runtime === "sim" && (
        <label className="traffic">
          <span>web traffic</span>
          <input
            type="range"
            min={0}
            max={4000}
            step={100}
            value={shown}
            onChange={(e) => setLoad(Number(e.target.value))}
            onPointerUp={commit}
            onKeyUp={commit}
          />
          <code>{(shown / 1000).toFixed(1)} cores</code>
        </label>
      )}
    </div>
  );
}

// ---------------------------------------------------------------- nodes

function NodeCard(props: {
  id: string;
  node?: ClusterNode;
  proc?: ProcessInfo;
  leader?: string;
  tasks: Task[];
  ov: Overview | null;
  colors: Record<string, string>;
  demo: boolean;
  run: (l: string, f: () => Promise<Record<string, string>>) => void;
}) {
  const { id, node, proc, leader, tasks, ov, colors, demo, run } = props;
  const killed = proc && !proc.alive;
  const isLeader = leader === id && !killed;
  const down = killed || node?.status === "down";
  const draining = node?.status === "draining";
  let role = proc?.role ?? (isLeader ? "leader" : node?.voter ? "follower" : "worker");
  if (isLeader) role = "leader";
  const usage = ov?.live?.nodes?.[id]?.usage;
  const cap = node?.capacity ?? { cpu_millis: 1, memory_mb: 1 };
  const alloc = tasks.reduce((a, t) => a + t.resources.cpu_millis, 0);
  const allocMem = tasks.reduce((a, t) => a + t.resources.memory_mb, 0);
  const zone = node?.labels?.zone;
  const cls = ["node", isLeader && "leader", down && "down", draining && "draining"].filter(Boolean).join(" ");

  let statusText = node?.status ?? "joining";
  if (killed) statusText = node?.status === "down" ? "down · process killed" : "process killed · detecting…";

  return (
    <div className={cls}>
      <div className="node-top">
        <div className="node-name">
          <span className="node-id">{id}</span>
          {zone && <span className="chip">{zone}</span>}
        </div>
        <span className={`role ${down ? "down" : role}`}>{down ? "offline" : role}</span>
      </div>
      <div className="node-status">
        <span className={`pill ${down ? "bad" : draining ? "warn" : "ok"}`}>{statusText}</span>
        {proc?.term ? <span className="muted mono">term {proc.term}</span> : null}
        {node?.down_reason && down ? <span className="muted small">{node.down_reason}</span> : null}
      </div>
      <Meter label="CPU" used={down ? 0 : usage?.cpu_millis ?? 0} alloc={alloc} cap={cap.cpu_millis} unit="m" />
      <Meter label="MEM" used={down ? 0 : usage?.memory_mb ?? 0} alloc={allocMem} cap={cap.memory_mb} unit="MB" />
      <div className="tasks">
        {tasks.length === 0 && <span className="muted small">{down ? "workloads evacuated" : "no tasks"}</span>}
        {tasks.map((t) => (
          <span
            key={t.id}
            className={`task ${t.state} ${t.replaced_node ? "moved" : ""}`}
            style={{ ["--c" as string]: colors[t.service] }}
            title={`${t.id} · ${t.state}${t.replaced_node ? ` · moved from ${t.replaced_node}` : ""}`}
          >
            {t.service}
            {t.replaced_node && <em>←{t.replaced_node}</em>}
          </span>
        ))}
      </div>
      <div className="node-actions">
        {demo &&
          (killed ? (
            <button className="btn small" onClick={() => run(`start ${id}`, () => api.start(id))}>
              ▶ Power on
            </button>
          ) : (
            <button className="btn small danger" onClick={() => run(`kill ${id}`, () => api.kill(id))}>
              ⏻ Kill
            </button>
          ))}
        {!down &&
          (draining ? (
            <button className="btn small" onClick={() => run(`uncordon ${id}`, () => api.uncordon(id))}>
              Uncordon
            </button>
          ) : (
            <button className="btn small ghost" onClick={() => run(`drain ${id}`, () => api.drain(id))}>
              Drain
            </button>
          ))}
      </div>
    </div>
  );
}

function Meter({ label, used, alloc, cap, unit }: { label: string; used: number; alloc: number; cap: number; unit: string }) {
  const u = Math.min(100, (100 * used) / cap);
  const a = Math.min(100, (100 * alloc) / cap);
  return (
    <div className="meter">
      <div className="meter-label">
        <span>{label}</span>
        <span className="mono">
          {Math.round(used)}
          {unit} <span className="muted">/ {cap}{unit}</span>
        </span>
      </div>
      <div className="bar">
        <div className="bar-alloc" style={{ width: `${a}%` }} />
        <div className="bar-used" style={{ width: `${u}%` }} />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------- services

function Services(props: {
  services: ServiceView[];
  ov: Overview | null;
  colors: Record<string, string>;
  hist: Record<string, number[]>;
  demo: boolean;
  run: (l: string, f: () => Promise<Record<string, string>>) => void;
}) {
  const { services, ov, colors, hist, run } = props;
  if (!services.length) return <div className="empty">No services yet. Deploy one with <code>nebula ctl deploy</code>.</div>;
  return (
    <table className="svc">
      <thead>
        <tr>
          <th>Service</th>
          <th>Replicas</th>
          <th>CPU util</th>
          <th>Autoscale</th>
          <th />
        </tr>
      </thead>
      <tbody>
        {services.map((s) => {
          const util = (ov?.live?.service_cpu_util?.[s.spec.name] ?? 0) * 100;
          const a = s.spec.autoscale;
          const ok = s.running >= s.spec.replicas;
          return (
            <tr key={s.spec.name}>
              <td>
                <div className="svc-name">
                  <i className="swatch" style={{ background: colors[s.spec.name] }} />
                  <div>
                    <strong>{s.spec.name}</strong>
                    <small className="mono">{s.spec.image || s.spec.command?.join(" ")}</small>
                  </div>
                </div>
              </td>
              <td>
                <span className={`replicas ${ok ? "ok" : "warn"}`}>
                  {s.running}/{s.spec.replicas}
                </span>
                <div className="dots">
                  {Array.from({ length: s.spec.replicas }).map((_, i) => (
                    <i key={i} className={i < s.running ? "on" : ""} style={{ ["--c" as string]: colors[s.spec.name] }} />
                  ))}
                </div>
              </td>
              <td>
                <div className="util">
                  <Sparkline values={hist[s.spec.name] ?? []} color={colors[s.spec.name]} max={160} />
                  <span className={`mono ${a && util > a.target_cpu_percent * 1.1 ? "hot" : ""}`}>{util.toFixed(0)}%</span>
                </div>
              </td>
              <td>
                {a ? (
                  <div className="as">
                    <span className="mono">
                      {a.min}–{a.max}
                    </span>
                    <small>target {a.target_cpu_percent}%</small>
                  </div>
                ) : (
                  <span className="muted">off</span>
                )}
              </td>
              <td className="actions">
                <button className="btn icon" title="scale down" onClick={() => run(`scale ${s.spec.name}`, () => api.scale(s.spec.name, Math.max(0, s.spec.replicas - 1)))}>
                  −
                </button>
                <button className="btn icon" title="scale up" onClick={() => run(`scale ${s.spec.name}`, () => api.scale(s.spec.name, s.spec.replicas + 1))}>
                  +
                </button>
              </td>
            </tr>
          );
        })}
      </tbody>
    </table>
  );
}

// ---------------------------------------------------------------- raft

function RaftPanel({ ov, procs }: { ov: Overview | null; procs: ProcessInfo[] }) {
  const peers = ov?.raft?.peers ?? [];
  const commit = ov?.raft?.commit_index ?? 0;
  const last = ov?.raft?.last_index ?? 0;
  const ids = procs.length ? procs.map((p) => p.id) : [ov?.raft?.id ?? "", ...peers.map((p) => p.id)];
  const n = ids.length || 1;
  const R = 62;
  return (
    <div className="raft">
      <svg viewBox="-100 -100 200 200" className="ring" role="img" aria-label="Raft cluster ring">
        <circle r={R} className="ring-track" />
        {ids.map((id, i) => {
          const ang = (i / n) * Math.PI * 2 - Math.PI / 2;
          const x = Math.cos(ang) * R;
          const y = Math.sin(ang) * R;
          const p = procs.find((q) => q.id === id);
          const isLeader = ov?.leader === id && p?.alive !== false;
          const dead = p ? !p.alive || !p.reachable : false;
          return (
            <g key={id}>
              {isLeader &&
                ids
                  .filter((o) => o !== id)
                  .map((o) => {
                    const j = ids.indexOf(o);
                    const a2 = (j / n) * Math.PI * 2 - Math.PI / 2;
                    const op = procs.find((q) => q.id === o);
                    const off = op ? !op.alive || !op.reachable : false;
                    return (
                      <line
                        key={o}
                        x1={x}
                        y1={y}
                        x2={Math.cos(a2) * R}
                        y2={Math.sin(a2) * R}
                        className={`hb ${off ? "off" : ""}`}
                      />
                    );
                  })}
              <circle cx={x} cy={y} r={isLeader ? 15 : 12} className={`rnode ${isLeader ? "leader" : ""} ${dead ? "dead" : ""}`} />
              <text x={x} y={y + 4} textAnchor="middle" className="rlabel">
                {id}
              </text>
            </g>
          );
        })}
        <text y={-4} textAnchor="middle" className="rterm">
          term {ov?.raft?.term ?? "–"}
        </text>
        <text y={12} textAnchor="middle" className="rsub">
          {ov?.leader ? `leader ${ov.leader}` : "no leader"}
        </text>
      </svg>
      <div className="peers">
        <div className="peer-row head">
          <span>peer</span>
          <span>match / last</span>
          <span>contact</span>
        </div>
        {peers.map((p) => {
          const behind = last - p.match_index;
          const stale = p.last_contact_ms > 1000;
          return (
            <div className="peer-row" key={p.id}>
              <span className="mono">{p.id}</span>
              <span className="peer-bar">
                <i style={{ width: `${last ? (100 * p.match_index) / last : 0}%` }} className={stale ? "stale" : ""} />
                <small className="mono">{stale ? "unreachable" : behind === 0 ? "in sync" : `${behind} behind`}</small>
              </span>
              <span className={`mono ${stale ? "bad-t" : "muted"}`}>{stale ? `${(p.last_contact_ms / 1000).toFixed(1)}s` : `${p.last_contact_ms}ms`}</span>
            </div>
          );
        })}
        <div className="raft-foot muted small mono">
          commit {commit} · log {last} · snapshot @{ov?.raft?.snapshot_index ?? 0}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------- events

const kindTone: Record<string, string> = {
  node_down: "bad",
  task_failed: "bad",
  recovery_incomplete: "bad",
  leader_elected: "violet",
  recovered: "ok",
  node_recovered: "ok",
  node_joined: "ok",
  autoscaled: "warn",
  service_scaled: "warn",
  node_drained: "warn",
};

function Events({ events }: { events: ClusterEvent[] }) {
  const shown = [...events].reverse().filter((e) => e.kind !== "task_scheduled" || e.message.includes("rescheduled")).slice(0, 60);
  return (
    <ol className="events">
      {shown.map((e) => (
        <li key={e.seq} className={kindTone[e.kind] ?? ""}>
          <time className="mono">{fmtTime(e.time)}</time>
          <span className="kind">{e.kind.replace(/_/g, " ")}</span>
          <span className="msg">{e.message}</span>
        </li>
      ))}
    </ol>
  );
}

function Recoveries({ ov }: { ov: Overview | null }) {
  const evs = (ov?.events ?? []).filter((e) => e.kind === "recovered" && e.duration_ms);
  if (!evs.length)
    return (
      <div className="empty">
        No failures yet. Kill a node and NebulaOS will detect it, reschedule its workloads and record how long it took.
      </div>
    );
  const max = Math.max(...evs.map((e) => e.duration_ms ?? 0), 1);
  return (
    <div className="recov">
      {evs.slice(-8).map((e) => (
        <div key={e.seq} className="recov-row">
          <span className="muted small">{fmtAgo(e.time)}</span>
          <div className="recov-bar">
            <i style={{ width: `${(100 * (e.duration_ms ?? 0)) / max}%` }} />
          </div>
          <span className="mono">{e.duration_ms}ms</span>
        </div>
      ))}
    </div>
  );
}
