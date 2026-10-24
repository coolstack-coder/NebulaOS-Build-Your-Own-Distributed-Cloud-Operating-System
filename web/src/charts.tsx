import type { ClusterEvent, Sample } from "./types";

const W = 640;
const H = 170;
const PAD = { l: 34, r: 10, t: 12, b: 22 };

function scaleX(t: number, t0: number, t1: number) {
  return PAD.l + ((t - t0) / Math.max(t1 - t0, 1)) * (W - PAD.l - PAD.r);
}
function scaleY(v: number, max: number) {
  return H - PAD.b - (v / Math.max(max, 1)) * (H - PAD.t - PAD.b);
}

function path(points: [number, number][]) {
  return points.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join("");
}

function Axis({ max, t0, t1, unit }: { max: number; t0: number; t1: number; unit?: string }) {
  const ticks = [0, max / 2, max];
  const secs = Math.round((t1 - t0) / 1000);
  return (
    <g className="axis">
      {ticks.map((v) => (
        <g key={v}>
          <line x1={PAD.l} x2={W - PAD.r} y1={scaleY(v, max)} y2={scaleY(v, max)} />
          <text x={PAD.l - 6} y={scaleY(v, max) + 3} textAnchor="end">
            {Math.round(v)}
            {unit}
          </text>
        </g>
      ))}
      <text x={PAD.l} y={H - 5}>-{secs}s</text>
      <text x={W - PAD.r} y={H - 5} textAnchor="end">now</text>
    </g>
  );
}

const markerKinds: Record<string, string> = {
  node_down: "var(--red)",
  leader_elected: "var(--violet)",
  recovered: "var(--green)",
  autoscaled: "var(--amber)",
};

function Markers({ events, t0, t1 }: { events: ClusterEvent[]; t0: number; t1: number }) {
  return (
    <g>
      {events
        .filter((e) => markerKinds[e.kind] && e.time >= t0 && e.time <= t1)
        .map((e) => {
          const x = scaleX(e.time, t0, t1);
          return (
            <g key={e.seq}>
              <line x1={x} x2={x} y1={PAD.t} y2={H - PAD.b} stroke={markerKinds[e.kind]} strokeDasharray="3 3" strokeOpacity={0.8} />
              <circle cx={x} cy={PAD.t} r={3} fill={markerKinds[e.kind]} />
            </g>
          );
        })}
    </g>
  );
}

export function ReplicaChart({ samples, events }: { samples: Sample[]; events: ClusterEvent[] }) {
  if (samples.length < 2) return <div className="chart-empty">collecting samples…</div>;
  const t1 = samples[samples.length - 1].t;
  const t0 = samples[0].t;
  const max = Math.max(4, ...samples.map((s) => Math.max(s.desired, s.running))) * 1.15;
  const run = samples.map((s) => [scaleX(s.t, t0, t1), scaleY(s.running, max)] as [number, number]);
  const des = samples.map((s) => [scaleX(s.t, t0, t1), scaleY(s.desired, max)] as [number, number]);
  const area = `${path(run)}L${run[run.length - 1][0]},${H - PAD.b}L${run[0][0]},${H - PAD.b}Z`;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="chart" role="img" aria-label="Running versus desired replicas">
      <defs>
        <linearGradient id="gRun" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0" stopColor="var(--cyan)" stopOpacity="0.35" />
          <stop offset="1" stopColor="var(--cyan)" stopOpacity="0" />
        </linearGradient>
      </defs>
      <Axis max={max} t0={t0} t1={t1} />
      <Markers events={events} t0={t0} t1={t1} />
      <path d={area} fill="url(#gRun)" />
      <path d={path(des)} className="line-desired" />
      <path d={path(run)} className="line-running" />
    </svg>
  );
}

export function CpuChart({ samples }: { samples: Sample[] }) {
  if (samples.length < 2) return <div className="chart-empty">collecting samples…</div>;
  const t1 = samples[samples.length - 1].t;
  const t0 = samples[0].t;
  const pct = samples.map((s) => (s.cpuCap ? (100 * s.cpu) / s.cpuCap : 0));
  const max = Math.max(25, ...pct) * 1.2;
  const pts = samples.map((s, i) => [scaleX(s.t, t0, t1), scaleY(pct[i], max)] as [number, number]);
  const area = `${path(pts)}L${pts[pts.length - 1][0]},${H - PAD.b}L${pts[0][0]},${H - PAD.b}Z`;
  return (
    <svg viewBox={`0 0 ${W} ${H}`} className="chart" role="img" aria-label="Cluster CPU utilisation">
      <defs>
        <linearGradient id="gCpu" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0" stopColor="var(--violet)" stopOpacity="0.45" />
          <stop offset="1" stopColor="var(--violet)" stopOpacity="0" />
        </linearGradient>
      </defs>
      <Axis max={max} t0={t0} t1={t1} unit="%" />
      <path d={area} fill="url(#gCpu)" />
      <path d={path(pts)} className="line-cpu" />
    </svg>
  );
}

export function Sparkline({ values, color, max }: { values: number[]; color: string; max?: number }) {
  if (values.length < 2) return <svg className="spark" />;
  const m = max ?? Math.max(...values, 1);
  const w = 90;
  const h = 24;
  const pts = values.map((v, i) => [(i / (values.length - 1)) * w, h - 2 - (Math.min(v, m) / m) * (h - 4)] as [number, number]);
  return (
    <svg className="spark" viewBox={`0 0 ${w} ${h}`}>
      <path d={path(pts)} fill="none" stroke={color} strokeWidth={1.6} />
    </svg>
  );
}
