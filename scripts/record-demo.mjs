// Records the NebulaOS demo video by driving the real dashboard with
// Playwright. Expects `nebula demo` to be running on :9000.
//
//   npm i playwright@1.49.1 && npx playwright install chromium
//   node scripts/record-demo.mjs out-dir
//
// Captions are injected into the page so the video explains itself.
import { chromium } from "playwright";
import fs from "node:fs";
import path from "node:path";

const BASE = process.env.NEBULA_UI || "http://127.0.0.1:9000";
const OUT = process.argv[2] || "recording";
const W = 1600;
const H = 1000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

fs.mkdirSync(OUT, { recursive: true });
const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: W, height: H },
  deviceScaleFactor: 1,
  recordVideo: { dir: OUT, size: { width: W, height: H } },
  colorScheme: "dark",
});
const page = await context.newPage();
const marks = [];
const t0 = Date.now();
const mark = (label) => marks.push({ t: (Date.now() - t0) / 1000, label });

async function overlayCSS() {
  await page.addStyleTag({
    content: `
    #rec-cap { position: fixed; left: 50%; bottom: 26px; transform: translateX(-50%); z-index: 9999;
      max-width: 1180px; padding: 14px 22px; border-radius: 14px; font: 600 19px/1.4 Inter, system-ui, sans-serif;
      color: #f5f3ff; background: rgba(12, 10, 32, .92); border: 1px solid rgba(167,139,250,.55);
      box-shadow: 0 18px 60px rgba(0,0,0,.6), 0 0 40px rgba(124,58,237,.25); transition: opacity .35s; text-align: center; }
    #rec-cap small { display:block; font-weight: 400; font-size: 15px; color: #c4b5fd; margin-top: 3px; }
    #rec-cap .step { color: #22d3ee; margin-right: 8px; }
    #rec-card { position: fixed; inset: 0; z-index: 10000; display: grid; place-items: center; text-align: center;
      background: radial-gradient(900px 500px at 50% 40%, rgba(124,58,237,.45), transparent 70%), #070912;
      font-family: Inter, system-ui, sans-serif; color: #e6e9f7; transition: opacity .6s; }
    #rec-card h1 { font-size: 64px; margin: 0; letter-spacing: -.03em; }
    #rec-card h2 { font-size: 24px; margin: 10px 0 26px; font-weight: 500; color: #c4b5fd; }
    #rec-card p { font-size: 18px; color: #8b93b8; margin: 6px 0; }
    #rec-card code { font-family: "JetBrains Mono", monospace; color: #22d3ee; font-size: 17px; }
    #rec-click { position: fixed; z-index: 9998; width: 46px; height: 46px; margin: -23px 0 0 -23px; border-radius: 50%;
      border: 3px solid #fbbf24; pointer-events: none; animation: recping .7s ease-out forwards; }
    @keyframes recping { from { transform: scale(.4); opacity: 1 } to { transform: scale(1.6); opacity: 0 } }`,
  });
}

async function caption(step, text, sub = "") {
  mark(text);
  await page.evaluate(
    ({ step, text, sub }) => {
      let el = document.getElementById("rec-cap");
      if (!el) {
        el = document.createElement("div");
        el.id = "rec-cap";
        document.body.appendChild(el);
      }
      el.innerHTML = `${step ? `<span class="step">${step}</span>` : ""}${text}${sub ? `<small>${sub}</small>` : ""}`;
      el.style.opacity = "1";
    },
    { step, text, sub },
  );
}

async function card(html, ms) {
  await page.evaluate((html) => {
    const el = document.createElement("div");
    el.id = "rec-card";
    el.innerHTML = html;
    document.body.appendChild(el);
  }, html);
  await sleep(ms);
  await page.evaluate(() => {
    const el = document.getElementById("rec-card");
    el.style.opacity = "0";
    setTimeout(() => el.remove(), 700);
  });
  await sleep(700);
}

async function clickWithPing(locator) {
  const box = await locator.boundingBox();
  if (box) {
    const x = box.x + box.width / 2;
    const y = box.y + box.height / 2;
    await page.mouse.move(x, y, { steps: 18 });
    await page.evaluate(({ x, y }) => {
      const p = document.createElement("div");
      p.id = "rec-click";
      p.style.left = x + "px";
      p.style.top = y + "px";
      document.body.appendChild(p);
      setTimeout(() => p.remove(), 800);
    }, { x, y });
  }
  await locator.click();
}

async function setTraffic(millis) {
  const slider = page.locator(".traffic input[type=range]");
  const box = await slider.boundingBox();
  const cur = Number(await slider.inputValue());
  const xFor = (v) => box.x + 8 + ((box.width - 16) * v) / 4000;
  const y = box.y + box.height / 2;
  await page.mouse.move(xFor(cur), y, { steps: 10 });
  await page.mouse.down();
  await page.mouse.move(xFor(millis), y, { steps: 30 });
  await page.mouse.up();
  // Make sure the exact value lands even if the drag rounded.
  await page.evaluate(
    (m) => fetch("/api/services/web", { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ sim_demand_millis: m }) }),
    millis,
  );
}

async function leader() {
  return page.evaluate(async () => (await (await fetch("/api/overview")).json()).leader);
}

async function lastRecovery(prevCount) {
  for (let i = 0; i < 60; i++) {
    const o = await page.evaluate(async () => (await fetch("/api/overview")).json());
    const recs = (o.events || []).filter((e) => e.kind === "recovered");
    if (recs.length > prevCount) return recs[recs.length - 1];
    await sleep(250);
  }
  return null;
}

async function countRecoveries() {
  const o = await page.evaluate(async () => (await fetch("/api/overview")).json());
  return (o.events || []).filter((e) => e.kind === "recovered").length;
}

// ---------------------------------------------------------------- script

await page.goto(BASE, { waitUntil: "networkidle" });
await overlayCSS();
await sleep(1500);

await card(
  `<div><h1>NebulaOS</h1><h2>a self-healing distributed cloud orchestrator — built from scratch in Go</h2>
   <p>Raft consensus · custom scheduler · failure detection · autoscaling · gRPC</p>
   <p style="margin-top:24px"><code>github.com/coolstack-coder/NebulaOS-Build-Your-Own-Distributed-Cloud-Operating-System</code></p></div>`,
  4500,
);

await caption("1", "A live 5-node cluster: every node is a Raft voter and a worker", "5 separate OS processes talking gRPC · 4 services / 12 replicas placed by the scheduler");
await sleep(6500);

const l0 = await leader();
await caption("2", `${l0} is the Raft leader — it runs the scheduler, failure detector and autoscaler`, "Cluster state is a replicated log: every decision is committed through Raft before it takes effect");
await page.mouse.move(1180, 760, { steps: 25 });
await sleep(6000);

await caption("3", "Traffic spike on web: 0.7 → 3.4 CPU cores", "web autoscales on CPU like a Kubernetes HPA — target 60% of each replica's request");
await setTraffic(3400);
await sleep(2500);
await caption("3", "Autoscaler: web CPU way over target → scale out", "new replicas go to the least-loaded nodes (scheduler scores: free capacity · spread · balance · stability)");
await sleep(9000);

const before = await countRecoveries();
await caption("4", "Chaos: SIGKILL the leader and one more node — 2 of 5 machines gone", "no graceful shutdown — exactly what a power failure looks like");
await sleep(2200);
await clickWithPing(page.getByRole("button", { name: /Kill 2 of 5 nodes/ }));
await sleep(600);
await caption("4", "Leader lost: followers' election timers fire → pre-vote → new leader elected", "3 of 5 voters remain — still a Raft majority, so the control plane survives");
await sleep(2600);
await caption("4", "Heartbeats missing for 2s → nodes declared down → replicas rescheduled", "lost tasks are recreated and bound to the surviving nodes");
const rec = await lastRecovery(before);
await sleep(1200);
const healed = rec ? `${rec.duration_ms} ms` : "under a second";
await caption("4", `Self-healed: every replica running again ${healed} after the failure was detected`, "workloads moved from the dead nodes are highlighted with ←origin");
await sleep(5500);

await caption("5", "Event log, Raft state and recovery timings are all replicated", "any surviving node can serve the full history");
await page.evaluate(() => window.scrollTo({ top: document.body.scrollHeight, behavior: "smooth" }));
await sleep(6500);
await page.evaluate(() => window.scrollTo({ top: 0, behavior: "smooth" }));
await sleep(1500);

await caption("6", "Power the machines back on", "they replay their Raft log from disk, catch up from the leader and rejoin as followers");
await clickWithPing(page.getByRole("button", { name: /Restart all/ }));
await sleep(7000);

await caption("7", "Traffic drops → scale in after a 15s stabilisation window", "prevents flapping: shrink only to the highest recommendation seen in the window");
await setTraffic(600);
await sleep(21000);

await caption("8", "Drain a node for maintenance → its workloads are evicted and rescheduled", "nebula ctl drain n1  ·  same API the dashboard uses");
await clickWithPing(page.locator(".node", { hasText: "n1" }).first().getByRole("button", { name: "Drain" }));
await sleep(5500);
await clickWithPing(page.locator(".node", { hasText: "n1" }).first().getByRole("button", { name: "Uncordon" }));
await sleep(1500);

await page.evaluate(() => document.getElementById("rec-cap")?.remove());
const benchPath = path.resolve(process.env.BENCH_JSON || "docs/BENCHMARKS.json");
let numbers = "";
try {
  const b = JSON.parse(fs.readFileSync(benchPath, "utf8"));
  const two = b.scenarios.find((s) => s.scenario.name === "leader+follower-crash");
  const med = (xs) => xs.slice().sort((a, c) => a - c)[Math.floor(xs.length / 2)];
  numbers = `<p>Benchmarked: leader + follower crash → full recovery in <code>${med(two.runs.map((r) => r.recovery_ms))} ms</code> (median)</p>
             <p>scheduling p50 <code>${b.sched_p50_ms.toFixed(0)} ms</code> · ${b.write_ops_per_sec.toFixed(0)} replicated writes/s</p>`;
} catch {}
await card(
  `<div><h1>NebulaOS</h1><h2>Raft from scratch · scheduler · self-healing · autoscaling · live dashboard</h2>${numbers}
   <p style="margin-top:24px"><code>github.com/coolstack-coder/NebulaOS-Build-Your-Own-Distributed-Cloud-Operating-System</code></p></div>`,
  5000,
);

const video = page.video();
await context.close();
await browser.close();
const src = await video.path();
const dst = path.join(OUT, "nebulaos-demo.webm");
fs.renameSync(src, dst);
fs.writeFileSync(path.join(OUT, "chapters.json"), JSON.stringify(marks, null, 2));
console.log("video:", dst);
