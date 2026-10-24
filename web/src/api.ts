import type { Overview } from "./types";

async function send(method: string, path: string, body?: unknown) {
  const res = await fetch(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  let data: Record<string, string> = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch {
    /* non-JSON error body */
  }
  if (!res.ok) throw new Error(data.error || text || res.statusText);
  return data;
}

export const api = {
  overview: async (signal?: AbortSignal): Promise<Overview> => {
    const res = await fetch("/api/overview", { signal, cache: "no-store" });
    if (!res.ok) throw new Error(res.statusText);
    return res.json();
  },
  scale: (name: string, replicas: number) => send("PATCH", `/api/services/${name}`, { replicas }),
  load: (name: string, millis: number) => send("PATCH", `/api/services/${name}`, { sim_demand_millis: millis }),
  deploy: (spec: unknown) => send("POST", "/api/services", spec),
  remove: (name: string) => send("DELETE", `/api/services/${name}`),
  drain: (node: string) => send("POST", `/api/nodes/${node}/drain`),
  uncordon: (node: string) => send("POST", `/api/nodes/${node}/uncordon`),
  kill: (node: string) => send("POST", `/api/chaos/kill/${node}`),
  start: (node: string) => send("POST", `/api/chaos/start/${node}`),
  killLeader: () => send("POST", "/api/chaos/kill-leader"),
  killTwo: () => send("POST", "/api/chaos/kill-two"),
  restartAll: () => send("POST", "/api/chaos/restart-all"),
};
