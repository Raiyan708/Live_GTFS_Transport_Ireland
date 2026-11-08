// Demo mode: replays a RECORDING of real NTA data (built by scripts/build_demo_data.py) through the same components the live
// dashboard uses. Hooks mirror the live ones, so components cannot tell the difference and the demo cannot drift from the app.
import { useEffect, useState, useSyncExternalStore } from "react";
import type { AlertsResponse, Snapshot, Stats, Vehicle } from "./api";
import type { ReliabilityResponse } from "./lib/reliability";
import type { RouteTable } from "./lib/routes";

export const IS_DEMO: boolean = import.meta.env.VITE_DEMO === "1" || (typeof location !== "undefined" && new URLSearchParams(location.search).has("demo"));

const BASE = import.meta.env.BASE_URL; // "/" locally, "/<repo>/" on GitHub Pages
export const REPLAY_STEP_MS = 6000; // one recorded frame per 6 s (frames are ~2.5 min of real time apart)

export interface Frame {
  t: number;
  stats: Stats;
  vehicles: Vehicle[];
  alerts: Pick<AlertsResponse, "summary" | "items">;
}
export interface DemoMeta {
  from: number;
  to: number;
  frames: number;
  median_gap_s: number;
  attribution: string;
  source: string;
  trip_updates_replayed: number;
  vehicle_events_replayed: number;
}

interface State {
  frames: Frame[] | null;
  meta: DemoMeta | null;
  index: number;
  loops: number;
  playing: boolean;
  error: string | null;
}

let state: State = { frames: null, meta: null, index: 0, loops: 0, playing: true, error: null };
const listeners = new Set<() => void>();
const emit = () => listeners.forEach((l) => l());
const set = (patch: Partial<State>) => {
  state = { ...state, ...patch };
  emit();
};
let started = false;

function start() {
  if (started || !IS_DEMO) return;
  started = true;
  Promise.all([fetch(`${BASE}demo/frames.json`).then((r) => r.json()), fetch(`${BASE}demo/meta.json`).then((r) => r.json())])
    .then(([frames, meta]) => set({ frames, meta }))
    .catch((e) => set({ error: String(e) }));
  setInterval(() => {
    if (!state.playing || !state.frames) return;
    const next = state.index + 1;
    set(next >= state.frames.length ? { index: 0, loops: state.loops + 1 } : { index: next });
  }, REPLAY_STEP_MS);
}

const subscribe = (l: () => void) => {
  listeners.add(l);
  return () => void listeners.delete(l);
};
const getState = () => state;

export function useDemoState() {
  useEffect(start, []);
  return useSyncExternalStore(subscribe, getState);
}
export const demoControls = {
  toggle: () => set({ playing: !state.playing }),
  seek: (i: number) => set({ index: Math.max(0, Math.min((state.frames?.length ?? 1) - 1, i)), playing: false }),
};

/** Same shape as the live useSnapshot. `version` changes every frame (also across loops) so the map re-aims its glides. */
export function useDemoSnapshot(): { snapshot: Snapshot | null; status: "replay" | "connecting"; receivedAt: number | null } {
  const s = useDemoState();
  const f = s.frames?.[s.index];
  if (!f) return { snapshot: null, status: "connecting", receivedAt: null };
  const version = s.loops * (s.frames?.length ?? 1) + s.index + 1;
  return { snapshot: { version, server_time: f.t, stats: f.stats, vehicles: f.vehicles }, status: "replay", receivedAt: Date.now() };
}

export function useDemoAlerts(): { alerts: AlertsResponse; stale: boolean } {
  const s = useDemoState();
  const f = s.frames?.[s.index];
  if (!f) return { alerts: { available: false, server_time: 0, summary: { active: 0, by_tier: {}, with_vehicle: 0, unconfirmed: 0, stalled: 0 }, items: [] }, stale: false };
  return { alerts: { available: true, server_time: f.t, ...f.alerts }, stale: false };
}

function useStatic<T>(file: string, initial: T): T {
  const [data, setData] = useState<T>(initial);
  useEffect(() => {
    if (!IS_DEMO) return;
    fetch(`${BASE}demo/${file}`).then((r) => r.json()).then(setData).catch(() => {});
  }, [file]);
  return data;
}

export function useDemoRoutes(): { routes: RouteTable; attribution: string | null } {
  const d = useStatic<{ routes: RouteTable; attribution: string | null }>("routes.json", { routes: {}, attribution: null });
  return { routes: d.routes ?? {}, attribution: d.attribution ?? null };
}

export function useDemoReliability(open: boolean): { data: ReliabilityResponse | null; loading: boolean; failed: boolean } {
  const [data, setData] = useState<ReliabilityResponse | null>(null);
  useEffect(() => {
    if (!open || data) return;
    fetch(`${BASE}demo/reliability.json`).then((r) => r.json()).then(setData).catch(() => {});
  }, [open, data]);
  return { data, loading: open && !data, failed: false };
}
