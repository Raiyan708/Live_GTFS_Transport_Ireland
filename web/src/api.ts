import { useEffect, useRef, useState } from "react";
import { IS_DEMO, useDemoAlerts, useDemoReliability, useDemoRoutes, useDemoSnapshot } from "./demo";
import type { ReliabilityResponse } from "./lib/reliability";
import type { RouteTable } from "./lib/routes";

export interface Vehicle {
  id: string;
  lat: number;
  lon: number;
  bearing: number | null;
  route_id: string | null;
  trip_id: string | null;
  delay_s: number | null;
  age_s: number; // seconds between the vehicle's own timestamp and the server's clock at snapshot time
  stalled?: 1; // present only when true: the trip's delay is growing 1:1 with the clock (not progressing)
}

export interface Stats {
  vehicles: number;
  with_delay: number;
  late: number;
  median_delay_s: number | null;
  stalled: number;
}

export interface Snapshot {
  version: number;
  server_time: number;
  stats: Stats;
  vehicles: Vehicle[];
}

/** live = events flowing; reconnecting = the browser is retrying; connecting = nothing received yet. */
export type LinkStatus = "connecting" | "live" | "reconnecting" | "replay";

function useLiveSnapshot(url = "/api/stream"): { snapshot: Snapshot | null; status: LinkStatus; receivedAt: number | null } {
  const [snapshot, setSnapshot] = useState<Snapshot | null>(null);
  const [status, setStatus] = useState<LinkStatus>("connecting");
  const [receivedAt, setReceivedAt] = useState<number | null>(null);
  const lastVersion = useRef(-1);

  useEffect(() => {
    const es = new EventSource(url); // reconnects on its own with backoff; we only report the state
    es.onopen = () => setStatus("live");
    es.onerror = () => setStatus("reconnecting");
    es.addEventListener("snapshot", (e) => {
      try {
        const snap = JSON.parse((e as MessageEvent).data) as Snapshot;
        if (snap.version === lastVersion.current) return; // a reconnect can replay the same version
        lastVersion.current = snap.version;
        setSnapshot(snap);
        setReceivedAt(Date.now());
        setStatus("live");
      } catch {
        /* a malformed event must not take the dashboard down: keep the last good snapshot */
      }
    });
    return () => es.close();
  }, [url]);

  return { snapshot, status, receivedAt };
}

export interface RoutesResponse {
  available: boolean;
  attribution: string | null;
  feed: { start?: string | null; end?: string | null };
  routes: RouteTable;
}

/** Route names change once per schedule period, so fetch once. A failure leaves an empty table (raw ids are shown). */
function useLiveRoutes(url = "/api/routes"): { routes: RouteTable; attribution: string | null } {
  const [data, setData] = useState<{ routes: RouteTable; attribution: string | null }>({ routes: {}, attribution: null });
  useEffect(() => {
    const ctrl = new AbortController();
    fetch(url, { signal: ctrl.signal })
      .then((r) => (r.ok ? (r.json() as Promise<RoutesResponse>) : Promise.reject(new Error(String(r.status)))))
      .then((d) => d.available && setData({ routes: d.routes, attribution: d.attribution }))
      .catch(() => {}); // names are a nicety: never break the dashboard over them
    return () => ctrl.abort();
  }, [url]);
  return data;
}

export interface AlertVehicle {
  id: string;
  lat: number;
  lon: number;
  age_s: number;
}

export interface Alert {
  key: string; // trip_key: "<service day>|<trip id>"
  trip_id: string | null;
  route_id: string | null;
  tier: 0 | 1 | 2; // 0 moderate, 1 severe, 2 critical: the same tiers as the delay bands
  severity: string;
  delay_s: number; // the CURRENT delay when the trip is still in the feed, else the delay when the alert last changed
  delay_at_alert_s: number;
  delay_is_current: boolean;
  stalled: boolean; // delay growing 1:1 with the clock: stuck or never started, not 'critically late' in the usual sense
  upcoming: boolean; // scheduled start still in the future: the delay is a prediction
  episode: number;
  changed_at: number; // when this ALERT last changed state: NOT when the trip last reported
  changed_min: number;
  vehicle: AlertVehicle | null; // null = no vehicle reporting for this trip (e.g. not started yet)
}

export interface AlertsResponse {
  available: boolean;
  stale?: boolean; // the server's last refresh of the store failed: this is the previous good copy
  reason?: string;
  server_time: number;
  summary: { active: number; by_tier: Record<string, number>; with_vehicle: number; unconfirmed: number; stalled: number };
  items: Alert[];
}

export const NO_ALERTS: AlertsResponse = { available: false, server_time: 0, summary: { active: 0, by_tier: {}, with_vehicle: 0, unconfirmed: 0, stalled: 0 }, items: [] };

/** Poll the hot path. A failed poll keeps the last good data and flips `stale`, so the panel can say so. */
function useLiveAlerts(intervalMs = 8000, url = "/api/alerts/active"): { alerts: AlertsResponse; stale: boolean } {
  const [alerts, setAlerts] = useState<AlertsResponse>(NO_ALERTS);
  const [stale, setStale] = useState(false);
  useEffect(() => {
    let alive = true;
    let ctrl = new AbortController();
    const poll = () => {
      ctrl.abort();
      ctrl = new AbortController();
      fetch(url, { signal: ctrl.signal })
        .then((r) => (r.ok ? (r.json() as Promise<AlertsResponse>) : Promise.reject(new Error(String(r.status)))))
        .then((d) => alive && (setAlerts(d), setStale(false)))
        .catch((e) => alive && e.name !== "AbortError" && setStale(true));
    };
    poll();
    const t = setInterval(poll, intervalMs);
    return () => {
      alive = false;
      clearInterval(t);
      ctrl.abort();
    };
  }, [intervalMs, url]);
  return { alerts, stale };
}

export type ReliabilityState = { data: ReliabilityResponse | null; loading: boolean; failed: boolean };

/** Loads the cold-path analytics only while the panel is open, refreshing every minute (the data changes every few minutes). */
function useLiveReliability(open: boolean, url = "/api/analytics/reliability?min_obs=100&min_trips=3&limit=15"): ReliabilityState {
  const [state, setState] = useState<ReliabilityState>({ data: null, loading: false, failed: false });
  useEffect(() => {
    if (!open) return;
    let alive = true;
    let ctrl = new AbortController();
    const load = () => {
      ctrl.abort();
      ctrl = new AbortController();
      setState((s) => ({ ...s, loading: s.data === null }));
      fetch(url, { signal: ctrl.signal })
        .then((r) => (r.ok ? (r.json() as Promise<ReliabilityResponse>) : Promise.reject(new Error(String(r.status)))))
        .then((d) => alive && setState({ data: d, loading: false, failed: false }))
        .catch((e) => alive && e.name !== "AbortError" && setState((s) => ({ ...s, loading: false, failed: true })));
    };
    load();
    const t = setInterval(load, 60_000);
    return () => {
      alive = false;
      clearInterval(t);
      ctrl.abort();
    };
  }, [open, url]);
  return state;
}

// Live or recorded: decided once per page load, so these are plain constants (no conditional hook calls).
export const useSnapshot = IS_DEMO ? useDemoSnapshot : useLiveSnapshot;
export const useRoutes = IS_DEMO ? useDemoRoutes : useLiveRoutes;
export const useAlerts = IS_DEMO ? useDemoAlerts : useLiveAlerts;
export const useReliability = IS_DEMO ? useDemoReliability : useLiveReliability;
