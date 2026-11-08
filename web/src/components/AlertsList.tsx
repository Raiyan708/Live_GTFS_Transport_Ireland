import { AnimatePresence, motion } from "motion/react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { Alert, AlertsResponse } from "../api";
import { BANDS, type BandId, formatDelay, pipelineLooksStale } from "../lib/delay";
import { type RouteTable, describeRoute } from "../lib/routes";
import { SeverityChips } from "./SeverityChips";

interface Props {
  alerts: AlertsResponse;
  stale: boolean;
  showPredicted: boolean;
  onShowPredicted: (v: boolean) => void;
  routes: RouteTable;
  selectedVehicleId: string | null;
  minBand: BandId | "all";
  onMinBand: (b: BandId | "all") => void;
  onPick: (a: Alert) => void;
  reducedMotion: boolean;
}

const MAX_ROWS = 80;
const NEW_FOR_MS = 12_000;
// alert tier 0/1/2 == band index 1/2/3 (moderate/severe/critical)
const bandOfTier = (tier: number) => BANDS[Math.min(3, tier + 1)];
const minTier = (b: BandId | "all") => (b === "all" ? 0 : BANDS.findIndex((x) => x.id === b) - 1);

/** Alerts that appeared AFTER the page opened get a short "NEW" highlight; the first load is not "new". */
function useFreshKeys(items: Alert[], available: boolean): Set<string> {
  const seen = useRef<Set<string> | null>(null);
  const [fresh, setFresh] = useState<Map<string, number>>(new Map());
  useEffect(() => {
    if (!available) return;
    const keys = new Set(items.map((a) => a.key));
    if (seen.current === null) {
      seen.current = keys;
      return;
    }
    const born = [...keys].filter((k) => !seen.current!.has(k));
    seen.current = keys;
    if (born.length) {
      const until = Date.now() + NEW_FOR_MS;
      setFresh((m) => new Map([...m, ...born.map((k) => [k, until] as const)]));
    }
  }, [items, available]);
  useEffect(() => {
    if (!fresh.size) return;
    const t = setTimeout(() => setFresh((m) => new Map([...m].filter(([, until]) => until > Date.now()))), NEW_FOR_MS + 200);
    return () => clearTimeout(t);
  }, [fresh]);
  return useMemo(() => new Set([...fresh].filter(([, u]) => u > Date.now()).map(([k]) => k)), [fresh]);
}

export function AlertsList({ alerts, stale, showPredicted, onShowPredicted, routes, selectedVehicleId, minBand, onMinBand, onPick, reducedMotion }: Props) {
  const fresh = useFreshKeys(alerts.items, alerts.available);
  const rows = useMemo(
    () => alerts.items.filter((a) => a.tier >= minTier(minBand) && (showPredicted || a.vehicle !== null)),
    [alerts.items, minBand, showPredicted],
  );
  const predicted = alerts.summary.active - alerts.summary.with_vehicle;
  const shown = rows.slice(0, MAX_ROWS);

  return (
    <section className="rail-list panel" aria-label="Active alerts">
      <div className="rail-head">
        <h2>Active alerts</h2>
        <p className="muted">
          {alerts.available
            ? `${rows.length.toLocaleString("en-IE")} trips ${showPredicted ? "incl. predicted" : "with a vehicle on the map"}`
            : "Alert pipeline offline"}
        </p>
        {(stale || alerts.stale) && alerts.available && <p className="warn">⚠ Can't reach the alert service. Showing the last data received.</p>}
        {alerts.available && pipelineLooksStale(alerts.summary.active, alerts.summary.unconfirmed) && (
          <p className="warn">
            ⚠ {alerts.summary.unconfirmed.toLocaleString("en-IE")} of {alerts.summary.active.toLocaleString("en-IE")} alerts no longer match the live feed.
            The alert pipeline may be stopped or behind, so treat this list as out of date.
          </p>
        )}
        <SeverityChips value={minBand} onChange={onMinBand} />
        {alerts.available && (
          <label className="switch" title="Trips with no vehicle position get a delay PREDICTED by NTA. On the recorded evening those flipped in bulk (up to 45% of them in one step), while alerts backed by a vehicle were stable (median 0.4% per step).">
            <input type="checkbox" checked={showPredicted} onChange={(e) => onShowPredicted(e.target.checked)} />
            <span>Include {predicted.toLocaleString("en-IE")} predicted delays (no vehicle)</span>
          </label>
        )}
      </div>
      <ul className="rows">
        <AnimatePresence initial={false}>
          {shown.map((a) => {
            const band = bandOfTier(a.tier);
            const route = describeRoute(routes, a.route_id);
            const onMap = a.vehicle !== null;
            return (
              <motion.li
                key={a.key}
                layout={reducedMotion ? false : "position"}
                initial={reducedMotion ? false : { opacity: 0, x: -12 }}
                animate={{ opacity: 1, x: 0 }}
                exit={reducedMotion ? undefined : { opacity: 0, x: 12 }}
                transition={{ type: "spring", stiffness: 420, damping: 38 }}
              >
                <button
                  className={`row ${a.vehicle?.id === selectedVehicleId ? "row-on" : ""} ${fresh.has(a.key) ? "row-new" : ""}`}
                  onClick={() => onMap && onPick(a)}
                  disabled={!onMap}
                  title={onMap ? "Show on the map" : "No vehicle is reporting a position for this trip (it may not have started yet)"}
                >
                  <span className="sev" style={{ color: band.color }} aria-hidden="true">{band.icon}</span>
                  <span className="row-main">
                    <span className="row-route">
                      {route.title}
                      {route.subtitle && <span className="row-dest"> {route.subtitle}</span>}
                    </span>
                    <span className="row-sub">
                      {[band.label, route.mode && route.mode !== "bus" ? route.mode.toUpperCase() : "", `changed ${a.changed_min}m ago`]
                        .filter(Boolean)
                        .join(" · ")}
                    </span>
                  </span>
                  <span className="row-end">
                    <span className="row-delay num">{formatDelay(a.delay_s).replace(" late", "")}</span>
                    {fresh.has(a.key) && <span className="tag tag-new">NEW</span>}
                    {a.stalled && <span className="tag tag-warn" title="The delay is growing as fast as the clock: this trip is not progressing along its timetable (stuck, or never started).">STALLED</span>}
                    {a.upcoming && !a.stalled && <span className="tag" title="Scheduled to start later: this delay is a prediction.">UPCOMING</span>}
                    {!onMap && <span className="tag">NO VEHICLE</span>}
                  </span>
                </button>
              </motion.li>
            );
          })}
        </AnimatePresence>
        {alerts.available && rows.length === 0 && <li className="empty">No active alerts at this severity.</li>}
        {!alerts.available && <li className="empty">The alert service isn't reachable. The "Live delays" tab still works.</li>}
        {rows.length > MAX_ROWS && <li className="empty">+ {rows.length - MAX_ROWS} more (worst shown first)</li>}
      </ul>
    </section>
  );
}
