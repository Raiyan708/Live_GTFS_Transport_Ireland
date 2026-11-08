import { AnimatePresence, motion } from "motion/react";
import { useEffect, useMemo, useRef, useState } from "react";
import type { ReliabilityState } from "../api";
import { formatDelay } from "../lib/delay";
import { RAMP, type RankedRoute, hourLabel, hourRange, niceMax, pct, rampColor, spanLabel } from "../lib/reliability";
import { type RouteTable, describeRoute } from "../lib/routes";

interface Tip {
  x: number;
  y: number;
  lines: string[];
}

interface Props {
  open: boolean;
  onClose: () => void;
  state: ReliabilityState;
  routes: RouteTable;
  reducedMotion: boolean;
}

/** All text comes from an external feed, so it is rendered as React text only (never HTML). */
export function ReliabilityPanel({ open, onClose, state, routes, reducedMotion }: Props) {
  const [view, setView] = useState<"chart" | "table">("chart");
  const [tip, setTip] = useState<Tip | null>(null);
  const root = useRef<HTMLElement>(null);
  const closeBtn = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    if (!open) return;
    closeBtn.current?.focus();
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  const { data, loading, failed } = state;
  const ranking = data?.ranking ?? [];
  const label = (id: string) => describeRoute(routes, id);
  const show = (e: React.MouseEvent, lines: string[]) => {
    const box = root.current?.getBoundingClientRect();
    if (!box) return;
    // Keep the whole tooltip inside the drawer (it clips overflow): shift left near the right edge, flip above near the bottom.
    const W = 250;
    const H = 24 + lines.length * 17;
    const x = Math.max(8, Math.min(e.clientX - box.left + 14, box.width - W - 8));
    const below = e.clientY - box.top + 14;
    const y = below + H > box.height - 8 ? Math.max(8, e.clientY - box.top - H - 10) : below;
    setTip({ x, y, lines });
  };
  const hide = () => setTip(null);

  const routeTip = (r: RankedRoute) => {
    const l = label(r.route_id);
    return [
      `${l.title}${l.subtitle ? " · " + l.subtitle : ""}`,
      l.agency ?? "",
      `Late share ${pct(r.late_ratio, 1)}`,
      `Average ${formatDelay(r.avg_delay_s).replace(" late", "")} · worst ${formatDelay(r.max_delay_s).replace(" late", "")}`,
      `${r.observations.toLocaleString("en-IE")} readings · up to ${r.trips_peak} trips at once`,
    ].filter(Boolean);
  };

  return (
    <AnimatePresence>
      {open && (
        <motion.aside
          ref={root}
          className="drawer panel"
          role="dialog"
          aria-label="Route reliability"
          initial={reducedMotion ? false : { opacity: 0, x: 40 }}
          animate={{ opacity: 1, x: 0 }}
          exit={reducedMotion ? undefined : { opacity: 0, x: 40 }}
          transition={{ type: "spring", stiffness: 360, damping: 34 }}
        >
          <header className="drawer-head">
            <div>
              <h2>Route reliability</h2>
              <p className="muted">Share of readings running 5+ minutes late, from the cold path</p>
            </div>
            <div className="seg" role="group" aria-label="View">
              <button className={view === "chart" ? "seg-on" : ""} aria-pressed={view === "chart"} onClick={() => setView("chart")}>Chart</button>
              <button className={view === "table" ? "seg-on" : ""} aria-pressed={view === "table"} onClick={() => setView("table")}>Table</button>
            </div>
            <button ref={closeBtn} className="close" onClick={onClose} aria-label="Close route reliability">✕</button>
          </header>

          <div className="drawer-body" onMouseLeave={hide}>
            {loading && <p className="notice">Loading…</p>}
            {failed && !data && <p className="notice warn">Couldn't reach the analytics service.</p>}
            {data && !data.available && <p className="notice warn">The data store isn't reachable ({data.reason}). Nothing to show yet.</p>}
            {data?.available && !data.has_data && <p className="notice">No curated data in this window yet. The Glue job has to run first.</p>}

            {data?.available && data.has_data && data.coverage && (
              <>
                <p className="coverage">
                  <strong>{data.coverage.span_hours} hours</strong> of data · {spanLabel(data.coverage.first_window, data.coverage.last_window)} Dublin time ·{" "}
                  {data.coverage.hours_with_data.length} of 24 hours of the day covered ·{" "}
                  {data.coverage.routes_ranked} of {data.coverage.routes_seen} routes ranked
                  {(failed || data.stale) && <span className="warn"> · ⚠ showing the last data received</span>}
                </p>
                <p className="muted fine-line">
                  Only readings from trips that have already started are counted (readings for upcoming trips are mostly timetable placeholders).
                  A route is ranked only with {data.coverage.min_obs}+ readings and {data.coverage.min_trips}+ trips at once: with fewer, one stalled
                  trip decides the whole route. Delays are NTA's numbers: a route that is late on almost every trip may be a timetable mismatch in the
                  data rather than poor service, and I cannot tell which.
                </p>

                {view === "chart" ? (
                  <>
                    <h3 className="chart-title">Least reliable routes</h3>
                    <ol className="bars" aria-label="Routes ranked by late share">
                      {(() => {
                        const max = niceMax(ranking.map((r) => r.late_ratio)) || 1;
                        return ranking.map((r, i) => {
                          const l = label(r.route_id);
                          return (
                            <motion.li
                              key={r.route_id}
                              className="bar-row"
                              initial={reducedMotion ? false : { opacity: 0, x: -10 }}
                              animate={{ opacity: 1, x: 0 }}
                              transition={{ delay: reducedMotion ? 0 : i * 0.025, type: "spring", stiffness: 420, damping: 34 }}
                              onMouseMove={(e) => show(e, routeTip(r))}
                              onMouseLeave={hide}
                            >
                              <span className="bar-rank num">{r.rank}</span>
                              <span className="bar-name">
                                <strong>{l.title}</strong> <span className="row-dest">{l.subtitle}</span>
                              </span>
                              <span className="bar-track" aria-hidden="true">
                                <motion.span
                                  className="bar-fill"
                                  initial={reducedMotion ? false : { width: 0 }}
                                  animate={{ width: `${(r.late_ratio / max) * 100}%` }}
                                  transition={{ delay: reducedMotion ? 0 : 0.1 + i * 0.025, duration: reducedMotion ? 0 : 0.55, ease: "easeOut" }}
                                />
                              </span>
                              <span className="bar-val num">{pct(r.late_ratio)}</span>
                            </motion.li>
                          );
                        });
                      })()}
                    </ol>

                    <h3 className="chart-title">When they run late <span className="muted">(share late by hour of day, worst routes)</span></h3>
                    <Heatmap data={data.heatmap} routes={routes} onTip={show} onHide={hide} reducedMotion={reducedMotion} minCell={data.coverage.min_cell_obs} />
                  </>
                ) : (
                  <table className="data-table">
                    <caption className="sr-only">Routes ranked by share of readings running 5 or more minutes late</caption>
                    <thead>
                      <tr><th>#</th><th>Route</th><th>Operator</th><th>Late share</th><th>Average</th><th>Worst</th><th>Readings</th><th>Trips</th></tr>
                    </thead>
                    <tbody>
                      {ranking.map((r) => {
                        const l = label(r.route_id);
                        return (
                          <tr key={r.route_id}>
                            <td className="num">{r.rank}</td>
                            <td><strong>{l.title}</strong> <span className="row-dest">{l.subtitle}</span></td>
                            <td>{l.agency ?? "—"}</td>
                            <td className="num">{pct(r.late_ratio, 1)}</td>
                            <td className="num">{formatDelay(r.avg_delay_s).replace(" late", "")}</td>
                            <td className="num">{formatDelay(r.max_delay_s).replace(" late", "")}</td>
                            <td className="num">{r.observations.toLocaleString("en-IE")}</td>
                            <td className="num">{r.trips_peak}</td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                )}
              </>
            )}
          </div>

          {tip && (
            <div className="tip" style={{ left: tip.x, top: tip.y }} role="tooltip">
              {tip.lines.map((t, i) => (
                <div key={i} className={i === 0 ? "tip-title" : "tip-line"}>{t}</div>
              ))}
            </div>
          )}
        </motion.aside>
      )}
    </AnimatePresence>
  );
}

function Heatmap({ data, routes, onTip, onHide, reducedMotion, minCell }: {
  data: { hours: number[]; routes: { route_id: string; cells: { hour: number; observations: number; late_ratio: number | null }[] }[] };
  routes: RouteTable;
  onTip: (e: React.MouseEvent, lines: string[]) => void;
  onHide: () => void;
  reducedMotion: boolean;
  minCell: number;
}) {
  const max = useMemo(() => niceMax(data.routes.flatMap((r) => r.cells.map((c) => c.late_ratio))), [data]);
  return (
    <div className="heat" role="img" aria-label="Heatmap of late share by route and hour of day. The table view has the same numbers for the ranked routes.">
      <div className="heat-axis" aria-hidden="true">
        <span />
        <div className="heat-cols">{data.hours.map((h) => <span key={h} className="num">{h % 3 === 0 ? String(h).padStart(2, "0") : ""}</span>)}</div>
      </div>
      {data.routes.map((row, ri) => {
        const l = describeRoute(routes, row.route_id);
        const byHour = new Map(row.cells.map((c) => [c.hour, c]));
        return (
          <div className="heat-row" key={row.route_id}>
            <span className="heat-name" title={`${l.title} ${l.subtitle ?? ""}`}><strong>{l.title}</strong> <span className="row-dest">{l.subtitle}</span></span>
            <div className="heat-cols">
              {data.hours.map((h) => {
                const c = byHour.get(h);
                const colour = c ? rampColor(c.late_ratio, max) : null;
                const kind = !c ? "none" : colour === null ? "thin" : "data";
                return (
                  <motion.span
                    key={h}
                    className={`cell cell-${kind}`}
                    style={colour ? { background: colour } : undefined}
                    initial={reducedMotion || kind !== "data" ? false : { opacity: 0, scale: 0.6 }}
                    animate={{ opacity: 1, scale: 1 }}
                    transition={{ delay: reducedMotion ? 0 : 0.15 + ri * 0.03 + h * 0.004, duration: 0.3 }}
                    onMouseMove={(e) =>
                      onTip(e, [
                        `${l.title}${l.subtitle ? " · " + l.subtitle : ""}`,
                        hourRange(h),
                        !c ? "No data for this hour yet" : c.late_ratio === null ? `Only ${c.observations} readings: too few to show (needs ${minCell}+)` : `${pct(c.late_ratio, 1)} late`,
                        c && c.late_ratio !== null ? `${c.observations.toLocaleString("en-IE")} readings` : "",
                      ].filter(Boolean))
                    }
                    onMouseLeave={onHide}
                  />
                );
              })}
            </div>
          </div>
        );
      })}
      <div className="heat-legend" aria-hidden="true">
        <span className="muted">0%</span>
        <span className="swatches">{RAMP.map((c) => <i key={c} style={{ background: c }} />)}</span>
        <span className="muted">{pct(max)} late</span>
        <span className="key"><i className="cell cell-thin" /> too few readings</span>
        <span className="key"><i className="cell cell-none" /> no data yet</span>
      </div>
      <p className="muted fine-line">Hours are Dublin local time. {hourLabel(0)}–{hourLabel(23)} are all shown; empty columns are hours not observed yet, not "on time".</p>
    </div>
  );
}
