import { AnimatePresence, motion } from "motion/react";
import { useMemo } from "react";
import type { Vehicle } from "../api";
import { BANDS, type BandId, bandFor, formatAge, formatDelay, isAlertGrade } from "../lib/delay";
import { type RouteTable, describeRoute, modeWord } from "../lib/routes";
import { SeverityChips } from "./SeverityChips";

interface Props {
  routes: RouteTable;
  vehicles: Vehicle[];
  selectedId: string | null;
  minBand: BandId | "all";
  onMinBand: (b: BandId | "all") => void;
  onPick: (v: Vehicle) => void;
  reducedMotion: boolean;
}

const MAX_ROWS = 60;

export function WorstList({ routes, vehicles, selectedId, minBand, onMinBand, onPick, reducedMotion }: Props) {
  const { rows, total } = useMemo(() => {
    const rank = (id: BandId) => BANDS.findIndex((b) => b.id === id);
    const min = minBand === "all" ? rank("moderate") : rank(minBand);
    const all = vehicles
      .filter((v) => isAlertGrade(v.delay_s) && rank(bandFor(v.delay_s).id) >= min)
      .sort((a, b) => (b.delay_s ?? 0) - (a.delay_s ?? 0) || a.id.localeCompare(b.id));
    return { rows: all.slice(0, MAX_ROWS), total: all.length };
  }, [vehicles, minBand]);

  return (
    <section className="rail-list panel" aria-label="Most delayed vehicles">
      <div className="rail-head">
        <h2>Live delays</h2>
        <p className="muted">{total.toLocaleString("en-IE")} vehicles right now, unfiltered by the alert engine{total > MAX_ROWS ? ` · showing worst ${MAX_ROWS}` : ""}</p>
        <SeverityChips value={minBand} onChange={onMinBand} />
      </div>
      <ul className="rows">
        <AnimatePresence initial={false}>
          {rows.map((v) => {
            const band = bandFor(v.delay_s);
            const route = describeRoute(routes, v.route_id);
            return (
              <motion.li
                key={v.id}
                layout={reducedMotion ? false : "position"}
                initial={reducedMotion ? false : { opacity: 0, x: -12 }}
                animate={{ opacity: 1, x: 0 }}
                exit={reducedMotion ? undefined : { opacity: 0, x: -12 }}
                transition={{ type: "spring", stiffness: 420, damping: 38 }}
              >
                <button className={`row ${v.id === selectedId ? "row-on" : ""}`} onClick={() => onPick(v)}>
                  <span className="sev" style={{ color: band.color }} aria-hidden="true">{band.icon}</span>
                  <span className="row-main">
                    <span className="row-route">
                      {route.title}
                      {route.subtitle && <span className="row-dest"> {route.subtitle}</span>}
                    </span>
                    <span className="row-sub">{[band.label, route.mode === "bus" ? "" : modeWord(route.mode), route.agency, formatAge(v.age_s)].filter(Boolean).join(" · ")}</span>
                  </span>
                  <span className="row-end">
                    <span className="row-delay num">{formatDelay(v.delay_s).replace(" late", "")}</span>
                    {v.stalled && <span className="tag tag-warn" title="Delay growing 1:1 with the clock: not progressing.">STALLED</span>}
                  </span>
                </button>
              </motion.li>
            );
          })}
        </AnimatePresence>
        {rows.length === 0 && <li className="empty">Nothing at this severity right now.</li>}
      </ul>
    </section>
  );
}
