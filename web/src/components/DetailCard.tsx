import { AnimatePresence, motion } from "motion/react";
import type { Vehicle } from "../api";
import { bandFor, formatAge, formatDelay } from "../lib/delay";
import { type RouteTable, describeRoute, modeWord } from "../lib/routes";

/** Selected-vehicle panel. All values are rendered as React text (never as HTML): the strings come from an external feed. */
export function DetailCard({ routes, vehicle, onClose, reducedMotion }: { routes: RouteTable; vehicle: Vehicle | null; onClose: () => void; reducedMotion: boolean }) {
  const route = describeRoute(routes, vehicle?.route_id ?? null);
  return (
    <AnimatePresence>
      {vehicle && (
        <motion.aside
          key={vehicle.id}
          className="detail panel"
          initial={reducedMotion ? false : { opacity: 0, y: 14 }}
          animate={{ opacity: 1, y: 0 }}
          exit={reducedMotion ? undefined : { opacity: 0, y: 14 }}
          transition={{ type: "spring", stiffness: 380, damping: 34 }}
          aria-label="Selected vehicle"
        >
          <header>
            <span className="sev" style={{ color: bandFor(vehicle.delay_s).color }} aria-hidden="true">{bandFor(vehicle.delay_s).icon}</span>
            <div>
              <h3>{route.title}{route.subtitle && <span className="row-dest"> {route.subtitle}</span>}</h3>
              <p className="muted">{bandFor(vehicle.delay_s).label} · {formatDelay(vehicle.delay_s)}{vehicle.stalled ? " · STALLED" : ""}</p>
            </div>
            <button className="close" onClick={onClose} aria-label="Close details">✕</button>
          </header>
          <dl>
            <div><dt>Operator</dt><dd>{[modeWord(route.mode), route.agency].filter(Boolean).join(" · ") || "—"}</dd></div>
            <div><dt>Vehicle</dt><dd className="num">{vehicle.id}</dd></div>
            <div><dt>Trip</dt><dd className="num">{vehicle.trip_id ?? "—"}</dd></div>
            <div><dt>Last position</dt><dd className="num">{formatAge(vehicle.age_s)}</dd></div>
            <div><dt>Location</dt><dd className="num">{vehicle.lat.toFixed(4)}, {vehicle.lon.toFixed(4)}</dd></div>
          </dl>
        </motion.aside>
      )}
    </AnimatePresence>
  );
}
