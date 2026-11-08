// Route naming from the GTFS static feed (served by /api/routes). The NTA realtime route_id ("1 120 c a") is an
// internal key, not something a rider recognises; the static feed maps it to "120 - Ashtown Station - Parnell St".

export type Mode = "bus" | "rail" | "tram" | "metro" | "ferry" | "other";

export interface RouteInfo {
  short: string | null;
  long: string | null;
  agency: string | null;
  mode: Mode;
}
export type RouteTable = Record<string, RouteInfo>;

export interface RouteLabel {
  title: string; // what a rider calls it: "120", "Red"
  subtitle: string | null; // where it goes
  agency: string | null;
  mode: Mode | null;
  known: boolean; // false = fell back to the raw feed id
}

const MODE_WORD: Record<Mode, string> = { bus: "BUS", rail: "RAIL", tram: "TRAM", metro: "METRO", ferry: "FERRY", other: "" };
export const modeWord = (m: Mode | null): string => (m ? MODE_WORD[m] : "");

/** Never throws and never invents a name: an unknown id is shown as-is and flagged as unknown. */
export function describeRoute(table: RouteTable, routeId: string | null): RouteLabel {
  if (!routeId) return { title: "Unknown route", subtitle: null, agency: null, mode: null, known: false };
  const r = Object.hasOwn(table, routeId) ? table[routeId] : undefined; // hasOwn: an id like "constructor" must not hit Object.prototype
  if (!r || !r.short) return { title: routeId, subtitle: r?.long ?? null, agency: r?.agency ?? null, mode: r?.mode ?? null, known: false };
  return { title: r.short, subtitle: r.long, agency: r.agency, mode: r.mode, known: true };
}
