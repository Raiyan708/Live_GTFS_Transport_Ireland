// Route-reliability chart helpers. Colour = the dataviz skill's single-hue SEQUENTIAL ramp (blue), dark-surface orientation:
// near-zero recedes into the surface (step 700), the largest share is the brightest (step 150). Magnitude is never red/green:
// status colours are reserved for status (see delay.ts).

export interface RankedRoute {
  route_id: string;
  rank: number;
  late_ratio: number;
  avg_delay_s: number;
  max_delay_s: number;
  observations: number;
  trips_peak: number; // most trips seen in one 5-minute window: the evidence base for the ranking
}
export interface HeatCell {
  hour: number;
  observations: number;
  late_ratio: number | null; // null = too few observations to draw a confident colour
}
export interface Coverage {
  first_window: string;
  last_window: string;
  windows: number;
  hours_with_data: number[];
  routes_seen: number;
  routes_ranked: number;
  observations: number;
  min_obs: number;
  min_trips: number;
  min_cell_obs: number;
  span_hours: number;
}
export interface ReliabilityResponse {
  available: boolean;
  stale?: boolean; // the server's last refresh failed: this is the previous good copy
  has_data: boolean;
  reason?: string;
  coverage: Coverage | null;
  ranking: RankedRoute[];
  heatmap: { hours: number[]; routes: { route_id: string; cells: HeatCell[] }[] };
}

/** dark -> bright, dataviz sequential blue steps 700,600,550,500,450,400,350,300,250,150 */
export const RAMP = ["#0d366b", "#184f95", "#1c5cab", "#256abf", "#2a78d6", "#3987e5", "#5598e7", "#6da7ec", "#86b6ef", "#b7d3f6"] as const;

/** Upper end of the colour scale: the data max rounded UP to the next 5%, never below 10%, so the legend reads in round numbers. */
export function niceMax(values: (number | null)[]): number {
  const m = Math.max(0, ...values.filter((v): v is number => v !== null));
  return Math.max(0.1, Math.ceil(m / 0.05) * 0.05);
}

/** Bucket a share into one of RAMP's steps. null (not enough data) has no colour: callers draw it differently. */
export function rampColor(value: number | null, max: number): string | null {
  if (value === null || Number.isNaN(value)) return null;
  const i = Math.min(RAMP.length - 1, Math.max(0, Math.floor((value / max) * RAMP.length)));
  return RAMP[i];
}

export const pct = (v: number, digits = 0): string => `${(v * 100).toFixed(digits)}%`;
export const hourLabel = (h: number): string => `${String(h).padStart(2, "0")}:00`;
export const hourRange = (h: number): string => `${hourLabel(h)}–${hourLabel((h + 1) % 24)}`;

/** "16:25 – 18:35" in the viewer's Europe/Dublin wall-clock. Falls back to the raw text if the date is unparseable. */
export function spanLabel(first: string, last: string): string {
  const f = new Date(first);
  const l = new Date(last);
  if (Number.isNaN(f.getTime()) || Number.isNaN(l.getTime())) return `${first} – ${last}`;
  const t = (d: Date) => d.toLocaleTimeString("en-IE", { hour: "2-digit", minute: "2-digit", hour12: false, timeZone: "Europe/Dublin" });
  return `${t(f)} – ${t(l)}`;
}
