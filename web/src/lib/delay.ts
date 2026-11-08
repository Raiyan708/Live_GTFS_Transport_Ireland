// Delay bands. The thresholds MIRROR the Python alert state machine (AlertConfig: on=300, escalation 600/900);
// tests/test_web_thresholds.py fails if the two ever drift apart.
// Colours are the dataviz skill's fixed status steps (good/warning/serious/critical) on a dark surface. The
// validator flagged yellow vs orange as close in hue (dE 13.6 < 15), so severity is NEVER carried by colour alone:
// every band also has a shape, a word and a size.

export const THRESHOLDS = { moderate: 300, severe: 600, critical: 900 } as const;

export type BandId = "calm" | "moderate" | "severe" | "critical";

export interface Band {
  id: BandId;
  label: string; // the word
  icon: string; // the shape (same glyphs as the Python severity_label)
  color: string;
  radius: number; // map dot radius in px: size is the third, colour-independent channel
  minSeconds: number;
}

export const BANDS: readonly Band[] = [
  { id: "calm", label: "NORMAL", icon: "○", color: "#6f8196", radius: 3.5, minSeconds: -Infinity },
  { id: "moderate", label: "MODERATE", icon: "◆", color: "#fab219", radius: 5, minSeconds: THRESHOLDS.moderate },
  { id: "severe", label: "SEVERE", icon: "▲", color: "#ec835a", radius: 6.5, minSeconds: THRESHOLDS.severe },
  { id: "critical", label: "CRITICAL", icon: "●", color: "#d03b3b", radius: 8, minSeconds: THRESHOLDS.critical },
];

const BY_ID = Object.fromEntries(BANDS.map((b) => [b.id, b])) as Record<BandId, Band>;
export const bandById = (id: BandId): Band => BY_ID[id];

/** Unknown delay is "calm" for sizing/ordering, but callers draw it hollow so it is never mistaken for on-time. */
export function bandFor(delaySeconds: number | null): Band {
  if (delaySeconds === null) return BY_ID.calm;
  if (delaySeconds >= THRESHOLDS.critical) return BY_ID.critical;
  if (delaySeconds >= THRESHOLDS.severe) return BY_ID.severe;
  if (delaySeconds >= THRESHOLDS.moderate) return BY_ID.moderate;
  return BY_ID.calm;
}

/** A healthy alert pipeline leaves ~0.4% of ACTIVE alerts that the live feed no longer confirms (measured: 2 of 463).
 *  Warn at 10% (and only with enough alerts to judge): that means the pipeline is stopped or far behind. */
export const PIPELINE_WARN_SHARE = 0.1;
export const PIPELINE_WARN_MIN_ALERTS = 20;
export const pipelineLooksStale = (active: number, unconfirmed: number): boolean =>
  active >= PIPELINE_WARN_MIN_ALERTS && unconfirmed / active >= PIPELINE_WARN_SHARE;

export const isAlertGrade = (delaySeconds: number | null): boolean =>
  delaySeconds !== null && delaySeconds >= THRESHOLDS.moderate;

export function formatDelay(delaySeconds: number | null): string {
  if (delaySeconds === null) return "no delay data";
  if (Math.abs(delaySeconds) < 60) return "on time";
  const min = Math.round(Math.abs(delaySeconds) / 60);
  return delaySeconds > 0 ? `${min} min late` : `${min} min early`;
}

export function formatAge(seconds: number): string {
  if (seconds < 60) return `${Math.max(0, Math.round(seconds))}s ago`;
  if (seconds < 3600) return `${Math.round(seconds / 60)} min ago`;
  return `${Math.round(seconds / 3600)} h ago`;
}
