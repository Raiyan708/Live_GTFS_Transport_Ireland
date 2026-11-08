// Position tweening. The feed refreshes every ~2 minutes, so between snapshots we GLIDE each vehicle from where it
// was drawn to its newly reported position. This animates real reported positions only: it never extrapolates a
// bus beyond where the data says it is (the UI says so in the footer).

export interface Pt {
  lon: number;
  lat: number;
}

export interface Track {
  from: Pt;
  to: Pt;
  t0: number; // ms timestamp when the glide started
  dur: number; // ms; 0 = snap
  born: number; // ms timestamp when the vehicle first appeared (drives the fade-in)
}

export const GLIDE_MS = 6000;
export const FADE_IN_MS = 700;
/** A jump longer than this is a data gap or a re-assignment, not a bus driving: snap instead of gliding across town.
 *  3 km in ~2 min is already ~90 km/h. */
export const MAX_GLIDE_METERS = 3000;

export const easeOutCubic = (t: number): number => 1 - Math.pow(1 - Math.min(1, Math.max(0, t)), 3);

/** Equirectangular approximation: accurate to well under 1% over the distances that matter here. */
export function distanceMeters(a: Pt, b: Pt): number {
  const R = 6371000;
  const dLat = ((b.lat - a.lat) * Math.PI) / 180;
  const dLon = (((b.lon - a.lon) * Math.PI) / 180) * Math.cos((((a.lat + b.lat) / 2) * Math.PI) / 180);
  return R * Math.hypot(dLat, dLon);
}

export function positionAt(t: Track, now: number): Pt {
  if (t.dur <= 0) return t.to;
  const k = easeOutCubic((now - t.t0) / t.dur);
  return { lon: t.from.lon + (t.to.lon - t.from.lon) * k, lat: t.from.lat + (t.to.lat - t.from.lat) * k };
}

export const alphaAt = (t: Track, now: number, reducedMotion: boolean): number =>
  reducedMotion ? 1 : easeOutCubic((now - t.born) / FADE_IN_MS);

/**
 * Build the next tracks from the previous ones and a new snapshot.
 *  - known vehicle: glide from where it is CURRENTLY drawn (even mid-glide) to the new position
 *  - new vehicle: appear in place and fade in
 *  - vanished vehicle: dropped
 */
export function retarget(
  prev: ReadonlyMap<string, Track>,
  next: ReadonlyArray<{ id: string; lon: number; lat: number }>,
  now: number,
  opts: { reducedMotion?: boolean } = {},
): Map<string, Track> {
  const out = new Map<string, Track>();
  for (const v of next) {
    const to = { lon: v.lon, lat: v.lat };
    const old = prev.get(v.id);
    if (!old) {
      out.set(v.id, { from: to, to, t0: now, dur: 0, born: now });
      continue;
    }
    const from = positionAt(old, now);
    const snap = opts.reducedMotion || distanceMeters(from, to) > MAX_GLIDE_METERS;
    out.set(v.id, { from: snap ? to : from, to, t0: now, dur: snap ? 0 : GLIDE_MS, born: old.born });
  }
  return out;
}
