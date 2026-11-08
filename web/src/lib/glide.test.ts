import { describe, expect, it } from "vitest";
import { GLIDE_MS, MAX_GLIDE_METERS, alphaAt, distanceMeters, easeOutCubic, positionAt, retarget } from "./glide";

const DUBLIN = { lon: -6.26, lat: 53.35 };
const v = (id: string, lon: number, lat: number) => ({ id, lon, lat });

describe("easing and distance", () => {
  it("easeOutCubic is 0 at the start, 1 at the end, monotone, and clamps", () => {
    expect(easeOutCubic(0)).toBe(0);
    expect(easeOutCubic(1)).toBe(1);
    expect(easeOutCubic(-5)).toBe(0);
    expect(easeOutCubic(9)).toBe(1);
    expect(easeOutCubic(0.3)).toBeLessThan(easeOutCubic(0.6));
  });
  it("distance is about 111 km per degree of latitude", () => {
    expect(distanceMeters(DUBLIN, { ...DUBLIN, lat: DUBLIN.lat + 1 })).toBeGreaterThan(110000);
    expect(distanceMeters(DUBLIN, { ...DUBLIN, lat: DUBLIN.lat + 1 })).toBeLessThan(112000);
    expect(distanceMeters(DUBLIN, DUBLIN)).toBe(0);
  });
});

describe("retarget", () => {
  it("new vehicles appear in place (no glide from nowhere) and fade in", () => {
    const t = retarget(new Map(), [v("a", -6.26, 53.35)], 1000).get("a")!;
    expect(positionAt(t, 1000)).toEqual({ lon: -6.26, lat: 53.35 });
    expect(alphaAt(t, 1000, false)).toBe(0);
    expect(alphaAt(t, 1000 + 5000, false)).toBe(1);
    expect(alphaAt(t, 1000, true)).toBe(1); // reduced motion: no fade
  });

  it("a known vehicle glides from where it is drawn to the new position, ending exactly on it", () => {
    const first = retarget(new Map(), [v("a", -6.26, 53.35)], 0);
    const second = retarget(first, [v("a", -6.25, 53.351)], 10_000);
    const t = second.get("a")!;
    expect(positionAt(t, 10_000)).toEqual({ lon: -6.26, lat: 53.35 });
    const mid = positionAt(t, 10_000 + GLIDE_MS / 2);
    expect(mid.lon).toBeGreaterThan(-6.26);
    expect(mid.lon).toBeLessThan(-6.25);
    expect(positionAt(t, 10_000 + GLIDE_MS)).toEqual({ lon: -6.25, lat: 53.351 });
    expect(positionAt(t, 10_000 + 10 * GLIDE_MS)).toEqual({ lon: -6.25, lat: 53.351 }); // stays put afterwards
  });

  it("a new snapshot mid-glide continues from the drawn position, with no visible jump", () => {
    const a = retarget(new Map(), [v("a", -6.26, 53.35)], 0);
    const b = retarget(a, [v("a", -6.25, 53.35)], 1000); // ~0.7 km: a plausible 2-minute drive
    expect(b.get("a")!.dur).toBeGreaterThan(0); // guard: this test is meaningless if the move snapped
    const drawn = positionAt(b.get("a")!, 1000 + 2000); // mid-glide
    expect(drawn.lon).toBeGreaterThan(-6.26);
    expect(drawn.lon).toBeLessThan(-6.25);
    const c = retarget(b, [v("a", -6.24, 53.35)], 1000 + 2000);
    expect(positionAt(c.get("a")!, 3000)).toEqual(drawn);
  });

  it("snaps instead of gliding across town when the jump is implausibly long", () => {
    const far = { lon: DUBLIN.lon, lat: DUBLIN.lat + (MAX_GLIDE_METERS * 1.5) / 111000 };
    const t = retarget(retarget(new Map(), [v("a", DUBLIN.lon, DUBLIN.lat)], 0), [v("a", far.lon, far.lat)], 100).get("a")!;
    expect(t.dur).toBe(0);
    expect(positionAt(t, 100)).toEqual(far);
  });

  it("reduced motion snaps everything", () => {
    const prev = retarget(new Map(), [v("a", -6.26, 53.35)], 0);
    expect(retarget(prev, [v("a", -6.259, 53.35)], 50, { reducedMotion: true }).get("a")!.dur).toBe(0);
  });

  it("keeps the original fade-in time for known vehicles and drops vanished ones", () => {
    const prev = retarget(new Map(), [v("a", -6.26, 53.35), v("gone", -6.2, 53.3)], 500);
    const next = retarget(prev, [v("a", -6.259, 53.35)], 9000);
    expect(next.has("gone")).toBe(false);
    expect(next.get("a")!.born).toBe(500);
  });
});
