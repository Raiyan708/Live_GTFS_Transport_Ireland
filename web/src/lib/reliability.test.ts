import { describe, expect, it } from "vitest";
import { RAMP, hourRange, niceMax, pct, rampColor, spanLabel } from "./reliability";

describe("niceMax", () => {
  it("rounds the data max up to the next 5% and never below 10%", () => {
    expect(niceMax([0.01, 0.12])).toBeCloseTo(0.15);
    expect(niceMax([0.5])).toBeCloseTo(0.5);
    expect(niceMax([0.51])).toBeCloseTo(0.55);
    expect(niceMax([0.0, 0.02])).toBeCloseTo(0.1);
    expect(niceMax([])).toBeCloseTo(0.1);
  });
  it("ignores masked (null) cells", () => {
    expect(niceMax([null, 0.2, null])).toBeCloseTo(0.2);
  });
});

describe("rampColor", () => {
  it("maps 0 to the darkest step and the maximum to the brightest", () => {
    expect(rampColor(0, 0.5)).toBe(RAMP[0]);
    expect(rampColor(0.5, 0.5)).toBe(RAMP[RAMP.length - 1]);
  });
  it("is monotone: a higher share never gets an earlier (darker) step", () => {
    let last = -1;
    for (let v = 0; v <= 0.4; v += 0.01) {
      const i = RAMP.indexOf(rampColor(v, 0.4)! as (typeof RAMP)[number]);
      expect(i).toBeGreaterThanOrEqual(last);
      last = i;
    }
  });
  it("clamps out-of-range values and gives masked data NO colour", () => {
    expect(rampColor(9, 0.5)).toBe(RAMP[RAMP.length - 1]);
    expect(rampColor(-1, 0.5)).toBe(RAMP[0]);
    expect(rampColor(null, 0.5)).toBeNull();
    expect(rampColor(Number.NaN, 0.5)).toBeNull();
  });
  it("brighter means larger: step lightness rises along the ramp", () => {
    const lum = (hex: string) => {
      const [r, g, b] = [1, 3, 5].map((i) => parseInt(hex.slice(i, i + 2), 16));
      return 0.2126 * r + 0.7152 * g + 0.0722 * b;
    };
    for (let i = 1; i < RAMP.length; i++) expect(lum(RAMP[i])).toBeGreaterThan(lum(RAMP[i - 1]));
  });
});

describe("labels", () => {
  it("formats shares and hour ranges, including the midnight wrap", () => {
    expect(pct(0.4321)).toBe("43%");
    expect(pct(0.4321, 1)).toBe("43.2%");
    expect(hourRange(7)).toBe("07:00–08:00");
    expect(hourRange(23)).toBe("23:00–00:00");
  });
  it("renders the data span in Dublin time and survives bad input", () => {
    expect(spanLabel("2026-11-08T16:25:00+00:00", "2026-11-08T18:35:00+00:00")).toBe("16:25 – 18:35");
    expect(spanLabel("nope", "also nope")).toBe("nope – also nope");
  });
});
