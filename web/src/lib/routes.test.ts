import { describe, expect, it } from "vitest";
import { type RouteTable, describeRoute, modeWord } from "./routes";

const T: RouteTable = {
  "1 120 c a": { short: "120", long: "Ashtown Station - Parnell St", agency: "Dublin Bus", mode: "bus" },
  "10000 RED g a": { short: "Red", long: "The Point - Tallaght", agency: "LUAS", mode: "tram" },
  "1 NOSHORT a": { short: null, long: "Somewhere - Elsewhere", agency: "Dublin Bus", mode: "bus" },
};

describe("describeRoute", () => {
  it("uses the rider-facing name when the static feed knows the route", () => {
    expect(describeRoute(T, "1 120 c a")).toEqual({ title: "120", subtitle: "Ashtown Station - Parnell St", agency: "Dublin Bus", mode: "bus", known: true });
    expect(describeRoute(T, "10000 RED g a").title).toBe("Red");
  });
  it("falls back to the raw id (flagged unknown) instead of inventing a name", () => {
    expect(describeRoute(T, "9 999 z z")).toEqual({ title: "9 999 z z", subtitle: null, agency: null, mode: null, known: false });
    expect(describeRoute({}, "1 120 c a").known).toBe(false); // the table failed to load: still usable
  });
  it("keeps whatever it knows when the short name is missing", () => {
    const r = describeRoute(T, "1 NOSHORT a");
    expect(r.title).toBe("1 NOSHORT a");
    expect(r.subtitle).toBe("Somewhere - Elsewhere");
    expect(r.known).toBe(false);
  });
  it("handles a missing id and ids that collide with Object.prototype", () => {
    expect(describeRoute(T, null).title).toBe("Unknown route");
    expect(describeRoute(T, "constructor").known).toBe(false);
    expect(describeRoute(T, "__proto__").known).toBe(false);
  });
  it("labels modes in words", () => {
    expect(modeWord("tram")).toBe("TRAM");
    expect(modeWord(null)).toBe("");
  });
});
