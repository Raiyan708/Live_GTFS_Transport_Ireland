import { describe, expect, it } from "vitest";
import { BANDS, bandFor, formatAge, formatDelay, isAlertGrade, pipelineLooksStale } from "./delay";

describe("bandFor", () => {
  it("switches exactly at the alert thresholds (inclusive lower bound)", () => {
    expect(bandFor(299).id).toBe("calm");
    expect(bandFor(300).id).toBe("moderate");
    expect(bandFor(599).id).toBe("moderate");
    expect(bandFor(600).id).toBe("severe");
    expect(bandFor(899).id).toBe("severe");
    expect(bandFor(900).id).toBe("critical");
  });
  it("treats early running and unknown delay as calm", () => {
    expect(bandFor(-240).id).toBe("calm");
    expect(bandFor(null).id).toBe("calm");
  });
  it("never relies on colour alone: every band has its own shape, word and size", () => {
    for (const key of ["icon", "label", "radius", "color"] as const) {
      expect(new Set(BANDS.map((b) => b[key])).size).toBe(BANDS.length);
    }
    expect(BANDS.map((b) => b.radius)).toEqual([...BANDS.map((b) => b.radius)].sort((a, b) => a - b)); // grows with severity
  });
});

describe("formatting", () => {
  it("describes delays in plain words", () => {
    expect(formatDelay(null)).toBe("no delay data");
    expect(formatDelay(30)).toBe("on time");
    expect(formatDelay(-30)).toBe("on time");
    expect(formatDelay(600)).toBe("10 min late");
    expect(formatDelay(-180)).toBe("3 min early");
  });
  it("describes data age", () => {
    expect(formatAge(5)).toBe("5s ago");
    expect(formatAge(150)).toBe("3 min ago");
    expect(formatAge(7300)).toBe("2 h ago");
  });
  it("only alert-grade delays count as alerts", () => {
    expect(isAlertGrade(299)).toBe(false);
    expect(isAlertGrade(300)).toBe(true);
    expect(isAlertGrade(null)).toBe(false);
  });
});

describe("pipelineLooksStale", () => {
  it("stays quiet at the healthy baseline and with too few alerts to judge", () => {
    expect(pipelineLooksStale(463, 2)).toBe(false); // measured on a healthy pipeline
    expect(pipelineLooksStale(10, 10)).toBe(false); // 100% of 10 alerts is not enough evidence
    expect(pipelineLooksStale(0, 0)).toBe(false);
  });
  it("fires from 10% disagreement once there are enough alerts", () => {
    expect(pipelineLooksStale(200, 20)).toBe(true);
    expect(pipelineLooksStale(200, 19)).toBe(false);
    expect(pipelineLooksStale(583, 400)).toBe(true); // the stopped-pipeline case seen on the live run
  });
});
