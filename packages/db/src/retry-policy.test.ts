import { describe, expect, it } from "vitest";
import {
  applyFullJitter,
  calculateExponentialDelayMs,
} from "./retry-policy.js";

const config = { baseDelayMs: 1000, maxDelayMs: 30000 };

describe("calculateExponentialDelayMs", () => {
  it("attempt 1 = 1000ms", () => {
    expect(calculateExponentialDelayMs(1, config)).toBe(1000);
  });

  it("attempt 2 = 2000ms", () => {
    expect(calculateExponentialDelayMs(2, config)).toBe(2000);
  });

  it("attempt 3 = 4000ms", () => {
    expect(calculateExponentialDelayMs(3, config)).toBe(4000);
  });

  it("attempt 4 = 8000ms", () => {
    expect(calculateExponentialDelayMs(4, config)).toBe(8000);
  });

  it("caps at maxDelayMs for large attempt numbers", () => {
    expect(calculateExponentialDelayMs(10, config)).toBe(30000);
    expect(calculateExponentialDelayMs(20, config)).toBe(30000);
  });

  it("treats attempt <= 0 as attempt 1 (documented floor policy)", () => {
    expect(calculateExponentialDelayMs(0, config)).toBe(1000);
    expect(calculateExponentialDelayMs(-5, config)).toBe(1000);
  });
});

describe("applyFullJitter", () => {
  it("random = 0 gives the minimum delay (0ms)", () => {
    expect(applyFullJitter(4000, () => 0)).toBe(0);
  });

  it("random near 1 stays within [0, exponentialDelayMs]", () => {
    const result = applyFullJitter(4000, () => 0.999999);
    expect(result).toBeGreaterThanOrEqual(0);
    expect(result).toBeLessThanOrEqual(4000);
  });

  it("deterministic random inputs produce deterministic output", () => {
    expect(applyFullJitter(1000, () => 0.5)).toBe(500);
    expect(applyFullJitter(2000, () => 0.25)).toBe(500);
  });

  it("never exceeds exponentialDelayMs across the full random range", () => {
    for (const r of [0, 0.1, 0.25, 0.5, 0.75, 0.99, 0.999999]) {
      expect(applyFullJitter(4000, () => r)).toBeLessThanOrEqual(4000);
    }
  });
});
