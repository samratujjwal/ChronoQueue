import { describe, expect, it } from "vitest";
import {
  getAllowedTransitions,
  isValidTransition,
  type JobStatus,
} from "./state-machine.js";

describe("job state machine", () => {
  const validTransitions: Array<[JobStatus, JobStatus]> = [
    ["PENDING", "QUEUED"],
    ["QUEUED", "PROCESSING"],
    ["PROCESSING", "SUCCEEDED"],
    ["PROCESSING", "RETRYING"],
    ["PROCESSING", "DEAD"],
    ["RETRYING", "QUEUED"],
    ["RETRYING", "DEAD"],
    // Day 14 DLQ: manual re-trigger moves a dead job back to QUEUED via a
    // guarded DB update; the Worker then claims it with a fresh leaseToken.
    ["DEAD", "QUEUED"],
  ];

  it.each(validTransitions)("allows %s -> %s", (from, to) => {
    expect(isValidTransition(from, to)).toBe(true);
  });

  const invalidTransitions: Array<[JobStatus, JobStatus]> = [
    ["PENDING", "PROCESSING"],
    ["PENDING", "SUCCEEDED"],
    ["QUEUED", "SUCCEEDED"],
    ["QUEUED", "RETRYING"],
    ["PROCESSING", "QUEUED"],
    ["SUCCEEDED", "PROCESSING"],
    ["SUCCEEDED", "PENDING"],
    ["DEAD", "PROCESSING"],
  ];

  it.each(invalidTransitions)("rejects %s -> %s", (from, to) => {
    expect(isValidTransition(from, to)).toBe(false);
  });

  it("SUCCEEDED is terminal: allows no transitions at all, including self-transitions", () => {
    expect(isValidTransition("SUCCEEDED", "SUCCEEDED")).toBe(false);
    expect(isValidTransition("SUCCEEDED", "DEAD")).toBe(false);
  });

  it("DEAD allows exactly one transition: manual re-trigger to QUEUED (Day 14 DLQ)", () => {
    expect(isValidTransition("DEAD", "QUEUED")).toBe(true);
    expect(isValidTransition("DEAD", "DEAD")).toBe(false);
    expect(isValidTransition("DEAD", "SUCCEEDED")).toBe(false);
    expect(getAllowedTransitions("DEAD")).toEqual(["QUEUED"]);
  });
});
