import { describe, expect, it } from "vitest";
import { isValidTransition, type JobStatus } from "./state-machine.js";

describe("job state machine", () => {
  const validTransitions: Array<[JobStatus, JobStatus]> = [
    ["PENDING", "QUEUED"],
    ["QUEUED", "PROCESSING"],
    ["PROCESSING", "SUCCEEDED"],
    ["PROCESSING", "RETRYING"],
    ["PROCESSING", "DEAD"],
    ["RETRYING", "QUEUED"],
    ["RETRYING", "DEAD"],
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
    ["DEAD", "QUEUED"],
  ];

  it.each(invalidTransitions)("rejects %s -> %s", (from, to) => {
    expect(isValidTransition(from, to)).toBe(false);
  });

  it("terminal states allow no transitions at all, including self-transitions", () => {
    expect(isValidTransition("SUCCEEDED", "SUCCEEDED")).toBe(false);
    expect(isValidTransition("DEAD", "DEAD")).toBe(false);
    expect(isValidTransition("SUCCEEDED", "DEAD")).toBe(false);
    expect(isValidTransition("DEAD", "SUCCEEDED")).toBe(false);
  });
});
