import { describe, expect, it } from "vitest";
import { isRetryableHttpStatus } from "./webhook-delivery.js";

describe("isRetryableHttpStatus", () => {
  const retryable = [500, 502, 503, 504];
  const nonRetryable = [400, 401, 403, 404, 409, 429];

  it.each(retryable)("treats %i as retryable", (code) => {
    expect(isRetryableHttpStatus(code)).toBe(true);
  });

  it.each(nonRetryable)("treats %i as non-retryable", (code) => {
    expect(isRetryableHttpStatus(code)).toBe(false);
  });
});
