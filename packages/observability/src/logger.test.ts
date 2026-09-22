import { describe, expect, it } from "vitest";
import { Writable } from "node:stream";
import { createServiceLogger } from "./logger.js";

function captureStream() {
  const lines: string[] = [];
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      lines.push(chunk.toString());
      callback();
    },
  });
  return { stream, lines };
}

function lastLog(lines: string[]): Record<string, unknown> {
  const raw = lines[lines.length - 1];
  if (!raw) {
    throw new Error("no log lines captured");
  }
  return JSON.parse(raw) as Record<string, unknown>;
}

describe("createServiceLogger", () => {
  it("adds a stable service base field to every line", () => {
    const { stream, lines } = captureStream();
    const logger = createServiceLogger("worker", { stream });
    logger.info({ event: "job_claimed" }, "test");
    expect(lastLog(lines).service).toBe("worker");
  });

  it("redacts leaseToken values instead of logging them raw", () => {
    const { stream, lines } = captureStream();
    const logger = createServiceLogger("worker", { stream });
    logger.info(
      { event: "job_claimed", leaseToken: "super-secret-token-abc123" },
      "test",
    );
    const logged = lastLog(lines);
    expect(logged.leaseToken).toBe("[Redacted]");
  });

  it("redacts nested authorization headers", () => {
    const { stream, lines } = captureStream();
    const logger = createServiceLogger("api", { stream });
    logger.info(
      { event: "x", req: { headers: { authorization: "Bearer <redacted>" } } },
      "test",
    );
    const logged = lastLog(lines);
    const req = logged.req as Record<string, Record<string, unknown>>;
    expect(req.headers?.authorization).toBe("[Redacted]");
  });
});
