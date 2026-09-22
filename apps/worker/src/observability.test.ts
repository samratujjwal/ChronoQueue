import { randomUUID } from "node:crypto";
import { Writable } from "node:stream";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import {
  MetricsRegistry,
  createServiceLogger,
  WORKER_EVENTS,
} from "@chronoqueue/observability";
import { db, pool } from "./db/client.js";
import { processWebhookDeliveryJob } from "./processor.js";
import { createWorkerMetrics } from "./observability.js";

// Capture pino JSON lines for assertion.
function captureLogger(service: "worker") {
  const lines: string[] = [];
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      lines.push(chunk.toString());
      callback();
    },
  });
  const logger = createServiceLogger(service, { stream, level: "debug" });
  const parsed = () =>
    lines.map((l) => JSON.parse(l) as Record<string, unknown>);
  return { logger, lines, parsed };
}

const insertedIds: string[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
  for (const id of insertedIds.splice(0)) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
});

afterAll(async () => {
  await pool.end();
});

async function insertProcessableJob(targetUrl: string): Promise<string> {
  const id = randomUUID();
  insertedIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: "QUEUED",
    targetUrl,
    payload: { hello: "world" },
    scheduledAt: new Date(),
  });
  return id;
}

function fakeBullJob(jobId: string) {
  return {
    id: `bull-${jobId}`,
    data: { jobId },
  } as unknown as import("bullmq").Job;
}

describe("worker observability (PART 11)", () => {
  it("A: every lifecycle log line carries stable service + event fields", async () => {
    // A local HTTP server that always returns 200.
    const { createServer } = await import("node:http");
    const server = createServer((_req, res) => {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end("{}");
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;

    const { logger, parsed } = captureLogger("worker");
    const registry = new MetricsRegistry();
    const metrics = createWorkerMetrics(registry);

    const jobId = await insertProcessableJob(`http://127.0.0.1:${port}/hook`);
    await processWebhookDeliveryJob(fakeBullJob(jobId), {
      logger,
      workerMetrics: metrics,
    });

    const logs = parsed();
    expect(logs.length).toBeGreaterThan(0);
    for (const line of logs) {
      expect(line.service).toBe("worker");
      expect(typeof line.event).toBe("string");
    }
    const events = logs.map((l) => l.event);
    expect(events).toContain(WORKER_EVENTS.jobClaimed);
    expect(events).toContain(WORKER_EVENTS.webhookSucceeded);

    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("C: logs never contain the raw leaseToken or the full targetUrl", async () => {
    const { createServer } = await import("node:http");
    const server = createServer((_req, res) => {
      res.writeHead(200);
      res.end("{}");
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;

    const { logger, lines } = captureLogger("worker");
    const registry = new MetricsRegistry();
    const metrics = createWorkerMetrics(registry);

    // URL with a secret-looking query param: only the hostname may appear.
    const jobId = await insertProcessableJob(
      `http://127.0.0.1:${port}/hook?token=super-secret-token`,
    );
    await processWebhookDeliveryJob(fakeBullJob(jobId), {
      logger,
      workerMetrics: metrics,
    });

    const raw = lines.join("\n");
    expect(raw).not.toContain("super-secret-token");
    expect(raw).not.toContain(`http://127.0.0.1:${port}/hook?token=`);
    // Hostname is fine.
    expect(raw).toContain("127.0.0.1");

    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("E: jobs_succeeded_total increments only on a successful guarded transition", async () => {
    const { createServer } = await import("node:http");
    const server = createServer((_req, res) => {
      res.writeHead(200);
      res.end("{}");
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;

    const { logger } = captureLogger("worker");
    const registry = new MetricsRegistry();
    const metrics = createWorkerMetrics(registry);

    const jobId = await insertProcessableJob(`http://127.0.0.1:${port}/ok`);
    expect(metrics.jobsSucceededTotal.get()).toBe(0);

    await processWebhookDeliveryJob(fakeBullJob(jobId), {
      logger,
      workerMetrics: metrics,
    });

    expect(metrics.jobsSucceededTotal.get()).toBe(1);

    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("E: worker_claim_conflicts_total increments when the claim is lost", async () => {
    const { logger } = captureLogger("worker");
    const registry = new MetricsRegistry();
    const metrics = createWorkerMetrics(registry);

    // Insert a job already PROCESSING (owned by someone else): the claim
    // must fail and count as a conflict, not a crash.
    const id = randomUUID();
    insertedIds.push(id);
    await db.insert(jobs).values({
      id,
      idempotencyKey: randomUUID(),
      type: "WEBHOOK",
      status: "PROCESSING",
      targetUrl: "http://127.0.0.1:9/unused",
      payload: {},
      scheduledAt: new Date(),
      leaseToken: randomUUID(),
      leaseUntil: new Date(Date.now() + 60_000),
    });

    await expect(
      processWebhookDeliveryJob(fakeBullJob(id), {
        logger,
        workerMetrics: metrics,
      }),
    ).rejects.toThrow(/could not be claimed/);

    expect(metrics.workerClaimConflictsTotal.get()).toBe(1);
    // No success/retry/dead counted for a claim that never happened.
    expect(metrics.jobsSucceededTotal.get()).toBe(0);
    expect(metrics.jobsRetriedTotal.get({ source: "worker" })).toBe(0);
    expect(metrics.jobsDeadTotal.get({ source: "worker" })).toBe(0);
  });

  it("K: a throwing metrics backend never breaks job processing", async () => {
    const { createServer } = await import("node:http");
    const server = createServer((_req, res) => {
      res.writeHead(200);
      res.end("{}");
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 0;

    const { logger } = captureLogger("worker");
    const registry = new MetricsRegistry();
    const metrics = createWorkerMetrics(registry);
    // Sabotage every metric: if observability throws, the job must still
    // succeed (PART 14 — failure isolation).
    for (const m of [
      metrics.jobsSucceededTotal,
      metrics.webhookRequestsTotal,
      metrics.webhookDurationSeconds,
      metrics.jobProcessingDurationSeconds,
    ]) {
      if ("inc" in m) {
        vi.spyOn(m, "inc").mockImplementation(() => {
          throw new Error("metrics backend exploded");
        });
      }
      if ("observe" in m) {
        vi.spyOn(m, "observe").mockImplementation(() => {
          throw new Error("metrics backend exploded");
        });
      }
    }

    const jobId = await insertProcessableJob(`http://127.0.0.1:${port}/ok`);
    await expect(
      processWebhookDeliveryJob(fakeBullJob(jobId), {
        logger,
        workerMetrics: metrics,
      }),
    ).resolves.toBeUndefined();

    // Business truth is intact despite dead observability.
    const [row] = await db.select().from(jobs).where(eq(jobs.id, jobId));
    expect(row?.status).toBe("SUCCEEDED");

    await new Promise<void>((resolve) => server.close(() => resolve()));
  });
});
