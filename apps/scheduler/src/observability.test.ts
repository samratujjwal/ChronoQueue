import { randomUUID } from "node:crypto";
import { Writable } from "node:stream";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import {
  MetricsRegistry,
  createServiceLogger,
  SCHEDULER_EVENTS,
} from "@chronoqueue/observability";
import { db, pool } from "./db/client.js";
import { connection } from "./queue/connection.js";
import { webhookQueue } from "./queue/webhook-queue.js";
import { runSchedulerPoll } from "./scheduler.js";
import { recoverStaleJobs } from "./recovery.js";
import { createSchedulerMetrics } from "./observability.js";

function captureLogger(service: "scheduler") {
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
    const bullJob = await webhookQueue.getJob(id);
    await bullJob?.remove();
  }
});

afterAll(async () => {
  await webhookQueue.close();
  await connection.quit();
  await pool.end();
});

async function insertPendingJob(scheduledAt: Date): Promise<string> {
  const id = randomUUID();
  insertedIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: "PENDING",
    targetUrl: "http://127.0.0.1:9/unused",
    payload: { test: true },
    scheduledAt,
  });
  return id;
}

async function insertStaleProcessingJob(): Promise<string> {
  const id = randomUUID();
  insertedIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: "PROCESSING",
    targetUrl: "http://127.0.0.1:9/unused",
    payload: {},
    scheduledAt: new Date(Date.now() - 60_000),
    leaseToken: randomUUID(),
    leaseUntil: new Date(Date.now() - 30_000),
    attempts: 0,
    maxAttempts: 3,
  });
  return id;
}

describe("scheduler observability (PART 11)", () => {
  it("A: scheduler log lines carry stable service + event fields", async () => {
    const { logger, parsed } = captureLogger("scheduler");
    const registry = new MetricsRegistry();
    const metrics = createSchedulerMetrics(registry);

    await insertPendingJob(new Date(Date.now() - 1000));
    await runSchedulerPoll({ logger, schedulerMetrics: metrics });

    const logs = parsed();
    expect(logs.length).toBeGreaterThan(0);
    for (const line of logs) {
      expect(line.service).toBe("scheduler");
      expect(typeof line.event).toBe("string");
    }
    const events = logs.map((l) => l.event);
    expect(events).toContain(SCHEDULER_EVENTS.schedulerPollCompleted);
  });

  it("J: empty polls stay silent at INFO (no per-second INFO spam)", async () => {
    const { logger, parsed } = captureLogger("scheduler");
    const registry = new MetricsRegistry();
    const metrics = createSchedulerMetrics(registry);

    // No due jobs inserted by this test. Sibling suites share this
    // database, so the poll may still observe their rows — the invariant
    // under test is what an EMPTY poll logs, not global database emptiness.
    const result = await runSchedulerPoll({
      logger,
      schedulerMetrics: metrics,
    });

    const infoLines = parsed().filter((l) => l.level === 30);

    // An empty poll's completion is DEBUG by design (PART 12) — it must
    // never surface at INFO, whatever else is in the database.
    const emptyCompletionsAtInfo = infoLines.filter(
      (l) =>
        l.event === SCHEDULER_EVENTS.schedulerPollCompleted && l.dueCount === 0,
    );
    expect(emptyCompletionsAtInfo).toHaveLength(0);

    if (result.dueCount === 0) {
      // Genuinely empty poll: the completion stays observable at DEBUG,
      // and there is total silence at INFO.
      const debugCompletions = parsed().filter(
        (l) =>
          l.level === 20 &&
          l.event === SCHEDULER_EVENTS.schedulerPollCompleted &&
          l.dueCount === 0,
      );
      expect(debugCompletions.length).toBeGreaterThanOrEqual(1);
      expect(infoLines).toHaveLength(0);
    }
  });

  it("E: scheduler_enqueue_failures_total splits db_claim vs bullmq_enqueue", async () => {
    const { logger } = captureLogger("scheduler");
    const registry = new MetricsRegistry();
    const metrics = createSchedulerMetrics(registry);

    await insertPendingJob(new Date(Date.now() - 1000));

    // Simulate a BullMQ outage: enqueue throws for every job.
    const failingEnqueue = vi.fn(async (_jobId: string) => {
      throw new Error("redis exploded");
    });

    await runSchedulerPoll({
      logger,
      schedulerMetrics: metrics,
      enqueue: failingEnqueue,
    });

    expect(failingEnqueue).toHaveBeenCalled();
    // Lower bound, not exact: sibling suites share this database, so this
    // poll may claim and fail more than just our job.
    expect(
      metrics.schedulerEnqueueFailuresTotal.get({ stage: "bullmq_enqueue" }),
    ).toBeGreaterThanOrEqual(1);
    expect(
      metrics.schedulerEnqueueFailuresTotal.get({ stage: "db_claim" }),
    ).toBe(0);
  });

  it("E: recovery increments jobs_retried_total{source=recovery} only on successful guarded recovery", async () => {
    const { logger, parsed } = captureLogger("scheduler");
    const registry = new MetricsRegistry();
    const metrics = createSchedulerMetrics(registry);

    const jobId = await insertStaleProcessingJob();

    // Sibling suites share this database and sweep it concurrently, so our
    // stale job may already be recovered by their sweep — the assertions
    // below hold whichever sweep wins the race.
    const result = await recoverStaleJobs({
      logger,
      schedulerMetrics: metrics,
    });

    // Metric integrity: every successful guarded write increments exactly
    // one of the two source-labeled counters — no phantom increments, none
    // missing. Counters and recoveredJobIds come from this same sweep call,
    // so this holds regardless of whose jobs were swept.
    const retried = metrics.jobsRetriedTotal.get({ source: "recovery" });
    const dead = metrics.jobsDeadTotal.get({ source: "recovery" });
    expect(retried + dead).toBe(result.recoveredJobIds.length);

    // Our job's guarded recovery is attempts 0 -> 1 < maxAttempts 3, i.e.
    // RETRYING: when this sweep recovered it, it counted exactly one
    // retried.
    if (result.recoveredJobIds.includes(jobId)) {
      expect(retried).toBeGreaterThanOrEqual(1);
    }

    // Our job is no longer stuck PROCESSING with an expired lease — either
    // this sweep or a sibling's recovered it.
    const [row] = await db
      .select({ status: jobs.status })
      .from(jobs)
      .where(eq(jobs.id, jobId))
      .limit(1);
    expect(row).toBeDefined();
    expect(
      result.recoveredJobIds.includes(jobId) || row?.status !== "PROCESSING",
    ).toBe(true);

    // When this sweep did the recovering, it logs the canonical event.
    if (result.recoveredJobIds.length > 0) {
      const events = parsed().map((l) => l.event);
      expect(events).toContain(SCHEDULER_EVENTS.expiredLeaseRecovered);
    }
  });

  it("K: a throwing metrics backend never breaks the scheduler poll", async () => {
    const { logger } = captureLogger("scheduler");
    const registry = new MetricsRegistry();
    const metrics = createSchedulerMetrics(registry);
    const incSpy = vi
      .spyOn(metrics.schedulerEnqueueFailuresTotal, "inc")
      .mockImplementation(() => {
        throw new Error("metrics backend exploded");
      });

    await insertPendingJob(new Date(Date.now() - 1000));

    const failingEnqueue = async (_jobId: string) => {
      throw new Error("redis exploded");
    };

    // The poll must run to completion even though every metrics write
    // throws: safely() contains the failure and the batch keeps moving.
    // dueCount is global (sibling suites share this database), so only a
    // lower bound is meaningful here.
    const result = await runSchedulerPoll({
      logger,
      schedulerMetrics: metrics,
      enqueue: failingEnqueue,
    });
    expect(result.dueCount).toBeGreaterThanOrEqual(1);
    // At least one job went through the failing-enqueue path, so the
    // throwing metrics backend was actually exercised — and contained.
    expect(incSpy).toHaveBeenCalled();
  });
});
