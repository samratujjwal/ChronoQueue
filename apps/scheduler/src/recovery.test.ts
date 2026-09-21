import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { db, pool } from "./db/client.js";
import { connection } from "./queue/connection.js";
import { webhookQueue } from "./queue/webhook-queue.js";
import { recoverStaleJobs } from "./recovery.js";
import { runSchedulerPoll } from "./scheduler.js";

const insertedIds: string[] = [];

afterEach(async () => {
  const idsToClean = insertedIds.splice(0);
  for (const id of idsToClean) {
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

async function insertProcessingJob(options: {
  leaseUntil: Date;
  attempts?: number;
  maxAttempts?: number;
}): Promise<string> {
  const id = randomUUID();
  insertedIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: "PROCESSING",
    targetUrl: "http://127.0.0.1:9/unused",
    payload: { test: true },
    attempts: options.attempts ?? 0,
    maxAttempts: options.maxAttempts ?? 5,
    leaseUntil: options.leaseUntil,
  });
  return id;
}

async function insertTerminalJob(
  status: "SUCCEEDED" | "DEAD",
): Promise<string> {
  const id = randomUUID();
  insertedIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status,
    targetUrl: "http://127.0.0.1:9/unused",
    payload: { test: true },
    leaseUntil: null,
  });
  return id;
}

async function fetchJob(id: string) {
  const [row] = await db.select().from(jobs).where(eq(jobs.id, id)).limit(1);
  return row;
}

describe("recoverStaleJobs (real PostgreSQL)", () => {
  it("Test 1 — recovers a job whose lease has expired", async () => {
    const id = await insertProcessingJob({
      leaseUntil: new Date(Date.now() - 5_000),
    });

    const result = await recoverStaleJobs();

    expect(result.recoveredJobIds).toContain(id);

    const row = await fetchJob(id);
    expect(row.status).toBe("RETRYING");
    expect(row.leaseUntil).toBeNull();
    expect(row.attempts).toBe(1);
    expect(row.nextAttemptAt).not.toBeNull();
  });

  it("Test 2 — does not touch a job with an active (future) lease", async () => {
    const id = await insertProcessingJob({
      leaseUntil: new Date(Date.now() + 60_000),
    });

    const result = await recoverStaleJobs();

    expect(result.recoveredJobIds).not.toContain(id);
    const row = await fetchJob(id);
    expect(row.status).toBe("PROCESSING");
    expect(row.leaseUntil).not.toBeNull();
  });

  it("Test 3 — two concurrent recovery operations cannot both recover the same job", async () => {
    const id = await insertProcessingJob({
      leaseUntil: new Date(Date.now() - 5_000),
    });

    const [resultA, resultB] = await Promise.all([
      recoverStaleJobs(),
      recoverStaleJobs(),
    ]);

    const gotA = resultA.recoveredJobIds.includes(id);
    const gotB = resultB.recoveredJobIds.includes(id);

    expect(gotA).not.toBe(gotB);

    const row = await fetchJob(id);
    expect(row.status).toBe("RETRYING");
    expect(row.attempts).toBe(1);
  });

  it("Test 4 — a SUCCEEDED job cannot be recovered", async () => {
    const id = await insertTerminalJob("SUCCEEDED");

    await recoverStaleJobs();

    const row = await fetchJob(id);
    expect(row.status).toBe("SUCCEEDED");
  });

  it("Test 5 — a DEAD job cannot be recovered", async () => {
    const id = await insertTerminalJob("DEAD");

    await recoverStaleJobs();

    const row = await fetchJob(id);
    expect(row.status).toBe("DEAD");
  });

  it("reaches DEAD instead of RETRYING when maxAttempts is already exhausted", async () => {
    const id = await insertProcessingJob({
      leaseUntil: new Date(Date.now() - 5_000),
      attempts: 4,
      maxAttempts: 5,
    });

    await recoverStaleJobs();

    const row = await fetchJob(id);
    expect(row.status).toBe("DEAD");
    expect(row.attempts).toBe(5);
    expect(row.nextAttemptAt).toBeNull();
    expect(row.leaseUntil).toBeNull();
  });

  it("Test 6 — full chain: recovery -> RETRYING -> Scheduler reschedules -> QUEUED -> real BullMQ job", async () => {
    // Simulates a worker that claimed the job then vanished before doing
    // anything else — a stale PROCESSING row with an expired lease. No
    // real Worker process is used; this is the standard, sane way to test
    // recovery logic without literally crashing a process (per the Day 13
    // spec: "do not create a fake distributed system merely for the test").
    const id = await insertProcessingJob({
      leaseUntil: new Date(Date.now() - 5_000),
    });

    const recovery = await recoverStaleJobs();
    expect(recovery.recoveredJobIds).toContain(id);

    const afterRecovery = await fetchJob(id);
    expect(afterRecovery.status).toBe("RETRYING");
    expect(afterRecovery.nextAttemptAt).not.toBeNull();

    // Make it immediately due for the Scheduler's normal poll (Day 11
    // RETRYING-selection logic, unchanged).
    await db
      .update(jobs)
      .set({ nextAttemptAt: new Date(Date.now() - 1_000) })
      .where(eq(jobs.id, id));

    const poll = await runSchedulerPoll();
    expect(poll.enqueuedJobIds).toContain(id);

    const afterPoll = await fetchJob(id);
    expect(afterPoll.status).toBe("QUEUED");

    const bullJob = await webhookQueue.getJob(id);
    expect(bullJob).toBeDefined();
    expect(bullJob?.data).toEqual({ jobId: id });
  });
});
