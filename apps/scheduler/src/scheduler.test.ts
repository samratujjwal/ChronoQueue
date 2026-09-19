import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";
import { db, pool } from "./db/client.js";
import { connection } from "./queue/connection.js";
import { webhookQueue } from "./queue/webhook-queue.js";
import { runSchedulerPoll } from "./scheduler.js";
import * as webhookProducer from "./queue/webhook-producer.js";

const insertedIds: string[] = [];

afterEach(async () => {
  vi.restoreAllMocks();
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

async function insertRetryingJob(nextAttemptAt: Date): Promise<string> {
  const id = randomUUID();
  insertedIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: "RETRYING",
    targetUrl: "http://127.0.0.1:9/unused",
    payload: { test: true },
    attempts: 1,
    nextAttemptAt,
  });
  return id;
}

async function fetchJob(id: string) {
  const [row] = await db.select().from(jobs).where(eq(jobs.id, id)).limit(1);
  return row;
}

describe("runSchedulerPoll — PENDING jobs (real PostgreSQL + real Redis + real BullMQ)", () => {
  it("does not enqueue a job scheduled in the future", async () => {
    const futureId = await insertPendingJob(new Date(Date.now() + 60_000));

    await runSchedulerPoll();

    const bullJob = await webhookQueue.getJob(futureId);
    expect(bullJob).toBeUndefined();
    expect((await fetchJob(futureId)).status).toBe("PENDING");
  });

  it("enqueues a job that is already due, with the correct BullMQ job id and data", async () => {
    const dueId = await insertPendingJob(new Date(Date.now() - 1_000));

    const result = await runSchedulerPoll();

    expect(result.enqueuedJobIds).toContain(dueId);

    const bullJob = await webhookQueue.getJob(dueId);
    expect(bullJob).toBeDefined();
    expect(bullJob?.id).toBe(dueId);
    expect(bullJob?.data).toEqual({ jobId: dueId });
  });

  it("transitions a due PENDING job to QUEUED", async () => {
    const dueId = await insertPendingJob(new Date(Date.now() - 1_000));

    await runSchedulerPoll();

    expect((await fetchJob(dueId)).status).toBe("QUEUED");
  });

  it("enqueues multiple due jobs and skips future jobs within the same poll", async () => {
    const dueIdA = await insertPendingJob(new Date(Date.now() - 3_000));
    const dueIdB = await insertPendingJob(new Date(Date.now() - 2_000));
    const futureId = await insertPendingJob(new Date(Date.now() + 60_000));

    const result = await runSchedulerPoll();

    expect(result.enqueuedJobIds).toEqual(
      expect.arrayContaining([dueIdA, dueIdB]),
    );
    expect(result.enqueuedJobIds).not.toContain(futureId);

    expect(await webhookQueue.getJob(dueIdA)).toBeDefined();
    expect(await webhookQueue.getJob(dueIdB)).toBeDefined();
    expect(await webhookQueue.getJob(futureId)).toBeUndefined();
  });
});

describe("runSchedulerPoll — RETRYING jobs (Day 11)", () => {
  it("does not select a RETRYING job whose nextAttemptAt is in the future", async () => {
    const futureId = await insertRetryingJob(new Date(Date.now() + 60_000));

    await runSchedulerPoll();

    expect(await webhookQueue.getJob(futureId)).toBeUndefined();
    expect((await fetchJob(futureId)).status).toBe("RETRYING");
  });

  it("selects a due RETRYING job and transitions it to QUEUED", async () => {
    const dueId = await insertRetryingJob(new Date(Date.now() - 1_000));

    const result = await runSchedulerPoll();

    expect(result.enqueuedJobIds).toContain(dueId);
    expect((await fetchJob(dueId)).status).toBe("QUEUED");

    const bullJob = await webhookQueue.getJob(dueId);
    expect(bullJob).toBeDefined();
    expect(bullJob?.data).toEqual({ jobId: dueId });
  });

  it("leaves a non-due RETRYING job unchanged", async () => {
    const futureId = await insertRetryingJob(new Date(Date.now() + 60_000));

    await runSchedulerPoll();

    const row = await fetchJob(futureId);
    expect(row.status).toBe("RETRYING");
  });

  it("handles a batch containing both due PENDING and due RETRYING jobs", async () => {
    const pendingId = await insertPendingJob(new Date(Date.now() - 2_000));
    const retryingId = await insertRetryingJob(new Date(Date.now() - 1_000));

    const result = await runSchedulerPoll();

    expect(result.enqueuedJobIds).toEqual(
      expect.arrayContaining([pendingId, retryingId]),
    );
    expect((await fetchJob(pendingId)).status).toBe("QUEUED");
    expect((await fetchJob(retryingId)).status).toBe("QUEUED");
  });
});

describe("runSchedulerPoll — concurrency and partial-batch failure", () => {
  it("does not let two concurrent polls both claim the same due job", async () => {
    const dueId = await insertPendingJob(new Date(Date.now() - 1_000));

    const [resultA, resultB] = await Promise.all([
      runSchedulerPoll(),
      runSchedulerPoll(),
    ]);

    const claimedByA = resultA.enqueuedJobIds.includes(dueId);
    const claimedByB = resultB.enqueuedJobIds.includes(dueId);

    // Exactly one of the two concurrent polls should have won the claim —
    // never both, never neither.
    expect(claimedByA).not.toBe(claimedByB);
    expect((await fetchJob(dueId)).status).toBe("QUEUED");
  });

  it("continues processing the remaining batch when one job's enqueue call fails", async () => {
    const idA = await insertPendingJob(new Date(Date.now() - 3_000));
    const idB = await insertPendingJob(new Date(Date.now() - 2_000));

    const spy = vi.spyOn(webhookProducer, "enqueueWebhookDelivery");
    spy.mockImplementationOnce(async () => {
      throw new Error("simulated enqueue failure");
    });

    const result = await runSchedulerPoll();

    // idA is ordered first (earlier scheduledAt) so it hits the mocked
    // failure; idB must still be processed normally.
    expect(result.enqueuedJobIds).toEqual([idB]);

    // Known dual-write limitation, demonstrated directly: idA's PostgreSQL
    // claim succeeded (status is QUEUED) even though the BullMQ enqueue
    // failed and no queue message exists for it. This is not solved in
    // Day 11 — see the comment in scheduler.ts.
    expect((await fetchJob(idA)).status).toBe("QUEUED");
    expect(await webhookQueue.getJob(idA)).toBeUndefined();

    expect((await fetchJob(idB)).status).toBe("QUEUED");
    expect(await webhookQueue.getJob(idB)).toBeDefined();
  });
});
