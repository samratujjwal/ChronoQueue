import { randomUUID } from "node:crypto";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { db, pool } from "./db/client.js";
import { connection } from "./queue/connection.js";
import { webhookQueue } from "./queue/webhook-queue.js";
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

async function insertPendingJob(scheduledAt: Date): Promise<string> {
  const id = randomUUID();
  insertedIds.push(id);
  await db.insert(jobs).values({
    id,
    type: "WEBHOOK",
    status: "PENDING",
    targetUrl: "http://127.0.0.1:9/unused",
    payload: { test: true },
    scheduledAt,
  });
  return id;
}

describe("runSchedulerPoll (real PostgreSQL + real Redis + real BullMQ)", () => {
  it("does not enqueue a job scheduled in the future", async () => {
    const futureId = await insertPendingJob(new Date(Date.now() + 60_000));

    await runSchedulerPoll();

    const bullJob = await webhookQueue.getJob(futureId);
    expect(bullJob).toBeUndefined();
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
