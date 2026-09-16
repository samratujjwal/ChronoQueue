import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { enqueueWebhookDelivery } from "./webhook-producer.js";
import { webhookQueue } from "./webhook-queue.js";

const createdJobIds: string[] = [];

afterAll(async () => {
  for (const id of createdJobIds) {
    const job = await webhookQueue.getJob(id);
    await job?.remove();
  }
  await webhookQueue.close();
});

describe("enqueueWebhookDelivery", () => {
  it("enqueues a job whose BullMQ id equals the PostgreSQL job id, with reference-only data", async () => {
    const postgresJobId = randomUUID();
    createdJobIds.push(postgresJobId);

    const job = await enqueueWebhookDelivery(postgresJobId);

    expect(job.id).toBe(postgresJobId);
    expect(job.name).toBe("deliver-webhook");
    expect(job.data).toEqual({ jobId: postgresJobId });
  });

  it("can be retrieved from the queue with matching id and data", async () => {
    const postgresJobId = randomUUID();
    createdJobIds.push(postgresJobId);

    await enqueueWebhookDelivery(postgresJobId);
    const fetched = await webhookQueue.getJob(postgresJobId);

    expect(fetched).toBeDefined();
    expect(fetched?.id).toBe(postgresJobId);
    expect(fetched?.data).toEqual({ jobId: postgresJobId });
  });

  it("removes cleanly and is no longer retrievable afterward", async () => {
    const postgresJobId = randomUUID();
    await enqueueWebhookDelivery(postgresJobId);

    const job = await webhookQueue.getJob(postgresJobId);
    await job?.remove();

    const afterRemoval = await webhookQueue.getJob(postgresJobId);
    expect(afterRemoval).toBeUndefined();
  });

  // Empirically verified against real Redis (not assumed): calling add()
  // twice with the same deterministic jobId does NOT throw, and does NOT
  // create a second entry in the queue — the waiting count stays flat
  // across the second call. BullMQ resolves the second call with a Job
  // object carrying the same id/data as the first. This is a property of
  // BullMQ's deterministic jobId handling, not our own idempotency
  // mechanism — PostgreSQL remains the actual source of truth for whether
  // a job has already been processed.
  it("does not throw and does not create a duplicate waiting job when the same jobId is enqueued twice", async () => {
    const postgresJobId = randomUUID();
    createdJobIds.push(postgresJobId);

    const first = await enqueueWebhookDelivery(postgresJobId);
    const countsAfterFirst = await webhookQueue.getJobCounts();

    const second = await enqueueWebhookDelivery(postgresJobId);
    const countsAfterSecond = await webhookQueue.getJobCounts();

    expect(second.id).toBe(first.id);
    expect(second.data).toEqual(first.data);
    expect(countsAfterSecond.waiting).toBe(countsAfterFirst.waiting);
  });
});
