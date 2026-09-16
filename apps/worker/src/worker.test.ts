import { randomUUID } from "node:crypto";
import { Queue, Worker, type Job } from "bullmq";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import { afterAll, describe, expect, it } from "vitest";
import { connection } from "./queue/connection.js";
import { db, pool } from "./db/client.js";
import { processWebhookDeliveryJob } from "./processor.js";

const queue = new Queue("webhook-delivery", { connection });
const testWorker = new Worker("webhook-delivery", processWebhookDeliveryJob, {
  connection,
});

const insertedPostgresIds: string[] = [];

afterAll(async () => {
  for (const id of insertedPostgresIds) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
  await testWorker.close();
  await queue.close();
  await connection.quit();
  await pool.end();
});

interface JobOutcome {
  status: "completed" | "failed";
  error?: Error;
}

function waitForJobOutcome(bullJobId: string): Promise<JobOutcome> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for job ${bullJobId} to settle`));
    }, 10_000);

    function onCompleted(job: Job) {
      if (job.id === bullJobId) {
        cleanup();
        resolve({ status: "completed" });
      }
    }

    function onFailed(job: Job | undefined, err: Error) {
      if (job?.id === bullJobId) {
        cleanup();
        resolve({ status: "failed", error: err });
      }
    }

    function cleanup() {
      clearTimeout(timeout);
      testWorker.off("completed", onCompleted);
      testWorker.off("failed", onFailed);
    }

    testWorker.on("completed", onCompleted);
    testWorker.on("failed", onFailed);
  });
}

describe("worker: processWebhookDeliveryJob (real Redis + real PostgreSQL)", () => {
  it("loads the referenced PostgreSQL job and completes the BullMQ job", async () => {
    const postgresJobId = randomUUID();
    insertedPostgresIds.push(postgresJobId);

    await db.insert(jobs).values({
      id: postgresJobId,
      type: "WEBHOOK",
      status: "PENDING",
      targetUrl: "https://example.com/worker-test",
      payload: { test: true },
    });

    const outcome = waitForJobOutcome(postgresJobId);
    await queue.add(
      "deliver-webhook",
      { jobId: postgresJobId },
      { jobId: postgresJobId, removeOnComplete: true, removeOnFail: true },
    );

    const result = await outcome;
    expect(result.status).toBe("completed");
  });

  it("fails the BullMQ job naturally when the referenced PostgreSQL job does not exist", async () => {
    const missingPostgresJobId = randomUUID();
    // intentionally not inserted into PostgreSQL

    const outcome = waitForJobOutcome(missingPostgresJobId);
    await queue.add(
      "deliver-webhook",
      { jobId: missingPostgresJobId },
      {
        jobId: missingPostgresJobId,
        removeOnComplete: true,
        removeOnFail: true,
      },
    );

    const result = await outcome;
    expect(result.status).toBe("failed");
    expect(result.error?.message).toContain(missingPostgresJobId);
  });
});
