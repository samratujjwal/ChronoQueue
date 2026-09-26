import { randomUUID } from "node:crypto";
import { Queue } from "bullmq";
import { Redis } from "ioredis";
import { afterAll, describe, expect, it } from "vitest";
import {
  bullJobExists,
  getQueueCounts,
  removeBullJob,
  waitFor,
} from "./queue-helpers.js";

const REDIS_URL = process.env.REDIS_URL;
if (!REDIS_URL) {
  throw new Error("REDIS_URL must be set to run queue helper tests");
}

const connection = new Redis(REDIS_URL, { maxRetriesPerRequest: null });
const queueName = `testkit-helpers-${randomUUID()}`;
const queue = new Queue(queueName, { connection });

afterAll(async () => {
  await queue.obliterate({ force: true });
  await queue.close();
  await connection.quit();
});

describe("bullJobExists / removeBullJob", () => {
  it("detects a known job and removes only that job", async () => {
    const jobId = `testkit-job-${randomUUID()}`;
    await queue.add("test", { marker: true }, { jobId });

    expect(await bullJobExists(queue, jobId)).toBe(true);
    expect(await bullJobExists(queue, `missing-${randomUUID()}`)).toBe(false);

    await removeBullJob(queue, jobId);
    expect(await bullJobExists(queue, jobId)).toBe(false);

    await expect(
      removeBullJob(queue, `missing-${randomUUID()}`),
    ).resolves.toBeUndefined();
  });
});

describe("getQueueCounts", () => {
  it("reflects jobs added to the queue", async () => {
    const jobId = `testkit-count-${randomUUID()}`;
    const before = await getQueueCounts(queue);
    await queue.add("test", { marker: true }, { jobId });
    const after = await getQueueCounts(queue);
    const waitingBefore = before["waiting"] ?? 0;
    const waitingAfter = after["waiting"] ?? 0;
    expect(waitingAfter).toBeGreaterThanOrEqual(waitingBefore + 1);
    await removeBullJob(queue, jobId);
  });
});

describe("waitFor", () => {
  it("resolves as soon as the condition becomes true", async () => {
    let ready = false;
    setTimeout(() => {
      ready = true;
    }, 150);
    const startedAt = Date.now();
    await waitFor(() => ready, { timeoutMs: 2000, intervalMs: 25 });
    expect(Date.now() - startedAt).toBeLessThan(2000);
  });

  it("supports async conditions", async () => {
    let calls = 0;
    await waitFor(
      async () => {
        calls += 1;
        return calls >= 3;
      },
      { timeoutMs: 2000, intervalMs: 10 },
    );
    expect(calls).toBeGreaterThanOrEqual(3);
  });

  it("times out with a descriptive error instead of sleeping arbitrarily", async () => {
    const startedAt = Date.now();
    await expect(
      waitFor(() => false, {
        timeoutMs: 300,
        intervalMs: 25,
        description: "never-true",
      }),
    ).rejects.toThrow(/Timed out after 300ms waiting for: never-true/);
    const elapsed = Date.now() - startedAt;
    expect(elapsed).toBeGreaterThanOrEqual(250);
    expect(elapsed).toBeLessThan(2000);
  });
});
