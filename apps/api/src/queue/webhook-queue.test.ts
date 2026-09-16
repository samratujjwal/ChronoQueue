import { afterAll, describe, expect, it } from "vitest";
import { webhookQueue } from "./webhook-queue.js";

describe("webhook delivery queue", () => {
  it("is instantiated with the expected name and becomes ready", async () => {
    await webhookQueue.waitUntilReady();
    expect(webhookQueue.name).toBe("webhook-delivery");
  });

  it("can add and remove a job against real Redis", async () => {
    const job = await webhookQueue.add("verification-job", { test: true });
    expect(job.id).toBeDefined();

    const fetched = await webhookQueue.getJob(job.id!);
    expect(fetched?.data).toEqual({ test: true });

    await job.remove();
    const afterRemoval = await webhookQueue.getJob(job.id!);
    expect(afterRemoval).toBeUndefined();
  });

  afterAll(async () => {
    await webhookQueue.close();
  });
});
