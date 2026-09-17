import { webhookQueue } from "./webhook-queue.js";

export interface WebhookDeliveryJobData {
  jobId: string;
}

// This mirrors apps/api/src/queue/webhook-producer.ts intentionally.
// It is a 4-line wrapper around a well-known queue/job-name convention
// ("webhook-delivery" / "deliver-webhook" / { jobId }), already duplicated
// in spirit across apps/api and apps/worker (each owns its own Redis
// connection with different settings). Introducing a shared package for
// this one function would be more architecture than the logic warrants.
export async function enqueueWebhookDelivery(jobId: string) {
  return webhookQueue.add(
    "deliver-webhook",
    { jobId } satisfies WebhookDeliveryJobData,
    { jobId },
  );
}
