import { webhookQueue } from "./webhook-queue.js";

export interface WebhookDeliveryJobData {
  jobId: string;
}

export async function enqueueWebhookDelivery(jobId: string) {
  return webhookQueue.add(
    "deliver-webhook",
    { jobId } satisfies WebhookDeliveryJobData,
    { jobId },
  );
}
