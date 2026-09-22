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

// Day 14 DLQ: enqueue for a manually re-triggered job.
//
// A previous attempt of this PG job almost certainly left a BullMQ job
// record (completed/failed) under the same deterministic jobId (the PG id).
// Queue.add with an existing jobId SILENTLY returns the old record instead
// of queueing a new message — without the removal below, a re-triggered
// job would sit in QUEUED forever with no BullMQ message to drive it.
// Removing it first is safe here: the operator explicitly requested
// reprocessing, the PG row is the source of truth, and the old record
// refers to a dead execution attempt, not live work.
export async function enqueueRetriggeredJob(jobId: string) {
  const staleRecord = await webhookQueue.getJob(jobId);
  await staleRecord?.remove();

  return webhookQueue.add(
    "deliver-webhook",
    { jobId } satisfies WebhookDeliveryJobData,
    { jobId },
  );
}
