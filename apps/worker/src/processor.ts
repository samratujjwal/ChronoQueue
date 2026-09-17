import type { Job } from "bullmq";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import { db } from "./db/client.js";
import { logger } from "./logger.js";
import { deliverWebhook } from "./webhook-delivery.js";

export interface WebhookDeliveryJobData {
  jobId: string;
}

function isWebhookDeliveryJobData(
  data: unknown,
): data is WebhookDeliveryJobData {
  return (
    typeof data === "object" &&
    data !== null &&
    "jobId" in data &&
    typeof (data as { jobId: unknown }).jobId === "string"
  );
}

export async function processWebhookDeliveryJob(job: Job): Promise<void> {
  if (!isWebhookDeliveryJobData(job.data)) {
    throw new Error(
      `Job ${job.id} has invalid data — expected { jobId: string }, got ${JSON.stringify(job.data)}`,
    );
  }

  const { jobId } = job.data;

  logger.info(
    { bullJobId: job.id, postgresJobId: jobId },
    "processing webhook delivery job",
  );

  const [businessJob] = await db
    .select()
    .from(jobs)
    .where(eq(jobs.id, jobId))
    .limit(1);

  if (!businessJob) {
    logger.error(
      { bullJobId: job.id, postgresJobId: jobId },
      "postgresql job not found for queued webhook delivery job",
    );
    throw new Error(
      `PostgreSQL job ${jobId} not found (referenced by BullMQ job ${job.id})`,
    );
  }

  logger.info(
    {
      bullJobId: job.id,
      postgresJobId: businessJob.id,
      status: businessJob.status,
    },
    "loaded postgresql job successfully",
  );

  const startedAt = Date.now();

  try {
    const statusCode = await deliverWebhook(
      businessJob.targetUrl,
      businessJob.payload,
    );
    const durationMs = Date.now() - startedAt;

    logger.info(
      {
        bullJobId: job.id,
        postgresJobId: businessJob.id,
        targetUrl: businessJob.targetUrl,
        statusCode,
        durationMs,
      },
      "webhook delivered successfully",
    );
  } catch (error) {
    const durationMs = Date.now() - startedAt;

    logger.error(
      {
        bullJobId: job.id,
        postgresJobId: businessJob.id,
        targetUrl: businessJob.targetUrl,
        durationMs,
        err: error instanceof Error ? error.message : String(error),
      },
      "webhook delivery failed",
    );

    throw error;
  }
}
