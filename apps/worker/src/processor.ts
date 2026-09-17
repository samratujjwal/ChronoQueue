import type { Job } from "bullmq";
import { and, eq, inArray } from "drizzle-orm";
import { jobs, isValidTransition, type JobStatus } from "@chronoqueue/db";
import { db } from "./db/client.js";
import { logger } from "./logger.js";
import { deliverWebhook, WebhookDeliveryError } from "./webhook-delivery.js";

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

// Nothing currently transitions PENDING -> QUEUED -> PROCESSING (the
// Scheduler only discovers due jobs and enqueues; see Day 9's documented
// limitation). The Worker claims whichever pre-execution status the job is
// actually in — this is a mechanical "I'm starting work on this" step, not
// a business-state decision, so it's a single conditional UPDATE rather
// than a chain of individually-validated state-machine hops. The WHERE
// clause is the actual safety net: only one concurrent claim can win.
const CLAIMABLE_STATUSES: JobStatus[] = ["PENDING", "QUEUED", "RETRYING"];

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

  const [claimed] = await db
    .update(jobs)
    .set({ status: "PROCESSING", updatedAt: new Date() })
    .where(and(eq(jobs.id, jobId), inArray(jobs.status, CLAIMABLE_STATUSES)))
    .returning();

  if (!claimed) {
    logger.error(
      { bullJobId: job.id, postgresJobId: jobId, status: businessJob.status },
      "could not claim postgresql job for processing — unexpected status",
    );
    throw new Error(
      `PostgreSQL job ${jobId} could not be claimed for processing (status was ${businessJob.status})`,
    );
  }

  const startedAt = Date.now();

  try {
    const statusCode = await deliverWebhook(claimed.targetUrl, claimed.payload);
    const durationMs = Date.now() - startedAt;
    const attempts = claimed.attempts + 1;

    if (!isValidTransition("PROCESSING", "SUCCEEDED")) {
      throw new Error("Illegal state transition PROCESSING -> SUCCEEDED");
    }

    await db
      .update(jobs)
      .set({
        status: "SUCCEEDED",
        attempts,
        nextAttemptAt: null,
        updatedAt: new Date(),
      })
      .where(and(eq(jobs.id, jobId), eq(jobs.status, "PROCESSING")));

    logger.info(
      {
        bullJobId: job.id,
        postgresJobId: businessJob.id,
        targetUrl: claimed.targetUrl,
        statusCode,
        durationMs,
        attempts,
      },
      "webhook delivered successfully",
    );
  } catch (error) {
    const durationMs = Date.now() - startedAt;
    const attempts = claimed.attempts + 1;

    const retryable =
      error instanceof WebhookDeliveryError ? error.retryable : false;
    const statusCode =
      error instanceof WebhookDeliveryError ? error.statusCode : undefined;
    const willRetry = retryable && attempts < claimed.maxAttempts;
    const nextStatus: JobStatus = willRetry ? "RETRYING" : "DEAD";

    if (isValidTransition("PROCESSING", nextStatus)) {
      await db
        .update(jobs)
        .set({
          status: nextStatus,
          attempts,
          nextAttemptAt: willRetry ? new Date() : null,
          updatedAt: new Date(),
        })
        .where(and(eq(jobs.id, jobId), eq(jobs.status, "PROCESSING")));
    } else {
      logger.error(
        { bullJobId: job.id, postgresJobId: businessJob.id, nextStatus },
        "illegal state transition computed by retry engine — postgresql row not updated",
      );
    }

    logger.error(
      {
        bullJobId: job.id,
        postgresJobId: businessJob.id,
        targetUrl: claimed.targetUrl,
        durationMs,
        attempts,
        maxAttempts: claimed.maxAttempts,
        retryable,
        statusCode,
        nextStatus,
        err: error instanceof Error ? error.message : String(error),
      },
      nextStatus === "RETRYING"
        ? "webhook delivery failed, scheduled for retry"
        : "webhook delivery failed permanently, job marked dead",
    );

    throw error;
  }
}
