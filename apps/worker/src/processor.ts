import type { Job } from "bullmq";
import { eq } from "drizzle-orm";
import {
  jobs,
  isValidTransition,
  type JobStatus,
  applyFullJitter,
  calculateExponentialDelayMs,
} from "@chronoqueue/db";
import { db } from "./db/client.js";
import { logger } from "./logger.js";
import { deliverWebhook, WebhookDeliveryError } from "./webhook-delivery.js";
import { config } from "./config/env.js";
import { claimJob, renewLease, completeProcessing } from "./lease.js";

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

// Periodically extends the lease while a job is still PROCESSING, so a
// slow-but-alive webhook attempt isn't mistaken for a crashed worker by
// Scheduler-side recovery. Fenced by leaseToken: if renewLease() reports
// this worker no longer owns the row (a newer attempt has since claimed
// it), renewal stops immediately rather than continuing to try. Renewal
// errors are caught and logged, never thrown, so a transient DB hiccup
// here cannot crash the process or leave an unhandled rejection.
function startLeaseRenewal(
  jobId: string,
  bullJobId: string | undefined,
  leaseToken: string,
): NodeJS.Timeout {
  const timer = setInterval(() => {
    renewLease(jobId, leaseToken)
      .then((renewed) => {
        if (renewed) {
          logger.debug({ bullJobId, postgresJobId: jobId }, "lease renewed");
          return;
        }

        logger.warn(
          { bullJobId, postgresJobId: jobId },
          "lease renewal fenced out — this worker no longer owns the processing attempt, stopping renewal",
        );
        clearInterval(timer);
      })
      .catch((error) => {
        logger.error(
          {
            bullJobId,
            postgresJobId: jobId,
            err: error instanceof Error ? error.message : String(error),
          },
          "lease renewal failed",
        );
      });
  }, config.WORKER_LEASE_RENEWAL_INTERVAL_MS);

  return timer;
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

  const claimResult = await claimJob(jobId);

  if (!claimResult) {
    logger.error(
      { bullJobId: job.id, postgresJobId: jobId, status: businessJob.status },
      "could not claim postgresql job for processing — unexpected status",
    );
    throw new Error(
      `PostgreSQL job ${jobId} could not be claimed for processing (status was ${businessJob.status})`,
    );
  }

  const { job: claimed, leaseToken } = claimResult;

  const startedAt = Date.now();
  const renewalTimer = startLeaseRenewal(jobId, job.id, leaseToken);

  try {
    try {
      const statusCode = await deliverWebhook(
        claimed.targetUrl,
        claimed.payload,
      );
      const durationMs = Date.now() - startedAt;
      const attempts = claimed.attempts + 1;

      if (!isValidTransition("PROCESSING", "SUCCEEDED")) {
        throw new Error("Illegal state transition PROCESSING -> SUCCEEDED");
      }

      const updated = await completeProcessing(jobId, leaseToken, {
        status: "SUCCEEDED",
        attempts,
        nextAttemptAt: null,
      });

      if (!updated) {
        logger.warn(
          { bullJobId: job.id, postgresJobId: businessJob.id },
          "fenced out before recording success — a newer processing attempt now owns this job; discarding this result",
        );
      }

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

      let nextAttemptAt: Date | null = null;
      let exponentialDelayMs: number | undefined;
      let jitteredDelayMs: number | undefined;

      if (willRetry) {
        exponentialDelayMs = calculateExponentialDelayMs(attempts, {
          baseDelayMs: config.RETRY_BASE_DELAY_MS,
          maxDelayMs: config.RETRY_MAX_DELAY_MS,
        });
        jitteredDelayMs = applyFullJitter(exponentialDelayMs);
        nextAttemptAt = new Date(Date.now() + jitteredDelayMs);
      }

      if (isValidTransition("PROCESSING", nextStatus)) {
        const updated = await completeProcessing(jobId, leaseToken, {
          status: nextStatus,
          attempts,
          nextAttemptAt,
        });

        if (!updated) {
          logger.warn(
            { bullJobId: job.id, postgresJobId: businessJob.id, nextStatus },
            "fenced out before recording failure outcome — a newer processing attempt now owns this job; discarding this result",
          );
        }
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
          exponentialDelayMs,
          jitteredDelayMs,
          nextAttemptAt,
          err: error instanceof Error ? error.message : String(error),
        },
        nextStatus === "RETRYING"
          ? "webhook delivery failed, scheduled for retry"
          : "webhook delivery failed permanently, job marked dead",
      );

      throw error;
    }
  } finally {
    clearInterval(renewalTimer);
  }
}
