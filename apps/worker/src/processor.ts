import type { Job } from "bullmq";
import type { Logger } from "pino";
import { eq } from "drizzle-orm";
import {
  jobs,
  isValidTransition,
  type JobStatus,
  applyFullJitter,
  calculateExponentialDelayMs,
} from "@chronoqueue/db";
import { safeError, safely, WORKER_EVENTS } from "@chronoqueue/observability";
import { db } from "./db/client.js";
import { logger as defaultLogger } from "./logger.js";
import {
  workerMetrics as defaultWorkerMetrics,
  type WorkerMetrics,
} from "./observability.js";
import { deliverWebhook, WebhookDeliveryError } from "./webhook-delivery.js";
import { config } from "./config/env.js";
import { claimJob, renewLease, completeProcessing } from "./lease.js";

export interface WebhookDeliveryJobData {
  jobId: string;
}

// Test seam: production call sites (BullMQ Worker) pass only `job`; tests
// inject a capture logger and a fresh metrics registry. Defaults keep the
// existing behavior unchanged.
export interface ProcessorDeps {
  logger?: Logger;
  workerMetrics?: WorkerMetrics;
}

// Day 14 DLQ: maps a delivery failure to a compact, queryable diagnosis
// that is persisted on the job row (last_error_code / last_error_message)
// so the dead-letter queue can show WHY a job died without log access.
function diagnoseFailure(error: unknown): {
  lastErrorCode: string;
  lastErrorMessage: string;
} {
  if (error instanceof WebhookDeliveryError) {
    const code =
      error.statusCode !== undefined
        ? `HTTP_${error.statusCode}`
        : error.kind === "timeout"
          ? "TIMEOUT"
          : "NETWORK_ERROR";
    return { lastErrorCode: code, lastErrorMessage: error.message };
  }
  return {
    lastErrorCode: "UNKNOWN_ERROR",
    lastErrorMessage: error instanceof Error ? error.message : String(error),
  };
}

// Operational hostname for logs: the full target URL can carry secrets in
// query params (e.g. ?token=...), so logs only ever see the hostname.
function targetHost(targetUrl: string): string {
  try {
    return new URL(targetUrl).hostname;
  } catch {
    return "[invalid-url]";
  }
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
//
// High-frequency by design: renewal logs at DEBUG only (PART 12).
function startLeaseRenewal(
  jobId: string,
  bullJobId: string | undefined,
  leaseToken: string,
  logger: Logger,
): NodeJS.Timeout {
  const timer = setInterval(() => {
    renewLease(jobId, leaseToken)
      .then((renewed) => {
        if (renewed) {
          logger.debug(
            {
              event: WORKER_EVENTS.leaseLost,
              leaseRenewed: true,
              bullJobId,
              jobId,
            },
            "lease renewed",
          );
          return;
        }

        // The token no longer owns the row — a newer attempt claimed it.
        // leaseOwned:false (never the token value) is the safe diagnostic.
        logger.warn(
          {
            event: WORKER_EVENTS.leaseLost,
            leaseOwned: false,
            bullJobId,
            jobId,
          },
          "lease renewal fenced out — this worker no longer owns the processing attempt, stopping renewal",
        );
        clearInterval(timer);
      })
      .catch((error) => {
        logger.error(
          {
            event: WORKER_EVENTS.leaseLost,
            bullJobId,
            jobId,
            ...safeError(error),
          },
          "lease renewal failed",
        );
      });
  }, config.WORKER_LEASE_RENEWAL_INTERVAL_MS);

  return timer;
}

export async function processWebhookDeliveryJob(
  job: Job,
  deps: ProcessorDeps = {},
): Promise<void> {
  const logger = deps.logger ?? defaultLogger;
  const metrics = deps.workerMetrics ?? defaultWorkerMetrics;

  if (!isWebhookDeliveryJobData(job.data)) {
    throw new Error(
      `Job ${job.id} has invalid data — expected { jobId: string }, got ${JSON.stringify(job.data)}`,
    );
  }

  const { jobId } = job.data;

  logger.info(
    {
      event: WORKER_EVENTS.jobProcessingStarted,
      bullJobId: job.id,
      jobId,
    },
    "processing webhook delivery job",
  );

  const [businessJob] = await db
    .select()
    .from(jobs)
    .where(eq(jobs.id, jobId))
    .limit(1);

  if (!businessJob) {
    logger.error(
      {
        event: WORKER_EVENTS.webhookFailed,
        bullJobId: job.id,
        jobId,
      },
      "postgresql job not found for queued webhook delivery job",
    );
    throw new Error(
      `PostgreSQL job ${jobId} not found (referenced by BullMQ job ${job.id})`,
    );
  }

  const claimResult = await claimJob(jobId);

  if (!claimResult) {
    // Another worker (or a previous attempt of this one) already owns or
    // finished this job. Not a failure — at-least-once coordination.
    safely(() => metrics.workerClaimConflictsTotal.inc());
    logger.warn(
      {
        event: WORKER_EVENTS.jobClaimed,
        claimed: false,
        bullJobId: job.id,
        jobId,
        status: businessJob.status,
      },
      "could not claim postgresql job for processing — already owned or finished",
    );
    throw new Error(
      `PostgreSQL job ${jobId} could not be claimed for processing (status was ${businessJob.status})`,
    );
  }

  const { job: claimed, leaseToken } = claimResult;
  const attempt = claimed.attempts + 1;

  logger.info(
    {
      event: WORKER_EVENTS.jobClaimed,
      claimed: true,
      leaseOwned: true,
      bullJobId: job.id,
      jobId,
      attempt,
    },
    "claimed postgresql job for processing",
  );

  // Monotonic clock for durations (PART 7): Date.now() can jump with NTP;
  // performance.now() cannot. Processing duration starts at successful
  // claim — queue wait time is deliberately excluded.
  const processingStart = performance.now();
  const renewalTimer = startLeaseRenewal(jobId, job.id, leaseToken, logger);

  try {
    try {
      logger.info(
        {
          event: WORKER_EVENTS.webhookStarted,
          bullJobId: job.id,
          jobId,
          attempt,
          targetHost: targetHost(claimed.targetUrl),
        },
        "starting webhook delivery attempt",
      );

      const webhookStart = performance.now();
      safely(() => metrics.webhookRequestsTotal.inc());
      const statusCode = await deliverWebhook(
        claimed.targetUrl,
        claimed.payload,
      );
      const webhookDurationSec = (performance.now() - webhookStart) / 1000;
      const durationMs = Math.round(performance.now() - processingStart);
      safely(() =>
        metrics.webhookDurationSeconds.observe(webhookDurationSec, {
          outcome: "success",
        }),
      );

      if (!isValidTransition("PROCESSING", "SUCCEEDED")) {
        throw new Error("Illegal state transition PROCESSING -> SUCCEEDED");
      }

      const updated = await completeProcessing(jobId, leaseToken, {
        status: "SUCCEEDED",
        attempts: attempt,
        nextAttemptAt: null,
        // A re-triggered job may carry failure diagnosis from its earlier
        // death; a fresh success clears it.
        lastErrorCode: null,
        lastErrorMessage: null,
      });

      if (!updated) {
        // Fenced out: the guarded transition did NOT succeed, so no
        // success metric is recorded (PART 6 — metrics follow transitions).
        logger.warn(
          {
            event: WORKER_EVENTS.leaseLost,
            leaseOwned: false,
            bullJobId: job.id,
            jobId,
            attempt,
          },
          "fenced out before recording success — a newer processing attempt now owns this job; discarding this result",
        );
      } else {
        safely(() => {
          metrics.jobsSucceededTotal.inc();
          metrics.jobProcessingDurationSeconds.observe(
            (performance.now() - processingStart) / 1000,
            { outcome: "succeeded" },
          );
        });
      }

      logger.info(
        {
          event: WORKER_EVENTS.webhookSucceeded,
          bullJobId: job.id,
          jobId,
          attempt,
          targetHost: targetHost(claimed.targetUrl),
          statusCode,
          durationMs,
          recorded: updated,
        },
        "webhook delivered successfully",
      );
    } catch (error) {
      const durationMs = Math.round(performance.now() - processingStart);

      const retryable =
        error instanceof WebhookDeliveryError ? error.retryable : false;
      const statusCode =
        error instanceof WebhookDeliveryError ? error.statusCode : undefined;
      const willRetry = retryable && attempt < claimed.maxAttempts;
      const nextStatus: JobStatus = willRetry ? "RETRYING" : "DEAD";
      const { lastErrorCode, lastErrorMessage } = diagnoseFailure(error);

      let nextAttemptAt: Date | null = null;
      let exponentialDelayMs: number | undefined;
      let jitteredDelayMs: number | undefined;

      if (willRetry) {
        exponentialDelayMs = calculateExponentialDelayMs(attempt, {
          baseDelayMs: config.RETRY_BASE_DELAY_MS,
          maxDelayMs: config.RETRY_MAX_DELAY_MS,
        });
        jitteredDelayMs = applyFullJitter(exponentialDelayMs);
        nextAttemptAt = new Date(Date.now() + jitteredDelayMs);
      }

      if (isValidTransition("PROCESSING", nextStatus)) {
        const updated = await completeProcessing(jobId, leaseToken, {
          status: nextStatus,
          attempts: attempt,
          nextAttemptAt,
          lastErrorCode,
          lastErrorMessage,
        });

        if (!updated) {
          logger.warn(
            {
              event: WORKER_EVENTS.leaseLost,
              leaseOwned: false,
              bullJobId: job.id,
              jobId,
              attempt,
              nextStatus,
            },
            "fenced out before recording failure outcome — a newer processing attempt now owns this job; discarding this result",
          );
        } else {
          // Metrics follow the guarded transition, not the attempt
          // (PART 6): only a successful PG write counts.
          safely(() => {
            if (nextStatus === "RETRYING") {
              metrics.jobsRetriedTotal.inc({ source: "worker" });
            } else {
              metrics.jobsDeadTotal.inc({ source: "worker" });
            }
            metrics.webhookFailuresTotal.inc({ error_code: lastErrorCode });
            metrics.webhookDurationSeconds.observe(durationMs / 1000, {
              outcome: "failure",
            });
            metrics.jobProcessingDurationSeconds.observe(durationMs / 1000, {
              outcome: nextStatus === "RETRYING" ? "retrying" : "dead",
            });
          });
        }
      } else {
        logger.error(
          {
            event: WORKER_EVENTS.webhookFailed,
            bullJobId: job.id,
            jobId,
            attempt,
            nextStatus,
          },
          "illegal state transition computed by retry engine — postgresql row not updated",
        );
      }

      // The failure diagnostic: which job, which attempt, why (code),
      // retryable, how long. No payload, no URL, no raw error dump.
      logger.error(
        {
          event: WORKER_EVENTS.webhookFailed,
          bullJobId: job.id,
          jobId,
          attempt,
          maxAttempts: claimed.maxAttempts,
          targetHost: targetHost(claimed.targetUrl),
          errorCode: lastErrorCode,
          retryable,
          statusCode,
          nextStatus,
          exponentialDelayMs,
          jitteredDelayMs,
          durationMs,
          ...safeError(error),
        },
        nextStatus === "RETRYING"
          ? "webhook delivery failed, scheduled for retry"
          : "webhook delivery failed permanently, job marked dead",
      );

      if (willRetry) {
        logger.info(
          {
            event: WORKER_EVENTS.jobRetryScheduled,
            bullJobId: job.id,
            jobId,
            attempt,
            nextAttemptAt: nextAttemptAt?.toISOString() ?? null,
            delayMs: jitteredDelayMs ?? null,
          },
          "job scheduled for retry",
        );
      } else {
        // DLQ observability (PART 10): everything needed to answer which
        // job, which attempt, why it died, and the durable diagnosis.
        // last_error_code/message remain the durable source in PG.
        logger.warn(
          {
            event: WORKER_EVENTS.jobMarkedDead,
            bullJobId: job.id,
            jobId,
            attempt,
            maxAttempts: claimed.maxAttempts,
            errorCode: lastErrorCode,
            errorMessage: lastErrorMessage,
            durationMs,
          },
          "job marked dead",
        );
      }

      throw error;
    }
  } finally {
    clearInterval(renewalTimer);
  }
}
