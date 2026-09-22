import type { Logger } from "pino";
import { and, eq, isNotNull, lt } from "drizzle-orm";
import {
  jobs,
  isValidTransition,
  type JobStatus,
  applyFullJitter,
  calculateExponentialDelayMs,
} from "@chronoqueue/db";
import {
  SCHEDULER_EVENTS,
  safeError,
  safely,
} from "@chronoqueue/observability";
import { db } from "./db/client.js";
import { logger as defaultLogger } from "./logger.js";
import {
  schedulerMetrics as defaultSchedulerMetrics,
  type SchedulerMetrics,
} from "./observability.js";
import { config } from "./config/env.js";

export interface RecoveryResult {
  staleCount: number;
  recoveredJobIds: string[];
}

// Test seam: production passes nothing; tests inject a capture logger and
// a fresh metrics registry.
export interface RecoveryDeps {
  logger?: Logger;
  schedulerMetrics?: SchedulerMetrics;
}

// A job is stale when it's still PROCESSING but its lease expired — the
// worker that claimed it is presumed crashed/unreachable (at-least-once,
// not exactly-once: we cannot know whether the webhook actually fired).
function staleCondition(now: Date) {
  return and(
    eq(jobs.status, "PROCESSING"),
    isNotNull(jobs.leaseUntil),
    lt(jobs.leaseUntil, now),
  );
}

export async function recoverStaleJobs(
  deps: RecoveryDeps = {},
): Promise<RecoveryResult> {
  const logger = deps.logger ?? defaultLogger;
  const metrics = deps.schedulerMetrics ?? defaultSchedulerMetrics;

  const staleJobs = await db
    .select({
      id: jobs.id,
      attempts: jobs.attempts,
      maxAttempts: jobs.maxAttempts,
    })
    .from(jobs)
    .where(staleCondition(new Date()));

  if (staleJobs.length === 0) {
    return { staleCount: 0, recoveredJobIds: [] };
  }

  logger.info(
    {
      event: SCHEDULER_EVENTS.expiredLeaseRecovered,
      staleCount: staleJobs.length,
    },
    "recovery: stale processing jobs found",
  );

  const recoveredJobIds: string[] = [];

  for (const job of staleJobs) {
    try {
      // Same retry-engine semantics as a real webhook failure (Day 10/11):
      // increment attempts, and only stay retryable while under
      // maxAttempts. A crash is treated as an incomplete attempt, not a
      // free pass — otherwise a job whose worker keeps crashing would
      // recover forever and never reach DEAD.
      const attempts = job.attempts + 1;
      const willRetry = attempts < job.maxAttempts;
      const nextStatus: JobStatus = willRetry ? "RETRYING" : "DEAD";

      if (!isValidTransition("PROCESSING", nextStatus)) {
        logger.error(
          {
            event: SCHEDULER_EVENTS.expiredLeaseRecovered,
            jobId: job.id,
            nextStatus,
          },
          "recovery: illegal transition, skipping job",
        );
        continue;
      }

      let nextAttemptAt: Date | null = null;
      if (willRetry) {
        const exponentialDelayMs = calculateExponentialDelayMs(attempts, {
          baseDelayMs: config.RETRY_BASE_DELAY_MS,
          maxDelayMs: config.RETRY_MAX_DELAY_MS,
        });
        nextAttemptAt = new Date(
          Date.now() + applyFullJitter(exponentialDelayMs),
        );
      }

      // Guarded claim, re-checked at UPDATE time (not the earlier SELECT's
      // snapshot): only recovers if the row is STILL PROCESSING with a
      // lease that is STILL expired right now. This protects against:
      //  - the worker completing/failing normally in between (Case 4),
      //  - the worker renewing the lease in between (Case 5),
      //  - a second recovery process already having won (Case 3).
      // Any of those make this affect zero rows, and we just skip it.
      const [recovered] = await db
        .update(jobs)
        .set({
          status: nextStatus,
          attempts,
          leaseUntil: null,
          leaseToken: null,
          nextAttemptAt,
          // Day 14 DLQ: a crash-recovered job carries no webhook error, but
          // the DLQ still needs to say why it died / was re-driven.
          lastErrorCode: "WORKER_CRASH",
          lastErrorMessage:
            "Worker lease expired while PROCESSING — presumed crashed; recovered by scheduler",
          updatedAt: new Date(),
        })
        .where(and(eq(jobs.id, job.id), staleCondition(new Date())))
        .returning({ id: jobs.id });

      if (!recovered) {
        logger.info(
          {
            event: SCHEDULER_EVENTS.expiredLeaseRecovered,
            recovered: false,
            jobId: job.id,
          },
          "recovery: job no longer stale by the time of update, skipping",
        );
        continue;
      }

      recoveredJobIds.push(job.id);

      // Metrics follow the guarded transition (PART 6): only a successful
      // PG write counts, labeled by source so dashboards can separate
      // crash-recoveries from webhook-failure retries/deaths.
      safely(() => {
        if (nextStatus === "RETRYING") {
          metrics.jobsRetriedTotal.inc({ source: "recovery" });
        } else {
          metrics.jobsDeadTotal.inc({ source: "recovery" });
        }
      });

      logger.info(
        {
          event: SCHEDULER_EVENTS.expiredLeaseRecovered,
          recovered: true,
          jobId: job.id,
          nextStatus,
          attempts,
          errorCode: "WORKER_CRASH",
        },
        "recovery: recovered stale processing job",
      );
    } catch (error) {
      logger.error(
        {
          event: SCHEDULER_EVENTS.expiredLeaseRecovered,
          jobId: job.id,
          ...safeError(error),
        },
        "recovery: failed to recover job, continuing with remaining batch",
      );
    }
  }

  return { staleCount: staleJobs.length, recoveredJobIds };
}
