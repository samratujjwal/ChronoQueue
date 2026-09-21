import { and, eq, isNotNull, lt } from "drizzle-orm";
import {
  jobs,
  isValidTransition,
  type JobStatus,
  applyFullJitter,
  calculateExponentialDelayMs,
} from "@chronoqueue/db";
import { db } from "./db/client.js";
import { logger } from "./logger.js";
import { config } from "./config/env.js";

export interface RecoveryResult {
  staleCount: number;
  recoveredJobIds: string[];
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

export async function recoverStaleJobs(): Promise<RecoveryResult> {
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
    { staleCount: staleJobs.length },
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
          { postgresJobId: job.id, nextStatus },
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
          updatedAt: new Date(),
        })
        .where(and(eq(jobs.id, job.id), staleCondition(new Date())))
        .returning({ id: jobs.id });

      if (!recovered) {
        logger.info(
          { postgresJobId: job.id },
          "recovery: job no longer stale by the time of update, skipping",
        );
        continue;
      }

      recoveredJobIds.push(job.id);

      logger.info(
        { postgresJobId: job.id, nextStatus, attempts },
        "recovery: recovered stale processing job",
      );
    } catch (error) {
      logger.error(
        {
          postgresJobId: job.id,
          err: error instanceof Error ? error.message : String(error),
        },
        "recovery: failed to recover job, continuing with remaining batch",
      );
    }
  }

  return { staleCount: staleJobs.length, recoveredJobIds };
}
