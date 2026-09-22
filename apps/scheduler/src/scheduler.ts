import type { Logger } from "pino";
import { and, eq, isNotNull, lte, or, sql } from "drizzle-orm";
import { jobs, isValidTransition } from "@chronoqueue/db";
import {
  SCHEDULER_EVENTS,
  safeError,
  safely,
} from "@chronoqueue/observability";
import { db } from "./db/client.js";
import { enqueueWebhookDelivery } from "./queue/webhook-producer.js";
import { logger as defaultLogger } from "./logger.js";
import {
  schedulerMetrics as defaultSchedulerMetrics,
  type SchedulerMetrics,
} from "./observability.js";
import { config } from "./config/env.js";

export interface PollResult {
  dueCount: number;
  enqueuedJobIds: string[];
}

// Test seam: production passes nothing; tests inject a capture logger, a
// fresh metrics registry, and optionally a stubbed enqueue function to
// simulate BullMQ failures deterministically.
export interface SchedulerDeps {
  logger?: Logger;
  schedulerMetrics?: SchedulerMetrics;
  enqueue?: (jobId: string) => Promise<{ id?: string } | unknown>;
}

// A job is due when either:
// - it's a first-time job: status=PENDING and scheduledAt <= now, or
// - it's a retry (Day 11): status=RETRYING and nextAttemptAt is set and <= now
function dueCondition(now: Date) {
  return or(
    and(eq(jobs.status, "PENDING"), lte(jobs.scheduledAt, now)),
    and(
      eq(jobs.status, "RETRYING"),
      isNotNull(jobs.nextAttemptAt),
      lte(jobs.nextAttemptAt, now),
    ),
  );
}

export async function runSchedulerPoll(
  deps: SchedulerDeps = {},
): Promise<PollResult> {
  const logger = deps.logger ?? defaultLogger;
  const metrics = deps.schedulerMetrics ?? defaultSchedulerMetrics;
  const enqueue = deps.enqueue ?? enqueueWebhookDelivery;

  const pollStart = performance.now();
  const dueJobs = await db
    .select({ id: jobs.id, status: jobs.status })
    .from(jobs)
    .where(dueCondition(new Date()))
    .orderBy(sql`coalesce(${jobs.nextAttemptAt}, ${jobs.scheduledAt}) asc`)
    .limit(config.SCHEDULER_BATCH_SIZE);

  if (dueJobs.length === 0) {
    // Deliberately silent at INFO: an idle poll every second is not an
    // event (PART 12). Poll latency is observable at DEBUG.
    logger.debug(
      {
        event: SCHEDULER_EVENTS.schedulerPollCompleted,
        dueCount: 0,
        enqueuedCount: 0,
        durationMs: Math.round(performance.now() - pollStart),
      },
      "scheduler poll completed — no due jobs",
    );
    return { dueCount: 0, enqueuedJobIds: [] };
  }

  logger.info(
    {
      event: SCHEDULER_EVENTS.schedulerPollCompleted,
      dueCount: dueJobs.length,
      durationMs: Math.round(performance.now() - pollStart),
    },
    "scheduler poll: due jobs found",
  );

  const enqueuedJobIds: string[] = [];

  for (const job of dueJobs) {
    if (!isValidTransition(job.status, "QUEUED")) {
      logger.error(
        {
          event: SCHEDULER_EVENTS.jobEnqueueFailed,
          stage: "db_claim",
          jobId: job.id,
          previousStatus: job.status,
        },
        "scheduler: illegal transition to QUEUED, skipping job",
      );
      safely(() =>
        metrics.schedulerEnqueueFailuresTotal.inc({ stage: "db_claim" }),
      );
      continue;
    }

    // Guarded claim: only succeeds if the row is still in the exact
    // status we just read it as. If another scheduler instance already
    // claimed it, this affects zero rows and we skip it — this is the
    // safety net against two schedulers both enqueuing the same job.
    let claimed: { id: string } | undefined;
    try {
      [claimed] = await db
        .update(jobs)
        .set({ status: "QUEUED", updatedAt: new Date() })
        .where(and(eq(jobs.id, job.id), eq(jobs.status, job.status)))
        .returning({ id: jobs.id });
    } catch (error) {
      // DB claim failure: distinct from a queue failure (PART 9).
      logger.error(
        {
          event: SCHEDULER_EVENTS.jobEnqueueFailed,
          stage: "db_claim",
          jobId: job.id,
          previousStatus: job.status,
          ...safeError(error),
        },
        "scheduler: failed to claim job in postgres, continuing with remaining batch",
      );
      safely(() =>
        metrics.schedulerEnqueueFailuresTotal.inc({ stage: "db_claim" }),
      );
      continue;
    }

    if (!claimed) {
      // Lost the race to another scheduler — healthy coordination, not a
      // failure. No failure metric; the winner enqueues the job.
      logger.info(
        {
          event: SCHEDULER_EVENTS.jobClaimedForEnqueue,
          claimed: false,
          jobId: job.id,
          previousStatus: job.status,
        },
        "scheduler: claim lost to another process, skipping job",
      );
      continue;
    }

    logger.debug(
      {
        event: SCHEDULER_EVENTS.jobClaimedForEnqueue,
        claimed: true,
        jobId: job.id,
        previousStatus: job.status,
        newStatus: "QUEUED",
      },
      "scheduler: claimed job for enqueue",
    );

    // Known limitation: the PostgreSQL claim (above) and the BullMQ
    // enqueue (below) are two separate systems with no shared
    // transaction. If the process crashes or enqueue fails between
    // these two steps, the job is left QUEUED in PostgreSQL with no
    // corresponding BullMQ message — a dual-write gap. Durable
    // enqueue/idempotency handling is deferred to a later reliability
    // phase; it is not solved here.
    logger.debug(
      {
        event: SCHEDULER_EVENTS.jobEnqueueStarted,
        jobId: job.id,
      },
      "scheduler: enqueueing job to bullmq",
    );

    try {
      const bullJob = (await enqueue(job.id)) as { id?: unknown };
      enqueuedJobIds.push(job.id);

      logger.debug(
        {
          event: SCHEDULER_EVENTS.jobEnqueueSucceeded,
          jobId: job.id,
          previousStatus: job.status,
          newStatus: "QUEUED",
          bullmqJobId: typeof bullJob?.id === "string" ? bullJob.id : undefined,
        },
        "scheduler: claimed and enqueued job",
      );
    } catch (error) {
      // BullMQ/Redis enqueue failure: distinct from a DB claim failure
      // (PART 9). The row stays QUEUED — the known dual-write gap — and
      // the failure is counted by stage so dashboards can alert on it.
      logger.error(
        {
          event: SCHEDULER_EVENTS.jobEnqueueFailed,
          stage: "bullmq_enqueue",
          jobId: job.id,
          previousStatus: job.status,
          ...safeError(error),
        },
        "scheduler: failed to enqueue job to bullmq, continuing with remaining batch",
      );
      safely(() =>
        metrics.schedulerEnqueueFailuresTotal.inc({ stage: "bullmq_enqueue" }),
      );
    }
  }

  return { dueCount: dueJobs.length, enqueuedJobIds };
}
