import { and, eq, isNotNull, lte, or, sql } from "drizzle-orm";
import { jobs, isValidTransition } from "@chronoqueue/db";
import { db } from "./db/client.js";
import { enqueueWebhookDelivery } from "./queue/webhook-producer.js";
import { logger } from "./logger.js";
import { config } from "./config/env.js";

export interface PollResult {
  dueCount: number;
  enqueuedJobIds: string[];
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

export async function runSchedulerPoll(): Promise<PollResult> {
  const dueJobs = await db
    .select({ id: jobs.id, status: jobs.status })
    .from(jobs)
    .where(dueCondition(new Date()))
    .orderBy(sql`coalesce(${jobs.nextAttemptAt}, ${jobs.scheduledAt}) asc`)
    .limit(config.SCHEDULER_BATCH_SIZE);

  if (dueJobs.length === 0) {
    return { dueCount: 0, enqueuedJobIds: [] };
  }

  logger.info({ dueCount: dueJobs.length }, "scheduler poll: due jobs found");

  const enqueuedJobIds: string[] = [];

  for (const job of dueJobs) {
    try {
      if (!isValidTransition(job.status, "QUEUED")) {
        logger.error(
          { postgresJobId: job.id, previousStatus: job.status },
          "scheduler: illegal transition to QUEUED, skipping job",
        );
        continue;
      }

      // Guarded claim: only succeeds if the row is still in the exact
      // status we just read it as. If another scheduler instance already
      // claimed it, this affects zero rows and we skip it — this is the
      // safety net against two schedulers both enqueuing the same job.
      const [claimed] = await db
        .update(jobs)
        .set({ status: "QUEUED", updatedAt: new Date() })
        .where(and(eq(jobs.id, job.id), eq(jobs.status, job.status)))
        .returning({ id: jobs.id });

      if (!claimed) {
        logger.info(
          { postgresJobId: job.id, previousStatus: job.status },
          "scheduler: claim lost to another process, skipping job",
        );
        continue;
      }

      // Known limitation: the PostgreSQL claim (above) and the BullMQ
      // enqueue (below) are two separate systems with no shared
      // transaction. If the process crashes or enqueue fails between
      // these two steps, the job is left QUEUED in PostgreSQL with no
      // corresponding BullMQ message — a dual-write gap. Durable
      // enqueue/idempotency handling is deferred to a later reliability
      // phase; it is not solved here.
      const bullJob = await enqueueWebhookDelivery(job.id);
      enqueuedJobIds.push(job.id);

      logger.info(
        {
          postgresJobId: job.id,
          previousStatus: job.status,
          newStatus: "QUEUED",
          bullmqJobId: bullJob.id,
        },
        "scheduler: claimed and enqueued job",
      );
    } catch (error) {
      logger.error(
        {
          postgresJobId: job.id,
          previousStatus: job.status,
          err: error instanceof Error ? error.message : String(error),
        },
        "scheduler: failed to claim/enqueue job, continuing with remaining batch",
      );
    }
  }

  return { dueCount: dueJobs.length, enqueuedJobIds };
}
