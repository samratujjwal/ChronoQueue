import { and, asc, eq, lte } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import { db } from "./db/client.js";
import { enqueueWebhookDelivery } from "./queue/webhook-producer.js";
import { logger } from "./logger.js";
import { config } from "./config/env.js";

export interface PollResult {
  dueCount: number;
  enqueuedJobIds: string[];
}

export async function runSchedulerPoll(): Promise<PollResult> {
  const dueJobs = await db
    .select({ id: jobs.id })
    .from(jobs)
    .where(and(eq(jobs.status, "PENDING"), lte(jobs.scheduledAt, new Date())))
    .orderBy(asc(jobs.scheduledAt))
    .limit(config.SCHEDULER_BATCH_SIZE);

  if (dueJobs.length === 0) {
    return { dueCount: 0, enqueuedJobIds: [] };
  }

  logger.info({ dueCount: dueJobs.length }, "scheduler poll: due jobs found");

  const enqueuedJobIds: string[] = [];

  for (const job of dueJobs) {
    const bullJob = await enqueueWebhookDelivery(job.id);
    enqueuedJobIds.push(job.id);
    logger.info(
      { postgresJobId: job.id, bullmqJobId: bullJob.id },
      "scheduler: enqueued job",
    );
  }

  return { dueCount: dueJobs.length, enqueuedJobIds };
}
