import { randomUUID } from "node:crypto";
import { inArray } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { jobs, type JobStatus } from "@chronoqueue/db";

export interface SeedJobsOptions {
  count: number;
  status?: JobStatus;
  scheduledAt?: Date | ((index: number) => Date);
  maxAttempts?: number;
  targetUrl?: string;
  idempotencyKeyPrefix?: string;
  payload?: unknown;
}

const INSERT_BATCH_SIZE = 500;

export async function seedJobs(
  db: NodePgDatabase<Record<string, never>>,
  options: SeedJobsOptions,
): Promise<string[]> {
  const {
    count,
    status = "PENDING",
    maxAttempts = 5,
    targetUrl = "http://127.0.0.1:9/unused",
    idempotencyKeyPrefix = "seed",
    payload = { seed: true },
  } = options;

  const ids: string[] = [];
  const rows: (typeof jobs.$inferInsert)[] = [];
  for (let i = 0; i < count; i += 1) {
    const id = randomUUID();
    ids.push(id);
    const scheduledAt =
      typeof options.scheduledAt === "function"
        ? options.scheduledAt(i)
        : (options.scheduledAt ?? new Date());
    rows.push({
      id,
      idempotencyKey: `${idempotencyKeyPrefix}-${i}-${randomUUID()}`,
      type: "WEBHOOK",
      status,
      targetUrl,
      payload,
      maxAttempts,
      scheduledAt,
    });
  }

  for (let i = 0; i < rows.length; i += INSERT_BATCH_SIZE) {
    await db.insert(jobs).values(rows.slice(i, i + INSERT_BATCH_SIZE));
  }
  return ids;
}

export async function cleanupSeededJobs(
  db: NodePgDatabase<Record<string, never>>,
  ids: string[],
): Promise<void> {
  for (let i = 0; i < ids.length; i += INSERT_BATCH_SIZE) {
    const chunk = ids.slice(i, i + INSERT_BATCH_SIZE);
    if (chunk.length > 0) {
      await db.delete(jobs).where(inArray(jobs.id, chunk));
    }
  }
}
