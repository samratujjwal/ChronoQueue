import { randomUUID } from "node:crypto";
import { and, eq, inArray } from "drizzle-orm";
import { jobs, type JobStatus } from "@chronoqueue/db";
import { db } from "./db/client.js";
import { config } from "./config/env.js";

const CLAIMABLE_STATUSES: JobStatus[] = ["PENDING", "QUEUED", "RETRYING"];

export interface ClaimResult {
  job: typeof jobs.$inferSelect;
  leaseToken: string;
}

// Generates a brand-new fencing token on every successful claim — never
// reused, even when a job is claimed again after crash recovery. The
// token, not `status = PROCESSING` alone, is what identifies which
// execution attempt currently owns the row.
export async function claimJob(jobId: string): Promise<ClaimResult | null> {
  const leaseToken = randomUUID();

  const [claimed] = await db
    .update(jobs)
    .set({
      status: "PROCESSING",
      leaseUntil: new Date(Date.now() + config.WORKER_LEASE_DURATION_MS),
      leaseToken,
      updatedAt: new Date(),
    })
    .where(and(eq(jobs.id, jobId), inArray(jobs.status, CLAIMABLE_STATUSES)))
    .returning();

  if (!claimed) {
    return null;
  }

  return { job: claimed, leaseToken };
}

// Fenced renewal: only succeeds while status is still PROCESSING AND the
// caller's token still matches the row's current token. If another
// execution attempt has since claimed the job (new token) or the job has
// left PROCESSING, this returns false — the caller no longer owns it.
export async function renewLease(
  jobId: string,
  leaseToken: string,
): Promise<boolean> {
  const [renewed] = await db
    .update(jobs)
    .set({ leaseUntil: new Date(Date.now() + config.WORKER_LEASE_DURATION_MS) })
    .where(
      and(
        eq(jobs.id, jobId),
        eq(jobs.status, "PROCESSING"),
        eq(jobs.leaseToken, leaseToken),
      ),
    )
    .returning({ id: jobs.id });

  return !!renewed;
}

// Fenced terminal transition: same token check as renewal. Used for
// SUCCEEDED, RETRYING, and DEAD outcomes alike — an old/stale worker's
// token can never complete a newer processing attempt. Always clears the
// lease fields on success.
//
// Day 14: also persists the latest failure diagnosis (lastErrorCode /
// lastErrorMessage) on RETRYING/DEAD, and clears it on SUCCEEDED. The
// fencing WHERE clause is unchanged.
export async function completeProcessing(
  jobId: string,
  leaseToken: string,
  update: {
    status: JobStatus;
    attempts: number;
    nextAttemptAt: Date | null;
    lastErrorCode?: string | null;
    lastErrorMessage?: string | null;
  },
): Promise<boolean> {
  const [updated] = await db
    .update(jobs)
    .set({
      status: update.status,
      attempts: update.attempts,
      nextAttemptAt: update.nextAttemptAt,
      lastErrorCode: update.lastErrorCode ?? null,
      lastErrorMessage: update.lastErrorMessage ?? null,
      leaseUntil: null,
      leaseToken: null,
      updatedAt: new Date(),
    })
    .where(
      and(
        eq(jobs.id, jobId),
        eq(jobs.status, "PROCESSING"),
        eq(jobs.leaseToken, leaseToken),
      ),
    )
    .returning({ id: jobs.id });

  return !!updated;
}
