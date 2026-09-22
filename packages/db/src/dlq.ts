import { and, count, desc, eq } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { jobs } from "./schema/jobs.js";
import { isValidTransition, type JobStatus } from "./state-machine.js";
// The apps build their own drizzle instances (`drizzle(pool)`); the
// repository functions take the instance as a parameter so both apps/api
// (HTTP layer) and apps/worker (tests) can share this logic without
// cross-app imports. Type-only import of the node-postgres driver keeps
// this package free of a runtime `pg` dependency.
type Db = NodePgDatabase;
export const DLQ_PAGE_SIZE_DEFAULT = 20;
export const DLQ_PAGE_SIZE_MAX = 100;
export interface DeadJobRow {
  id: string;
  type: (typeof jobs.$inferSelect)["type"];
  status: JobStatus;
  targetUrl: string;
  payload: unknown;
  attempts: number;
  maxAttempts: number;
  scheduledAt: Date;
  nextAttemptAt: Date | null;
  lastErrorCode: string | null;
  lastErrorMessage: string | null;
  createdAt: Date;
  updatedAt: Date;
}
const DLQ_COLUMNS = {
  id: jobs.id,
  type: jobs.type,
  status: jobs.status,
  targetUrl: jobs.targetUrl,
  payload: jobs.payload,
  attempts: jobs.attempts,
  maxAttempts: jobs.maxAttempts,
  scheduledAt: jobs.scheduledAt,
  nextAttemptAt: jobs.nextAttemptAt,
  lastErrorCode: jobs.lastErrorCode,
  lastErrorMessage: jobs.lastErrorMessage,
  createdAt: jobs.createdAt,
  updatedAt: jobs.updatedAt,
};
export interface DeadJobPage {
  jobs: DeadJobRow[];
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
}
// Durable DLQ listing (Day 14): only DEAD rows, newest-dead first.
// Ordering is (updated_at DESC, id DESC) — updated_at is bumped on every
// terminal write, so it reflects when the job died; id breaks ties
// deterministically. Pagination is mandatory: no unbounded SELECT.
export async function listDeadJobs(
  db: Db,
  options: { page?: number; pageSize?: number } = {},
): Promise<DeadJobPage> {
  const page = options.page ?? 1;
  const pageSize = options.pageSize ?? DLQ_PAGE_SIZE_DEFAULT;
  if (!Number.isInteger(page) || page < 1) {
    throw new Error(`Invalid DLQ page: ${options.page}`);
  }
  if (
    !Number.isInteger(pageSize) ||
    pageSize < 1 ||
    pageSize > DLQ_PAGE_SIZE_MAX
  ) {
    throw new Error(`Invalid DLQ page size: ${options.pageSize}`);
  }
  const [countRow] = await db
    .select({ total: count() })
    .from(jobs)
    .where(eq(jobs.status, "DEAD"));
  const total = countRow?.total ?? 0;
  const rows = await db
    .select(DLQ_COLUMNS)
    .from(jobs)
    .where(eq(jobs.status, "DEAD"))
    .orderBy(desc(jobs.updatedAt), desc(jobs.id))
    .limit(pageSize)
    .offset((page - 1) * pageSize);
  return {
    jobs: rows,
    page,
    pageSize,
    total,
    totalPages: Math.ceil(total / pageSize),
  };
}
// Fetch a single job by ID (used by GET /jobs/:id). Returns undefined
// when no such row exists; any status is returned — the caller decides
// what is eligible.
export async function getJobById(
  db: Db,
  id: string,
): Promise<DeadJobRow | undefined> {
  const [row] = await db
    .select(DLQ_COLUMNS)
    .from(jobs)
    .where(eq(jobs.id, id))
    .limit(1);
  return row;
}
export type RetriggerResult =
  | { ok: true; job: Pick<DeadJobRow, "id" | "status" | "attempts"> }
  | { ok: false; reason: "not_found" | "not_dead" };
// Manual re-trigger (Day 14): DEAD -> QUEUED via a SINGLE guarded UPDATE.
//
// Concurrency: the WHERE clause (id AND status = 'DEAD') is evaluated
// atomically by PostgreSQL. Two simultaneous requests race on the row
// lock; the loser re-evaluates the predicate against the winner's
// committed version, matches zero rows, and gets { ok: false } — exactly
// one request can ever transition a given DEAD row.
//
// Deliberately NOT done here:
// - no leaseToken is created or reused (the Worker mints a fresh one on
//   claim — see claimJob in apps/worker/src/lease.ts);
// - attempts is preserved, not reset (a re-triggered job gets one more
//   execution; if it fails again it returns to DEAD with honest history);
// - nextAttemptAt stays NULL (QUEUED means "ready now"; the scheduler only
//   uses nextAttemptAt for RETRYING rows).
export async function retriggerDeadJob(
  db: Db,
  id: string,
): Promise<RetriggerResult> {
  if (!isValidTransition("DEAD", "QUEUED")) {
    throw new Error("Illegal state transition DEAD -> QUEUED");
  }
  const [retriggered] = await db
    .update(jobs)
    .set({
      status: "QUEUED",
      nextAttemptAt: null,
      updatedAt: new Date(),
    })
    .where(and(eq(jobs.id, id), eq(jobs.status, "DEAD")))
    .returning({ id: jobs.id, status: jobs.status, attempts: jobs.attempts });
  if (retriggered) {
    return { ok: true, job: retriggered };
  }
  // Zero rows affected: either the job never existed, or it exists but is
  // not DEAD (possibly just re-triggered by a concurrent request). This
  // follow-up SELECT is only for accurate error reporting — the guarded
  // UPDATE above is the correctness gate, not this read.
  const [existing] = await db
    .select({ id: jobs.id })
    .from(jobs)
    .where(eq(jobs.id, id))
    .limit(1);
  return existing
    ? { ok: false, reason: "not_dead" }
    : { ok: false, reason: "not_found" };
}
