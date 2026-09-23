import { count, desc, eq } from "drizzle-orm";
import type { NodePgDatabase } from "drizzle-orm/node-postgres";
import { jobs } from "./schema/jobs.js";
import type { JobStatus } from "./state-machine.js";

type Db = Pick<NodePgDatabase, "select">;

export const JOBS_PAGE_SIZE_DEFAULT = 20;
export const JOBS_PAGE_SIZE_MAX = 100;

export const DASHBOARD_STATUSES: readonly JobStatus[] = [
  "PENDING",
  "QUEUED",
  "PROCESSING",
  "RETRYING",
  "SUCCEEDED",
  "DEAD",
];

const JOB_LIST_COLUMNS = {
  id: jobs.id,
  type: jobs.type,
  status: jobs.status,
  attempts: jobs.attempts,
  maxAttempts: jobs.maxAttempts,
  scheduledAt: jobs.scheduledAt,
  nextAttemptAt: jobs.nextAttemptAt,
  lastErrorCode: jobs.lastErrorCode,
  createdAt: jobs.createdAt,
  updatedAt: jobs.updatedAt,
};

export interface JobListRow {
  id: string;
  type: (typeof jobs.$inferSelect)["type"];
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  scheduledAt: Date;
  nextAttemptAt: Date | null;
  lastErrorCode: string | null;
  createdAt: Date;
  updatedAt: Date;
}

export interface JobListPage {
  jobs: JobListRow[];
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
}

export interface ListJobsOptions {
  page?: number;
  pageSize?: number;
  status?: JobStatus;
}

function normalizePage(value: number | undefined): number {
  if (value === undefined) return 1;
  if (!Number.isInteger(value) || value < 1) {
    throw new Error(`page must be a positive integer, got ${value}`);
  }
  return value;
}

function normalizePageSize(value: number | undefined): number {
  if (value === undefined) return JOBS_PAGE_SIZE_DEFAULT;
  if (!Number.isInteger(value) || value < 1 || value > JOBS_PAGE_SIZE_MAX) {
    throw new Error(
      `pageSize must be an integer between 1 and ${JOBS_PAGE_SIZE_MAX}, got ${value}`,
    );
  }
  return value;
}

export async function listJobs(
  db: Db,
  options: ListJobsOptions = {},
): Promise<JobListPage> {
  const page = normalizePage(options.page);
  const pageSize = normalizePageSize(options.pageSize);
  const where = options.status ? eq(jobs.status, options.status) : undefined;

  const [countRow] = await db
    .select({ total: count() })
    .from(jobs)
    .where(where);
  const total = countRow?.total ?? 0;
  const totalPages = total === 0 ? 0 : Math.ceil(total / pageSize);

  const rows = await db
    .select(JOB_LIST_COLUMNS)
    .from(jobs)
    .where(where)
    .orderBy(desc(jobs.updatedAt), desc(jobs.id))
    .limit(pageSize)
    .offset((page - 1) * pageSize);

  return {
    jobs: rows.map((row) => ({
      id: row.id,
      type: row.type,
      status: row.status,
      attempts: row.attempts,
      maxAttempts: row.maxAttempts,
      scheduledAt: row.scheduledAt,
      nextAttemptAt: row.nextAttemptAt,
      lastErrorCode: row.lastErrorCode,
      createdAt: row.createdAt,
      updatedAt: row.updatedAt,
    })),
    page,
    pageSize,
    total,
    totalPages,
  };
}

export interface JobStats {
  total: number;
  byStatus: Record<JobStatus, number>;
}

export async function getJobStats(db: Db): Promise<JobStats> {
  const rows = await db
    .select({ status: jobs.status, n: count() })
    .from(jobs)
    .groupBy(jobs.status);

  const byStatus = Object.fromEntries(
    DASHBOARD_STATUSES.map((status) => [status, 0]),
  ) as Record<JobStatus, number>;

  let total = 0;
  for (const row of rows) {
    const n = Number(row.n);
    byStatus[row.status] = n;
    total += n;
  }

  return { total, byStatus };
}
