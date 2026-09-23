export type JobStatus =
  | "PENDING"
  | "QUEUED"
  | "PROCESSING"
  | "RETRYING"
  | "SUCCEEDED"
  | "DEAD";

export type JobType = "WEBHOOK";

export const ALL_STATUSES: readonly JobStatus[] = [
  "PENDING",
  "QUEUED",
  "PROCESSING",
  "RETRYING",
  "SUCCEEDED",
  "DEAD",
];

export interface JobListRow {
  id: string;
  type: JobType;
  status: JobStatus;
  attempts: number;
  maxAttempts: number;
  scheduledAt: string;
  nextAttemptAt: string | null;
  lastErrorCode: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface JobListPage {
  jobs: JobListRow[];
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
}

export interface JobStats {
  total: number;
  byStatus: Record<JobStatus, number>;
}

export interface DeadJobRow {
  id: string;
  type: JobType;
  status: JobStatus;
  targetUrl: string;
  payload: unknown;
  attempts: number;
  maxAttempts: number;
  scheduledAt: string;
  nextAttemptAt: string | null;
  lastErrorCode: string | null;
  lastErrorMessage: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface DeadJobPage {
  jobs: DeadJobRow[];
  page: number;
  pageSize: number;
  total: number;
  totalPages: number;
}

export interface JobDetail {
  id: string;
  type: JobType;
  status: JobStatus;
  targetUrl: string;
  payload: unknown;
  attempts: number;
  maxAttempts: number;
  scheduledAt: string;
  nextAttemptAt: string | null;
  lastErrorCode: string | null;
  lastErrorMessage: string | null;
  createdAt: string;
  updatedAt: string;
}

export interface RetryResult {
  id: string;
  status: JobStatus;
  attempts: number;
}
