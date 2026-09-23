import type {
  DeadJobPage,
  JobDetail,
  JobListPage,
  JobStats,
  JobStatus,
  RetryResult,
} from "./types.js";

export class ApiError extends Error {
  status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let res: Response;
  try {
    res = await fetch(path, init);
  } catch {
    throw new ApiError(0, "Network error: could not reach the API");
  }

  if (!res.ok) {
    let message = `Request failed with status ${res.status}`;
    try {
      const body = (await res.json()) as {
        error?: { message?: string };
      };
      if (body.error?.message) {
        message = body.error.message;
      }
    } catch {}
    throw new ApiError(res.status, message);
  }

  return (await res.json()) as T;
}

function withQuery(
  path: string,
  params: Record<string, string | undefined>,
): string {
  const q = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined) {
      q.set(key, value);
    }
  }
  const qs = q.toString();
  return qs ? `${path}?${qs}` : path;
}

export function listJobs(options: {
  page?: number;
  pageSize?: number;
  status?: JobStatus | "ALL";
}): Promise<JobListPage> {
  return request<JobListPage>(
    withQuery("/jobs", {
      page: options.page === undefined ? undefined : String(options.page),
      pageSize:
        options.pageSize === undefined ? undefined : String(options.pageSize),
      status:
        options.status === undefined || options.status === "ALL"
          ? undefined
          : options.status,
    }),
  );
}

export function getJobStats(): Promise<JobStats> {
  return request<JobStats>("/jobs/stats");
}

export function getJob(id: string): Promise<JobDetail> {
  return request<JobDetail>(`/jobs/${encodeURIComponent(id)}`);
}

export function listDeadJobs(options: {
  page?: number;
  pageSize?: number;
}): Promise<DeadJobPage> {
  return request<DeadJobPage>(
    withQuery("/jobs/dead", {
      page: options.page === undefined ? undefined : String(options.page),
      pageSize:
        options.pageSize === undefined ? undefined : String(options.pageSize),
    }),
  );
}

export function retryJob(id: string): Promise<RetryResult> {
  return request<RetryResult>(`/jobs/${encodeURIComponent(id)}/retry`, {
    method: "POST",
  });
}
