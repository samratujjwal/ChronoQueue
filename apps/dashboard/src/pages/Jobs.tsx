import { useEffect, useState } from "react";
import { ApiError, listJobs } from "../api/client.js";
import {
  ALL_STATUSES,
  type JobListPage,
  type JobStatus,
} from "../api/types.js";
import { StatusBadge } from "../components/StatusBadge.js";
import { Pagination } from "../components/Pagination.js";
import { LoadingState } from "../components/LoadingState.js";
import { EmptyState } from "../components/EmptyState.js";
import { ErrorState } from "../components/ErrorState.js";
import { formatDate, shortId } from "../format.js";

const PAGE_SIZE = 20;

export function Jobs() {
  const [status, setStatus] = useState<JobStatus | "ALL">("ALL");
  const [page, setPage] = useState(1);
  const [data, setData] = useState<JobListPage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const result = await listJobs({ page, pageSize: PAGE_SIZE, status });
        if (!cancelled) {
          setData(result);
        }
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof ApiError ? e.message : "Unexpected error");
        }
      } finally {
        if (!cancelled) {
          setLoading(false);
        }
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [status, page]);

  function changeStatus(next: JobStatus | "ALL") {
    setStatus(next);
    setPage(1);
  }

  return (
    <div>
      <h1>Jobs</h1>

      <div className="toolbar">
        <label htmlFor="status-filter">Status</label>
        <select
          id="status-filter"
          value={status}
          onChange={(e) => changeStatus(e.target.value as JobStatus | "ALL")}
        >
          <option value="ALL">All</option>
          {ALL_STATUSES.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
        {data ? (
          <span className="toolbar-meta">{data.total.toLocaleString()} jobs</span>
        ) : null}
      </div>

      {loading ? (
        <LoadingState label="Loading jobs…" />
      ) : error || !data ? (
        <ErrorState
          message={error ?? "Jobs unavailable"}
          onRetry={() => window.location.reload()}
        />
      ) : data.jobs.length === 0 ? (
        <EmptyState
          title="No jobs found"
          hint="Try a different status filter."
        />
      ) : (
        <div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Attempts</th>
                  <th>Scheduled</th>
                  <th>Updated</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {data.jobs.map((job) => (
                  <tr key={job.id}>
                    <td>
                      <code title={job.id}>{shortId(job.id)}</code>
                    </td>
                    <td>{job.type}</td>
                    <td>
                      <StatusBadge status={job.status} />
                    </td>
                    <td>
                      {job.attempts}/{job.maxAttempts}
                    </td>
                    <td>{formatDate(job.scheduledAt)}</td>
                    <td>{formatDate(job.updatedAt)}</td>
                    <td>
                      <a href={`#/jobs/${job.id}`}>View</a>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <Pagination page={data.page} totalPages={data.totalPages} onPage={setPage} />
        </div>
      )}
    </div>
  );
}
