import { useEffect, useState } from "react";
import { ApiError, listDeadJobs, retryJob } from "../api/client.js";
import type { DeadJobPage, DeadJobRow } from "../api/types.js";
import { StatusBadge } from "../components/StatusBadge.js";
import { Pagination } from "../components/Pagination.js";
import { LoadingState } from "../components/LoadingState.js";
import { EmptyState } from "../components/EmptyState.js";
import { ErrorState } from "../components/ErrorState.js";
import { formatDate, shortId } from "../format.js";

const PAGE_SIZE = 20;

interface Notice {
  kind: "success" | "error";
  text: string;
}

export function DeadQueue() {
  const [data, setData] = useState<DeadJobPage | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [retryingId, setRetryingId] = useState<string | null>(null);
  const [notice, setNotice] = useState<Notice | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const result = await listDeadJobs({ page, pageSize: PAGE_SIZE });
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
  }, [page]);

  function retryMessage(status: number, message: string): string {
    if (status === 404) {
      return `Job no longer exists: ${message}`;
    }
    if (status === 409) {
      return `Job is no longer dead (already retried or picked up): ${message}`;
    }
    if (status === 500) {
      return `Server error while retrying: ${message}`;
    }
    if (status === 0) {
      return message;
    }
    return `Retry failed: ${message}`;
  }

  async function onRetry(row: DeadJobRow) {
    if (retryingId) {
      return;
    }
    setRetryingId(row.id);
    setNotice(null);
    try {
      await retryJob(row.id);
      setData((prev) =>
        prev
          ? {
              ...prev,
              jobs: prev.jobs.filter((j) => j.id !== row.id),
              total: Math.max(0, prev.total - 1),
            }
          : prev
      );
      setNotice({
        kind: "success",
        text: `Job ${shortId(row.id)} re-queued successfully.`,
      });
    } catch (e) {
      const apiError = e instanceof ApiError ? e : null;
      setNotice({
        kind: "error",
        text: retryMessage(
          apiError?.status ?? -1,
          apiError?.message ?? "Unexpected error"
        ),
      });
    } finally {
      setRetryingId(null);
    }
  }

  return (
    <div>
      <h1>Dead Letter Queue</h1>
      <p className="muted">
        Jobs that exhausted all attempts. Retrying moves a job back to QUEUED;
        the backend owns the transition.
      </p>

      {notice ? (
        <div className={`notice notice-${notice.kind}`} role="status">
          {notice.text}
        </div>
      ) : null}

      {loading ? (
        <LoadingState label="Loading dead jobs…" />
      ) : error || !data ? (
        <ErrorState
          message={error ?? "Dead jobs unavailable"}
          onRetry={() => window.location.reload()}
        />
      ) : data.jobs.length === 0 ? (
        <EmptyState
          title="Dead queue is empty"
          hint="No jobs have exhausted their attempts."
        />
      ) : (
        <div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Status</th>
                  <th>Error code</th>
                  <th>Error message</th>
                  <th>Attempts</th>
                  <th>Updated</th>
                  <th>Action</th>
                </tr>
              </thead>
              <tbody>
                {data.jobs.map((job) => (
                  <tr key={job.id}>
                    <td>
                      <a href={`#/jobs/${job.id}`} title={job.id}>
                        <code>{shortId(job.id)}</code>
                      </a>
                    </td>
                    <td>
                      <StatusBadge status={job.status} />
                    </td>
                    <td>
                      <code>{job.lastErrorCode ?? "—"}</code>
                    </td>
                    <td className="error-message">
                      {job.lastErrorMessage ?? "—"}
                    </td>
                    <td>
                      {job.attempts}/{job.maxAttempts}
                    </td>
                    <td>{formatDate(job.updatedAt)}</td>
                    <td>
                      <button
                        type="button"
                        className="btn btn-small"
                        disabled={retryingId !== null}
                        onClick={() => onRetry(job)}
                      >
                        {retryingId === job.id ? "Retrying…" : "Retry"}
                      </button>
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
