import { useEffect, useState } from "react";
import { ApiError, getJobStats, listJobs } from "../api/client.js";
import {
  ALL_STATUSES,
  type JobListRow,
  type JobStats,
} from "../api/types.js";
import { StatCard } from "../components/StatCard.js";
import { StatusBadge } from "../components/StatusBadge.js";
import { LoadingState } from "../components/LoadingState.js";
import { EmptyState } from "../components/EmptyState.js";
import { ErrorState } from "../components/ErrorState.js";
import { formatDate, shortId } from "../format.js";

export function Overview() {
  const [stats, setStats] = useState<JobStats | null>(null);
  const [recent, setRecent] = useState<JobListRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const [s, page] = await Promise.all([
          getJobStats(),
          listJobs({ page: 1, pageSize: 10 }),
        ]);
        if (!cancelled) {
          setStats(s);
          setRecent(page.jobs);
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
  }, []);

  if (loading) {
    return <LoadingState label="Loading overview…" />;
  }

  if (error || !stats) {
    return (
      <ErrorState
        message={error ?? "Statistics unavailable"}
        onRetry={() => window.location.reload()}
      />
    );
  }

  return (
    <div>
      <h1>Overview</h1>

      <div className="stat-grid">
        <StatCard label="Total jobs" value={stats.total} />
        {ALL_STATUSES.map((status) => (
          <StatCard
            key={status}
            label={status}
            value={stats.byStatus[status] ?? 0}
          />
        ))}
      </div>

      <section>
        <h2>Status distribution</h2>
        {stats.total === 0 ? (
          <EmptyState title="No jobs yet" hint="Create a job via POST /jobs to see it here." />
        ) : (
          <div>
            <div
              className="dist-bar"
              role="img"
              aria-label="Job status distribution"
            >
              {ALL_STATUSES.map((status) => {
                const count = stats.byStatus[status] ?? 0;
                const pct = (count / stats.total) * 100;
                if (pct <= 0) {
                  return null;
                }
                return (
                  <div
                    key={status}
                    className={`dist-seg dist-${status.toLowerCase()}`}
                    style={{ width: `${pct}%` }}
                    title={`${status}: ${count}`}
                  />
                );
              })}
            </div>
            <ul className="dist-legend">
              {ALL_STATUSES.map((status) => (
                <li key={status}>
                  <StatusBadge status={status} />
                  <span>{(stats.byStatus[status] ?? 0).toLocaleString()}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </section>

      <section>
        <h2>Recent jobs</h2>
        {recent.length === 0 ? (
          <EmptyState title="No jobs yet" />
        ) : (
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Status</th>
                  <th>Attempts</th>
                  <th>Updated</th>
                </tr>
              </thead>
              <tbody>
                {recent.map((job) => (
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
                      {job.attempts}/{job.maxAttempts}
                    </td>
                    <td>{formatDate(job.updatedAt)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        <p>
          <a href="#/jobs">View all jobs →</a>
        </p>
      </section>
    </div>
  );
}
