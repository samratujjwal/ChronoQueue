import { useEffect, useState } from "react";
import { ApiError, getJob, retryJob } from "../api/client.js";
import type { JobDetail as JobDetailData, JobStatus } from "../api/types.js";
import { StatusBadge } from "../components/StatusBadge.js";
import { LoadingState } from "../components/LoadingState.js";
import { EmptyState } from "../components/EmptyState.js";
import { ErrorState } from "../components/ErrorState.js";
import { formatDate } from "../format.js";

const STATE_FLOW: readonly JobStatus[] = [
  "PENDING",
  "QUEUED",
  "PROCESSING",
  "RETRYING",
  "SUCCEEDED",
];

function Field({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="field">
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

export function JobDetail({ id }: { id: string }) {
  const [job, setJob] = useState<JobDetailData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notFound, setNotFound] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      setNotFound(false);
      try {
        const result = await getJob(id);
        if (!cancelled) {
          setJob(result);
        }
      } catch (e) {
        if (!cancelled) {
          if (e instanceof ApiError && e.status === 404) {
            setNotFound(true);
          } else {
            setError(e instanceof ApiError ? e.message : "Unexpected error");
          }
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
  }, [id]);

  async function onRetry() {
    if (!job || retrying) {
      return;
    }
    setRetrying(true);
    setNotice(null);
    try {
      const result = await retryJob(job.id);
      setJob({ ...job, status: result.status, attempts: result.attempts });
      setNotice(`Job re-queued successfully (attempts: ${result.attempts}).`);
    } catch (e) {
      const message = e instanceof ApiError ? e.message : "Unexpected error";
      if (e instanceof ApiError && e.status === 409) {
        setNotice(`Could not retry: ${message}`);
      } else {
        setNotice(`Retry failed: ${message}`);
      }
    } finally {
      setRetrying(false);
    }
  }

  if (loading) {
    return <LoadingState label="Loading job…" />;
  }

  if (notFound) {
    return <EmptyState title="Job not found" hint={`No job exists with id ${id}.`} />;
  }

  if (error || !job) {
    return (
      <ErrorState
        message={error ?? "Job unavailable"}
        onRetry={() => window.location.reload()}
      />
    );
  }

  return (
    <div>
      <p>
        <a href="#/jobs">← Back to jobs</a>
      </p>
      <h1>
        Job <code className="id-full">{job.id}</code>
      </h1>

      {notice ? (
        <div className="notice" role="status">
          {notice}
        </div>
      ) : null}

      <dl className="detail-grid">
        <Field label="Status" value={<StatusBadge status={job.status} />} />
        <Field label="Type" value={job.type} />
        <Field label="Attempts" value={`${job.attempts} / ${job.maxAttempts}`} />
        <Field label="Target URL" value={<code className="wrap">{job.targetUrl}</code>} />
        <Field label="Scheduled at" value={formatDate(job.scheduledAt)} />
        <Field label="Next attempt at" value={formatDate(job.nextAttemptAt)} />
        <Field label="Created at" value={formatDate(job.createdAt)} />
        <Field label="Updated at" value={formatDate(job.updatedAt)} />
        <Field label="Last error code" value={job.lastErrorCode ?? "—"} />
        <Field label="Last error message" value={job.lastErrorMessage ?? "—"} />
      </dl>

      <details className="payload">
        <summary>Payload (JSON)</summary>
        <pre>{JSON.stringify(job.payload, null, 2)}</pre>
      </details>

      <section>
        <h2>State machine</h2>
        <p className="muted">
          State-machine representation, not an event history. ChronoQueue does
          not record per-transition events; the diagram below shows the allowed
          transitions with this job&apos;s current state highlighted.
          RETRYING loops back to QUEUED, and any state can end in DEAD.
        </p>
        <div className="state-flow" aria-label="Job state machine">
          {STATE_FLOW.map((state, i) => (
            <span key={state} className="flow-step">
              {i > 0 ? <span className="flow-arrow" aria-hidden="true">→</span> : null}
              <span
                className={state === job.status ? "flow-current" : "flow-state"}
              >
                {state}
              </span>
            </span>
          ))}
          <span className="flow-step">
            <span className="flow-arrow" aria-hidden="true">→</span>
            <span className={job.status === "DEAD" ? "flow-current flow-dead" : "flow-state flow-dead"}>
              DEAD
            </span>
          </span>
        </div>
      </section>

      {job.status === "DEAD" ? (
        <section>
          <button
            type="button"
            className="btn btn-primary"
            disabled={retrying}
            onClick={onRetry}
          >
            {retrying ? "Retrying…" : "Retry job"}
          </button>
        </section>
      ) : null}
    </div>
  );
}
