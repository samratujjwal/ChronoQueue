import { MetricsRegistry } from "@chronoqueue/observability";

// In-process metrics for the worker service. Recorded exactly when the
// underlying business transition succeeds (see processor.ts) — never
// speculatively. Exposed for tests via createWorkerMetrics(); a future
// day can serve this registry over HTTP if needed.
export const metrics = new MetricsRegistry();

export interface WorkerMetrics {
  registry: MetricsRegistry;
  jobsSucceededTotal: import("@chronoqueue/observability").Counter;
  jobsRetriedTotal: import("@chronoqueue/observability").Counter;
  jobsDeadTotal: import("@chronoqueue/observability").Counter;
  webhookRequestsTotal: import("@chronoqueue/observability").Counter;
  webhookFailuresTotal: import("@chronoqueue/observability").Counter;
  workerClaimConflictsTotal: import("@chronoqueue/observability").Counter;
  jobProcessingDurationSeconds: import("@chronoqueue/observability").Histogram;
  webhookDurationSeconds: import("@chronoqueue/observability").Histogram;
}

export function createWorkerMetrics(registry: MetricsRegistry): WorkerMetrics {
  return {
    registry,
    jobsSucceededTotal: registry.counter(
      "jobs_succeeded_total",
      "Jobs whose PROCESSING -> SUCCEEDED transition succeeded",
    ),
    jobsRetriedTotal: registry.counter(
      "jobs_retried_total",
      "Jobs whose PROCESSING -> RETRYING transition succeeded",
      ["source"],
    ),
    jobsDeadTotal: registry.counter(
      "jobs_dead_total",
      "Jobs whose PROCESSING -> DEAD transition succeeded",
      ["source"],
    ),
    webhookRequestsTotal: registry.counter(
      "webhook_requests_total",
      "Actual webhook HTTP execution attempts",
    ),
    webhookFailuresTotal: registry.counter(
      "webhook_failures_total",
      "Failed webhook HTTP execution attempts",
      ["error_code"],
    ),
    workerClaimConflictsTotal: registry.counter(
      "worker_claim_conflicts_total",
      "Worker processing attempts that could not claim the PG row (already owned/finished)",
    ),
    jobProcessingDurationSeconds: registry.histogram(
      "job_processing_duration_seconds",
      "Time from successful claim to terminal PG write (excludes queue wait)",
      undefined,
      ["outcome"],
    ),
    webhookDurationSeconds: registry.histogram(
      "webhook_duration_seconds",
      "Webhook HTTP execution duration only",
      undefined,
      ["outcome"],
    ),
  };
}

export const workerMetrics = createWorkerMetrics(metrics);

// Test support: forget recorded values between tests.
export function resetWorkerMetrics(): void {
  metrics.reset();
}
