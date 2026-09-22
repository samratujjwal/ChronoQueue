import { MetricsRegistry } from "@chronoqueue/observability";

// In-process metrics for the API service. Exposed at GET /metrics in the
// standard Prometheus text format. The Scheduler and Worker each keep
// their own in-process registries (separate processes); this one covers
// the API process only.
export const metrics = new MetricsRegistry();

export interface ApiMetrics {
  registry: MetricsRegistry;
  jobsCreatedTotal: import("@chronoqueue/observability").Counter;
  jobCreationIdempotentHitsTotal: import("@chronoqueue/observability").Counter;
  jobsRetriggeredTotal: import("@chronoqueue/observability").Counter;
}

export function createApiMetrics(registry: MetricsRegistry): ApiMetrics {
  return {
    registry,
    jobsCreatedTotal: registry.counter(
      "jobs_created_total",
      "Jobs created via POST /jobs (201 responses)",
    ),
    jobCreationIdempotentHitsTotal: registry.counter(
      "job_creation_idempotent_hits_total",
      "POST /jobs requests deduplicated by idempotency key (200 responses)",
    ),
    jobsRetriggeredTotal: registry.counter(
      "jobs_retriggered_total",
      "DEAD jobs manually re-triggered via POST /jobs/:id/retry",
    ),
  };
}

export const apiMetrics = createApiMetrics(metrics);

// Test support: forget recorded values between tests.
export function resetApiMetrics(): void {
  metrics.reset();
}
