import { MetricsRegistry } from "@chronoqueue/observability";

// In-process metrics for the scheduler service. The enqueue-failure
// counter is split by stage so an engineer can tell a DB claim problem
// from a Redis/BullMQ problem (PART 9) — two very different incidents.
export const metrics = new MetricsRegistry();

export interface SchedulerMetrics {
  registry: MetricsRegistry;
  schedulerEnqueueFailuresTotal: import("@chronoqueue/observability").Counter;
  jobsRetriedTotal: import("@chronoqueue/observability").Counter;
  jobsDeadTotal: import("@chronoqueue/observability").Counter;
}

export function createSchedulerMetrics(
  registry: MetricsRegistry,
): SchedulerMetrics {
  return {
    registry,
    schedulerEnqueueFailuresTotal: registry.counter(
      "scheduler_enqueue_failures_total",
      "Scheduler claim/enqueue failures split by stage (db_claim vs bullmq_enqueue)",
      ["stage"],
    ),
    jobsRetriedTotal: registry.counter(
      "jobs_retried_total",
      "Stale-lease recoveries whose PROCESSING -> RETRYING transition succeeded",
      ["source"],
    ),
    jobsDeadTotal: registry.counter(
      "jobs_dead_total",
      "Stale-lease recoveries whose PROCESSING -> DEAD transition succeeded",
      ["source"],
    ),
  };
}

export const schedulerMetrics = createSchedulerMetrics(metrics);

// Test support: forget recorded values between tests.
export function resetSchedulerMetrics(): void {
  metrics.reset();
}
