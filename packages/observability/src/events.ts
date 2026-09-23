// Canonical lifecycle event names. Every important log line carries a
// stable `event` field so an engineer can reconstruct a job's journey
// (API -> Scheduler -> Queue -> Worker -> Webhook -> Retry/DLQ) with a
// single grep instead of guessing at message text.

export const API_EVENTS = {
  jobCreated: "job_created",
  jobCreationIdempotentHit: "job_creation_idempotent_hit",
  jobRetrieved: "job_retrieved",
  // deadJobsListed: "dead_jobs_listed",
  deadJobsListed: "dead_jobs_listed",
  jobsListed: "jobs_listed",
  jobStatsRetrieved: "job_stats_retrieved",

  jobRetriggerRequested: "job_retrigger_requested",
  jobRetriggered: "job_retriggered",
  jobRetriggerConflict: "job_retrigger_conflict",
  jobRetriggerFailed: "job_retrigger_failed",
  readinessCheckFailed: "readiness_check_failed",
} as const;

export const SCHEDULER_EVENTS = {
  schedulerStarted: "scheduler_started",
  schedulerShutdown: "scheduler_shutdown",
  schedulerPollCompleted: "scheduler_poll_completed",
  jobClaimedForEnqueue: "job_claimed_for_enqueue",
  jobEnqueueStarted: "job_enqueue_started",
  jobEnqueueSucceeded: "job_enqueue_succeeded",
  jobEnqueueFailed: "job_enqueue_failed",
  expiredLeaseRecovered: "expired_lease_recovered",
} as const;

export const WORKER_EVENTS = {
  workerStarted: "worker_started",
  workerShutdown: "worker_shutdown",
  jobClaimed: "job_claimed",
  jobProcessingStarted: "job_processing_started",
  webhookStarted: "webhook_started",
  webhookSucceeded: "webhook_succeeded",
  webhookFailed: "webhook_failed",
  jobRetryScheduled: "job_retry_scheduled",
  jobMarkedDead: "job_marked_dead",
  leaseLost: "lease_lost",
} as const;

export type ApiEvent = (typeof API_EVENTS)[keyof typeof API_EVENTS];
export type SchedulerEvent =
  (typeof SCHEDULER_EVENTS)[keyof typeof SCHEDULER_EVENTS];
export type WorkerEvent = (typeof WORKER_EVENTS)[keyof typeof WORKER_EVENTS];
