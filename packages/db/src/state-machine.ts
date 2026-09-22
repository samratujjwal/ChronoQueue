import { jobStatusEnum } from "./schema/jobs.js";

export type JobStatus = (typeof jobStatusEnum.enumValues)[number];

const transitions: Record<JobStatus, readonly JobStatus[]> = {
  PENDING: ["QUEUED"],
  QUEUED: ["PROCESSING"],
  PROCESSING: ["SUCCEEDED", "RETRYING", "DEAD"],
  RETRYING: ["QUEUED", "DEAD"],
  SUCCEEDED: [],
  // DEAD is re-enterable exactly once per manual re-trigger (Day 14 DLQ):
  // a guarded DB update moves DEAD -> QUEUED, after which the normal
  // Worker claim path takes over with a fresh leaseToken.
  DEAD: ["QUEUED"],
};

export function isValidTransition(from: JobStatus, to: JobStatus): boolean {
  return transitions[from].includes(to);
}

export function getAllowedTransitions(from: JobStatus): readonly JobStatus[] {
  return transitions[from];
}
