import { jobStatusEnum } from "../db/schema/jobs.js";

export type JobStatus = (typeof jobStatusEnum.enumValues)[number];

const transitions: Record<JobStatus, readonly JobStatus[]> = {
  PENDING: ["QUEUED"],
  QUEUED: ["PROCESSING"],
  PROCESSING: ["SUCCEEDED", "RETRYING"],
  RETRYING: ["QUEUED", "DEAD"],
  SUCCEEDED: [],
  DEAD: [],
};

export function isValidTransition(from: JobStatus, to: JobStatus): boolean {
  return transitions[from].includes(to);
}

export function getAllowedTransitions(from: JobStatus): readonly JobStatus[] {
  return transitions[from];
}
