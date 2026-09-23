import type { JobStatus } from "../api/types.js";

const STATUS_CLASS: Record<JobStatus, string> = {
  PENDING: "badge badge-pending",
  QUEUED: "badge badge-queued",
  PROCESSING: "badge badge-processing",
  RETRYING: "badge badge-retrying",
  SUCCEEDED: "badge badge-succeeded",
  DEAD: "badge badge-dead",
};

export function StatusBadge({ status }: { status: JobStatus }) {
  return <span className={STATUS_CLASS[status]}>{status}</span>;
}
