import type { Queue } from "bullmq";

export interface WaitForOptions {
  timeoutMs?: number;
  intervalMs?: number;
  description?: string;
}

export async function bullJobExists(
  queue: Queue,
  jobId: string,
): Promise<boolean> {
  const job = await queue.getJob(jobId);
  return job !== undefined;
}

export async function removeBullJob(
  queue: Queue,
  jobId: string,
): Promise<void> {
  const job = await queue.getJob(jobId);
  await job?.remove();
}

export async function getQueueCounts(
  queue: Queue,
): Promise<Record<string, number>> {
  const counts = await queue.getJobCounts();
  return { ...counts };
}

export async function waitFor(
  condition: () => boolean | Promise<boolean>,
  options: WaitForOptions = {},
): Promise<void> {
  const {
    timeoutMs = 5000,
    intervalMs = 50,
    description = "condition",
  } = options;
  const deadline = Date.now() + timeoutMs;
  while (true) {
    if (await condition()) {
      return;
    }
    if (Date.now() >= deadline) {
      throw new Error(
        `Timed out after ${timeoutMs}ms waiting for: ${description}`,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs));
  }
}
