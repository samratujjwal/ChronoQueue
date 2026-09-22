import { WORKER_EVENTS, safeError } from "@chronoqueue/observability";
import { logger } from "./logger.js";
import { worker } from "./worker.js";
import { connection } from "./queue/connection.js";
import { pool } from "./db/client.js";

worker.on("ready", () => {
  logger.info(
    { event: WORKER_EVENTS.workerStarted },
    "worker ready, listening on webhook-delivery",
  );
});

worker.on("completed", (job) => {
  logger.info(
    { event: WORKER_EVENTS.webhookSucceeded, bullJobId: job.id },
    "bullmq job completed",
  );
});

worker.on("failed", (job, err) => {
  logger.error(
    {
      event: WORKER_EVENTS.webhookFailed,
      bullJobId: job?.id,
      ...safeError(err),
    },
    "bullmq job failed",
  );
});

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  logger.info(
    { event: WORKER_EVENTS.workerShutdown, signal },
    "shutting down worker",
  );

  await worker.close();
  await connection.quit();
  await pool.end();

  logger.info(
    { event: WORKER_EVENTS.workerShutdown },
    "worker shutdown complete",
  );
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
