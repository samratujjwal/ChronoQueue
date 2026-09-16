import { logger } from "./logger.js";
import { worker } from "./worker.js";
import { connection } from "./queue/connection.js";
import { pool } from "./db/client.js";

worker.on("ready", () => {
  logger.info("worker ready, listening on webhook-delivery");
});

worker.on("completed", (job) => {
  logger.info({ bullJobId: job.id }, "job completed");
});

worker.on("failed", (job, err) => {
  logger.error({ bullJobId: job?.id, err: err.message }, "job failed");
});

let shuttingDown = false;

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  logger.info({ signal }, "shutting down worker");

  await worker.close();
  await connection.quit();
  await pool.end();

  logger.info("worker shutdown complete");
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
