import { logger } from "./logger.js";
import { config } from "./config/env.js";
import { runSchedulerPoll } from "./scheduler.js";
import { recoverStaleJobs } from "./recovery.js";
import { connection } from "./queue/connection.js";
import { webhookQueue } from "./queue/webhook-queue.js";
import { pool } from "./db/client.js";

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

let shuttingDown = false;
let currentPollPromise: Promise<void> | null = null;

async function runCycle(): Promise<void> {
  try {
    await recoverStaleJobs();
  } catch (error) {
    logger.error(
      { err: error instanceof Error ? error.message : String(error) },
      "stale job recovery failed",
    );
  }

  try {
    await runSchedulerPoll();
  } catch (error) {
    logger.error(
      { err: error instanceof Error ? error.message : String(error) },
      "scheduler poll failed",
    );
  }
}

async function loop(): Promise<void> {
  while (!shuttingDown) {
    currentPollPromise = runCycle();

    await currentPollPromise;
    currentPollPromise = null;

    if (shuttingDown) {
      break;
    }

    await sleep(config.SCHEDULER_POLL_INTERVAL_MS);
  }
}

logger.info(
  {
    pollIntervalMs: config.SCHEDULER_POLL_INTERVAL_MS,
    batchSize: config.SCHEDULER_BATCH_SIZE,
  },
  "scheduler started",
);

const loopPromise = loop();

async function shutdown(signal: string): Promise<void> {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;

  logger.info({ signal }, "shutting down scheduler");

  if (currentPollPromise) {
    await currentPollPromise;
  }
  await loopPromise;

  await webhookQueue.close();
  await connection.quit();
  await pool.end();

  logger.info("scheduler shutdown complete");
  process.exit(0);
}

process.on("SIGTERM", () => void shutdown("SIGTERM"));
process.on("SIGINT", () => void shutdown("SIGINT"));
