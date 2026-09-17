import { Redis } from "ioredis";
import { config } from "../config/env.js";

// This connection is for producer (Queue) use only — the Scheduler enqueues
// jobs, it does not consume them. maxRetriesPerRequest is intentionally
// left at its ioredis default, matching apps/api's producer connection.
// Do not reuse this for a BullMQ Worker connection, which requires
// maxRetriesPerRequest: null instead (see apps/worker/src/queue/connection.ts).
export const connection = new Redis(config.REDIS_URL);
