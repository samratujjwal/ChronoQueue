import { Redis } from "ioredis";
import { config } from "../config/env.js";

// BullMQ requires maxRetriesPerRequest: null for Worker connections — it
// uses blocking Redis commands internally and needs to retry indefinitely
// rather than give up after a fixed number of attempts. This is the
// opposite of the producer-side connection in apps/api, which intentionally
// keeps the default so HTTP-facing calls fail fast. Do not reuse this
// connection for a Queue producer, and do not remove this setting.
export const connection = new Redis(config.REDIS_URL, {
  maxRetriesPerRequest: null,
});
