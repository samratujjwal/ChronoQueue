import { Redis } from "ioredis";
import { config } from "../config/env.js";

// This connection is for producer (Queue) use only.
// maxRetriesPerRequest is intentionally left at its ioredis default so
// HTTP-facing calls fail relatively fast instead of retrying indefinitely.
// A future Worker connection must set maxRetriesPerRequest: null instead —
// do not reuse this module for that.
export const connection = new Redis(config.REDIS_URL);
