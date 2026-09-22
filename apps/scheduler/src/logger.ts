import { createServiceLogger } from "@chronoqueue/observability";
import { config } from "./config/env.js";

export const logger = createServiceLogger("scheduler", {
  level: config.LOG_LEVEL,
});
