import Fastify from "fastify";
import { config } from "./config/env.js";
import { registerErrorHandler } from "./plugins/error-handler.js";
import { registerJobRoutes } from "./routes/jobs.js";

export function buildApp() {
  const app = Fastify({
    logger: {
      level: config.LOG_LEVEL,
    },
  });

  registerErrorHandler(app);
  registerJobRoutes(app);

  app.get("/health", async () => {
    return {
      status: "ok",
    };
  });

  app.get("/ready", async () => {
    return {
      status: "ready",
    };
  });

  return app;
}
