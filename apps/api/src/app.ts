import Fastify from "fastify";
import { SERVICE_REDACT_PATHS, safeError } from "@chronoqueue/observability";
import { config } from "./config/env.js";
import { registerErrorHandler } from "./plugins/error-handler.js";
import { registerJobRoutes } from "./routes/jobs.js";
import { metrics } from "./observability.js";

// Route pattern for logs: Fastify v5 exposes the matched route via
// routeOptions.url. We prefer it over the raw URL so query strings
// (which can carry tokens) never reach the logs.
function routePattern(request: {
  routeOptions?: { url?: string };
  url: string;
}): string {
  const pattern = request.routeOptions?.url;
  if (typeof pattern === "string" && pattern.length > 0) {
    return pattern;
  }
  const queryIndex = request.url.indexOf("?");
  return queryIndex === -1 ? request.url : request.url.slice(0, queryIndex);
}

export function buildApp(opts?: { logStream?: NodeJS.WritableStream }) {
  const app = Fastify({
    // The API reuses Fastify's own request ID (request.id) — no second
    // request-ID system (PART 1). The stable `service` base field and the
    // shared redaction paths come from @chronoqueue/observability so the
    // API logs under the same secrets policy as Scheduler/Worker.
    // logStream is a test-only seam: pino captures its destination at
    // logger creation, so stdout patching after buildApp() cannot
    // intercept logs. Production never passes it (default: stdout).
    logger: {
      level: config.LOG_LEVEL,
      base: { service: "api" },
      redact: { paths: SERVICE_REDACT_PATHS, censor: "[Redacted]" },
      ...(opts?.logStream ? { stream: opts.logStream } : {}),
    },
  });


  // Request lifecycle: stable service + event fields on every request.
  // Durations use Fastify's own elapsed timer. Only meaningful
  // request/response pairs are logged at INFO; 5xx responses escalate
  // to ERROR via the error handler (PART 12).
  app.addHook("onRequest", (request, _reply, done) => {
    request.log.info(
      {
        event: "request_received",
        requestId: request.id,
        method: request.method,
        route: routePattern(request),
      },
      "request received",
    );
    done();
  });

  app.addHook("onResponse", (request, reply, done) => {
    request.log.info(
      {
        event: "request_completed",
        requestId: request.id,
        method: request.method,
        route: routePattern(request),
        statusCode: reply.statusCode,
        durationMs: Math.round(reply.elapsedTime),
      },
      "request completed",
    );
    done();
  });

  registerErrorHandler(app);
  registerJobRoutes(app);

  app.get("/health", async () => {
    return {
      status: "ok",
    };
  });

  app.get("/ready", async (request, reply) => {
    // Cheap real dependency checks (PART 8): a trivial SELECT on
    // PostgreSQL and a PING on the Redis connection. Both are logged
    // safely — failures carry an error code, never credentials.
    const checks: Record<string, { ok: boolean; errorCode?: string }> = {};

    try {
      const { db } = await import("./db/client.js");
      await db.execute("SELECT 1");
      checks.postgres = { ok: true };
    } catch (error) {
      checks.postgres = {
        ok: false,
        errorCode: safeError(error).code ?? "unknown",
      };
    }

    try {
      const { connection } = await import("./queue/connection.js");
      const pong = await connection.ping();
      checks.redis = { ok: pong === "PONG" };
    } catch (error) {
      checks.redis = {
        ok: false,
        errorCode: safeError(error).code ?? "unknown",
      };
    }

    const allOk = Object.values(checks).every((c) => c.ok);

    if (!allOk) {
      request.log.warn(
        {
          event: "readiness_check_failed",
          requestId: request.id,
          checks,
        },
        "readiness check failed",
      );
      reply.status(503);
      return { status: "not_ready", checks };
    }

    return { status: "ready", checks };
  });

  // Prometheus metrics in the standard text exposition format
  // (PART 8). This exposes the API process's own registry; the
  // Scheduler and Worker each keep their own in-process registries
  // because they run as separate processes.
  app.get("/metrics", async (_request, reply) => {
    reply.header("Content-Type", "text/plain; version=0.0.4");
    return metrics.toPrometheus();
  });

  return app;
}
