import pino, { type DestinationStream, type Logger } from "pino";

export type ServiceName = "api" | "scheduler" | "worker";

export interface ServiceLoggerOptions {
  level?: string;
  stream?: DestinationStream;
}

// Defense-in-depth redaction. The primary protection is never logging
// secrets in the first place (see safeError() and code review); these
// paths are a safety net so that an accidental `leaseToken` or credential
// field is censored before serialization instead of hitting the log sink.
//
// Shared with Fastify's native logger option: the API uses Fastify's own
// logger rather than a standalone pino instance, so the base field and
// redaction are configured there — same secrets policy.
export const SERVICE_REDACT_PATHS: string[] = [
  "leaseToken",
  "*.leaseToken",
  "authorization",
  "*.authorization",
  "req.headers.authorization",
  "DATABASE_URL",
  "REDIS_URL",
  "*.DATABASE_URL",
  "*.REDIS_URL",
  "password",
  "*.password",
  "secret",
  "*.secret",
];

// Every service logs with a stable `service` base field so an engineer can
// tell at a glance whether a line came from the API, the scheduler, or a
// worker — the first question in any 3 AM incident.
export function createServiceLogger(
  service: ServiceName,
  options: ServiceLoggerOptions = {},
): Logger {
  return pino(
    {
      level: options.level ?? process.env.LOG_LEVEL ?? "info",
      base: { service },
      redact: { paths: SERVICE_REDACT_PATHS, censor: "[Redacted]" },
    },
    options.stream,
  );
}
