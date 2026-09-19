import type { FastifyError, FastifyInstance } from "fastify";
import { z } from "zod";
import { eq } from "drizzle-orm";
import { db } from "../db/client.js";
import { jobs } from "@chronoqueue/db";

const createJobBodySchema = z.object({
  type: z.literal("WEBHOOK"),
  targetUrl: z.string().url(),
  payload: z.record(z.string(), z.unknown()),
  scheduledAt: z.coerce.date().optional(),
  maxAttempts: z.number().int().positive().optional(),
});

const RETURNING_COLUMNS = {
  id: jobs.id,
  type: jobs.type,
  status: jobs.status,
  targetUrl: jobs.targetUrl,
  scheduledAt: jobs.scheduledAt,
  createdAt: jobs.createdAt,
};

function badRequest(message: string): FastifyError {
  const error = new Error(message) as FastifyError;
  error.statusCode = 400;
  return error;
}

// PostgreSQL's UNIQUE constraint on idempotency_key is the actual
// correctness guarantee here (see PART 1/CONCURRENCY in the Day 12 spec):
// a SELECT-then-INSERT would race under concurrent requests, so instead we
// always attempt the INSERT and only fall back to a lookup when Postgres
// itself reports a conflict on this exact constraint.
const IDEMPOTENCY_KEY_CONSTRAINT = "jobs_idempotency_key_unique";

function isIdempotencyKeyConflict(error: unknown): boolean {
  const cause =
    error && typeof error === "object" && "cause" in error
      ? (error as { cause: unknown }).cause
      : undefined;

  return (
    !!cause &&
    typeof cause === "object" &&
    "code" in cause &&
    (cause as { code?: unknown }).code === "23505" &&
    "constraint" in cause &&
    (cause as { constraint?: unknown }).constraint ===
      IDEMPOTENCY_KEY_CONSTRAINT
  );
}

export function registerJobRoutes(app: FastifyInstance): void {
  app.post("/jobs", async (request, reply) => {
    const idempotencyKeyHeader = request.headers["idempotency-key"];
    const idempotencyKey =
      typeof idempotencyKeyHeader === "string"
        ? idempotencyKeyHeader.trim()
        : "";

    if (!idempotencyKey) {
      throw badRequest("Idempotency-Key header is required");
    }

    const parsed = createJobBodySchema.safeParse(request.body);

    if (!parsed.success) {
      const message = parsed.error.issues
        .map((issue) => `${issue.path.join(".") || "body"}: ${issue.message}`)
        .join("; ");
      throw badRequest(message);
    }

    const { type, targetUrl, payload, scheduledAt, maxAttempts } = parsed.data;

    try {
      const [created] = await db
        .insert(jobs)
        .values({
          idempotencyKey,
          type,
          status: "PENDING",
          targetUrl,
          payload,
          ...(scheduledAt ? { scheduledAt } : {}),
          ...(maxAttempts ? { maxAttempts } : {}),
        })
        .returning(RETURNING_COLUMNS);

      reply.status(201).send(created);
    } catch (error) {
      if (!isIdempotencyKeyConflict(error)) {
        throw error;
      }

      // Another request (possibly concurrent) already created a job with
      // this exact idempotency key. Return that existing logical job
      // instead of creating a duplicate.
      const [existing] = await db
        .select(RETURNING_COLUMNS)
        .from(jobs)
        .where(eq(jobs.idempotencyKey, idempotencyKey))
        .limit(1);

      if (!existing) {
        // Should not happen: the constraint violation guarantees a row
        // exists, but guard defensively rather than silently succeeding.
        throw error;
      }

      reply.status(200).send(existing);
    }
  });
}
