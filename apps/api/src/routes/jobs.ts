import type { FastifyError, FastifyInstance } from "fastify";
import { z } from "zod";
import { and, eq } from "drizzle-orm";
import { db } from "../db/client.js";
import {
  DLQ_PAGE_SIZE_DEFAULT,
  DLQ_PAGE_SIZE_MAX,
  getJobById,
  isValidTransition,
  jobs,
  listDeadJobs,
  retriggerDeadJob,
} from "@chronoqueue/db";
import { API_EVENTS, safeError, safely } from "@chronoqueue/observability";
import { enqueueRetriggeredJob } from "../queue/webhook-producer.js";
import { apiMetrics } from "../observability.js";

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

// Full diagnostic view for the DLQ (Day 14): everything an operator needs
// to decide whether a dead job is worth re-triggering.
const DLQ_JOB_COLUMNS = {
  id: jobs.id,
  type: jobs.type,
  status: jobs.status,
  targetUrl: jobs.targetUrl,
  payload: jobs.payload,
  attempts: jobs.attempts,
  maxAttempts: jobs.maxAttempts,
  scheduledAt: jobs.scheduledAt,
  nextAttemptAt: jobs.nextAttemptAt,
  lastErrorCode: jobs.lastErrorCode,
  lastErrorMessage: jobs.lastErrorMessage,
  createdAt: jobs.createdAt,
  updatedAt: jobs.updatedAt,
};

function badRequest(message: string): FastifyError {
  const error = new Error(message) as FastifyError;
  error.statusCode = 400;
  return error;
}

function notFound(message: string): FastifyError {
  const error = new Error(message) as FastifyError;
  error.statusCode = 404;
  return error;
}

function conflict(message: string): FastifyError {
  const error = new Error(message) as FastifyError;
  error.statusCode = 409;
  return error;
}

function internalError(message: string): FastifyError {
  const error = new Error(message) as FastifyError;
  error.statusCode = 500;
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

// The idempotency key itself is safe to log (it is a client-provided dedup
// token, never a secret), but its presence is the diagnostic — never its
// value in a way that could be cross-referenced. We log hasIdempotencyKey
// only.
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

      request.log.info(
        {
          event: API_EVENTS.jobCreated,
          jobId: created?.id,
          requestId: request.id,
        },
        "job created",
      );

      // Metrics follow the successful write (PART 6), not the attempt.
      safely(() => apiMetrics.jobsCreatedTotal.inc());

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

      request.log.info(
        {
          event: API_EVENTS.jobCreationIdempotentHit,
          jobId: existing.id,
          requestId: request.id,
        },
        "idempotent job creation hit",
      );

      safely(() => apiMetrics.jobCreationIdempotentHitsTotal.inc());

      reply.status(200).send(existing);
    }
  });

  // NOTE: /jobs/dead is registered before /jobs/:id. Fastify prioritises
  // static segments over parameters anyway, but explicit ordering keeps
  // the intent obvious.
  app.get("/jobs/dead", async (request, reply) => {
    const parsed = z
      .object({
        page: z.coerce.number().int().min(1).default(1),
        pageSize: z.coerce
          .number()
          .int()
          .min(1)
          .max(DLQ_PAGE_SIZE_MAX)
          .default(DLQ_PAGE_SIZE_DEFAULT),
      })
      .safeParse(request.query);

    if (!parsed.success) {
      const message = parsed.error.issues
        .map((issue) => `${issue.path.join(".") || "query"}: ${issue.message}`)
        .join("; ");
      throw badRequest(message);
    }

    const page = await listDeadJobs(db, parsed.data);

    request.log.info(
      {
        event: API_EVENTS.deadJobsListed,
        requestId: request.id,
        page: parsed.data.page,
        pageSize: parsed.data.pageSize,
        total: page.total,
      },
      "dead jobs listed",
    );

    reply.status(200).send(page);
  });

  app.get("/jobs/:id", async (request, reply) => {
    const parsed = z
      .object({ id: z.string().uuid() })
      .safeParse(request.params);

    if (!parsed.success) {
      throw badRequest("id: must be a valid UUID");
    }

    const job = await getJobById(db, parsed.data.id);

    if (!job) {
      request.log.info(
        {
          event: API_EVENTS.jobRetrieved,
          requestId: request.id,
          jobId: parsed.data.id,
          found: false,
        },
        "job lookup missed",
      );
      throw notFound(`job ${parsed.data.id} not found`);
    }

    request.log.info(
      {
        event: API_EVENTS.jobRetrieved,
        requestId: request.id,
        jobId: job.id,
        found: true,
      },
      "job retrieved",
    );

    reply.status(200).send(job);
  });

  app.post("/jobs/:id/retry", async (request, reply) => {
    const parsed = z
      .object({ id: z.string().uuid() })
      .safeParse(request.params);

    if (!parsed.success) {
      throw badRequest("id: must be a valid UUID");
    }

    const { id } = parsed.data;

    request.log.info(
      {
        event: API_EVENTS.jobRetriggerRequested,
        requestId: request.id,
        jobId: id,
      },
      "job re-trigger requested",
    );

    if (!isValidTransition("DEAD", "QUEUED")) {
      // Defensive: the state machine must permit the re-trigger.
      throw internalError("DEAD -> QUEUED transition is not allowed");
    }

    const result = await retriggerDeadJob(db, id);

    if (!result.ok) {
      if (result.reason === "not_found") {
        request.log.info(
          {
            event: API_EVENTS.jobRetriggerFailed,
            requestId: request.id,
            jobId: id,
            reason: "not_found",
          },
          "job re-trigger failed: job not found",
        );
        throw notFound(`job ${id} not found`);
      }
      request.log.info(
        {
          event: API_EVENTS.jobRetriggerConflict,
          requestId: request.id,
          jobId: id,
          reason: result.reason,
        },
        "job re-trigger conflict: job is not dead",
      );
      throw conflict(
        `job ${id} is not eligible for retry: only DEAD jobs can be manually re-triggered`,
      );
    }

    // The DB transition above is durable; the BullMQ message is
    // coordination. Enqueue AFTER the guarded update so a crash between
    // the two can only strand a QUEUED row (the scheduler's known
    // dual-write gap) — never lose an operator's re-trigger silently.
    try {
      await enqueueRetriggeredJob(id);
    } catch (error) {
      // Enqueue failed: compensate by moving the row back to DEAD via a
      // guarded update, so the job is NOT left stranded in QUEUED with no
      // BullMQ message. The 500 tells the operator to retry; the job's
      // history (attempts, failure diagnosis) is untouched.
      await db
        .update(jobs)
        .set({ status: "DEAD", updatedAt: new Date() })
        .where(and(eq(jobs.id, id), eq(jobs.status, "QUEUED")));

      request.log.error(
        {
          event: API_EVENTS.jobRetriggerFailed,
          requestId: request.id,
          postgresJobId: id,
          ...safeError(error),
        },
        "dlq: failed to enqueue re-triggered job, returned it to DEAD",
      );

      throw internalError(
        `failed to enqueue re-triggered job ${id}; job returned to DEAD`,
      );
    }

    request.log.info(
      {
        event: API_EVENTS.jobRetriggered,
        requestId: request.id,
        jobId: result.job.id,
      },
      "job re-triggered",
    );

    // Count only the fully successful re-trigger (DB + BullMQ), not the
    // attempt (PART 6).
    safely(() => apiMetrics.jobsRetriggeredTotal.inc());

    reply.status(200).send(result.job);
  });
}
