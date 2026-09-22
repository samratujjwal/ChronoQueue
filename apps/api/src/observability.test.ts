import { randomUUID } from "node:crypto";
import { Writable } from "node:stream";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import { buildApp } from "./app.js";
import { db } from "./db/client.js";
// import { pool } from "./db/client.js";
import { connection as redisConnection } from "./queue/connection.js";
import { webhookQueue } from "./queue/webhook-queue.js";
import { apiMetrics, resetApiMetrics } from "./observability.js";

const insertedIds: string[] = [];
const bullJobIdsToClean: string[] = [];

afterEach(async () => {
  resetApiMetrics();
  for (const id of bullJobIdsToClean.splice(0)) {
    const bullJob = await webhookQueue.getJob(id);
    await bullJob?.remove();
  }
  for (const id of insertedIds.splice(0)) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
});

afterAll(async () => {
  await webhookQueue.close();
  await redisConnection.quit();
  // await pool.end();
});

function jobPayload(targetUrl = "http://127.0.0.1:9/hook") {
  return {
    type: "WEBHOOK",
    targetUrl,
    payload: { hello: "world" },
  };
}

describe("api observability (PART 11)", () => {
  it("H: GET /metrics returns Prometheus text exposition format", async () => {
    const app = buildApp();
    const res = await app.inject({ method: "GET", url: "/metrics" });

    expect(res.statusCode).toBe(200);
    expect(res.headers["content-type"]).toContain("text/plain");
    // HELP/TYPE annotations are part of the exposition format.
    expect(res.body).toContain("# HELP jobs_created_total");
    expect(res.body).toContain("# TYPE jobs_created_total counter");

    await app.close();
  });

  it("E: jobs_created_total increments only on successful creation (201, not 400)", async () => {
    const app = buildApp();
    expect(apiMetrics.jobsCreatedTotal.get()).toBe(0);

    // Invalid body -> 400, no metric.
    const bad = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: { "idempotency-key": randomUUID() },
      payload: { type: "WEBHOOK" },
    });
    expect(bad.statusCode).toBe(400);
    expect(apiMetrics.jobsCreatedTotal.get()).toBe(0);

    // Valid body -> 201, metric increments.
    const key = randomUUID();
    const good = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: { "idempotency-key": key },
      payload: jobPayload(),
    });
    expect(good.statusCode).toBe(201);
    expect(apiMetrics.jobsCreatedTotal.get()).toBe(1);
    insertedIds.push(good.json().id);

    // Same idempotency key -> 200 idempotent hit, jobs_created_total
    // stays put while the idempotent-hit counter moves.
    const dupe = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: { "idempotency-key": key },
      payload: jobPayload(),
    });
    expect(dupe.statusCode).toBe(200);
    expect(apiMetrics.jobsCreatedTotal.get()).toBe(1);
    expect(apiMetrics.jobCreationIdempotentHitsTotal.get()).toBe(1);

    await app.close();
  });

  it("E: jobs_retriggered_total increments only on a fully successful re-trigger", async () => {
    const app = buildApp();

    // Create then kill a job so it lands in DEAD.
    const key = randomUUID();
    const created = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: { "idempotency-key": key },
      payload: { ...jobPayload(), maxAttempts: 1 },
    });
    const jobId = created.json().id as string;
    insertedIds.push(jobId);
    bullJobIdsToClean.push(jobId);

    await db
      .update(jobs)
      .set({
        status: "DEAD",
        attempts: 1,
        lastErrorCode: "HTTP_500",
        lastErrorMessage: "boom",
      })
      .where(eq(jobs.id, jobId));

    expect(apiMetrics.jobsRetriggeredTotal.get()).toBe(0);

    // Non-dead job -> 409, no metric.
    const live = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: { "idempotency-key": randomUUID() },
      payload: jobPayload(),
    });
    const liveId = live.json().id as string;
    insertedIds.push(liveId);
    const conflict = await app.inject({
      method: "POST",
      url: `/jobs/${liveId}/retry`,
    });
    expect(conflict.statusCode).toBe(409);
    expect(apiMetrics.jobsRetriggeredTotal.get()).toBe(0);

    // Real re-trigger -> 200, metric increments.
    const retriggered = await app.inject({
      method: "POST",
      url: `/jobs/${jobId}/retry`,
    });
    expect(retriggered.statusCode).toBe(200);
    expect(apiMetrics.jobsRetriggeredTotal.get()).toBe(1);

    await app.close();
  });

  it("I: GET /ready returns 200 with passing dependency checks", async () => {
    const app = buildApp();
    const res = await app.inject({ method: "GET", url: "/ready" });

    expect(res.statusCode).toBe(200);
    const body = res.json() as {
      status: string;
      checks: Record<string, { ok: boolean }>;
    };
    expect(body.status).toBe("ready");
    expect(body.checks.postgres?.ok).toBe(true);
    expect(body.checks.redis?.ok).toBe(true);

    await app.close();
  });

  it("B: request lifecycle logs carry the same requestId on received/completed", async () => {
    const lines: string[] = [];
    // Pino captures its destination at logger creation, so patching
    // process.stdout.write after buildApp() cannot intercept anything.
    // The logStream seam hands the app a capture stream up front.
    const logStream = new Writable({
      write(chunk, _encoding, callback) {
        lines.push(chunk.toString());
        callback();
      },
    });
    const app = buildApp({ logStream });
    await app.inject({ method: "GET", url: "/health" });

    const parsed = lines
      .map((l) => {
        try {
          return JSON.parse(l) as Record<string, unknown>;
        } catch {
          return null;
        }
      })
      .filter(Boolean) as Record<string, unknown>[];

    const received = parsed.find((l) => l.event === "request_received");
    const completed = parsed.find((l) => l.event === "request_completed");
    expect(received).toBeDefined();
    expect(completed).toBeDefined();
    expect(received?.service).toBe("api");
    expect(completed?.service).toBe("api");
    expect(received?.requestId).toBe(completed?.requestId);
    expect(typeof completed?.durationMs).toBe("number");

    await app.close();
  });
});
