import { randomUUID } from "node:crypto";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { buildApp } from "../app.js";
import { db } from "../db/client.js";
import { jobs, type JobStatus } from "@chronoqueue/db";
import { eq } from "drizzle-orm";
import { webhookQueue } from "../queue/webhook-queue.js";
import { connection as redisConnection } from "../queue/connection.js";

const app = buildApp();
const insertedIds: string[] = [];
// BullMQ job ids (== PG job ids) created by re-trigger tests; removed
// after each test so Redis stays clean.
const bullJobIdsToClean: string[] = [];
// Rows created by the DLQ tests below; removed after each test so
// per-test counts stay deterministic.
const dlqTestIds: string[] = [];

afterEach(async () => {
  for (const id of bullJobIdsToClean.splice(0)) {
    const bullJob = await webhookQueue.getJob(id);
    await bullJob?.remove();
  }
  for (const id of dlqTestIds.splice(0)) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
});

afterAll(async () => {
  for (const id of insertedIds) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
  await app.close();
  await webhookQueue.close();
  await redisConnection.quit();
});

function headers(key: string) {
  return { "idempotency-key": key };
}

describe("POST /jobs — validation", () => {
  it("rejects a missing Idempotency-Key header", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      payload: {
        type: "WEBHOOK",
        targetUrl: "https://example.com/webhook",
        payload: { event: "user.created" },
      },
    });
    expect(res.statusCode).toBe(400);
  });

  it("rejects missing type", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload: {
        targetUrl: "https://example.com/webhook",
        payload: { event: "user.created" },
      },
    });
    expect(res.statusCode).toBe(400);
  });

  it("rejects unsupported type", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload: {
        type: "EMAIL",
        targetUrl: "https://example.com/webhook",
        payload: { event: "user.created" },
      },
    });
    expect(res.statusCode).toBe(400);
  });

  it("rejects missing targetUrl", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload: {
        type: "WEBHOOK",
        payload: { event: "user.created" },
      },
    });
    expect(res.statusCode).toBe(400);
  });

  it("rejects malformed targetUrl", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload: {
        type: "WEBHOOK",
        targetUrl: "not-a-url",
        payload: { event: "user.created" },
      },
    });
    expect(res.statusCode).toBe(400);
  });

  it("rejects missing payload", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload: {
        type: "WEBHOOK",
        targetUrl: "https://example.com/webhook",
      },
    });
    expect(res.statusCode).toBe(400);
  });

  it("rejects invalid maxAttempts", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload: {
        type: "WEBHOOK",
        targetUrl: "https://example.com/webhook",
        payload: { event: "user.created" },
        maxAttempts: 0,
      },
    });
    expect(res.statusCode).toBe(400);
  });

  it("rejects malformed scheduledAt", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload: {
        type: "WEBHOOK",
        targetUrl: "https://example.com/webhook",
        payload: { event: "user.created" },
        scheduledAt: "not-a-date",
      },
    });
    expect(res.statusCode).toBe(400);
  });
});

describe("POST /jobs — success (real PostgreSQL)", () => {
  it("creates a PENDING job and returns 201", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload: {
        type: "WEBHOOK",
        targetUrl: "https://example.com/webhook",
        payload: { event: "user.created", userId: "123" },
      },
    });

    expect(res.statusCode).toBe(201);
    const body = res.json();
    expect(body.status).toBe("PENDING");
    expect(body.type).toBe("WEBHOOK");
    expect(body.targetUrl).toBe("https://example.com/webhook");
    expect(typeof body.id).toBe("string");

    insertedIds.push(body.id);
  });
});

describe("POST /jobs — idempotency (real PostgreSQL)", () => {
  it("returns the same logical job for two sequential requests with the same key", async () => {
    const key = randomUUID();
    const payload = {
      type: "WEBHOOK",
      targetUrl: "https://example.com/idempotency-test",
      payload: { event: "order.created" },
    };

    const first = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(key),
      payload,
    });
    expect(first.statusCode).toBe(201);
    const firstBody = first.json();
    insertedIds.push(firstBody.id);

    const second = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(key),
      payload,
    });
    expect(second.statusCode).toBe(200);
    const secondBody = second.json();

    expect(secondBody.id).toBe(firstBody.id);

    const rows = await db
      .select()
      .from(jobs)
      .where(eq(jobs.idempotencyKey, key));
    expect(rows.length).toBe(1);
  });

  it("creates independent jobs for different idempotency keys", async () => {
    const payload = {
      type: "WEBHOOK",
      targetUrl: "https://example.com/idempotency-test",
      payload: { event: "order.created" },
    };

    const resA = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload,
    });
    const resB = await app.inject({
      method: "POST",
      url: "/jobs",
      headers: headers(randomUUID()),
      payload,
    });

    expect(resA.statusCode).toBe(201);
    expect(resB.statusCode).toBe(201);

    const idA = resA.json().id;
    const idB = resB.json().id;
    expect(idA).not.toBe(idB);

    insertedIds.push(idA, idB);
  });

  it("handles two concurrent requests with the same key: exactly one database row", async () => {
    const key = randomUUID();
    const payload = {
      type: "WEBHOOK",
      targetUrl: "https://example.com/idempotency-concurrent-test",
      payload: { event: "order.created" },
    };

    const [resA, resB] = await Promise.all([
      app.inject({
        method: "POST",
        url: "/jobs",
        headers: headers(key),
        payload,
      }),
      app.inject({
        method: "POST",
        url: "/jobs",
        headers: headers(key),
        payload,
      }),
    ]);

    const statusCodes = [resA.statusCode, resB.statusCode].sort();
    expect(statusCodes).toEqual([200, 201]);
    expect(resA.json().id).toBe(resB.json().id);

    insertedIds.push(resA.json().id);

    const rows = await db
      .select()
      .from(jobs)
      .where(eq(jobs.idempotencyKey, key));
    expect(rows.length).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Day 14 DLQ tests (real PostgreSQL + real Redis)
// ---------------------------------------------------------------------------

async function insertDlqJob(options: {
  status: JobStatus;
  updatedAt?: Date;
  attempts?: number;
  maxAttempts?: number;
  lastErrorCode?: string | null;
}): Promise<string> {
  const id = randomUUID();
  dlqTestIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: options.status,
    targetUrl: "http://127.0.0.1:9/unused",
    payload: { test: true },
    attempts: options.attempts ?? 0,
    maxAttempts: options.maxAttempts ?? 5,
    lastErrorCode: options.lastErrorCode ?? null,
    lastErrorMessage: options.lastErrorCode ? "simulated failure" : null,
    ...(options.updatedAt ? { updatedAt: options.updatedAt } : {}),
  });
  return id;
}

async function fetchJobRow(id: string) {
  const [row] = await db.select().from(jobs).where(eq(jobs.id, id)).limit(1);
  return row;
}

describe("GET /jobs/dead — DLQ listing (real PostgreSQL)", () => {
  it("returns only DEAD jobs, newest first, with working pagination", async () => {
    const base = Date.now() - 60_000;
    const oldest = await insertDlqJob({
      status: "DEAD",
      updatedAt: new Date(base),
      lastErrorCode: "HTTP_500",
    });
    const middle = await insertDlqJob({
      status: "DEAD",
      updatedAt: new Date(base + 1_000),
      lastErrorCode: "TIMEOUT",
    });
    const newest = await insertDlqJob({
      status: "DEAD",
      updatedAt: new Date(base + 2_000),
      lastErrorCode: "NETWORK_ERROR",
    });

    const page1 = await app.inject({
      method: "GET",
      url: "/jobs/dead?pageSize=2&page=1",
    });
    expect(page1.statusCode).toBe(200);
    const body1 = page1.json();
    expect(body1.total).toBe(3);
    expect(body1.totalPages).toBe(2);
    expect(body1.page).toBe(1);
    expect(body1.pageSize).toBe(2);
    expect(body1.jobs).toHaveLength(2);
    // Newest-dead first, deterministic order.
    expect(body1.jobs[0].id).toBe(newest);
    expect(body1.jobs[1].id).toBe(middle);
    expect(body1.jobs[0].lastErrorCode).toBe("NETWORK_ERROR");

    const page2 = await app.inject({
      method: "GET",
      url: "/jobs/dead?pageSize=2&page=2",
    });
    expect(page2.statusCode).toBe(200);
    const body2 = page2.json();
    expect(body2.jobs).toHaveLength(1);
    expect(body2.jobs[0].id).toBe(oldest);
  });

  it("excludes PENDING/QUEUED/PROCESSING/RETRYING/SUCCEEDED jobs", async () => {
    const deadId = await insertDlqJob({ status: "DEAD" });
    await insertDlqJob({ status: "PENDING" });
    await insertDlqJob({ status: "QUEUED" });
    await insertDlqJob({ status: "PROCESSING" });
    await insertDlqJob({ status: "RETRYING" });
    await insertDlqJob({ status: "SUCCEEDED" });

    const res = await app.inject({ method: "GET", url: "/jobs/dead" });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.total).toBe(1);
    expect(body.jobs).toHaveLength(1);
    expect(body.jobs[0].id).toBe(deadId);
    expect(body.jobs[0].status).toBe("DEAD");
  });

  it("rejects invalid pagination parameters", async () => {
    for (const url of [
      "/jobs/dead?pageSize=0",
      "/jobs/dead?pageSize=101",
      "/jobs/dead?pageSize=abc",
      "/jobs/dead?page=0",
      "/jobs/dead?page=-3",
    ]) {
      const res = await app.inject({ method: "GET", url });
      expect(res.statusCode).toBe(400);
    }
  });

  it("applies default pagination", async () => {
    await insertDlqJob({ status: "DEAD" });
    const res = await app.inject({ method: "GET", url: "/jobs/dead" });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.page).toBe(1);
    expect(body.pageSize).toBe(20);
    expect(body.total).toBe(1);
  });
});

describe("GET /jobs/:id", () => {
  it("returns the job with full diagnostic fields", async () => {
    const id = await insertDlqJob({
      status: "DEAD",
      attempts: 3,
      lastErrorCode: "HTTP_500",
    });

    const res = await app.inject({ method: "GET", url: `/jobs/${id}` });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.id).toBe(id);
    expect(body.status).toBe("DEAD");
    expect(body.attempts).toBe(3);
    expect(body.lastErrorCode).toBe("HTTP_500");
    expect(body.targetUrl).toBe("http://127.0.0.1:9/unused");
  });

  it("returns 404 for an unknown id", async () => {
    const res = await app.inject({
      method: "GET",
      url: `/jobs/${randomUUID()}`,
    });
    expect(res.statusCode).toBe(404);
  });

  it("returns 400 for a malformed id", async () => {
    const res = await app.inject({ method: "GET", url: "/jobs/not-a-uuid" });
    expect(res.statusCode).toBe(400);
  });
});

describe("POST /jobs/:id/retry — manual re-trigger (real PostgreSQL + real Redis)", () => {
  it("transitions DEAD -> QUEUED, preserves attempts, creates no lease, and enqueues a BullMQ job", async () => {
    const id = await insertDlqJob({
      status: "DEAD",
      attempts: 5,
      maxAttempts: 5,
      lastErrorCode: "HTTP_500",
    });
    bullJobIdsToClean.push(id);

    const res = await app.inject({
      method: "POST",
      url: `/jobs/${id}/retry`,
    });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.status).toBe("QUEUED");

    const row = await fetchJobRow(id);
    expect(row.status).toBe("QUEUED");
    // Attempt history is preserved, not reset.
    expect(row.attempts).toBe(5);
    // The re-trigger must not create or reuse a leaseToken.
    expect(row.leaseToken).toBeNull();
    expect(row.leaseUntil).toBeNull();
    expect(row.nextAttemptAt).toBeNull();
    // Failure diagnosis is kept for the record.
    expect(row.lastErrorCode).toBe("HTTP_500");

    // The job must actually be drivable by the Worker now.
    const bullJob = await webhookQueue.getJob(id);
    expect(bullJob).toBeDefined();
    expect(bullJob!.data).toEqual({ jobId: id });
  });

  it("re-enqueue replaces a stale BullMQ record left by the dead attempt (deterministic jobId collision)", async () => {
    const id = await insertDlqJob({
      status: "DEAD",
      lastErrorCode: "HTTP_500",
    });
    bullJobIdsToClean.push(id);

    // Simulate the leftover BullMQ record from the dead attempt: same
    // deterministic jobId (= PG id), sitting in Redis.
    const stale = await webhookQueue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id },
    );
    const staleTimestamp = stale.timestamp;
    expect(await webhookQueue.getJob(id)).toBeDefined();

    const res = await app.inject({
      method: "POST",
      url: `/jobs/${id}/retry`,
    });
    expect(res.statusCode).toBe(200);

    // Exactly one BullMQ job must exist for this PG job, and it must be a
    // FRESH live record — not the silently-returned stale one.
    const queued = await webhookQueue.getJobs(["waiting", "active", "delayed"]);
    const matches = queued.filter((j) => j.id === id);
    expect(matches).toHaveLength(1);
    expect(matches[0].timestamp).toBeGreaterThan(staleTimestamp);
    expect(matches[0].data).toEqual({ jobId: id });
  });

  it("rejects re-triggering non-DEAD jobs with 409", async () => {
    const statuses: JobStatus[] = [
      "PENDING",
      "QUEUED",
      "PROCESSING",
      "RETRYING",
      "SUCCEEDED",
    ];
    for (const status of statuses) {
      const id = await insertDlqJob({ status });
      const res = await app.inject({
        method: "POST",
        url: `/jobs/${id}/retry`,
      });
      expect(res.statusCode).toBe(409);
      expect((await fetchJobRow(id)).status).toBe(status);
    }
  });

  it("returns 404 for an unknown id", async () => {
    const res = await app.inject({
      method: "POST",
      url: `/jobs/${randomUUID()}/retry`,
    });
    expect(res.statusCode).toBe(404);
  });

  it("returns 400 for a malformed id", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs/not-a-uuid/retry",
    });
    expect(res.statusCode).toBe(400);
  });

  it("two concurrent re-triggers: exactly one succeeds, no duplicate execution state", async () => {
    const id = await insertDlqJob({
      status: "DEAD",
      attempts: 5,
      lastErrorCode: "HTTP_500",
    });
    bullJobIdsToClean.push(id);

    const [resA, resB] = await Promise.all([
      app.inject({ method: "POST", url: `/jobs/${id}/retry` }),
      app.inject({ method: "POST", url: `/jobs/${id}/retry` }),
    ]);

    const statusCodes = [resA.statusCode, resB.statusCode].sort();
    expect(statusCodes).toEqual([200, 409]);

    const row = await fetchJobRow(id);
    expect(row.status).toBe("QUEUED");
    expect(row.attempts).toBe(5);

    // Exactly one BullMQ message for this job — no duplicate execution.
    const queued = await webhookQueue.getJobs(["waiting", "active", "delayed"]);
    expect(queued.filter((j) => j.id === id)).toHaveLength(1);
  });
});
