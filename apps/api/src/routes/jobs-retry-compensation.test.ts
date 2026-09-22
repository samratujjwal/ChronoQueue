import { randomUUID } from "node:crypto";
import { afterAll, afterEach, describe, expect, it, vi } from "vitest";

// Audit P1: manual-retrigger compensation race.
//
// The retry route runs: DEAD -> QUEUED (guarded DB update), then a BullMQ
// enqueue. If the enqueue throws, the route compensates with a STATE-GUARDED
// update (WHERE id = ? AND status = 'QUEUED'). These tests prove, against
// the real route + real guarded query, that the compensation can never
// kill a newer legitimate execution attempt.
//
// We mock only the Redis-facing enqueue (webhook-producer), never the DB
// logic: the guarded re-trigger and the guarded compensation both run for
// real against PostgreSQL.
vi.mock("../queue/webhook-producer.js", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../queue/webhook-producer.js")>();
  return {
    ...actual,
    enqueueRetriggeredJob: vi.fn(),
  };
});

import { buildApp } from "../app.js";
import { db } from "../db/client.js";
import { jobs, type JobStatus } from "@chronoqueue/db";
import { eq } from "drizzle-orm";
import { webhookQueue } from "../queue/webhook-queue.js";
import { connection as redisConnection } from "../queue/connection.js";

const { enqueueRetriggeredJob } = await import("../queue/webhook-producer.js");
const mockEnqueueRetriggeredJob = vi.mocked(enqueueRetriggeredJob);

const app = buildApp();
const insertedIds: string[] = [];

afterEach(async () => {
  mockEnqueueRetriggeredJob.mockReset();
  for (const id of insertedIds.splice(0)) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
});

afterAll(async () => {
  await app.close();
  await webhookQueue.close();
  await redisConnection.quit();
});

async function insertJob(options: {
  status: JobStatus;
  attempts?: number;
  lastErrorCode?: string | null;
}): Promise<string> {
  const id = randomUUID();
  insertedIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: options.status,
    targetUrl: "http://127.0.0.1:9/unused",
    payload: { test: true },
    attempts: options.attempts ?? 0,
    maxAttempts: 5,
    lastErrorCode: options.lastErrorCode ?? null,
    lastErrorMessage: options.lastErrorCode ? "simulated failure" : null,
  });
  return id;
}

async function fetchJobRow(id: string) {
  const [row] = await db.select().from(jobs).where(eq(jobs.id, id)).limit(1);
  return row;
}

describe("POST /jobs/:id/retry — compensation race (audit P1)", () => {
  it("compensation MUST NOT overwrite a newer PROCESSING attempt (T1/T2/T3/T4 race)", async () => {
    const id = await insertJob({
      status: "DEAD",
      attempts: 5,
      lastErrorCode: "HTTP_500",
    });
    const workerToken = randomUUID();

    mockEnqueueRetriggeredJob.mockImplementationOnce(async (jobId: string) => {
      // T2: the BullMQ add ambiguously "failed" but the message actually
      // landed, and a worker claimed the row before T1 saw the error.
      // This mirrors production claimJob exactly: fresh token + lease,
      // guarded to claimable statuses only.
      await db
        .update(jobs)
        .set({
          status: "PROCESSING",
          leaseToken: workerToken,
          leaseUntil: new Date(Date.now() + 30_000),
          updatedAt: new Date(),
        })
        .where(eq(jobs.id, jobId));
      // T3: ...but the API only observed the enqueue failure.
      throw new Error("simulated Redis failure after ambiguous add");
    });

    const res = await app.inject({
      method: "POST",
      url: `/jobs/${id}/retry`,
    });
    expect(res.statusCode).toBe(500);

    // T4: the compensation ran, but its WHERE status = 'QUEUED' guard must
    // have matched zero rows — the newer PROCESSING attempt survives
    // untouched, still owned by the worker's token.
    const row = await fetchJobRow(id);
    expect(row.status).toBe("PROCESSING");
    expect(row.leaseToken).toBe(workerToken);
    expect(row.leaseUntil).not.toBeNull();
    expect(row.attempts).toBe(5);
    expect(row.lastErrorCode).toBe("HTTP_500");
  });

  it("compensation still returns a genuinely stranded QUEUED row to DEAD", async () => {
    const id = await insertJob({
      status: "DEAD",
      attempts: 5,
      lastErrorCode: "HTTP_500",
    });

    // Enqueue genuinely fails; nothing else touches the row, so it is
    // still QUEUED when compensation runs.
    mockEnqueueRetriggeredJob.mockRejectedValueOnce(
      new Error("simulated Redis outage"),
    );

    const res = await app.inject({
      method: "POST",
      url: `/jobs/${id}/retry`,
    });
    expect(res.statusCode).toBe(500);

    const row = await fetchJobRow(id);
    expect(row.status).toBe("DEAD");
    // History is preserved, not destroyed: the operator can retry again.
    expect(row.attempts).toBe(5);
    expect(row.lastErrorCode).toBe("HTTP_500");
    expect(row.lastErrorMessage).toBe("simulated failure");
    expect(row.leaseToken).toBeNull();
    expect(row.leaseUntil).toBeNull();
  });

  it("a compensated job can be re-triggered again afterwards (no poisoned state)", async () => {
    const id = await insertJob({
      status: "DEAD",
      attempts: 5,
      lastErrorCode: "HTTP_500",
    });

    mockEnqueueRetriggeredJob.mockRejectedValueOnce(
      new Error("simulated Redis outage"),
    );
    const failed = await app.inject({
      method: "POST",
      url: `/jobs/${id}/retry`,
    });
    expect(failed.statusCode).toBe(500);
    expect((await fetchJobRow(id)).status).toBe("DEAD");

    // Redis is back: the retry succeeds and the job is drivable again.
    mockEnqueueRetriggeredJob.mockResolvedValueOnce(undefined as never);
    const retried = await app.inject({
      method: "POST",
      url: `/jobs/${id}/retry`,
    });
    expect(retried.statusCode).toBe(200);
    expect(retried.json().status).toBe("QUEUED");

    const row = await fetchJobRow(id);
    expect(row.status).toBe("QUEUED");
    expect(row.attempts).toBe(5);
    expect(row.leaseToken).toBeNull();
  });
});
