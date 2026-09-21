import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { Queue, Worker, type Job } from "bullmq";
import { eq } from "drizzle-orm";
import { jobs } from "@chronoqueue/db";
import { afterAll, describe, expect, it } from "vitest";
import { connection } from "./queue/connection.js";
import { db, pool } from "./db/client.js";
import { processWebhookDeliveryJob } from "./processor.js";
import { WEBHOOK_TIMEOUT_MS } from "./webhook-delivery.js";
import { claimJob, renewLease, completeProcessing } from "./lease.js";

const queue = new Queue("webhook-delivery", { connection });
const testWorker = new Worker("webhook-delivery", processWebhookDeliveryJob, {
  connection,
});

const insertedPostgresIds: string[] = [];
const serversToClose: Server[] = [];

afterAll(async () => {
  for (const id of insertedPostgresIds) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
  await Promise.all(
    serversToClose.map(
      (server) => new Promise<void>((resolve) => server.close(() => resolve())),
    ),
  );
  await testWorker.close();
  await queue.close();
  await connection.quit();
  await pool.end();
});

interface JobOutcome {
  status: "completed" | "failed";
  error?: Error;
}

const OUTCOME_WAIT_MS = WEBHOOK_TIMEOUT_MS + 5_000;

function waitForJobOutcome(bullJobId: string): Promise<JobOutcome> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(new Error(`Timed out waiting for job ${bullJobId} to settle`));
    }, OUTCOME_WAIT_MS);

    function onCompleted(job: Job) {
      if (job.id === bullJobId) {
        cleanup();
        resolve({ status: "completed" });
      }
    }

    function onFailed(job: Job | undefined, err: Error) {
      if (job?.id === bullJobId) {
        cleanup();
        resolve({ status: "failed", error: err });
      }
    }

    function cleanup() {
      clearTimeout(timeout);
      testWorker.off("completed", onCompleted);
      testWorker.off("failed", onFailed);
    }

    testWorker.on("completed", onCompleted);
    testWorker.on("failed", onFailed);
  });
}

interface CapturedRequest {
  method?: string;
  contentType?: string;
  body: unknown;
}

function startCapturingServer(
  responder: (req: CapturedRequest, res: ServerResponse) => void,
): Promise<{ server: Server; port: number }> {
  return new Promise((resolve) => {
    const server = createServer((req: IncomingMessage, res: ServerResponse) => {
      const chunks: Buffer[] = [];
      req.on("data", (chunk: Buffer) => chunks.push(chunk));
      req.on("end", () => {
        const rawBody = Buffer.concat(chunks).toString("utf8");
        let parsedBody: unknown;
        try {
          parsedBody = rawBody ? JSON.parse(rawBody) : undefined;
        } catch {
          parsedBody = rawBody;
        }
        responder(
          {
            method: req.method,
            contentType: req.headers["content-type"],
            body: parsedBody,
          },
          res,
        );
      });
    });

    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      resolve({ server, port });
    });
  });
}

async function insertPendingJob(options: {
  port: number;
  maxAttempts?: number;
  nextAttemptAt?: Date;
}): Promise<string> {
  const id = randomUUID();
  insertedPostgresIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: "PENDING",
    targetUrl: `http://127.0.0.1:${options.port}/webhook`,
    payload: { test: true },
    ...(options.maxAttempts ? { maxAttempts: options.maxAttempts } : {}),
    ...(options.nextAttemptAt ? { nextAttemptAt: options.nextAttemptAt } : {}),
  });
  return id;
}

async function fetchJob(id: string) {
  const [row] = await db.select().from(jobs).where(eq(jobs.id, id)).limit(1);
  return row;
}

describe("worker: processWebhookDeliveryJob (real Redis + real PostgreSQL + real HTTP)", () => {
  it("delivers the webhook over real HTTP and completes the BullMQ job", async () => {
    let captured: CapturedRequest | undefined;

    const { server, port } = await startCapturingServer((req, res) => {
      captured = req;
      res.writeHead(202, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    });
    serversToClose.push(server);

    const postgresJobId = randomUUID();
    insertedPostgresIds.push(postgresJobId);
    const testPayload = { event: "user.created", userId: "abc-123" };

    await db.insert(jobs).values({
      id: postgresJobId,
      idempotencyKey: randomUUID(),
      type: "WEBHOOK",
      status: "PENDING",
      targetUrl: `http://127.0.0.1:${port}/webhook`,
      payload: testPayload,
    });

    const outcome = waitForJobOutcome(postgresJobId);
    await queue.add(
      "deliver-webhook",
      { jobId: postgresJobId },
      { jobId: postgresJobId, removeOnComplete: true, removeOnFail: true },
    );

    const result = await outcome;

    expect(result.status).toBe("completed");
    expect(captured?.method).toBe("POST");
    expect(captured?.contentType).toBe("application/json");
    expect(captured?.body).toEqual(testPayload);
  });

  it("fails the BullMQ job when the webhook endpoint responds with HTTP 500", async () => {
    let requestReceived = false;

    const { server, port } = await startCapturingServer((_req, res) => {
      requestReceived = true;
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "internal" }));
    });
    serversToClose.push(server);

    const postgresJobId = randomUUID();
    insertedPostgresIds.push(postgresJobId);

    await db.insert(jobs).values({
      id: postgresJobId,
      idempotencyKey: randomUUID(),
      type: "WEBHOOK",
      status: "PENDING",
      targetUrl: `http://127.0.0.1:${port}/webhook`,
      payload: { test: true },
    });

    const outcome = waitForJobOutcome(postgresJobId);
    await queue.add(
      "deliver-webhook",
      { jobId: postgresJobId },
      { jobId: postgresJobId, removeOnComplete: true, removeOnFail: true },
    );

    const result = await outcome;

    expect(requestReceived).toBe(true);
    expect(result.status).toBe("failed");
    expect(result.error?.message).toContain("500");
  });

  it(
    "fails the BullMQ job when the webhook endpoint times out",
    async () => {
      let requestReceived = false;

      const { server, port } = await startCapturingServer((_req, _res) => {
        requestReceived = true;
        // Intentionally never call res.end() — simulates a hanging endpoint.
      });
      serversToClose.push(server);

      const postgresJobId = randomUUID();
      insertedPostgresIds.push(postgresJobId);

      await db.insert(jobs).values({
        id: postgresJobId,
        idempotencyKey: randomUUID(),
        type: "WEBHOOK",
        status: "PENDING",
        targetUrl: `http://127.0.0.1:${port}/webhook`,
        payload: { test: true },
      });

      const outcome = waitForJobOutcome(postgresJobId);
      await queue.add(
        "deliver-webhook",
        { jobId: postgresJobId },
        { jobId: postgresJobId, removeOnComplete: true, removeOnFail: true },
      );

      const result = await outcome;

      expect(requestReceived).toBe(true);
      expect(result.status).toBe("failed");
      expect(result.error?.message.toLowerCase()).toContain("timed out");
    },
    WEBHOOK_TIMEOUT_MS + 10_000,
  );

  it("fails the BullMQ job naturally when the referenced PostgreSQL job does not exist", async () => {
    const missingPostgresJobId = randomUUID();
    // intentionally not inserted into PostgreSQL

    const outcome = waitForJobOutcome(missingPostgresJobId);
    await queue.add(
      "deliver-webhook",
      { jobId: missingPostgresJobId },
      {
        jobId: missingPostgresJobId,
        removeOnComplete: true,
        removeOnFail: true,
      },
    );

    const result = await outcome;
    expect(result.status).toBe("failed");
    expect(result.error?.message).toContain(missingPostgresJobId);
  });
});

describe("worker: retry engine (real PostgreSQL state transitions)", () => {
  it("HTTP 500 is retryable: attempts increments, status becomes RETRYING", async () => {
    const { server, port } = await startCapturingServer((_req, res) => {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end();
    });
    serversToClose.push(server);

    const id = await insertPendingJob({ port });
    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );
    await outcome;

    const row = await fetchJob(id);
    expect(row.status).toBe("RETRYING");
    expect(row.attempts).toBe(1);
    expect(row.nextAttemptAt).not.toBeNull();
    expect(row.leaseUntil).toBeNull();
  });

  it("HTTP 503 is retryable: attempts increments, status becomes RETRYING", async () => {
    const { server, port } = await startCapturingServer((_req, res) => {
      res.writeHead(503, { "Content-Type": "application/json" });
      res.end();
    });
    serversToClose.push(server);

    const id = await insertPendingJob({ port });
    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );
    await outcome;

    const row = await fetchJob(id);
    expect(row.status).toBe("RETRYING");
    expect(row.attempts).toBe(1);
  });

  it("network/transport failure is retryable: attempts increments, status becomes RETRYING", async () => {
    const id = randomUUID();
    insertedPostgresIds.push(id);
    await db.insert(jobs).values({
      id,
      idempotencyKey: randomUUID(),
      type: "WEBHOOK",
      status: "PENDING",
      targetUrl: "http://127.0.0.1:1/unreachable",
      payload: { test: true },
    });

    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );
    await outcome;

    const row = await fetchJob(id);
    expect(row.status).toBe("RETRYING");
    expect(row.attempts).toBe(1);
  });

  it(
    "timeout is retryable: attempts increments, status becomes RETRYING",
    async () => {
      const { server, port } = await startCapturingServer((_req, _res) => {
        // Intentionally never respond.
      });
      serversToClose.push(server);

      const id = await insertPendingJob({ port });
      const outcome = waitForJobOutcome(id);
      await queue.add(
        "deliver-webhook",
        { jobId: id },
        { jobId: id, removeOnComplete: true, removeOnFail: true },
      );
      await outcome;

      const row = await fetchJob(id);
      expect(row.status).toBe("RETRYING");
      expect(row.attempts).toBe(1);
    },
    WEBHOOK_TIMEOUT_MS + 10_000,
  );

  it("HTTP 400 is non-retryable: attempts increments, status becomes DEAD", async () => {
    const { server, port } = await startCapturingServer((_req, res) => {
      res.writeHead(400, { "Content-Type": "application/json" });
      res.end();
    });
    serversToClose.push(server);

    const id = await insertPendingJob({
      port,
      nextAttemptAt: new Date(Date.now() + 60_000),
    });
    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );
    await outcome;

    const row = await fetchJob(id);
    expect(row.status).toBe("DEAD");
    expect(row.attempts).toBe(1);
    expect(row.nextAttemptAt).toBeNull();
    expect(row.leaseUntil).toBeNull();
  });

  it("HTTP 404 is non-retryable: attempts increments, status becomes DEAD", async () => {
    const { server, port } = await startCapturingServer((_req, res) => {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end();
    });
    serversToClose.push(server);

    const id = await insertPendingJob({ port });
    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );
    await outcome;

    const row = await fetchJob(id);
    expect(row.status).toBe("DEAD");
    expect(row.attempts).toBe(1);
  });

  it("retryable failure on the final allowed attempt marks the job DEAD (max attempts reached)", async () => {
    const { server, port } = await startCapturingServer((_req, res) => {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end();
    });
    serversToClose.push(server);

    const id = await insertPendingJob({
      port,
      maxAttempts: 1,
      nextAttemptAt: new Date(Date.now() + 60_000),
    });
    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );
    await outcome;

    const row = await fetchJob(id);
    expect(row.status).toBe("DEAD");
    expect(row.attempts).toBe(1);
    expect(row.nextAttemptAt).toBeNull();
  });

  it("successful 2xx increments attempts exactly once and marks the job SUCCEEDED", async () => {
    const { server, port } = await startCapturingServer((_req, res) => {
      res.writeHead(202, { "Content-Type": "application/json" });
      res.end();
    });
    serversToClose.push(server);

    const id = await insertPendingJob({
      port,
      nextAttemptAt: new Date(Date.now() + 60_000),
    });
    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );
    await outcome;

    const row = await fetchJob(id);
    expect(row.status).toBe("SUCCEEDED");
    expect(row.attempts).toBe(1);
    expect(row.nextAttemptAt).toBeNull();
    expect(row.leaseUntil).toBeNull();
  });
});

describe("worker: concurrency safety (real PostgreSQL claim guard)", () => {
  it("only one of two concurrent processing attempts for the same job reaches webhook execution", async () => {
    let hitCount = 0;

    const { server, port } = await startCapturingServer((_req, res) => {
      hitCount += 1;
      res.writeHead(202, { "Content-Type": "application/json" });
      res.end();
    });
    serversToClose.push(server);

    const id = randomUUID();
    insertedPostgresIds.push(id);
    await db.insert(jobs).values({
      id,
      idempotencyKey: randomUUID(),
      type: "WEBHOOK",
      status: "PENDING",
      targetUrl: `http://127.0.0.1:${port}/webhook`,
      payload: { test: true },
    });

    // Two logical processing attempts referencing the SAME PostgreSQL job
    // (e.g. a duplicate/replayed BullMQ delivery). BullMQ itself already
    // guarantees a single job id is only dispatched to one worker at a
    // time, so calling the processor directly with two distinct fake
    // BullMQ Job objects is what actually exercises the invariant Day 12
    // cares about: the PostgreSQL claim guard, not BullMQ's own dispatch.
    const fakeJobA = {
      id: "concurrency-test-a",
      data: { jobId: id },
    } as unknown as Job;
    const fakeJobB = {
      id: "concurrency-test-b",
      data: { jobId: id },
    } as unknown as Job;

    const [resultA, resultB] = await Promise.allSettled([
      processWebhookDeliveryJob(fakeJobA),
      processWebhookDeliveryJob(fakeJobB),
    ]);

    const outcomes = [resultA, resultB];
    const fulfilled = outcomes.filter((r) => r.status === "fulfilled");
    const rejected = outcomes.filter((r) => r.status === "rejected");

    expect(fulfilled.length).toBe(1);
    expect(rejected.length).toBe(1);
    expect((rejected[0] as PromiseRejectedResult).reason.message).toContain(
      "could not be claimed",
    );

    // The critical assertion: the losing attempt must never have reached
    // deliverWebhook — the local server should have been hit exactly once.
    expect(hitCount).toBe(1);

    const row = await fetchJob(id);
    expect(row.status).toBe("SUCCEEDED");
    expect(row.attempts).toBe(1);
  });
});

describe("worker: lease (real PostgreSQL)", () => {
  it("claim sets leaseUntil to a future timestamp", async () => {
    const { server, port } = await startCapturingServer((_req, res) => {
      setTimeout(() => {
        res.writeHead(202, { "Content-Type": "application/json" });
        res.end();
      }, 300);
    });
    serversToClose.push(server);

    const id = await insertPendingJob({ port });

    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );

    // Peek mid-flight, before the response is sent — claim should have
    // already set a future leaseUntil.
    await new Promise((r) => setTimeout(r, 100));
    const midFlight = await fetchJob(id);
    expect(midFlight.status).toBe("PROCESSING");
    expect(midFlight.leaseUntil).not.toBeNull();
    expect(midFlight.leaseUntil!.getTime()).toBeGreaterThan(Date.now());

    await outcome;
  });

  it("renews the lease while processing is still in progress", async () => {
    const { server, port } = await startCapturingServer((_req, res) => {
      setTimeout(() => {
        res.writeHead(202, { "Content-Type": "application/json" });
        res.end();
      }, 900);
    });
    serversToClose.push(server);

    const id = await insertPendingJob({ port });

    const outcome = waitForJobOutcome(id);
    await queue.add(
      "deliver-webhook",
      { jobId: id },
      { jobId: id, removeOnComplete: true, removeOnFail: true },
    );

    await new Promise((r) => setTimeout(r, 150));
    const early = await fetchJob(id);
    expect(early.status).toBe("PROCESSING");
    expect(early.leaseUntil).not.toBeNull();

    await new Promise((r) => setTimeout(r, 500));
    const later = await fetchJob(id);
    expect(later.status).toBe("PROCESSING");
    expect(later.leaseUntil).not.toBeNull();
    // The critical assertion: leaseUntil actually moved forward, proving
    // renewal happened (not just that claim set it once).
    expect(later.leaseUntil!.getTime()).toBeGreaterThan(
      early.leaseUntil!.getTime(),
    );

    await outcome;

    const final = await fetchJob(id);
    expect(final.status).toBe("SUCCEEDED");
    expect(final.leaseUntil).toBeNull();
  });
});

describe("worker: lease fencing (stale worker cannot affect a newer processing attempt)", () => {
  it("a stale worker's token cannot renew, succeed, retry, or dead a newer worker's processing attempt", async () => {
    const id = randomUUID();
    insertedPostgresIds.push(id);

    // Simulate: Worker A claimed this job earlier and went stale
    // (crashed/paused). Scheduler recovery would have already run,
    // transitioning it to RETRYING and clearing the lease fields —
    // replicate that end state directly, since this test is Worker-scoped.
    await db.insert(jobs).values({
      id,
      idempotencyKey: randomUUID(),
      type: "WEBHOOK",
      status: "RETRYING",
      targetUrl: "http://127.0.0.1:9/unused",
      payload: { test: true },
      leaseUntil: null,
      leaseToken: null,
    });

    const staleToken = "stale-token-AAA";

    // Worker B claims it for real, via the actual production claim path.
    const claimResult = await claimJob(id);
    expect(claimResult).not.toBeNull();
    const { leaseToken: tokenB } = claimResult!;
    expect(tokenB).not.toBe(staleToken);

    // --- Test A: stale renewal must fail, and not touch Worker B's lease ---
    const staleRenewResult = await renewLease(id, staleToken);
    expect(staleRenewResult).toBe(false);

    const afterStaleRenew = await fetchJob(id);
    expect(afterStaleRenew.status).toBe("PROCESSING");
    expect(afterStaleRenew.leaseToken).toBe(tokenB);

    // --- Test B: stale success must fail, job stays PROCESSING under B ---
    const staleSucceedResult = await completeProcessing(id, staleToken, {
      status: "SUCCEEDED",
      attempts: 99,
      nextAttemptAt: null,
    });
    expect(staleSucceedResult).toBe(false);

    const afterStaleSucceed = await fetchJob(id);
    expect(afterStaleSucceed.status).toBe("PROCESSING");
    expect(afterStaleSucceed.leaseToken).toBe(tokenB);
    expect(afterStaleSucceed.attempts).not.toBe(99);

    // --- Test C: stale retry and stale dead must also both fail ---
    const staleRetryResult = await completeProcessing(id, staleToken, {
      status: "RETRYING",
      attempts: 99,
      nextAttemptAt: new Date(),
    });
    expect(staleRetryResult).toBe(false);

    const staleDeadResult = await completeProcessing(id, staleToken, {
      status: "DEAD",
      attempts: 99,
      nextAttemptAt: null,
    });
    expect(staleDeadResult).toBe(false);

    const afterAllStaleAttempts = await fetchJob(id);
    expect(afterAllStaleAttempts.status).toBe("PROCESSING");
    expect(afterAllStaleAttempts.leaseToken).toBe(tokenB);
    expect(afterAllStaleAttempts.attempts).toBe(0);

    // --- Confirm Worker B's OWN token still legitimately works ---
    const legitRenew = await renewLease(id, tokenB);
    expect(legitRenew).toBe(true);

    const legitComplete = await completeProcessing(id, tokenB, {
      status: "SUCCEEDED",
      attempts: 1,
      nextAttemptAt: null,
    });
    expect(legitComplete).toBe(true);

    const finalRow = await fetchJob(id);
    expect(finalRow.status).toBe("SUCCEEDED");
    expect(finalRow.leaseToken).toBeNull();
    expect(finalRow.leaseUntil).toBeNull();
    expect(finalRow.attempts).toBe(1);
  });
});
