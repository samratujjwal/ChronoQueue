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
        // Intentionally never call res.end() — simulates a hanging endpoint.
      });
      serversToClose.push(server);

      const postgresJobId = randomUUID();
      insertedPostgresIds.push(postgresJobId);

      await db.insert(jobs).values({
        id: postgresJobId,
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
  });
});
