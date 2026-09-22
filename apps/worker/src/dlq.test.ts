import { randomUUID } from "node:crypto";
import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { Queue, Worker, type Job } from "bullmq";
import { eq } from "drizzle-orm";
import { jobs, retriggerDeadJob } from "@chronoqueue/db";
import { afterAll, describe, expect, it } from "vitest";
import { connection } from "./queue/connection.js";
import { db, pool } from "./db/client.js";
import { processWebhookDeliveryJob } from "./processor.js";
import { claimJob, completeProcessing, renewLease } from "./lease.js";

const queue = new Queue("webhook-delivery", { connection });
const testWorker = new Worker("webhook-delivery", processWebhookDeliveryJob, {
  connection,
});

const insertedPostgresIds: string[] = [];
const serversToClose: Server[] = [];

afterAll(async () => {
  for (const id of insertedPostgresIds) {
    await db.delete(jobs).where(eq(jobs.id, id));
    const bullJob = await queue.getJob(id);
    await bullJob?.remove();
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

function startFailingServer(statusCode: number): Promise<{
  server: Server;
  port: number;
}> {
  return new Promise((resolve) => {
    const server = createServer(
      (_req: IncomingMessage, res: ServerResponse) => {
        res.writeHead(statusCode, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "boom" }));
      },
    );
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      const port = typeof address === "object" && address ? address.port : 0;
      resolve({ server, port });
    });
  });
}

async function fetchJob(id: string) {
  const [row] = await db.select().from(jobs).where(eq(jobs.id, id)).limit(1);
  return row;
}

async function insertDeadJob(
  overrides: {
    attempts?: number;
    maxAttempts?: number;
    updatedAt?: Date;
  } = {},
): Promise<string> {
  const id = randomUUID();
  insertedPostgresIds.push(id);
  await db.insert(jobs).values({
    id,
    idempotencyKey: randomUUID(),
    type: "WEBHOOK",
    status: "DEAD",
    targetUrl: "http://127.0.0.1:9/unused",
    payload: { test: true },
    attempts: overrides.attempts ?? 5,
    maxAttempts: overrides.maxAttempts ?? 5,
    lastErrorCode: "HTTP_500",
    lastErrorMessage: "Webhook responded with HTTP 500",
    leaseUntil: null,
    leaseToken: null,
    nextAttemptAt: null,
    ...(overrides.updatedAt ? { updatedAt: overrides.updatedAt } : {}),
  });
  return id;
}

describe("dlq: DEAD job creation (real PostgreSQL + real HTTP)", () => {
  it("permanent failure (HTTP 500, maxAttempts=1) lands the job in DEAD with lease cleared and failure diagnosis persisted", async () => {
    const { server, port } = await startFailingServer(500);
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
      maxAttempts: 1,
    });

    // Invoke the production processor directly (the existing suite covers
    // the full BullMQ Worker path); it must throw after recording DEAD.
    const fakeBullJob = {
      id: `bull-${id}`,
      data: { jobId: id },
    } as unknown as Job;
    await expect(processWebhookDeliveryJob(fakeBullJob)).rejects.toThrow(/500/);

    const row = await fetchJob(id);
    expect(row.status).toBe("DEAD");
    expect(row.attempts).toBe(1);
    expect(row.leaseUntil).toBeNull();
    expect(row.leaseToken).toBeNull();
    expect(row.nextAttemptAt).toBeNull();
    expect(row.lastErrorCode).toBe("HTTP_500");
    expect(row.lastErrorMessage).toContain("500");
  });

  it("a successful delivery clears any failure diagnosis left by an earlier death", async () => {
    const { server, port } = await startFailingServer(500);
    serversToClose.push(server);

    // First death: HTTP 500 with maxAttempts=1.
    const id = randomUUID();
    insertedPostgresIds.push(id);
    await db.insert(jobs).values({
      id,
      idempotencyKey: randomUUID(),
      type: "WEBHOOK",
      status: "PENDING",
      targetUrl: `http://127.0.0.1:${port}/webhook`,
      payload: { test: true },
      maxAttempts: 1,
    });
    const dyingJob = {
      id: `bull-die-${id}`,
      data: { jobId: id },
    } as unknown as Job;
    await expect(processWebhookDeliveryJob(dyingJob)).rejects.toThrow();
    expect((await fetchJob(id)).lastErrorCode).toBe("HTTP_500");

    // Manual re-trigger, then point the job at a healthy endpoint and run
    // the processor again (it claims via the production path internally).
    const retriggered = await retriggerDeadJob(db, id);
    expect(retriggered.ok).toBe(true);

    const healthy = createServer(
      (_req: IncomingMessage, res: ServerResponse) => {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ ok: true }));
      },
    );
    serversToClose.push(healthy);
    await new Promise<void>((resolve) =>
      healthy.listen(0, "127.0.0.1", () => resolve()),
    );
    const healthyPort = (healthy.address() as { port: number }).port;
    await db
      .update(jobs)
      .set({ targetUrl: `http://127.0.0.1:${healthyPort}/webhook` })
      .where(eq(jobs.id, id));

    const succeedingJob = {
      id: `bull-ok-${id}`,
      data: { jobId: id },
    } as unknown as Job;
    await processWebhookDeliveryJob(succeedingJob);

    const row = await fetchJob(id);
    expect(row.status).toBe("SUCCEEDED");
    expect(row.lastErrorCode).toBeNull();
    expect(row.lastErrorMessage).toBeNull();
  });
});

describe("dlq: re-triggered execution gets a NEW leaseToken (real PostgreSQL)", () => {
  it("DEAD -> QUEUED -> PROCESSING mints a fresh token; attempts are preserved, not reset", async () => {
    const id = await insertDeadJob({ attempts: 5, maxAttempts: 5 });

    const retriggered = await retriggerDeadJob(db, id);
    expect(retriggered.ok).toBe(true);
    if (!retriggered.ok) return;

    expect(retriggered.job.status).toBe("QUEUED");
    // Historical attempt information is preserved.
    expect(retriggered.job.attempts).toBe(5);

    const afterRetrigger = await fetchJob(id);
    expect(afterRetrigger.status).toBe("QUEUED");
    expect(afterRetrigger.attempts).toBe(5);
    // No lease is created or reused by the re-trigger itself.
    expect(afterRetrigger.leaseToken).toBeNull();
    expect(afterRetrigger.leaseUntil).toBeNull();
    expect(afterRetrigger.nextAttemptAt).toBeNull();

    // The normal Worker claim path takes over with a NEW token.
    const claim = await claimJob(id);
    expect(claim).not.toBeNull();
    const tokenB = claim!.leaseToken;
    expect(typeof tokenB).toBe("string");
    expect(tokenB.length).toBeGreaterThan(0);

    const afterClaim = await fetchJob(id);
    expect(afterClaim.status).toBe("PROCESSING");
    expect(afterClaim.leaseToken).toBe(tokenB);
    expect(afterClaim.leaseUntil).not.toBeNull();
  });
});

describe("dlq: fencing regression — a stale pre-death token cannot mutate the re-triggered attempt", () => {
  it("token A (genuinely minted by an earlier attempt) cannot renew or complete the attempt owned by token B", async () => {
    // Earlier attempt: the job is QUEUED and claimed via the PRODUCTION
    // path, so token A is genuinely minted by claimJob — not fabricated.
    const id = randomUUID();
    insertedPostgresIds.push(id);
    await db.insert(jobs).values({
      id,
      idempotencyKey: randomUUID(),
      type: "WEBHOOK",
      status: "QUEUED",
      targetUrl: "http://127.0.0.1:9/unused",
      payload: { test: true },
      attempts: 5,
      maxAttempts: 5,
    });

    const claimA = await claimJob(id);
    expect(claimA).not.toBeNull();
    const staleTokenA = claimA!.leaseToken;

    // The earlier attempt dies via the production fenced path; the row is
    // DEAD with lease fields cleared, exactly as the real Worker leaves it.
    expect(
      await completeProcessing(id, staleTokenA, {
        status: "DEAD",
        attempts: 5,
        nextAttemptAt: null,
        lastErrorCode: "HTTP_500",
        lastErrorMessage: "boom",
      }),
    ).toBe(true);

    let row = await fetchJob(id);
    expect(row.status).toBe("DEAD");
    expect(row.leaseToken).toBeNull();
    expect(row.leaseUntil).toBeNull();

    // Operator re-triggers; Worker B claims via the production path.
    const retriggered = await retriggerDeadJob(db, id);
    expect(retriggered.ok).toBe(true);

    const claim = await claimJob(id);
    expect(claim).not.toBeNull();
    const tokenB = claim!.leaseToken;
    expect(tokenB).not.toBe(staleTokenA);

    // Stale renewal must fail and leave B's lease untouched.
    expect(await renewLease(id, staleTokenA)).toBe(false);
    row = await fetchJob(id);
    expect(row.status).toBe("PROCESSING");
    expect(row.leaseToken).toBe(tokenB);

    // Stale terminal writes must all fail.
    expect(
      await completeProcessing(id, staleTokenA, {
        status: "SUCCEEDED",
        attempts: 99,
        nextAttemptAt: null,
      }),
    ).toBe(false);
    expect(
      await completeProcessing(id, staleTokenA, {
        status: "RETRYING",
        attempts: 99,
        nextAttemptAt: new Date(),
      }),
    ).toBe(false);
    expect(
      await completeProcessing(id, staleTokenA, {
        status: "DEAD",
        attempts: 99,
        nextAttemptAt: null,
      }),
    ).toBe(false);

    row = await fetchJob(id);
    expect(row.status).toBe("PROCESSING");
    expect(row.leaseToken).toBe(tokenB);
    expect(row.attempts).toBe(5);

    // Worker B's own token still works.
    expect(await renewLease(id, tokenB)).toBe(true);
    expect(
      await completeProcessing(id, tokenB, {
        status: "SUCCEEDED",
        attempts: 6,
        nextAttemptAt: null,
      }),
    ).toBe(true);

    row = await fetchJob(id);
    expect(row.status).toBe("SUCCEEDED");
    expect(row.leaseToken).toBeNull();
  });
});
