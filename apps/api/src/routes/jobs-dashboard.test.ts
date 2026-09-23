import { randomUUID } from "node:crypto";
import { afterAll, afterEach, describe, expect, it } from "vitest";
import { eq } from "drizzle-orm";
import { buildApp } from "../app.js";
import { db } from "../db/client.js";
import {
  DASHBOARD_STATUSES,
  getJobStats,
  jobs,
  listJobs,
  type JobStatus,
} from "@chronoqueue/db";
import { webhookQueue } from "../queue/webhook-queue.js";
import { connection as redisConnection } from "../queue/connection.js";

const app = buildApp();
const insertedIds: string[] = [];
const bullJobIdsToClean: string[] = [];

afterEach(async () => {
  for (const id of bullJobIdsToClean.splice(0)) {
    const bullJob = await webhookQueue.getJob(id);
    await bullJob?.remove();
  }
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
  updatedAt?: Date;
  attempts?: number;
  maxAttempts?: number;
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
    maxAttempts: options.maxAttempts ?? 5,
    lastErrorCode: options.lastErrorCode ?? null,
    lastErrorMessage: options.lastErrorCode ? "simulated failure" : null,
    ...(options.updatedAt ? { updatedAt: options.updatedAt } : {}),
  });
  return id;
}

async function getStatsTotal(): Promise<number> {
  const res = await app.inject({ method: "GET", url: "/jobs/stats" });
  expect(res.statusCode).toBe(200);
  return res.json().total as number;
}

describe("GET /jobs — dashboard listing (real PostgreSQL)", () => {
  it("applies default pagination", async () => {
    const before = await getStatsTotal();
    for (let i = 0; i < 25; i++) {
      await insertJob({ status: "PENDING" });
    }

    const res = await app.inject({ method: "GET", url: "/jobs" });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.page).toBe(1);
    expect(body.pageSize).toBe(20);
    expect(body.total).toBe(before + 25);
    expect(body.totalPages).toBe(Math.ceil((before + 25) / 20));
    expect(body.jobs).toHaveLength(20);
  });

  it("supports custom page and pageSize", async () => {
    const before = await getStatsTotal();
    for (let i = 0; i < 25; i++) {
      await insertJob({ status: "QUEUED" });
    }

    const res = await app.inject({
      method: "GET",
      url: "/jobs?page=2&pageSize=10",
    });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.page).toBe(2);
    expect(body.pageSize).toBe(10);
    expect(body.total).toBe(before + 25);
    expect(body.totalPages).toBe(Math.ceil((before + 25) / 10));
    expect(body.jobs).toHaveLength(10);
  });

  it("rejects pageSize above the 100 cap", async () => {
    for (const url of ["/jobs?pageSize=101", "/jobs?pageSize=1000"]) {
      const res = await app.inject({ method: "GET", url });
      expect(res.statusCode).toBe(400);
    }
  });

  it("rejects invalid page values", async () => {
    for (const url of ["/jobs?page=0", "/jobs?page=-2", "/jobs?page=abc"]) {
      const res = await app.inject({ method: "GET", url });
      expect(res.statusCode).toBe(400);
    }
  });

  it("rejects invalid pageSize values", async () => {
    for (const url of [
      "/jobs?pageSize=0",
      "/jobs?pageSize=-5",
      "/jobs?pageSize=abc",
    ]) {
      const res = await app.inject({ method: "GET", url });
      expect(res.statusCode).toBe(400);
    }
  });

  it("rejects an invalid status with 400", async () => {
    const res = await app.inject({ method: "GET", url: "/jobs?status=BOGUS" });
    expect(res.statusCode).toBe(400);
  });

  it("filters by a valid status", async () => {
    const deadBefore = (
      await app.inject({ method: "GET", url: "/jobs?status=DEAD" })
    ).json().total as number;

    const deadA = await insertJob({
      status: "DEAD",
      lastErrorCode: "HTTP_500",
    });
    const deadB = await insertJob({ status: "DEAD", lastErrorCode: "TIMEOUT" });
    await insertJob({ status: "PENDING" });

    const res = await app.inject({ method: "GET", url: "/jobs?status=DEAD" });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.total).toBe(deadBefore + 2);
    expect(body.jobs.map((j: { id: string }) => j.id)).toEqual(
      expect.arrayContaining([deadA, deadB]),
    );
    for (const job of body.jobs) {
      expect(job.status).toBe("DEAD");
    }
  });

  it("computes total and totalPages correctly", async () => {
    const before = await getStatsTotal();
    for (let i = 0; i < 5; i++) {
      await insertJob({ status: "SUCCEEDED" });
    }

    const res = await app.inject({ method: "GET", url: "/jobs?pageSize=2" });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    const total = before + 5;
    expect(body.total).toBe(total);
    expect(body.totalPages).toBe(Math.ceil(total / 2));

    const lastPage = await app.inject({
      method: "GET",
      url: `/jobs?pageSize=2&page=${Math.ceil(total / 2)}`,
    });
    expect(lastPage.statusCode).toBe(200);
    expect(lastPage.json().jobs.length).toBeLessThanOrEqual(2);
  });

  it("orders deterministically: newest updatedAt first, id as tie-break", async () => {
    const base = Date.now() - 120_000;
    const oldest = await insertJob({
      status: "PROCESSING",
      updatedAt: new Date(base),
    });
    const middle = await insertJob({
      status: "PROCESSING",
      updatedAt: new Date(base + 1_000),
    });
    const newest = await insertJob({
      status: "PROCESSING",
      updatedAt: new Date(base + 2_000),
    });

    const res = await app.inject({
      method: "GET",
      url: "/jobs?pageSize=100",
    });
    expect(res.statusCode).toBe(200);
    const ids = res.json().jobs.map((j: { id: string }) => j.id) as string[];
    const positions = [newest, middle, oldest].map((id) => ids.indexOf(id));
    expect(positions[0]).toBeGreaterThanOrEqual(0);
    expect(positions[1]).toBeGreaterThanOrEqual(0);
    expect(positions[2]).toBeGreaterThanOrEqual(0);
    expect(positions[0]).toBeLessThan(positions[1]);
    expect(positions[1]).toBeLessThan(positions[2]);

    const sameTs = new Date(base + 60_000);
    const tieA = await insertJob({ status: "RETRYING", updatedAt: sameTs });
    const tieB = await insertJob({ status: "RETRYING", updatedAt: sameTs });
    const expectedFirst = [tieA, tieB].sort().reverse()[0];

    const res2 = await app.inject({ method: "GET", url: "/jobs?pageSize=100" });
    expect(res2.statusCode).toBe(200);
    const ids2 = res2.json().jobs.map((j: { id: string }) => j.id) as string[];
    const posA = ids2.indexOf(tieA);
    const posB = ids2.indexOf(tieB);
    expect(Math.min(posA, posB)).toBeGreaterThanOrEqual(0);
    expect(ids2[Math.min(posA, posB)]).toBe(expectedFirst);

    const repeat = await app.inject({
      method: "GET",
      url: "/jobs?pageSize=100",
    });
    expect(repeat.json().jobs.map((j: { id: string }) => j.id)).toEqual(ids2);
  });

  it("exposes only the lightweight list columns", async () => {
    await insertJob({ status: "PENDING" });

    const res = await app.inject({ method: "GET", url: "/jobs?pageSize=1" });
    expect(res.statusCode).toBe(200);
    const job = res.json().jobs[0];

    expect(Object.keys(job).sort()).toEqual(
      [
        "attempts",
        "createdAt",
        "id",
        "lastErrorCode",
        "maxAttempts",
        "nextAttemptAt",
        "scheduledAt",
        "status",
        "type",
        "updatedAt",
      ].sort(),
    );
    for (const forbidden of [
      "idempotencyKey",
      "leaseToken",
      "leaseUntil",
      "payload",
      "targetUrl",
    ]) {
      expect(job).not.toHaveProperty(forbidden);
    }
  });
});

describe("GET /jobs/stats — dashboard stats (real PostgreSQL)", () => {
  it("returns correct counts per status", async () => {
    const before = (
      await app.inject({ method: "GET", url: "/jobs/stats" })
    ).json() as { total: number; byStatus: Record<JobStatus, number> };

    await insertJob({ status: "PENDING" });
    await insertJob({ status: "PENDING" });
    await insertJob({ status: "QUEUED" });
    await insertJob({ status: "DEAD", lastErrorCode: "HTTP_500" });

    const res = await app.inject({ method: "GET", url: "/jobs/stats" });
    expect(res.statusCode).toBe(200);
    const body = res.json() as {
      total: number;
      byStatus: Record<JobStatus, number>;
    };
    expect(body.total).toBe(before.total + 4);
    expect(body.byStatus.PENDING).toBe(before.byStatus.PENDING + 2);
    expect(body.byStatus.QUEUED).toBe(before.byStatus.QUEUED + 1);
    expect(body.byStatus.DEAD).toBe(before.byStatus.DEAD + 1);
  });

  it("always returns all six statuses", async () => {
    const res = await app.inject({ method: "GET", url: "/jobs/stats" });
    expect(res.statusCode).toBe(200);
    const body = res.json() as {
      total: number;
      byStatus: Record<JobStatus, number>;
    };
    expect(Object.keys(body).sort()).toEqual(["byStatus", "total"]);
    expect(Object.keys(body.byStatus).sort()).toEqual(
      [...DASHBOARD_STATUSES].sort(),
    );
    for (const status of DASHBOARD_STATUSES) {
      expect(typeof body.byStatus[status]).toBe("number");
    }
    const sum = DASHBOARD_STATUSES.reduce(
      (acc, s) => acc + body.byStatus[s],
      0,
    );
    expect(body.total).toBe(sum);
  });

  it("returns zeros on an empty database", async () => {
    let assertionsPassed = false;
    await db
      .transaction(async (tx) => {
        await tx.delete(jobs);

        const stats = await getJobStats(tx);
        expect(stats.total).toBe(0);
        for (const status of DASHBOARD_STATUSES) {
          expect(stats.byStatus[status]).toBe(0);
        }

        const page = await listJobs(tx, {});
        expect(page.jobs).toEqual([]);
        expect(page.total).toBe(0);
        expect(page.totalPages).toBe(0);

        assertionsPassed = true;
        tx.rollback();
      })
      .catch(() => {});
    expect(assertionsPassed).toBe(true);
  });
});

describe("existing job endpoints remain intact", () => {
  it("GET /jobs/dead still lists dead jobs", async () => {
    const id = await insertJob({ status: "DEAD", lastErrorCode: "HTTP_500" });

    const res = await app.inject({ method: "GET", url: "/jobs/dead" });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.jobs.map((j: { id: string }) => j.id)).toContain(id);
    const row = body.jobs.find((j: { id: string }) => j.id === id);
    expect(row.lastErrorCode).toBe("HTTP_500");
    expect(row).toHaveProperty("payload");
    expect(row).toHaveProperty("targetUrl");
  });

  it("GET /jobs/:id still returns the full diagnostic row", async () => {
    const id = await insertJob({ status: "DEAD", lastErrorCode: "TIMEOUT" });

    const res = await app.inject({ method: "GET", url: `/jobs/${id}` });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.id).toBe(id);
    expect(body.status).toBe("DEAD");
    expect(body).toHaveProperty("payload");
    expect(body).toHaveProperty("targetUrl");
    expect(body).toHaveProperty("lastErrorMessage");
  });

  it("POST /jobs/:id/retry still re-triggers a dead job", async () => {
    const id = await insertJob({
      status: "DEAD",
      attempts: 2,
      maxAttempts: 5,
      lastErrorCode: "NETWORK_ERROR",
    });
    bullJobIdsToClean.push(id);

    const res = await app.inject({ method: "POST", url: `/jobs/${id}/retry` });
    expect(res.statusCode).toBe(200);
    const body = res.json();
    expect(body.id).toBe(id);
    expect(body.status).toBe("QUEUED");
    expect(body.attempts).toBe(2);
  });
});
