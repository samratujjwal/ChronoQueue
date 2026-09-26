import { eq, inArray } from "drizzle-orm";
import { drizzle } from "drizzle-orm/node-postgres";
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import { jobs } from "@chronoqueue/db";
import { cleanupSeededJobs, seedJobs } from "./seed.js";

const DATABASE_URL = process.env.DATABASE_URL;
if (!DATABASE_URL) {
  throw new Error("DATABASE_URL must be set to run seed helper tests");
}

const pool = new Pool({ connectionString: DATABASE_URL });
const db = drizzle(pool);

afterAll(async () => {
  await pool.end();
});

async function countByIds(ids: string[]): Promise<number> {
  if (ids.length === 0) {
    return 0;
  }
  const rows = await db
    .select({ id: jobs.id })
    .from(jobs)
    .where(inArray(jobs.id, ids));
  return rows.length;
}

describe("seedJobs", () => {
  it("inserts the requested number of jobs and returns their ids", async () => {
    const ids = await seedJobs(db, { count: 25, status: "PENDING" });
    try {
      expect(ids).toHaveLength(25);
      expect(new Set(ids).size).toBe(25);
      expect(await countByIds(ids)).toBe(25);

      const rows = await db
        .select({ status: jobs.status, maxAttempts: jobs.maxAttempts })
        .from(jobs)
        .where(inArray(jobs.id, ids));
      for (const row of rows) {
        expect(row.status).toBe("PENDING");
        expect(row.maxAttempts).toBe(5);
      }
    } finally {
      await cleanupSeededJobs(db, ids);
    }
    expect(await countByIds(ids)).toBe(0);
  });

  it("supports a scheduledAt function for staggered jobs", async () => {
    const base = Date.now();
    const ids = await seedJobs(db, {
      count: 5,
      scheduledAt: (i) => new Date(base + i * 60000),
    });
    try {
      const rows = await db
        .select({ scheduledAt: jobs.scheduledAt })
        .from(jobs)
        .where(inArray(jobs.id, ids));
      const times = rows
        .map((r) => r.scheduledAt.getTime())
        .sort((a, b) => a - b);
      for (let i = 1; i < times.length; i += 1) {
        expect(times[i]! - times[i - 1]!).toBeGreaterThanOrEqual(59000);
      }
    } finally {
      await cleanupSeededJobs(db, ids);
    }
  });

  it("honours custom status, targetUrl and maxAttempts", async () => {
    const ids = await seedJobs(db, {
      count: 3,
      status: "QUEUED",
      targetUrl: "http://127.0.0.1:9/custom",
      maxAttempts: 2,
    });
    try {
      const rows = await db
        .select({
          status: jobs.status,
          targetUrl: jobs.targetUrl,
          maxAttempts: jobs.maxAttempts,
        })
        .from(jobs)
        .where(inArray(jobs.id, ids));
      expect(rows).toHaveLength(3);
      for (const row of rows) {
        expect(row.status).toBe("QUEUED");
        expect(row.targetUrl).toBe("http://127.0.0.1:9/custom");
        expect(row.maxAttempts).toBe(2);
      }
    } finally {
      await cleanupSeededJobs(db, ids);
    }
  });
});

describe("cleanupSeededJobs", () => {
  it("removes only the given ids and leaves other rows alone", async () => {
    const batchA = await seedJobs(db, {
      count: 10,
      idempotencyKeyPrefix: "cleanup-a",
    });
    const batchB = await seedJobs(db, {
      count: 10,
      idempotencyKeyPrefix: "cleanup-b",
    });
    try {
      await cleanupSeededJobs(db, batchA);
      expect(await countByIds(batchA)).toBe(0);
      expect(await countByIds(batchB)).toBe(10);

      const leftover = await db
        .select({ id: jobs.id })
        .from(jobs)
        .where(eq(jobs.idempotencyKey, "definitely-not-a-seed-key"));
      expect(leftover).toHaveLength(0);
    } finally {
      await cleanupSeededJobs(db, batchA);
      await cleanupSeededJobs(db, batchB);
    }
    expect(await countByIds(batchB)).toBe(0);
  });

  it("is a no-op for an empty id list", async () => {
    await expect(cleanupSeededJobs(db, [])).resolves.toBeUndefined();
  });
});
