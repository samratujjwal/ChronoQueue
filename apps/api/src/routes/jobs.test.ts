import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { buildApp } from "../app.js";
import { db } from "../db/client.js";
import { jobs } from "@chronoqueue/db";
import { eq } from "drizzle-orm";

const app = buildApp();
const insertedIds: string[] = [];

afterAll(async () => {
  for (const id of insertedIds) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
  await app.close();
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
