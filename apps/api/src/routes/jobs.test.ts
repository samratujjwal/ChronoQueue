import { afterAll, describe, expect, it } from "vitest";
import { buildApp } from "../app.js";
import { db } from "../db/client.js";
import { jobs } from "../db/schema/jobs.js";
import { eq } from "drizzle-orm";

const app = buildApp();
const insertedIds: string[] = [];

afterAll(async () => {
  for (const id of insertedIds) {
    await db.delete(jobs).where(eq(jobs.id, id));
  }
  await app.close();
});

describe("POST /jobs — validation", () => {
  it("rejects missing type", async () => {
    const res = await app.inject({
      method: "POST",
      url: "/jobs",
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
