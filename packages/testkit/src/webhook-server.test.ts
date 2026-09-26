import { describe, expect, it } from "vitest";
import { startWebhookServer } from "./webhook-server.js";

describe("startWebhookServer", () => {
  it("applies the configured response status", async () => {
    const server = await startWebhookServer({ status: 201 });
    try {
      const res = await fetch(server.url, { method: "POST" });
      await res.arrayBuffer();
      expect(res.status).toBe(201);
    } finally {
      await server.close();
    }
  });

  it("applies artificial latency before responding", async () => {
    const server = await startWebhookServer({ latencyMs: 300 });
    try {
      const startedAt = performance.now();
      const res = await fetch(server.url);
      await res.arrayBuffer();
      const elapsed = performance.now() - startedAt;
      expect(res.status).toBe(200);
      expect(elapsed).toBeGreaterThanOrEqual(250);
    } finally {
      await server.close();
    }
  });

  it("failure rate is statistically reasonable without exact assertions", async () => {
    const server = await startWebhookServer({
      failureRate: 0.5,
      failureStatus: 500,
    });
    try {
      let failures = 0;
      const total = 400;
      for (let i = 0; i < total; i += 1) {
        const res = await fetch(server.url);
        await res.arrayBuffer();
        if (res.status === 500) {
          failures += 1;
        }
      }
      expect(failures).toBeGreaterThan(total * 0.25);
      expect(failures).toBeLessThan(total * 0.75);
    } finally {
      await server.close();
    }
  });

  it("failureRate 0 never fails and failureRate 1 always fails", async () => {
    const healthy = await startWebhookServer({ failureRate: 0 });
    try {
      for (let i = 0; i < 10; i += 1) {
        const res = await fetch(healthy.url);
        await res.arrayBuffer();
        expect(res.status).toBe(200);
      }
    } finally {
      await healthy.close();
    }

    const failing = await startWebhookServer({
      failureRate: 1,
      failureStatus: 503,
    });
    try {
      for (let i = 0; i < 10; i += 1) {
        const res = await fetch(failing.url);
        await res.arrayBuffer();
        expect(res.status).toBe(503);
      }
    } finally {
      await failing.close();
    }
  });

  it("counts incoming requests and exposes them", async () => {
    const server = await startWebhookServer({ status: 200 });
    try {
      expect(server.getRequestCount()).toBe(0);
      for (let i = 0; i < 7; i += 1) {
        const res = await fetch(server.url, { method: "POST" });
        await res.arrayBuffer();
      }
      expect(server.getRequestCount()).toBe(7);
      const requests = server.getRequests();
      expect(requests).toHaveLength(7);
      for (const r of requests) {
        expect(r.method).toBe("POST");
        expect(r.statusSent).toBe(200);
      }
    } finally {
      await server.close();
    }
  });

  it("listens on an ephemeral port", async () => {
    const first = await startWebhookServer();
    const second = await startWebhookServer();
    try {
      expect(first.port).toBeGreaterThan(0);
      expect(second.port).toBeGreaterThan(0);
      expect(first.port).not.toBe(second.port);
      expect(first.url).toContain(`:${first.port}`);
    } finally {
      await first.close();
      await second.close();
    }
  });
});
