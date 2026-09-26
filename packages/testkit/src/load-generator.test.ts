import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";
import { afterAll, describe, expect, it } from "vitest";
import { runLoadTest } from "./load-generator.js";

interface Captured {
  method: string;
  header: string | undefined;
  reqIndex: string | undefined;
  body: string;
}

const captured: Captured[] = [];
let server: Server;
let baseUrl = "";

function readBody(req: IncomingMessage): Promise<string> {
  return new Promise((resolve) => {
    let data = "";
    req.on("data", (chunk: Buffer) => {
      data += chunk.toString();
    });
    req.on("end", () => resolve(data));
  });
}

async function startTestServer(): Promise<void> {
  server = createServer(async (req: IncomingMessage, res: ServerResponse) => {
    const body = await readBody(req);
    if (req.url === "/ok") {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    } else if (req.url === "/echo") {
      captured.push({
        method: req.method ?? "",
        header: req.headers["x-test-header"] as string | undefined,
        reqIndex: req.headers["x-req-index"] as string | undefined,
        body,
      });
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    } else if (req.url === "/bad") {
      res.writeHead(500, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false }));
    } else {
      res.writeHead(404, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: false }));
    }
  });
  await new Promise<void>((resolve) =>
    server.listen(0, "127.0.0.1", () => resolve()),
  );
  const address = server.address();
  const port =
    typeof address === "object" && address !== null ? address.port : 0;
  baseUrl = `http://127.0.0.1:${port}`;
}

async function closedPortUrl(): Promise<string> {
  const probe = createServer();
  await new Promise<void>((resolve) =>
    probe.listen(0, "127.0.0.1", () => resolve()),
  );
  const address = probe.address();
  const port =
    typeof address === "object" && address !== null ? address.port : 0;
  await new Promise<void>((resolve) => probe.close(() => resolve()));
  return `http://127.0.0.1:${port}/ok`;
}

await startTestServer();

afterAll(async () => {
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

describe("runLoadTest against a local HTTP server", () => {
  it("reports correct counts for all-successful requests", async () => {
    const result = await runLoadTest({
      url: `${baseUrl}/ok`,
      totalRequests: 60,
      concurrency: 6,
    });

    expect(result.totalRequests).toBe(60);
    expect(result.successful).toBe(60);
    expect(result.clientErrors).toBe(0);
    expect(result.serverErrors).toBe(0);
    expect(result.networkErrors).toBe(0);
    expect(
      result.successful +
        result.clientErrors +
        result.serverErrors +
        result.networkErrors +
        result.otherStatuses,
    ).toBe(60);
    expect(result.totalDurationMs).toBeGreaterThan(0);
    expect(result.requestsPerSec).toBeGreaterThan(0);
    expect(result.p50Ms).toBeGreaterThanOrEqual(0);
    expect(result.p50Ms).toBeLessThanOrEqual(result.p95Ms);
    expect(result.p95Ms).toBeLessThanOrEqual(result.p99Ms);
  });

  it("classifies 5xx responses as server errors", async () => {
    const result = await runLoadTest({
      url: `${baseUrl}/bad`,
      totalRequests: 20,
      concurrency: 4,
    });

    expect(result.totalRequests).toBe(20);
    expect(result.serverErrors).toBe(20);
    expect(result.successful).toBe(0);
  });

  it("classifies 4xx responses as client errors", async () => {
    const result = await runLoadTest({
      url: `${baseUrl}/missing`,
      totalRequests: 20,
      concurrency: 4,
    });

    expect(result.totalRequests).toBe(20);
    expect(result.clientErrors).toBe(20);
    expect(result.successful).toBe(0);
  });

  it("classifies connection failures as network errors", async () => {
    const url = await closedPortUrl();
    const result = await runLoadTest({
      url,
      totalRequests: 10,
      concurrency: 2,
      timeoutMs: 2000,
    });

    expect(result.totalRequests).toBe(10);
    expect(result.networkErrors).toBe(10);
    expect(result.successful).toBe(0);
  });

  it("sends a JSON body and custom headers with POST", async () => {
    captured.length = 0;
    const result = await runLoadTest({
      url: `${baseUrl}/echo`,
      totalRequests: 5,
      concurrency: 2,
      method: "POST",
      headers: { "x-test-header": "hello" },
      body: { event: "user.created" },
    });

    expect(result.successful).toBe(5);
    expect(captured).toHaveLength(5);
    for (const c of captured) {
      expect(c.method).toBe("POST");
      expect(c.header).toBe("hello");
      expect(JSON.parse(c.body)).toEqual({ event: "user.created" });
    }
  });

  it("reports an exact per-status breakdown in statusCounts", async () => {
    const ok = await runLoadTest({
      url: `${baseUrl}/ok`,
      totalRequests: 10,
      concurrency: 3,
    });
    expect(ok.statusCounts).toEqual({ 200: 10 });

    const bad = await runLoadTest({
      url: `${baseUrl}/bad`,
      totalRequests: 5,
      concurrency: 2,
    });
    expect(bad.statusCounts).toEqual({ 500: 5 });

    const missing = await runLoadTest({
      url: `${baseUrl}/missing`,
      totalRequests: 4,
      concurrency: 2,
    });
    expect(missing.statusCounts).toEqual({ 404: 4 });
  });

  it("applies per-request headers from headersFor", async () => {
    captured.length = 0;
    const result = await runLoadTest({
      url: `${baseUrl}/echo`,
      totalRequests: 10,
      concurrency: 4,
      method: "POST",
      headers: { "x-test-header": "static" },
      headersFor: (index) => ({ "x-req-index": String(index) }),
      body: { ping: true },
    });

    expect(result.successful).toBe(10);
    expect(captured).toHaveLength(10);
    const seen = new Set<number>();
    for (const c of captured) {
      expect(c.header).toBe("static");
      expect(c.reqIndex).toBeDefined();
      seen.add(Number(c.reqIndex));
    }
    expect([...seen].sort((a, b) => a - b)).toEqual([
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    ]);
  });
});
