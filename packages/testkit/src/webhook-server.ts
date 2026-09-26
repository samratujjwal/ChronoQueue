import {
  createServer,
  type IncomingMessage,
  type Server,
  type ServerResponse,
} from "node:http";

export interface WebhookServerOptions {
  status?: number;
  latencyMs?: number;
  failureRate?: number;
  failureStatus?: number;
}

export interface CapturedWebhookRequest {
  method: string;
  url: string;
  statusSent: number;
  receivedAtMs: number;
}

export interface WebhookServer {
  url: string;
  port: number;
  getRequestCount(): number;
  getRequests(): CapturedWebhookRequest[];
  close(): Promise<void>;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function startWebhookServer(
  options: WebhookServerOptions = {},
): Promise<WebhookServer> {
  const {
    status = 200,
    latencyMs = 0,
    failureRate = 0,
    failureStatus = 500,
  } = options;

  const captured: CapturedWebhookRequest[] = [];

  const server: Server = createServer(
    (req: IncomingMessage, res: ServerResponse) => {
      req.resume();
      req.on("end", () => {
        void (async () => {
          if (latencyMs > 0) {
            await sleep(latencyMs);
          }
          const failed = Math.random() < failureRate;
          const statusToSend = failed ? failureStatus : status;
          captured.push({
            method: req.method ?? "",
            url: req.url ?? "",
            statusSent: statusToSend,
            receivedAtMs: Date.now(),
          });
          res.writeHead(statusToSend, { "content-type": "application/json" });
          res.end(JSON.stringify({ ok: !failed }));
        })();
      });
    },
  );

  await new Promise<void>((resolve) =>
    server.listen(0, "127.0.0.1", () => resolve()),
  );
  const address = server.address();
  const port =
    typeof address === "object" && address !== null ? address.port : 0;

  return {
    url: `http://127.0.0.1:${port}/webhook`,
    port,
    getRequestCount: () => captured.length,
    getRequests: () => [...captured],
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close((err) => (err ? reject(err) : resolve()));
      }),
  };
}
