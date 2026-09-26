export interface LoadGeneratorOptions {
  url: string;
  totalRequests: number;
  concurrency: number;
  method?: string;
  headers?: Record<string, string>;
  headersFor?: (index: number) => Record<string, string>;
  body?: unknown;
  timeoutMs?: number;
}

export interface LoadTestResult {
  totalRequests: number;
  successful: number;
  clientErrors: number;
  serverErrors: number;
  networkErrors: number;
  otherStatuses: number;
  totalDurationMs: number;
  requestsPerSec: number;
  p50Ms: number;
  p95Ms: number;
  p99Ms: number;
  statusCounts: Record<number, number>;
}

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) {
    return 0;
  }
  const rank = Math.ceil((p / 100) * sorted.length) - 1;
  const index = Math.max(0, Math.min(sorted.length - 1, rank));
  return sorted[index] ?? 0;
}

export async function runLoadTest(
  options: LoadGeneratorOptions,
): Promise<LoadTestResult> {
  const {
    url,
    totalRequests,
    concurrency,
    method = "GET",
    headers = {},
    timeoutMs = 30000,
  } = options;

  const bodyText =
    options.body === undefined ? undefined : JSON.stringify(options.body);
  const requestHeaders: Record<string, string> = { ...headers };
  if (
    bodyText !== undefined &&
    requestHeaders["content-type"] === undefined &&
    requestHeaders["Content-Type"] === undefined
  ) {
    requestHeaders["content-type"] = "application/json";
  }

  let nextIndex = 0;
  let successful = 0;
  let clientErrors = 0;
  let serverErrors = 0;
  let networkErrors = 0;
  let otherStatuses = 0;
  const statusCounts: Record<number, number> = {};
  const latencies: number[] = [];

  const startedAt = performance.now();

  async function worker(): Promise<void> {
    while (true) {
      const current = nextIndex;
      if (current >= totalRequests) {
        return;
      }
      nextIndex = current + 1;

      const requestStartedAt = performance.now();
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), timeoutMs);
      const perRequestHeaders =
        options.headersFor === undefined
          ? requestHeaders
          : { ...requestHeaders, ...options.headersFor(current) };
      try {
        const response = await fetch(url, {
          method,
          headers: perRequestHeaders,
          body: bodyText,
          signal: controller.signal,
        });
        await response.arrayBuffer();
        latencies.push(performance.now() - requestStartedAt);
        statusCounts[response.status] =
          (statusCounts[response.status] ?? 0) + 1;

        if (response.status >= 200 && response.status < 300) {
          successful += 1;
        } else if (response.status >= 400 && response.status < 500) {
          clientErrors += 1;
        } else if (response.status >= 500 && response.status < 600) {
          serverErrors += 1;
        } else {
          otherStatuses += 1;
        }
      } catch {
        networkErrors += 1;
      } finally {
        clearTimeout(timer);
      }
    }
  }

  const workerCount = Math.max(1, Math.min(concurrency, totalRequests));
  const workers: Promise<void>[] = [];
  for (let i = 0; i < workerCount; i += 1) {
    workers.push(worker());
  }
  await Promise.all(workers);

  const totalDurationMs = performance.now() - startedAt;
  latencies.sort((a, b) => a - b);

  return {
    totalRequests,
    successful,
    clientErrors,
    serverErrors,
    networkErrors,
    otherStatuses,
    totalDurationMs,
    requestsPerSec:
      totalDurationMs > 0 ? (totalRequests / totalDurationMs) * 1000 : 0,
    p50Ms: percentile(latencies, 50),
    p95Ms: percentile(latencies, 95),
    p99Ms: percentile(latencies, 99),
    statusCounts,
  };
}
