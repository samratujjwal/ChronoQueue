import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { runLoadTest } from "./load-generator.js";
import {
  apiBaseUrl,
  checkApiHealth,
  countJobsByPrefix,
  counterDelta,
  deleteJobsByPrefix,
  findDuplicateKeys,
  readApiCounters,
  requireDatabaseUrl,
  round2,
} from "./scenario-helpers.js";

const RUN_ID = Date.now().toString(36);
const PREFIX = `day17-l1-${RUN_ID}-`;

const pool = new Pool({ connectionString: requireDatabaseUrl() });
const baseUrl = apiBaseUrl();

beforeAll(async () => {
  await checkApiHealth(baseUrl);
}, 30000);

afterAll(async () => {
  const deleted = await deleteJobsByPrefix(pool, PREFIX);
  console.log(JSON.stringify({ scenario: "L1", cleanupDeletedJobs: deleted }));
  await pool.end();
});

interface RunConfig {
  name: string;
  totalRequests: number;
  concurrency: number;
}

async function runL1(config: RunConfig): Promise<void> {
  const keyPrefix = `${PREFIX}${config.name}-`;
  const beforeJobs = await countJobsByPrefix(pool, keyPrefix);
  const beforeMetrics = await readApiCounters(baseUrl);

  const result = await runLoadTest({
    url: `${baseUrl}/jobs`,
    totalRequests: config.totalRequests,
    concurrency: config.concurrency,
    method: "POST",
    headersFor: (index) => ({ "Idempotency-Key": `${keyPrefix}${index}` }),
    body: {
      type: "WEBHOOK",
      targetUrl: "https://example.com/webhook",
      payload: { scenario: "l1", run: config.name },
    },
    timeoutMs: 30000,
  });

  const afterJobs = await countJobsByPrefix(pool, keyPrefix);
  const afterMetrics = await readApiCounters(baseUrl);
  const duplicates = await findDuplicateKeys(pool, keyPrefix);

  const summary = {
    scenario: "L1",
    run: config.name,
    totalRequests: result.totalRequests,
    concurrency: config.concurrency,
    status2xx: result.successful,
    status4xx: result.clientErrors,
    status5xx: result.serverErrors,
    networkErrors: result.networkErrors,
    otherStatuses: result.otherStatuses,
    statusCounts: result.statusCounts,
    totalDurationMs: round2(result.totalDurationMs),
    requestsPerSec: round2(result.requestsPerSec),
    p50Ms: round2(result.p50Ms),
    p95Ms: round2(result.p95Ms),
    p99Ms: round2(result.p99Ms),
    dbJobsBefore: beforeJobs,
    dbJobsAfter: afterJobs,
    dbNewJobs: afterJobs - beforeJobs,
    duplicateKeys: duplicates,
    metricsDelta: {
      jobs_created_total: counterDelta(
        beforeMetrics,
        afterMetrics,
        "jobs_created_total",
      ),
      job_creation_idempotent_hits_total: counterDelta(
        beforeMetrics,
        afterMetrics,
        "job_creation_idempotent_hits_total",
      ),
    },
  };
  console.log(`L1_RESULT ${JSON.stringify(summary)}`);

  expect(result.successful).toBe(config.totalRequests);
  expect(result.clientErrors).toBe(0);
  expect(result.serverErrors).toBe(0);
  expect(result.networkErrors).toBe(0);
  expect(result.otherStatuses).toBe(0);
  expect(afterJobs - beforeJobs).toBe(config.totalRequests);
  expect(duplicates).toEqual([]);
  expect(summary.metricsDelta.jobs_created_total).toBe(config.totalRequests);
  expect(summary.metricsDelta.job_creation_idempotent_hits_total).toBe(0);
}

describe("L1 API throughput", () => {
  it("100 requests / concurrency 10", { timeout: 120000 }, async () => {
    await runL1({ name: "run1", totalRequests: 100, concurrency: 10 });
  });

  it("500 requests / concurrency 25", { timeout: 180000 }, async () => {
    await runL1({ name: "run2", totalRequests: 500, concurrency: 25 });
  });

  it("1000 requests / concurrency 50", { timeout: 300000 }, async () => {
    await runL1({ name: "run3", totalRequests: 1000, concurrency: 50 });
  });
});
