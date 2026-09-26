import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { runLoadTest } from "./load-generator.js";
import {
  apiBaseUrl,
  checkApiHealth,
  countJobsByPrefix,
  counterDelta,
  deleteJobsByPrefix,
  readApiCounters,
  requireDatabaseUrl,
  round2,
} from "./scenario-helpers.js";

const RUN_ID = Date.now().toString(36);
const IDEMPOTENCY_KEY = `day17-l2-${RUN_ID}`;
const PREFIX = `day17-l2-${RUN_ID}`;

const pool = new Pool({ connectionString: requireDatabaseUrl() });
const baseUrl = apiBaseUrl();

beforeAll(async () => {
  await checkApiHealth(baseUrl);
}, 30000);

afterAll(async () => {
  const deleted = await deleteJobsByPrefix(pool, PREFIX);
  console.log(JSON.stringify({ scenario: "L2", cleanupDeletedJobs: deleted }));
  await pool.end();
});

describe("L2 idempotency concurrency", () => {
  it(
    "100 concurrent requests with the same Idempotency-Key create exactly one job",
    { timeout: 120000 },
    async () => {
      const beforeJobs = await countJobsByPrefix(pool, PREFIX);
      const beforeMetrics = await readApiCounters(baseUrl);

      const result = await runLoadTest({
        url: `${baseUrl}/jobs`,
        totalRequests: 100,
        concurrency: 100,
        method: "POST",
        headers: { "Idempotency-Key": IDEMPOTENCY_KEY },
        body: {
          type: "WEBHOOK",
          targetUrl: "https://example.com/webhook",
          payload: { scenario: "l2" },
        },
        timeoutMs: 30000,
      });

      const afterJobs = await countJobsByPrefix(pool, PREFIX);
      const afterMetrics = await readApiCounters(baseUrl);

      const created201 = result.statusCounts[201] ?? 0;
      const replayed200 = result.statusCounts[200] ?? 0;

      const summary = {
        scenario: "L2",
        totalRequests: result.totalRequests,
        concurrency: 100,
        created201,
        replayed200,
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
      console.log(`L2_RESULT ${JSON.stringify(summary)}`);

      expect(result.successful).toBe(100);
      expect(created201).toBe(1);
      expect(replayed200).toBe(99);
      expect(result.clientErrors).toBe(0);
      expect(result.serverErrors).toBe(0);
      expect(result.networkErrors).toBe(0);
      expect(result.otherStatuses).toBe(0);
      expect(afterJobs - beforeJobs).toBe(1);
      expect(summary.metricsDelta.jobs_created_total).toBe(1);
      expect(summary.metricsDelta.job_creation_idempotent_hits_total).toBe(99);
    },
  );
});
