import { Pool } from "pg";

export function apiBaseUrl(): string {
  return process.env.API_BASE_URL ?? "http://127.0.0.1:3000";
}

export function requireDatabaseUrl(): string {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) {
    throw new Error("DATABASE_URL must be set to run load scenarios");
  }
  return databaseUrl;
}

export async function checkApiHealth(baseUrl: string): Promise<void> {
  const res = await fetch(`${baseUrl}/health`);
  if (!res.ok) {
    throw new Error(
      `API not reachable at ${baseUrl}/health (status ${res.status})`,
    );
  }
}

export async function countJobsByPrefix(
  pool: Pool,
  prefix: string,
): Promise<number> {
  const result = await pool.query(
    "select count(*)::int as n from jobs where idempotency_key like $1",
    [`${prefix}%`],
  );
  const row = result.rows[0] as { n: number } | undefined;
  return row?.n ?? 0;
}

export async function findDuplicateKeys(
  pool: Pool,
  prefix: string,
): Promise<Array<{ key: string; count: number }>> {
  const result = await pool.query(
    "select idempotency_key as key, count(*)::int as count from jobs where idempotency_key like $1 group by idempotency_key having count(*) > 1",
    [`${prefix}%`],
  );
  return result.rows as Array<{ key: string; count: number }>;
}

export async function deleteJobsByPrefix(
  pool: Pool,
  prefix: string,
): Promise<number> {
  const result = await pool.query(
    "delete from jobs where idempotency_key like $1",
    [`${prefix}%`],
  );
  return result.rowCount ?? 0;
}

export async function readApiCounters(
  baseUrl: string,
): Promise<Record<string, number>> {
  const res = await fetch(`${baseUrl}/metrics`);
  if (!res.ok) {
    throw new Error(`GET ${baseUrl}/metrics failed with status ${res.status}`);
  }
  const text = await res.text();
  const counters: Record<string, number> = {};
  for (const line of text.split("\n")) {
    const trimmed = line.trim();
    if (trimmed === "" || trimmed.startsWith("#")) {
      continue;
    }
    const match = /^([a-z_][a-z0-9_]*) ([0-9.eE+-]+)$/.exec(trimmed);
    if (match?.[1] !== undefined && match[2] !== undefined) {
      counters[match[1]] = Number(match[2]);
    }
  }
  return counters;
}

export function counterDelta(
  before: Record<string, number>,
  after: Record<string, number>,
  name: string,
): number {
  return (after[name] ?? 0) - (before[name] ?? 0);
}

export function round2(value: number): number {
  return Math.round(value * 100) / 100;
}
