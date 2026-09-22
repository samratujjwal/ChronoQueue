import { sql } from "drizzle-orm";
import {
  check,
  index,
  integer,
  jsonb,
  pgEnum,
  pgTable,
  text,
  timestamp,
  uuid,
} from "drizzle-orm/pg-core";
export const jobTypeEnum = pgEnum("job_type", ["WEBHOOK"]);

export const jobStatusEnum = pgEnum("job_status", [
  "PENDING",
  "QUEUED",
  "PROCESSING",
  "SUCCEEDED",
  "RETRYING",
  "DEAD",
]);

export const jobs = pgTable(
  "jobs",
  {
    id: uuid("id").primaryKey().defaultRandom(),
    idempotencyKey: text("idempotency_key").notNull().unique(),
    type: jobTypeEnum("type").notNull(),
    status: jobStatusEnum("status").notNull().default("PENDING"),
    targetUrl: text("target_url").notNull(),
    payload: jsonb("payload").notNull(),
    attempts: integer("attempts").notNull().default(0),
    maxAttempts: integer("max_attempts").notNull().default(5),
    scheduledAt: timestamp("scheduled_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    nextAttemptAt: timestamp("next_attempt_at", { withTimezone: true }),
    leaseUntil: timestamp("lease_until", { withTimezone: true }),
    leaseToken: text("lease_token"),
    // Failure diagnosis for the dead-letter queue (Day 14). Set by the
    // worker/recovery whenever a job lands in RETRYING or DEAD, cleared on
    // SUCCEEDED. Nullable so pre-existing rows need no backfill.
    lastErrorCode: text("last_error_code"),
    lastErrorMessage: text("last_error_message"),
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("idx_jobs_status_scheduled_at").on(table.status, table.scheduledAt),
    // Serves the DLQ listing: WHERE status = 'DEAD'
    // ORDER BY updated_at DESC, id DESC (Day 14).
    index("idx_jobs_dead_updated_at")
      .on(table.updatedAt.desc(), table.id.desc())
      .where(sql`${table.status} = 'DEAD'`),
    check("attempts_non_negative", sql`${table.attempts} >= 0`),
    check("max_attempts_positive", sql`${table.maxAttempts} > 0`),
  ],
);
