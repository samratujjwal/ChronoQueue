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
    createdAt: timestamp("created_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
    updatedAt: timestamp("updated_at", { withTimezone: true })
      .notNull()
      .defaultNow(),
  },
  (table) => [
    index("idx_jobs_status_scheduled_at").on(table.status, table.scheduledAt),
    check("attempts_non_negative", sql`${table.attempts} >= 0`),
    check("max_attempts_positive", sql`${table.maxAttempts} > 0`),
  ],
);
