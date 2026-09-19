ALTER TABLE "jobs" ADD COLUMN "idempotency_key" text;--> statement-breakpoint
UPDATE "jobs" SET "idempotency_key" = "id"::text WHERE "idempotency_key" IS NULL;--> statement-breakpoint
ALTER TABLE "jobs" ALTER COLUMN "idempotency_key" SET NOT NULL;--> statement-breakpoint
ALTER TABLE "jobs" ADD CONSTRAINT "jobs_idempotency_key_unique" UNIQUE("idempotency_key");