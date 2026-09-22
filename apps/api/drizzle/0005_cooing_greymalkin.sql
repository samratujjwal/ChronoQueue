ALTER TABLE "jobs" ADD COLUMN "last_error_code" text;--> statement-breakpoint
ALTER TABLE "jobs" ADD COLUMN "last_error_message" text;--> statement-breakpoint
CREATE INDEX "idx_jobs_dead_updated_at" ON "jobs" USING btree ("updated_at" DESC NULLS LAST,"id" DESC NULLS LAST) WHERE "jobs"."status" = 'DEAD';