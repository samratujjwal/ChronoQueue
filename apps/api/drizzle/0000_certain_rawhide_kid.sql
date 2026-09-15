CREATE TYPE "public"."job_status" AS ENUM('PENDING', 'QUEUED', 'PROCESSING', 'SUCCEEDED', 'RETRYING', 'DEAD');--> statement-breakpoint
CREATE TYPE "public"."job_type" AS ENUM('WEBHOOK');--> statement-breakpoint
CREATE TABLE "jobs" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"type" "job_type" NOT NULL,
	"status" "job_status" DEFAULT 'PENDING' NOT NULL,
	"target_url" text NOT NULL,
	"payload" jsonb NOT NULL,
	"attempts" integer DEFAULT 0 NOT NULL,
	"max_attempts" integer DEFAULT 5 NOT NULL,
	"scheduled_at" timestamp with time zone DEFAULT now() NOT NULL,
	"next_attempt_at" timestamp with time zone,
	"created_at" timestamp with time zone DEFAULT now() NOT NULL,
	"updated_at" timestamp with time zone DEFAULT now() NOT NULL,
	CONSTRAINT "attempts_non_negative" CHECK ("jobs"."attempts" >= 0),
	CONSTRAINT "max_attempts_positive" CHECK ("jobs"."max_attempts" > 0)
);
--> statement-breakpoint
CREATE INDEX "idx_jobs_status_scheduled_at" ON "jobs" USING btree ("status","scheduled_at");