-- -- Repair note: 0002 and 0003 already added these two columns, but 0004 was
-- -- (re)generated against the 0001 snapshot (meta lacked 0002/0003 snapshots),
-- -- so it duplicated them. IF NOT EXISTS makes the chain reproducible from a
-- -- fresh database while staying a no-op for databases that already have them.
-- ALTER TABLE "jobs" ADD COLUMN IF NOT EXISTS "lease_until" timestamp with time zone;--> statement-breakpoint
-- ALTER TABLE "jobs" ADD COLUMN IF NOT EXISTS "lease_token" text;

-- Repair note: 0002 and 0003 already added these two columns, but 0004 was
-- (re)generated against the 0001 snapshot (meta lacked 0002/0003 snapshots),
-- so it duplicated them. IF NOT EXISTS makes the chain reproducible from a
-- fresh database while staying a no-op for databases that already have them.
ALTER TABLE "jobs" ADD COLUMN IF NOT EXISTS "lease_until" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "jobs" ADD COLUMN IF NOT EXISTS "lease_token" text;
