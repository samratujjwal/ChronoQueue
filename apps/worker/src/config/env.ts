import "dotenv/config";
import { z } from "zod";

const envSchema = z.object({
  NODE_ENV: z
    .enum(["development", "test", "production"])
    .default("development"),
  LOG_LEVEL: z
    .enum(["fatal", "error", "warn", "info", "debug", "trace"])
    .default("info"),
  DATABASE_URL: z
    .string()
    .url()
    .refine(
      (value) =>
        value.startsWith("postgres://") || value.startsWith("postgresql://"),
      { message: "DATABASE_URL must start with postgres:// or postgresql://" },
    ),
  REDIS_URL: z
    .string()
    .url()
    .refine(
      (value) => value.startsWith("redis://") || value.startsWith("rediss://"),
      { message: "REDIS_URL must start with redis:// or rediss://" },
    ),
  RETRY_BASE_DELAY_MS: z.coerce.number().int().positive().default(1000),
  RETRY_MAX_DELAY_MS: z.coerce.number().int().positive().default(30000),
  WORKER_LEASE_DURATION_MS: z.coerce.number().int().positive().default(30_000),
  WORKER_LEASE_RENEWAL_INTERVAL_MS: z.coerce
    .number()
    .int()
    .positive()
    .default(10_000),
});

function loadConfig() {
  const parsed = envSchema.safeParse(process.env);

  if (!parsed.success) {
    console.error("Invalid environment configuration:");
    console.error(JSON.stringify(parsed.error.flatten().fieldErrors, null, 2));
    process.exit(1);
  }

  return parsed.data;
}

export const config = loadConfig();
export type Config = typeof config;
