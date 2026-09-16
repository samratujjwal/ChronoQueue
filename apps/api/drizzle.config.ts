import { defineConfig } from "drizzle-kit";

export default defineConfig({
  schema: "../../packages/db/src/schema/jobs.ts",
  out: "./drizzle",
  dialect: "postgresql",
  dbCredentials: {
    url: process.env.DATABASE_URL ?? "",
  },
});
