import type { FastifyError, FastifyInstance } from "fastify";
import { z } from "zod";
import { db } from "../db/client.js";
import { jobs } from "../db/schema/jobs.js";

const createJobBodySchema = z.object({
  type: z.literal("WEBHOOK"),
  targetUrl: z.string().url(),
  payload: z.record(z.string(), z.unknown()),
  scheduledAt: z.coerce.date().optional(),
  maxAttempts: z.number().int().positive().optional(),
});

function badRequest(message: string): FastifyError {
  const error = new Error(message) as FastifyError;
  error.statusCode = 400;
  return error;
}

export function registerJobRoutes(app: FastifyInstance): void {
  app.post("/jobs", async (request, reply) => {
    const parsed = createJobBodySchema.safeParse(request.body);

    if (!parsed.success) {
      const message = parsed.error.issues
        .map((issue) => `${issue.path.join(".") || "body"}: ${issue.message}`)
        .join("; ");
      throw badRequest(message);
    }

    const { type, targetUrl, payload, scheduledAt, maxAttempts } = parsed.data;

    const [created] = await db
      .insert(jobs)
      .values({
        type,
        status: "PENDING",
        targetUrl,
        payload,
        ...(scheduledAt ? { scheduledAt } : {}),
        ...(maxAttempts ? { maxAttempts } : {}),
      })
      .returning({
        id: jobs.id,
        type: jobs.type,
        status: jobs.status,
        targetUrl: jobs.targetUrl,
        scheduledAt: jobs.scheduledAt,
        createdAt: jobs.createdAt,
      });

    reply.status(201).send(created);
  });
}
