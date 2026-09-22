import type {
  FastifyError,
  FastifyInstance,
  FastifyReply,
  FastifyRequest,
} from "fastify";
import { safeError } from "@chronoqueue/observability";
import { config } from "../config/env.js";

interface ErrorResponseBody {
  error: {
    message: string;
    statusCode: number;
  };
}

export function registerErrorHandler(app: FastifyInstance): void {
  app.setNotFoundHandler((request: FastifyRequest, reply: FastifyReply) => {
    const body: ErrorResponseBody = {
      error: {
        message: `Route ${request.method} ${request.url} not found`,
        statusCode: 404,
      },
    };
    reply.status(404).send(body);
  });

  app.setErrorHandler(
    (error: FastifyError, request: FastifyRequest, reply: FastifyReply) => {
      const statusCode =
        error.statusCode && error.statusCode >= 400 && error.statusCode < 600
          ? error.statusCode
          : 500;

      const isServerError = statusCode >= 500;
      const isProduction = config.NODE_ENV === "production";

      // Safe logging: raw Error objects can carry response bodies, headers,
      // or config snapshots. Log only the stable diagnostic fields plus
      // the event marker for correlation (PART 4).
      request.log.error(
        {
          event: "request_failed",
          requestId: request.id,
          statusCode,
          ...safeError(error),
        },
        "request failed",
      );

      const message =
        isServerError && isProduction ? "Internal Server Error" : error.message;

      const body: ErrorResponseBody = {
        error: {
          message,
          statusCode,
        },
      };

      reply.status(statusCode).send(body);
    },
  );
}
