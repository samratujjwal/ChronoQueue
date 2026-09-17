export const WEBHOOK_TIMEOUT_MS = 10_000;

const RETRYABLE_HTTP_STATUS_CODES = new Set([500, 502, 503, 504]);

export function isRetryableHttpStatus(statusCode: number): boolean {
  return RETRYABLE_HTTP_STATUS_CODES.has(statusCode);
}

export type WebhookFailureKind = "http_status" | "network" | "timeout";

export class WebhookDeliveryError extends Error {
  readonly retryable: boolean;
  readonly kind: WebhookFailureKind;
  readonly statusCode?: number;

  constructor(
    message: string,
    options: {
      retryable: boolean;
      kind: WebhookFailureKind;
      statusCode?: number;
    },
  ) {
    super(message);
    this.name = "WebhookDeliveryError";
    this.retryable = options.retryable;
    this.kind = options.kind;
    this.statusCode = options.statusCode;
  }
}

export async function deliverWebhook(
  targetUrl: string,
  payload: unknown,
): Promise<number> {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);

  try {
    let response: Response;

    try {
      response = await fetch(targetUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
    } catch (error) {
      if (error instanceof Error && error.name === "AbortError") {
        throw new WebhookDeliveryError(
          `Webhook request timed out after ${WEBHOOK_TIMEOUT_MS}ms`,
          { retryable: true, kind: "timeout" },
        );
      }
      throw new WebhookDeliveryError(
        `Webhook request failed: ${error instanceof Error ? error.message : String(error)}`,
        { retryable: true, kind: "network" },
      );
    }

    if (!response.ok) {
      throw new WebhookDeliveryError(
        `Webhook responded with HTTP ${response.status}`,
        {
          retryable: isRetryableHttpStatus(response.status),
          kind: "http_status",
          statusCode: response.status,
        },
      );
    }

    return response.status;
  } finally {
    clearTimeout(timeoutId);
  }
}
