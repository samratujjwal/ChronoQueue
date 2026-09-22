// Safe error serialization for logs. Never dump a raw Error object: it can
// carry response bodies, headers, or config snapshots. Extract only the
// fields an engineer needs to diagnose the failure.

export interface SafeErrorInfo {
  name: string;
  message: string;
  code?: string;
}

const MAX_MESSAGE_LENGTH = 500;

function truncate(message: string): string {
  if (message.length <= MAX_MESSAGE_LENGTH) {
    return message;
  }
  return message.slice(0, MAX_MESSAGE_LENGTH) + "…";
}

export function safeError(error: unknown): SafeErrorInfo {
  if (error instanceof Error) {
    const info: SafeErrorInfo = {
      name: error.name,
      message: truncate(error.message),
    };
    // Node-style `code` (e.g. ECONNREFUSED) is diagnostic gold and never
    // secret. Only string codes are kept.
    const code =
      "code" in error ? (error as { code?: unknown }).code : undefined;
    if (typeof code === "string" && code.length > 0) {
      info.code = code;
    }
    return info;
  }
  return { name: "UnknownError", message: truncate(String(error)) };
}

// Observability must NEVER break business logic (Day 15, PART 14). Metric
// recording and log emission are wrapped in safely() at the call sites
// where a throw would otherwise escape into job processing.
export function safely(fn: () => void): void {
  try {
    fn();
  } catch {
    // Intentionally silent: observability failures are not actionable at
    // the call site, and must not fail the job being processed.
  }
}
