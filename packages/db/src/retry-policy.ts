export interface RetryPolicyConfig {
  baseDelayMs: number;
  maxDelayMs: number;
}

/**
 * exponentialDelay = min(baseDelay * 2^(attempt - 1), maxDelay)
 *
 * `attempt` is the 1-based execution-attempt count that just occurred
 * (the post-increment `attempts` value). attempt <= 0 is treated as
 * attempt 1 — a defensive floor; this should never happen in practice
 * since `attempts` is always incremented before this is called.
 */
export function calculateExponentialDelayMs(
  attempt: number,
  config: RetryPolicyConfig,
): number {
  const safeAttempt = attempt > 0 ? attempt : 1;
  const exponential = config.baseDelayMs * 2 ** (safeAttempt - 1);
  return Math.min(exponential, config.maxDelayMs);
}

/**
 * Full jitter: an integer uniformly distributed in [0, exponentialDelayMs]
 * (inclusive both ends, for random in [0, 1)).
 */
export function applyFullJitter(
  exponentialDelayMs: number,
  random: () => number = Math.random,
): number {
  return Math.floor(random() * (exponentialDelayMs + 1));
}
