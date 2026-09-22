// Minimal Prometheus exposition-format (0.0.4) metrics. Deliberately
// dependency-free: the three services each record into an in-process
// registry, and apps/api exposes its registry at GET /metrics. No
// pushgateway, no remote write, no extra npm dependency — Day 15 is
// instrumentation, later days consume it.
//
// Cardinality rule (enforced at registration): per-job / per-request
// identity (jobId, requestId, leaseToken, URLs, idempotency keys) must
// NEVER be a metric label. That identity belongs in structured logs,
// which are already keyed by jobId/requestId. A metric label that can
// take unbounded values would explode the memory of any Prometheus
// server scraping us.

export type LabelValues = Record<string, string>;

const FORBIDDEN_LABEL_NAMES = new Set([
  "jobid",
  "job_id",
  "requestid",
  "request_id",
  "leasetoken",
  "lease_token",
  "token",
  "url",
  "targeturl",
  "target_url",
  "webhookurl",
  "webhook_url",
  "idempotencykey",
  "idempotency_key",
  "payload",
  "body",
]);

const NAME_RE = /^[a-zA-Z_:][a-zA-Z0-9_:]*$/;

function validateName(kind: string, name: string): void {
  if (!NAME_RE.test(name)) {
    throw new Error(`Invalid ${kind} name "${name}"`);
  }
}

function validateLabelNames(labelNames: readonly string[]): void {
  for (const label of labelNames) {
    validateName("label", label);
    if (FORBIDDEN_LABEL_NAMES.has(label.toLowerCase())) {
      throw new Error(
        `Forbidden high-cardinality metric label "${label}": put per-job/per-request identity in logs, not metric labels`,
      );
    }
  }
}

function checkLabels(labelNames: readonly string[], labels: LabelValues): void {
  for (const key of Object.keys(labels)) {
    if (!labelNames.includes(key)) {
      throw new Error(
        `Unexpected label "${key}" (declared labels: [${labelNames.join(", ")}])`,
      );
    }
  }
}

function seriesKey(labels: LabelValues): string {
  return Object.keys(labels)
    .sort()
    .map((k) => `${k}=${labels[k] ?? ""}`)
    .join(",");
}

function escapeLabelValue(value: string): string {
  return value
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/\n/g, "\\n");
}

function renderLabels(labels: LabelValues): string {
  const keys = Object.keys(labels).sort();
  if (keys.length === 0) {
    return "";
  }
  return `{${keys.map((k) => `${k}="${escapeLabelValue(labels[k] ?? "")}"`).join(",")}}`;
}

interface Series {
  labels: LabelValues;
  value: number;
}

export class Counter {
  readonly name: string;
  readonly help: string;
  readonly labelNames: readonly string[];
  private series = new Map<string, Series>();

  constructor(name: string, help: string, labelNames: readonly string[] = []) {
    validateName("counter", name);
    validateLabelNames(labelNames);
    this.name = name;
    this.help = help;
    this.labelNames = labelNames;
  }

  // Counters only move forward. They are incremented exactly when the
  // underlying business transition succeeds — never speculatively.
  inc(labels: LabelValues = {}, value = 1): void {
    if (!(value >= 0)) {
      throw new Error(`Counter "${this.name}" cannot increment by ${value}`);
    }
    checkLabels(this.labelNames, labels);
    const key = seriesKey(labels);
    const existing = this.series.get(key);
    if (existing) {
      existing.value += value;
    } else {
      this.series.set(key, { labels: { ...labels }, value });
    }
  }

  get(labels: LabelValues = {}): number {
    return this.series.get(seriesKey(labels))?.value ?? 0;
  }

  clear(): void {
    this.series.clear();
  }

  render(): string {
    const lines = [
      `# HELP ${this.name} ${this.help}`,
      `# TYPE ${this.name} counter`,
    ];
    for (const { labels, value } of this.series.values()) {
      lines.push(`${this.name}${renderLabels(labels)} ${value}`);
    }
    return lines.join("\n");
  }
}

export class Gauge {
  readonly name: string;
  readonly help: string;
  readonly labelNames: readonly string[];
  private series = new Map<string, Series>();

  constructor(name: string, help: string, labelNames: readonly string[] = []) {
    validateName("gauge", name);
    validateLabelNames(labelNames);
    this.name = name;
    this.help = help;
    this.labelNames = labelNames;
  }

  set(labels: LabelValues, value: number): void {
    checkLabels(this.labelNames, labels);
    this.series.set(seriesKey(labels), { labels: { ...labels }, value });
  }

  setValue(value: number): void {
    this.set({}, value);
  }

  get(labels: LabelValues = {}): number {
    return this.series.get(seriesKey(labels))?.value ?? 0;
  }

  clear(): void {
    this.series.clear();
  }

  render(): string {
    const lines = [
      `# HELP ${this.name} ${this.help}`,
      `# TYPE ${this.name} gauge`,
    ];
    for (const { labels, value } of this.series.values()) {
      lines.push(`${this.name}${renderLabels(labels)} ${value}`);
    }
    return lines.join("\n");
  }
}

export class Histogram {
  readonly name: string;
  readonly help: string;
  readonly labelNames: readonly string[];
  readonly buckets: readonly number[];
  private counts = new Map<
    string,
    { labels: LabelValues; buckets: number[] }
  >();
  private sums = new Map<string, { labels: LabelValues; sum: number }>();
  private totals = new Map<string, number>();

  constructor(
    name: string,
    help: string,
    buckets: readonly number[],
    labelNames: readonly string[] = [],
  ) {
    validateName("histogram", name);
    validateLabelNames(labelNames);
    if (buckets.length === 0) {
      throw new Error(`Histogram "${name}" needs at least one bucket`);
    }
    const sorted = [...buckets].sort((a, b) => a - b);
    for (let i = 1; i < sorted.length; i++) {
      const prev = sorted[i - 1];
      const curr = sorted[i];
      if (prev !== undefined && curr !== undefined && prev >= curr) {
        throw new Error(
          `Histogram "${name}" buckets must be strictly increasing`,
        );
      }
    }
    this.name = name;
    this.help = help;
    this.buckets = sorted;
    this.labelNames = labelNames;
  }

  // value is in the unit named by the metric (e.g. *_seconds takes seconds).
  observe(value: number, labels: LabelValues = {}): void {
    if (!(value >= 0)) {
      throw new Error(`Histogram "${this.name}" cannot observe ${value}`);
    }
    checkLabels(this.labelNames, labels);
    const key = seriesKey(labels);
    let entry = this.counts.get(key);
    if (!entry) {
      entry = { labels: { ...labels }, buckets: this.buckets.map(() => 0) };
      this.counts.set(key, entry);
      this.sums.set(key, { labels: { ...labels }, sum: 0 });
      this.totals.set(key, 0);
    }
    entry.buckets.forEach((_, i) => {
      const bound = this.buckets[i];
      if (bound !== undefined && value <= bound) {
        const bucketArr = entry?.buckets;
        if (bucketArr && bucketArr[i] !== undefined) {
          bucketArr[i] = (bucketArr[i] as number) + 1;
        }
      }
    });
    const sumEntry = this.sums.get(key);
    if (sumEntry) {
      sumEntry.sum += value;
    }
    this.totals.set(key, (this.totals.get(key) ?? 0) + 1);
  }

  getCount(labels: LabelValues = {}): number {
    return this.totals.get(seriesKey(labels)) ?? 0;
  }

  getSum(labels: LabelValues = {}): number {
    return this.sums.get(seriesKey(labels))?.sum ?? 0;
  }

  clear(): void {
    this.counts.clear();
    this.sums.clear();
    this.totals.clear();
  }

  render(): string {
    const lines = [
      `# HELP ${this.name} ${this.help}`,
      `# TYPE ${this.name} histogram`,
    ];
    for (const { labels, buckets } of this.counts.values()) {
      // Buckets are stored cumulatively by observe() (each bucket counts
      // all values <= its bound), so render outputs them directly.
      buckets.forEach((count, i) => {
        const bound = this.buckets[i];
        lines.push(
          `${this.name}_bucket${renderLabels({ ...labels, le: String(bound) })} ${count}`,
        );
      });
      const total = this.totals.get(seriesKey(labels)) ?? 0;
      lines.push(
        `${this.name}_bucket${renderLabels({ ...labels, le: "+Inf" })} ${total}`,
      );
      lines.push(
        `${this.name}_sum${renderLabels(labels)} ${this.sums.get(seriesKey(labels))?.sum ?? 0}`,
      );
      lines.push(`${this.name}_count${renderLabels(labels)} ${total}`);
    }
    return lines.join("\n");
  }
}

// Sensible default buckets (seconds) for webhook / job-processing
// durations. The webhook timeout is 10s and the lease is 30s, so the top
// buckets capture slow-but-alive attempts without drowning the common
// millisecond-scale case.
export const DURATION_BUCKETS_SECONDS: readonly number[] = [
  0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
];

export class MetricsRegistry {
  private readonly metrics: Array<Counter | Gauge | Histogram> = [];
  private readonly names = new Set<string>();

  private register<T extends Counter | Gauge | Histogram>(metric: T): T {
    if (this.names.has(metric.name)) {
      throw new Error(`Metric "${metric.name}" is already registered`);
    }
    this.names.add(metric.name);
    this.metrics.push(metric);
    return metric;
  }

  counter(
    name: string,
    help: string,
    labelNames: readonly string[] = [],
  ): Counter {
    return this.register(new Counter(name, help, labelNames));
  }

  gauge(name: string, help: string, labelNames: readonly string[] = []): Gauge {
    return this.register(new Gauge(name, help, labelNames));
  }

  histogram(
    name: string,
    help: string,
    buckets: readonly number[] = DURATION_BUCKETS_SECONDS,
    labelNames: readonly string[] = [],
  ): Histogram {
    return this.register(new Histogram(name, help, buckets, labelNames));
  }

  toPrometheus(): string {
    return this.metrics.map((m) => m.render()).join("\n") + "\n";
  }

  // Test support: forget recorded values, keep definitions.
  reset(): void {
    for (const metric of this.metrics) {
      metric.clear();
    }
  }
}
