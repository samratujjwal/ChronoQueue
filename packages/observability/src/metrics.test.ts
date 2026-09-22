import { describe, expect, it } from "vitest";
import { Counter, Gauge, Histogram, MetricsRegistry } from "./metrics.js";

describe("Counter", () => {
  it("increments and reads back values", () => {
    const c = new Counter("jobs_created_total", "test");
    expect(c.get()).toBe(0);
    c.inc();
    c.inc({}, 2);
    expect(c.get()).toBe(3);
  });

  it("supports low-cardinality labels", () => {
    const c = new Counter("jobs_dead_total", "test", ["source"]);
    c.inc({ source: "worker" });
    c.inc({ source: "worker" });
    c.inc({ source: "recovery" });
    expect(c.get({ source: "worker" })).toBe(2);
    expect(c.get({ source: "recovery" })).toBe(1);
  });

  it("rejects high-cardinality label names at construction", () => {
    for (const label of [
      "jobId",
      "job_id",
      "requestId",
      "request_id",
      "leaseToken",
      "lease_token",
      "targetUrl",
      "target_url",
      "idempotencyKey",
      "idempotency_key",
    ]) {
      expect(() => new Counter("c_test_forbidden", "test", [label])).toThrow(
        /Forbidden high-cardinality/,
      );
    }
  });

  it("rejects undeclared labels on inc", () => {
    const c = new Counter("c_test_declared", "test", ["source"]);
    expect(() => c.inc({ bogus: "x" })).toThrow(/Unexpected label/);
  });

  it("rejects negative increments", () => {
    const c = new Counter("c_test_negative", "test");
    expect(() => c.inc({}, -1)).toThrow();
  });
});

describe("Gauge", () => {
  it("sets and reads back values", () => {
    const g = new Gauge("dlq_jobs", "test");
    g.setValue(7);
    expect(g.get()).toBe(7);
    g.setValue(0);
    expect(g.get()).toBe(0);
  });
});

describe("Histogram", () => {
  it("observes values into buckets and tracks sum/count", () => {
    const h = new Histogram(
      "webhook_duration_seconds",
      "test",
      [0.5, 1],
      ["outcome"],
    );
    h.observe(0.25, { outcome: "success" });
    h.observe(0.75, { outcome: "success" });
    h.observe(5, { outcome: "failure" });

    expect(h.getCount({ outcome: "success" })).toBe(2);
    expect(h.getSum({ outcome: "success" })).toBeCloseTo(1.0);
    expect(h.getCount({ outcome: "failure" })).toBe(1);

    const rendered = h.render();
    // cumulative buckets: 0.25 lands in le=0.5; 0.75 lands in le=1.
    // Labels render alphabetically (le before outcome).
    expect(rendered).toContain(
      'webhook_duration_seconds_bucket{le="0.5",outcome="success"} 1',
    );
    expect(rendered).toContain(
      'webhook_duration_seconds_bucket{le="1",outcome="success"} 2',
    );
    expect(rendered).toContain(
      'webhook_duration_seconds_bucket{le="+Inf",outcome="success"} 2',
    );
    expect(rendered).toContain(
      'webhook_duration_seconds_count{outcome="success"} 2',
    );
  });

  it("rejects forbidden label names", () => {
    expect(
      () => new Histogram("h_test_forbidden", "test", [1], ["jobId"]),
    ).toThrow(/Forbidden high-cardinality/);
  });
});

describe("MetricsRegistry", () => {
  it("renders Prometheus exposition format", () => {
    const registry = new MetricsRegistry();
    const c = registry.counter(
      "jobs_succeeded_total",
      "Jobs that reached SUCCEEDED",
    );
    c.inc();
    c.inc();
    const text = registry.toPrometheus();
    expect(text).toContain("# HELP jobs_succeeded_total");
    expect(text).toContain("# TYPE jobs_succeeded_total counter");
    expect(text).toContain("jobs_succeeded_total 2");
  });

  it("rejects duplicate metric names", () => {
    const registry = new MetricsRegistry();
    registry.counter("dup_metric_total", "test");
    expect(() => registry.counter("dup_metric_total", "test")).toThrow(
      /already registered/,
    );
  });

  it("reset() clears values but keeps definitions", () => {
    const registry = new MetricsRegistry();
    const c = registry.counter("resettable_total", "test");
    c.inc();
    expect(c.get()).toBe(1);
    registry.reset();
    expect(c.get()).toBe(0);
    // definition survives: re-increment works, no duplicate error
    c.inc();
    expect(c.get()).toBe(1);
  });
});
