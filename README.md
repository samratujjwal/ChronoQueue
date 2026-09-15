# Architecture Decisions

## ADR-001: Separate API and Worker

### Decision

The API server and worker will run as separate processes.

### Why?

Webhook execution can be slow or unreliable.

If the API directly executes webhook requests, slow external services
can consume API resources and affect normal API traffic.

Separate workers provide:

- Failure isolation
- Independent scaling
- Better resource control
- Clear separation of responsibilities

---

## ADR-002: PostgreSQL as Source of Truth

### Decision

PostgreSQL will store durable job state.

### Why?

Job state must survive:

- API crashes
- Worker crashes
- Redis restarts
- Application deployments

Redis will be used for fast coordination and queue operations.

---

## ADR-003: At-Least-Once Delivery

### Decision

ChronoQueue will target at-least-once job delivery with idempotent processing.

### Why?

Distributed systems can experience:

- Worker crashes
- Network failures
- Timeouts
- Duplicate message delivery

Exactly-once execution is difficult to guarantee.

Therefore consumers must be designed to safely handle duplicate delivery.
