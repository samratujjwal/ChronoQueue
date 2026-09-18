# ChronoQueue

Production-oriented asynchronous webhook delivery system built with Node.js, TypeScript, Fastify, PostgreSQL, Redis, BullMQ, Drizzle ORM, and Vitest.

## Overview

ChronoQueue accepts webhook delivery jobs through an HTTP API, persists them in PostgreSQL, schedules due jobs through a dedicated scheduler, and processes deliveries asynchronously through a separate BullMQ worker.

The system is designed around a simple reliability principle:

> PostgreSQL is the durable source of truth. Redis/BullMQ is used for asynchronous coordination, not as the business database.

### Core flow

```text
Client
  |
  | POST /jobs
  v
Fastify API
  |
  | Persist job
  v
PostgreSQL
  |
  | Due jobs
  v
Scheduler
  |
  | { jobId }
  v
BullMQ / Redis
  |
  v
Worker
  |
  | Fetch job by ID
  v
PostgreSQL
  |
  | HTTP POST
  v
Webhook Endpoint
```

## Architecture

ChronoQueue is split into independent responsibilities:

- **API** — validates requests and creates durable jobs in PostgreSQL.
- **PostgreSQL** — stores the complete job state, payload, attempts, scheduling information, and timestamps.
- **Scheduler** — periodically finds due jobs and enqueues them into BullMQ.
- **Redis / BullMQ** — provides asynchronous job coordination and delivery execution.
- **Worker** — consumes queue jobs, retrieves the durable job from PostgreSQL, delivers the webhook, and updates its state.
- **Drizzle ORM** — provides the typed PostgreSQL schema and queries.
- **Vitest** — provides unit and integration testing.

## Design Principles

### PostgreSQL is the source of truth

The database stores the actual webhook job and its lifecycle state.

Redis does not contain the business payload as the source of truth.

This means a BullMQ job contains only a lightweight reference:

```json
{
  "jobId": "postgres-job-uuid"
}
```

The Worker uses that ID to retrieve the authoritative job from PostgreSQL.

### Lightweight queue payloads

BullMQ jobs intentionally avoid duplicating the webhook payload.

Instead of:

```json
{
  "targetUrl": "https://example.com/webhook",
  "payload": {
    "event": "payment.created"
  }
}
```

ChronoQueue sends:

```json
{
  "jobId": "..."
}
```

This keeps queue messages small and avoids having two independent copies of business data.

### Deterministic BullMQ job IDs

The PostgreSQL job ID is reused as the BullMQ `jobId`.

This provides deterministic identity and prevents duplicate waiting jobs for the same database job.

## Job Lifecycle

A webhook job can move through the following states:

```text
PENDING
   |
   v
QUEUED
   |
   v
PROCESSING
   |
   +------> SUCCEEDED
   |
   +------> RETRYING
   |           |
   |           v
   |       PROCESSING
   |
   +------> DEAD
```

### Current statuses

| Status | Meaning |
|---|---|
| `PENDING` | Job has been persisted but is not yet queued for delivery |
| `QUEUED` | Job has been scheduled/enqueued for processing |
| `PROCESSING` | Worker is currently attempting delivery |
| `SUCCEEDED` | Webhook delivery completed successfully |
| `RETRYING` | Delivery failed with a retryable error and can be attempted again |
| `DEAD` | Delivery is permanently failed or has exhausted its attempts |

## Webhook Delivery

The Worker performs HTTP webhook delivery using Node.js native `fetch`.

Requests are sent as JSON HTTP POST requests.

The current delivery timeout is:

```text
10 seconds
```

If the endpoint does not respond within the configured timeout, the Worker treats the operation as a retryable delivery failure.

## Retry and Failure Classification

ChronoQueue distinguishes between retryable and terminal failures.

### Retryable failures

Examples currently covered by the test suite:

- HTTP `500`
- HTTP `503`
- network / transport errors
- request timeout

These move the job toward:

```text
RETRYING
```

### Terminal failures

Examples currently covered:

- HTTP `400`
- HTTP `404`

These move the job to:

```text
DEAD
```

### Final retryable attempt

If a retryable failure occurs on the final allowed attempt, the job is moved to:

```text
DEAD
```

This prevents infinite delivery attempts.

## Scheduler

The Scheduler is a separate process responsible for finding jobs whose scheduled execution time is due.

It:

1. Queries PostgreSQL for due jobs.
2. Ignores future jobs.
3. Enqueues the due job into BullMQ.
4. Uses the PostgreSQL job ID as the BullMQ job ID.
5. Keeps the queue payload limited to `{ jobId }`.

Scheduler integration tests currently cover:

- future jobs are not enqueued
- due jobs are enqueued
- BullMQ receives the correct job ID
- multiple due jobs are handled while future jobs are skipped

## API

### `POST /jobs`

Creates a webhook delivery job.

Example request:

```http
POST /jobs
Content-Type: application/json
```

```json
{
  "type": "WEBHOOK",
  "targetUrl": "https://example.com/webhook",
  "payload": {
    "event": "payment.created",
    "orderId": "12345"
  },
  "maxAttempts": 5
}
```

Supported fields include:

- `type`
- `targetUrl`
- `payload`
- `scheduledAt`
- `maxAttempts`

The API validates the request before inserting the job into PostgreSQL.

### `GET /health`

Basic API health endpoint.

### `GET /ready`

Readiness endpoint for checking application dependencies/readiness.

## Database Schema

The `jobs` table currently contains:

| Column | Description |
|---|---|
| `id` | UUID primary key |
| `type` | Job type (`WEBHOOK`) |
| `status` | Current lifecycle status |
| `targetUrl` | Webhook destination |
| `payload` | JSON webhook payload |
| `attempts` | Number of delivery attempts |
| `maxAttempts` | Maximum allowed attempts |
| `scheduledAt` | Scheduled execution time |
| `nextAttemptAt` | Next retry time |
| `createdAt` | Creation timestamp |
| `updatedAt` | Last update timestamp |

The schema also includes validation constraints and an index for scheduled job lookup.

## Project Structure

```text
chronoqueue/
├── apps/
│   ├── api/
│   │   └── src/
│   │       ├── db/
│   │       ├── queue/
│   │       ├── routes/
│   │       ├── app.ts
│   │       └── server.ts
│   │
│   ├── scheduler/
│   │   └── src/
│   │
│   └── worker/
│       └── src/
│           ├── db/
│           ├── queue/
│           ├── config/
│           ├── logger.ts
│           ├── processor.ts
│           ├── worker.ts
│           └── index.ts
│
├── packages/
│   ├── db/
│   │   └── src/
│   │       ├── schema/
│   │       └── index.ts
│   ├── config/
│   ├── types/
│   └── utils/
│
├── docs/
│   ├── README.md
│   ├── architecture.md
│   └── decisions.md
│
├── package.json
└── package-lock.json
```

## Tech Stack

### Backend

- Node.js
- TypeScript
- Fastify

### Database

- PostgreSQL
- Drizzle ORM

### Queue / Coordination

- Redis
- BullMQ

### Testing

- Vitest
- Real PostgreSQL integration tests
- Real Redis/BullMQ integration tests
- Real HTTP delivery tests

### Logging

- Pino

## Testing

ChronoQueue currently includes integration-focused tests across the API, Worker, Scheduler, PostgreSQL, Redis, BullMQ, and HTTP delivery layers.

Examples of tested behavior include:

- API request validation
- PostgreSQL job persistence
- BullMQ producer behavior
- deterministic queue job IDs
- duplicate queue protection
- successful webhook delivery
- HTTP `500` retry behavior
- HTTP `503` retry behavior
- network/transport failure handling
- timeout handling
- HTTP `400` terminal failure
- HTTP `404` terminal failure
- final-attempt failure
- missing PostgreSQL jobs
- scheduler handling of due and future jobs
- webhook request method and payload

## Local Development

### Prerequisites

Install:

- Node.js
- npm
- Docker

Run PostgreSQL and Redis through Docker.

Example:

```bash
docker compose up -d
```

Verify PostgreSQL:

```bash
docker exec chronoqueue-postgres pg_isready -U postgres
```

Verify Redis:

```bash
docker exec chronoqueue-redis redis-cli ping
```

Expected Redis response:

```text
PONG
```

### Install dependencies

```bash
npm install
```

### Build

Build the database package:

```bash
npm run build -w @chronoqueue/db
```

Build/typecheck the required workspaces as needed.

### Tests

Run Worker tests:

```bash
npm run test -w @chronoqueue/worker
```

Run Scheduler tests:

```bash
npm run test -w @chronoqueue/scheduler
```

Run tests for the complete repository:

```bash
npm test
```

## Environment Variables

The applications use environment variables for external services.

Typical variables include:

```env
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/chronoqueue
REDIS_URL=redis://localhost:6379
```

Do not commit real credentials or secrets to the repository.

## Reliability Model

ChronoQueue intentionally separates **durability** from **coordination**.

```text
PostgreSQL
    |
    | Durable business state
    |
    +--------------------+
                         |
                         v
                    Scheduler
                         |
                         v
                  Redis / BullMQ
                         |
                         v
                      Worker
                         |
                         v
                    PostgreSQL
```

If queue coordination is interrupted, the database still contains the durable job information.

The Worker can reconstruct the delivery request from the PostgreSQL job ID instead of depending on the queue to contain the complete business payload.

## Production-Oriented Decisions

### Separate Worker Process

Webhook delivery is isolated from the API process.

This prevents long-running or slow external HTTP requests from directly blocking the API server's request handling path.

### Separate Redis Connections

The API and Worker use independent Redis connections appropriate to their responsibilities.

### Explicit Timeout

External webhook endpoints cannot be allowed to hang indefinitely.

ChronoQueue currently applies a 10-second request timeout and classifies timeout failures as retryable.

### Explicit Failure Classification

Not every HTTP error should trigger another attempt.

The system distinguishes between:

```text
Retryable
---------
500
503
Network errors
Timeouts

Terminal
--------
400
404
Final retryable attempt
```

This keeps retry behavior intentional rather than blindly retrying every failure.

## Current Implementation Status

### Implemented

- Fastify API
- PostgreSQL persistence
- Drizzle ORM schema
- Redis
- BullMQ queue
- deterministic BullMQ job IDs
- lightweight `{ jobId }` queue payload
- dedicated Worker process
- webhook HTTP delivery
- 10-second timeout
- retryable/terminal failure classification
- attempt tracking
- job state transitions
- dedicated Scheduler
- due-job detection
- integration testing with real PostgreSQL
- integration testing with real Redis/BullMQ
- integration testing with real HTTP endpoints
- graceful Worker shutdown
- structured Pino logging

### Not Yet Implemented

The following are intentionally outside the current implementation unless added later:

- authentication and authorization
- webhook signature verification
- dashboard/UI
- rate limiting
- per-tenant quotas
- delivery analytics
- dead-letter management UI
- distributed scheduler locking
- horizontal worker autoscaling
- production deployment infrastructure

## Design Goal

ChronoQueue is built as an engineering-focused backend project rather than a simple CRUD application.

The main goal is to demonstrate practical understanding of:

- asynchronous processing
- durable job state
- queue-based architecture
- worker processes
- retry semantics
- failure classification
- timeout handling
- PostgreSQL as a source of truth
- Redis/BullMQ coordination
- integration testing
- graceful shutdown
- production-oriented backend design
