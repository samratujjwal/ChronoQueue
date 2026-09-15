## Components

### API

Responsibilities:

- Accept job creation requests
- Validate input
- Authenticate requests
- Persist jobs
- Return job information
- Expose job status APIs

The API should not execute webhook jobs synchronously.

### PostgreSQL

PostgreSQL is the durable source of truth.

It stores:

- Jobs
- Job status
- Attempts
- Scheduling information
- Failure information
- Idempotency information

### Redis

Redis is used for fast coordination and queue-related operations.

It should not be treated as the primary durable business database.

### Scheduler

The scheduler determines which scheduled jobs are due for execution.

It moves due work toward the execution queue.

### Worker

The worker:

1. Receives a job
2. Executes the webhook
3. Records success/failure
4. Retries retryable failures
5. Sends permanently failed jobs to the DLQ

### Dashboard

The dashboard provides operational visibility into:

- Job counts
- Job status
- Queue state
- Attempts
- Failed jobs
- Dead-letter jobs
