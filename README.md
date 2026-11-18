# pgqueue

A durable job queue on PostgreSQL, written in Go. Workers can crash at any point and every job's
database side effect is still applied exactly once.

## How it works

- **No double claims.** Workers claim jobs with `FOR UPDATE SKIP LOCKED`, so concurrent workers
  never take the same row and never wait on each other.
- **Leases.** A claim is a lease. A heartbeat extends it while the job runs; if the worker crashes or
  stalls, the lease runs out and another worker reclaims the job.
- **Fencing.** Every claim increments the job's attempt number, which acts as a fencing token. A worker
  that lost its lease cannot complete the job, because completion checks the token.
- **Exactly-once side effects.** The handler gets a transaction. Its writes and the job's completion
  commit together, so a fenced-out worker's writes roll back with it.
- **Idempotent enqueue.** Each job has an idempotency key per queue; enqueuing the same key twice
  returns the existing job.
- **Retries.** Failed jobs retry with exponential backoff and are marked `dead` after `max_attempts`.
- **Metrics.** Prometheus counters for claims, completions, failures and lost leases, plus a job
  duration histogram.

```go
q := queue.New(pool, "emails")
q.Enqueue(ctx, "welcome-42", payload, 5)
q.Run(ctx, queue.WorkerConfig{ID: "w1", Concurrency: 8, Lease: 30 * time.Second,
    Poll: 100 * time.Millisecond, RetryDelay: time.Second},
    func(ctx context.Context, tx pgx.Tx, j queue.Job) error {
        _, err := tx.Exec(ctx, `INSERT INTO sent_emails (job_id) VALUES ($1)`, j.ID)
        return err
    })
```

## Tests

Run against a real PostgreSQL 16 server with the race detector:

```bash
PGQUEUE_TEST_DSN=postgres://postgres:postgres@localhost:5432/postgres go test -race ./...
```

They cover duplicate enqueues, 20 workers claiming 500 jobs at once with no overlap, an expired
lease being reclaimed while the stale worker is fenced out and its write rolled back, retry with
backoff until a job is dead, and exactly-once side effects while workers crash mid-job.

## Benchmark

```bash
PGQUEUE_DSN=postgres://postgres:postgres@localhost:5432/postgres \
  go run ./cmd/bench -jobs 10000 -workers 4 -concurrency 8 -crashes 2
```

On a local run (PostgreSQL 16 in Docker on a MacBook): 10,000 jobs processed in 2.5 s, about
4,000 jobs per second, with 2 of 4 workers crashing mid-run. 8 stranded jobs were reclaimed, and all
10,000 side effects were applied exactly once.

## Limits

- One PostgreSQL database is the coordination point, so throughput is bounded by it.
- Exactly-once covers side effects written through the job's transaction. Calls to outside services
  still need their own idempotency keys.
- Polling, not `LISTEN/NOTIFY`, wakes idle workers.
