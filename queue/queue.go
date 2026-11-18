// Package queue is a durable job queue on PostgreSQL.
//
// Workers claim jobs with FOR UPDATE SKIP LOCKED, so concurrent workers never
// take the same row. Each claim is a lease: a worker that crashes or stalls
// loses its jobs when the lease runs out, and another worker picks them up.
// Every claim bumps the job's attempt number, which acts as a fencing token:
// a worker whose lease was taken over can no longer complete the job.
package queue

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql
var schemaSQL string

// ErrLeaseLost means another worker took over the job after this worker's lease expired.
var ErrLeaseLost = errors.New("pgqueue: lease lost")

type Job struct {
	ID          int64
	Queue       string
	Key         string
	Payload     json.RawMessage
	Attempt     int // fencing token for this claim
	MaxAttempts int
}

type Queue struct {
	pool *pgxpool.Pool
	name string
}

func New(pool *pgxpool.Pool, name string) *Queue {
	return &Queue{pool: pool, name: name}
}

// Migrate creates the jobs table if it does not exist.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, schemaSQL)
	return err
}

// Enqueue adds a job once per idempotency key and returns its id. Enqueuing the
// same key again returns the existing job's id and reports created=false.
func (q *Queue) Enqueue(ctx context.Context, key string, payload any, maxAttempts int) (id int64, created bool, err error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, false, err
	}
	err = q.pool.QueryRow(ctx, `
		INSERT INTO pgqueue_jobs (queue, idempotency_key, payload, max_attempts)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (queue, idempotency_key) DO NOTHING
		RETURNING id`, q.name, key, body, maxAttempts).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, err
	}
	err = q.pool.QueryRow(ctx,
		`SELECT id FROM pgqueue_jobs WHERE queue = $1 AND idempotency_key = $2`, q.name, key).Scan(&id)
	return id, false, err
}

// Claim leases up to n runnable jobs: queued jobs whose run time has come, and
// running jobs whose lease has expired.
func (q *Queue) Claim(ctx context.Context, worker string, n int, lease time.Duration) ([]Job, error) {
	rows, err := q.pool.Query(ctx, `
		UPDATE pgqueue_jobs j
		SET status = 'running', attempts = j.attempts + 1, locked_by = $2,
		    lease_until = now() + make_interval(secs => $4)
		WHERE j.id IN (
			SELECT id FROM pgqueue_jobs
			WHERE queue = $1
			  AND ((status = 'queued' AND run_at <= now())
			    OR (status = 'running' AND lease_until < now()))
			ORDER BY run_at, id
			FOR UPDATE SKIP LOCKED
			LIMIT $3)
		RETURNING j.id, j.queue, j.idempotency_key, j.payload, j.attempts, j.max_attempts`,
		q.name, worker, n, lease.Seconds())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Job, error) {
		var j Job
		err := r.Scan(&j.ID, &j.Queue, &j.Key, &j.Payload, &j.Attempt, &j.MaxAttempts)
		return j, err
	})
}

// Extend pushes the lease forward while a long job is still running.
func (q *Queue) Extend(ctx context.Context, j Job, worker string, lease time.Duration) error {
	tag, err := q.pool.Exec(ctx, `
		UPDATE pgqueue_jobs SET lease_until = now() + make_interval(secs => $4)
		WHERE id = $1 AND attempts = $2 AND locked_by = $3 AND status = 'running'`,
		j.ID, j.Attempt, worker, lease.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// CompleteTx marks the job done inside tx. Run the job's database side effects
// in the same transaction: if the lease was lost, this returns ErrLeaseLost,
// the caller rolls back, and the side effects never commit.
func CompleteTx(ctx context.Context, tx pgx.Tx, j Job, worker string) error {
	tag, err := tx.Exec(ctx, `
		UPDATE pgqueue_jobs SET status = 'done', completed_at = now(), lease_until = NULL
		WHERE id = $1 AND attempts = $2 AND locked_by = $3 AND status = 'running'`,
		j.ID, j.Attempt, worker)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Fail records an error. The job is retried with exponential backoff, or marked
// dead once it has used all its attempts.
func (q *Queue) Fail(ctx context.Context, j Job, worker string, cause error, baseDelay time.Duration) error {
	delay := time.Duration(float64(baseDelay) * math.Pow(2, float64(j.Attempt-1)))
	tag, err := q.pool.Exec(ctx, `
		UPDATE pgqueue_jobs
		SET status = CASE WHEN attempts >= max_attempts THEN 'dead' ELSE 'queued' END,
		    run_at = now() + make_interval(secs => $4),
		    last_error = $5, lease_until = NULL, locked_by = NULL
		WHERE id = $1 AND attempts = $2 AND locked_by = $3 AND status = 'running'`,
		j.ID, j.Attempt, worker, delay.Seconds(), cause.Error())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Stats counts jobs by status for this queue.
func (q *Queue) Stats(ctx context.Context) (map[string]int64, error) {
	rows, err := q.pool.Query(ctx,
		`SELECT status, count(*) FROM pgqueue_jobs WHERE queue = $1 GROUP BY status`, q.name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{"queued": 0, "running": 0, "done": 0, "dead": 0}
	for rows.Next() {
		var s string
		var n int64
		if err := rows.Scan(&s, &n); err != nil {
			return nil, err
		}
		out[s] = n
	}
	return out, rows.Err()
}

func (j Job) String() string { return fmt.Sprintf("job %d (%s, attempt %d)", j.ID, j.Key, j.Attempt) }
