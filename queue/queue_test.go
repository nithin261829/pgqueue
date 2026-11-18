package queue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func setup(t *testing.T) (*pgxpool.Pool, *Queue) {
	t.Helper()
	dsn := os.Getenv("PGQUEUE_TEST_DSN")
	if dsn == "" {
		t.Skip("set PGQUEUE_TEST_DSN to run against PostgreSQL")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := Migrate(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return pool, New(pool, fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()))
}

func TestEnqueueIsIdempotent(t *testing.T) {
	_, q := setup(t)
	ctx := context.Background()
	id1, created1, err := q.Enqueue(ctx, "order-42", map[string]int{"amount": 10}, 3)
	if err != nil || !created1 {
		t.Fatalf("first enqueue: id=%d created=%v err=%v", id1, created1, err)
	}
	id2, created2, err := q.Enqueue(ctx, "order-42", map[string]int{"amount": 10}, 3)
	if err != nil || created2 || id2 != id1 {
		t.Fatalf("second enqueue: id=%d created=%v err=%v, want id %d not created", id2, created2, err, id1)
	}
}

func TestConcurrentClaimsNeverOverlap(t *testing.T) {
	_, q := setup(t)
	ctx := context.Background()
	const n = 500
	for i := 0; i < n; i++ {
		if _, _, err := q.Enqueue(ctx, fmt.Sprint(i), nil, 3); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				jobs, err := q.Claim(ctx, fmt.Sprint("w", w), 10, time.Minute)
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(seen) != n {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), n)
	}
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("job %d claimed %d times", id, c)
		}
	}
}

func TestExpiredLeaseIsReclaimedAndStaleWorkerIsFenced(t *testing.T) {
	pool, q := setup(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS fence_effects (job_id bigint, worker text)`); err != nil {
		t.Fatal(err)
	}
	if _, _, err := q.Enqueue(ctx, "report", nil, 3); err != nil {
		t.Fatal(err)
	}
	first, err := q.Claim(ctx, "w1", 1, time.Second)
	if err != nil || len(first) != 1 {
		t.Fatalf("w1 claim: %v %v", first, err)
	}
	if again, _ := q.Claim(ctx, "w2", 1, time.Second); len(again) != 0 {
		t.Fatal("job claimed twice while its lease was valid")
	}
	time.Sleep(1200 * time.Millisecond)
	second, err := q.Claim(ctx, "w2", 1, time.Minute)
	if err != nil || len(second) != 1 || second[0].Attempt != 2 {
		t.Fatalf("w2 reclaim after expiry: %v %v", second, err)
	}

	// w1 wakes up late: its write and its completion must both be rejected.
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO fence_effects VALUES ($1, 'w1')`, first[0].ID); err != nil {
			return err
		}
		return CompleteTx(ctx, tx, first[0], "w1")
	})
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale completion: got %v, want ErrLeaseLost", err)
	}
	err = pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO fence_effects VALUES ($1, 'w2')`, second[0].ID); err != nil {
			return err
		}
		return CompleteTx(ctx, tx, second[0], "w2")
	})
	if err != nil {
		t.Fatal(err)
	}
	var writers []string
	rows, _ := pool.Query(ctx, `SELECT worker FROM fence_effects WHERE job_id = $1`, first[0].ID)
	writers, err = pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(writers) != 1 || writers[0] != "w2" {
		t.Fatalf("side effects = %v (%v), want only w2", writers, err)
	}
}

func TestFailedJobsRetryThenGoDead(t *testing.T) {
	pool, q := setup(t)
	ctx := context.Background()
	id, _, _ := q.Enqueue(ctx, "flaky", nil, 2)
	status := func() (s string, runAt time.Time) {
		pool.QueryRow(ctx, `SELECT status, run_at FROM pgqueue_jobs WHERE id = $1`, id).Scan(&s, &runAt)
		return
	}
	for attempt := 1; attempt <= 2; attempt++ {
		jobs, err := q.Claim(ctx, "w", 1, time.Minute)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("attempt %d claim: %v %v", attempt, jobs, err)
		}
		if err := q.Fail(ctx, jobs[0], "w", errors.New("upstream 503"), time.Second); err != nil {
			t.Fatal(err)
		}
		s, runAt := status()
		if attempt == 1 {
			if s != "queued" || !runAt.After(time.Now()) {
				t.Fatalf("after first failure: status %s, run_at %v; want queued with a delay", s, runAt)
			}
			if jobs, _ := q.Claim(ctx, "w", 1, time.Minute); len(jobs) != 0 {
				t.Fatal("job claimed before its backoff ended")
			}
			pool.Exec(ctx, `UPDATE pgqueue_jobs SET run_at = now() WHERE id = $1`, id)
		} else if s != "dead" {
			t.Fatalf("after last attempt: status %s, want dead", s)
		}
	}
}

func TestExactlyOnceEffectsWhenWorkersCrash(t *testing.T) {
	pool, q := setup(t)
	ctx := context.Background()
	if _, err := pool.Exec(ctx,
		`CREATE TABLE IF NOT EXISTS effects (queue text, job_id bigint, applied int, PRIMARY KEY (queue, job_id))`); err != nil {
		t.Fatal(err)
	}
	const n = 300
	for i := 0; i < n; i++ {
		q.Enqueue(ctx, fmt.Sprint(i), nil, 5)
	}
	handler := func(ctx context.Context, tx pgx.Tx, j Job) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO effects VALUES ($1, $2, 1)
			ON CONFLICT (queue, job_id) DO UPDATE SET applied = effects.applied + 1`, q.name, j.ID)
		time.Sleep(5 * time.Millisecond)
		return err
	}
	cfg := WorkerConfig{Concurrency: 4, Lease: time.Second, Poll: 20 * time.Millisecond, RetryDelay: 10 * time.Millisecond}

	// Two workers crash part-way through: their contexts are cancelled mid-job.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()
	var wg sync.WaitGroup
	for w := 0; w < 3; w++ {
		wctx, crash := context.WithCancel(runCtx)
		if w < 2 {
			time.AfterFunc(time.Duration(150*(w+1))*time.Millisecond, crash)
		} else {
			defer crash()
		}
		c := cfg
		c.ID = fmt.Sprint("worker-", w)
		wg.Add(1)
		go func() { defer wg.Done(); q.Run(wctx, c, handler) }()
	}

	deadline := time.Now().Add(60 * time.Second)
	for {
		s, err := q.Stats(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if s["done"] == n {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out: %v", s)
		}
		time.Sleep(100 * time.Millisecond)
	}
	stop()
	wg.Wait()

	var rows, over int
	pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE applied <> 1) FROM effects WHERE queue = $1`,
		q.name).Scan(&rows, &over)
	if rows != n || over != 0 {
		t.Fatalf("effects: %d rows, %d applied more than once; want %d rows, 0 duplicates", rows, over, n)
	}
	// The crashed workers must have stranded at least one job that another worker then reclaimed.
	var reclaimed int
	pool.QueryRow(ctx, `SELECT count(*) FROM pgqueue_jobs WHERE queue = $1 AND attempts > 1`, q.name).Scan(&reclaimed)
	if reclaimed == 0 {
		t.Fatal("no job was reclaimed after a crash, so recovery was not exercised")
	}
	t.Logf("%d jobs were reclaimed after worker crashes and still applied exactly once", reclaimed)
}
