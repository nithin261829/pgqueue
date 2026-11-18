// Command bench measures queue throughput and checks that every job's side
// effect was applied exactly once, optionally while crashing workers.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/nithin261829/pgqueue/queue"
)

func main() {
	jobs := flag.Int("jobs", 10000, "jobs to enqueue")
	workers := flag.Int("workers", 4, "worker processes to simulate")
	concurrency := flag.Int("concurrency", 8, "jobs in flight per worker")
	crashes := flag.Int("crashes", 2, "workers to crash part-way through")
	flag.Parse()

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("PGQUEUE_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := queue.Migrate(ctx, pool); err != nil {
		log.Fatal(err)
	}
	name := fmt.Sprintf("bench-%d", time.Now().UnixNano())
	q := queue.New(pool, name)
	pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS bench_effects (queue text, job_id bigint, applied int, PRIMARY KEY (queue, job_id))`)

	start := time.Now()
	for i := 0; i < *jobs; i++ {
		if _, _, err := q.Enqueue(ctx, fmt.Sprint(i), map[string]int{"n": i}, 5); err != nil {
			log.Fatal(err)
		}
	}
	enqueueTime := time.Since(start)

	handler := func(ctx context.Context, tx pgx.Tx, j queue.Job) error {
		_, err := tx.Exec(ctx, `INSERT INTO bench_effects VALUES ($1, $2, 1)
			ON CONFLICT (queue, job_id) DO UPDATE SET applied = bench_effects.applied + 1`, name, j.ID)
		return err
	}
	runCtx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	start = time.Now()
	for w := 0; w < *workers; w++ {
		wctx, crash := context.WithCancel(runCtx)
		defer crash()
		if w < *crashes {
			time.AfterFunc(time.Duration(200+rand.Intn(800))*time.Millisecond, crash)
		}
		cfg := queue.WorkerConfig{ID: fmt.Sprint("bench-", w), Concurrency: *concurrency,
			Lease: 2 * time.Second, Poll: 10 * time.Millisecond, RetryDelay: 50 * time.Millisecond}
		wg.Add(1)
		go func() { defer wg.Done(); q.Run(wctx, cfg, handler) }()
	}
	for {
		s, err := q.Stats(ctx)
		if err != nil {
			log.Fatal(err)
		}
		if s["done"] == int64(*jobs) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	processTime := time.Since(start)
	stop()
	wg.Wait()

	var rows, dup, reclaimed int
	pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE applied <> 1) FROM bench_effects WHERE queue = $1`, name).Scan(&rows, &dup)
	pool.QueryRow(ctx, `SELECT count(*) FROM pgqueue_jobs WHERE queue = $1 AND attempts > 1`, name).Scan(&reclaimed)
	fmt.Printf("enqueued %d jobs in %s\n", *jobs, enqueueTime.Round(time.Millisecond))
	fmt.Printf("processed %d jobs in %s (%.0f jobs/s) with %d workers x %d slots, %d crashed\n",
		*jobs, processTime.Round(time.Millisecond), float64(*jobs)/processTime.Seconds(), *workers, *concurrency, *crashes)
	fmt.Printf("reclaimed after crashes: %d; side effects: %d rows, %d applied more than once\n", reclaimed, rows, dup)
	if rows != *jobs || dup != 0 {
		os.Exit(1)
	}
}
