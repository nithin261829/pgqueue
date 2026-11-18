package queue

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus"
)

// Handler does a job's work inside tx. Database writes made through tx commit
// only if the job is still this worker's when it completes.
type Handler func(ctx context.Context, tx pgx.Tx, j Job) error

type WorkerConfig struct {
	ID          string
	Concurrency int
	Lease       time.Duration
	Poll        time.Duration
	RetryDelay  time.Duration
}

var (
	claimed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_jobs_claimed_total", Help: "Jobs claimed by workers"}, []string{"queue"})
	completed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_jobs_completed_total", Help: "Jobs completed"}, []string{"queue"})
	failed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_jobs_failed_total", Help: "Job attempts that returned an error"}, []string{"queue"})
	leaseLost = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "pgqueue_lease_lost_total", Help: "Completions rejected because another worker took the job"}, []string{"queue"})
	duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "pgqueue_job_duration_seconds", Help: "Time from claim to completion", Buckets: prometheus.DefBuckets},
		[]string{"queue"})
)

// MustRegisterMetrics adds the worker metrics to a Prometheus registry.
func MustRegisterMetrics(r prometheus.Registerer) {
	r.MustRegister(claimed, completed, failed, leaseLost, duration)
}

// Run processes jobs until ctx is cancelled. Each slot keeps its lease alive with
// a heartbeat at a third of the lease length.
func (q *Queue) Run(ctx context.Context, cfg WorkerConfig, h Handler) {
	var wg sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				jobs, err := q.Claim(ctx, cfg.ID, 1, cfg.Lease)
				if err != nil || len(jobs) == 0 {
					sleep(ctx, cfg.Poll)
					continue
				}
				claimed.WithLabelValues(q.name).Inc()
				q.process(ctx, cfg, h, jobs[0])
			}
		}()
	}
	wg.Wait()
}

func (q *Queue) process(ctx context.Context, cfg WorkerConfig, h Handler, j Job) {
	start := time.Now()
	hbCtx, stopHB := context.WithCancel(ctx)
	defer stopHB()
	go func() {
		t := time.NewTicker(cfg.Lease / 3)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if q.Extend(hbCtx, j, cfg.ID, cfg.Lease) != nil {
					return
				}
			}
		}
	}()

	err := pgx.BeginFunc(ctx, q.pool, func(tx pgx.Tx) error {
		if err := h(ctx, tx, j); err != nil {
			return err
		}
		return CompleteTx(ctx, tx, j, cfg.ID)
	})
	stopHB()
	switch {
	case err == nil:
		completed.WithLabelValues(q.name).Inc()
		duration.WithLabelValues(q.name).Observe(time.Since(start).Seconds())
	case errors.Is(err, ErrLeaseLost):
		leaseLost.WithLabelValues(q.name).Inc()
	case ctx.Err() != nil:
		// Shutting down: leave the job for its lease to expire.
	default:
		failed.WithLabelValues(q.name).Inc()
		if ferr := q.Fail(context.Background(), j, cfg.ID, err, cfg.RetryDelay); errors.Is(ferr, ErrLeaseLost) {
			leaseLost.WithLabelValues(q.name).Inc()
		}
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}
