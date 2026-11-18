CREATE TABLE IF NOT EXISTS pgqueue_jobs (
    id              bigserial PRIMARY KEY,
    queue           text        NOT NULL,
    idempotency_key text        NOT NULL,
    payload         jsonb       NOT NULL DEFAULT '{}',
    status          text        NOT NULL DEFAULT 'queued'
                    CHECK (status IN ('queued', 'running', 'done', 'dead')),
    attempts        int         NOT NULL DEFAULT 0,
    max_attempts    int         NOT NULL DEFAULT 5,
    run_at          timestamptz NOT NULL DEFAULT now(),
    lease_until     timestamptz,
    locked_by       text,
    last_error      text,
    created_at      timestamptz NOT NULL DEFAULT now(),
    completed_at    timestamptz,
    UNIQUE (queue, idempotency_key)
);

-- Claims scan only runnable rows, so keep a partial index on them.
CREATE INDEX IF NOT EXISTS pgqueue_jobs_runnable
    ON pgqueue_jobs (queue, run_at, id)
    WHERE status IN ('queued', 'running');
