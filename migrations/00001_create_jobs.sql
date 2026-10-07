-- +goose Up
CREATE TABLE jobs (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue           TEXT        NOT NULL DEFAULT 'default',
    type            TEXT        NOT NULL,
    payload         JSONB       NOT NULL DEFAULT '{}',
    priority        SMALLINT    NOT NULL DEFAULT 0,
    status          TEXT        NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'running', 'retrying', 'succeeded', 'dead')),
    attempts        INT         NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts    INT         NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by       TEXT,
    locked_until    TIMESTAMPTZ,
    last_error      TEXT,
    idempotency_key TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ
);

-- Claim path: only rows that can be picked up, in claim order.
CREATE INDEX jobs_claimable_idx ON jobs (queue, priority DESC, run_at, id)
    WHERE status IN ('pending', 'retrying');

-- Reaper path: running jobs ordered by lease expiry.
CREATE INDEX jobs_lease_idx ON jobs (locked_until) WHERE status = 'running';

-- Dashboard counts and filtered listings.
CREATE INDEX jobs_queue_status_idx ON jobs (queue, status);

-- Idempotent enqueue (target of INSERT ... ON CONFLICT).
CREATE UNIQUE INDEX jobs_idempotency_idx ON jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE TABLE job_errors (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    job_id      BIGINT      NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    attempt     INT         NOT NULL,
    error       TEXT        NOT NULL,
    worker_id   TEXT,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX job_errors_job_idx ON job_errors (job_id, attempt);

-- Wake idle workers when a job becomes runnable now. Delayed jobs are found by polling.
-- Notifications are delivered on commit, so a worker never sees an uncommitted job.
-- +goose StatementBegin
CREATE FUNCTION forgeq_notify_job() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.status IN ('pending', 'retrying') AND NEW.run_at <= now() THEN
        PERFORM pg_notify('forgeq_jobs', NEW.queue);
    END IF;
    RETURN NULL;
END;
$$;
-- +goose StatementEnd

CREATE TRIGGER jobs_notify
    AFTER INSERT OR UPDATE OF status ON jobs
    FOR EACH ROW EXECUTE FUNCTION forgeq_notify_job();

-- +goose Down
DROP TRIGGER IF EXISTS jobs_notify ON jobs;
DROP FUNCTION IF EXISTS forgeq_notify_job();
DROP TABLE IF EXISTS job_errors;
DROP TABLE IF EXISTS jobs;
