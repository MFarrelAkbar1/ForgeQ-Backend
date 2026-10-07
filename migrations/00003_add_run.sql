-- A job's "run" counts manual retries from the dead-letter queue: it starts
-- at 1 and is incremented each time a dead job is retried. Each error row
-- records the run it belongs to, so the history reads "run 2, attempt 1"
-- instead of showing "attempt 1" twice.
--
-- ADD COLUMN with a constant default is a metadata-only change (PostgreSQL 11+),
-- so it does not rewrite the table.

-- +goose Up
ALTER TABLE jobs ADD COLUMN run INT NOT NULL DEFAULT 1 CHECK (run > 0);
ALTER TABLE job_errors ADD COLUMN run INT NOT NULL DEFAULT 1;

DROP INDEX job_errors_job_idx;
CREATE INDEX job_errors_job_idx ON job_errors (job_id, run, attempt);

-- +goose Down
DROP INDEX job_errors_job_idx;
CREATE INDEX job_errors_job_idx ON job_errors (job_id, attempt);
ALTER TABLE job_errors DROP COLUMN run;
ALTER TABLE jobs DROP COLUMN run;
