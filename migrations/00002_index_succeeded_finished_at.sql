-- +goose NO TRANSACTION
-- CONCURRENTLY avoids locking jobs against writes while the index builds,
-- but cannot run inside a transaction.

-- +goose Up
-- Retention path: find old succeeded jobs without scanning the whole table.
CREATE INDEX CONCURRENTLY IF NOT EXISTS jobs_succeeded_finished_idx
    ON jobs (finished_at) WHERE status = 'succeeded';

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS jobs_succeeded_finished_idx;
