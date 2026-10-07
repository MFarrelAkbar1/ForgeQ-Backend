-- The per-row NOTIFY trigger made every enqueue transaction take PostgreSQL's
-- global notification lock at commit, serializing commits (and their WAL
-- flushes): enqueue throughput dropped from ~3000/s to ~150/s. Wake-ups are
-- now sent by store.Notifier from its own transaction, coalesced per queue.

-- +goose Up
DROP TRIGGER IF EXISTS jobs_notify ON jobs;
DROP FUNCTION IF EXISTS forgeq_notify_job();

-- +goose Down
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
