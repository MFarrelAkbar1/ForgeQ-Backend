// Package store is the PostgreSQL persistence layer for jobs. Every state
// transition is a single guarded SQL statement (or one transaction), so the
// database stays the only source of truth even with many API and worker
// processes running concurrently.
package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
)

// NotifyChannel is the LISTEN/NOTIFY channel workers listen on for wake-ups
// (sent by Notifier). The payload is the queue name.
const NotifyChannel = "forgeq_jobs"

var (
	// ErrNotFound is returned when a job does not exist.
	ErrNotFound = errors.New("job not found")
	// ErrLeaseLost is returned when a worker acts on a job it no longer owns,
	// e.g. because the lease expired and the reaper handed it to someone else.
	ErrLeaseLost = errors.New("job lease lost")
	// ErrInvalidState is returned when an operation is not allowed in the
	// job's current status, e.g. retrying a job that is not dead.
	ErrInvalidState = errors.New("operation not allowed in the job's current status")
)

// Store implements job persistence on top of a pgx connection pool.
type Store struct {
	pool   *pgxpool.Pool
	notify func(queue string)
}

// Option configures a Store.
type Option func(*Store)

// WithNotifier makes the Store wake workers (through n) after every change
// that makes a job runnable now: enqueue, manual retry, release and reap.
func WithNotifier(n *Notifier) Option {
	return func(s *Store) { s.notify = n.Notify }
}

// New returns a Store using pool.
func New(pool *pgxpool.Pool, opts ...Option) *Store {
	s := &Store{pool: pool, notify: func(string) {}}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Connect opens a connection pool and verifies it with a ping.
// maxConns caps the pool (0 keeps pgx's default of max(4, NumCPU)). Every
// process holds up to maxConns connections, so replicas x maxConns must fit
// within the server's max_connections.
func Connect(ctx context.Context, databaseURL string, maxConns int32) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return pool, nil
}

const jobColumns = `id, queue, type, payload, priority, status, run, attempts, max_attempts,
	run_at, locked_by, locked_until, last_error, idempotency_key,
	created_at, updated_at, started_at, finished_at`

// prefixed qualifies each column in a comma-separated list with prefix.
func prefixed(prefix, columns string) string {
	cols := strings.Split(columns, ",")
	for i, c := range cols {
		cols[i] = prefix + strings.TrimSpace(c)
	}
	return strings.Join(cols, ", ")
}

func scanJob(row pgx.Row) (*job.Job, error) {
	var j job.Job
	err := row.Scan(
		&j.ID, &j.Queue, &j.Type, &j.Payload, &j.Priority, &j.Status, &j.Run, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LockedBy, &j.LockedUntil, &j.LastError, &j.IdempotencyKey,
		&j.CreatedAt, &j.UpdatedAt, &j.StartedAt, &j.FinishedAt,
	)
	if err != nil {
		return nil, err
	}
	return &j, nil
}

func collectJobs(rows pgx.Rows) ([]*job.Job, error) {
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (*job.Job, error) {
		return scanJob(r)
	})
}

// Enqueue inserts a new job. If n.IdempotencyKey is set and a job with the
// same (queue, key) already exists, that job is returned with created=false.
//
// The insert uses ON CONFLICT DO NOTHING against the partial unique index, so
// concurrent requests with the same key cannot both insert: the loser waits
// for the winner to commit, inserts nothing, and then reads the winner's row.
func (s *Store) Enqueue(ctx context.Context, n job.NewJob) (j *job.Job, created bool, err error) {
	if err := n.Normalize(); err != nil {
		return nil, false, err
	}
	var key *string
	if n.IdempotencyKey != "" {
		key = &n.IdempotencyKey
	}

	const insert = `
		INSERT INTO jobs (queue, type, payload, priority, max_attempts, run_at, idempotency_key)
		VALUES ($1, $2, $3, $4, $5, COALESCE($6, now()), $7)
		ON CONFLICT (queue, idempotency_key) WHERE idempotency_key IS NOT NULL DO NOTHING
		RETURNING ` + jobColumns

	const existing = `SELECT ` + jobColumns + ` FROM jobs WHERE queue = $1 AND idempotency_key = $2`

	// The existing row can vanish between the two statements (deleted via the
	// API or by retention), so retry a few times instead of failing.
	for range 3 {
		j, err = scanJob(s.pool.QueryRow(ctx, insert,
			n.Queue, n.Type, n.Payload, n.Priority, n.MaxAttempts, n.RunAt, key))
		if err == nil {
			// Delayed jobs are found by polling. Decide from the request, not
			// from j.RunAt: that is the database clock, and comparing it with
			// this process's clock skips wake-ups whenever the DB runs ahead.
			if n.RunAt == nil || !n.RunAt.After(time.Now()) {
				s.notify(j.Queue)
			}
			return j, true, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, false, fmt.Errorf("insert job: %w", err)
		}

		j, err = scanJob(s.pool.QueryRow(ctx, existing, n.Queue, n.IdempotencyKey))
		if err == nil {
			return j, false, nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, false, fmt.Errorf("load existing job: %w", err)
		}
	}
	return nil, false, errors.New("enqueue: idempotency key conflict did not resolve")
}

// Get returns the job with the given id.
func (s *Store) Get(ctx context.Context, id int64) (*job.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `SELECT `+jobColumns+` FROM jobs WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get job %d: %w", id, err)
	}
	return j, nil
}

// ListFilter selects jobs for List. Zero fields mean "no filter".
type ListFilter struct {
	Queue  string
	Status job.Status
	Cursor int64 // return jobs with id < Cursor (keyset pagination, newest first)
	Limit  int
}

// List returns jobs newest first.
func (s *Store) List(ctx context.Context, f ListFilter) ([]*job.Job, error) {
	const q = `
		SELECT ` + jobColumns + ` FROM jobs
		WHERE ($1 = '' OR queue = $1)
		  AND ($2 = '' OR status = $2)
		  AND ($3 = 0 OR id < $3)
		ORDER BY id DESC
		LIMIT $4`
	rows, err := s.pool.Query(ctx, q, f.Queue, string(f.Status), f.Cursor, f.Limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	return jobs, nil
}

// RetryDead moves a dead job back to pending with a fresh attempt budget and
// the next run number, so it runs again as soon as possible. Its rows in job_errors are kept, so the
// earlier failures stay visible. It returns ErrNotFound, or ErrInvalidState
// when the job is not dead.
func (s *Store) RetryDead(ctx context.Context, id int64) (*job.Job, error) {
	j, err := scanJob(s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'pending', run = run + 1, attempts = 0, run_at = now(), last_error = NULL,
		    started_at = NULL, finished_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'dead'
		RETURNING `+jobColumns, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, s.notFoundOrInvalid(ctx, id)
	}
	if err != nil {
		return nil, fmt.Errorf("retry job %d: %w", id, err)
	}
	s.notify(j.Queue)
	return j, nil
}

// Delete removes a dead or succeeded job together with its error history.
// Jobs that may still run are refused with ErrInvalidState, so a worker never
// has a job deleted from under it.
func (s *Store) Delete(ctx context.Context, id int64) error {
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM jobs WHERE id = $1 AND status IN ('dead', 'succeeded')`, id)
	if err != nil {
		return fmt.Errorf("delete job %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return s.notFoundOrInvalid(ctx, id)
	}
	return nil
}

// notFoundOrInvalid explains why a status-guarded statement on id matched no row.
func (s *Store) notFoundOrInvalid(ctx context.Context, id int64) error {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM jobs WHERE id = $1)`, id).Scan(&exists)
	if err != nil {
		return fmt.Errorf("check job %d: %w", id, err)
	}
	if !exists {
		return ErrNotFound
	}
	return ErrInvalidState
}

// Errors returns the error history of a job, oldest attempt first.
func (s *Store) Errors(ctx context.Context, jobID int64) ([]job.AttemptError, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT run, attempt, error, worker_id, occurred_at
		FROM job_errors WHERE job_id = $1 ORDER BY id`, jobID)
	if err != nil {
		return nil, fmt.Errorf("list job errors: %w", err)
	}
	errs, err := pgx.CollectRows(rows, pgx.RowToStructByPos[job.AttemptError])
	if err != nil {
		return nil, fmt.Errorf("list job errors: %w", err)
	}
	return errs, nil
}

var claimQuery = `
	WITH next AS (
		SELECT id FROM jobs
		WHERE queue = $1
		  AND status IN ('pending', 'retrying')
		  AND run_at <= now()
		ORDER BY priority DESC, run_at, id
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	)
	UPDATE jobs j
	SET status       = 'running',
	    attempts     = j.attempts + 1,
	    locked_by    = $3,
	    locked_until = now() + $4 * interval '1 millisecond',
	    started_at   = now(),
	    updated_at   = now()
	FROM next
	WHERE j.id = next.id
	RETURNING ` + prefixed("j.", jobColumns)

// Claim atomically takes up to limit runnable jobs from queue for workerID and
// leases them for lease. FOR UPDATE SKIP LOCKED makes concurrent claimers skip
// rows another transaction is already claiming instead of blocking on them, so
// each job is handed to exactly one worker.
//
// attempts is incremented here, not on failure, so a job whose worker crashes
// still uses up an attempt and a poison job cannot loop forever.
func (s *Store) Claim(ctx context.Context, queue, workerID string, limit int, lease time.Duration) ([]*job.Job, error) {
	rows, err := s.pool.Query(ctx, claimQuery, queue, limit, workerID, lease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	jobs, err := collectJobs(rows)
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	return jobs, nil
}

// Complete marks a running job owned by workerID as succeeded.
// It returns ErrLeaseLost if the worker no longer owns the job.
func (s *Store) Complete(ctx context.Context, id int64, workerID string) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs
		SET status = 'succeeded', locked_by = NULL, locked_until = NULL,
		    finished_at = now(), updated_at = now()
		WHERE id = $1 AND status = 'running' AND locked_by = $2`, id, workerID)
	if err != nil {
		return fmt.Errorf("complete job %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Heartbeat extends the lease of a running job owned by workerID to now+lease.
// It returns ErrLeaseLost if the worker no longer owns the job, which tells the
// worker to stop processing it.
func (s *Store) Heartbeat(ctx context.Context, id int64, workerID string, lease time.Duration) error {
	tag, err := s.pool.Exec(ctx, `
		UPDATE jobs SET locked_until = now() + $3 * interval '1 millisecond'
		WHERE id = $1 AND status = 'running' AND locked_by = $2`,
		id, workerID, lease.Milliseconds())
	if err != nil {
		return fmt.Errorf("heartbeat job %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Release hands a running job back to the queue during graceful shutdown. The
// attempt is not counted (attempts is decremented) because the job did not
// fail; the worker just stopped. It returns ErrLeaseLost if the worker no
// longer owns the job.
func (s *Store) Release(ctx context.Context, id int64, workerID string) error {
	var queue string
	err := s.pool.QueryRow(ctx, `
		UPDATE jobs
		SET status = 'pending', attempts = attempts - 1, run_at = now(),
		    locked_by = NULL, locked_until = NULL, started_at = NULL, updated_at = now()
		WHERE id = $1 AND status = 'running' AND locked_by = $2
		RETURNING queue`, id, workerID).Scan(&queue)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrLeaseLost
	}
	if err != nil {
		return fmt.Errorf("release job %d: %w", id, err)
	}
	s.notify(queue)
	return nil
}

// Failure describes a failed attempt reported by a worker.
type Failure struct {
	JobID     int64
	WorkerID  string
	Error     string
	RetryAt   time.Time // when to run again if attempts remain
	Permanent bool      // skip remaining attempts and go straight to dead
}

// Fail records a failed attempt. The job moves to retrying (run again at
// f.RetryAt) or to dead when it is out of attempts or the failure is
// permanent. The new status is returned. It returns ErrLeaseLost if the
// worker no longer owns the job.
func (s *Store) Fail(ctx context.Context, f Failure) (job.Status, error) {
	var status job.Status
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		var run, attempt int
		err := tx.QueryRow(ctx, `
			UPDATE jobs
			SET status = CASE WHEN $4 OR attempts >= max_attempts THEN 'dead' ELSE 'retrying' END,
			    run_at = CASE WHEN $4 OR attempts >= max_attempts THEN run_at ELSE $5 END,
			    finished_at = CASE WHEN $4 OR attempts >= max_attempts THEN now() END,
			    last_error = $3, locked_by = NULL, locked_until = NULL, updated_at = now()
			WHERE id = $1 AND status = 'running' AND locked_by = $2
			RETURNING status, run, attempts`,
			f.JobID, f.WorkerID, f.Error, f.Permanent, f.RetryAt).Scan(&status, &run, &attempt)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrLeaseLost
		}
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO job_errors (job_id, run, attempt, error, worker_id) VALUES ($1, $2, $3, $4, $5)`,
			f.JobID, run, attempt, f.Error, f.WorkerID)
		return err
	})
	if errors.Is(err, ErrLeaseLost) {
		return "", err
	}
	if err != nil {
		return "", fmt.Errorf("fail job %d: %w", f.JobID, err)
	}
	return status, nil
}

// ReapedJob is a job whose lease expired and that the reaper took back.
type ReapedJob struct {
	ID       int64
	Queue    string
	Status   job.Status // retrying, or dead if it was out of attempts
	Attempts int
	WorkerID string // the worker that held the expired lease
}

// LeaseExpiredError is recorded in the error history of reaped jobs.
const LeaseExpiredError = "lease expired: worker crashed or stalled"

// reapQuery requeues up to $1 running jobs whose lease has expired, in one
// atomic statement: lock the expired rows (skipping rows a worker is updating
// right now), move them to retrying/dead, and log the attempt in job_errors.
// Row locking re-checks locked_until, so a heartbeat that lands first wins.
const reapQuery = `
	WITH expired AS (
		SELECT id, locked_by FROM jobs
		WHERE status = 'running' AND locked_until < now()
		ORDER BY locked_until
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	), reaped AS (
		UPDATE jobs j
		SET status       = CASE WHEN j.attempts >= j.max_attempts THEN 'dead' ELSE 'retrying' END,
		    finished_at  = CASE WHEN j.attempts >= j.max_attempts THEN now() END,
		    run_at       = now(),
		    last_error   = $2,
		    locked_by    = NULL,
		    locked_until = NULL,
		    updated_at   = now()
		FROM expired
		WHERE j.id = expired.id
		RETURNING j.id, j.queue, j.status, j.run, j.attempts, expired.locked_by
	), logged AS (
		INSERT INTO job_errors (job_id, run, attempt, error, worker_id)
		SELECT id, run, attempts, $2, locked_by FROM reaped
	)
	SELECT id, queue, status, attempts, locked_by FROM reaped`

// Reap takes back up to limit running jobs whose lease has expired. Jobs with
// attempts left become retrying and run again immediately; the rest go dead.
// Safe to call from many processes at once.
func (s *Store) Reap(ctx context.Context, limit int) ([]ReapedJob, error) {
	rows, err := s.pool.Query(ctx, reapQuery, limit, LeaseExpiredError)
	if err != nil {
		return nil, fmt.Errorf("reap expired leases: %w", err)
	}
	reaped, err := pgx.CollectRows(rows, pgx.RowToStructByPos[ReapedJob])
	if err != nil {
		return nil, fmt.Errorf("reap expired leases: %w", err)
	}
	for _, j := range reaped {
		if j.Status == job.StatusRetrying {
			s.notify(j.Queue)
		}
	}
	return reaped, nil
}

// DeleteSucceededBefore deletes up to limit succeeded jobs that finished before
// cutoff, returning how many were deleted. Their job_errors rows cascade.
// Deleting in bounded batches keeps each transaction and its locks short.
func (s *Store) DeleteSucceededBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM jobs WHERE id IN (
			SELECT id FROM jobs
			WHERE status = 'succeeded' AND finished_at < $1
			ORDER BY finished_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)`, cutoff, limit)
	if err != nil {
		return 0, fmt.Errorf("delete old succeeded jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}

// QueueStat is the number of jobs in one queue with one status.
type QueueStat struct {
	Queue  string
	Status job.Status
	Count  int64
}

// Stats counts jobs per queue and status, ordered by queue. It is backed by
// jobs_queue_status_idx; retention keeps the succeeded count bounded.
func (s *Store) Stats(ctx context.Context) ([]QueueStat, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT queue, status, count(*) FROM jobs GROUP BY queue, status ORDER BY queue, status`)
	if err != nil {
		return nil, fmt.Errorf("job stats: %w", err)
	}
	stats, err := pgx.CollectRows(rows, pgx.RowToStructByPos[QueueStat])
	if err != nil {
		return nil, fmt.Errorf("job stats: %w", err)
	}
	return stats, nil
}
