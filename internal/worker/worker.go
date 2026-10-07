// Package worker claims jobs from PostgreSQL and runs them on a bounded pool
// of goroutines per queue.
package worker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
)

// Store is the persistence the worker depends on; *store.Store implements it.
type Store interface {
	Claim(ctx context.Context, queue, workerID string, limit int, lease time.Duration) ([]*job.Job, error)
	Heartbeat(ctx context.Context, id int64, workerID string, lease time.Duration) error
	Complete(ctx context.Context, id int64, workerID string) error
	Fail(ctx context.Context, f store.Failure) (job.Status, error)
	Release(ctx context.Context, id int64, workerID string) error
}

// ErrShutdown is the cancellation cause seen by handlers that are still
// running when the worker's shutdown timeout expires.
var ErrShutdown = errors.New("worker shutting down")

// Config controls a Worker.
type Config struct {
	ID            string         // unique per process; recorded as the lease owner
	Queues        map[string]int // queue name -> number of concurrent jobs
	PollInterval  time.Duration  // fallback poll when no notification arrives
	LeaseDuration time.Duration  // how long a claim is valid without a heartbeat
	// HeartbeatInterval is how often a running job's lease is extended. It must
	// be well below LeaseDuration so one slow heartbeat does not lose the lease.
	HeartbeatInterval time.Duration
	JobTimeout        time.Duration // default per-job timeout
	// RetryDelay returns the delay before the next run after a failed attempt.
	// attempts is the number of attempts made so far (>= 1); see backoff.Policy.
	RetryDelay func(attempts int) time.Duration
	// ShutdownTimeout is how long Run waits for running jobs after ctx is
	// cancelled before cancelling them and releasing their leases.
	ShutdownTimeout time.Duration
	// Observer receives job events (e.g. for metrics); nil records nothing.
	Observer Observer
}

const (
	finishTimeout  = 10 * time.Second // budget for reporting a result to the store
	maxErrorLength = 4096             // longer handler errors are truncated
	// releaseGrace is how long cancelled handlers get to return so their jobs
	// can be released before Run gives up on them.
	releaseGrace = 5 * time.Second
)

// Worker runs registered handlers for jobs claimed from the store.
type Worker struct {
	cfg      Config
	store    Store
	registry *Registry
	log      *slog.Logger
	wake     map[string]chan struct{} // per queue; buffered(1) so wake-ups coalesce
}

// New returns a Worker. Call Run to start it.
func New(cfg Config, s Store, r *Registry, logger *slog.Logger) *Worker {
	if cfg.Observer == nil {
		cfg.Observer = nopObserver{}
	}
	wake := make(map[string]chan struct{}, len(cfg.Queues))
	for q := range cfg.Queues {
		wake[q] = make(chan struct{}, 1)
	}
	return &Worker{
		cfg:      cfg,
		store:    s,
		registry: r,
		log:      logger.With("worker_id", cfg.ID),
		wake:     wake,
	}
}

// Notify wakes the fetch loop of queue so it claims immediately instead of
// waiting for the next poll. It never blocks; unknown queues are ignored.
func (w *Worker) Notify(queue string) {
	ch, ok := w.wake[queue]
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
	default: // a wake-up is already pending
	}
}

// Run starts one fetch loop per queue and blocks until ctx is cancelled and
// the worker has shut down gracefully:
//
//  1. Cancelling ctx (e.g. on SIGTERM) stops claiming new jobs.
//  2. Running jobs get up to ShutdownTimeout to finish and report normally.
//  3. Jobs still running after that are cancelled (cause ErrShutdown); when
//     their handlers return, the jobs are released back to pending without
//     using up an attempt, so another worker picks them up right away.
//  4. A handler that ignores cancellation for releaseGrace is abandoned; its
//     lease expires and the reaper retries the job.
func (w *Worker) Run(ctx context.Context) error {
	if len(w.cfg.Queues) == 0 {
		return errors.New("worker: no queues configured")
	}
	w.log.Info("worker started", "queues", w.cfg.Queues, "types", w.registry.Types())

	// Jobs run under their own context, not ctx, so a shutdown signal only
	// stops claiming; jobs are cancelled explicitly once the timeout passes.
	jobsCtx, stopJobs := context.WithCancelCause(context.Background())
	defer stopJobs(nil)

	var fetchers, inflight sync.WaitGroup
	for queue, concurrency := range w.cfg.Queues {
		fetchers.Go(func() { w.runQueue(ctx, jobsCtx, &inflight, queue, concurrency) })
	}
	// Fetchers return once ctx is cancelled; after this no new job can start.
	fetchers.Wait()

	w.log.Info("shutting down: waiting for running jobs", "timeout", w.cfg.ShutdownTimeout)
	if !waitTimeout(&inflight, w.cfg.ShutdownTimeout) {
		w.log.Warn("shutdown timeout reached, cancelling running jobs")
		stopJobs(ErrShutdown)
		if !waitTimeout(&inflight, releaseGrace) {
			w.log.Error("some handlers ignored cancellation; the reaper will retry their jobs once the leases expire")
		}
	}
	w.log.Info("worker stopped")
	return nil
}

// waitTimeout waits for wg up to d and reports whether it finished in time.
func waitTimeout(wg *sync.WaitGroup, d time.Duration) bool {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-done:
		return true
	case <-t.C:
		return false
	}
}

// runQueue is the fetch loop for one queue. It claims only as many jobs as it
// has free slots, so a busy worker leaves jobs for other instances. It returns
// when ctx is cancelled; started jobs keep running under jobsCtx.
func (w *Worker) runQueue(ctx, jobsCtx context.Context, inflight *sync.WaitGroup, queue string, concurrency int) {
	slots := make(chan struct{}, concurrency) // semaphore: one token per running job
	freed := make(chan struct{}, 1)           // signalled when a job finishes
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		free := concurrency - len(slots)
		claimed := 0
		if free > 0 {
			jobs, err := w.store.Claim(ctx, queue, w.cfg.ID, free, w.cfg.LeaseDuration)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				w.log.Error("claim failed", "queue", queue, "err", err)
			}
			claimed = len(jobs)
			for _, j := range jobs {
				slots <- struct{}{}
				inflight.Go(func() {
					defer func() {
						<-slots
						select {
						case freed <- struct{}{}:
						default:
						}
					}()
					w.process(jobsCtx, j)
				})
			}
		}

		if ctx.Err() != nil {
			return
		}
		// A full batch suggests more work is waiting: claim again immediately.
		if claimed > 0 && claimed == free {
			continue
		}

		// Only wake on a finished job when the pool is full; otherwise the
		// queue is drained and we wait for a notification or the next poll.
		var freedCh <-chan struct{}
		if len(slots) == concurrency {
			freedCh = freed
		}
		timer.Reset(w.cfg.PollInterval)
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-w.wake[queue]:
		case <-freedCh:
		}
	}
}

// process runs one claimed job while a heartbeat keeps its lease alive, then
// reports the outcome.
func (w *Worker) process(base context.Context, j *job.Job) {
	log := w.log.With("job_id", j.ID, "queue", j.Queue, "type", j.Type, "attempt", j.Attempts)
	start := time.Now()
	w.cfg.Observer.JobStarted(j.Queue)

	// leaseCtx is cancelled with cause store.ErrLeaseLost when a heartbeat
	// finds that this worker no longer owns the job.
	leaseCtx, loseLease := context.WithCancelCause(base)
	defer loseLease(nil)

	hbCtx, stopHeartbeat := context.WithCancel(leaseCtx)
	hbDone := make(chan struct{})
	go func() {
		defer close(hbDone)
		w.heartbeat(hbCtx, j, loseLease, log)
	}()

	err := w.execute(leaseCtx, j)
	// Stop the heartbeat before reporting so it cannot race with Complete/Fail.
	stopHeartbeat()
	<-hbDone

	took := time.Since(start)
	var outcome Outcome
	switch cause := context.Cause(leaseCtx); {
	case errors.Is(cause, store.ErrLeaseLost):
		// Another worker may be running the job now; any write from us would
		// be rejected by the fence anyway, so do not even try.
		log.Warn("lease lost while running; result discarded", "err", err, "took", took)
		outcome = OutcomeLeaseLost
	case err != nil && errors.Is(cause, ErrShutdown):
		// Interrupted by shutdown, not by a fault of the job: hand it back.
		outcome = w.release(j, log)
	default:
		// Includes a handler that finished successfully despite the shutdown.
		outcome = w.finish(j, err, took, log)
	}
	w.cfg.Observer.JobFinished(j.Queue, w.typeLabel(j.Type), outcome, took)
}

// typeLabel bounds metric cardinality: job types come from clients, so only
// registered types are reported by name.
func (w *Worker) typeLabel(jobType string) string {
	if _, ok := w.registry.lookup(jobType); ok {
		return jobType
	}
	return "unknown"
}

// release returns an interrupted job to pending without using up an attempt.
func (w *Worker) release(j *job.Job, log *slog.Logger) Outcome {
	ctx, cancel := context.WithTimeout(context.Background(), finishTimeout)
	defer cancel()
	err := w.store.Release(ctx, j.ID, w.cfg.ID)
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		log.Warn("could not release job: lease already lost")
		return OutcomeLeaseLost
	case err != nil:
		log.Error("could not release job; the reaper will retry it once the lease expires", "err", err)
		return OutcomeError
	default:
		log.Info("job released for another worker")
		return OutcomeReleased
	}
}

// heartbeat extends the job's lease every HeartbeatInterval until ctx is done.
// The update is fenced on (status='running', locked_by=this worker): if it
// matches no row, the reaper has requeued the job (and maybe another worker
// claimed it), so the job is cancelled via lose.
func (w *Worker) heartbeat(ctx context.Context, j *job.Job, lose context.CancelCauseFunc, log *slog.Logger) {
	t := time.NewTicker(w.cfg.HeartbeatInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		hbCtx, cancel := context.WithTimeout(ctx, w.cfg.HeartbeatInterval)
		err := w.store.Heartbeat(hbCtx, j.ID, w.cfg.ID, w.cfg.LeaseDuration)
		cancel()
		switch {
		case errors.Is(err, store.ErrLeaseLost):
			log.Warn("lease lost, cancelling job")
			lose(store.ErrLeaseLost)
			return
		case err != nil && ctx.Err() == nil:
			// Transient error: keep trying. If the lease expires in the
			// meantime the reaper requeues the job and the next heartbeat
			// gets ErrLeaseLost.
			log.Warn("heartbeat failed", "err", err)
		}
	}
}

// execute calls the handler with a per-job timeout, converting panics into errors.
func (w *Worker) execute(ctx context.Context, j *job.Job) (err error) {
	h, ok := w.registry.lookup(j.Type)
	if !ok {
		return Permanent(fmt.Errorf("no handler registered for job type %q", j.Type))
	}
	timeout := cmp.Or(h.timeout, w.cfg.JobTimeout)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()

	err = h.fn(ctx, j)
	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		err = fmt.Errorf("job timed out after %s: %w", timeout, err)
	}
	return err
}

// finish records the result in the store. The store calls use their own
// context so a result is still saved while the worker is shutting down.
func (w *Worker) finish(j *job.Job, jobErr error, took time.Duration, log *slog.Logger) Outcome {
	ctx, cancel := context.WithTimeout(context.Background(), finishTimeout)
	defer cancel()

	if jobErr == nil {
		err := w.store.Complete(ctx, j.ID, w.cfg.ID)
		switch {
		case errors.Is(err, store.ErrLeaseLost):
			log.Warn("job finished but lease was lost; result discarded", "took", took)
			return OutcomeLeaseLost
		case err != nil:
			log.Error("could not mark job succeeded", "err", err)
			return OutcomeError
		default:
			log.Info("job succeeded", "took", took)
			return OutcomeSucceeded
		}
	}

	retryAt := time.Now().Add(w.cfg.RetryDelay(j.Attempts))
	status, err := w.store.Fail(ctx, store.Failure{
		JobID:     j.ID,
		WorkerID:  w.cfg.ID,
		Error:     truncate(jobErr.Error(), maxErrorLength),
		RetryAt:   retryAt,
		Permanent: IsPermanent(jobErr),
	})
	switch {
	case errors.Is(err, store.ErrLeaseLost):
		log.Warn("job failed but lease was lost; result discarded", "err", jobErr, "took", took)
		return OutcomeLeaseLost
	case err != nil:
		log.Error("could not record job failure", "job_err", jobErr, "err", err)
		return OutcomeError
	case status == job.StatusDead:
		log.Error("job dead", "err", jobErr, "took", took)
		return OutcomeDead
	default:
		log.Warn("job failed, will retry", "err", jobErr, "retry_at", retryAt, "took", took)
		return OutcomeRetrying
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	// Cut on a rune boundary: PostgreSQL rejects invalid UTF-8.
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return s[:n] + "...(truncated)"
}
