package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
)

// ReaperStore is the persistence the reaper depends on; *store.Store implements it.
type ReaperStore interface {
	Reap(ctx context.Context, limit int) ([]store.ReapedJob, error)
	DeleteSucceededBefore(ctx context.Context, cutoff time.Time, limit int) (int64, error)
}

// ReaperConfig controls a Reaper.
type ReaperConfig struct {
	Interval  time.Duration // how often to run
	Retention time.Duration // succeeded jobs older than this are deleted
	Observer  Observer      // optional; nil records nothing
}

const reaperBatch = 1000

// Reaper periodically recovers jobs whose worker died (expired lease) and
// deletes old succeeded jobs. Every worker process runs one; the queries use
// SKIP LOCKED so concurrent reapers split the work instead of conflicting.
type Reaper struct {
	cfg   ReaperConfig
	store ReaperStore
	log   *slog.Logger
	now   func() time.Time
}

// NewReaper returns a Reaper. Call Run to start it.
func NewReaper(cfg ReaperConfig, s ReaperStore, logger *slog.Logger) *Reaper {
	if cfg.Observer == nil {
		cfg.Observer = nopObserver{}
	}
	return &Reaper{cfg: cfg, store: s, log: logger.With("component", "reaper"), now: time.Now}
}

// Run calls RunOnce immediately and then every Interval until ctx is done.
func (r *Reaper) Run(ctx context.Context) {
	t := time.NewTicker(r.cfg.Interval)
	defer t.Stop()
	for {
		r.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// RunOnce does a single reap and retention pass. Errors are logged, not
// returned: both passes are retried on the next tick.
func (r *Reaper) RunOnce(ctx context.Context) {
	r.reapExpired(ctx)
	r.deleteOldSucceeded(ctx)
}

func (r *Reaper) reapExpired(ctx context.Context) {
	for ctx.Err() == nil {
		reaped, err := r.store.Reap(ctx, reaperBatch)
		if err != nil {
			if ctx.Err() == nil {
				r.log.Error("reap failed", "err", err)
			}
			return
		}
		for _, j := range reaped {
			r.cfg.Observer.JobReaped(j.Status)
			r.log.Warn("lease expired, job taken back",
				"job_id", j.ID, "status", j.Status, "attempt", j.Attempts, "stale_worker_id", j.WorkerID)
		}
		if len(reaped) < reaperBatch {
			return
		}
	}
}

func (r *Reaper) deleteOldSucceeded(ctx context.Context) {
	cutoff := r.now().Add(-r.cfg.Retention)
	var total int64
	for ctx.Err() == nil {
		n, err := r.store.DeleteSucceededBefore(ctx, cutoff, reaperBatch)
		total += n
		if err != nil {
			if ctx.Err() == nil {
				r.log.Error("retention cleanup failed", "err", err)
			}
			break
		}
		if n < reaperBatch {
			break
		}
	}
	if total > 0 {
		r.cfg.Observer.JobsDeleted(total)
		r.log.Info("deleted old succeeded jobs", "count", total, "finished_before", cutoff)
	}
}
