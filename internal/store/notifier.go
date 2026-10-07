package store

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Notifier sends worker wake-ups on NotifyChannel, coalesced per queue.
//
// Why not NOTIFY inside the job-writing transaction (e.g. from a trigger)?
// A transaction that issued NOTIFY takes a global lock while it commits, so
// every enqueue would commit one after another and Postgres could no longer
// group their WAL flushes; measured enqueue throughput fell ~20x. Instead,
// writers call Notify after their transaction committed, and one goroutine
// sends at most one pg_notify per queue per window from its own transaction.
//
// The first wake-up in a quiet period is sent immediately; later ones within
// the window are merged into a single notification at the end of it. Lost
// notifications are harmless because workers also poll.
type Notifier struct {
	pool   *pgxpool.Pool
	window time.Duration
	log    *slog.Logger

	mu      sync.Mutex
	pending map[string]struct{}
	kick    chan struct{} // buffered(1): "something is pending"
}

// NewNotifier returns a Notifier; call Run to start sending.
func NewNotifier(pool *pgxpool.Pool, window time.Duration, logger *slog.Logger) *Notifier {
	return &Notifier{
		pool:    pool,
		window:  window,
		log:     logger,
		pending: map[string]struct{}{},
		kick:    make(chan struct{}, 1),
	}
}

// Notify schedules a wake-up for queue. It never blocks.
func (n *Notifier) Notify(queue string) {
	n.mu.Lock()
	n.pending[queue] = struct{}{}
	n.mu.Unlock()
	select {
	case n.kick <- struct{}{}:
	default:
	}
}

// Run sends pending wake-ups until ctx is done, then flushes once more so
// wake-ups recorded during shutdown (e.g. released jobs) still go out.
func (n *Notifier) Run(ctx context.Context) {
	defer func() {
		flushCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		n.flush(flushCtx)
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.kick:
		}
		n.flush(ctx)

		// Hold off for one window; Notify calls meanwhile accumulate and are
		// sent together on the next iteration.
		select {
		case <-ctx.Done():
			return
		case <-time.After(n.window):
		}
	}
}

func (n *Notifier) flush(ctx context.Context) {
	n.mu.Lock()
	queues := make([]string, 0, len(n.pending))
	for q := range n.pending {
		queues = append(queues, q)
	}
	clear(n.pending)
	n.mu.Unlock()
	if len(queues) == 0 {
		return
	}
	_, err := n.pool.Exec(ctx, `SELECT pg_notify($1, q) FROM unnest($2::text[]) AS q`, NotifyChannel, queues)
	if err != nil && ctx.Err() == nil {
		n.log.Warn("could not send wake-up notification; workers will poll", "queues", queues, "err", err)
	}
}
