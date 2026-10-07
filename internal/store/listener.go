package store

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// Listen subscribes to NotifyChannel on a dedicated connection (not one from
// the pool, since LISTEN state is tied to a session and WaitForNotification
// holds the connection indefinitely) and calls onNotify with each payload.
//
// Notifications are only a latency optimization: workers keep polling, so
// a lost connection merely delays jobs until the next poll. Listen therefore
// reconnects with backoff and never returns an error. It blocks until ctx is done.
func Listen(ctx context.Context, databaseURL string, logger *slog.Logger, onNotify func(payload string)) {
	const (
		minBackoff = time.Second
		maxBackoff = 30 * time.Second
	)
	backoff := minBackoff
	for ctx.Err() == nil {
		err := listenOnce(ctx, databaseURL, func(payload string) {
			backoff = minBackoff // the connection works, reset backoff
			onNotify(payload)
		})
		if ctx.Err() != nil {
			return
		}
		logger.Warn("notification listener disconnected, relying on polling",
			"err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

func listenOnce(ctx context.Context, databaseURL string, onNotify func(string)) error {
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(context.WithoutCancel(ctx))

	if _, err := conn.Exec(ctx, "LISTEN "+NotifyChannel); err != nil {
		return fmt.Errorf("listen: %w", err)
	}
	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			return fmt.Errorf("wait for notification: %w", err)
		}
		onNotify(n.Payload)
	}
}
