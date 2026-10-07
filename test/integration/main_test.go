//go:build integration

// Package integration runs ForgeQ against a real PostgreSQL started with
// testcontainers. Run with: go test -tags integration ./test/integration/...
package integration

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/migrations"
)

var (
	databaseURL string
	pool        *pgxpool.Pool
)

func TestMain(m *testing.M) {
	code, err := setup(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "integration setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func setup(m *testing.M) (int, error) {
	ctx := context.Background()
	ctr, err := postgres.Run(ctx, "postgres:18-alpine",
		postgres.WithDatabase("forgeq"),
		postgres.WithUsername("forgeq"),
		postgres.WithPassword("forgeq"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return 0, fmt.Errorf("start postgres: %w", err)
	}
	defer func() { _ = ctr.Terminate(ctx) }()

	databaseURL, err = ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return 0, err
	}
	if err := migrations.Up(ctx, databaseURL); err != nil {
		return 0, err
	}
	pool, err = store.Connect(ctx, databaseURL, 0)
	if err != nil {
		return 0, err
	}
	defer pool.Close()

	return m.Run(), nil
}

// newStore empties the tables and returns a Store. Tests share one database,
// so they must not call t.Parallel.
func newStore(t *testing.T) *store.Store {
	t.Helper()
	_, err := pool.Exec(context.Background(), `TRUNCATE jobs, job_errors RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return store.New(pool)
}

func enqueue(t *testing.T, s *store.Store, n job.NewJob) *job.Job {
	t.Helper()
	j, _, err := s.Enqueue(context.Background(), n)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return j
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// waitFor polls cond until it returns true or the timeout expires.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
