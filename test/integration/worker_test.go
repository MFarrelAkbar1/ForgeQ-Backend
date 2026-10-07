//go:build integration

package integration

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/worker"
)

// TestWorkerEndToEnd runs two worker instances against the same queue and
// checks that every job runs exactly once and failures are recorded.
func TestWorkerEndToEnd(t *testing.T) {
	s := newStore(t)
	const okJobs = 50

	var runs atomic.Int64
	reg := worker.NewRegistry()
	reg.RegisterHandler("ok", func(context.Context, *job.Job) error {
		runs.Add(1)
		time.Sleep(2 * time.Millisecond)
		return nil
	})
	reg.RegisterHandler("broken", func(context.Context, *job.Job) error {
		return worker.Permanent(errors.New("cannot process"))
	})

	for range okJobs {
		enqueue(t, s, job.NewJob{Type: "ok"})
	}
	broken := enqueue(t, s, job.NewJob{Type: "broken"})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{}, 2)
	for _, id := range []string{"w1", "w2"} {
		w := worker.New(worker.Config{
			ID:                id,
			Queues:            map[string]int{"default": 4},
			PollInterval:      50 * time.Millisecond,
			LeaseDuration:     lease,
			HeartbeatInterval: time.Second,
			JobTimeout:        5 * time.Second,
			RetryDelay:        func(int) time.Duration { return time.Minute },
			ShutdownTimeout:   5 * time.Second,
		}, s, reg, discardLogger())
		go func() {
			_ = w.Run(ctx)
			done <- struct{}{}
		}()
	}

	waitFor(t, 15*time.Second, "all jobs to finish", func() bool {
		return countByStatus(t, job.StatusSucceeded) == okJobs && countByStatus(t, job.StatusDead) == 1
	})
	cancel()
	<-done
	<-done

	if got := runs.Load(); got != okJobs {
		t.Errorf("handler ran %d times, want %d", got, okJobs)
	}
	j, err := s.Get(context.Background(), broken.ID)
	if err != nil {
		t.Fatal(err)
	}
	if j.LastError == nil || *j.LastError != "cannot process" {
		t.Errorf("dead job last_error = %v", j.LastError)
	}
}

func countByStatus(t *testing.T, status job.Status) int {
	t.Helper()
	var n int
	err := pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs WHERE status = $1`, status).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestWorkerKeepsLeaseAliveAndStopsWhenTakenOver runs a job longer than its
// lease: heartbeats must keep it owned, and when ownership moves to another
// worker the handler's ctx must be cancelled with ErrLeaseLost and the stale
// worker must not overwrite the new owner's state.
func TestWorkerKeepsLeaseAliveAndStopsWhenTakenOver(t *testing.T) {
	s := newStore(t)
	const shortLease = time.Second

	started := make(chan struct{})
	causes := make(chan error, 1)
	reg := worker.NewRegistry()
	reg.RegisterHandler("long", func(ctx context.Context, _ *job.Job) error {
		close(started)
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return ctx.Err()
	})
	j := enqueue(t, s, job.NewJob{Type: "long"})

	w := worker.New(worker.Config{
		ID:                "w1",
		Queues:            map[string]int{"default": 1},
		PollInterval:      50 * time.Millisecond,
		LeaseDuration:     shortLease,
		HeartbeatInterval: 100 * time.Millisecond,
		JobTimeout:        time.Minute,
		RetryDelay:        func(int) time.Duration { return time.Minute },
		ShutdownTimeout:   5 * time.Second,
	}, s, reg, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	<-started
	time.Sleep(3 * shortLease) // well past the original lease
	got, err := s.Get(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != job.StatusRunning || *got.LockedBy != "w1" || !got.LockedUntil.After(time.Now()) {
		t.Fatalf("heartbeat did not keep the lease: %+v", got)
	}

	// Simulate another worker taking the job over.
	if _, err := pool.Exec(ctx, `UPDATE jobs SET locked_by = 'w2' WHERE id = $1`, j.ID); err != nil {
		t.Fatal(err)
	}

	select {
	case cause := <-causes:
		if !errors.Is(cause, store.ErrLeaseLost) {
			t.Fatalf("cause = %v, want ErrLeaseLost", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale worker's job was not cancelled")
	}

	time.Sleep(200 * time.Millisecond) // give a buggy worker time to write
	got, _ = s.Get(ctx, j.ID)
	if got.Status != job.StatusRunning || *got.LockedBy != "w2" {
		t.Errorf("stale worker overwrote the new owner's job: %+v", got)
	}
}
