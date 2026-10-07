//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/worker"
)

func shutdownWorker(s *store.Store, reg *worker.Registry, shutdownTimeout time.Duration) *worker.Worker {
	return worker.New(worker.Config{
		ID:                "w1",
		Queues:            map[string]int{"default": 2},
		PollInterval:      50 * time.Millisecond,
		LeaseDuration:     lease,
		HeartbeatInterval: time.Second,
		JobTimeout:        time.Minute,
		RetryDelay:        func(int) time.Duration { return time.Minute },
		ShutdownTimeout:   shutdownTimeout,
	}, s, reg, discardLogger())
}

// runThenStop starts w, waits for started, cancels and waits for Run to return.
func runThenStop(t *testing.T, w *worker.Worker, started <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	select {
	case <-started:
	case <-time.After(10 * time.Second):
		t.Fatal("job did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("worker did not stop")
	}
}

func TestShutdownReleasesJobWithoutUsingAnAttempt(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	started := make(chan struct{})
	reg := worker.NewRegistry()
	reg.RegisterHandler("stuck", func(ctx context.Context, _ *job.Job) error {
		close(started)
		<-ctx.Done() // only returns when the worker gives up waiting
		return ctx.Err()
	})
	j := enqueue(t, s, job.NewJob{Type: "stuck", MaxAttempts: 3})

	runThenStop(t, shutdownWorker(s, reg, 200*time.Millisecond), started)

	got, err := s.Get(ctx, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != job.StatusPending || got.Attempts != 0 || got.LockedBy != nil || got.LockedUntil != nil {
		t.Fatalf("released job = %+v, want pending with attempts 0 and no lock", got)
	}
	if history, _ := s.Errors(ctx, j.ID); len(history) != 0 {
		t.Errorf("shutdown recorded an error: %+v", history)
	}

	// Another worker can claim it immediately, as its first attempt.
	again, err := s.Claim(ctx, "default", "w2", 1, lease)
	if err != nil || len(again) != 1 || again[0].Attempts != 1 {
		t.Fatalf("reclaim = %v (err %v), want the job on attempt 1", ids(again), err)
	}
}

func TestShutdownLetsRunningJobsFinishAndStopsClaiming(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()

	started := make(chan struct{})
	reg := worker.NewRegistry()
	reg.RegisterHandler("slow", func(ctx context.Context, _ *job.Job) error {
		close(started)
		time.Sleep(300 * time.Millisecond)
		return ctx.Err()
	})
	running := enqueue(t, s, job.NewJob{Type: "slow"})

	w := shutdownWorker(s, reg, 10*time.Second)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(runCtx)
		close(done)
	}()
	<-started
	cancel()
	// Enqueued after the shutdown began: must not be picked up.
	late := enqueue(t, s, job.NewJob{Type: "slow"})
	<-done

	if got, _ := s.Get(ctx, running.ID); got.Status != job.StatusSucceeded {
		t.Errorf("running job status = %s, want succeeded", got.Status)
	}
	if got, _ := s.Get(ctx, late.ID); got.Status != job.StatusPending || got.Attempts != 0 {
		t.Errorf("late job = %+v, want untouched", got)
	}
}
