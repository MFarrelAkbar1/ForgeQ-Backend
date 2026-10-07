//go:build integration

package integration

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/worker"
)

// TestStalledWorkerCausesDuplicateExecution documents ForgeQ's delivery
// guarantee: at-least-once, not exactly-once. Worker w1 stalls long enough
// for its lease to expire (think GC pause or network partition) and ignores
// cancellation; the reaper hands the job to w2. Both handlers run to the
// end, so the side effect happens twice. The fence still keeps the job's
// state correct: w1's late Complete is rejected and the job succeeds once.
// This is why handlers must be idempotent.
func TestStalledWorkerCausesDuplicateExecution(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var sideEffects atomic.Int32
	w1Started := make(chan struct{})
	w1Resume := make(chan struct{})
	reg1 := worker.NewRegistry()
	reg1.RegisterHandler("charge", func(context.Context, *job.Job) error {
		close(w1Started)
		<-w1Resume // stalled, and not watching ctx: a badly behaved handler
		sideEffects.Add(1)
		return nil
	})
	j := enqueue(t, s, job.NewJob{Type: "charge", MaxAttempts: 3})

	w1 := worker.New(worker.Config{
		ID:                "w1",
		Queues:            map[string]int{"default": 1},
		PollInterval:      50 * time.Millisecond,
		LeaseDuration:     time.Minute,
		HeartbeatInterval: 100 * time.Millisecond,
		JobTimeout:        time.Minute,
		RetryDelay:        func(int) time.Duration { return time.Minute },
		ShutdownTimeout:   5 * time.Second,
	}, s, reg1, discardLogger())
	w1Done := make(chan struct{})
	go func() {
		_ = w1.Run(ctx)
		close(w1Done)
	}()
	<-w1Started

	// The lease expires and the reaper requeues the job.
	reaper := newReaper(s, time.Hour)
	waitFor(t, 5*time.Second, "reaper to requeue the job", func() bool {
		expireLease(t, j.ID)
		reaper.RunOnce(ctx)
		got, _ := s.Get(ctx, j.ID)
		return got.Status == job.StatusRetrying
	})

	// w2 claims it, performs the side effect and completes.
	claimed, err := s.Claim(ctx, "default", "w2", 1, time.Minute)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("w2 claim = %v, err %v", ids(claimed), err)
	}
	sideEffects.Add(1)
	if err := s.Complete(ctx, j.ID, "w2"); err != nil {
		t.Fatalf("w2 complete: %v", err)
	}

	// w1 wakes up and finishes too.
	close(w1Resume)
	waitFor(t, 5*time.Second, "w1 to finish its stale run", func() bool { return sideEffects.Load() == 2 })
	cancel()
	<-w1Done

	got, _ := s.Get(context.Background(), j.ID)
	if got.Status != job.StatusSucceeded || got.Attempts != 2 {
		t.Errorf("job = %+v, want succeeded on attempt 2", got)
	}
	if n := sideEffects.Load(); n != 2 {
		t.Fatalf("side effects = %d, want 2 (the duplicate this test documents)", n)
	}
	t.Logf("side effect executed %d times for one job: delivery is at-least-once", sideEffects.Load())
}
