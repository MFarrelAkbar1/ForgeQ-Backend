//go:build integration

package integration

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/worker"
)

func newReaper(s *store.Store, retention time.Duration) *worker.Reaper {
	return worker.NewReaper(worker.ReaperConfig{Interval: time.Hour, Retention: retention}, s, discardLogger())
}

// expireLease makes a running job's lease look expired, as if its worker had
// stopped heartbeating.
func expireLease(t *testing.T, id int64) {
	t.Helper()
	_, err := pool.Exec(context.Background(),
		`UPDATE jobs SET locked_until = now() - interval '1 second' WHERE id = $1`, id)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReaperRecoversExpiredLeases(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	lastTry := enqueue(t, s, job.NewJob{Type: "t", MaxAttempts: 1})
	retryable := enqueue(t, s, job.NewJob{Type: "t", MaxAttempts: 3})
	healthy := enqueue(t, s, job.NewJob{Type: "t"})

	if _, err := s.Claim(ctx, "default", "crashed", 3, time.Minute); err != nil {
		t.Fatal(err)
	}
	expireLease(t, lastTry.ID)
	expireLease(t, retryable.ID)

	newReaper(s, time.Hour).RunOnce(ctx)

	checks := []struct {
		id     int64
		status job.Status
	}{
		{lastTry.ID, job.StatusDead},
		{retryable.ID, job.StatusRetrying},
		{healthy.ID, job.StatusRunning}, // lease still valid: untouched
	}
	for _, c := range checks {
		j, err := s.Get(ctx, c.id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status != c.status {
			t.Errorf("job %d: status = %s, want %s", c.id, j.Status, c.status)
		}
		if c.status == job.StatusRunning {
			continue
		}
		if j.LockedBy != nil || j.LastError == nil || *j.LastError != store.LeaseExpiredError {
			t.Errorf("job %d not cleaned up after reap: %+v", c.id, j)
		}
		history, _ := s.Errors(ctx, c.id)
		if len(history) != 1 || history[0].WorkerID == nil || *history[0].WorkerID != "crashed" {
			t.Errorf("job %d: history = %+v, want one entry blaming the crashed worker", c.id, history)
		}
	}

	// The retrying job is immediately claimable by a healthy worker.
	again, err := s.Claim(ctx, "default", "healthy", 10, time.Minute)
	if err != nil || len(again) != 1 || again[0].ID != retryable.ID || again[0].Attempts != 2 {
		t.Errorf("reclaim = %v (err %v), want job %d on attempt 2", ids(again), err, retryable.ID)
	}
}

// TestReaperTakeoverCancelsStaleWorker is the full lease-loss scenario: worker
// w1 stalls (its lease expires), the reaper requeues the job, w2 claims it,
// w1's next heartbeat is rejected by the fence and its handler is cancelled,
// and only w2's result is kept.
func TestReaperTakeoverCancelsStaleWorker(t *testing.T) {
	s := newStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	causes := make(chan error, 1)
	release := make(chan struct{})
	reg := worker.NewRegistry()
	reg.RegisterHandler("long", func(ctx context.Context, _ *job.Job) error {
		close(started)
		<-ctx.Done()
		causes <- context.Cause(ctx)
		<-release // hold w1's only slot so it cannot re-claim the job itself
		return ctx.Err()
	})
	j := enqueue(t, s, job.NewJob{Type: "long", MaxAttempts: 3})

	w1 := worker.New(worker.Config{
		ID:                "w1",
		Queues:            map[string]int{"default": 1},
		PollInterval:      50 * time.Millisecond,
		LeaseDuration:     2 * time.Second,
		HeartbeatInterval: 200 * time.Millisecond,
		JobTimeout:        time.Minute,
		RetryDelay:        func(int) time.Duration { return time.Minute },
		ShutdownTimeout:   5 * time.Second,
	}, s, reg, discardLogger())
	w1Done := make(chan struct{})
	go func() {
		_ = w1.Run(ctx)
		close(w1Done)
	}()
	<-started

	// Expire the lease and reap. A heartbeat landing between the two
	// statements re-extends the lease, in which case we simply try again.
	reaper := newReaper(s, time.Hour)
	waitFor(t, 5*time.Second, "reaper to take the job back", func() bool {
		expireLease(t, j.ID)
		reaper.RunOnce(ctx)
		got, _ := s.Get(ctx, j.ID)
		return got.Status == job.StatusRetrying
	})

	claimed, err := s.Claim(ctx, "default", "w2", 1, time.Minute)
	if err != nil || len(claimed) != 1 || claimed[0].ID != j.ID {
		t.Fatalf("w2 claim = %v, err %v", ids(claimed), err)
	}

	select {
	case cause := <-causes:
		if !errors.Is(cause, store.ErrLeaseLost) {
			t.Fatalf("w1 handler cause = %v, want ErrLeaseLost", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("w1 handler was not cancelled after the takeover")
	}
	close(release)

	if err := s.Complete(ctx, j.ID, "w2"); err != nil {
		t.Fatalf("w2 complete: %v", err)
	}
	cancel()
	<-w1Done

	got, _ := s.Get(context.Background(), j.ID)
	if got.Status != job.StatusSucceeded || got.Attempts != 2 {
		t.Errorf("final job = %+v, want succeeded on attempt 2", got)
	}
	history, _ := s.Errors(context.Background(), j.ID)
	if len(history) != 1 || history[0].Error != store.LeaseExpiredError {
		t.Errorf("history = %+v, want only the reaper's entry (w1 must not write)", history)
	}
}

func TestRetentionDeletesOnlyOldSucceeded(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const retention = 7 * 24 * time.Hour

	// Each job is forced into a status and finish time with plain SQL.
	type fixture struct {
		name     string
		status   job.Status
		age      time.Duration // how long ago it finished (or was created)
		wantKept bool
	}
	fixtures := []fixture{
		{"old succeeded", job.StatusSucceeded, 8 * 24 * time.Hour, false},
		{"recent succeeded", job.StatusSucceeded, 6 * 24 * time.Hour, true},
		{"old dead", job.StatusDead, 30 * 24 * time.Hour, true},
		{"old pending", job.StatusPending, 30 * 24 * time.Hour, true},
	}
	ids := make([]int64, len(fixtures))
	for i, f := range fixtures {
		j := enqueue(t, s, job.NewJob{Type: "t"})
		ids[i] = j.ID
		finished := time.Now().Add(-f.age)
		var finishedAt *time.Time
		if f.status != job.StatusPending {
			finishedAt = &finished
		}
		_, err := pool.Exec(ctx, `
			UPDATE jobs SET status = $2, finished_at = $3, created_at = $4 WHERE id = $1`,
			j.ID, f.status, finishedAt, finished)
		if err != nil {
			t.Fatal(err)
		}
	}
	// Error history of the deleted job must go with it.
	if _, err := pool.Exec(ctx, `INSERT INTO job_errors (job_id, attempt, error) VALUES ($1, 1, 'x')`, ids[0]); err != nil {
		t.Fatal(err)
	}

	newReaper(s, retention).RunOnce(ctx)

	for i, f := range fixtures {
		_, err := s.Get(ctx, ids[i])
		kept := !errors.Is(err, store.ErrNotFound)
		if kept != f.wantKept {
			t.Errorf("%s: kept = %v, want %v", f.name, kept, f.wantKept)
		}
	}
	var orphans int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_errors WHERE job_id = $1`, ids[0]).Scan(&orphans); err != nil {
		t.Fatal(err)
	}
	if orphans != 0 {
		t.Errorf("job_errors of deleted job: %d rows remain", orphans)
	}
}
