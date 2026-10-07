package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
)

// fakeStore hands out queued jobs once and records outcomes.
type fakeStore struct {
	mu        sync.Mutex
	queued    []*job.Job
	completed []int64
	failures  []store.Failure
	released  []int64
	done      chan struct{} // receives one value per reported outcome

	heartbeats   int
	heartbeatErr error // returned by Heartbeat when set
}

func newFakeStore(jobs ...*job.Job) *fakeStore {
	return &fakeStore{queued: jobs, done: make(chan struct{}, len(jobs))}
}

func (f *fakeStore) Claim(_ context.Context, queue, _ string, limit int, _ time.Duration) ([]*job.Job, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out, rest []*job.Job
	for _, j := range f.queued {
		if j.Queue == queue && len(out) < limit {
			j.Attempts++
			out = append(out, j)
		} else {
			rest = append(rest, j)
		}
	}
	f.queued = rest
	return out, nil
}

func (f *fakeStore) Heartbeat(context.Context, int64, string, time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.heartbeats++
	return f.heartbeatErr
}

func (f *fakeStore) Complete(_ context.Context, id int64, _ string) error {
	f.mu.Lock()
	f.completed = append(f.completed, id)
	f.mu.Unlock()
	f.done <- struct{}{}
	return nil
}

func (f *fakeStore) Fail(_ context.Context, fl store.Failure) (job.Status, error) {
	f.mu.Lock()
	f.failures = append(f.failures, fl)
	f.mu.Unlock()
	f.done <- struct{}{}
	if fl.Permanent {
		return job.StatusDead, nil
	}
	return job.StatusRetrying, nil
}

func (f *fakeStore) Release(_ context.Context, id int64, _ string) error {
	f.mu.Lock()
	f.released = append(f.released, id)
	f.mu.Unlock()
	f.done <- struct{}{}
	return nil
}

func testConfig() Config {
	return Config{
		ID:                "test-worker",
		Queues:            map[string]int{"default": 2},
		PollInterval:      10 * time.Millisecond,
		LeaseDuration:     time.Minute,
		HeartbeatInterval: 10 * time.Millisecond,
		JobTimeout:        time.Second,
		RetryDelay:        func(int) time.Duration { return time.Minute },
		ShutdownTimeout:   time.Second,
	}
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// runUntilDone runs the worker until n outcomes are reported, then stops it.
func runUntilDone(t *testing.T, cfg Config, fs *fakeStore, reg *Registry, n int) {
	t.Helper()
	w := New(cfg, fs, reg, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	for range n {
		select {
		case <-fs.done:
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatal("timed out waiting for job outcomes")
		}
	}
	cancel()
	if err := <-errc; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestWorkerOutcomes(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterHandler("ok", func(context.Context, *job.Job) error { return nil })
	reg.RegisterHandler("transient", func(context.Context, *job.Job) error { return errors.New("boom") })
	reg.RegisterHandler("permanent", func(context.Context, *job.Job) error { return Permanent(errors.New("bad input")) })
	reg.RegisterHandler("panics", func(context.Context, *job.Job) error { panic("oops") })
	reg.RegisterHandler("slow", func(ctx context.Context, _ *job.Job) error {
		<-ctx.Done()
		return ctx.Err()
	}, WithTimeout(20*time.Millisecond))

	fs := newFakeStore(
		&job.Job{ID: 1, Queue: "default", Type: "ok"},
		&job.Job{ID: 2, Queue: "default", Type: "transient"},
		&job.Job{ID: 3, Queue: "default", Type: "permanent"},
		&job.Job{ID: 4, Queue: "default", Type: "panics"},
		&job.Job{ID: 5, Queue: "default", Type: "slow"},
		&job.Job{ID: 6, Queue: "default", Type: "unknown"},
	)
	runUntilDone(t, testConfig(), fs, reg, 6)

	if len(fs.completed) != 1 || fs.completed[0] != 1 {
		t.Errorf("completed = %v, want [1]", fs.completed)
	}

	want := map[int64]struct {
		permanent bool
		errSubstr string
	}{
		2: {false, "boom"},
		3: {true, "bad input"},
		4: {false, "handler panicked: oops"},
		5: {false, "timed out"},
		6: {true, "no handler registered"},
	}
	if len(fs.failures) != len(want) {
		t.Fatalf("got %d failures, want %d: %+v", len(fs.failures), len(want), fs.failures)
	}
	for _, f := range fs.failures {
		w, ok := want[f.JobID]
		if !ok {
			t.Errorf("unexpected failure for job %d", f.JobID)
			continue
		}
		if f.Permanent != w.permanent {
			t.Errorf("job %d: Permanent = %v, want %v", f.JobID, f.Permanent, w.permanent)
		}
		if !strings.Contains(f.Error, w.errSubstr) {
			t.Errorf("job %d: Error = %q, want it to contain %q", f.JobID, f.Error, w.errSubstr)
		}
		if f.WorkerID != "test-worker" {
			t.Errorf("job %d: WorkerID = %q", f.JobID, f.WorkerID)
		}
		if f.RetryAt.Before(time.Now().Add(50 * time.Second)) {
			t.Errorf("job %d: RetryAt = %v, want about a minute from now", f.JobID, f.RetryAt)
		}
	}
}

func TestWorkerRespectsConcurrency(t *testing.T) {
	const jobs, concurrency = 20, 3

	var mu sync.Mutex
	running, peak := 0, 0
	reg := NewRegistry()
	reg.RegisterHandler("work", func(context.Context, *job.Job) error {
		mu.Lock()
		running++
		peak = max(peak, running)
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		mu.Lock()
		running--
		mu.Unlock()
		return nil
	})

	var queued []*job.Job
	for i := range jobs {
		queued = append(queued, &job.Job{ID: int64(i + 1), Queue: "default", Type: "work"})
	}
	fs := newFakeStore(queued...)
	cfg := testConfig()
	cfg.Queues = map[string]int{"default": concurrency}
	runUntilDone(t, cfg, fs, reg, jobs)

	if len(fs.completed) != jobs {
		t.Errorf("completed %d jobs, want %d", len(fs.completed), jobs)
	}
	if peak > concurrency {
		t.Errorf("peak concurrency = %d, want <= %d", peak, concurrency)
	}
}

func TestWorkerCancelsJobOnLeaseLoss(t *testing.T) {
	causes := make(chan error, 1)
	reg := NewRegistry()
	reg.RegisterHandler("long", func(ctx context.Context, _ *job.Job) error {
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return ctx.Err()
	})

	fs := newFakeStore(&job.Job{ID: 1, Queue: "default", Type: "long"})
	fs.heartbeatErr = store.ErrLeaseLost
	w := New(testConfig(), fs, reg, discardLogger())
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()

	select {
	case cause := <-causes:
		if !errors.Is(cause, store.ErrLeaseLost) {
			t.Errorf("handler ctx cause = %v, want ErrLeaseLost", cause)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler was not cancelled after lease loss")
	}
	cancel()
	<-errc

	if len(fs.completed) != 0 || len(fs.failures) != 0 {
		t.Errorf("worker reported a result after losing the lease: completed=%v failures=%v",
			fs.completed, fs.failures)
	}
}

func TestWorkerHeartbeatsLongJobs(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterHandler("long", func(context.Context, *job.Job) error {
		time.Sleep(100 * time.Millisecond) // ~10 heartbeat intervals
		return nil
	})
	fs := newFakeStore(&job.Job{ID: 1, Queue: "default", Type: "long"})
	runUntilDone(t, testConfig(), fs, reg, 1)

	if fs.heartbeats < 3 {
		t.Errorf("heartbeats = %d, want several during a long job", fs.heartbeats)
	}
	if len(fs.completed) != 1 {
		t.Errorf("completed = %v, want [1]", fs.completed)
	}
}

// startAndStop runs w until started is closed, cancels it, and returns how
// long Run took to return after the cancel.
func startAndStop(t *testing.T, w *Worker, started <-chan struct{}) time.Duration {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- w.Run(ctx) }()
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not start")
	}
	cancel()
	stoppedAt := time.Now()
	if err := <-errc; err != nil {
		t.Fatalf("Run: %v", err)
	}
	return time.Since(stoppedAt)
}

func TestShutdownWaitsForRunningJobs(t *testing.T) {
	started := make(chan struct{})
	reg := NewRegistry()
	reg.RegisterHandler("work", func(ctx context.Context, _ *job.Job) error {
		close(started)
		time.Sleep(100 * time.Millisecond)
		return ctx.Err() // nil: shutdown must not cancel a job within the timeout
	})
	fs := newFakeStore(&job.Job{ID: 1, Queue: "default", Type: "work"})

	took := startAndStop(t, New(testConfig(), fs, reg, discardLogger()), started)

	if len(fs.completed) != 1 || len(fs.released) != 0 {
		t.Errorf("completed=%v released=%v, want the job completed", fs.completed, fs.released)
	}
	if took >= testConfig().ShutdownTimeout {
		t.Errorf("Run took %v to return, want it to stop as soon as the job finished", took)
	}
}

func TestShutdownTimeoutReleasesJobs(t *testing.T) {
	started := make(chan struct{})
	causes := make(chan error, 1)
	reg := NewRegistry()
	reg.RegisterHandler("stuck", func(ctx context.Context, _ *job.Job) error {
		close(started)
		<-ctx.Done()
		causes <- context.Cause(ctx)
		return ctx.Err()
	})
	fs := newFakeStore(&job.Job{ID: 1, Queue: "default", Type: "stuck"})
	cfg := testConfig()
	cfg.ShutdownTimeout = 50 * time.Millisecond

	startAndStop(t, New(cfg, fs, reg, discardLogger()), started)

	if cause := <-causes; !errors.Is(cause, ErrShutdown) {
		t.Errorf("handler cause = %v, want ErrShutdown", cause)
	}
	if len(fs.released) != 1 || len(fs.failures) != 0 || len(fs.completed) != 0 {
		t.Errorf("released=%v failures=%v completed=%v, want only a release",
			fs.released, fs.failures, fs.completed)
	}
}

func TestRegistryRejectsDuplicates(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterHandler("a", func(context.Context, *job.Job) error { return nil })
	defer func() {
		if recover() == nil {
			t.Error("expected panic on duplicate registration")
		}
	}()
	reg.RegisterHandler("a", func(context.Context, *job.Job) error { return nil })
}

func TestPermanent(t *testing.T) {
	base := errors.New("x")
	if Permanent(nil) != nil {
		t.Error("Permanent(nil) should be nil")
	}
	err := Permanent(base)
	if !IsPermanent(err) || !errors.Is(err, base) {
		t.Error("Permanent should be detectable and unwrap to its cause")
	}
	if IsPermanent(base) {
		t.Error("plain error should not be permanent")
	}
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	s := strings.Repeat("é", 10) // 2 bytes per rune
	got := truncate(s, 5)
	if !strings.HasPrefix(got, "éé...") {
		t.Errorf("truncate = %q, want cut on a rune boundary", got)
	}
}

// recordingObserver collects outcomes reported by the worker.
type recordingObserver struct {
	nopObserver
	mu       sync.Mutex
	started  int
	outcomes map[string]Outcome // job type -> outcome
}

func (o *recordingObserver) JobStarted(string) {
	o.mu.Lock()
	o.started++
	o.mu.Unlock()
}

func (o *recordingObserver) JobFinished(_, jobType string, outcome Outcome, _ time.Duration) {
	o.mu.Lock()
	o.outcomes[jobType] = outcome
	o.mu.Unlock()
}

func TestWorkerReportsOutcomes(t *testing.T) {
	reg := NewRegistry()
	reg.RegisterHandler("ok", func(context.Context, *job.Job) error { return nil })
	reg.RegisterHandler("flaky", func(context.Context, *job.Job) error { return errors.New("x") })
	reg.RegisterHandler("bad", func(context.Context, *job.Job) error { return Permanent(errors.New("x")) })

	fs := newFakeStore(
		&job.Job{ID: 1, Queue: "default", Type: "ok"},
		&job.Job{ID: 2, Queue: "default", Type: "flaky"},
		&job.Job{ID: 3, Queue: "default", Type: "bad"},
		&job.Job{ID: 4, Queue: "default", Type: "client-invented-type"},
	)
	obs := &recordingObserver{outcomes: map[string]Outcome{}}
	cfg := testConfig()
	cfg.Observer = obs
	runUntilDone(t, cfg, fs, reg, 4)

	want := map[string]Outcome{
		"ok":      OutcomeSucceeded,
		"flaky":   OutcomeRetrying,
		"bad":     OutcomeDead,
		"unknown": OutcomeDead, // unregistered types are not used as label values
	}
	if obs.started != 4 {
		t.Errorf("started = %d, want 4", obs.started)
	}
	if len(obs.outcomes) != len(want) {
		t.Errorf("outcomes = %v, want %v", obs.outcomes, want)
	}
	for typ, o := range want {
		if obs.outcomes[typ] != o {
			t.Errorf("outcome[%s] = %q, want %q", typ, obs.outcomes[typ], o)
		}
	}
}
