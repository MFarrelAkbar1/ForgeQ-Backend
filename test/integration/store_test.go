//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/store"
)

const lease = 30 * time.Second

func TestClaimOrderAndSchedule(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	future := time.Now().Add(time.Hour)

	low := enqueue(t, s, job.NewJob{Type: "t", Priority: 0})
	high := enqueue(t, s, job.NewJob{Type: "t", Priority: 10})
	enqueue(t, s, job.NewJob{Type: "t", Priority: 100, RunAt: &future}) // delayed
	enqueue(t, s, job.NewJob{Type: "t", Queue: "other"})                // other queue

	jobs, err := s.Claim(ctx, "default", "w1", 10, lease)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].ID != high.ID || jobs[1].ID != low.ID {
		t.Fatalf("claimed %v, want [%d %d]", ids(jobs), high.ID, low.ID)
	}
	for _, j := range jobs {
		if j.Status != job.StatusRunning || j.Attempts != 1 || j.LockedBy == nil || *j.LockedBy != "w1" {
			t.Errorf("claimed job %d in unexpected state: %+v", j.ID, j)
		}
		if j.LockedUntil == nil || j.LockedUntil.Before(time.Now().Add(lease-5*time.Second)) {
			t.Errorf("job %d lease ends %v, want about %s from now", j.ID, j.LockedUntil, lease)
		}
	}

	again, err := s.Claim(ctx, "default", "w2", 10, lease)
	if err != nil {
		t.Fatal(err)
	}
	if len(again) != 0 {
		t.Errorf("second claim got %v, want nothing", ids(again))
	}
}

// TestConcurrentClaimsAreExclusive is the core guarantee: many claimers racing
// on the same queue never receive the same job.
func TestConcurrentClaimsAreExclusive(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const total, claimers, batch = 300, 8, 5

	for i := range total {
		enqueue(t, s, job.NewJob{Type: "t", Payload: json.RawMessage(fmt.Sprintf(`{"n":%d}`, i))})
	}

	var mu sync.Mutex
	seen := map[int64]string{}
	var dupes []string
	var wg sync.WaitGroup
	for w := range claimers {
		worker := fmt.Sprintf("w%d", w)
		wg.Go(func() {
			for {
				jobs, err := s.Claim(ctx, "default", worker, batch, lease)
				if err != nil {
					t.Errorf("%s: claim: %v", worker, err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					if prev, ok := seen[j.ID]; ok {
						dupes = append(dupes, fmt.Sprintf("job %d: %s and %s", j.ID, prev, worker))
					}
					seen[j.ID] = worker
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(dupes) > 0 {
		t.Fatalf("jobs claimed more than once: %v", dupes)
	}
	if len(seen) != total {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), total)
	}
}

func TestEnqueueIdempotencyUnderConcurrency(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const callers = 20

	type result struct {
		id      int64
		created bool
		err     error
	}
	results := make(chan result, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for range callers {
		wg.Go(func() {
			<-start
			j, created, err := s.Enqueue(ctx, job.NewJob{Type: "t", IdempotencyKey: "order-42"})
			r := result{created: created, err: err}
			if j != nil {
				r.id = j.ID
			}
			results <- r
		})
	}
	close(start)
	wg.Wait()
	close(results)

	createdCount := 0
	var firstID int64
	for r := range results {
		if r.err != nil {
			t.Fatalf("enqueue: %v", r.err)
		}
		if firstID == 0 {
			firstID = r.id
		}
		if r.id != firstID {
			t.Errorf("got job id %d, want every caller to get %d", r.id, firstID)
		}
		if r.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Errorf("created = %d, want exactly 1", createdCount)
	}
	if n := countJobs(t); n != 1 {
		t.Errorf("rows in jobs = %d, want 1", n)
	}

	// The same key on another queue is a different job.
	other, created, err := s.Enqueue(ctx, job.NewJob{Type: "t", Queue: "other", IdempotencyKey: "order-42"})
	if err != nil || !created || other.ID == firstID {
		t.Errorf("other queue: created=%v id=%v err=%v, want a new job", created, other, err)
	}
}

func TestCompleteIsFenced(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	enqueue(t, s, job.NewJob{Type: "t"})
	claimed, err := s.Claim(ctx, "default", "owner", 1, lease)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	id := claimed[0].ID

	if err := s.Complete(ctx, id, "intruder"); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("Complete by non-owner: err = %v, want ErrLeaseLost", err)
	}
	if _, err := s.Fail(ctx, store.Failure{JobID: id, WorkerID: "intruder", Error: "x"}); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("Fail by non-owner: err = %v, want ErrLeaseLost", err)
	}
	if err := s.Complete(ctx, id, "owner"); err != nil {
		t.Fatalf("Complete by owner: %v", err)
	}
	if err := s.Complete(ctx, id, "owner"); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("second Complete: err = %v, want ErrLeaseLost", err)
	}

	j, err := s.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if j.Status != job.StatusSucceeded || j.LockedBy != nil || j.FinishedAt == nil {
		t.Errorf("job after complete = %+v", j)
	}
}

func TestFailRetriesThenDies(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	enqueue(t, s, job.NewJob{Type: "t", MaxAttempts: 2})

	claimAndFail := func(msg string) job.Status {
		t.Helper()
		jobs, err := s.Claim(ctx, "default", "w1", 1, lease)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("claim: %v %v", ids(jobs), err)
		}
		status, err := s.Fail(ctx, store.Failure{
			JobID: jobs[0].ID, WorkerID: "w1", Error: msg, RetryAt: time.Now(),
		})
		if err != nil {
			t.Fatalf("fail: %v", err)
		}
		return status
	}

	if got := claimAndFail("first"); got != job.StatusRetrying {
		t.Fatalf("after attempt 1: status = %s, want retrying", got)
	}
	if got := claimAndFail("second"); got != job.StatusDead {
		t.Fatalf("after attempt 2: status = %s, want dead", got)
	}

	j, err := s.Get(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if j.Attempts != 2 || j.LastError == nil || *j.LastError != "second" || j.FinishedAt == nil {
		t.Errorf("dead job = %+v", j)
	}
	history, err := s.Errors(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 2 || history[0].Attempt != 1 || history[1].Error != "second" {
		t.Errorf("history = %+v", history)
	}
}

func TestPermanentFailureSkipsRetries(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	enqueue(t, s, job.NewJob{Type: "t", MaxAttempts: 5})
	jobs, _ := s.Claim(ctx, "default", "w1", 1, lease)
	status, err := s.Fail(ctx, store.Failure{JobID: jobs[0].ID, WorkerID: "w1", Error: "bad", Permanent: true})
	if err != nil || status != job.StatusDead {
		t.Fatalf("status = %s, err = %v; want dead", status, err)
	}
}

func TestListFilters(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for range 3 {
		enqueue(t, s, job.NewJob{Type: "t"})
	}
	enqueue(t, s, job.NewJob{Type: "t", Queue: "emails"})
	if _, err := s.Claim(ctx, "default", "w1", 1, lease); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		f    store.ListFilter
		want []int64
	}{
		{"all newest first", store.ListFilter{Limit: 10}, []int64{4, 3, 2, 1}},
		{"by queue", store.ListFilter{Queue: "emails", Limit: 10}, []int64{4}},
		{"by status", store.ListFilter{Status: job.StatusRunning, Limit: 10}, []int64{1}},
		{"cursor", store.ListFilter{Cursor: 3, Limit: 10}, []int64{2, 1}},
		{"limit", store.ListFilter{Limit: 2}, []int64{4, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := s.List(ctx, tt.f)
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(ids(jobs)) != fmt.Sprint(tt.want) {
				t.Errorf("got %v, want %v", ids(jobs), tt.want)
			}
		})
	}

	if _, err := s.Get(ctx, 999); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("Get missing: err = %v, want ErrNotFound", err)
	}
}

func ids(jobs []*job.Job) []int64 {
	out := make([]int64, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

func countJobs(t *testing.T) int {
	t.Helper()
	var n int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM jobs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestHeartbeatIsFenced(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	enqueue(t, s, job.NewJob{Type: "t"})
	claimed, err := s.Claim(ctx, "default", "owner", 1, time.Second)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	id := claimed[0].ID
	before := *claimed[0].LockedUntil

	if err := s.Heartbeat(ctx, id, "owner", time.Minute); err != nil {
		t.Fatalf("owner heartbeat: %v", err)
	}
	j, _ := s.Get(ctx, id)
	if !j.LockedUntil.After(before.Add(50 * time.Second)) {
		t.Errorf("lease not extended: before %v, after %v", before, j.LockedUntil)
	}

	if err := s.Heartbeat(ctx, id, "intruder", time.Minute); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("non-owner heartbeat: err = %v, want ErrLeaseLost", err)
	}
	if err := s.Complete(ctx, id, "owner"); err != nil {
		t.Fatal(err)
	}
	if err := s.Heartbeat(ctx, id, "owner", time.Minute); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("heartbeat after completion: err = %v, want ErrLeaseLost", err)
	}
}

func TestRetryDeadKeepsHistory(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	enqueue(t, s, job.NewJob{Type: "t", MaxAttempts: 1})
	claimed, err := s.Claim(ctx, "default", "w1", 1, lease)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: %v %v", ids(claimed), err)
	}
	id := claimed[0].ID
	if _, err := s.Fail(ctx, store.Failure{JobID: id, WorkerID: "w1", Error: "boom"}); err != nil {
		t.Fatal(err)
	}

	j, err := s.RetryDead(ctx, id)
	if err != nil {
		t.Fatalf("RetryDead: %v", err)
	}
	if j.Status != job.StatusPending || j.Run != 2 || j.Attempts != 0 || j.LastError != nil || j.FinishedAt != nil {
		t.Errorf("retried job = %+v, want pending on run 2 with attempts reset", j)
	}
	if _, err := s.RetryDead(ctx, id); !errors.Is(err, store.ErrInvalidState) {
		t.Errorf("retry of pending job: err = %v, want ErrInvalidState", err)
	}
	if _, err := s.RetryDead(ctx, 999); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("retry of missing job: err = %v, want ErrNotFound", err)
	}

	// It runs again with a fresh budget, and the old failure is still in history.
	again, err := s.Claim(ctx, "default", "w2", 1, lease)
	if err != nil || len(again) != 1 || again[0].Attempts != 1 {
		t.Fatalf("reclaim = %v (err %v), want the job on attempt 1", ids(again), err)
	}
	// Fail it again: the new error belongs to run 2, attempt 1.
	if _, err := s.Fail(ctx, store.Failure{JobID: id, WorkerID: "w2", Error: "boom again"}); err != nil {
		t.Fatal(err)
	}
	history, _ := s.Errors(ctx, id)
	want := []struct {
		run, attempt int
		err          string
	}{{1, 1, "boom"}, {2, 1, "boom again"}}
	if len(history) != len(want) {
		t.Fatalf("history = %+v, want %d entries", history, len(want))
	}
	for i, w := range want {
		h := history[i]
		if h.Run != w.run || h.Attempt != w.attempt || h.Error != w.err {
			t.Errorf("history[%d] = run %d attempt %d %q, want run %d attempt %d %q",
				i, h.Run, h.Attempt, h.Error, w.run, w.attempt, w.err)
		}
	}
}

func TestDeleteOnlyFinishedJobs(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	pending := enqueue(t, s, job.NewJob{Type: "t"})
	done := enqueue(t, s, job.NewJob{Type: "t"})
	if _, err := pool.Exec(ctx, `UPDATE jobs SET status = 'succeeded' WHERE id = $1`, done.ID); err != nil {
		t.Fatal(err)
	}

	if err := s.Delete(ctx, pending.ID); !errors.Is(err, store.ErrInvalidState) {
		t.Errorf("delete pending: err = %v, want ErrInvalidState", err)
	}
	if err := s.Delete(ctx, done.ID); err != nil {
		t.Errorf("delete succeeded: %v", err)
	}
	if err := s.Delete(ctx, done.ID); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("delete twice: err = %v, want ErrNotFound", err)
	}
}

func TestStats(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	for range 2 {
		enqueue(t, s, job.NewJob{Type: "t"})
	}
	enqueue(t, s, job.NewJob{Type: "t", Queue: "emails"})
	if _, err := s.Claim(ctx, "default", "w1", 1, lease); err != nil {
		t.Fatal(err)
	}

	stats, err := s.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := []store.QueueStat{
		{Queue: "default", Status: job.StatusPending, Count: 1},
		{Queue: "default", Status: job.StatusRunning, Count: 1},
		{Queue: "emails", Status: job.StatusPending, Count: 1},
	}
	if fmt.Sprint(stats) != fmt.Sprint(want) {
		t.Errorf("stats = %v, want %v", stats, want)
	}
}
