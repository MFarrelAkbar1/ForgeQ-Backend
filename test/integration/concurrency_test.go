//go:build integration

package integration

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/worker"
)

// TestManyWorkersRunEachJobExactlyOnce runs several full worker instances
// (fetch loops, heartbeats, notifications) against one queue and counts how
// often each job's handler executes. Without crashes or lease loss, every
// job must run exactly once and succeed on its first attempt. (With crashes
// delivery is at-least-once; that is exercised by the loadgen chaos mode.)
func TestManyWorkersRunEachJobExactlyOnce(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const (
		total       = 3000
		workers     = 4
		concurrency = 8
	)

	// Bulk insert: enqueueing one by one is not what this test measures.
	_, err := pool.Exec(ctx, `
		INSERT INTO jobs (type, payload, priority)
		SELECT 'count', jsonb_build_object('n', g), (g % 3)::smallint
		FROM generate_series(1, $1) AS g`, total)
	if err != nil {
		t.Fatal(err)
	}

	executions := make([]atomic.Int32, total+1) // indexed by job id (1..total)
	reg := worker.NewRegistry()
	reg.RegisterHandler("count", func(_ context.Context, j *job.Job) error {
		executions[j.ID].Add(1)
		time.Sleep(time.Duration(rand.IntN(1500)) * time.Microsecond)
		return nil
	})

	runCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	counters := make([]*countingObserver, workers)
	for i := range workers {
		counters[i] = &countingObserver{}
		w := worker.New(worker.Config{
			ID:                fmt.Sprintf("w%d", i),
			Queues:            map[string]int{"default": concurrency},
			PollInterval:      20 * time.Millisecond,
			LeaseDuration:     lease,
			HeartbeatInterval: 5 * time.Second,
			JobTimeout:        10 * time.Second,
			RetryDelay:        func(int) time.Duration { return time.Second },
			ShutdownTimeout:   5 * time.Second,
			Observer:          counters[i],
		}, s, reg, discardLogger())
		wg.Go(func() { _ = w.Run(runCtx) })
	}

	start := time.Now()
	waitFor(t, 60*time.Second, "all jobs to succeed", func() bool {
		return countByStatus(t, job.StatusSucceeded) == total
	})
	took := time.Since(start)
	cancel()
	wg.Wait()

	var dupes, missing []int64
	for id := int64(1); id <= total; id++ {
		switch n := executions[id].Load(); {
		case n == 0:
			missing = append(missing, id)
		case n > 1:
			dupes = append(dupes, id)
		}
	}
	if len(missing) > 0 || len(dupes) > 0 {
		t.Fatalf("missing %d jobs (%v), duplicated %d jobs (%v)",
			len(missing), head(missing), len(dupes), head(dupes))
	}

	var maxAttempts, errorRows int
	if err := pool.QueryRow(ctx, `SELECT max(attempts) FROM jobs`).Scan(&maxAttempts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM job_errors`).Scan(&errorRows); err != nil {
		t.Fatal(err)
	}
	if maxAttempts != 1 || errorRows != 0 {
		t.Errorf("max attempts = %d, error rows = %d; want every job done on its first attempt", maxAttempts, errorRows)
	}

	// Every worker should have taken a share; one worker doing everything
	// would mean the others were starved rather than skipping locked rows.
	shares := make([]int64, workers)
	for i, c := range counters {
		shares[i] = c.finished.Load()
		if shares[i] == 0 {
			t.Errorf("worker w%d processed no jobs", i)
		}
	}
	t.Logf("%d jobs across %d workers x %d slots in %s (%.0f jobs/s), per worker %v",
		total, workers, concurrency, took.Round(time.Millisecond), float64(total)/took.Seconds(), shares)
}

// TestClaimContentionAcrossQueues hammers Claim from many goroutines across
// two queues with random batch sizes and checks that no job is handed out
// twice and none is lost, and that each job only goes to its own queue.
func TestClaimContentionAcrossQueues(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	const perQueue, claimers = 2500, 32
	queues := []string{"default", "emails"}

	for _, q := range queues {
		_, err := pool.Exec(ctx, `
			INSERT INTO jobs (queue, type) SELECT $1, 't' FROM generate_series(1, $2)`, q, perQueue)
		if err != nil {
			t.Fatal(err)
		}
	}

	var mu sync.Mutex
	owner := map[int64]string{}
	var conflicts []string
	var wg sync.WaitGroup
	for c := range claimers {
		queue := queues[c%len(queues)]
		id := fmt.Sprintf("c%d", c)
		wg.Go(func() {
			for {
				jobs, err := s.Claim(ctx, queue, id, 1+rand.IntN(10), lease)
				if err != nil {
					t.Errorf("%s: %v", id, err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					if j.Queue != queue {
						conflicts = append(conflicts, fmt.Sprintf("job %d from %s claimed on %s", j.ID, j.Queue, queue))
					}
					if prev, dup := owner[j.ID]; dup {
						conflicts = append(conflicts, fmt.Sprintf("job %d claimed by %s and %s", j.ID, prev, id))
					}
					owner[j.ID] = id
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()

	if len(conflicts) > 0 {
		t.Fatalf("%d conflicts, first: %v", len(conflicts), conflicts[:min(5, len(conflicts))])
	}
	if len(owner) != perQueue*len(queues) {
		t.Fatalf("claimed %d distinct jobs, want %d", len(owner), perQueue*len(queues))
	}
	if n := countByStatus(t, job.StatusRunning); n != perQueue*len(queues) {
		t.Errorf("running jobs in db = %d, want %d", n, perQueue*len(queues))
	}
}

func head(ids []int64) []int64 { return ids[:min(10, len(ids))] }

// countingObserver counts jobs finished by one worker instance.
type countingObserver struct{ finished atomic.Int64 }

func (*countingObserver) JobStarted(string) {}
func (o *countingObserver) JobFinished(string, string, worker.Outcome, time.Duration) {
	o.finished.Add(1)
}
func (*countingObserver) JobReaped(job.Status) {}
func (*countingObserver) JobsDeleted(int64)    {}
