//go:build integration

package integration

import (
	"context"
	"testing"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
)

// listen subscribes to wake-ups and returns the channel of queue names once
// the listener is connected (it probes with a direct pg_notify until one
// arrives, then drains it).
func listen(t *testing.T, ctx context.Context) <-chan string {
	t.Helper()
	got := make(chan string, 1000)
	go store.Listen(ctx, databaseURL, discardLogger(), func(q string) { got <- q })

	deadline := time.After(10 * time.Second)
	for {
		if _, err := pool.Exec(ctx, `SELECT pg_notify($1, 'probe')`, store.NotifyChannel); err != nil {
			t.Fatal(err)
		}
		select {
		case <-got:
			time.Sleep(50 * time.Millisecond)
			for len(got) > 0 {
				<-got
			}
			return got
		case <-time.After(100 * time.Millisecond):
		case <-deadline:
			t.Fatal("listener did not connect")
		}
	}
}

// notifyingStore returns a Store whose writes send wake-ups via a running Notifier.
func notifyingStore(t *testing.T, ctx context.Context, window time.Duration) *store.Store {
	t.Helper()
	newStore(t) // truncate
	n := store.NewNotifier(pool, window, discardLogger())
	go n.Run(ctx)
	return store.New(pool, store.WithNotifier(n))
}

// collect returns the notifications received within d.
func collect(ch <-chan string, d time.Duration) []string {
	var out []string
	timeout := time.After(d)
	for {
		select {
		case q := <-ch:
			out = append(out, q)
		case <-timeout:
			return out
		}
	}
}

func TestEnqueueWakesWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := listen(t, ctx)
	s := notifyingStore(t, ctx, 50*time.Millisecond)

	enqueue(t, s, job.NewJob{Type: "t", Queue: "emails"})
	if n := collect(got, 500*time.Millisecond); len(n) != 1 || n[0] != "emails" {
		t.Fatalf("notifications = %v, want [emails]", n)
	}

	// A delayed job is not runnable yet: no wake-up (polling finds it later).
	later := time.Now().Add(time.Hour)
	enqueue(t, s, job.NewJob{Type: "t", Queue: "emails", RunAt: &later})
	if n := collect(got, 300*time.Millisecond); len(n) != 0 {
		t.Errorf("delayed job sent notifications %v", n)
	}
}

// TestNotificationsAreCoalesced is the reason the Notifier exists: a burst of
// enqueues must produce a handful of notifications per queue, not one each.
func TestNotificationsAreCoalesced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := listen(t, ctx)
	s := notifyingStore(t, ctx, 100*time.Millisecond)

	const burst = 200
	start := time.Now()
	for i := range burst {
		queue := "a"
		if i%2 == 1 {
			queue = "b"
		}
		enqueue(t, s, job.NewJob{Type: "t", Queue: queue})
	}
	elapsed := time.Since(start)
	notes := collect(got, 500*time.Millisecond)

	perQueue := map[string]int{}
	for _, q := range notes {
		perQueue[q]++
	}
	// At most one per queue per window, plus the leading-edge send.
	limit := int(elapsed/(100*time.Millisecond)) + 2
	for _, q := range []string{"a", "b"} {
		if perQueue[q] == 0 || perQueue[q] > limit {
			t.Errorf("queue %s: %d notifications for %d enqueues in %s, want 1..%d",
				q, perQueue[q], burst/2, elapsed.Round(time.Millisecond), limit)
		}
	}
}

func TestRequeuesWakeWorkers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := notifyingStore(t, ctx, 10*time.Millisecond)

	// Make one dead job and one running job before listening.
	enqueue(t, s, job.NewJob{Type: "t", Queue: "dl", MaxAttempts: 1})
	enqueue(t, s, job.NewJob{Type: "t", Queue: "rel"})
	dead, _ := s.Claim(ctx, "dl", "w1", 1, lease)
	if _, err := s.Fail(ctx, store.Failure{JobID: dead[0].ID, WorkerID: "w1", Error: "x"}); err != nil {
		t.Fatal(err)
	}
	running, _ := s.Claim(ctx, "rel", "w1", 1, lease)
	got := listen(t, ctx)

	if _, err := s.RetryDead(ctx, dead[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Release(ctx, running[0].ID, "w1"); err != nil {
		t.Fatal(err)
	}
	notes := collect(got, 300*time.Millisecond)
	seen := map[string]bool{}
	for _, q := range notes {
		seen[q] = true
	}
	if !seen["dl"] || !seen["rel"] {
		t.Errorf("notifications = %v, want wake-ups for dl (retry) and rel (release)", notes)
	}
}

// TestInsertDoesNotNotifyByItself guards against reintroducing NOTIFY in the
// write path (e.g. a trigger): a plain insert must not produce a wake-up.
func TestInsertDoesNotNotifyByItself(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	got := listen(t, ctx)
	s := newStore(t) // no notifier
	enqueue(t, s, job.NewJob{Type: "t", Queue: "emails"})
	if n := collect(got, 300*time.Millisecond); len(n) != 0 {
		t.Errorf("insert without notifier produced notifications %v", n)
	}
}
