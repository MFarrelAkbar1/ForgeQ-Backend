// Command loadgen enqueues many bench jobs through the HTTP API, waits until
// the workers have processed them, and reports throughput and latency read
// from Postgres. With -chaos it also kills and restarts worker containers at
// random while the load runs, then verifies that no job was lost and reports
// how many ran more than once (delivery is at-least-once).
//
//	go run ./cmd/loadgen -jobs 5000 -concurrency 50 -sleep 10ms
//	go run ./cmd/loadgen -jobs 3000 -chaos
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type options struct {
	apiURL      string
	apiKey      string
	databaseURL string
	queue       string
	jobs        int
	concurrency int
	sleep       time.Duration
	maxAttempts int
	timeout     time.Duration
	label       string
	markdown    bool

	chaos       bool
	chaosEvery  time.Duration
	composeProj string
	composeSvc  string
}

func main() {
	var o options
	flag.StringVar(&o.apiURL, "api", "http://localhost:"+envOr("API_PORT", "8080"), "API base URL")
	flag.StringVar(&o.apiKey, "api-key", os.Getenv("API_KEY"), "API key (default $API_KEY)")
	flag.StringVar(&o.databaseURL, "db", os.Getenv("DATABASE_URL"), "Postgres URL used to measure results (default $DATABASE_URL)")
	flag.StringVar(&o.queue, "queue", "bench", "queue to enqueue into (workers must consume it)")
	flag.IntVar(&o.jobs, "jobs", 5000, "number of jobs to enqueue")
	flag.IntVar(&o.concurrency, "concurrency", 50, "concurrent enqueue requests")
	flag.DurationVar(&o.sleep, "sleep", 10*time.Millisecond, "simulated work per job")
	flag.IntVar(&o.maxAttempts, "max-attempts", 25, "max_attempts per job (high so chaos cannot exhaust it)")
	flag.DurationVar(&o.timeout, "timeout", 10*time.Minute, "give up after this long")
	flag.StringVar(&o.label, "label", "", "configuration label for the report, e.g. \"2 workers x 10\"")
	flag.BoolVar(&o.markdown, "markdown", false, "also print the result as a markdown table row")
	flag.BoolVar(&o.chaos, "chaos", false, "kill/stop and restart worker containers at random during the run")
	flag.DurationVar(&o.chaosEvery, "chaos-every", 3*time.Second, "mean time between chaos actions")
	flag.StringVar(&o.composeProj, "compose-project", "forgeq", "docker compose project name (chaos mode)")
	flag.StringVar(&o.composeSvc, "compose-service", "worker", "docker compose service to disrupt (chaos mode)")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

func run(o options) error {
	if o.databaseURL == "" {
		return errors.New("-db or DATABASE_URL is required to measure results")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, o.timeout)
	defer cancel()

	pool, err := pgxpool.New(ctx, o.databaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	runID := newRunID()
	fmt.Printf("run %s: %d jobs on queue %q, %s work each, %d concurrent enqueues\n",
		runID, o.jobs, o.queue, o.sleep, o.concurrency)

	var monkey *chaosMonkey
	if o.chaos {
		monkey = newChaosMonkey(o.composeProj, o.composeSvc, o.chaosEvery)
		monkey.start(ctx)
	}

	enq, err := enqueueAll(ctx, o, runID)
	if err != nil {
		return err
	}
	fmt.Printf("enqueued %d jobs in %s (%.0f req/s, %d failed requests)\n",
		enq.ok, enq.took.Round(time.Millisecond), float64(enq.ok)/enq.took.Seconds(), enq.failed)
	if enq.ok != o.jobs {
		return fmt.Errorf("only %d of %d enqueues succeeded", enq.ok, o.jobs)
	}

	if err := waitForCompletion(ctx, pool, o.queue, runID, o.jobs); err != nil {
		return err
	}

	var chaosReport *chaosResult
	if monkey != nil {
		actions := monkey.stop()
		res, err := monkey.verify(context.Background(), runID, o.jobs)
		if err != nil {
			return fmt.Errorf("chaos verification: %w", err)
		}
		res.actions = actions
		chaosReport = &res
	}

	m, err := measure(context.Background(), pool, o.queue, runID)
	if err != nil {
		return err
	}
	printReport(o, m, chaosReport)

	if m.succeeded != o.jobs {
		return fmt.Errorf("%d of %d jobs succeeded", m.succeeded, o.jobs)
	}
	if chaosReport != nil && chaosReport.neverExecuted > 0 {
		return fmt.Errorf("%d jobs succeeded without a logged execution", chaosReport.neverExecuted)
	}
	return nil
}

func newRunID() string {
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
