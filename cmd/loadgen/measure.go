package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// waitForCompletion polls until every job of the run has succeeded, printing
// progress. Dead jobs fail the run: bench jobs only die if something is wrong.
func waitForCompletion(ctx context.Context, pool *pgxpool.Pool, queue, runID string, total int) error {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	lastPrint := time.Now()
	for {
		counts, err := statusCounts(ctx, pool, queue, runID)
		if err != nil {
			return err
		}
		if counts["dead"] > 0 {
			return fmt.Errorf("%d bench jobs are dead", counts["dead"])
		}
		if counts["succeeded"] == total {
			return nil
		}
		if time.Since(lastPrint) >= 2*time.Second {
			fmt.Printf("  progress: %v\n", counts)
			lastPrint = time.Now()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for jobs (%v): %w", counts, ctx.Err())
		case <-t.C:
		}
	}
}

func statusCounts(ctx context.Context, pool *pgxpool.Pool, queue, runID string) (map[string]int, error) {
	rows, err := pool.Query(ctx, `
		SELECT status, count(*) FROM jobs
		WHERE queue = $1 AND payload->>'run' = $2
		GROUP BY status`, queue, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			return nil, err
		}
		counts[status] = n
	}
	return counts, rows.Err()
}

type measurement struct {
	succeeded   int
	redelivered int           // jobs that needed more than one attempt
	makespan    time.Duration // first enqueue -> last finish
	startP      [3]float64    // enqueue -> start of the successful attempt, p50/p95/p99 (seconds)
	e2eP        [3]float64    // enqueue -> finish, p50/p95/p99 (seconds)
}

func (m measurement) throughput() float64 { return float64(m.succeeded) / m.makespan.Seconds() }

// measure computes the run's numbers in SQL. All timestamps come from the
// database clock, so client/server clock skew does not matter.
func measure(ctx context.Context, pool *pgxpool.Pool, queue, runID string) (measurement, error) {
	var m measurement
	var startP, e2eP []float64
	var makespanSecs float64
	err := pool.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE attempts > 1),
		       extract(epoch FROM max(finished_at) - min(created_at)),
		       percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM started_at - created_at)),
		       percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (ORDER BY extract(epoch FROM finished_at - created_at))
		FROM jobs
		WHERE queue = $1 AND payload->>'run' = $2 AND status = 'succeeded'`,
		queue, runID).Scan(&m.succeeded, &m.redelivered, &makespanSecs, &startP, &e2eP)
	if err != nil {
		return m, fmt.Errorf("measure: %w", err)
	}
	m.makespan = time.Duration(makespanSecs * float64(time.Second))
	copy(m.startP[:], startP)
	copy(m.e2eP[:], e2eP)
	return m, nil
}

func ms(secs float64) string { return fmt.Sprintf("%.0f ms", secs*1000) }

func printReport(o options, m measurement, chaos *chaosResult) {
	var b strings.Builder
	fmt.Fprintf(&b, "\n== result %s\n", o.label)
	fmt.Fprintf(&b, "jobs succeeded:      %d / %d\n", m.succeeded, o.jobs)
	fmt.Fprintf(&b, "makespan:            %s (first enqueue -> last finish)\n", m.makespan.Round(time.Millisecond))
	fmt.Fprintf(&b, "throughput:          %.0f jobs/s\n", m.throughput())
	fmt.Fprintf(&b, "enqueue -> start:    p50 %s  p95 %s  p99 %s\n", ms(m.startP[0]), ms(m.startP[1]), ms(m.startP[2]))
	fmt.Fprintf(&b, "enqueue -> finish:   p50 %s  p95 %s  p99 %s\n", ms(m.e2eP[0]), ms(m.e2eP[1]), ms(m.e2eP[2]))
	fmt.Fprintf(&b, "jobs with >1 attempt: %d\n", m.redelivered)
	if chaos != nil {
		fmt.Fprintf(&b, "\n== chaos\n")
		fmt.Fprintf(&b, "disruptions:         %d (%d SIGKILL, %d graceful stop)\n",
			chaos.actions.kills+chaos.actions.stops, chaos.actions.kills, chaos.actions.stops)
		fmt.Fprintf(&b, "handler executions:  %d for %d jobs\n", chaos.executions, o.jobs)
		fmt.Fprintf(&b, "jobs executed twice+: %d (max %d executions for one job)\n", chaos.duplicated, chaos.maxPerJob)
		fmt.Fprintf(&b, "jobs lost:           %d\n", chaos.neverExecuted)
	}
	fmt.Print(b.String())
	if o.markdown {
		fmt.Printf("\n| %s | %d | %.0f | %s | %s | %s |\n",
			o.label, m.succeeded, m.throughput(), ms(m.startP[0]), ms(m.startP[1]), ms(m.startP[2]))
	}
}
