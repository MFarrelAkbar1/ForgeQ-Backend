package metrics

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
	"github.com/MFarrelAkbar1/ForgeQ/internal/worker"
)

func TestWorkerMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewWorker(reg)

	m.JobStarted("emails")
	m.JobStarted("emails")
	m.JobFinished("emails", "send_email", worker.OutcomeSucceeded, 200*time.Millisecond)
	m.JobReaped(job.StatusRetrying)
	m.JobsDeleted(3)

	if got := testutil.ToFloat64(m.inflight.WithLabelValues("emails")); got != 1 {
		t.Errorf("inflight = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.processed.WithLabelValues("emails", "send_email", "succeeded")); got != 1 {
		t.Errorf("processed = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.duration); got != 1 {
		t.Errorf("duration series = %d, want 1", got)
	}
	if got := testutil.ToFloat64(m.reaped.WithLabelValues("retrying")); got != 1 {
		t.Errorf("reaped = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.deleted); got != 3 {
		t.Errorf("deleted = %v, want 3", got)
	}
}

func TestQueueDepthFillsMissingStatuses(t *testing.T) {
	reg := prometheus.NewRegistry()
	RegisterQueueDepth(reg, func(context.Context) ([]store.QueueStat, error) {
		return []store.QueueStat{
			{Queue: "emails", Status: job.StatusPending, Count: 4},
			{Queue: "emails", Status: job.StatusDead, Count: 1},
		}, nil
	})

	want := `
# HELP forgeq_jobs Jobs in the database by queue and status.
# TYPE forgeq_jobs gauge
forgeq_jobs{queue="emails",status="dead"} 1
forgeq_jobs{queue="emails",status="pending"} 4
forgeq_jobs{queue="emails",status="retrying"} 0
forgeq_jobs{queue="emails",status="running"} 0
forgeq_jobs{queue="emails",status="succeeded"} 0
`
	if err := testutil.GatherAndCompare(reg, strings.NewReader(want), "forgeq_jobs"); err != nil {
		t.Error(err)
	}
}

func TestQueueDepthReportsErrors(t *testing.T) {
	reg := prometheus.NewRegistry()
	RegisterQueueDepth(reg, func(context.Context) ([]store.QueueStat, error) {
		return nil, errors.New("db down")
	})
	if _, err := reg.Gather(); err == nil || !strings.Contains(err.Error(), "db down") {
		t.Errorf("Gather error = %v, want the stats error surfaced", err)
	}
}

func TestAPIMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()
	m := NewAPI(reg)
	m.JobEnqueued(true)
	m.JobEnqueued(false)
	m.HTTPRequest("GET", "/api/v1/jobs/{id}", 200, 5*time.Millisecond)

	if got := testutil.ToFloat64(m.enqueued.WithLabelValues("duplicate")); got != 1 {
		t.Errorf("duplicate enqueues = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.requests.WithLabelValues("GET", "/api/v1/jobs/{id}", "200")); got != 1 {
		t.Errorf("requests = %v, want 1", got)
	}
}
