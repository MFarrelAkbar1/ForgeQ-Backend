// Package metrics exports ForgeQ's Prometheus metrics. Each process builds its
// own registry (no global state) and passes the recorders below to the worker
// and api packages, which only see their small Observer interfaces.
package metrics

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/MFarrelAkbar1/ForgeQ/internal/api"
	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ/internal/store"
	"github.com/MFarrelAkbar1/ForgeQ/internal/worker"
)

const namespace = "forgeq"

// NewRegistry returns a registry with the Go runtime and process collectors.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}

// Handler serves the registry in the Prometheus exposition format.
func Handler(reg *prometheus.Registry) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})
}

// Worker records job processing; it implements worker.Observer.
type Worker struct {
	inflight  *prometheus.GaugeVec
	processed *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	reaped    *prometheus.CounterVec
	deleted   prometheus.Counter
}

var _ worker.Observer = (*Worker)(nil)

// NewWorker registers the worker metrics on reg.
func NewWorker(reg prometheus.Registerer) *Worker {
	f := promauto.With(reg)
	return &Worker{
		inflight: f.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "jobs_inflight",
			Help: "Jobs currently being processed by this worker.",
		}, []string{"queue"}),
		processed: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_processed_total",
			Help: "Jobs processed by this worker, by outcome (succeeded, retrying, dead, lease_lost, released, error).",
		}, []string{"queue", "type", "outcome"}),
		duration: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "job_duration_seconds",
			Help:    "Time spent running a job's handler.",
			Buckets: []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300},
		}, []string{"queue", "type"}),
		reaped: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_reaped_total",
			Help: "Jobs whose lease expired and that the reaper took back, by resulting status.",
		}, []string{"status"}),
		deleted: f.NewCounter(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_retention_deleted_total",
			Help: "Succeeded jobs deleted by retention.",
		}),
	}
}

func (m *Worker) JobStarted(queue string) { m.inflight.WithLabelValues(queue).Inc() }

func (m *Worker) JobFinished(queue, jobType string, outcome worker.Outcome, took time.Duration) {
	m.inflight.WithLabelValues(queue).Dec()
	m.processed.WithLabelValues(queue, jobType, string(outcome)).Inc()
	m.duration.WithLabelValues(queue, jobType).Observe(took.Seconds())
}

func (m *Worker) JobReaped(status job.Status) { m.reaped.WithLabelValues(string(status)).Inc() }

func (m *Worker) JobsDeleted(n int64) { m.deleted.Add(float64(n)) }

// API records enqueue and HTTP traffic; it implements api.Observer.
type API struct {
	enqueued *prometheus.CounterVec
	requests *prometheus.CounterVec
	latency  *prometheus.HistogramVec
}

var _ api.Observer = (*API)(nil)

// NewAPI registers the API metrics on reg. Labels deliberately avoid
// client-chosen values (queue, type) so a client cannot blow up cardinality;
// per-queue numbers come from the queue depth collector instead.
func NewAPI(reg prometheus.Registerer) *API {
	f := promauto.With(reg)
	return &API{
		enqueued: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "jobs_enqueued_total",
			Help: "Enqueue requests, by result (created, duplicate for an idempotent replay).",
		}, []string{"result"}),
		requests: f.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace, Name: "http_requests_total",
			Help: "HTTP requests by method, route pattern and status code.",
		}, []string{"method", "route", "code"}),
		latency: f.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace, Name: "http_request_duration_seconds",
			Help:    "HTTP request latency by method and route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}
}

func (m *API) JobEnqueued(created bool) {
	result := "duplicate"
	if created {
		result = "created"
	}
	m.enqueued.WithLabelValues(result).Inc()
}

func (m *API) HTTPRequest(method, route string, code int, took time.Duration) {
	m.requests.WithLabelValues(method, route, strconv.Itoa(code)).Inc()
	m.latency.WithLabelValues(method, route).Observe(took.Seconds())
}

// StatsFunc returns job counts per queue and status.
type StatsFunc func(ctx context.Context) ([]store.QueueStat, error)

// queueCollector reads job counts from the database on every scrape, so the
// numbers are exact and shared by all replicas (a counter kept in memory by
// one api process would not see jobs enqueued through another).
type queueCollector struct {
	stats   StatsFunc
	timeout time.Duration
	desc    *prometheus.Desc
}

// RegisterQueueDepth registers the forgeq_jobs{queue,status} gauge on reg.
func RegisterQueueDepth(reg prometheus.Registerer, stats StatsFunc) {
	reg.MustRegister(&queueCollector{
		stats:   stats,
		timeout: 3 * time.Second,
		desc: prometheus.NewDesc(namespace+"_jobs", "Jobs in the database by queue and status.",
			[]string{"queue", "status"}, nil),
	})
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) { ch <- c.desc }

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	stats, err := c.stats(ctx)
	if err != nil {
		// Fails this metric (and shows the error on the scrape) instead of
		// silently reporting stale or zero values.
		ch <- prometheus.NewInvalidMetric(c.desc, err)
		return
	}

	// Report every status for each known queue so absent statuses read as 0
	// instead of disappearing from graphs and alerts.
	counts := map[string]map[job.Status]int64{}
	for _, s := range stats {
		if counts[s.Queue] == nil {
			counts[s.Queue] = map[job.Status]int64{}
		}
		counts[s.Queue][s.Status] = s.Count
	}
	for queue, byStatus := range counts {
		for _, status := range job.Statuses {
			ch <- prometheus.MustNewConstMetric(c.desc, prometheus.GaugeValue,
				float64(byStatus[status]), queue, string(status))
		}
	}
}
