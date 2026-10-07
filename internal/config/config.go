// Package config loads process configuration from environment variables.
package config

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds settings shared by the api and worker binaries. Each binary
// only reads the fields it needs.
type Config struct {
	DatabaseURL string // DATABASE_URL (required)
	DBMaxConns  int32  // DB_MAX_CONNS: pgx pool size per process (default 10)
	LogLevel    string // LOG_LEVEL: debug|info|warn|error
	LogFormat   string // LOG_FORMAT: json|text

	HTTPAddr string // HTTP_ADDR for the API server
	// APIKey (API_KEY) protects all of /api/v1 and the dashboard; empty disables auth.
	APIKey string

	Worker Worker
}

// Worker holds worker-only settings.
type Worker struct {
	ID                string         // WORKER_ID (default: hostname-random)
	OpsAddr           string         // OPS_ADDR: /healthz, /readyz and /metrics for the worker
	Queues            map[string]int // WORKER_QUEUES, e.g. "default:10,emails:5"
	PollInterval      time.Duration  // POLL_INTERVAL
	LeaseDuration     time.Duration  // LEASE_DURATION
	HeartbeatInterval time.Duration  // HEARTBEAT_INTERVAL (must be < LEASE_DURATION)
	JobTimeout        time.Duration  // DEFAULT_JOB_TIMEOUT
	ShutdownTimeout   time.Duration  // SHUTDOWN_TIMEOUT: how long running jobs may finish after SIGTERM
	ReaperInterval    time.Duration  // REAPER_INTERVAL: how often expired leases and old jobs are cleaned up
	JobRetention      time.Duration  // JOB_RETENTION: succeeded jobs older than this are deleted ("7d" allowed)
	BackoffBase       time.Duration  // BACKOFF_BASE: delay before the first retry
	BackoffMax        time.Duration  // BACKOFF_MAX: upper bound for any retry delay
	BackoffJitter     time.Duration  // BACKOFF_JITTER: random extra delay in [0, jitter)
	DemoFailRate      float64        // DEMO_FAIL_RATE for the demo handlers
}

// Load reads configuration from the environment, collecting every problem
// into one error so misconfiguration is reported all at once.
func Load() (Config, error) {
	var errs []error
	p := parser{errs: &errs}

	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		DBMaxConns:  int32(p.int("DB_MAX_CONNS", 10)),
		LogLevel:    p.str("LOG_LEVEL", "info"),
		LogFormat:   p.str("LOG_FORMAT", "json"),
		HTTPAddr:    p.str("HTTP_ADDR", ":8080"),
		APIKey:      os.Getenv("API_KEY"),
		Worker: Worker{
			ID:                p.str("WORKER_ID", defaultWorkerID()),
			OpsAddr:           p.str("OPS_ADDR", ":9090"),
			Queues:            p.queues("WORKER_QUEUES", "default:10"),
			PollInterval:      p.duration("POLL_INTERVAL", time.Second),
			LeaseDuration:     p.duration("LEASE_DURATION", 30*time.Second),
			HeartbeatInterval: p.duration("HEARTBEAT_INTERVAL", 10*time.Second),
			JobTimeout:        p.duration("DEFAULT_JOB_TIMEOUT", 30*time.Second),
			ShutdownTimeout:   p.duration("SHUTDOWN_TIMEOUT", 25*time.Second),
			ReaperInterval:    p.duration("REAPER_INTERVAL", 15*time.Second),
			JobRetention:      p.duration("JOB_RETENTION", 7*24*time.Hour),
			BackoffBase:       p.duration("BACKOFF_BASE", 2*time.Second),
			BackoffMax:        p.duration("BACKOFF_MAX", time.Hour),
			BackoffJitter:     p.duration("BACKOFF_JITTER", time.Second),
			DemoFailRate:      p.float("DEMO_FAIL_RATE", 0.2),
		},
	}

	if cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required"))
	}
	if cfg.Worker.HeartbeatInterval >= cfg.Worker.LeaseDuration {
		errs = append(errs, errors.New("HEARTBEAT_INTERVAL must be shorter than LEASE_DURATION"))
	}
	if cfg.Worker.BackoffBase > cfg.Worker.BackoffMax {
		errs = append(errs, errors.New("BACKOFF_BASE must not exceed BACKOFF_MAX"))
	}
	if cfg.Worker.DemoFailRate < 0 || cfg.Worker.DemoFailRate > 1 {
		errs = append(errs, errors.New("DEMO_FAIL_RATE must be between 0 and 1"))
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// parser reads typed values, recording errors instead of returning them.
type parser struct{ errs *[]error }

func (p parser) fail(key string, err error) {
	*p.errs = append(*p.errs, fmt.Errorf("%s: %w", key, err))
}

func (p parser) str(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}

func (p parser) duration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := parseDuration(v)
	if err != nil {
		p.fail(key, err)
		return def
	}
	if d <= 0 {
		p.fail(key, errors.New("must be positive"))
		return def
	}
	return d
}

// parseDuration extends time.ParseDuration with a whole-day suffix ("7d"),
// which reads better for retention periods than "168h".
func parseDuration(s string) (time.Duration, error) {
	if days, ok := strings.CutSuffix(s, "d"); ok {
		n, err := strconv.Atoi(days)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}

func (p parser) int(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		p.fail(key, errors.New("must be a positive integer"))
		return def
	}
	return n
}

func (p parser) float(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		p.fail(key, err)
		return def
	}
	return f
}

func (p parser) queues(key, def string) map[string]int {
	v := p.str(key, def)
	q, err := ParseQueues(v)
	if err != nil {
		p.fail(key, err)
	}
	return q
}

// ParseQueues parses "name:concurrency" pairs separated by commas.
// A bare name gets concurrency 1.
func ParseQueues(s string) (map[string]int, error) {
	queues := make(map[string]int)
	for part := range strings.SplitSeq(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, n, hasN := strings.Cut(part, ":")
		name = strings.TrimSpace(name)
		concurrency := 1
		if hasN {
			var err error
			concurrency, err = strconv.Atoi(strings.TrimSpace(n))
			if err != nil || concurrency < 1 {
				return nil, fmt.Errorf("queue %q: concurrency must be a positive integer", name)
			}
		}
		if name == "" {
			return nil, fmt.Errorf("empty queue name in %q", part)
		}
		if _, dup := queues[name]; dup {
			return nil, fmt.Errorf("queue %q listed twice", name)
		}
		queues[name] = concurrency
	}
	if len(queues) == 0 {
		return nil, errors.New("at least one queue is required")
	}
	return queues, nil
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "worker"
	}
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return host + "-" + hex.EncodeToString(b)
}
