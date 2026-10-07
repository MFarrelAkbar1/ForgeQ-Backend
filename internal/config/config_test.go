package config

import (
	"maps"
	"strings"
	"testing"
	"time"
)

func TestParseQueues(t *testing.T) {
	tests := []struct {
		in      string
		want    map[string]int
		wantErr bool
	}{
		{in: "default:10", want: map[string]int{"default": 10}},
		{in: "default:10, emails:5 ,reports", want: map[string]int{"default": 10, "emails": 5, "reports": 1}},
		{in: "", wantErr: true},
		{in: "a:0", wantErr: true},
		{in: "a:x", wantErr: true},
		{in: ":3", wantErr: true},
		{in: "a,a", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := ParseQueues(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !maps.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

// clearEnv blanks every variable Load reads so tests do not depend on the
// caller's environment (Task, for example, loads .env).
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"DATABASE_URL", "DB_MAX_CONNS", "LOG_LEVEL", "LOG_FORMAT", "HTTP_ADDR", "OPS_ADDR", "API_KEY", "WORKER_ID", "WORKER_QUEUES",
		"POLL_INTERVAL", "LEASE_DURATION", "DEFAULT_JOB_TIMEOUT", "DEMO_FAIL_RATE",
		"BACKOFF_BASE", "BACKOFF_MAX", "BACKOFF_JITTER", "HEARTBEAT_INTERVAL", "SHUTDOWN_TIMEOUT",
		"REAPER_INTERVAL", "JOB_RETENTION",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://localhost/forgeq")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.Worker.LeaseDuration != 30*time.Second ||
		cfg.Worker.JobRetention != 7*24*time.Hour {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
	if cfg.Worker.ID == "" {
		t.Error("worker ID should default to a generated value")
	}
}

func TestParseDuration(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{in: "7d", want: 7 * 24 * time.Hour},
		{in: "1d", want: 24 * time.Hour},
		{in: "90m", want: 90 * time.Minute},
		{in: "1.5d", wantErr: true},
		{in: "d", wantErr: true},
		{in: "soon", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseDuration(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	clearEnv(t)
	t.Setenv("POLL_INTERVAL", "soon")
	t.Setenv("DEMO_FAIL_RATE", "2")
	t.Setenv("BACKOFF_BASE", "2h")
	t.Setenv("BACKOFF_MAX", "1h")
	t.Setenv("HEARTBEAT_INTERVAL", "1m")
	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"DATABASE_URL", "POLL_INTERVAL", "DEMO_FAIL_RATE", "BACKOFF_BASE", "HEARTBEAT_INTERVAL"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q should mention %s", err, want)
		}
	}
}
