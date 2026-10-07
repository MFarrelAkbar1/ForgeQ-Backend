package job

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestCanTransition(t *testing.T) {
	allowed := map[[2]Status]bool{
		{StatusPending, StatusRunning}:   true,
		{StatusRetrying, StatusRunning}:  true,
		{StatusRunning, StatusSucceeded}: true,
		{StatusRunning, StatusRetrying}:  true,
		{StatusRunning, StatusDead}:      true,
		{StatusRunning, StatusPending}:   true,
		{StatusDead, StatusPending}:      true,
	}

	// Check every pair so a newly added edge has to be listed here on purpose.
	for _, from := range Statuses {
		for _, to := range Statuses {
			want := allowed[[2]Status{from, to}]
			if got := CanTransition(from, to); got != want {
				t.Errorf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
			}
		}
	}
}

func TestStatusValid(t *testing.T) {
	for _, s := range Statuses {
		if !s.Valid() {
			t.Errorf("%s should be valid", s)
		}
	}
	if Status("bogus").Valid() {
		t.Error("bogus status should be invalid")
	}
}

func TestNewJobNormalizeDefaults(t *testing.T) {
	n := NewJob{Type: " send_email "}
	if err := n.Normalize(); err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if n.Type != "send_email" {
		t.Errorf("Type = %q, want trimmed", n.Type)
	}
	if n.Queue != DefaultQueue {
		t.Errorf("Queue = %q, want %q", n.Queue, DefaultQueue)
	}
	if n.MaxAttempts != DefaultMaxAttempts {
		t.Errorf("MaxAttempts = %d, want %d", n.MaxAttempts, DefaultMaxAttempts)
	}
	if string(n.Payload) != "{}" {
		t.Errorf("Payload = %s, want {}", n.Payload)
	}
}

func TestNewJobNormalizeInvalid(t *testing.T) {
	tests := []struct {
		name  string
		job   NewJob
		field string
	}{
		{"missing type", NewJob{}, "type"},
		{"uppercase type", NewJob{Type: "SendEmail"}, "type"},
		{"bad queue", NewJob{Type: "x", Queue: "has space"}, "queue"},
		{"long type", NewJob{Type: strings.Repeat("a", maxNameLen+1)}, "type"},
		{"invalid payload", NewJob{Type: "x", Payload: json.RawMessage(`{`)}, "payload"},
		{"priority too high", NewJob{Type: "x", Priority: MaxPriority + 1}, "priority"},
		{"priority too low", NewJob{Type: "x", Priority: MinPriority - 1}, "priority"},
		{"negative attempts", NewJob{Type: "x", MaxAttempts: -1}, "max_attempts"},
		{"too many attempts", NewJob{Type: "x", MaxAttempts: MaxAttemptsLimit + 1}, "max_attempts"},
		{"long key", NewJob{Type: "x", IdempotencyKey: strings.Repeat("k", maxIdempotencyLen+1)}, "idempotency_key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.job.Normalize()
			verr, ok := errors.AsType[*ValidationError](err)
			if !ok {
				t.Fatalf("Normalize() error = %v, want *ValidationError", err)
			}
			if _, ok := verr.Fields[tt.field]; !ok {
				t.Errorf("Fields = %v, want key %q", verr.Fields, tt.field)
			}
		})
	}
}
