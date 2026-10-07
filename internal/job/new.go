package job

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	DefaultQueue       = "default"
	DefaultMaxAttempts = 5

	MinPriority       = -100
	MaxPriority       = 100
	MaxAttemptsLimit  = 100
	maxNameLen        = 100
	maxIdempotencyLen = 255
)

var namePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_.\-]*$`)

// NewJob is a request to enqueue a job. Zero values are replaced by defaults
// in Normalize.
type NewJob struct {
	Queue          string
	Type           string
	Payload        json.RawMessage
	Priority       int
	MaxAttempts    int
	RunAt          *time.Time // nil means run as soon as possible
	IdempotencyKey string     // empty means no deduplication
}

// Normalize fills defaults and validates the request.
func (n *NewJob) Normalize() error {
	n.Queue = strings.TrimSpace(n.Queue)
	n.Type = strings.TrimSpace(n.Type)
	n.IdempotencyKey = strings.TrimSpace(n.IdempotencyKey)
	if n.Queue == "" {
		n.Queue = DefaultQueue
	}
	if n.MaxAttempts == 0 {
		n.MaxAttempts = DefaultMaxAttempts
	}
	if len(n.Payload) == 0 {
		n.Payload = json.RawMessage(`{}`)
	}

	fields := map[string]string{}
	if msg := validateName(n.Type); msg != "" {
		fields["type"] = msg
	}
	if msg := validateName(n.Queue); msg != "" {
		fields["queue"] = msg
	}
	if !json.Valid(n.Payload) {
		fields["payload"] = "must be valid JSON"
	}
	if n.Priority < MinPriority || n.Priority > MaxPriority {
		fields["priority"] = fmt.Sprintf("must be between %d and %d", MinPriority, MaxPriority)
	}
	if n.MaxAttempts < 1 || n.MaxAttempts > MaxAttemptsLimit {
		fields["max_attempts"] = fmt.Sprintf("must be between 1 and %d", MaxAttemptsLimit)
	}
	if len(n.IdempotencyKey) > maxIdempotencyLen {
		fields["idempotency_key"] = fmt.Sprintf("must be at most %d characters", maxIdempotencyLen)
	}
	if len(fields) > 0 {
		return &ValidationError{Fields: fields}
	}
	return nil
}

func validateName(s string) string {
	switch {
	case s == "":
		return "is required"
	case len(s) > maxNameLen:
		return fmt.Sprintf("must be at most %d characters", maxNameLen)
	case !namePattern.MatchString(s):
		return "may only contain lowercase letters, digits, '_', '.' and '-'"
	}
	return ""
}

// ValidationError describes invalid fields of a request, keyed by JSON field name.
type ValidationError struct {
	Fields map[string]string
}

func (e *ValidationError) Error() string {
	keys := make([]string, 0, len(e.Fields))
	for k := range e.Fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = k + " " + e.Fields[k]
	}
	return "invalid job: " + strings.Join(parts, "; ")
}
