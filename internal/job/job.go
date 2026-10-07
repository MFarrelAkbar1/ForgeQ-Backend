// Package job holds the domain model of ForgeQ: jobs, their statuses and the
// rules for moving between them. It has no I/O so it can be tested in isolation.
package job

import (
	"encoding/json"
	"time"
)

// Status is the lifecycle state of a job.
type Status string

const (
	StatusPending   Status = "pending"   // waiting to run (possibly delayed via RunAt)
	StatusRunning   Status = "running"   // claimed by a worker holding a lease
	StatusRetrying  Status = "retrying"  // failed, waiting for its next attempt at RunAt
	StatusSucceeded Status = "succeeded" // finished successfully (terminal)
	StatusDead      Status = "dead"      // out of attempts or permanently failed (dead-letter)
)

// Statuses lists every status in lifecycle order.
var Statuses = []Status{StatusPending, StatusRunning, StatusRetrying, StatusSucceeded, StatusDead}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	for _, v := range Statuses {
		if s == v {
			return true
		}
	}
	return false
}

// transitions is the job state machine: from -> allowed targets.
var transitions = map[Status][]Status{
	StatusPending:  {StatusRunning},
	StatusRetrying: {StatusRunning},
	// running -> pending happens when a worker releases its lease on shutdown.
	StatusRunning: {StatusSucceeded, StatusRetrying, StatusDead, StatusPending},
	// dead -> pending is a manual retry from the API or dashboard.
	StatusDead:      {StatusPending},
	StatusSucceeded: nil,
}

// CanTransition reports whether the state machine allows moving from -> to.
func CanTransition(from, to Status) bool {
	for _, s := range transitions[from] {
		if s == to {
			return true
		}
	}
	return false
}

// Job is a unit of background work as stored in the database.
type Job struct {
	ID             int64           `json:"id"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int16           `json:"priority"`
	Status         Status          `json:"status"`
	Run            int             `json:"run"`      // 1 + number of manual retries from dead
	Attempts       int             `json:"attempts"` // attempts within the current run
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LockedBy       *string         `json:"locked_by,omitempty"`
	LockedUntil    *time.Time      `json:"locked_until,omitempty"`
	LastError      *string         `json:"last_error,omitempty"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

// AttemptError is one failed attempt in a job's error history.
type AttemptError struct {
	Run        int       `json:"run"`
	Attempt    int       `json:"attempt"`
	Error      string    `json:"error"`
	WorkerID   *string   `json:"worker_id,omitempty"`
	OccurredAt time.Time `json:"occurred_at"`
}
