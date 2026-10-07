package worker

import (
	"time"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
)

// Outcome is how a claimed job left the worker.
type Outcome string

const (
	OutcomeSucceeded Outcome = "succeeded"  // handler returned nil
	OutcomeRetrying  Outcome = "retrying"   // failed, will run again later
	OutcomeDead      Outcome = "dead"       // failed permanently or out of attempts
	OutcomeLeaseLost Outcome = "lease_lost" // another worker owns the job now; result discarded
	OutcomeReleased  Outcome = "released"   // interrupted by shutdown, handed back
	OutcomeError     Outcome = "error"      // the result could not be saved
)

// Observer receives job lifecycle events, e.g. to export metrics. Methods are
// called from many goroutines and must be safe for concurrent use.
type Observer interface {
	JobStarted(queue string)
	JobFinished(queue, jobType string, outcome Outcome, took time.Duration)
	JobReaped(status job.Status)
	JobsDeleted(n int64)
}

type nopObserver struct{}

func (nopObserver) JobStarted(string)                                  {}
func (nopObserver) JobFinished(string, string, Outcome, time.Duration) {}
func (nopObserver) JobReaped(job.Status)                               {}
func (nopObserver) JobsDeleted(int64)                                  {}
