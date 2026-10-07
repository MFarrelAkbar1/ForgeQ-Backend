// Package handlers contains demo job handlers. They simulate work with sleeps
// and fail at random so retries and the dead-letter queue can be observed.
package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ/internal/worker"
)

// Options tunes the simulation.
type Options struct {
	FailRate float64      // probability in [0,1] that an attempt fails with a transient error
	Logger   *slog.Logger // used by the bench handler to record executions
}

// Register adds the demo handlers and the bench handler to r.
func Register(r *worker.Registry, opts Options) {
	d := demo{failRate: opts.FailRate, rand: rand.Float64}
	r.RegisterHandler("send_email", d.sendEmail)
	r.RegisterHandler("image_resize", d.imageResize)
	r.RegisterHandler("generate_report", d.generateReport, worker.WithTimeout(2*time.Minute))

	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	r.RegisterHandler(BenchType, bench(logger))
}

// BenchType is the job type used by cmd/loadgen.
const BenchType = "bench"

// BenchExecutedMsg is logged once per completed bench execution; loadgen's
// chaos mode counts these lines to detect jobs that ran more than once.
const BenchExecutedMsg = "bench job executed"

type benchPayload struct {
	Run     string `json:"run"`      // loadgen run id
	SleepMS int    `json:"sleep_ms"` // simulated work
}

// bench sleeps for sleep_ms and never fails on its own, so benchmark numbers
// measure the queue rather than random failures.
func bench(logger *slog.Logger) worker.HandlerFunc {
	return func(ctx context.Context, j *job.Job) error {
		var p benchPayload
		if err := decode(j, &p); err != nil {
			return err
		}
		t := time.NewTimer(time.Duration(p.SleepMS) * time.Millisecond)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
		logger.Info(BenchExecutedMsg, "job_id", j.ID, "run", p.Run, "attempt", j.Attempts)
		return nil
	}
}

type demo struct {
	failRate float64
	rand     func() float64 // injectable for tests
}

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (d demo) sendEmail(ctx context.Context, j *job.Job) error {
	var p emailPayload
	if err := decode(j, &p); err != nil {
		return err
	}
	if p.To == "" {
		return worker.Permanent(errors.New("payload.to is required"))
	}
	return d.simulate(ctx, 50*time.Millisecond, 300*time.Millisecond, "smtp server unavailable")
}

type imagePayload struct {
	URL    string `json:"url"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
}

func (d demo) imageResize(ctx context.Context, j *job.Job) error {
	var p imagePayload
	if err := decode(j, &p); err != nil {
		return err
	}
	if p.URL == "" || p.Width <= 0 || p.Height <= 0 {
		return worker.Permanent(errors.New("payload needs url and positive width/height"))
	}
	return d.simulate(ctx, 200*time.Millisecond, time.Second, "image storage timeout")
}

type reportPayload struct {
	ReportID string `json:"report_id"`
}

func (d demo) generateReport(ctx context.Context, j *job.Job) error {
	var p reportPayload
	if err := decode(j, &p); err != nil {
		return err
	}
	if p.ReportID == "" {
		return worker.Permanent(errors.New("payload.report_id is required"))
	}
	return d.simulate(ctx, time.Second, 3*time.Second, "report database deadlock")
}

// decode unmarshals the payload; a malformed payload will never succeed, so
// the error is permanent.
func decode(j *job.Job, v any) error {
	if err := json.Unmarshal(j.Payload, v); err != nil {
		return worker.Permanent(fmt.Errorf("decode payload: %w", err))
	}
	return nil
}

// simulate sleeps for a random duration in [lo, hi) while honouring ctx, then
// fails with transientErr with probability failRate.
func (d demo) simulate(ctx context.Context, lo, hi time.Duration, transientErr string) error {
	work := lo + time.Duration(d.rand()*float64(hi-lo))
	t := time.NewTimer(work)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
	}
	if d.rand() < d.failRate {
		return errors.New(transientErr)
	}
	return nil
}
