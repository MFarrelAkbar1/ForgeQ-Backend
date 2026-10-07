package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/job"
	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/worker"
)

func TestDemoHandlers(t *testing.T) {
	succeed := demo{failRate: 0, rand: func() float64 { return 0 }}
	fail := demo{failRate: 1, rand: func() float64 { return 0 }}

	tests := []struct {
		name          string
		d             demo
		fn            func(demo) worker.HandlerFunc
		payload       string
		wantErr       bool
		wantPermanent bool
	}{
		{"email ok", succeed, func(d demo) worker.HandlerFunc { return d.sendEmail }, `{"to":"a@b.c"}`, false, false},
		{"email missing to", succeed, func(d demo) worker.HandlerFunc { return d.sendEmail }, `{}`, true, true},
		{"email transient", fail, func(d demo) worker.HandlerFunc { return d.sendEmail }, `{"to":"a@b.c"}`, true, false},
		{"image ok", succeed, func(d demo) worker.HandlerFunc { return d.imageResize }, `{"url":"x","width":1,"height":1}`, false, false},
		{"image bad size", succeed, func(d demo) worker.HandlerFunc { return d.imageResize }, `{"url":"x","width":0,"height":1}`, true, true},
		{"report ok", succeed, func(d demo) worker.HandlerFunc { return d.generateReport }, `{"report_id":"r1"}`, false, false},
		{"report malformed", succeed, func(d demo) worker.HandlerFunc { return d.generateReport }, `[]`, true, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := &job.Job{Payload: json.RawMessage(tt.payload)}
			err := tt.fn(tt.d)(context.Background(), j)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if got := worker.IsPermanent(err); got != tt.wantPermanent {
				t.Errorf("IsPermanent = %v, want %v (err %v)", got, tt.wantPermanent, err)
			}
		})
	}
}

func TestBenchHandler(t *testing.T) {
	var buf strings.Builder
	h := bench(slog.New(slog.NewJSONHandler(&buf, nil)))
	j := &job.Job{ID: 7, Attempts: 1, Payload: json.RawMessage(`{"run":"r1","sleep_ms":1}`)}
	if err := h(context.Background(), j); err != nil {
		t.Fatalf("bench: %v", err)
	}
	if !strings.Contains(buf.String(), BenchExecutedMsg) || !strings.Contains(buf.String(), `"job_id":7`) {
		t.Errorf("bench log = %s", buf.String())
	}
	if err := h(context.Background(), &job.Job{Payload: json.RawMessage(`[]`)}); !worker.IsPermanent(err) {
		t.Errorf("malformed payload: err = %v, want permanent", err)
	}
}

func TestSimulateHonoursContext(t *testing.T) {
	d := demo{rand: func() float64 { return 0.5 }}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j := &job.Job{Payload: json.RawMessage(`{"report_id":"r1"}`)}
	if err := d.generateReport(ctx, j); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}
