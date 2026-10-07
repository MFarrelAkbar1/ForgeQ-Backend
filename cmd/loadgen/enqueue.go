package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/internal/handlers"
)

type enqueueResult struct {
	ok, failed int
	took       time.Duration
}

// enqueueAll posts o.jobs bench jobs with o.concurrency parallel requests.
func enqueueAll(ctx context.Context, o options, runID string) (enqueueResult, error) {
	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        o.concurrency,
			MaxIdleConnsPerHost: o.concurrency,
		},
	}
	defer client.CloseIdleConnections()

	payload, err := json.Marshal(map[string]any{"run": runID, "sleep_ms": o.sleep.Milliseconds()})
	if err != nil {
		return enqueueResult{}, err
	}

	var ok, failed atomic.Int64
	var firstErr error
	var errOnce sync.Once
	next := make(chan int)
	start := time.Now()

	var wg sync.WaitGroup
	for range o.concurrency {
		wg.Go(func() {
			for range next {
				if err := enqueueOne(ctx, client, o, payload); err != nil {
					failed.Add(1)
					errOnce.Do(func() { firstErr = err })
					continue
				}
				ok.Add(1)
			}
		})
	}
	for i := range o.jobs {
		select {
		case next <- i:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			break
		}
	}
	close(next)
	wg.Wait()

	res := enqueueResult{ok: int(ok.Load()), failed: int(failed.Load()), took: time.Since(start)}
	if firstErr != nil && res.ok == 0 {
		return res, fmt.Errorf("enqueue: %w", firstErr)
	}
	if firstErr != nil {
		fmt.Println("first enqueue error:", firstErr)
	}
	return res, ctx.Err()
}

func enqueueOne(ctx context.Context, client *http.Client, o options, payload json.RawMessage) error {
	body, err := json.Marshal(map[string]any{
		"type":         handlers.BenchType,
		"queue":        o.queue,
		"payload":      payload,
		"max_attempts": o.maxAttempts,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, o.apiURL+"/api/v1/jobs", bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if o.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+o.apiKey)
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("status %d: %s", resp.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}
