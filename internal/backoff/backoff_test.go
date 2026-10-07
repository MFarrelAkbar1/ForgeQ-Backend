package backoff

import (
	"testing"
	"time"
)

func fixed(v float64) func() float64 { return func() float64 { return v } }

func TestDelay(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Minute, Jitter: 500 * time.Millisecond}

	tests := []struct {
		name     string
		attempts int
		rand     float64
		want     time.Duration
	}{
		{"first retry waits base", 1, 0, time.Second},
		{"second retry doubles", 2, 0, 2 * time.Second},
		{"fifth retry", 5, 0, 16 * time.Second},
		{"jitter is added", 3, 0.5, 4*time.Second + 250*time.Millisecond},
		{"capped at max", 7, 0, time.Minute}, // 64s > 60s
		{"cap includes jitter", 6, 0.99, 32*time.Second + 495*time.Millisecond},
		{"huge attempts do not overflow", 10_000, 0.99, time.Minute},
		{"zero attempts treated as first", 0, 0, time.Second},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := p
			p.Rand = fixed(tt.rand)
			if got := p.Delay(tt.attempts); got != tt.want {
				t.Errorf("Delay(%d) = %v, want %v", tt.attempts, got, tt.want)
			}
		})
	}
}

func TestDelayJitterBounds(t *testing.T) {
	p := Policy{Base: time.Second, Max: time.Hour, Jitter: time.Second}
	for range 1000 {
		d := p.Delay(2) // real random source
		if d < 2*time.Second || d >= 3*time.Second {
			t.Fatalf("Delay(2) = %v, want in [2s, 3s)", d)
		}
	}
}

func TestDelayNeverExceedsMax(t *testing.T) {
	p := Policy{Base: time.Second, Max: 10 * time.Second, Jitter: 5 * time.Second, Rand: fixed(0.999)}
	for attempts := 1; attempts <= 100; attempts++ {
		if d := p.Delay(attempts); d > p.Max {
			t.Fatalf("Delay(%d) = %v exceeds max %v", attempts, d, p.Max)
		}
	}
}
