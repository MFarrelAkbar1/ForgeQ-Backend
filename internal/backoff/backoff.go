// Package backoff computes retry delays with exponential growth and jitter.
package backoff

import (
	"math/rand/v2"
	"time"
)

// Policy is an exponential backoff with additive jitter:
//
//	delay = min(Base * 2^(attempts-1) + rand[0, Jitter), Max)
//
// attempts is the number of attempts already made (>= 1), so the first retry
// waits about Base. Jitter spreads out retries of jobs that failed together
// (e.g. during a downstream outage) so they do not all hit the dependency at
// the same instant when it recovers.
type Policy struct {
	Base   time.Duration
	Max    time.Duration
	Jitter time.Duration
	// Rand returns a float in [0, 1). Nil uses math/rand/v2; tests inject a fixed value.
	Rand func() float64
}

// Delay returns how long to wait before the next attempt.
func (p Policy) Delay(attempts int) time.Duration {
	exp := max(attempts-1, 0)

	// Double step by step so a large attempt count cannot overflow int64.
	d := p.Base
	for range exp {
		if d >= p.Max {
			break
		}
		d *= 2
	}

	if p.Jitter > 0 {
		r := p.Rand
		if r == nil {
			r = rand.Float64
		}
		d += time.Duration(r() * float64(p.Jitter))
	}
	return min(d, p.Max)
}
