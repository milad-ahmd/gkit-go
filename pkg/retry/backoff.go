package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff computes the wait duration before the next attempt.
type Backoff interface {
	// Next returns the delay before attempt n (0-indexed).
	Next(attempt int) time.Duration
}

// ConstantBackoff waits the same duration between every attempt.
type ConstantBackoff struct {
	Delay time.Duration
}

func (b ConstantBackoff) Next(_ int) time.Duration { return b.Delay }

// ExponentialBackoff implements truncated binary exponential backoff.
//
//	delay(n) = min(Initial * Multiplier^n, Max)
type ExponentialBackoff struct {
	Initial    time.Duration
	Multiplier float64
	Max        time.Duration
}

// DefaultExponential is a sensible default for most network operations.
var DefaultExponential = ExponentialBackoff{
	Initial:    100 * time.Millisecond,
	Multiplier: 2.0,
	Max:        30 * time.Second,
}

func (b ExponentialBackoff) Next(attempt int) time.Duration {
	d := float64(b.Initial) * math.Pow(b.Multiplier, float64(attempt))
	if b.Max > 0 && time.Duration(d) > b.Max {
		return b.Max
	}
	return time.Duration(d)
}

// Jitter wraps a Backoff and adds random jitter to prevent thundering herd.
// It uses full-jitter: sleep = random_between(0, computed_delay).
type Jitter struct {
	Base Backoff
}

func (j Jitter) Next(attempt int) time.Duration {
	base := j.Base.Next(attempt)
	if base <= 0 {
		return 0
	}
	// rand.N is the new idiomatic way in math/rand/v2
	return rand.N(base)
}

// WithJitter wraps any Backoff with full-jitter.
func WithJitter(b Backoff) Backoff { return Jitter{Base: b} }
