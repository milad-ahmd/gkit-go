// Package ratelimit provides a token-bucket rate limiter with per-key variants.
//
// The token bucket algorithm allows bursting up to the configured capacity,
// then refills tokens at a steady rate. It is suitable for per-user, per-IP,
// and per-endpoint rate limiting.
//
// Basic usage:
//
//	limiter := ratelimit.New(100, 10) // 100 req/s, burst of 10
//
//	if !limiter.Allow() {
//	    http.Error(w, "rate limited", http.StatusTooManyRequests)
//	    return
//	}
//
// Per-key usage (e.g., per-IP):
//
//	kl := ratelimit.NewKeyed[string](100, 10,
//	    ratelimit.WithKeyTTL[string](5*time.Minute),
//	)
//	if !kl.Allow(r.RemoteAddr) { ... }
package ratelimit

import (
	"context"
	"math"
	"sync"
	"time"
)

// Limiter is a token-bucket rate limiter.
type Limiter struct {
	mu       sync.Mutex
	rate     float64 // tokens per second
	burst    float64 // maximum bucket size
	tokens   float64 // current token count
	lastFill time.Time
}

// New creates a Limiter that allows rate events per second with a burst
// capacity of burst. burst must be >= 1.
func New(rate float64, burst int) *Limiter {
	return &Limiter{
		rate:     rate,
		burst:    float64(burst),
		tokens:   float64(burst),
		lastFill: time.Now(),
	}
}

// Allow reports whether one token is available and consumes it.
// It never blocks.
func (l *Limiter) Allow() bool {
	return l.AllowN(1)
}

// AllowN reports whether n tokens are available and consumes them.
func (l *Limiter) AllowN(n int) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	if l.tokens < float64(n) {
		return false
	}
	l.tokens -= float64(n)
	return true
}

// Wait blocks until one token is available or ctx is cancelled.
func (l *Limiter) Wait(ctx context.Context) error {
	return l.WaitN(ctx, 1)
}

// WaitN blocks until n tokens are available or ctx is cancelled.
func (l *Limiter) WaitN(ctx context.Context, n int) error {
	for {
		l.mu.Lock()
		l.refill()
		if l.tokens >= float64(n) {
			l.tokens -= float64(n)
			l.mu.Unlock()
			return nil
		}
		// Compute how long until n tokens are available.
		need := float64(n) - l.tokens
		waitDur := time.Duration((need / l.rate) * float64(time.Second))
		if waitDur < time.Millisecond {
			waitDur = time.Millisecond // minimum poll interval
		}
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(waitDur):
		}
	}
}

// Tokens returns the current number of available tokens (approximate).
func (l *Limiter) Tokens() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	return l.tokens
}

func (l *Limiter) refill() {
	now := time.Now()
	elapsed := now.Sub(l.lastFill).Seconds()
	l.tokens = math.Min(l.burst, l.tokens+elapsed*l.rate)
	l.lastFill = now
}
