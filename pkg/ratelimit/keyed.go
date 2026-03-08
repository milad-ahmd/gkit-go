package ratelimit

import (
	"sync"
	"time"
)

// KeyedLimiter maintains a separate token-bucket Limiter per key.
// It is safe for concurrent use. Idle limiters are evicted after a
// configurable TTL to prevent unbounded memory growth.
type KeyedLimiter[K comparable] struct {
	mu      sync.Mutex
	rate    float64
	burst   int
	ttl     time.Duration
	entries map[K]*keyedEntry
}

type keyedEntry struct {
	limiter  *Limiter
	lastSeen time.Time
}

// KeyedOption configures a KeyedLimiter.
type KeyedOption[K comparable] func(*KeyedLimiter[K])

// WithKeyTTL sets how long an idle per-key limiter is retained.
// Default: 10 minutes.
func WithKeyTTL[K comparable](d time.Duration) KeyedOption[K] {
	return func(kl *KeyedLimiter[K]) { kl.ttl = d }
}

// NewKeyed creates a per-key rate limiter.
// Each key gets its own token bucket with the given rate (events/s) and burst.
func NewKeyed[K comparable](rate float64, burst int, opts ...KeyedOption[K]) *KeyedLimiter[K] {
	kl := &KeyedLimiter[K]{
		rate:    rate,
		burst:   burst,
		ttl:     10 * time.Minute,
		entries: make(map[K]*keyedEntry),
	}
	for _, o := range opts {
		o(kl)
	}
	return kl
}

// Allow reports whether key is within its rate limit.
func (kl *KeyedLimiter[K]) Allow(key K) bool {
	return kl.limiterFor(key).Allow()
}

// AllowN reports whether n tokens are available for key.
func (kl *KeyedLimiter[K]) AllowN(key K, n int) bool {
	return kl.limiterFor(key).AllowN(n)
}

// Delete removes the per-key limiter for key.
func (kl *KeyedLimiter[K]) Delete(key K) {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	delete(kl.entries, key)
}

// Evict removes all limiters that have been idle for longer than the TTL.
// Call this periodically (e.g., from a ticker goroutine) to reclaim memory.
func (kl *KeyedLimiter[K]) Evict() int {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	cutoff := time.Now().Add(-kl.ttl)
	n := 0
	for k, e := range kl.entries {
		if e.lastSeen.Before(cutoff) {
			delete(kl.entries, k)
			n++
		}
	}
	return n
}

// Len returns the number of active per-key limiters.
func (kl *KeyedLimiter[K]) Len() int {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	return len(kl.entries)
}

func (kl *KeyedLimiter[K]) limiterFor(key K) *Limiter {
	kl.mu.Lock()
	defer kl.mu.Unlock()
	e, ok := kl.entries[key]
	if !ok {
		e = &keyedEntry{limiter: New(kl.rate, kl.burst)}
		kl.entries[key] = e
	}
	e.lastSeen = time.Now()
	return e.limiter
}
