package ratelimit_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miladhzz/gkit/pkg/ratelimit"
)

func TestLimiter_AllowsBurst(t *testing.T) {
	l := ratelimit.New(1, 5) // 1 req/s, burst 5
	for i := range 5 {
		if !l.Allow() {
			t.Fatalf("expected Allow on burst token %d", i)
		}
	}
	// 6th should be denied (burst exhausted, refill rate is slow).
	if l.Allow() {
		t.Fatal("expected deny after burst exhausted")
	}
}

func TestLimiter_RefillsOverTime(t *testing.T) {
	l := ratelimit.New(1000, 1) // 1000 req/s, burst 1
	l.Allow()                   // consume the only token
	time.Sleep(2 * time.Millisecond)
	if !l.Allow() {
		t.Error("expected token to refill after wait")
	}
}

func TestLimiter_Wait_Succeeds(t *testing.T) {
	l := ratelimit.New(1000, 1) // 1000 req/s — refill is fast
	l.Allow()                   // consume

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx); err != nil {
		t.Fatalf("Wait failed: %v", err)
	}
}

func TestLimiter_Wait_ContextCancelled(t *testing.T) {
	l := ratelimit.New(0.001, 1) // extremely slow refill
	l.Allow()                    // consume

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := l.Wait(ctx); err == nil {
		t.Fatal("expected context error")
	}
}

func TestLimiter_Concurrent(t *testing.T) {
	const (
		rate    = 10_000.0
		burst   = 100
		callers = 50
	)
	l := ratelimit.New(rate, burst)
	var allowed atomic.Int64

	done := make(chan struct{})
	for range callers {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					if l.Allow() {
						allowed.Add(1)
					}
				}
			}
		}()
	}
	time.Sleep(10 * time.Millisecond)
	close(done)

	// Rough sanity check: we should have allowed some but not unlimited.
	got := allowed.Load()
	if got == 0 {
		t.Error("expected some requests to be allowed")
	}
}

// ---- KeyedLimiter -------------------------------------------------------

func TestKeyedLimiter_PerKeyIsolation(t *testing.T) {
	kl := ratelimit.NewKeyed[string](10, 2)

	// "alice" can consume 2 tokens.
	if !kl.Allow("alice") || !kl.Allow("alice") {
		t.Fatal("expected alice's burst to be available")
	}
	if kl.Allow("alice") {
		t.Fatal("expected alice to be rate-limited after burst")
	}

	// "bob" still has his own fresh bucket.
	if !kl.Allow("bob") {
		t.Fatal("expected bob's bucket to be independent")
	}
}

func TestKeyedLimiter_Evict(t *testing.T) {
	kl := ratelimit.NewKeyed[string](10, 5,
		ratelimit.WithKeyTTL[string](10*time.Millisecond),
	)
	kl.Allow("a")
	kl.Allow("b")

	if kl.Len() != 2 {
		t.Fatalf("expected 2 keys, got %d", kl.Len())
	}

	time.Sleep(20 * time.Millisecond)
	evicted := kl.Evict()
	if evicted != 2 {
		t.Errorf("expected 2 evictions, got %d", evicted)
	}
	if kl.Len() != 0 {
		t.Errorf("expected 0 keys after eviction, got %d", kl.Len())
	}
}
