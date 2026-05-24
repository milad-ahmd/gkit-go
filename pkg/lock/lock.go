// Package lock provides a Redis-backed distributed lock with automatic
// lease renewal and context-aware acquisition.
//
// # Algorithm
//
// Uses the single-node Redis locking algorithm:
//  1. SET key token PX ttlMs NX   — atomic acquire
//  2. A background goroutine re-extends the TTL at ttl/3 intervals (keepalive).
//  3. Release uses a Lua script that deletes the key only when the stored token
//     matches — preventing a holder from releasing another holder's lock.
//
// For production multi-node Redis use Redlock; this implementation is correct
// for single Redis or Redis Sentinel with a primary.
//
// # Usage
//
//	locker := lock.New(redisClient)
//
//	// Acquire and auto-release.
//	if err := locker.WithLock(ctx, "billing:invoice:123", 30*time.Second, func(ctx context.Context) error {
//	    return processInvoice(ctx)
//	}); err != nil {
//	    if errors.Is(err, lock.ErrNotAcquired) { /* someone else holds it */ }
//	}
//
//	// Manual lifecycle.
//	l, err := locker.Acquire(ctx, "report:monthly", 30*time.Second)
//	defer l.Release(ctx)
package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// ErrNotAcquired is returned when the lock is already held by another holder.
var ErrNotAcquired = errors.New("lock: not acquired")

// releaseScript atomically deletes the key only if our token matches.
// This ensures we never release a lock we no longer own (e.g. after TTL expiry).
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("DEL", KEYS[1])
else
    return 0
end`)

// renewScript atomically extends the TTL only if our token matches.
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
    return redis.call("PEXPIRE", KEYS[1], ARGV[2])
else
    return 0
end`)

// Locker creates and manages distributed locks.
type Locker struct {
	client  *redis.Client
	retryOk bool
	retryN  int
	retryD  time.Duration
}

// New creates a Locker backed by the given Redis client.
func New(client *redis.Client, opts ...Option) *Locker {
	l := &Locker{client: client}
	for _, o := range opts {
		o(l)
	}
	return l
}

// Option configures a Locker.
type Option func(*Locker)

// WithRetry retries acquisition up to n times with interval d between attempts.
// Useful for turning a non-blocking TryAcquire into a bounded-wait Acquire.
func WithRetry(n int, interval time.Duration) Option {
	return func(l *Locker) {
		l.retryOk = true
		l.retryN = n
		l.retryD = interval
	}
}

// --------------------------------------------------------------------------
// Lock handle

// Lock represents an acquired distributed lock.
type Lock struct {
	client *redis.Client
	key    string
	token  string
	ttl    time.Duration
	stop   chan struct{} // closed to stop the keepalive goroutine
	done   chan struct{} // closed when keepalive goroutine exits
}

// Release releases the lock. Safe to call multiple times.
func (l *Lock) Release(ctx context.Context) error {
	// Signal keepalive to stop and wait for it to exit.
	select {
	case <-l.stop:
	default:
		close(l.stop)
	}
	<-l.done

	n, err := releaseScript.Run(ctx, l.client, []string{l.key}, l.token).Int()
	if err != nil {
		return fmt.Errorf("lock: release %q: %w", l.key, err)
	}
	if n == 0 {
		return fmt.Errorf("lock: release %q: token mismatch (lock expired?)", l.key)
	}
	return nil
}

// keepalive periodically renews the lock TTL to prevent expiry while the
// holder is still running. It runs in its own goroutine.
func (l *Lock) keepalive() {
	defer close(l.done)

	// Renew at 1/3 of the TTL to give three chances before expiry.
	ticker := time.NewTicker(l.ttl / 3)
	defer ticker.Stop()

	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ttlMs := l.ttl.Milliseconds()
			_ = renewScript.Run(ctx, l.client, []string{l.key}, l.token, ttlMs).Err()
			cancel()
		}
	}
}

// --------------------------------------------------------------------------
// Locker methods

// TryAcquire attempts a single non-blocking acquisition.
// Returns ErrNotAcquired if the lock is already held.
func (l *Locker) TryAcquire(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	token := uuid.New().String()
	err := l.client.SetArgs(ctx, key, token, redis.SetArgs{Mode: "NX", TTL: ttl}).Err()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrNotAcquired
		}
		return nil, fmt.Errorf("lock: acquire %q: %w", key, err)
	}
	lk := &Lock{
		client: l.client,
		key:    key,
		token:  token,
		ttl:    ttl,
		stop:   make(chan struct{}),
		done:   make(chan struct{}),
	}
	go lk.keepalive()
	return lk, nil
}

// Acquire blocks until the lock is acquired or ctx is cancelled.
// If WithRetry was configured, it retries at most n times; otherwise it retries
// indefinitely until ctx is done.
func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (*Lock, error) {
	attempt := 0
	for {
		lk, err := l.TryAcquire(ctx, key, ttl)
		if err == nil {
			return lk, nil
		}
		if !errors.Is(err, ErrNotAcquired) {
			return nil, err
		}

		attempt++
		if l.retryOk && attempt >= l.retryN {
			return nil, ErrNotAcquired
		}

		interval := 100 * time.Millisecond
		if l.retryOk {
			interval = l.retryD
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("lock: acquire %q: %w", key, ctx.Err())
		case <-time.After(interval):
		}
	}
}

// WithLock is a convenience wrapper that acquires, runs fn, and releases.
//
//	err := locker.WithLock(ctx, "payments:settle", 30*time.Second, func(ctx context.Context) error {
//	    return settle(ctx)
//	})
func (l *Locker) WithLock(ctx context.Context, key string, ttl time.Duration, fn func(context.Context) error) error {
	lk, err := l.Acquire(ctx, key, ttl)
	if err != nil {
		return err
	}
	defer func() { _ = lk.Release(ctx) }()
	return fn(ctx)
}
