// Package circuitbreaker implements the Circuit Breaker pattern for protecting
// downstream dependencies from cascading failures.
//
// A Breaker has three states:
//
//   - Closed  — requests flow through normally; failures are counted.
//   - Open    — requests fail immediately without calling the underlying function.
//   - HalfOpen — after the open timeout, a limited number of probe requests are
//     allowed through to check whether the dependency has recovered.
//
// Usage:
//
//	cb := circuitbreaker.New(
//	    circuitbreaker.WithFailureThreshold(5),
//	    circuitbreaker.WithSuccessThreshold(2),
//	    circuitbreaker.WithOpenTimeout(30*time.Second),
//	    circuitbreaker.WithOnStateChange(func(from, to circuitbreaker.State) {
//	        slog.Info("circuit breaker state changed", "from", from, "to", to)
//	    }),
//	)
//
//	result, err := cb.Execute(ctx, func(ctx context.Context) (string, error) {
//	    return client.Call(ctx)
//	})
package circuitbreaker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// State represents the current state of a Breaker.
type State int

const (
	// StateClosed is the normal operating state. Requests flow through.
	StateClosed State = iota
	// StateOpen means the breaker has tripped. Requests fail immediately.
	StateOpen
	// StateHalfOpen is a probe state. A limited number of requests are allowed.
	StateHalfOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return fmt.Sprintf("unknown(%d)", int(s))
	}
}

// ErrOpen is returned when Execute is called while the breaker is open.
var ErrOpen = errors.New("circuitbreaker: circuit is open")

// options holds Breaker configuration.
type options struct {
	failureThreshold int
	successThreshold int
	openTimeout      time.Duration
	onStateChange    func(from, to State)
}

// Option configures a Breaker.
type Option func(*options)

// WithFailureThreshold sets the number of consecutive failures that trip
// the breaker from Closed → Open. Default: 5.
func WithFailureThreshold(n int) Option {
	return func(o *options) { o.failureThreshold = n }
}

// WithSuccessThreshold sets the number of consecutive successes in HalfOpen
// state required to close the breaker. Default: 1.
func WithSuccessThreshold(n int) Option {
	return func(o *options) { o.successThreshold = n }
}

// WithOpenTimeout sets how long the breaker stays open before entering HalfOpen.
// Default: 60s.
func WithOpenTimeout(d time.Duration) Option {
	return func(o *options) { o.openTimeout = d }
}

// WithOnStateChange registers a callback invoked on every state transition.
func WithOnStateChange(fn func(from, to State)) Option {
	return func(o *options) { o.onStateChange = fn }
}

// Breaker is a thread-safe circuit breaker.
type Breaker struct {
	mu       sync.Mutex
	state    State
	failures int // consecutive failures in Closed state
	probes   int // consecutive successes in HalfOpen state
	openedAt time.Time
	opts     options
}

// New creates a Breaker with sensible defaults.
func New(opts ...Option) *Breaker {
	o := options{
		failureThreshold: 5,
		successThreshold: 1,
		openTimeout:      60 * time.Second,
	}
	for _, opt := range opts {
		opt(&o)
	}
	return &Breaker{opts: o}
}

// Execute calls fn if the breaker permits it. It returns ErrOpen when the
// circuit is open. State transitions happen automatically based on the result.
func Execute[T any](ctx context.Context, b *Breaker, fn func(context.Context) (T, error)) (T, error) {
	if err := b.allow(); err != nil {
		var zero T
		return zero, err
	}
	result, err := fn(ctx)
	b.record(err)
	return result, err
}

// ExecuteVoid is like Execute for functions that return only an error.
func ExecuteVoid(ctx context.Context, b *Breaker, fn func(context.Context) error) error {
	_, err := Execute(ctx, b, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
	return err
}

// State returns the current breaker state without locking (approximate).
func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.maybeTransitionFromOpen()
	return b.state
}

// Reset manually resets the breaker to Closed state.
func (b *Breaker) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.transition(StateClosed)
	b.failures = 0
	b.probes = 0
}

// ---- internal -----------------------------------------------------------

func (b *Breaker) allow() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.maybeTransitionFromOpen()

	switch b.state {
	case StateClosed:
		return nil
	case StateHalfOpen:
		return nil // allow probe
	default:
		return fmt.Errorf("%w (will retry after %s)",
			ErrOpen, time.Until(b.openedAt.Add(b.opts.openTimeout)).Truncate(time.Millisecond))
	}
}

func (b *Breaker) record(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch b.state {
	case StateClosed:
		if err != nil {
			b.failures++
			if b.failures >= b.opts.failureThreshold {
				b.openedAt = time.Now()
				b.transition(StateOpen)
			}
		} else {
			b.failures = 0
		}

	case StateHalfOpen:
		if err != nil {
			b.failures = 0
			b.probes = 0
			b.openedAt = time.Now()
			b.transition(StateOpen)
		} else {
			b.probes++
			if b.probes >= b.opts.successThreshold {
				b.failures = 0
				b.probes = 0
				b.transition(StateClosed)
			}
		}
	}
}

// maybeTransitionFromOpen checks whether the open timeout has elapsed and
// transitions to HalfOpen if so. Must be called with b.mu held.
func (b *Breaker) maybeTransitionFromOpen() {
	if b.state == StateOpen && time.Since(b.openedAt) >= b.opts.openTimeout {
		b.transition(StateHalfOpen)
	}
}

func (b *Breaker) transition(to State) {
	if b.state == to {
		return
	}
	from := b.state
	b.state = to
	if b.opts.onStateChange != nil {
		go b.opts.onStateChange(from, to)
	}
}
