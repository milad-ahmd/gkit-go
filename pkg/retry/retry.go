// Package retry provides generic retry utilities with configurable backoff strategies.
//
// It supports context cancellation, typed results, and hook callbacks — making it
// suitable for database calls, HTTP requests, and any other fallible operations.
//
// Basic usage:
//
//	result, err := retry.Do(ctx, func(ctx context.Context) (string, error) {
//	    return fetchData(ctx)
//	}, retry.WithMaxAttempts(5), retry.WithBackoff(retry.DefaultExponential))
package retry

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrMaxAttempts is returned when all retry attempts are exhausted.
type ErrMaxAttempts struct {
	Attempts int
	Last     error
}

func (e *ErrMaxAttempts) Error() string {
	return fmt.Sprintf("retry: exhausted after %d attempts: %v", e.Attempts, e.Last)
}

func (e *ErrMaxAttempts) Unwrap() error { return e.Last }

// IsMaxAttempts reports whether err is an ErrMaxAttempts.
func IsMaxAttempts(err error) bool {
	var target *ErrMaxAttempts
	return errors.As(err, &target)
}

// StopError wraps an error to signal that no further retries should occur.
// Use Stop to wrap a non-retryable error inside the retry function.
type StopError struct{ Err error }

func (e *StopError) Error() string { return e.Err.Error() }
func (e *StopError) Unwrap() error { return e.Err }

// Stop wraps err so that the retry loop exits immediately without further attempts.
func Stop(err error) error { return &StopError{Err: err} }

// OnRetryFunc is called before each retry attempt (not the first attempt).
// attempt is the 1-based number of the completed attempt that failed.
type OnRetryFunc func(ctx context.Context, attempt int, err error)

// options holds all retry configuration.
type options struct {
	maxAttempts int
	backoff     Backoff
	onRetry     OnRetryFunc
	sleep       func(context.Context, time.Duration) error
}

func defaultOptions() *options {
	return &options{
		maxAttempts: 3,
		backoff:     ConstantBackoff{Delay: 0},
		sleep:       contextSleep,
	}
}

// Option configures retry behaviour.
type Option func(*options)

// WithMaxAttempts sets the maximum number of attempts (including the first).
// Panics if n < 1.
func WithMaxAttempts(n int) Option {
	if n < 1 {
		panic("retry: MaxAttempts must be >= 1")
	}
	return func(o *options) { o.maxAttempts = n }
}

// WithBackoff sets the Backoff strategy used to compute inter-attempt delays.
func WithBackoff(b Backoff) Option {
	return func(o *options) { o.backoff = b }
}

// WithOnRetry registers a callback invoked before each retry attempt.
func WithOnRetry(fn OnRetryFunc) Option {
	return func(o *options) { o.onRetry = fn }
}

// Do calls fn up to the configured number of times, waiting between attempts
// according to the configured Backoff. It returns the first successful result
// or the last error wrapped in ErrMaxAttempts.
//
// fn may return Stop(err) to abort the retry loop immediately.
// The context is propagated to both fn and the sleep between attempts.
func Do[T any](ctx context.Context, fn func(context.Context) (T, error), opts ...Option) (T, error) {
	cfg := defaultOptions()
	for _, o := range opts {
		o(cfg)
	}

	var (
		zero T
		last error
	)

	for attempt := range cfg.maxAttempts {
		if ctx.Err() != nil {
			return zero, ctx.Err()
		}

		result, err := fn(ctx)
		if err == nil {
			return result, nil
		}

		// Unwrap StopError — user signals no more retries.
		var stop *StopError
		if errors.As(err, &stop) {
			return zero, stop.Err
		}

		last = err

		if attempt < cfg.maxAttempts-1 {
			if cfg.onRetry != nil {
				cfg.onRetry(ctx, attempt+1, err)
			}
			delay := cfg.backoff.Next(attempt)
			if delay > 0 {
				if sleepErr := cfg.sleep(ctx, delay); sleepErr != nil {
					return zero, sleepErr
				}
			}
		}
	}

	return zero, &ErrMaxAttempts{Attempts: cfg.maxAttempts, Last: last}
}

// DoVoid is like Do but for functions that return only an error.
func DoVoid(ctx context.Context, fn func(context.Context) error, opts ...Option) error {
	_, err := Do(ctx, func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	}, opts...)
	return err
}

// contextSleep sleeps for d, but returns ctx.Err() if the context is cancelled first.
func contextSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
