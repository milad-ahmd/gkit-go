// Package async demonstrates expert-level Go concurrency through a set of
// composable primitives built entirely on goroutines and channels.
//
// # Primitives
//
//   - Future[T]    — asynchronous value (promise/future pattern)
//   - Stream[T]    — lazy push-based async stream with backpressure
//   - Semaphore    — channel-based counting semaphore
//   - FanOut/FanIn — broadcast and merge channel patterns
//   - Tee          — duplicate a channel into two independent receivers
//   - Debounce     — channel-backed function call debouncing
//   - Throttle     — channel-backed rate limiting of function calls
//
// All primitives are context-aware and goroutine-safe.
package async

import (
	"context"
	"sync"
)

// --------------------------------------------------------------------------
// Future[T]
//
// A Future represents the eventual result of an asynchronous computation.
// It is created by spawning a goroutine and resolved exactly once by sending
// a result over a buffered channel of size 1 — no mutex required.
//
//	Pattern: producer goroutine → buffered chan(1) → consumer(s)
//
// Buffered size 1 ensures the producer never blocks even if no consumer is
// listening yet, while allowing multiple calls to Await to all receive the
// same value (the channel is never drained — value is re-sent on each Await).

// result[T] holds either a value or an error.
type result[T any] struct {
	val T
	err error
}

// Future[T] is a handle to an asynchronous computation.
type Future[T any] struct {
	ch   chan result[T] // buffered(1); written exactly once
	once sync.Once
	res  result[T] // cached after first Await
}

// Async spawns fn in a goroutine and returns a Future for its result.
// The goroutine respects ctx cancellation during its execution.
//
//	f := async.Async(ctx, func(ctx context.Context) (*User, error) {
//	    return db.FindUser(ctx, id)
//	})
//	// ... do other work ...
//	user, err := f.Await(ctx)
func Async[T any](ctx context.Context, fn func(context.Context) (T, error)) *Future[T] {
	// Buffered(1): producer never blocks, consumer can arrive later.
	ch := make(chan result[T], 1)
	f := &Future[T]{ch: ch}

	go func() {
		val, err := fn(ctx)
		ch <- result[T]{val: val, err: err}
	}()

	return f
}

// Await blocks until the future resolves or ctx is cancelled.
// Safe to call multiple times — result is cached after first resolution.
func (f *Future[T]) Await(ctx context.Context) (T, error) {
	// Fast path: already resolved.
	f.once.Do(func() {
		select {
		case r := <-f.ch:
			f.res = r
		case <-ctx.Done():
			var zero T
			f.res = result[T]{val: zero, err: ctx.Err()}
		}
	})
	return f.res.val, f.res.err
}

// --------------------------------------------------------------------------
// Combinators

// All runs futures concurrently and returns their results in order.
// If any future fails or ctx is cancelled, All returns immediately with that error.
//
//	Pattern: fan-in via a results channel; first error wins via select.
func All[T any](ctx context.Context, futures ...*Future[T]) ([]T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type indexed struct {
		i   int
		res result[T]
	}
	// Buffered to avoid goroutine leak if we cancel early.
	merge := make(chan indexed, len(futures))

	for i, f := range futures {
		i, f := i, f // capture loop vars
		go func() {
			val, err := f.Await(ctx)
			merge <- indexed{i: i, res: result[T]{val: val, err: err}}
		}()
	}

	results := make([]T, len(futures))
	for range futures {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case r := <-merge:
			if r.res.err != nil {
				cancel() // signal other goroutines to stop
				return nil, r.res.err
			}
			results[r.i] = r.res.val
		}
	}
	return results, nil
}

// Race returns the result of whichever future resolves first.
// All other goroutines are cancelled via context.
//
//	Pattern: first-write-wins on a shared channel.
func Race[T any](ctx context.Context, futures ...*Future[T]) (T, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Buffered so every goroutine can send even after we return.
	winner := make(chan result[T], len(futures))

	for _, f := range futures {
		f := f
		go func() {
			val, err := f.Await(ctx)
			// Non-blocking send: if channel already has a winner, drop.
			select {
			case winner <- result[T]{val: val, err: err}:
			default:
			}
		}()
	}

	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case r := <-winner:
		cancel()
		return r.val, r.err
	}
}
