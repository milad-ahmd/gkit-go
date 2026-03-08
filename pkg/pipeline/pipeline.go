// Package pipeline provides generic concurrent data-processing primitives.
//
// Three patterns are provided:
//
//  1. Process — fan-out: transform a slice of items concurrently.
//  2. Chain   — sequential stages that each transform the same type.
//  3. Compose — two typed stages combined into a single function (A→B→C).
//
// All functions propagate context cancellation and collect errors with
// short-circuit (first error wins) or drain-all behaviour.
//
// Example — concurrent image resize:
//
//	resized, err := pipeline.Process(ctx, imageURLs,
//	    func(ctx context.Context, url string) (Image, error) {
//	        return downloadAndResize(ctx, url)
//	    },
//	    pipeline.WithWorkers(8),
//	    pipeline.WithOrdered(true),
//	)
//
// Example — sequential enrichment chain:
//
//	enrich := pipeline.Chain(
//	    parseUser,
//	    validateUser,
//	    enrichFromDB,
//	)
//	user, err := enrich(ctx, rawInput)
package pipeline

import (
	"context"
	"sync"
)

// StageFunc is a function that transforms a value of type T.
type StageFunc[T any] func(ctx context.Context, in T) (T, error)

// MapFunc transforms a value from type In to Out.
type MapFunc[In, Out any] func(ctx context.Context, in In) (Out, error)

// ---- options -------------------------------------------------------------

type processOptions struct {
	workers int
	ordered bool
}

// Option configures Process behaviour.
type Option func(*processOptions)

// WithWorkers sets the number of concurrent goroutines. Default: GOMAXPROCS.
func WithWorkers(n int) Option {
	return func(o *processOptions) {
		if n > 0 {
			o.workers = n
		}
	}
}

// WithOrdered preserves the input order in the output slice. When false
// (default) results are returned as they complete, which is faster.
func WithOrdered(ordered bool) Option {
	return func(o *processOptions) { o.ordered = ordered }
}

// ---- Process -------------------------------------------------------------

// Process applies fn to each item in items concurrently and returns the
// results. The first non-nil error cancels remaining work and is returned.
//
// When WithOrdered(true) is set, output[i] corresponds to items[i].
// Otherwise outputs are returned in completion order.
func Process[In, Out any](
	ctx context.Context,
	items []In,
	fn MapFunc[In, Out],
	opts ...Option,
) ([]Out, error) {
	if len(items) == 0 {
		return nil, nil
	}

	o := &processOptions{workers: len(items), ordered: false}
	for _, opt := range opts {
		opt(o)
	}
	if o.workers > len(items) {
		o.workers = len(items)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type result struct {
		idx int
		val Out
		err error
	}

	type work struct {
		idx int
		val In
	}

	jobs := make(chan work, len(items))
	results := make(chan result, len(items))

	// Launch workers.
	var wg sync.WaitGroup
	for range o.workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range jobs {
				if ctx.Err() != nil {
					results <- result{idx: job.idx, err: ctx.Err()}
					continue
				}
				val, err := fn(ctx, job.val)
				results <- result{idx: job.idx, val: val, err: err}
			}
		}()
	}

	// Send work.
	for i, item := range items {
		jobs <- work{idx: i, val: item}
	}
	close(jobs)

	// Close results when all workers finish.
	go func() { wg.Wait(); close(results) }()

	// Collect results.
	out := make([]Out, len(items))
	unordered := make([]Out, 0, len(items))

	var firstErr error
	for r := range results {
		if r.err != nil && firstErr == nil {
			firstErr = r.err
			cancel()
		}
		if r.err == nil {
			if o.ordered {
				out[r.idx] = r.val
			} else {
				unordered = append(unordered, r.val)
			}
		}
	}

	if firstErr != nil {
		return nil, firstErr
	}
	if o.ordered {
		return out, nil
	}
	return unordered, nil
}

// ---- Chain ---------------------------------------------------------------

// Chain composes multiple same-type stages into a single function.
// Stages are applied in order; the output of stage n is the input of stage n+1.
// The first error short-circuits the chain.
//
//	pipeline := pipeline.Chain(parse, validate, enrich)
//	result, err := pipeline(ctx, input)
func Chain[T any](stages ...StageFunc[T]) StageFunc[T] {
	return func(ctx context.Context, in T) (T, error) {
		var err error
		for _, stage := range stages {
			if ctx.Err() != nil {
				var zero T
				return zero, ctx.Err()
			}
			in, err = stage(ctx, in)
			if err != nil {
				var zero T
				return zero, err
			}
		}
		return in, nil
	}
}

// ---- Compose -------------------------------------------------------------

// Compose combines two typed functions (A→B) and (B→C) into a single (A→C).
// This is the typed two-stage composition that Go generics allow at the
// function level.
//
//	fetch  := func(ctx context.Context, id string) (*RawData, error) { ... }
//	parse  := func(ctx context.Context, raw *RawData) (*Model, error) { ... }
//	result := pipeline.Compose(fetch, parse)
func Compose[A, B, C any](first MapFunc[A, B], second MapFunc[B, C]) MapFunc[A, C] {
	return func(ctx context.Context, in A) (C, error) {
		b, err := first(ctx, in)
		if err != nil {
			var zero C
			return zero, err
		}
		return second(ctx, b)
	}
}

// Compose3 combines three typed functions (A→B), (B→C), (C→D) into (A→D).
func Compose3[A, B, C, D any](f1 MapFunc[A, B], f2 MapFunc[B, C], f3 MapFunc[C, D]) MapFunc[A, D] {
	return Compose(Compose(f1, f2), f3)
}

// ---- Map (alias for single-item transform with retries, etc.) ------------

// Map is a convenience alias: it applies fn to a single value.
// Useful for chaining with Compose.
func Map[In, Out any](ctx context.Context, in In, fn MapFunc[In, Out]) (Out, error) {
	return fn(ctx, in)
}
