// Package pool provides a generic, bounded worker pool with backpressure.
//
// Jobs are submitted via Submit and processed concurrently by a fixed number
// of goroutines. The pool supports graceful shutdown, per-job error callbacks,
// and live statistics.
//
// Example:
//
//	p := pool.New[string](4, func(ctx context.Context, job string) error {
//	    fmt.Println("processing:", job)
//	    return nil
//	})
//	p.Start(ctx)
//	defer p.Stop()
//
//	p.Submit(ctx, "hello")
package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// ErrPoolStopped is returned by Submit when the pool is no longer accepting jobs.
var ErrPoolStopped = errors.New("pool: pool is stopped")

// ErrQueueFull is returned by TrySubmit when the job queue is full.
var ErrQueueFull = errors.New("pool: queue is full")

// WorkerFunc is the function executed for each job.
type WorkerFunc[T any] func(ctx context.Context, job T) error

// OnErrorFunc is called when a worker returns a non-nil error.
type OnErrorFunc[T any] func(ctx context.Context, job T, err error)

// Stats contains live pool statistics.
type Stats struct {
	Submitted  uint64
	Completed  uint64
	Errors     uint64
	InFlight   int64
	QueueDepth int
}

// options holds pool configuration.
type options[T any] struct {
	queueSize int
	onError   OnErrorFunc[T]
}

// Option configures a Pool.
type Option[T any] func(*options[T])

// WithQueueSize sets the capacity of the internal job queue.
// Defaults to the number of workers.
func WithQueueSize[T any](n int) Option[T] {
	return func(o *options[T]) { o.queueSize = n }
}

// WithOnError registers a callback invoked when a worker returns an error.
func WithOnError[T any](fn OnErrorFunc[T]) Option[T] {
	return func(o *options[T]) { o.onError = fn }
}

// Pool is a bounded, generic worker pool.
//
// The zero value is not usable; create one with New.
type Pool[T any] struct {
	workers int
	fn      WorkerFunc[T]
	opts    options[T]

	jobs chan T
	wg   sync.WaitGroup

	stopped atomic.Bool

	submitted atomic.Uint64
	completed atomic.Uint64
	errors    atomic.Uint64
	inFlight  atomic.Int64
}

// New creates a new Pool with the given number of workers and worker function.
// Call Start to begin processing jobs.
func New[T any](workers int, fn WorkerFunc[T], opts ...Option[T]) *Pool[T] {
	if workers < 1 {
		panic("pool: workers must be >= 1")
	}

	cfg := options[T]{queueSize: workers}
	for _, o := range opts {
		o(&cfg)
	}

	return &Pool[T]{
		workers: workers,
		fn:      fn,
		opts:    cfg,
		jobs:    make(chan T, cfg.queueSize),
	}
}

// Start launches the worker goroutines. It is safe to call Start once.
// The context is propagated to each worker invocation.
func (p *Pool[T]) Start(ctx context.Context) {
	for range p.workers {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.run(ctx)
		}()
	}
}

// Stop drains the queue, waits for all in-flight jobs to finish,
// and shuts down the pool. After Stop returns, no more jobs are accepted.
func (p *Pool[T]) Stop() {
	p.stopped.Store(true)
	close(p.jobs)
	p.wg.Wait()
}

// Submit enqueues a job, blocking until the queue has capacity or the context
// is cancelled. Returns ErrPoolStopped if the pool has been stopped.
func (p *Pool[T]) Submit(ctx context.Context, job T) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.jobs <- job:
		p.submitted.Add(1)
		return nil
	}
}

// TrySubmit enqueues a job without blocking.
// Returns ErrQueueFull if the queue is at capacity, ErrPoolStopped if stopped.
func (p *Pool[T]) TrySubmit(job T) error {
	if p.stopped.Load() {
		return ErrPoolStopped
	}
	select {
	case p.jobs <- job:
		p.submitted.Add(1)
		return nil
	default:
		return ErrQueueFull
	}
}

// Stats returns a snapshot of current pool statistics.
func (p *Pool[T]) Stats() Stats {
	return Stats{
		Submitted:  p.submitted.Load(),
		Completed:  p.completed.Load(),
		Errors:     p.errors.Load(),
		InFlight:   p.inFlight.Load(),
		QueueDepth: len(p.jobs),
	}
}

func (p *Pool[T]) run(ctx context.Context) {
	for job := range p.jobs {
		p.inFlight.Add(1)
		err := p.fn(ctx, job)
		p.inFlight.Add(-1)
		p.completed.Add(1)

		if err != nil {
			p.errors.Add(1)
			if p.opts.onError != nil {
				p.opts.onError(ctx, job, err)
			}
		}
	}
}
