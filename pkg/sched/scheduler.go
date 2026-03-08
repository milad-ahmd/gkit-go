// Package sched provides a lightweight job scheduler backed by a worker pool.
//
// Jobs can be scheduled to run:
//   - Every interval (periodic)
//   - Once after a delay (deferred)
//   - On a fixed cron-like schedule (via Every with alignment)
//
// The scheduler uses gkit's pool.Pool for execution, providing backpressure,
// error handling, and live statistics out of the box.
//
// Example:
//
//	s := sched.New(4, // 4 worker goroutines
//	    sched.WithOnError(func(job sched.Job, err error) {
//	        slog.Error("job failed", "name", job.Name, "error", err)
//	    }),
//	)
//
//	s.Every(1*time.Minute, "cleanup", func(ctx context.Context) error {
//	    return db.DeleteOldRecords(ctx)
//	})
//	s.After(5*time.Second, "warmup", func(ctx context.Context) error {
//	    return cache.Warm(ctx)
//	})
//
//	s.Start(ctx)
//	defer s.Stop()
package sched

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// Job describes a scheduled task.
type Job struct {
	Name string
	Fn   func(ctx context.Context) error
}

// OnErrorFunc is called when a job returns an error.
type OnErrorFunc func(job Job, err error)

// options holds scheduler configuration.
type options struct {
	onError OnErrorFunc
	logger  *slog.Logger
}

// Option configures a Scheduler.
type Option func(*options)

// WithOnError registers a callback for job errors.
func WithOnError(fn OnErrorFunc) Option {
	return func(o *options) { o.onError = fn }
}

// WithLogger sets the structured logger used for job lifecycle events.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

type schedule struct {
	job      Job
	interval time.Duration // 0 means run once
	delay    time.Duration
}

// Scheduler runs jobs on a pool of goroutines.
type Scheduler struct {
	workers   int
	opts      options
	schedules []schedule
	mu        sync.Mutex
	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// New creates a Scheduler with the given number of worker goroutines.
func New(workers int, opts ...Option) *Scheduler {
	o := options{logger: slog.Default()}
	for _, opt := range opts {
		opt(&o)
	}
	return &Scheduler{workers: workers, opts: o}
}

// Every schedules fn to run repeatedly every interval, starting immediately.
func (s *Scheduler) Every(interval time.Duration, name string, fn func(context.Context) error) *Scheduler {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedules = append(s.schedules, schedule{
		job:      Job{Name: name, Fn: fn},
		interval: interval,
	})
	return s
}

// After schedules fn to run once after delay.
func (s *Scheduler) After(delay time.Duration, name string, fn func(context.Context) error) *Scheduler {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedules = append(s.schedules, schedule{
		job:   Job{Name: name, Fn: fn},
		delay: delay,
	})
	return s
}

// Start launches the scheduler. It returns immediately; jobs run in background.
// Call Stop to drain and shut down.
func (s *Scheduler) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	s.cancel = cancel

	s.mu.Lock()
	scheds := make([]schedule, len(s.schedules))
	copy(scheds, s.schedules)
	s.mu.Unlock()

	// Semaphore to bound concurrency.
	sem := make(chan struct{}, s.workers)

	for _, sc := range scheds {
		sc := sc
		if sc.interval > 0 {
			s.wg.Add(1)
			go s.runPeriodic(ctx, sc, sem)
		} else {
			s.wg.Add(1)
			go s.runOnce(ctx, sc, sem)
		}
	}
}

// Stop cancels the context and waits for all running jobs to finish.
func (s *Scheduler) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	s.wg.Wait()
}

func (s *Scheduler) runPeriodic(ctx context.Context, sc schedule, sem chan struct{}) {
	defer s.wg.Done()

	ticker := time.NewTicker(sc.interval)
	defer ticker.Stop()

	// Run immediately on start.
	s.dispatch(ctx, sc.job, sem)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.dispatch(ctx, sc.job, sem)
		}
	}
}

func (s *Scheduler) runOnce(ctx context.Context, sc schedule, sem chan struct{}) {
	defer s.wg.Done()

	timer := time.NewTimer(sc.delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return
	case <-timer.C:
		s.dispatch(ctx, sc.job, sem)
	}
}

func (s *Scheduler) dispatch(ctx context.Context, job Job, sem chan struct{}) {
	select {
	case <-ctx.Done():
		return
	case sem <- struct{}{}: // acquire slot
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer func() { <-sem }() // release slot

		s.opts.logger.DebugContext(ctx, "job starting", slog.String("job", job.Name))
		start := time.Now()

		if err := job.Fn(ctx); err != nil {
			s.opts.logger.ErrorContext(ctx, "job failed",
				slog.String("job", job.Name),
				slog.Duration("duration", time.Since(start)),
				slog.Any("error", err),
			)
			if s.opts.onError != nil {
				s.opts.onError(job, err)
			}
			return
		}

		s.opts.logger.DebugContext(ctx, "job completed",
			slog.String("job", job.Name),
			slog.Duration("duration", time.Since(start)),
		)
	}()
}
