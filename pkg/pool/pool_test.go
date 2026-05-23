package pool_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/pool"
)

func TestPool_ProcessesAllJobs(t *testing.T) {
	const numJobs = 100
	var processed atomic.Int64

	p := pool.New[int](4, func(_ context.Context, _ int) error {
		processed.Add(1)
		return nil
	}, pool.WithQueueSize[int](numJobs))

	ctx := context.Background()
	p.Start(ctx)

	for i := range numJobs {
		if err := p.Submit(ctx, i); err != nil {
			t.Fatalf("Submit(%d): %v", i, err)
		}
	}

	p.Stop()

	if got := processed.Load(); got != numJobs {
		t.Fatalf("processed %d jobs, want %d", got, numJobs)
	}
}

func TestPool_Stats(t *testing.T) {
	const numJobs = 50
	var errCount atomic.Int64
	errFake := errors.New("fake")

	p := pool.New[int](2, func(_ context.Context, job int) error {
		if job%2 == 0 {
			return errFake
		}
		return nil
	},
		pool.WithQueueSize[int](numJobs),
		pool.WithOnError[int](func(_ context.Context, _ int, _ error) {
			errCount.Add(1)
		}),
	)

	ctx := context.Background()
	p.Start(ctx)

	for i := range numJobs {
		p.Submit(ctx, i) //nolint:errcheck
	}

	p.Stop()

	stats := p.Stats()
	if stats.Submitted != numJobs {
		t.Errorf("Submitted: got %d, want %d", stats.Submitted, numJobs)
	}
	if stats.Completed != numJobs {
		t.Errorf("Completed: got %d, want %d", stats.Completed, numJobs)
	}
	wantErrors := uint64(numJobs / 2)
	if stats.Errors != wantErrors {
		t.Errorf("Errors: got %d, want %d", stats.Errors, wantErrors)
	}
}

func TestPool_SubmitAfterStop(t *testing.T) {
	p := pool.New[int](1, func(_ context.Context, _ int) error { return nil })
	p.Start(context.Background())
	p.Stop()

	err := p.Submit(context.Background(), 1)
	if !errors.Is(err, pool.ErrPoolStopped) {
		t.Fatalf("expected ErrPoolStopped, got %v", err)
	}
}

func TestPool_TrySubmit_QueueFull(t *testing.T) {
	// Use a channel to hold the worker deterministically.
	hold := make(chan struct{})
	p := pool.New[int](1, func(_ context.Context, _ int) error {
		<-hold
		return nil
	}, pool.WithQueueSize[int](1))

	p.Start(context.Background())
	defer func() {
		close(hold)
		p.Stop()
	}()

	// Job 0: worker dequeues it and parks on <-hold.
	_ = p.TrySubmit(0)
	time.Sleep(20 * time.Millisecond) // let worker dequeue and block

	// Job 1: fills the queue buffer.
	_ = p.TrySubmit(1)

	// Queue is now full — next TrySubmit must return ErrQueueFull.
	err := p.TrySubmit(2)
	if !errors.Is(err, pool.ErrQueueFull) {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
}

func TestPool_ContextCancellation(t *testing.T) {
	// Hold the worker to guarantee the queue stays full throughout the test.
	hold := make(chan struct{})

	p := pool.New[int](1, func(_ context.Context, _ int) error {
		<-hold
		return nil
	}, pool.WithQueueSize[int](1))

	p.Start(context.Background())
	defer func() {
		close(hold)
		p.Stop()
	}()

	// Job 0: worker dequeues and blocks on hold.
	if err := p.TrySubmit(0); err != nil {
		t.Fatal("TrySubmit(0):", err)
	}
	time.Sleep(20 * time.Millisecond) // let worker dequeue and block

	// Job 1: fills the queue buffer (capacity 1).
	if err := p.TrySubmit(1); err != nil {
		t.Fatal("TrySubmit(1):", err)
	}

	// Worker busy + queue full → Submit must respect context deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := p.Submit(ctx, 2)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}
}

func BenchmarkPool_Throughput(b *testing.B) {
	p := pool.New[int](8, func(_ context.Context, _ int) error { return nil },
		pool.WithQueueSize[int](b.N+8))
	ctx := context.Background()
	p.Start(ctx)
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		p.Submit(ctx, i) //nolint:errcheck
	}
	p.Stop()
}
