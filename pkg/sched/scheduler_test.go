package sched_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miladhzz/gkit/pkg/sched"
)

func TestScheduler_Every_RunsRepeatedly(t *testing.T) {
	var count atomic.Int64

	s := sched.New(2)
	s.Every(20*time.Millisecond, "counter", func(_ context.Context) error {
		count.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	time.Sleep(90 * time.Millisecond)
	cancel()
	s.Stop()

	got := count.Load()
	if got < 3 {
		t.Errorf("expected >= 3 runs in 90ms at 20ms interval, got %d", got)
	}
}

func TestScheduler_After_RunsOnce(t *testing.T) {
	var count atomic.Int64

	s := sched.New(1)
	s.After(20*time.Millisecond, "once", func(_ context.Context) error {
		count.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)

	time.Sleep(80 * time.Millisecond)
	cancel()
	s.Stop()

	if count.Load() != 1 {
		t.Errorf("expected exactly 1 run for After job, got %d", count.Load())
	}
}

func TestScheduler_OnError_Called(t *testing.T) {
	errFake := errors.New("job error")
	var errCount atomic.Int64

	s := sched.New(1,
		sched.WithOnError(func(_ sched.Job, _ error) {
			errCount.Add(1)
		}),
	)
	s.Every(10*time.Millisecond, "failing", func(_ context.Context) error {
		return errFake
	})

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
	s.Stop()

	if errCount.Load() == 0 {
		t.Error("expected onError to be called at least once")
	}
}

func TestScheduler_StopCancelsJobs(t *testing.T) {
	var ran atomic.Int64
	s := sched.New(1)
	s.Every(5*time.Millisecond, "ticker", func(_ context.Context) error {
		ran.Add(1)
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Start(ctx)
	time.Sleep(30 * time.Millisecond)

	cancel() // triggers Stop
	s.Stop()

	snapshot := ran.Load()
	time.Sleep(30 * time.Millisecond)

	if ran.Load() != snapshot {
		t.Error("jobs ran after Stop()")
	}
}
