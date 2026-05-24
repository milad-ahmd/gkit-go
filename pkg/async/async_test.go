package async_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/async"
)

// --------------------------------------------------------------------------
// Future

func TestAsync_ReturnsValue(t *testing.T) {
	f := async.Async(context.Background(), func(_ context.Context) (int, error) {
		return 42, nil
	})
	v, err := f.Await(context.Background())
	if err != nil || v != 42 {
		t.Errorf("Await = (%d, %v), want (42, nil)", v, err)
	}
}

func TestAsync_ReturnsError(t *testing.T) {
	want := errors.New("boom")
	f := async.Async(context.Background(), func(_ context.Context) (int, error) {
		return 0, want
	})
	_, err := f.Await(context.Background())
	if !errors.Is(err, want) {
		t.Errorf("err = %v, want %v", err, want)
	}
}

func TestAll_AllSucceed(t *testing.T) {
	ctx := context.Background()
	futures := make([]*async.Future[int], 5)
	for i := range futures {
		i := i
		futures[i] = async.Async(ctx, func(_ context.Context) (int, error) { return i, nil })
	}
	got, err := async.All(ctx, futures...)
	if err != nil {
		t.Fatalf("All: %v", err)
	}
	sum := 0
	for _, v := range got {
		sum += v
	}
	if sum != 10 {
		t.Errorf("sum = %d, want 10", sum)
	}
}

func TestAll_OneError(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	futures := []*async.Future[int]{
		async.Async(ctx, func(_ context.Context) (int, error) { return 1, nil }),
		async.Async(ctx, func(_ context.Context) (int, error) { return 0, boom }),
		async.Async(ctx, func(_ context.Context) (int, error) { return 3, nil }),
	}
	_, err := async.All(ctx, futures...)
	if !errors.Is(err, boom) {
		t.Errorf("err = %v, want boom", err)
	}
}

func TestRace_FirstWins(t *testing.T) {
	ctx := context.Background()
	fast := async.Async(ctx, func(_ context.Context) (string, error) {
		return "fast", nil
	})
	slow := async.Async(ctx, func(_ context.Context) (string, error) {
		time.Sleep(100 * time.Millisecond)
		return "slow", nil
	})
	v, err := async.Race(ctx, fast, slow)
	if err != nil || v != "fast" {
		t.Errorf("Race = (%q, %v), want (fast, nil)", v, err)
	}
}

// --------------------------------------------------------------------------
// Stream

func TestGenerate_Collect(t *testing.T) {
	ctx := context.Background()
	s := async.Generate(ctx, func(_ context.Context, send func(int) bool) {
		for i := range 5 {
			if !send(i) {
				return
			}
		}
	})
	got, err := s.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(got) != 5 {
		t.Errorf("got %v, want [0 1 2 3 4]", got)
	}
}

func TestFromSlice(t *testing.T) {
	ctx := context.Background()
	s := async.FromSlice(ctx, []string{"a", "b", "c"})
	got, err := s.Collect(ctx)
	if err != nil || len(got) != 3 || got[1] != "b" {
		t.Errorf("got = %v err = %v", got, err)
	}
}

func TestMap(t *testing.T) {
	ctx := context.Background()
	s := async.FromSlice(ctx, []int{1, 2, 3})
	doubled := async.Map(s, func(v int) int { return v * 2 })
	got, _ := doubled.Collect(ctx)
	if len(got) != 3 || got[2] != 6 {
		t.Errorf("got = %v", got)
	}
}

func TestFilter(t *testing.T) {
	ctx := context.Background()
	s := async.FromSlice(ctx, []int{1, 2, 3, 4, 5})
	evens := async.Filter(s, func(v int) bool { return v%2 == 0 })
	got, _ := evens.Collect(ctx)
	if len(got) != 2 || got[0] != 2 || got[1] != 4 {
		t.Errorf("got = %v, want [2 4]", got)
	}
}

func TestBatch(t *testing.T) {
	ctx := context.Background()
	s := async.FromSlice(ctx, []int{1, 2, 3, 4, 5})
	batched := async.Batch(s, 2, 100*time.Millisecond)
	got, err := batched.Collect(ctx)
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	total := 0
	for _, b := range got {
		total += len(b)
	}
	if total != 5 {
		t.Errorf("total items in batches = %d, want 5", total)
	}
}

func TestTake(t *testing.T) {
	ctx := context.Background()
	s := async.FromSlice(ctx, []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
	got, _ := async.Take(s, 3).Collect(ctx)
	if len(got) != 3 {
		t.Errorf("Take(3) got %d items", len(got))
	}
}

// --------------------------------------------------------------------------
// Channel patterns

func TestFanOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	in := make(chan int, 5)
	for i := range 5 {
		in <- i
	}
	close(in)

	outs := async.FanOut(ctx, in, 3)
	for i, out := range outs {
		var items []int
		for v := range out {
			items = append(items, v)
		}
		if len(items) != 5 {
			t.Errorf("output %d: got %d items, want 5", i, len(items))
		}
	}
}

func TestFanIn(t *testing.T) {
	ctx := context.Background()
	a := make(chan int, 3)
	b := make(chan int, 3)
	a <- 1
	a <- 2
	a <- 3
	close(a)
	b <- 4
	b <- 5
	close(b)

	out := async.FanIn(ctx, a, b)
	var items []int
	for v := range out {
		items = append(items, v)
	}
	if len(items) != 5 {
		t.Errorf("FanIn got %d items, want 5", len(items))
	}
}

func TestTee_BothReceive(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	in := make(chan int, 3)
	in <- 1
	in <- 2
	in <- 3
	close(in)

	a, b := async.Tee(ctx, in)
	var as, bs []int
	for v := range a {
		as = append(as, v)
	}
	for v := range b {
		bs = append(bs, v)
	}
	if len(as) != 3 || len(bs) != 3 {
		t.Errorf("Tee: a=%v b=%v, want 3 each", as, bs)
	}
}

func TestOrDone_CtxCancel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	in := make(chan int) // never sends
	out := async.OrDone(ctx, in)

	select {
	case _, ok := <-out:
		if ok {
			t.Error("expected channel to be closed")
		}
	case <-time.After(200 * time.Millisecond):
		t.Error("OrDone did not close after ctx cancel")
	}
}

// --------------------------------------------------------------------------
// Semaphore

func TestSemaphore_Limits(t *testing.T) {
	ctx := context.Background()
	sem := async.NewSemaphore(2)

	if err := sem.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sem.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if sem.TryAcquire() {
		t.Error("TryAcquire should fail when semaphore is exhausted")
	}
	sem.Release()
	if !sem.TryAcquire() {
		t.Error("TryAcquire should succeed after Release")
	}
	sem.Release()
	sem.Release()
}

func TestSemaphore_ContextCancel(t *testing.T) {
	sem := async.NewSemaphore(1)
	_ = sem.Acquire(context.Background()) // fill it up

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := sem.Acquire(ctx)
	if err == nil {
		t.Error("expected context error")
	}
}

func TestSemaphore_BoundsConcurrency(t *testing.T) {
	const limit = 3
	sem := async.NewSemaphore(limit)

	var concurrent atomic.Int32
	var maxSeen atomic.Int32
	var wg sync.WaitGroup

	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = sem.Acquire(context.Background())
			defer sem.Release()

			n := concurrent.Add(1)
			if n > maxSeen.Load() {
				maxSeen.Store(n)
			}
			time.Sleep(5 * time.Millisecond)
			concurrent.Add(-1)
		}()
	}
	wg.Wait()

	if maxSeen.Load() > limit {
		t.Errorf("max concurrent = %d, want ≤ %d", maxSeen.Load(), limit)
	}
}

// --------------------------------------------------------------------------
// Debounce

func TestDebounce_FiredOnce(t *testing.T) {
	calls := atomic.Int32{}
	fn := async.Debounce(60*time.Millisecond, func() { calls.Add(1) })

	for range 10 {
		fn()
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	if n := calls.Load(); n != 1 {
		t.Errorf("fn called %d times, want 1", n)
	}
}

// --------------------------------------------------------------------------
// Throttle

func TestThrottle_AtMostOncePerWindow(t *testing.T) {
	calls := atomic.Int32{}
	fn := async.Throttle(100*time.Millisecond, func() { calls.Add(1) })

	for range 5 {
		fn()
	}
	if n := calls.Load(); n > 1 {
		t.Errorf("fn called %d times in one window, want ≤ 1", n)
	}
}

// --------------------------------------------------------------------------
// Barrier

func TestBarrier_AllReleasedTogether(t *testing.T) {
	const n = 5
	b := async.NewBarrier(n)

	var released atomic.Int32
	var wg sync.WaitGroup

	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(time.Duration(time.Now().UnixNano()%50) * time.Millisecond)
			_ = b.Wait(context.Background())
			released.Add(1)
		}()
	}
	wg.Wait()

	if released.Load() != n {
		t.Errorf("released %d goroutines, want %d", released.Load(), n)
	}
}
