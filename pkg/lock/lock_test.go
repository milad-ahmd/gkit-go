package lock_test

import (
	"context"
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/milad-ahmd/gkit-go/pkg/lock"
)

func redisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis integration test")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { c.Close() })
	return c
}

func TestTryAcquire_Exclusive(t *testing.T) {
	ctx := context.Background()
	locker := lock.New(redisClient(t))

	lk, err := locker.TryAcquire(ctx, "test:exclusive", 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	defer lk.Release(ctx)

	_, err = locker.TryAcquire(ctx, "test:exclusive", 10*time.Second)
	if !errors.Is(err, lock.ErrNotAcquired) {
		t.Errorf("expected ErrNotAcquired, got %v", err)
	}
}

func TestRelease_AllowsReacquire(t *testing.T) {
	ctx := context.Background()
	locker := lock.New(redisClient(t))

	lk, err := locker.TryAcquire(ctx, "test:reacquire", 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if err := lk.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	lk2, err := locker.TryAcquire(ctx, "test:reacquire", 10*time.Second)
	if err != nil {
		t.Fatalf("second TryAcquire after release: %v", err)
	}
	defer lk2.Release(ctx)
}

func TestWithLock_MutualExclusion(t *testing.T) {
	ctx := context.Background()
	locker := lock.New(redisClient(t), lock.WithRetry(50, 20*time.Millisecond))

	const workers = 5
	var (
		concurrent int64
		maxSeen    int64
		wg         sync.WaitGroup
	)

	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = locker.WithLock(ctx, "test:mutex", 10*time.Second, func(_ context.Context) error {
				// Increment, sleep briefly, decrement — only one goroutine should be inside.
				n := atomic.AddInt64(&concurrent, 1)
				if n > atomic.LoadInt64(&maxSeen) {
					atomic.StoreInt64(&maxSeen, n)
				}
				time.Sleep(10 * time.Millisecond)
				atomic.AddInt64(&concurrent, -1)
				return nil
			})
		}()
	}
	wg.Wait()

	if maxSeen > 1 {
		t.Errorf("concurrent holders = %d, want ≤ 1", maxSeen)
	}
}

func TestWithLock_ContextCancelled(t *testing.T) {
	ctx := context.Background()
	locker := lock.New(redisClient(t))

	// Hold the lock.
	lk, _ := locker.TryAcquire(ctx, "test:cancel", 10*time.Second)
	defer lk.Release(ctx)

	// Attempt to acquire with a very short context — should fail.
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()

	_, err := locker.Acquire(short, "test:cancel", 10*time.Second)
	if err == nil {
		t.Fatal("expected error when context cancelled")
	}
}
