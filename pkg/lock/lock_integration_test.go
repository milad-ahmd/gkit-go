//go:build integration

package lock_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/miladhzz/gkit/pkg/lock"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func startRedis(t *testing.T) *redis.Client {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "redis:7-alpine",
			ExposedPorts: []string{"6379/tcp"},
			WaitingFor:   wait.ForListeningPort("6379/tcp"),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start redis: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "6379/tcp")
	return redis.NewClient(&redis.Options{Addr: fmt.Sprintf("%s:%s", host, port.Port())})
}

func TestLock_Integration_AcquireAndRelease(t *testing.T) {
	client := startRedis(t)
	locker := lock.New(client)
	ctx := context.Background()

	lk, err := locker.TryAcquire(ctx, "test:resource", 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if err := lk.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}
}

func TestLock_Integration_ExclusiveAccess(t *testing.T) {
	client := startRedis(t)
	locker := lock.New(client)
	ctx := context.Background()

	lk1, err := locker.TryAcquire(ctx, "test:exclusive", 10*time.Second)
	if err != nil {
		t.Fatalf("first TryAcquire: %v", err)
	}
	defer lk1.Release(ctx)

	// Second acquire on the same key must fail.
	_, err = locker.TryAcquire(ctx, "test:exclusive", 10*time.Second)
	if !errors.Is(err, lock.ErrNotAcquired) {
		t.Fatalf("expected ErrNotAcquired, got %v", err)
	}
}

func TestLock_Integration_ReacquireAfterRelease(t *testing.T) {
	client := startRedis(t)
	locker := lock.New(client)
	ctx := context.Background()

	lk, err := locker.TryAcquire(ctx, "test:reacquire", 10*time.Second)
	if err != nil {
		t.Fatalf("TryAcquire: %v", err)
	}
	if err := lk.Release(ctx); err != nil {
		t.Fatalf("Release: %v", err)
	}

	// Should be available again.
	lk2, err := locker.TryAcquire(ctx, "test:reacquire", 10*time.Second)
	if err != nil {
		t.Fatalf("second TryAcquire after release: %v", err)
	}
	_ = lk2.Release(ctx)
}

func TestLock_Integration_WithLock(t *testing.T) {
	client := startRedis(t)
	locker := lock.New(client)
	ctx := context.Background()

	counter := 0
	err := locker.WithLock(ctx, "test:withlock", 10*time.Second, func(ctx context.Context) error {
		counter++
		return nil
	})
	if err != nil {
		t.Fatalf("WithLock: %v", err)
	}
	if counter != 1 {
		t.Fatalf("expected counter=1, got %d", counter)
	}
}

func TestLock_Integration_WithRetry(t *testing.T) {
	client := startRedis(t)
	ctx := context.Background()

	// Acquire with holder1.
	holder1 := lock.New(client)
	lk, err := holder1.TryAcquire(ctx, "test:retry", 2*time.Second)
	if err != nil {
		t.Fatalf("holder1 acquire: %v", err)
	}

	// Release after 300ms in background.
	go func() {
		time.Sleep(300 * time.Millisecond)
		_ = lk.Release(ctx)
	}()

	// Contender with retry should eventually succeed.
	contender := lock.New(client, lock.WithRetry(10, 100*time.Millisecond))
	lk2, err := contender.Acquire(ctx, "test:retry", 2*time.Second)
	if err != nil {
		t.Fatalf("contender acquire with retry: %v", err)
	}
	_ = lk2.Release(ctx)
}
