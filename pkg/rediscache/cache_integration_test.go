//go:build integration

package rediscache_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/milad-ahmd/gkit-go/pkg/rediscache"
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
	addr := fmt.Sprintf("%s:%s", host, port.Port())

	return redis.NewClient(&redis.Options{Addr: addr})
}

func TestRedisCache_Integration_SetAndGet(t *testing.T) {
	client := startRedis(t)
	ctx := context.Background()

	cache := rediscache.New[string](client, rediscache.WithKeyPrefix[string]("test:"))

	if err := cache.Set(ctx, "greeting", "hello", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, ok, err := cache.Get(ctx, "greeting")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("expected cache hit")
	}
	if val != "hello" {
		t.Fatalf("expected %q, got %q", "hello", val)
	}
}

func TestRedisCache_Integration_Miss(t *testing.T) {
	client := startRedis(t)
	ctx := context.Background()

	cache := rediscache.New[string](client, rediscache.WithKeyPrefix[string]("test:"))

	_, ok, err := cache.Get(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Fatal("expected cache miss")
	}
}

func TestRedisCache_Integration_Delete(t *testing.T) {
	client := startRedis(t)
	ctx := context.Background()

	cache := rediscache.New[int](client, rediscache.WithKeyPrefix[int]("test:"))

	if err := cache.Set(ctx, "num", 42, time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if err := cache.Delete(ctx, "num"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, ok, err := cache.Get(ctx, "num")
	if err != nil {
		t.Fatalf("Get after Delete: %v", err)
	}
	if ok {
		t.Fatal("expected miss after delete")
	}
}

func TestRedisCache_Integration_TTLExpiry(t *testing.T) {
	client := startRedis(t)
	ctx := context.Background()

	cache := rediscache.New[string](client, rediscache.WithKeyPrefix[string]("test:"))

	if err := cache.Set(ctx, "expires", "soon", 500*time.Millisecond); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Should hit before expiry.
	_, ok, err := cache.Get(ctx, "expires")
	if err != nil || !ok {
		t.Fatalf("expected hit before expiry: ok=%v err=%v", ok, err)
	}

	time.Sleep(700 * time.Millisecond)

	_, ok, err = cache.Get(ctx, "expires")
	if err != nil {
		t.Fatalf("Get after TTL: %v", err)
	}
	if ok {
		t.Fatal("expected miss after TTL expiry")
	}
}

func TestRedisCache_Integration_MGet(t *testing.T) {
	client := startRedis(t)
	ctx := context.Background()

	type Product struct {
		Name  string
		Price float64
	}

	cache := rediscache.New[Product](client, rediscache.WithKeyPrefix[Product]("products:"))
	_ = cache.Set(ctx, "a", Product{"Widget", 9.99}, time.Minute)
	_ = cache.Set(ctx, "b", Product{"Gadget", 19.99}, time.Minute)

	result, err := cache.MGet(ctx, "a", "b", "c")
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 results, got %d", len(result))
	}
	if result["a"].Name != "Widget" {
		t.Fatalf("expected Widget, got %q", result["a"].Name)
	}
}

func TestRedisCache_Integration_Flush(t *testing.T) {
	client := startRedis(t)
	ctx := context.Background()

	cache := rediscache.New[string](client, rediscache.WithKeyPrefix[string]("flush:"))
	_ = cache.Set(ctx, "x", "1", time.Minute)
	_ = cache.Set(ctx, "y", "2", time.Minute)

	if err := cache.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	_, okX, _ := cache.Get(ctx, "x")
	_, okY, _ := cache.Get(ctx, "y")
	if okX || okY {
		t.Fatal("expected all keys flushed")
	}
}

func TestRedisCache_Integration_Ping(t *testing.T) {
	client := startRedis(t)
	ctx := context.Background()

	cache := rediscache.New[string](client)
	if err := cache.Check(ctx); err != nil {
		t.Fatalf("Check (ping): %v", err)
	}
}
