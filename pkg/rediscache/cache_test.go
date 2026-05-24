package rediscache_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/milad-ahmd/gkit-go/pkg/rediscache"
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

func TestCache_SetGet(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New[string](redisClient(t),
		rediscache.WithKeyPrefix[string]("test:setget:"),
	)
	_ = c.Flush(ctx)
	t.Cleanup(func() { _ = c.Flush(ctx) })

	if err := c.Set(ctx, "hello", "world", time.Minute); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, ok, err := c.Get(ctx, "hello")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !ok {
		t.Fatal("expected cache hit")
	}
	if val != "world" {
		t.Errorf("val = %q, want %q", val, "world")
	}
}

func TestCache_Miss(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New[string](redisClient(t),
		rediscache.WithKeyPrefix[string]("test:miss:"),
	)
	_ = c.Flush(ctx)
	t.Cleanup(func() { _ = c.Flush(ctx) })

	_, ok, err := c.Get(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Error("expected cache miss")
	}
}

func TestCache_Delete(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New[int](redisClient(t),
		rediscache.WithKeyPrefix[int]("test:del:"),
	)
	_ = c.Flush(ctx)
	t.Cleanup(func() { _ = c.Flush(ctx) })

	_ = c.Set(ctx, "x", 42, time.Minute)
	if err := c.Delete(ctx, "x"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ := c.Get(ctx, "x")
	if ok {
		t.Error("expected miss after delete")
	}
}

func TestCache_TTL(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New[string](redisClient(t),
		rediscache.WithKeyPrefix[string]("test:ttl:"),
	)
	_ = c.Flush(ctx)
	t.Cleanup(func() { _ = c.Flush(ctx) })

	_ = c.Set(ctx, "expire", "soon", 50*time.Millisecond)
	time.Sleep(150 * time.Millisecond)

	_, ok, err := c.Get(ctx, "expire")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if ok {
		t.Error("expected miss after TTL expiry")
	}
}

func TestCache_MGet(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New[int](redisClient(t),
		rediscache.WithKeyPrefix[int]("test:mget:"),
	)
	_ = c.Flush(ctx)
	t.Cleanup(func() { _ = c.Flush(ctx) })

	_ = c.Set(ctx, "a", 1, time.Minute)
	_ = c.Set(ctx, "b", 2, time.Minute)

	got, err := c.MGet(ctx, "a", "b", "missing")
	if err != nil {
		t.Fatalf("MGet: %v", err)
	}
	if got["a"] != 1 || got["b"] != 2 {
		t.Errorf("MGet result = %v", got)
	}
	if _, ok := got["missing"]; ok {
		t.Error("missing key should not be in result")
	}
}

func TestCache_Check(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New[string](redisClient(t))
	if err := c.Check(ctx); err != nil {
		t.Fatalf("Check: %v", err)
	}
}

type product struct {
	ID    string  `json:"id"`
	Name  string  `json:"name"`
	Price float64 `json:"price"`
}

func TestCache_Struct(t *testing.T) {
	ctx := context.Background()
	c := rediscache.New[*product](redisClient(t),
		rediscache.WithKeyPrefix[*product]("test:struct:"),
	)
	_ = c.Flush(ctx)
	t.Cleanup(func() { _ = c.Flush(ctx) })

	p := &product{ID: "p1", Name: "Widget", Price: 9.99}
	_ = c.Set(ctx, "p1", p, time.Minute)

	got, ok, err := c.Get(ctx, "p1")
	if err != nil || !ok {
		t.Fatalf("Get: ok=%v err=%v", ok, err)
	}
	if got.Name != "Widget" || got.Price != 9.99 {
		t.Errorf("got = %+v", got)
	}
}
