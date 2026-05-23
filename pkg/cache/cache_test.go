package cache_test

import (
	"context"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/cache"
)

func TestCache_SetAndGet(t *testing.T) {
	c := cache.New[string, int](10)
	c.Set("a", 1)
	c.Set("b", 2)

	if v, ok := c.Get("a"); !ok || v != 1 {
		t.Fatalf("Get(a): got (%d, %v), want (1, true)", v, ok)
	}
	if v, ok := c.Get("b"); !ok || v != 2 {
		t.Fatalf("Get(b): got (%d, %v), want (2, true)", v, ok)
	}
}

func TestCache_MissingKey(t *testing.T) {
	c := cache.New[string, int](10)
	if _, ok := c.Get("missing"); ok {
		t.Fatal("expected cache miss")
	}
}

func TestCache_EvictsLRU(t *testing.T) {
	c := cache.New[string, int](3)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)

	// Access "a" to make it recently used.
	c.Get("a")

	// "b" is now the least recently used.
	c.Set("d", 4) // should evict "b"

	if _, ok := c.Get("b"); ok {
		t.Fatal("expected 'b' to be evicted")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatal("expected 'a' to still be present")
	}
	if _, ok := c.Get("d"); !ok {
		t.Fatal("expected 'd' to be present")
	}
}

func TestCache_Delete(t *testing.T) {
	c := cache.New[string, int](10)
	c.Set("x", 99)
	c.Delete("x")

	if _, ok := c.Get("x"); ok {
		t.Fatal("expected cache miss after Delete")
	}
}

func TestCache_Clear(t *testing.T) {
	c := cache.New[string, int](10)
	for i := range 5 {
		c.Set(string(rune('a'+i)), i)
	}
	c.Clear()

	if c.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", c.Len())
	}
}

func TestCache_TTL_Expiry(t *testing.T) {
	c := cache.New[string, int](10, cache.WithTTL[string, int](20*time.Millisecond))
	c.Set("k", 42)

	if v, ok := c.Get("k"); !ok || v != 42 {
		t.Fatal("expected key to be present before TTL expires")
	}

	time.Sleep(30 * time.Millisecond)

	if _, ok := c.Get("k"); ok {
		t.Fatal("expected key to be expired")
	}
}

func TestCache_Janitor(t *testing.T) {
	c := cache.New[string, int](100, cache.WithTTL[string, int](20*time.Millisecond))
	for i := range 10 {
		c.Set(string(rune('a'+i)), i)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.StartJanitor(ctx, 10*time.Millisecond)

	time.Sleep(50 * time.Millisecond) // wait for janitor to run

	if c.Len() != 0 {
		t.Fatalf("janitor did not clean up: Len = %d", c.Len())
	}
}

func TestCache_Stats(t *testing.T) {
	c := cache.New[string, int](3)
	c.Set("a", 1)
	c.Set("b", 2)
	c.Set("c", 3)
	c.Set("d", 4) // evicts "a"

	c.Get("b") // hit
	c.Get("b") // hit
	c.Get("z") // miss

	stats := c.Stats()
	if stats.Hits != 2 {
		t.Errorf("Hits: got %d, want 2", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("Misses: got %d, want 1", stats.Misses)
	}
	if stats.Evicts != 1 {
		t.Errorf("Evicts: got %d, want 1", stats.Evicts)
	}
}

func TestCache_UpdateExistingKey(t *testing.T) {
	c := cache.New[string, int](3)
	c.Set("a", 1)
	c.Set("a", 99)

	if v, ok := c.Get("a"); !ok || v != 99 {
		t.Fatalf("expected updated value 99, got (%d, %v)", v, ok)
	}
	if c.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", c.Len())
	}
}

func BenchmarkCache_SetGet(b *testing.B) {
	c := cache.New[int, int](1024)
	b.ReportAllocs()
	for i := range b.N {
		c.Set(i%512, i)
		c.Get(i % 512)
	}
}
