package cache_test

import (
	"fmt"
	"testing"

	"github.com/miladhzz/gkit/pkg/cache"
)

func BenchmarkCache_Set(b *testing.B) {
	c := cache.New[string, int](1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Set(fmt.Sprintf("key-%d", i%500), i)
			i++
		}
	})
}

func BenchmarkCache_Get_Hit(b *testing.B) {
	c := cache.New[string, int](1000)
	for i := range 1000 {
		c.Set(fmt.Sprintf("key-%d", i), i)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			c.Get(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}

func BenchmarkCache_Get_Miss(b *testing.B) {
	c := cache.New[string, int](100)
	b.ResetTimer()
	for b.Loop() {
		c.Get("nonexistent")
	}
}

func BenchmarkCache_SetAndGet(b *testing.B) {
	c := cache.New[string, string](512)
	b.ResetTimer()
	for i := range b.N {
		key := fmt.Sprintf("k%d", i%256)
		c.Set(key, "value")
		c.Get(key)
	}
}
