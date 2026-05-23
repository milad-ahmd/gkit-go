package ratelimit_test

import (
	"testing"

	"github.com/milad-ahmd/gkit-go/pkg/ratelimit"
)

func BenchmarkRateLimiter_Allow(b *testing.B) {
	lim := ratelimit.New(1_000_000, 1_000_000) // high limits so we don't block
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lim.Allow()
		}
	})
}

func BenchmarkRateLimiter_AllowN(b *testing.B) {
	lim := ratelimit.New(1_000_000, 1_000_000)
	b.ResetTimer()
	for b.Loop() {
		lim.AllowN(1)
	}
}

func BenchmarkKeyedRateLimiter_Allow(b *testing.B) {
	klim := ratelimit.NewKeyed[string](1_000_000, 1_000_000)
	keys := []string{"user-a", "user-b", "user-c", "user-d"}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			klim.Allow(keys[i%len(keys)])
			i++
		}
	})
}
