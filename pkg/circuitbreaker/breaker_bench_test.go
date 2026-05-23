package circuitbreaker_test

import (
	"context"
	"testing"

	"github.com/milad-ahmd/gkit-go/pkg/circuitbreaker"
)

func BenchmarkCircuitBreaker_Execute_Closed(b *testing.B) {
	cb := circuitbreaker.New(circuitbreaker.WithFailureThreshold(100))
	ctx := context.Background()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = circuitbreaker.Execute(ctx, cb, func(ctx context.Context) (int, error) {
				return 1, nil
			})
		}
	})
}

func BenchmarkCircuitBreaker_StateCheck(b *testing.B) {
	cb := circuitbreaker.New()
	b.ResetTimer()
	for b.Loop() {
		_ = cb.State()
	}
}
