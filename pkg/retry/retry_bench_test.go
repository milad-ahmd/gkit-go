package retry_test

import (
	"context"
	"errors"
	"testing"

	"github.com/milad-ahmd/gkit-go/pkg/retry"
)

var errBench = errors.New("bench error")

func BenchmarkRetry_SucceedFirstAttempt(b *testing.B) {
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, _ = retry.Do(ctx, func(ctx context.Context) (int, error) {
			return 1, nil
		}, retry.WithMaxAttempts(3), retry.WithBackoff(retry.ConstantBackoff{}))
	}
}

func BenchmarkRetry_SucceedThirdAttempt(b *testing.B) {
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		attempt := 0
		_, _ = retry.Do(ctx, func(ctx context.Context) (int, error) {
			attempt++
			if attempt < 3 {
				return 0, errBench
			}
			return 1, nil
		}, retry.WithMaxAttempts(5), retry.WithBackoff(retry.ConstantBackoff{}))
	}
}

func BenchmarkRetry_BackoffNext(b *testing.B) {
	exp := retry.ExponentialBackoff{Multiplier: 2}
	b.ResetTimer()
	for i := range b.N {
		exp.Next(i % 10)
	}
}
