package pipeline_test

import (
	"context"
	"testing"

	"github.com/miladhzz/gkit/pkg/pipeline"
)

func BenchmarkPipeline_Process_1Worker(b *testing.B) {
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}
	fn := func(ctx context.Context, v int) (int, error) { return v * 2, nil }
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, _ = pipeline.Process(ctx, items, fn, pipeline.WithWorkers(1))
	}
}

func BenchmarkPipeline_Process_8Workers(b *testing.B) {
	items := make([]int, 100)
	for i := range items {
		items[i] = i
	}
	fn := func(ctx context.Context, v int) (int, error) { return v * 2, nil }
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, _ = pipeline.Process(ctx, items, fn, pipeline.WithWorkers(8))
	}
}

func BenchmarkPipeline_Chain(b *testing.B) {
	double := func(ctx context.Context, v int) (int, error) { return v * 2, nil }
	add1 := func(ctx context.Context, v int) (int, error) { return v + 1, nil }
	chained := pipeline.Chain(double, add1)
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		_, _ = chained(ctx, 5)
	}
}
