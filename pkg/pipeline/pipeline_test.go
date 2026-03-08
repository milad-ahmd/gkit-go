package pipeline_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/miladhzz/gkit/pkg/pipeline"
)

func TestProcess_TransformsAll(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	got, err := pipeline.Process(context.Background(), input,
		func(_ context.Context, n int) (int, error) { return n * 2, nil },
		pipeline.WithWorkers(3),
		pipeline.WithOrdered(true),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, v := range got {
		if v != input[i]*2 {
			t.Errorf("got[%d] = %d, want %d", i, v, input[i]*2)
		}
	}
}

func TestProcess_OrderPreserved(t *testing.T) {
	input := make([]int, 100)
	for i := range input {
		input[i] = i
	}
	got, err := pipeline.Process(context.Background(), input,
		func(_ context.Context, n int) (int, error) { return n, nil },
		pipeline.WithOrdered(true),
		pipeline.WithWorkers(16),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, v := range got {
		if v != i {
			t.Errorf("order broken at index %d: got %d", i, v)
		}
	}
}

func TestProcess_ErrorShortCircuits(t *testing.T) {
	errBoom := errors.New("boom")
	_, err := pipeline.Process(context.Background(), []int{1, 2, 3},
		func(_ context.Context, n int) (int, error) {
			if n == 2 {
				return 0, errBoom
			}
			return n, nil
		},
	)
	if !errors.Is(err, errBoom) {
		t.Fatalf("expected errBoom, got %v", err)
	}
}

func TestProcess_EmptyInput(t *testing.T) {
	got, err := pipeline.Process(context.Background(), []int{},
		func(_ context.Context, n int) (int, error) { return n, nil },
	)
	if err != nil || len(got) != 0 {
		t.Fatalf("expected empty result, got %v err=%v", got, err)
	}
}

func TestProcess_UnorderedHasAllResults(t *testing.T) {
	input := []int{1, 2, 3, 4, 5}
	got, err := pipeline.Process(context.Background(), input,
		func(_ context.Context, n int) (string, error) { return fmt.Sprintf("%d", n), nil },
		pipeline.WithOrdered(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(got)
	want := []string{"1", "2", "3", "4", "5"}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestChain_AppliesStagesInOrder(t *testing.T) {
	add1 := func(_ context.Context, n int) (int, error) { return n + 1, nil }
	double := func(_ context.Context, n int) (int, error) { return n * 2, nil }
	sub3 := func(_ context.Context, n int) (int, error) { return n - 3, nil }

	p := pipeline.Chain(add1, double, sub3)
	// (5 + 1) * 2 - 3 = 9
	got, err := p(context.Background(), 5)
	if err != nil || got != 9 {
		t.Fatalf("got (%d, %v), want (9, nil)", got, err)
	}
}

func TestChain_StopsOnError(t *testing.T) {
	errStop := errors.New("stop")
	calls := 0
	stage := func(_ context.Context, n int) (int, error) { calls++; return 0, errStop }
	noop := func(_ context.Context, n int) (int, error) { calls++; return n, nil }

	p := pipeline.Chain(stage, noop)
	_, err := p(context.Background(), 0)

	if !errors.Is(err, errStop) {
		t.Fatalf("expected errStop, got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call (short-circuit), got %d", calls)
	}
}

func TestCompose_TypedTransform(t *testing.T) {
	toString := func(_ context.Context, n int) (string, error) {
		return fmt.Sprintf("item-%d", n), nil
	}
	addSuffix := func(_ context.Context, s string) (string, error) {
		return s + "-ok", nil
	}

	combined := pipeline.Compose(toString, addSuffix)
	got, err := combined(context.Background(), 42)
	if err != nil || got != "item-42-ok" {
		t.Fatalf("got (%q, %v), want (item-42-ok, nil)", got, err)
	}
}

func TestCompose3(t *testing.T) {
	f1 := func(_ context.Context, n int) (int, error) { return n + 1, nil }
	f2 := func(_ context.Context, n int) (string, error) { return fmt.Sprintf("%d", n), nil }
	f3 := func(_ context.Context, s string) ([]byte, error) { return []byte(s), nil }

	f := pipeline.Compose3(f1, f2, f3)
	got, err := f(context.Background(), 9)
	if err != nil || string(got) != "10" {
		t.Fatalf("got (%q, %v), want (10, nil)", got, err)
	}
}

func BenchmarkProcess_Parallel(b *testing.B) {
	input := make([]int, 1000)
	ctx := context.Background()
	b.ReportAllocs()
	for range b.N {
		pipeline.Process(ctx, input, //nolint:errcheck
			func(_ context.Context, n int) (int, error) { return n * 2, nil },
			pipeline.WithWorkers(8),
		)
	}
}
