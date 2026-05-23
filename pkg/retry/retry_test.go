package retry_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/retry"
)

var errFake = errors.New("fake error")

func TestDo_SuccessOnFirstAttempt(t *testing.T) {
	calls := 0
	got, err := retry.Do(context.Background(), func(_ context.Context) (int, error) {
		calls++
		return 42, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Fatalf("got %d, want 42", got)
	}
	if calls != 1 {
		t.Fatalf("called %d times, want 1", calls)
	}
}

func TestDo_RetriesAndSucceeds(t *testing.T) {
	var calls atomic.Int32
	got, err := retry.Do(context.Background(), func(_ context.Context) (string, error) {
		if calls.Add(1) < 3 {
			return "", errFake
		}
		return "ok", nil
	}, retry.WithMaxAttempts(5))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "ok" {
		t.Fatalf("got %q, want ok", got)
	}
}

func TestDo_ExhaustsAttempts(t *testing.T) {
	var calls atomic.Int32
	_, err := retry.Do(context.Background(), func(_ context.Context) (int, error) {
		calls.Add(1)
		return 0, errFake
	}, retry.WithMaxAttempts(3))

	if !retry.IsMaxAttempts(err) {
		t.Fatalf("expected ErrMaxAttempts, got %v", err)
	}
	if !errors.Is(err, errFake) {
		t.Fatalf("expected wrapped errFake, got %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("called %d times, want 3", calls.Load())
	}
}

func TestDo_StopAborts(t *testing.T) {
	calls := 0
	_, err := retry.Do(context.Background(), func(_ context.Context) (int, error) {
		calls++
		return 0, retry.Stop(errFake)
	}, retry.WithMaxAttempts(10))

	if !errors.Is(err, errFake) {
		t.Fatalf("expected errFake, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("called %d times, want 1", calls)
	}
}

func TestDo_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel

	_, err := retry.Do(ctx, func(_ context.Context) (int, error) {
		return 0, errFake
	}, retry.WithMaxAttempts(5))

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDo_OnRetryCallback(t *testing.T) {
	var retryAttempts []int
	retry.Do(context.Background(), func(_ context.Context) (int, error) { //nolint:errcheck
		return 0, errFake
	},
		retry.WithMaxAttempts(4),
		retry.WithOnRetry(func(_ context.Context, attempt int, _ error) {
			retryAttempts = append(retryAttempts, attempt)
		}),
	)

	// Attempts 1, 2, 3 fail → onRetry called with 1, 2, 3.
	if len(retryAttempts) != 3 {
		t.Fatalf("onRetry called %d times, want 3", len(retryAttempts))
	}
}

func TestDoVoid(t *testing.T) {
	calls := 0
	err := retry.DoVoid(context.Background(), func(_ context.Context) error {
		calls++
		if calls < 2 {
			return errFake
		}
		return nil
	}, retry.WithMaxAttempts(5))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 2 {
		t.Fatalf("called %d times, want 2", calls)
	}
}

func TestExponentialBackoff(t *testing.T) {
	b := retry.ExponentialBackoff{
		Initial:    100 * time.Millisecond,
		Multiplier: 2.0,
		Max:        1 * time.Second,
	}

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 100 * time.Millisecond},
		{1, 200 * time.Millisecond},
		{2, 400 * time.Millisecond},
		{3, 800 * time.Millisecond},
		{4, 1 * time.Second}, // capped
		{5, 1 * time.Second}, // capped
	}

	for _, tt := range tests {
		got := b.Next(tt.attempt)
		if got != tt.want {
			t.Errorf("attempt %d: got %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestJitterBackoff_WithinRange(t *testing.T) {
	base := retry.ConstantBackoff{Delay: 1 * time.Second}
	jitter := retry.WithJitter(base)

	for range 100 {
		d := jitter.Next(0)
		if d < 0 || d > time.Second {
			t.Fatalf("jitter out of range: %v", d)
		}
	}
}

func BenchmarkDo_NoRetry(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for range b.N {
		retry.Do(ctx, func(_ context.Context) (int, error) { //nolint:errcheck
			return 1, nil
		})
	}
}
