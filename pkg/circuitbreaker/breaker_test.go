package circuitbreaker_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/circuitbreaker"
)

var errFake = errors.New("fake error")

func TestBreaker_ClosedToOpen(t *testing.T) {
	cb := circuitbreaker.New(
		circuitbreaker.WithFailureThreshold(3),
		circuitbreaker.WithOpenTimeout(1*time.Hour),
	)

	ctx := context.Background()
	fail := func(_ context.Context) (int, error) { return 0, errFake }

	for range 3 {
		circuitbreaker.Execute(ctx, cb, fail) //nolint:errcheck
	}

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open, got %s", cb.State())
	}
}

func TestBreaker_OpenRejectsRequests(t *testing.T) {
	cb := circuitbreaker.New(
		circuitbreaker.WithFailureThreshold(1),
		circuitbreaker.WithOpenTimeout(1*time.Hour),
	)
	ctx := context.Background()
	circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 0, errFake }) //nolint:errcheck

	_, err := circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 1, nil })
	if !errors.Is(err, circuitbreaker.ErrOpen) {
		t.Fatalf("expected ErrOpen, got %v", err)
	}
}

func TestBreaker_OpenToHalfOpen(t *testing.T) {
	cb := circuitbreaker.New(
		circuitbreaker.WithFailureThreshold(1),
		circuitbreaker.WithOpenTimeout(20*time.Millisecond),
	)
	ctx := context.Background()
	circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 0, errFake }) //nolint:errcheck

	time.Sleep(30 * time.Millisecond) // wait for open timeout

	if state := cb.State(); state != circuitbreaker.StateHalfOpen {
		t.Fatalf("expected HalfOpen, got %s", state)
	}
}

func TestBreaker_HalfOpenToClosed(t *testing.T) {
	cb := circuitbreaker.New(
		circuitbreaker.WithFailureThreshold(1),
		circuitbreaker.WithSuccessThreshold(2),
		circuitbreaker.WithOpenTimeout(10*time.Millisecond),
	)
	ctx := context.Background()
	circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 0, errFake }) //nolint:errcheck
	time.Sleep(20 * time.Millisecond)

	succeed := func(_ context.Context) (int, error) { return 1, nil }
	circuitbreaker.Execute(ctx, cb, succeed) //nolint:errcheck
	circuitbreaker.Execute(ctx, cb, succeed) //nolint:errcheck

	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected Closed after successes, got %s", cb.State())
	}
}

func TestBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := circuitbreaker.New(
		circuitbreaker.WithFailureThreshold(1),
		circuitbreaker.WithOpenTimeout(10*time.Millisecond),
	)
	ctx := context.Background()
	circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 0, errFake }) //nolint:errcheck
	time.Sleep(20 * time.Millisecond)

	// One failure in HalfOpen should re-open.
	circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 0, errFake }) //nolint:errcheck

	if cb.State() != circuitbreaker.StateOpen {
		t.Fatalf("expected Open after HalfOpen failure, got %s", cb.State())
	}
}

func TestBreaker_Reset(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.WithFailureThreshold(1))
	ctx := context.Background()
	circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 0, errFake }) //nolint:errcheck

	cb.Reset()

	if cb.State() != circuitbreaker.StateClosed {
		t.Fatalf("expected Closed after Reset, got %s", cb.State())
	}

	_, err := circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 42, nil })
	if err != nil {
		t.Fatalf("unexpected error after Reset: %v", err)
	}
}

func TestBreaker_OnStateChange(t *testing.T) {
	var transitions []string
	var mu atomic.Value // use atomic to avoid data race in callback goroutine

	cb := circuitbreaker.New(
		circuitbreaker.WithFailureThreshold(1),
		circuitbreaker.WithOpenTimeout(10*time.Millisecond),
		circuitbreaker.WithOnStateChange(func(from, to circuitbreaker.State) {
			val, _ := mu.Load().([]string)
			mu.Store(append(val, from.String()+"->"+to.String()))
		}),
	)
	ctx := context.Background()

	circuitbreaker.Execute(ctx, cb, func(_ context.Context) (int, error) { return 0, errFake }) //nolint:errcheck
	time.Sleep(30 * time.Millisecond)                                                           // trigger HalfOpen
	cb.State()

	time.Sleep(10 * time.Millisecond) // let callback goroutine run

	transitions, _ = mu.Load().([]string)
	if len(transitions) == 0 {
		t.Error("expected at least one state change callback")
	}
	_ = transitions
}

func TestBreaker_ExecuteVoid(t *testing.T) {
	cb := circuitbreaker.New(circuitbreaker.WithFailureThreshold(10))
	ctx := context.Background()

	err := circuitbreaker.ExecuteVoid(ctx, cb, func(_ context.Context) error { return nil })
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
