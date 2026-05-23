package graceful_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/milad-ahmd/gkit-go/pkg/graceful"
)

func TestGroup_Shutdown_CallsHooksInReverseOrder(t *testing.T) {
	var order []string

	g := graceful.New()
	g.Register("first", func(_ context.Context) error {
		order = append(order, "first")
		return nil
	})
	g.Register("second", func(_ context.Context) error {
		order = append(order, "second")
		return nil
	})
	g.Register("third", func(_ context.Context) error {
		order = append(order, "third")
		return nil
	})

	if err := g.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"third", "second", "first"}
	for i, got := range order {
		if got != want[i] {
			t.Errorf("order[%d]: got %q, want %q", i, got, want[i])
		}
	}
}

func TestGroup_Shutdown_CollectsErrors(t *testing.T) {
	errA := errors.New("error A")
	errB := errors.New("error B")

	g := graceful.New()
	g.Register("a", func(_ context.Context) error { return errA })
	g.Register("b", func(_ context.Context) error { return errB })

	err := g.Shutdown(context.Background())
	if err == nil {
		t.Fatal("expected combined error, got nil")
	}
	if !errors.Is(err, errA) {
		t.Errorf("expected errA in combined error, got: %v", err)
	}
	if !errors.Is(err, errB) {
		t.Errorf("expected errB in combined error, got: %v", err)
	}
}

func TestGroup_Shutdown_RespectsTimeout(t *testing.T) {
	g := graceful.New(graceful.WithTimeout(10 * time.Millisecond))
	g.Register("slow", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			return nil
		}
	})

	err := g.Shutdown(context.Background())
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("expected DeadlineExceeded, got: %v", err)
	}
}

func TestGroup_Shutdown_NoHooks(t *testing.T) {
	g := graceful.New()
	if err := g.Shutdown(context.Background()); err != nil {
		t.Fatalf("unexpected error with no hooks: %v", err)
	}
}
