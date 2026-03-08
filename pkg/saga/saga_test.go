package saga_test

import (
	"context"
	"errors"
	"testing"

	"github.com/miladhzz/gkit/pkg/saga"
)

var errFail = errors.New("step failed")
var errComp = errors.New("compensation failed")

func ok(_ context.Context) error  { return nil }
func fail(_ context.Context) error { return errFail }

func TestSaga_AllStepsSucceed(t *testing.T) {
	executed := 0
	s := saga.New("test",
		saga.Step{Name: "step1", Execute: func(ctx context.Context) error { executed++; return nil }},
		saga.Step{Name: "step2", Execute: func(ctx context.Context) error { executed++; return nil }},
		saga.Step{Name: "step3", Execute: func(ctx context.Context) error { executed++; return nil }},
	)
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if executed != 3 {
		t.Errorf("executed %d, want 3", executed)
	}
}

func TestSaga_FailureTriggersCompensation(t *testing.T) {
	var compensated []string

	comp := func(name string) func(context.Context) error {
		return func(ctx context.Context) error {
			compensated = append(compensated, name)
			return nil
		}
	}

	s := saga.New("place-order",
		saga.Step{Name: "reserve", Execute: ok, Compensate: comp("reserve")},
		saga.Step{Name: "charge", Execute: ok, Compensate: comp("charge")},
		saga.Step{Name: "notify", Execute: fail}, // fails here
	)

	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}

	var se *saga.Error
	if !errors.As(err, &se) {
		t.Fatalf("expected *saga.Error, got %T", err)
	}
	if se.FailedStep != "notify" {
		t.Errorf("FailedStep = %q, want notify", se.FailedStep)
	}
	if !errors.Is(err, errFail) {
		t.Error("errors.Is should match original cause")
	}

	// Compensation should be LIFO: charge → reserve (notify had none).
	if len(compensated) != 2 {
		t.Fatalf("compensated %v, want [charge reserve]", compensated)
	}
	if compensated[0] != "charge" || compensated[1] != "reserve" {
		t.Errorf("compensation order = %v, want [charge reserve]", compensated)
	}
}

func TestSaga_CompensationError(t *testing.T) {
	s := saga.New("saga",
		saga.Step{
			Name:       "step1",
			Execute:    ok,
			Compensate: func(_ context.Context) error { return errComp },
		},
		saga.Step{Name: "step2", Execute: fail},
	)

	err := s.Run(context.Background())

	var se *saga.Error
	if !errors.As(err, &se) {
		t.Fatalf("expected *saga.Error")
	}
	if !se.HasCompensationErrors() {
		t.Error("expected compensation errors")
	}
	if se.CompensationErrors[0].Err != errComp {
		t.Errorf("compensation err = %v", se.CompensationErrors[0].Err)
	}
}

func TestSaga_NilCompensateSkipped(t *testing.T) {
	// Step with no Compensate should not panic.
	s := saga.New("saga",
		saga.Step{Name: "s1", Execute: ok, Compensate: nil},
		saga.Step{Name: "s2", Execute: fail},
	)
	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
}
