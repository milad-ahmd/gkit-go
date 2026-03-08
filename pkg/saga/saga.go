// Package saga implements the Saga pattern for managing distributed
// transactions via compensating actions.
//
// A saga is a sequence of steps. Each step has an Execute function and an
// optional Compensate function. If any Execute fails, the saga automatically
// calls Compensate on all previously completed steps in reverse order (LIFO).
//
// # Usage
//
//	s := saga.New("place-order",
//	    saga.Step{
//	        Name: "reserve-inventory",
//	        Execute:    func(ctx context.Context) error { return inventory.Reserve(ctx, item) },
//	        Compensate: func(ctx context.Context) error { return inventory.Release(ctx, item) },
//	    },
//	    saga.Step{
//	        Name: "charge-payment",
//	        Execute:    func(ctx context.Context) error { return payments.Charge(ctx, amount) },
//	        Compensate: func(ctx context.Context) error { return payments.Refund(ctx, amount) },
//	    },
//	    saga.Step{
//	        Name: "send-confirmation",
//	        Execute:    func(ctx context.Context) error { return email.Send(ctx, order) },
//	        // No compensation needed — idempotent / best-effort.
//	    },
//	)
//
//	if err := s.Run(ctx); err != nil {
//	    var se *saga.Error
//	    if errors.As(err, &se) {
//	        log.Printf("saga failed at %q: %v", se.FailedStep, se.Cause)
//	        log.Printf("compensation errors: %v", se.CompensationErrors)
//	    }
//	}
package saga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
)

// Step is a single unit of work inside a saga.
type Step struct {
	// Name is a human-readable identifier used in logs and errors.
	Name string

	// Execute performs the forward action. A non-nil error triggers rollback.
	Execute func(ctx context.Context) error

	// Compensate reverses the effect of Execute. May be nil for idempotent or
	// fire-and-forget steps where rollback is not possible / not needed.
	Compensate func(ctx context.Context) error
}

// --------------------------------------------------------------------------
// Saga

// Saga is an ordered sequence of Steps.
type Saga struct {
	name   string
	steps  []Step
	log    *slog.Logger
	onStep func(name string, phase string, err error)
}

// New creates a Saga.
func New(name string, steps ...Step) *Saga {
	return &Saga{
		name:  name,
		steps: steps,
		log:   slog.Default(),
	}
}

// WithLogger sets the logger used for step lifecycle events.
func (s *Saga) WithLogger(l *slog.Logger) *Saga {
	s.log = l
	return s
}

// WithOnStep registers a hook called after each execute/compensate attempt.
func (s *Saga) WithOnStep(fn func(name string, phase string, err error)) *Saga {
	s.onStep = fn
	return s
}

// Run executes all steps in order. On failure it compensates completed steps
// in reverse order and returns a *Error describing what happened.
func (s *Saga) Run(ctx context.Context) error {
	s.log.Info("saga: starting", "saga", s.name, "steps", len(s.steps))

	completed := make([]int, 0, len(s.steps))

	for i, step := range s.steps {
		s.log.Info("saga: executing step", "saga", s.name, "step", step.Name)

		err := step.Execute(ctx)
		s.notify(step.Name, "execute", err)

		if err != nil {
			s.log.Error("saga: step failed, rolling back",
				"saga", s.name, "step", step.Name, "error", err)

			compErrs := s.compensate(ctx, completed)
			return &Error{
				SagaName:           s.name,
				FailedStep:         step.Name,
				Cause:              err,
				CompensationErrors: compErrs,
			}
		}

		s.log.Info("saga: step completed", "saga", s.name, "step", step.Name)
		completed = append(completed, i)
	}

	s.log.Info("saga: completed successfully", "saga", s.name)
	return nil
}

// compensate runs compensations in reverse order and collects any errors.
func (s *Saga) compensate(ctx context.Context, completed []int) []CompensationError {
	var errs []CompensationError

	for i := len(completed) - 1; i >= 0; i-- {
		step := s.steps[completed[i]]
		if step.Compensate == nil {
			s.log.Info("saga: no compensation for step (skipping)",
				"saga", s.name, "step", step.Name)
			continue
		}

		s.log.Info("saga: compensating step", "saga", s.name, "step", step.Name)
		err := step.Compensate(ctx)
		s.notify(step.Name, "compensate", err)

		if err != nil {
			s.log.Error("saga: compensation failed",
				"saga", s.name, "step", step.Name, "error", err)
			errs = append(errs, CompensationError{Step: step.Name, Err: err})
		}
	}

	return errs
}

func (s *Saga) notify(name, phase string, err error) {
	if s.onStep != nil {
		s.onStep(name, phase, err)
	}
}

// --------------------------------------------------------------------------
// Error

// Error is returned by Saga.Run when a step fails. It contains the failure
// cause and any errors that occurred during compensation.
type Error struct {
	SagaName           string
	FailedStep         string
	Cause              error
	CompensationErrors []CompensationError
}

// CompensationError records a failure during compensation of a specific step.
type CompensationError struct {
	Step string
	Err  error
}

func (e *Error) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "saga %q failed at step %q: %v", e.SagaName, e.FailedStep, e.Cause)
	if len(e.CompensationErrors) > 0 {
		b.WriteString("; compensation errors: ")
		for i, ce := range e.CompensationErrors {
			if i > 0 {
				b.WriteString(", ")
			}
			fmt.Fprintf(&b, "%s: %v", ce.Step, ce.Err)
		}
	}
	return b.String()
}

func (e *Error) Unwrap() error { return e.Cause }

// HasCompensationErrors reports whether any compensation step failed.
func (e *Error) HasCompensationErrors() bool { return len(e.CompensationErrors) > 0 }

// Is enables errors.Is matching against the original cause.
func (e *Error) Is(target error) bool { return errors.Is(e.Cause, target) }
