// Package clock provides a testable abstraction over time operations.
package clock

import "time"

// Clock abstracts time operations to make code testable.
type Clock interface {
	Now() time.Time
	Since(t time.Time) time.Duration
	Sleep(d time.Duration)
	After(d time.Duration) <-chan time.Time
}

// Real is the production clock backed by the real system clock.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now() }
func (Real) Since(t time.Time) time.Duration        { return time.Since(t) }
func (Real) Sleep(d time.Duration)                  { time.Sleep(d) }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// New returns a new Real clock.
func New() Clock { return Real{} }
