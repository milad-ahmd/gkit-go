// Package health provides a composable health-check system with an HTTP handler
// suitable for Kubernetes liveness and readiness probes.
//
// Checkers are registered by name and run concurrently on each request.
// The HTTP response is 200 OK when all checks pass, or 503 Service Unavailable
// when at least one fails, with a JSON body listing each check and its status.
//
//	h := health.New()
//
//	h.Register("database", health.CheckerFunc(func(ctx context.Context) error {
//	    return db.PingContext(ctx)
//	}))
//	h.Register("cache", health.CheckerFunc(func(ctx context.Context) error {
//	    return rdb.Ping(ctx).Err()
//	}))
//
//	mux.Handle("/healthz/ready", h.ReadyHandler())
//	mux.Handle("/healthz/live",  h.LiveHandler())
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Checker reports whether a dependency is healthy.
type Checker interface {
	Check(ctx context.Context) error
}

// CheckerFunc adapts a plain function to the Checker interface.
type CheckerFunc func(ctx context.Context) error

func (f CheckerFunc) Check(ctx context.Context) error { return f(ctx) }

// Status represents the health status of a single check.
type Status struct {
	Name    string `json:"name"`
	Healthy bool   `json:"healthy"`
	Error   string `json:"error,omitempty"`
}

// Report is the full health-check result.
type Report struct {
	Healthy  bool     `json:"healthy"`
	Checks   []Status `json:"checks"`
	Duration string   `json:"duration"`
}

type registration struct {
	name    string
	checker Checker
}

// Group runs a set of named health checks.
type Group struct {
	mu      sync.RWMutex
	checks  []registration
	timeout time.Duration
}

// New creates a Group with a default per-check timeout of 5 seconds.
func New(opts ...Option) *Group {
	g := &Group{timeout: 5 * time.Second}
	for _, o := range opts {
		o(g)
	}
	return g
}

// Option configures a Group.
type Option func(*Group)

// WithTimeout sets the maximum time each individual check may take.
func WithTimeout(d time.Duration) Option {
	return func(g *Group) { g.timeout = d }
}

// Register adds a named checker. Safe to call concurrently.
func (g *Group) Register(name string, c Checker) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.checks = append(g.checks, registration{name: name, checker: c})
}

// Check runs all registered checkers concurrently and returns a Report.
func (g *Group) Check(ctx context.Context) Report {
	start := time.Now()

	g.mu.RLock()
	checks := make([]registration, len(g.checks))
	copy(checks, g.checks)
	g.mu.RUnlock()

	type result struct {
		idx    int
		status Status
	}

	results := make([]result, len(checks))
	var wg sync.WaitGroup

	for i, reg := range checks {
		wg.Add(1)
		go func(idx int, r registration) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(ctx, g.timeout)
			defer cancel()

			s := Status{Name: r.name, Healthy: true}
			if err := r.checker.Check(ctx); err != nil {
				s.Healthy = false
				s.Error = err.Error()
			}
			results[idx] = result{idx: idx, status: s}
		}(i, reg)
	}

	wg.Wait()

	report := Report{
		Healthy:  true,
		Checks:   make([]Status, len(checks)),
		Duration: time.Since(start).String(),
	}
	for _, r := range results {
		report.Checks[r.idx] = r.status
		if !r.status.Healthy {
			report.Healthy = false
		}
	}
	return report
}

// ReadyHandler returns an HTTP handler for readiness probes.
// Returns 200 when all checks pass, 503 otherwise.
func (g *Group) ReadyHandler() http.Handler {
	return g.handler()
}

// LiveHandler returns an HTTP handler for liveness probes.
// An empty Group always returns 200 (the process is alive).
func (g *Group) LiveHandler() http.Handler {
	live := New()
	return live.handler()
}

func (g *Group) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		report := g.Check(r.Context())
		w.Header().Set("Content-Type", "application/json")
		if !report.Healthy {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(report) //nolint:errcheck
	})
}
