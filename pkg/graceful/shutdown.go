// Package graceful coordinates ordered, timeout-aware shutdown of multiple services.
//
// Services are registered with a name and a shutdown function. When Shutdown is
// called (or a OS signal is received via ListenAndShutdown), each shutdown
// function is invoked with a deadline context. Shutdown order is the reverse of
// registration order (LIFO), mirroring the typical dependency graph.
//
// Example:
//
//	g := graceful.New(graceful.WithTimeout(10 * time.Second))
//
//	g.Register("http-server", func(ctx context.Context) error {
//	    return srv.Shutdown(ctx)
//	})
//	g.Register("worker-pool", func(ctx context.Context) error {
//	    pool.Stop()
//	    return nil
//	})
//
//	// Block until SIGINT/SIGTERM or ctx is cancelled.
//	if err := g.ListenAndShutdown(ctx); err != nil {
//	    log.Fatal(err)
//	}
package graceful

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ShutdownFunc is a function that shuts down a named service.
type ShutdownFunc func(ctx context.Context) error

type registration struct {
	name string
	fn   ShutdownFunc
}

// Group coordinates graceful shutdown of multiple services.
type Group struct {
	timeout time.Duration
	mu      sync.Mutex
	hooks   []registration
}

// Option configures a Group.
type Option func(*Group)

// WithTimeout sets the maximum total time allowed for all shutdown hooks to complete.
// Defaults to 30 seconds.
func WithTimeout(d time.Duration) Option {
	return func(g *Group) { g.timeout = d }
}

// New creates a new Group.
func New(opts ...Option) *Group {
	g := &Group{timeout: 30 * time.Second}
	for _, o := range opts {
		o(g)
	}
	return g
}

// Register adds a named shutdown hook. Hooks run in reverse registration order
// (last registered = first to shut down).
func (g *Group) Register(name string, fn ShutdownFunc) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hooks = append(g.hooks, registration{name: name, fn: fn})
}

// Shutdown runs all registered hooks sequentially in reverse order,
// using a context with the configured timeout. Errors from individual
// hooks are collected and returned as a combined error.
func (g *Group) Shutdown(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()

	g.mu.Lock()
	hooks := make([]registration, len(g.hooks))
	copy(hooks, g.hooks)
	g.mu.Unlock()

	var errs []error
	for i := len(hooks) - 1; i >= 0; i-- {
		r := hooks[i]
		if err := r.fn(ctx); err != nil {
			errs = append(errs, fmt.Errorf("graceful: %s: %w", r.name, err))
		}
	}
	return errors.Join(errs...)
}

// ListenAndShutdown blocks until ctx is cancelled or SIGINT/SIGTERM is received,
// then calls Shutdown and returns its error.
func (g *Group) ListenAndShutdown(ctx context.Context) error {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)

	select {
	case <-ctx.Done():
	case <-quit:
	}

	return g.Shutdown(context.Background())
}
