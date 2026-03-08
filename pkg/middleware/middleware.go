// Package middleware provides composable HTTP middleware for production services.
//
// Use Chain to build a middleware stack:
//
//	handler := middleware.Chain(
//	    middleware.RequestID(),
//	    middleware.Logging(logger),
//	    middleware.Recovery(logger),
//	    middleware.Metrics(reg),
//	    middleware.Timeout(5*time.Second),
//	)(myHandler)
package middleware

import "net/http"

// Middleware wraps an http.Handler.
type Middleware func(http.Handler) http.Handler

// Chain builds a single Middleware that applies each middleware in order.
// The first middleware is the outermost (applied first on the way in,
// last on the way out).
func Chain(middlewares ...Middleware) Middleware {
	return func(next http.Handler) http.Handler {
		for i := len(middlewares) - 1; i >= 0; i-- {
			next = middlewares[i](next)
		}
		return next
	}
}

// Apply wraps handler with all middlewares (convenience shorthand).
func Apply(handler http.Handler, middlewares ...Middleware) http.Handler {
	return Chain(middlewares...)(handler)
}
