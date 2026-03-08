package middleware

import (
	"context"
	"net/http"
	"time"
)

// Timeout cancels the request context after d. If the handler does not
// complete before d, subsequent writes return an error and the client receives
// a 503 Service Unavailable (if headers haven't been sent yet).
func Timeout(d time.Duration) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx, cancel := context.WithTimeout(r.Context(), d)
			defer cancel()
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// RateLimit returns a middleware that uses the provided allow function.
// When allow() returns false, the handler responds with 429 Too Many Requests.
func RateLimit(allow func(r *http.Request) bool) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !allow(r) {
				http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
