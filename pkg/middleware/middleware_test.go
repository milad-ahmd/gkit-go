package middleware_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/milad-ahmd/gkit-go/pkg/middleware"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) //nolint:errcheck
	})
}

func TestChain_AppliesInOrder(t *testing.T) {
	var order []string
	mk := func(label string) middleware.Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, label+"-in")
				next.ServeHTTP(w, r)
				order = append(order, label+"-out")
			})
		}
	}

	h := middleware.Chain(mk("A"), mk("B"), mk("C"))(okHandler())
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	want := []string{"A-in", "B-in", "C-in", "C-out", "B-out", "A-out"}
	for i, got := range order {
		if got != want[i] {
			t.Errorf("order[%d]: got %q, want %q", i, got, want[i])
		}
	}
}

func TestRequestID_SetsHeader(t *testing.T) {
	h := middleware.Apply(okHandler(), middleware.RequestID())
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if id := rec.Header().Get(middleware.RequestIDHeader); id == "" {
		t.Error("expected X-Request-Id header to be set")
	}
}

func TestRequestID_PropagatesExisting(t *testing.T) {
	h := middleware.Apply(okHandler(), middleware.RequestID())
	req := httptest.NewRequest("GET", "/", nil)
	req.Header.Set(middleware.RequestIDHeader, "test-id-123")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if got := rec.Header().Get(middleware.RequestIDHeader); got != "test-id-123" {
		t.Errorf("expected propagated request ID, got %q", got)
	}
}

func TestRequestID_InContext(t *testing.T) {
	var capturedID string
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedID = middleware.RequestIDFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	h := middleware.Apply(inner, middleware.RequestID())
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	if capturedID == "" {
		t.Error("expected request ID in context")
	}
}

func TestRecovery_CatchesPanic(t *testing.T) {
	panicking := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic("something bad happened")
	})

	h := middleware.Apply(panicking, middleware.Recovery(slog.Default()))
	rec := httptest.NewRecorder()
	// Should not panic.
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestLogging_WritesOutput(t *testing.T) {
	var buf strings.Builder
	logger := slog.New(slog.NewTextHandler(&buf, nil))

	h := middleware.Apply(okHandler(), middleware.Logging(logger))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/test", nil))

	if !strings.Contains(buf.String(), "http request") {
		t.Errorf("expected 'http request' in log output, got: %s", buf.String())
	}
}

func TestTimeout_CancelsContext(t *testing.T) {
	var ctxCancelled bool
	slow := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
		ctxCancelled = true
	})

	h := middleware.Apply(slow, middleware.Timeout(1)) // 1 nanosecond
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/", nil))

	if !ctxCancelled {
		t.Error("expected context to be cancelled by Timeout middleware")
	}
}

func TestRateLimit_DeniesWhenLimited(t *testing.T) {
	allow := false
	h := middleware.Apply(okHandler(), middleware.RateLimit(func(_ *http.Request) bool {
		return allow
	}))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if rec.Code != http.StatusTooManyRequests {
		t.Errorf("expected 429, got %d", rec.Code)
	}
}

func TestRateLimit_AllowsWhenOK(t *testing.T) {
	h := middleware.Apply(okHandler(), middleware.RateLimit(func(_ *http.Request) bool { return true }))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}
