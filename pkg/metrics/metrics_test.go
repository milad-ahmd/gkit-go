package metrics_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/miladhzz/gkit/pkg/metrics"
)

func TestRegistry_HandlerExposesMetrics(t *testing.T) {
	reg := metrics.NewRegistry("test")

	counter := reg.NewCounter(metrics.CounterOpts{
		Name: "requests_total",
		Help: "Total requests.",
	})
	counter.WithLabelValues().Inc()
	counter.WithLabelValues().Inc()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	if !strings.Contains(body, "test_requests_total") {
		t.Errorf("expected 'test_requests_total' in metrics output, got:\n%s", body)
	}
}

func TestPoolCollector_Collect(t *testing.T) {
	reg := metrics.NewRegistry("test2")

	called := false
	reg.MustRegister(metrics.NewPoolCollector("my_pool", func() metrics.PoolSnapshot {
		called = true
		return metrics.PoolSnapshot{Submitted: 10, Completed: 9, Errors: 1, InFlight: 1, QueueDepth: 3}
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)

	if !called {
		t.Error("expected snapshot function to be called during scrape")
	}

	body := rec.Body.String()
	for _, want := range []string{
		"gkit_pool_jobs_submitted_total",
		"gkit_pool_jobs_completed_total",
		"gkit_pool_jobs_errors_total",
		"gkit_pool_jobs_in_flight",
		"gkit_pool_queue_depth",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected metric %q in output", want)
		}
	}
}

func TestCacheCollector_Collect(t *testing.T) {
	reg := metrics.NewRegistry("test3")

	reg.MustRegister(metrics.NewCacheCollector("my_cache", func() metrics.CacheSnapshot {
		return metrics.CacheSnapshot{Hits: 100, Misses: 20, Evicts: 5, Len: 75}
	}))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/metrics", nil)
	reg.Handler().ServeHTTP(rec, req)

	body := rec.Body.String()
	for _, want := range []string{
		"gkit_cache_hits_total",
		"gkit_cache_misses_total",
		"gkit_cache_evictions_total",
		"gkit_cache_size",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("expected metric %q in output", want)
		}
	}
}
