package middleware

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// HTTPMetrics holds Prometheus instruments for HTTP server observability.
type HTTPMetrics struct {
	requestsTotal    *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	requestsInFlight *prometheus.GaugeVec
	responseBytes    *prometheus.CounterVec
}

// NewHTTPMetrics creates and registers HTTP metrics with the given registerer.
func NewHTTPMetrics(reg prometheus.Registerer) *HTTPMetrics {
	m := &HTTPMetrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_requests_total",
				Help: "Total HTTP requests, partitioned by method, path, and status.",
			},
			[]string{"method", "path", "status"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "http_request_duration_seconds",
				Help:    "HTTP request latency histogram.",
				Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10},
			},
			[]string{"method", "path"},
		),
		requestsInFlight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "http_requests_in_flight",
				Help: "Current number of HTTP requests being served.",
			},
			[]string{"method"},
		),
		responseBytes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "http_response_bytes_total",
				Help: "Total bytes sent in HTTP responses.",
			},
			[]string{"method"},
		),
	}
	reg.MustRegister(m.requestsTotal, m.requestDuration, m.requestsInFlight, m.responseBytes)
	return m
}

// Middleware returns an HTTP middleware that records metrics.
// path should be a template like "/products/{id}" to avoid high cardinality.
// Use r.Pattern (Go 1.22+ ServeMux) or a route variable from your router.
func (m *HTTPMetrics) Middleware() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			method := r.Method
			m.requestsInFlight.WithLabelValues(method).Inc()
			start := time.Now()

			wrapped := wrapResponseWriter(w)
			next.ServeHTTP(wrapped, r)

			dur := time.Since(start).Seconds()
			status := strconv.Itoa(wrapped.status)
			// Use the matched pattern if available (Go 1.22+ net/http).
			path := r.Pattern
			if path == "" {
				path = r.URL.Path
			}

			m.requestsInFlight.WithLabelValues(method).Dec()
			m.requestsTotal.WithLabelValues(method, path, status).Inc()
			m.requestDuration.WithLabelValues(method, path).Observe(dur)
			m.responseBytes.WithLabelValues(method).Add(float64(wrapped.bytes))
		})
	}
}
