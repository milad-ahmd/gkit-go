package interceptors

import (
	"context"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPCMetrics holds the Prometheus instruments for gRPC server observability.
type RPCMetrics struct {
	requestsTotal    *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	requestsInFlight *prometheus.GaugeVec
}

// NewRPCMetrics creates and registers the gRPC metrics with the given registerer.
// Typical use: pass your application's prometheus.Registry.
func NewRPCMetrics(reg prometheus.Registerer) *RPCMetrics {
	m := &RPCMetrics{
		requestsTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "grpc_server_requests_total",
				Help: "Total number of gRPC requests completed, by method and code.",
			},
			[]string{"method", "code"},
		),
		requestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "grpc_server_request_duration_seconds",
				Help:    "Histogram of gRPC request latencies.",
				Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5},
			},
			[]string{"method"},
		),
		requestsInFlight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "grpc_server_requests_in_flight",
				Help: "Number of gRPC requests currently being processed.",
			},
			[]string{"method"},
		),
	}
	reg.MustRegister(m.requestsTotal, m.requestDuration, m.requestsInFlight)
	return m
}

// UnaryInterceptor returns a grpc.UnaryServerInterceptor that records metrics.
func (m *RPCMetrics) UnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		method := info.FullMethod
		m.requestsInFlight.WithLabelValues(method).Inc()
		start := time.Now()

		resp, err := handler(ctx, req)

		m.requestsInFlight.WithLabelValues(method).Dec()
		m.requestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())

		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		m.requestsTotal.WithLabelValues(method, code.String()).Inc()

		return resp, err
	}
}

// StreamInterceptor returns a grpc.StreamServerInterceptor that records metrics.
func (m *RPCMetrics) StreamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv any,
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		method := info.FullMethod
		m.requestsInFlight.WithLabelValues(method).Inc()
		start := time.Now()

		err := handler(srv, ss)

		m.requestsInFlight.WithLabelValues(method).Dec()
		m.requestDuration.WithLabelValues(method).Observe(time.Since(start).Seconds())

		code := codes.OK
		if err != nil {
			code = status.Code(err)
		}
		m.requestsTotal.WithLabelValues(method, code.String()).Inc()

		return err
	}
}
