package store

import (
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
)

type options struct {
	maxConns int32
	minConns int32
	logger   *slog.Logger
	promReg  prometheus.Registerer
}

func defaultOptions() *options {
	return &options{
		logger: slog.Default(),
	}
}

// Option configures a DB.
type Option func(*options)

// WithMaxConns sets the maximum pool size (default: pgx default = 4).
func WithMaxConns(n int32) Option {
	return func(o *options) { o.maxConns = n }
}

// WithMinConns sets the minimum number of idle connections kept open.
func WithMinConns(n int32) Option {
	return func(o *options) { o.minConns = n }
}

// WithLogger sets the logger used for connection events.
func WithLogger(l *slog.Logger) Option {
	return func(o *options) { o.logger = l }
}

// WithPrometheus registers pool metrics into the given Prometheus registerer.
func WithPrometheus(reg prometheus.Registerer) Option {
	return func(o *options) { o.promReg = reg }
}
