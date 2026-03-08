// Package metrics provides Prometheus integration helpers for gkit components.
//
// It wraps prometheus.Registry to give you a single place to register all
// application metrics, exposes a ready-to-use HTTP handler for /metrics, and
// ships typed constructors for the four most common metric kinds.
//
// Additionally, PoolCollector and CacheCollector implement prometheus.Collector
// so pool and cache instances can be registered directly:
//
//	reg := metrics.NewRegistry("myapp")
//	reg.MustRegister(metrics.NewPoolCollector("order_pool", orderPool))
//	reg.MustRegister(metrics.NewCacheCollector("product_cache", productCache))
//	http.Handle("/metrics", reg.Handler())
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Registry wraps a prometheus.Registry with a namespace for consistent labelling.
type Registry struct {
	r         *prometheus.Registry
	namespace string
}

// NewRegistry creates a Registry that prefixes all metric names with namespace.
// It pre-registers the standard Go and process collectors.
func NewRegistry(namespace string) *Registry {
	r := prometheus.NewRegistry()
	r.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return &Registry{r: r, namespace: namespace}
}

// MustRegister registers one or more collectors, panicking on error.
func (reg *Registry) MustRegister(cs ...prometheus.Collector) {
	reg.r.MustRegister(cs...)
}

// Register registers one or more collectors, returning any error.
func (reg *Registry) Register(c prometheus.Collector) error {
	return reg.r.Register(c)
}

// Registry returns the underlying prometheus.Registerer, useful when passing
// to libraries that accept a prometheus.Registerer directly (e.g. interceptors).
func (reg *Registry) Registry() prometheus.Registerer { return reg.r }

// Handler returns an HTTP handler that serves the /metrics endpoint.
func (reg *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(reg.r, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// ---- typed metric constructors ------------------------------------------

// CounterOpts configures a counter metric.
type CounterOpts struct {
	Name   string
	Help   string
	Labels []string
}

// NewCounter creates and registers a *prometheus.CounterVec.
func (reg *Registry) NewCounter(o CounterOpts) *prometheus.CounterVec {
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: reg.namespace,
		Name:      o.Name,
		Help:      o.Help,
	}, o.Labels)
	reg.r.MustRegister(c)
	return c
}

// GaugeOpts configures a gauge metric.
type GaugeOpts struct {
	Name   string
	Help   string
	Labels []string
}

// NewGauge creates and registers a *prometheus.GaugeVec.
func (reg *Registry) NewGauge(o GaugeOpts) *prometheus.GaugeVec {
	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: reg.namespace,
		Name:      o.Name,
		Help:      o.Help,
	}, o.Labels)
	reg.r.MustRegister(g)
	return g
}

// HistogramOpts configures a histogram metric.
type HistogramOpts struct {
	Name    string
	Help    string
	Labels  []string
	Buckets []float64
}

// NewHistogram creates and registers a *prometheus.HistogramVec.
func (reg *Registry) NewHistogram(o HistogramOpts) *prometheus.HistogramVec {
	buckets := o.Buckets
	if len(buckets) == 0 {
		buckets = prometheus.DefBuckets
	}
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: reg.namespace,
		Name:      o.Name,
		Help:      o.Help,
		Buckets:   buckets,
	}, o.Labels)
	reg.r.MustRegister(h)
	return h
}
