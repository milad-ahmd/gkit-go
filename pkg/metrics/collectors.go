package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// ---- Pool collector -----------------------------------------------------

// PoolSnapshot is the data the pool collector exports per scrape.
type PoolSnapshot struct {
	Submitted  uint64
	Completed  uint64
	Errors     uint64
	InFlight   int64
	QueueDepth int
}

// PoolCollector is a prometheus.Collector that scrapes a worker pool.
// Use a snapshot function to decouple metric types from pool types.
//
//	collector := metrics.NewPoolCollector("order_pool", func() metrics.PoolSnapshot {
//	    s := orderPool.Stats()
//	    return metrics.PoolSnapshot{Submitted: s.Submitted, ...}
//	})
type PoolCollector struct {
	fn        func() PoolSnapshot
	submitted *prometheus.Desc
	completed *prometheus.Desc
	errors    *prometheus.Desc
	inFlight  *prometheus.Desc
	queueLen  *prometheus.Desc
}

// NewPoolCollector returns a Collector that calls fn on each Prometheus scrape.
// name labels the pool (e.g. "order_pool") in all exported metrics.
func NewPoolCollector(name string, fn func() PoolSnapshot) *PoolCollector {
	labels := prometheus.Labels{"pool": name}
	desc := func(metric, help string) *prometheus.Desc {
		return prometheus.NewDesc("gkit_pool_"+metric, help, nil, labels)
	}
	return &PoolCollector{
		fn:        fn,
		submitted: desc("jobs_submitted_total", "Total number of jobs submitted."),
		completed: desc("jobs_completed_total", "Total number of jobs completed."),
		errors:    desc("jobs_errors_total", "Total number of jobs that returned an error."),
		inFlight:  desc("jobs_in_flight", "Number of jobs currently being processed."),
		queueLen:  desc("queue_depth", "Number of jobs waiting in the queue buffer."),
	}
}

func (c *PoolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.submitted
	ch <- c.completed
	ch <- c.errors
	ch <- c.inFlight
	ch <- c.queueLen
}

func (c *PoolCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.fn()
	ch <- prometheus.MustNewConstMetric(c.submitted, prometheus.CounterValue, float64(s.Submitted))
	ch <- prometheus.MustNewConstMetric(c.completed, prometheus.CounterValue, float64(s.Completed))
	ch <- prometheus.MustNewConstMetric(c.errors, prometheus.CounterValue, float64(s.Errors))
	ch <- prometheus.MustNewConstMetric(c.inFlight, prometheus.GaugeValue, float64(s.InFlight))
	ch <- prometheus.MustNewConstMetric(c.queueLen, prometheus.GaugeValue, float64(s.QueueDepth))
}

// ---- Cache collector ----------------------------------------------------

// CacheSnapshot is the data the cache collector exports per scrape.
type CacheSnapshot struct {
	Hits   uint64
	Misses uint64
	Evicts uint64
	Len    int
}

// CacheCollector is a prometheus.Collector that scrapes a cache.
//
//	collector := metrics.NewCacheCollector("product_cache", func() metrics.CacheSnapshot {
//	    s := productCache.Stats()
//	    return metrics.CacheSnapshot{Hits: s.Hits, ...}
//	})
type CacheCollector struct {
	fn     func() CacheSnapshot
	hits   *prometheus.Desc
	misses *prometheus.Desc
	evicts *prometheus.Desc
	size   *prometheus.Desc
}

// NewCacheCollector returns a Collector that calls fn on each Prometheus scrape.
// name labels the cache (e.g. "product_cache") in all exported metrics.
func NewCacheCollector(name string, fn func() CacheSnapshot) *CacheCollector {
	labels := prometheus.Labels{"cache": name}
	desc := func(metric, help string) *prometheus.Desc {
		return prometheus.NewDesc("gkit_cache_"+metric, help, nil, labels)
	}
	return &CacheCollector{
		fn:     fn,
		hits:   desc("hits_total", "Total cache hits."),
		misses: desc("misses_total", "Total cache misses."),
		evicts: desc("evictions_total", "Total cache evictions (LRU or TTL)."),
		size:   desc("size", "Current number of items in the cache."),
	}
}

func (c *CacheCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.hits
	ch <- c.misses
	ch <- c.evicts
	ch <- c.size
}

func (c *CacheCollector) Collect(ch chan<- prometheus.Metric) {
	s := c.fn()
	ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(s.Hits))
	ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(s.Misses))
	ch <- prometheus.MustNewConstMetric(c.evicts, prometheus.CounterValue, float64(s.Evicts))
	ch <- prometheus.MustNewConstMetric(c.size, prometheus.GaugeValue, float64(s.Len))
}
