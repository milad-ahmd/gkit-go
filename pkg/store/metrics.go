package store

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type poolCollector struct {
	pool       *pgxpool.Pool
	totalConns *prometheus.Desc
	idleConns  *prometheus.Desc
	usedConns  *prometheus.Desc
	maxConns   *prometheus.Desc
	acquireOps *prometheus.Desc
}

func newPoolCollector(pool *pgxpool.Pool) *poolCollector {
	labels := prometheus.Labels{}
	return &poolCollector{
		pool: pool,
		totalConns: prometheus.NewDesc(
			"pgx_pool_total_connections",
			"Total number of connections in the pool.", nil, labels),
		idleConns: prometheus.NewDesc(
			"pgx_pool_idle_connections",
			"Number of idle connections in the pool.", nil, labels),
		usedConns: prometheus.NewDesc(
			"pgx_pool_used_connections",
			"Number of connections currently in use.", nil, labels),
		maxConns: prometheus.NewDesc(
			"pgx_pool_max_connections",
			"Maximum number of connections allowed.", nil, labels),
		acquireOps: prometheus.NewDesc(
			"pgx_pool_acquire_total",
			"Total number of connection acquire operations.", nil, labels),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.totalConns
	ch <- c.idleConns
	ch <- c.usedConns
	ch <- c.maxConns
	ch <- c.acquireOps
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	stat := c.pool.Stat()
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.GaugeValue, float64(stat.TotalConns()))
	ch <- prometheus.MustNewConstMetric(c.idleConns, prometheus.GaugeValue, float64(stat.IdleConns()))
	ch <- prometheus.MustNewConstMetric(c.usedConns, prometheus.GaugeValue, float64(stat.AcquiredConns()))
	ch <- prometheus.MustNewConstMetric(c.maxConns, prometheus.GaugeValue, float64(stat.MaxConns()))
	ch <- prometheus.MustNewConstMetric(c.acquireOps, prometheus.CounterValue, float64(stat.AcquireCount()))
}
