// Package metrics defines the Prometheus metrics exposed by the gateway.
// All metrics are registered against the default registry and served at /metrics.
package metrics

import (
	"runtime"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Buckets tuned to the gateway's synchronous write path: most XADDs complete in
// well under 10ms, with retries/backoff pushing the tail toward seconds.
var latencyBuckets = []float64{.0005, .001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5}

var (
	// EventsForwardedTotal counts events successfully written to Redis.
	EventsForwardedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_events_forwarded_total",
		Help: "Total number of events successfully written to a Redis stream.",
	})

	// EventsFailedTotal counts events that could not be written to Redis after
	// exhausting retries (the caller received a 503 and should retry).
	EventsFailedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_events_failed_total",
		Help: "Total number of events that failed to write to Redis after retries.",
	})

	// InFlightRequests is the number of webhook requests currently being served
	// (i.e. blocked on a Redis write). Sustained values near REDIS_POOL_SIZE
	// indicate the write path is the bottleneck.
	InFlightRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_inflight_requests",
		Help: "Number of webhook requests currently being served (blocked on a Redis write).",
	})

	// RedisWriteSeconds measures the duration of the Redis write (XADD
	// round-trip, including any retries), isolating Redis latency from handler
	// overhead.
	RedisWriteSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_redis_write_seconds",
		Help:    "Duration of the Redis write (XADD round-trip, including retries).",
		Buckets: latencyBuckets,
	})

	// EventE2ESeconds measures the end-to-end time from HTTP intake to a
	// successful Redis write — the full handler latency the producer observes.
	EventE2ESeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_event_e2e_seconds",
		Help:    "End-to-end time from event ingestion (HTTP receive) to successful Redis write.",
		Buckets: latencyBuckets,
	})

	// HTTPRequestsTotal counts all HTTP requests by method, route, and status.
	HTTPRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gateway_http_requests_total",
		Help: "Total HTTP requests by method, route, and response status code.",
	}, []string{"method", "route", "status"})

	// HTTPRequestDuration measures HTTP request latency by method, route, and
	// status — including failed requests so error latency is visible.
	HTTPRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "gateway_http_request_duration_seconds",
		Help:    "HTTP request latency by method, route, and status (includes failed requests).",
		Buckets: latencyBuckets,
	}, []string{"method", "route", "status"})
)

// PoolStats is the subset of go-redis PoolStats we expose.
type PoolStats struct {
	Hits       uint32
	Misses     uint32
	Timeouts   uint32
	TotalConns uint32
	IdleConns  uint32
	StaleConns uint32
}

// PoolStatsFunc returns current Redis connection pool statistics.
type PoolStatsFunc func() PoolStats

// RegisterBuildInfo registers a gauge that always equals 1 and carries version
// labels — the standard way to expose build metadata in Prometheus.
func RegisterBuildInfo(version, commit, date string) {
	promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "gateway_build_info",
		Help: "Build information (always 1). Use labels to identify the deployed version.",
	}, []string{"version", "commit", "date", "go_version"}).With(prometheus.Labels{
		"version":    version,
		"commit":     commit,
		"date":       date,
		"go_version": runtime.Version(),
	}).Set(1)
}

// RegisterRedisPool registers per-scrape gauges sourced from the Redis
// connection pool statistics. Each is sampled at scrape time via GaugeFunc.
func RegisterRedisPool(fn PoolStatsFunc) {
	gauge := func(name, help string, f func(PoolStats) float64) {
		promauto.NewGaugeFunc(prometheus.GaugeOpts{Name: name, Help: help},
			func() float64 { return f(fn()) })
	}
	gauge("gateway_redis_pool_total_conns", "Total connections in the Redis pool.", func(s PoolStats) float64 { return float64(s.TotalConns) })
	gauge("gateway_redis_pool_idle_conns", "Idle connections in the Redis pool.", func(s PoolStats) float64 { return float64(s.IdleConns) })
	gauge("gateway_redis_pool_stale_conns", "Stale connections removed from the Redis pool.", func(s PoolStats) float64 { return float64(s.StaleConns) })
	gauge("gateway_redis_pool_hits_total", "Cumulative pool hits (free connection found).", func(s PoolStats) float64 { return float64(s.Hits) })
	gauge("gateway_redis_pool_misses_total", "Cumulative pool misses (no free connection).", func(s PoolStats) float64 { return float64(s.Misses) })
	gauge("gateway_redis_pool_timeouts_total", "Cumulative pool wait timeouts.", func(s PoolStats) float64 { return float64(s.Timeouts) })
}
