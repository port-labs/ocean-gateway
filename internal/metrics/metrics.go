// Package metrics defines the Prometheus metrics exposed by the gateway.
// All metrics are registered against the default registry and served at /metrics.
package metrics

import (
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
	// (i.e. blocked on a Redis write). It is the key saturation signal now that
	// writes are synchronous — sustained values near the Redis pool size mean
	// the write path is the bottleneck.
	InFlightRequests = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "gateway_inflight_requests",
		Help: "Number of webhook requests currently being served (blocked on a Redis write).",
	})

	// RedisWriteSeconds measures the duration of the Redis write itself (the
	// XADD round-trip, including any retries), isolating Redis latency from
	// handler overhead.
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
)
