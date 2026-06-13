// Package metrics defines the Prometheus metrics exposed by the gateway.
// All metrics are registered against the default registry and served at /metrics.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Buckets tuned to the observed latency range of the gateway: most events
// complete in under 10ms, but queue drain under load can reach seconds.
var latencyBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10}

var (
	// EventsForwardedTotal counts events successfully written to Redis.
	EventsForwardedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_events_forwarded_total",
		Help: "Total number of events successfully written to a Redis stream.",
	})

	// EventsDroppedTotal counts events discarded after Redis retry exhaustion.
	EventsDroppedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_events_dropped_total",
		Help: "Total number of events dropped after exhausting Redis write retries.",
	})

	// QueueDequeuedTotal counts events pulled from the in-memory queue.
	// Use rate(gateway_queue_dequeued_total[1m]) to derive the handling rate.
	QueueDequeuedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gateway_queue_dequeued_total",
		Help: "Total number of events dequeued from the in-memory buffer (use rate() for handling rate).",
	})

	// QueueDelaySeconds measures how long each event waited in the in-memory
	// queue before being picked up by a worker.
	QueueDelaySeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_queue_delay_seconds",
		Help:    "Time events spend in the in-memory queue before a worker dequeues them.",
		Buckets: latencyBuckets,
	})

	// EventE2ESeconds measures the end-to-end time from HTTP intake to a
	// successful Redis XADD — i.e. queue wait + write latency.
	EventE2ESeconds = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "gateway_event_e2e_seconds",
		Help:    "End-to-end time from event ingestion (HTTP receive) to successful Redis XADD.",
		Buckets: latencyBuckets,
	})
)

// RegisterQueueDepth creates and registers a GaugeFunc that samples the current
// number of events in the in-memory queue at every Prometheus scrape. Using a
// GaugeFunc means zero overhead on the hot enqueue/dequeue path.
func RegisterQueueDepth(depthFn func() int) {
	promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "gateway_queue_depth_events",
		Help: "Current number of events buffered in the in-memory queue.",
	}, func() float64 {
		return float64(depthFn())
	})
}
