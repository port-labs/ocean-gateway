// Package worker drains the in-memory queue and forwards events to Redis
// streams, retrying transient failures and dropping events that persistently
// fail so the queue cannot clog during a Redis outage.
package worker

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/port-labs/ocean-gateway/internal/event"
	"github.com/port-labs/ocean-gateway/internal/queue"
)

// Forwarder appends a batch of events to their target streams in one
// round-trip, returning a per-event error slice aligned with events (nil =
// success) so the caller can retry only the failures.
type Forwarder interface {
	AddBatch(ctx context.Context, events []*event.Event) []error
}

// Pool consumes from a queue with a fixed number of worker goroutines, draining
// events in batches and forwarding each batch in a single pipelined round-trip.
type Pool struct {
	q          *queue.Queue
	fwd        Forwarder
	log        *slog.Logger
	workers    int
	batchSize  int
	maxRetries int
	backoff    time.Duration

	dropped atomic.Int64
}

// New builds a worker pool. batchSize caps how many events a worker drains per
// pipelined round-trip.
func New(q *queue.Queue, fwd Forwarder, log *slog.Logger, workers, batchSize, maxRetries int, backoff time.Duration) *Pool {
	if workers < 1 {
		workers = 1
	}
	if batchSize < 1 {
		batchSize = 1
	}
	return &Pool{
		q:          q,
		fwd:        fwd,
		log:        log,
		workers:    workers,
		batchSize:  batchSize,
		maxRetries: maxRetries,
		backoff:    backoff,
	}
}

// Run starts the workers and blocks until the queue is closed and drained.
// Cancelling ctx stops the retry loops promptly.
func (p *Pool) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.loop(ctx)
		}()
	}
	wg.Wait()
}

func (p *Pool) loop(ctx context.Context) {
	for {
		batch, err := p.q.DequeueBatch(p.batchSize)
		if err == queue.ErrClosed {
			return
		}
		p.forwardBatch(ctx, batch)
	}
}

// forwardBatch pipelines the batch with exponential backoff, retrying only the
// events that fail and dropping the stragglers once retries are exhausted.
func (p *Pool) forwardBatch(ctx context.Context, batch []*event.Event) {
	pending := batch
	delay := p.backoff
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		errs := p.fwd.AddBatch(ctx, pending)
		var failed []*event.Event
		for i, err := range errs {
			if err != nil {
				failed = append(failed, pending[i])
			}
		}
		if len(failed) == 0 {
			return
		}
		if attempt == p.maxRetries {
			p.dropped.Add(int64(len(failed)))
			p.log.Error("dropping events after retries exhausted",
				"count", len(failed),
				"attempts", attempt+1,
				"dropped_total", p.dropped.Load(),
				"sample_orgId", failed[0].OrgID,
				"sample_liveEventsUuid", failed[0].LiveEventsUUID,
			)
			return
		}
		pending = failed
		select {
		case <-ctx.Done():
			p.dropped.Add(int64(len(pending)))
			return
		case <-time.After(delay):
			delay *= 2
		}
	}
}

// Dropped returns the total number of events dropped after exhausting retries.
func (p *Pool) Dropped() int64 {
	return p.dropped.Load()
}
