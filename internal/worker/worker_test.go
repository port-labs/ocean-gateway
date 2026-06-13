package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/port-labs/ocean-gateway/internal/event"
	"github.com/port-labs/ocean-gateway/internal/queue"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// stubForwarder fails every event for the first failCalls AddBatch invocations,
// then succeeds.
type stubForwarder struct {
	mu        sync.Mutex
	calls     int
	failCalls int
	succeeded atomic.Int64
}

func (s *stubForwarder) AddBatch(_ context.Context, events []*event.Event) []error {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	errs := make([]error, len(events))
	if n <= s.failCalls {
		for i := range errs {
			errs[i] = errors.New("transient")
		}
		return errs
	}
	s.succeeded.Add(int64(len(events)))
	return errs
}

func runPool(t *testing.T, q *queue.Queue, p *Pool) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		p.Run(context.Background())
		close(done)
	}()
	// Give workers time to drain, then close and wait.
	time.Sleep(50 * time.Millisecond)
	q.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("pool did not stop")
	}
}

func TestForwardSucceedsAfterRetry(t *testing.T) {
	q := queue.New(1 << 20)
	fwd := &stubForwarder{failCalls: 2} // fail twice, succeed on 3rd
	p := New(q, fwd, quietLogger(), 1, 1, 3, time.Millisecond)
	_ = q.Enqueue(&event.Event{LogIngestID: "log", Payload: []byte("x")})
	runPool(t, q, p)

	if fwd.succeeded.Load() != 1 {
		t.Fatalf("succeeded = %d want 1", fwd.succeeded.Load())
	}
	if p.Dropped() != 0 {
		t.Fatalf("dropped = %d want 0", p.Dropped())
	}
}

func TestForwardDropsAfterRetriesExhausted(t *testing.T) {
	q := queue.New(1 << 20)
	fwd := &stubForwarder{failCalls: 1000} // always fails
	p := New(q, fwd, quietLogger(), 1, 1, 2, time.Millisecond)
	_ = q.Enqueue(&event.Event{LogIngestID: "log", Payload: []byte("x")})
	runPool(t, q, p)

	if p.Dropped() != 1 {
		t.Fatalf("dropped = %d want 1", p.Dropped())
	}
}

// recordingForwarder records every event it receives across batches.
type recordingForwarder struct {
	mu       sync.Mutex
	received int
	maxBatch int
	batches  int
}

func (r *recordingForwarder) AddBatch(_ context.Context, events []*event.Event) []error {
	r.mu.Lock()
	r.received += len(events)
	r.batches++
	if len(events) > r.maxBatch {
		r.maxBatch = len(events)
	}
	r.mu.Unlock()
	return make([]error, len(events))
}

func TestBatchDrainsAllEvents(t *testing.T) {
	q := queue.New(1 << 30)
	const n = 5000
	for i := 0; i < n; i++ {
		_ = q.Enqueue(&event.Event{LogIngestID: "log", Payload: []byte("x")})
	}
	fwd := &recordingForwarder{}
	p := New(q, fwd, quietLogger(), 1, 500, 3, time.Millisecond)
	runPool(t, q, p)

	if fwd.received != n {
		t.Fatalf("received = %d want %d", fwd.received, n)
	}
	// With 5000 pre-queued events and batchSize 500, batches should be large.
	if fwd.maxBatch <= 1 {
		t.Fatalf("expected batched draining, maxBatch = %d", fwd.maxBatch)
	}
	if fwd.batches >= n {
		t.Fatalf("expected far fewer batches than events, got %d batches for %d events", fwd.batches, n)
	}
}
