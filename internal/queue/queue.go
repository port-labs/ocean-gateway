// Package queue implements a FIFO, memory-bounded in-process event queue.
//
// The bound is on the approximate byte footprint of buffered events (not their
// count). When a new event would exceed the bound, Enqueue returns ErrFull so
// the caller can apply backpressure (the HTTP intake responds 503). Workers
// block in Dequeue until an event is available or the queue is closed.
package queue

import (
	"container/list"
	"errors"
	"sync"

	"github.com/port-labs/ocean-gateway/internal/event"
)

// ErrFull is returned by Enqueue when the queue is at its memory bound.
var ErrFull = errors.New("queue full")

// ErrClosed is returned by Dequeue once the queue is closed and drained.
var ErrClosed = errors.New("queue closed")

// Queue is a goroutine-safe, memory-bounded FIFO of events.
type Queue struct {
	mu       sync.Mutex
	cond     *sync.Cond
	items    *list.List
	curBytes int64
	maxBytes int64
	closed   bool
}

// New returns a queue bounded to maxBytes of buffered event payload.
func New(maxBytes int64) *Queue {
	q := &Queue{
		items:    list.New(),
		maxBytes: maxBytes,
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Enqueue appends e unless doing so would exceed the memory bound, in which
// case it returns ErrFull. An always-admit policy is applied to an empty queue
// so that a single oversized event is never permanently rejected.
func (q *Queue) Enqueue(e *event.Event) error {
	sz := int64(e.Size())
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrClosed
	}
	if q.curBytes > 0 && q.curBytes+sz > q.maxBytes {
		return ErrFull
	}
	q.items.PushBack(e)
	q.curBytes += sz
	q.cond.Signal()
	return nil
}

// Dequeue removes and returns the oldest event, blocking while the queue is
// empty. It returns ErrClosed once the queue is closed and fully drained.
func (q *Queue) Dequeue() (*event.Event, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.items.Len() == 0 {
		if q.closed {
			return nil, ErrClosed
		}
		q.cond.Wait()
	}
	front := q.items.Front()
	q.items.Remove(front)
	e := front.Value.(*event.Event)
	q.curBytes -= int64(e.Size())
	return e, nil
}

// DequeueBatch removes and returns up to max events, blocking until at least
// one is available. It greedily takes everything currently buffered (capped at
// max) in a single lock acquisition, so batch size self-tunes to load: 1 under
// light load, up to max under a burst. It returns ErrClosed once the queue is
// closed and fully drained.
func (q *Queue) DequeueBatch(max int) ([]*event.Event, error) {
	if max < 1 {
		max = 1
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.items.Len() == 0 {
		if q.closed {
			return nil, ErrClosed
		}
		q.cond.Wait()
	}
	n := q.items.Len()
	if n > max {
		n = max
	}
	out := make([]*event.Event, 0, n)
	for i := 0; i < n; i++ {
		front := q.items.Front()
		q.items.Remove(front)
		e := front.Value.(*event.Event)
		q.curBytes -= int64(e.Size())
		out = append(out, e)
	}
	return out, nil
}

// Len returns the number of buffered events.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.items.Len()
}

// Bytes returns the approximate buffered byte count.
func (q *Queue) Bytes() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.curBytes
}

// Close marks the queue closed and wakes all blocked consumers. Already-queued
// events remain available to Dequeue until drained.
func (q *Queue) Close() {
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	q.cond.Broadcast()
}
