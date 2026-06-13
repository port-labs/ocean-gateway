package queue

import (
	"sync"
	"testing"

	"github.com/port-labs/ocean-gateway/internal/event"
)

func ev(payload string) *event.Event {
	return &event.Event{OrgID: "org", LiveEventsUUID: "uuid", Payload: []byte(payload)}
}

func TestEnqueueDequeueFIFO(t *testing.T) {
	q := New(1 << 20)
	for _, s := range []string{"a", "b", "c"} {
		if err := q.Enqueue(ev(s)); err != nil {
			t.Fatalf("enqueue %q: %v", s, err)
		}
	}
	for _, want := range []string{"a", "b", "c"} {
		got, err := q.Dequeue()
		if err != nil {
			t.Fatalf("dequeue: %v", err)
		}
		if string(got.Payload) != want {
			t.Fatalf("got %q want %q", got.Payload, want)
		}
	}
}

func TestEnqueueFullReturnsErrFull(t *testing.T) {
	one := ev("x")
	// Bound to just over a single event so the second enqueue overflows.
	q := New(int64(one.Size()) + 1)
	if err := q.Enqueue(ev("x")); err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if err := q.Enqueue(ev("y")); err != ErrFull {
		t.Fatalf("got %v want ErrFull", err)
	}
}

func TestDequeueFreesBytes(t *testing.T) {
	one := ev("x")
	q := New(int64(one.Size()) + 1)
	_ = q.Enqueue(ev("x"))
	if _, err := q.Dequeue(); err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if q.Bytes() != 0 {
		t.Fatalf("bytes = %d, want 0", q.Bytes())
	}
	if err := q.Enqueue(ev("z")); err != nil {
		t.Fatalf("enqueue after drain: %v", err)
	}
}

func TestOversizedEventAdmittedWhenEmpty(t *testing.T) {
	q := New(1) // smaller than any event
	if err := q.Enqueue(ev("big payload")); err != nil {
		t.Fatalf("oversized event into empty queue should be admitted: %v", err)
	}
}

func TestDequeueBatchGreedyAndFIFO(t *testing.T) {
	q := New(1 << 20)
	for _, s := range []string{"a", "b", "c", "d"} {
		_ = q.Enqueue(ev(s))
	}
	// max larger than buffered => takes all, in order.
	batch, err := q.DequeueBatch(10)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(batch) != 4 {
		t.Fatalf("len = %d want 4", len(batch))
	}
	for i, want := range []string{"a", "b", "c", "d"} {
		if string(batch[i].Payload) != want {
			t.Fatalf("batch[%d] = %q want %q", i, batch[i].Payload, want)
		}
	}
	if q.Bytes() != 0 {
		t.Fatalf("bytes = %d want 0", q.Bytes())
	}
}

func TestDequeueBatchCappedAtMax(t *testing.T) {
	q := New(1 << 20)
	for i := 0; i < 10; i++ {
		_ = q.Enqueue(ev("x"))
	}
	batch, err := q.DequeueBatch(3)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(batch) != 3 {
		t.Fatalf("len = %d want 3 (capped)", len(batch))
	}
	if q.Len() != 7 {
		t.Fatalf("remaining = %d want 7", q.Len())
	}
}

func TestDequeueBatchCloseUnblocks(t *testing.T) {
	q := New(1 << 20)
	done := make(chan error, 1)
	go func() {
		_, err := q.DequeueBatch(100)
		done <- err
	}()
	q.Close()
	if err := <-done; err != ErrClosed {
		t.Fatalf("got %v want ErrClosed", err)
	}
}

func TestCloseUnblocksDequeue(t *testing.T) {
	q := New(1 << 20)
	done := make(chan error, 1)
	go func() {
		_, err := q.Dequeue()
		done <- err
	}()
	q.Close()
	if err := <-done; err != ErrClosed {
		t.Fatalf("got %v want ErrClosed", err)
	}
}

func TestConcurrentProducersConsumers(t *testing.T) {
	q := New(1 << 30)
	const n = 1000
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i++ {
			for q.Enqueue(ev("p")) == ErrFull {
			}
		}
	}()

	got := make(chan int, 1)
	go func() {
		count := 0
		for count < n {
			if _, err := q.Dequeue(); err == ErrClosed {
				break
			}
			count++
		}
		got <- count
	}()

	wg.Wait()
	if c := <-got; c != n {
		t.Fatalf("consumed %d want %d", c, n)
	}
}
