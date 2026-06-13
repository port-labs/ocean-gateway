package redisstream

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/event"
)

func TestStreamKey(t *testing.T) {
	want := "log_abc/live-events/raw/event-stream"
	if got := StreamKey("log_abc"); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestAddWritesPayloadAndHeaders(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	w := NewWriter(rdb, 0, 0, 0)
	ctx := context.Background()
	e := &event.Event{
		LogIngestID: "log_1",
		Payload:     []byte(`{"hello":"world"}`),
		Headers:     []byte(`{"X-Event-Type":["issue_updated"]}`),
	}
	if err := w.Add(ctx, e); err != nil {
		t.Fatalf("add: %v", err)
	}

	entries, err := rdb.XRange(ctx, StreamKey("log_1"), "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries want 1", len(entries))
	}
	if got := entries[0].Values[payloadField]; got != `{"hello":"world"}` {
		t.Fatalf("payload = %v", got)
	}
	if got := entries[0].Values[headersField]; got != `{"X-Event-Type":["issue_updated"]}` {
		t.Fatalf("headers = %v", got)
	}
}

func TestAddBatchFansOutByLogIngestID(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	w := NewWriter(rdb, 0, 0, 0)
	ctx := context.Background()

	events := []*event.Event{
		{LogIngestID: "log_a", Payload: []byte("a1")},
		{LogIngestID: "log_b", Payload: []byte("b1")},
		{LogIngestID: "log_a", Payload: []byte("a2")},
	}
	errs := w.AddBatch(ctx, events)
	if len(errs) != 3 {
		t.Fatalf("errs len = %d want 3", len(errs))
	}
	for i, e := range errs {
		if e != nil {
			t.Fatalf("event %d errored: %v", i, e)
		}
	}

	if n := rdb.XLen(ctx, StreamKey("log_a")).Val(); n != 2 {
		t.Fatalf("log_a stream len = %d want 2", n)
	}
	if n := rdb.XLen(ctx, StreamKey("log_b")).Val(); n != 1 {
		t.Fatalf("log_b stream len = %d want 1", n)
	}
}

func TestAddBatchEmpty(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	if errs := NewWriter(rdb, 0, 0, 0).AddBatch(context.Background(), nil); errs != nil {
		t.Fatalf("empty batch errs = %v want nil", errs)
	}
}

func TestStreamIdleTTLExpiresKey(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	ctx := context.Background()

	w := NewWriter(rdb, 0, 0, time.Hour) // streamTTL 1h, no event trim
	if err := w.Add(ctx, &event.Event{LogIngestID: "log_ttl", Payload: []byte("x")}); err != nil {
		t.Fatalf("add: %v", err)
	}

	// Key exists with a TTL near 1h.
	ttl := rdb.TTL(ctx, StreamKey("log_ttl")).Val()
	if ttl <= 0 || ttl > time.Hour {
		t.Fatalf("ttl = %v, want (0, 1h]", ttl)
	}

	// After the idle window with no further writes, the key is gone.
	mr.FastForward(time.Hour + time.Minute)
	if n := rdb.Exists(ctx, StreamKey("log_ttl")).Val(); n != 0 {
		t.Fatalf("stream key still exists after idle TTL (exists=%d)", n)
	}
}

func TestStreamIdleTTLRefreshedOnWrite(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	ctx := context.Background()

	w := NewWriter(rdb, 0, 0, time.Hour)
	key := StreamKey("log_active")
	if err := w.Add(ctx, &event.Event{LogIngestID: "log_active", Payload: []byte("1")}); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Advance most of the window, then write again — TTL should reset.
	mr.FastForward(50 * time.Minute)
	if err := w.Add(ctx, &event.Event{LogIngestID: "log_active", Payload: []byte("2")}); err != nil {
		t.Fatalf("add: %v", err)
	}
	// Past the original window but within the refreshed one: still alive.
	mr.FastForward(20 * time.Minute)
	if n := rdb.Exists(ctx, key).Val(); n != 1 {
		t.Fatalf("active stream expired despite refresh (exists=%d)", n)
	}
	if n := rdb.XLen(ctx, key).Val(); n != 2 {
		t.Fatalf("xlen = %d want 2", n)
	}
}

func TestEventTTLTrimsOldEntries(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	ctx := context.Background()
	key := StreamKey("log_age")

	// Seed an ancient entry with an explicit tiny id (1ms since epoch). Using an
	// explicit id keeps the test independent of the server's auto-id clock.
	if err := rdb.XAdd(ctx, &redis.XAddArgs{Stream: key, ID: "1-0", Values: map[string]any{payloadField: "old"}}).Err(); err != nil {
		t.Fatalf("seed old entry: %v", err)
	}

	// A normal write with a 1h event TTL sets MINID to (now-1h) in ms — far
	// above the ancient id — so the old entry is trimmed and only the new stays.
	w := NewWriter(rdb, 0, time.Hour, 0)
	if err := w.Add(ctx, &event.Event{LogIngestID: "log_age", Payload: []byte("new")}); err != nil {
		t.Fatalf("add new: %v", err)
	}

	entries, err := rdb.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(entries) != 1 || entries[0].Values[payloadField] != "new" {
		t.Fatalf("remaining entries = %v, want only 'new' (old should be trimmed)", entries)
	}
}
