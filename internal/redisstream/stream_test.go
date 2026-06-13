package redisstream

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/event"
)

func TestStreamKey(t *testing.T) {
	got := StreamKey("org_1", "uuid_2")
	want := "org_1/uuid_2/live-events/raw/event-stream"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestAddWritesToStream(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	w := NewWriter(rdb, 0)
	ctx := context.Background()
	if err := w.Add(ctx, "org_1", "uuid_2", []byte(`{"hello":"world"}`)); err != nil {
		t.Fatalf("add: %v", err)
	}

	key := StreamKey("org_1", "uuid_2")
	entries, err := rdb.XRange(ctx, key, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries want 1", len(entries))
	}
	if got := entries[0].Values[payloadField]; got != `{"hello":"world"}` {
		t.Fatalf("payload = %v", got)
	}
}

func TestAddBatchFansOutToStreams(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	w := NewWriter(rdb, 0)
	ctx := context.Background()

	// Two orgs share liveEventsUuid "u"; events should fan out by org.
	events := []*event.Event{
		{OrgID: "org_a", LiveEventsUUID: "u", Payload: []byte("a1")},
		{OrgID: "org_b", LiveEventsUUID: "u", Payload: []byte("b1")},
		{OrgID: "org_a", LiveEventsUUID: "u", Payload: []byte("a2")},
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

	if n := rdb.XLen(ctx, StreamKey("org_a", "u")).Val(); n != 2 {
		t.Fatalf("org_a stream len = %d want 2", n)
	}
	if n := rdb.XLen(ctx, StreamKey("org_b", "u")).Val(); n != 1 {
		t.Fatalf("org_b stream len = %d want 1", n)
	}
}

func TestAddBatchEmpty(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()
	if errs := NewWriter(rdb, 0).AddBatch(context.Background(), nil); errs != nil {
		t.Fatalf("empty batch errs = %v want nil", errs)
	}
}
