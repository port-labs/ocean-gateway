package cache

import (
	"testing"
	"time"
)

func TestSetGetHit(t *testing.T) {
	c := New()
	c.Set("k", Value{LiveEventsUUID: "u", OrgID: "o"}, time.Hour)
	got, ok := c.Get("k")
	if !ok {
		t.Fatal("expected hit")
	}
	if got.LiveEventsUUID != "u" || got.OrgID != "o" {
		t.Fatalf("got %+v", got)
	}
}

func TestGetMiss(t *testing.T) {
	c := New()
	if _, ok := c.Get("nope"); ok {
		t.Fatal("expected miss")
	}
}

func TestExpiry(t *testing.T) {
	c := New()
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }
	c.Set("k", Value{LiveEventsUUID: "u"}, time.Hour)

	now = now.Add(59 * time.Minute)
	if _, ok := c.Get("k"); !ok {
		t.Fatal("should still be valid before TTL")
	}
	now = now.Add(2 * time.Minute) // past TTL
	if _, ok := c.Get("k"); ok {
		t.Fatal("should be expired after TTL")
	}
}
