// Package integration exercises the full intake → Redis path in-process,
// standing in for the curl + live-Redis end-to-end check.
package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/redisstream"
	"github.com/port-labs/ocean-gateway/internal/server"
)

func TestEndToEndWebhookToStream(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	writer := redisstream.NewWriter(rdb, 0, time.Hour, time.Hour)
	h := server.NewHandler(writer, log, 2, time.Millisecond)
	redisPing := func(ctx context.Context) error { return rdb.Ping(ctx).Err() }
	ts := httptest.NewServer(server.New(h, redisPing, log))
	defer ts.Close()

	logIngestID := "pkhQJcQcUYfnTCHn"
	req, _ := http.NewRequest(http.MethodPost,
		ts.URL+"/live-events/"+logIngestID+"/integration/webhook",
		strings.NewReader(`{"hello":"world"}`))
	req.Header.Set("X-Event-Type", "issue_updated")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d want 202", resp.StatusCode)
	}

	// Synchronous write: the event must be in the stream immediately on 202.
	key := redisstream.StreamKey(logIngestID)
	entries, err := rdb.XRange(context.Background(), key, "-", "+").Result()
	if err != nil {
		t.Fatalf("xrange: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("stream %q got %d entries want 1", key, len(entries))
	}
	if entries[0].Values["payload"] != `{"hello":"world"}` {
		t.Fatalf("payload = %v", entries[0].Values["payload"])
	}
	var hdr map[string][]string
	if err := json.Unmarshal([]byte(entries[0].Values["headers"].(string)), &hdr); err != nil {
		t.Fatalf("headers not json: %v", err)
	}
	if got := hdr["X-Event-Type"]; len(got) != 1 || got[0] != "issue_updated" {
		t.Fatalf("X-Event-Type = %v", got)
	}

	// Stream key carries the idle TTL.
	if ttl := rdb.TTL(context.Background(), key).Val(); ttl <= 0 || ttl > time.Hour {
		t.Fatalf("stream TTL = %v, want (0, 1h]", ttl)
	}
}
