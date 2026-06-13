// Package integration exercises the full intake → queue → worker → Redis path
// in-process, standing in for the curl + live-Redis end-to-end check.
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

	"github.com/port-labs/ocean-gateway/internal/queue"
	"github.com/port-labs/ocean-gateway/internal/redisstream"
	"github.com/port-labs/ocean-gateway/internal/server"
	"github.com/port-labs/ocean-gateway/internal/worker"
)

func TestEndToEndWebhookToStream(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	q := queue.New(1 << 20)
	writer := redisstream.NewWriter(rdb, 0, time.Hour, time.Hour)
	pool := worker.New(q, writer, log, 4, 500, 3, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() { pool.Run(ctx); close(poolDone) }()

	h := server.NewHandler(q, log)
	ts := httptest.NewServer(server.New(h, log))
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

	// The worker should forward to the stream keyed by logIngestId shortly.
	key := redisstream.StreamKey(logIngestID)
	var entries []redis.XMessage
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ = rdb.XRange(context.Background(), key, "-", "+").Result()
		if len(entries) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(entries) != 1 {
		t.Fatalf("stream %q got %d entries want 1", key, len(entries))
	}
	if entries[0].Values["payload"] != `{"hello":"world"}` {
		t.Fatalf("payload = %v", entries[0].Values["payload"])
	}
	// Headers captured as a JSON object including the custom header.
	var hdr map[string][]string
	if err := json.Unmarshal([]byte(entries[0].Values["headers"].(string)), &hdr); err != nil {
		t.Fatalf("headers not json: %v", err)
	}
	if got := hdr["X-Event-Type"]; len(got) != 1 || got[0] != "issue_updated" {
		t.Fatalf("X-Event-Type header = %v", got)
	}

	cancel()
	q.Close()
	<-poolDone
}
