// Package integration exercises the full intake → queue → worker → Redis path
// in-process, standing in for the curl + live-Redis end-to-end check.
package integration

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/cache"
	"github.com/port-labs/ocean-gateway/internal/port"
	"github.com/port-labs/ocean-gateway/internal/queue"
	"github.com/port-labs/ocean-gateway/internal/redisstream"
	"github.com/port-labs/ocean-gateway/internal/server"
	"github.com/port-labs/ocean-gateway/internal/worker"
)

type fixedResolver struct{ integ port.Integration }

func (f fixedResolver) GetIntegrationByLogIngestID(_ context.Context, _, _ string) (port.Integration, error) {
	return f.integ, nil
}

func TestEndToEndWebhookToStream(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer rdb.Close()

	q := queue.New(1 << 20)
	writer := redisstream.NewWriter(rdb, 0)
	pool := worker.New(q, writer, log, 4, 500, 3, time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	poolDone := make(chan struct{})
	go func() { pool.Run(ctx); close(poolDone) }()

	// Resolver returns the integration identity; orgId comes from the JWT.
	res := fixedResolver{integ: port.Integration{OrgID: "ignored", LiveEventsUUID: "BTcNKoxlO9tedphi"}}
	h := server.NewHandler(cache.New(), res, q, log, time.Hour)
	ts := httptest.NewServer(server.New(h, log))
	defer ts.Close()

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"orgId": "org_ukrSy0JXDGngBGUH"})
	signed, _ := tok.SignedString([]byte("secret"))

	req, _ := http.NewRequest(http.MethodPost,
		ts.URL+"/live-events/pkhQJcQcUYfnTCHn/integration/webhook",
		strings.NewReader(`{"hello":"world"}`))
	req.Header.Set("Authorization", "Bearer "+signed)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d want 202", resp.StatusCode)
	}

	// The worker should forward to the per-integration stream shortly.
	key := redisstream.StreamKey("org_ukrSy0JXDGngBGUH", "BTcNKoxlO9tedphi")
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

	cancel()
	q.Close()
	<-poolDone
}
