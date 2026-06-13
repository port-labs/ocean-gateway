package server

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/port-labs/ocean-gateway/internal/cache"
	"github.com/port-labs/ocean-gateway/internal/port"
	"github.com/port-labs/ocean-gateway/internal/queue"
)

type stubResolver struct {
	integ port.Integration
	err   error
	calls int
}

func (s *stubResolver) GetIntegrationByLogIngestID(_ context.Context, _, _ string) (port.Integration, error) {
	s.calls++
	return s.integ, s.err
}

func token(t *testing.T, orgID string) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"orgId": orgID})
	s, err := tok.SignedString([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func doRequest(t *testing.T, h *Handler, logIngestID, authHdr, body string) *httptest.ResponseRecorder {
	t.Helper()
	srv := New(h, quiet())
	req := httptest.NewRequest(http.MethodPost, "/live-events/"+logIngestID+"/integration/webhook", strings.NewReader(body))
	if authHdr != "" {
		req.Header.Set("Authorization", authHdr)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func TestWebhookSuccess(t *testing.T) {
	c := cache.New()
	q := queue.New(1 << 20)
	res := &stubResolver{integ: port.Integration{OrgID: "org_x", LiveEventsUUID: "uuid_x"}}
	h := NewHandler(c, res, q, quiet(), time.Hour)

	rec := doRequest(t, h, "log123", "Bearer "+token(t, "org_jwt"), `{"a":1}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202", rec.Code)
	}

	e, err := q.Dequeue()
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	// orgId comes from the JWT; liveEventsUuid from the resolver.
	if e.OrgID != "org_jwt" || e.LiveEventsUUID != "uuid_x" {
		t.Fatalf("event = %+v", e)
	}
	if string(e.Payload) != `{"a":1}` {
		t.Fatalf("payload = %q", e.Payload)
	}
}

func TestWebhookCacheHitSkipsResolver(t *testing.T) {
	c := cache.New()
	c.Set("log123", cache.Value{LiveEventsUUID: "cached_uuid", OrgID: "cached_org"}, time.Hour)
	q := queue.New(1 << 20)
	res := &stubResolver{}
	h := NewHandler(c, res, q, quiet(), time.Hour)

	rec := doRequest(t, h, "log123", "Bearer "+token(t, "org_jwt"), `{}`)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	if res.calls != 0 {
		t.Fatalf("resolver called %d times on cache hit", res.calls)
	}
	e, _ := q.Dequeue()
	if e.LiveEventsUUID != "cached_uuid" || e.OrgID != "cached_org" {
		t.Fatalf("event = %+v", e)
	}
}

func TestWebhookMissingAuth(t *testing.T) {
	h := NewHandler(cache.New(), &stubResolver{}, queue.New(1<<20), quiet(), time.Hour)
	rec := doRequest(t, h, "log123", "", `{}`)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d want 401", rec.Code)
	}
}

func TestWebhookIntegrationNotFound(t *testing.T) {
	res := &stubResolver{err: port.ErrIntegrationNotFound}
	h := NewHandler(cache.New(), res, queue.New(1<<20), quiet(), time.Hour)
	rec := doRequest(t, h, "log123", "Bearer "+token(t, "org"), `{}`)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d want 404", rec.Code)
	}
}

func TestWebhookQueueFullReturns503(t *testing.T) {
	res := &stubResolver{integ: port.Integration{OrgID: "o", LiveEventsUUID: "u"}}
	q := queue.New(1) // forces ErrFull on the second event
	c := cache.New()
	h := NewHandler(c, res, q, quiet(), time.Hour)

	// First request is admitted (empty-queue always-admit), filling the queue.
	if rec := doRequest(t, h, "log123", "Bearer "+token(t, "org"), `data`); rec.Code != http.StatusAccepted {
		t.Fatalf("first status = %d want 202", rec.Code)
	}
	// Second request must be rejected with 503.
	if rec := doRequest(t, h, "log123", "Bearer "+token(t, "org"), `data`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("second status = %d want 503", rec.Code)
	}
}
