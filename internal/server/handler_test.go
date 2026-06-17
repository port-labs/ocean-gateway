package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/port-labs/ocean-gateway/internal/event"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// stubWriter records events and can be made to fail a number of times.
type stubWriter struct {
	mu        sync.Mutex
	events    []*event.Event
	failTimes int // fail the first N Add calls
	calls     int
}

func (s *stubWriter) Add(_ context.Context, e *event.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failTimes {
		return errors.New("redis down")
	}
	s.events = append(s.events, e)
	return nil
}

func newHandler(w StreamWriter) *Handler {
	return NewHandler(w, quiet(), 2, time.Millisecond)
}

func noopPing(_ context.Context) error { return nil }

func doRequest(t *testing.T, h *Handler, logIngestID, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	srv := New(h, noopPing, "test", "none", quiet())
	req := httptest.NewRequest(http.MethodPost, "/live-events/"+logIngestID+"/integration/webhook", strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func TestWebhookSuccessWritesPayloadAndHeaders(t *testing.T) {
	w := &stubWriter{}
	h := newHandler(w)

	rec := doRequest(t, h, "log123", `{"a":1}`, map[string]string{"X-Event-Type": "issue_updated"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202", rec.Code)
	}
	if len(w.events) != 1 {
		t.Fatalf("writer got %d events want 1", len(w.events))
	}
	e := w.events[0]
	if e.LogIngestID != "log123" || string(e.Payload) != `{"a":1}` {
		t.Fatalf("event = %+v", e)
	}
	var hdr map[string][]string
	if err := json.Unmarshal(e.Headers, &hdr); err != nil {
		t.Fatalf("headers not json: %v", err)
	}
	if got := hdr["X-Event-Type"]; len(got) != 1 || got[0] != "issue_updated" {
		t.Fatalf("X-Event-Type = %v", got)
	}
}

func TestWebhookRetriesThenSucceeds(t *testing.T) {
	w := &stubWriter{failTimes: 2} // fail twice, succeed on 3rd (maxRetries=2)
	h := newHandler(w)
	rec := doRequest(t, h, "log123", `{}`, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202", rec.Code)
	}
	if w.calls != 3 {
		t.Fatalf("calls = %d want 3", w.calls)
	}
}

func TestWebhookRedisFailureReturns503(t *testing.T) {
	w := &stubWriter{failTimes: 99} // always fails
	h := newHandler(w)
	rec := doRequest(t, h, "log123", `{}`, nil)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d want 503", rec.Code)
	}
}

func TestWebhookUnknownPathReturns404(t *testing.T) {
	srv := New(newHandler(&stubWriter{}), noopPing, "test", "none", quiet())
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}"))
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d want 404", rec.Code)
	}
}

func TestWebhookAlternatePathSuffix(t *testing.T) {
	w := &stubWriter{}
	h := newHandler(w)
	srv := New(h, noopPing, "test", "none", quiet())

	for _, path := range []string{
		"/live-events/log123/integration/webhook",
		"/live-events/log123/custom/hook",
		"/live-events/log123",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		if rec.Code != http.StatusAccepted {
			t.Fatalf("%s: status = %d want 202", path, rec.Code)
		}
	}

	if len(w.events) != 3 {
		t.Fatalf("writer got %d events want 3", len(w.events))
	}
	for _, e := range w.events {
		if e.LogIngestID != "log123" {
			t.Fatalf("logIngestId = %q want log123", e.LogIngestID)
		}
	}
}

func TestHealthzRedisUp(t *testing.T) {
	srv := New(newHandler(&stubWriter{}), noopPing, "test", "none", quiet())
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d want 200", rec.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("body not json: %v", err)
	}
	if body["status"] != "ok" {
		t.Fatalf("status = %v want ok", body["status"])
	}
}

func TestHealthzRedisDown(t *testing.T) {
	failPing := func(_ context.Context) error { return errors.New("connection refused") }
	srv := New(newHandler(&stubWriter{}), failPing, "test", "none", quiet())
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d want 503", rec.Code)
	}
	var body map[string]any
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("body not json: %v", err)
	}
	if body["status"] != "degraded" {
		t.Fatalf("status = %v want degraded", body["status"])
	}
}
