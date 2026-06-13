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

func doRequest(t *testing.T, h *Handler, logIngestID, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	srv := New(h, quiet())
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

func TestWebhookMissingLogIngestId(t *testing.T) {
	h := newHandler(&stubWriter{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}"))
	h.Webhook(rec, req) // no chi URL param => empty logIngestId
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d want 400", rec.Code)
	}
}
