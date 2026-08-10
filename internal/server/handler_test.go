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

func doRequest(t *testing.T, h *Handler, liveEventsUUID, body string, headers map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	srv := New(h, noopPing, "test", "none", quiet())
	req := httptest.NewRequest(http.MethodPost, "/live-events/"+liveEventsUUID+"/integration/webhook", strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	return rec
}

func TestWebhookJSONArraySplitsIntoSeparateEvents(t *testing.T) {
	w := &stubWriter{}
	h := newHandler(w)

	rec := doRequest(t, h, "log123", `[{"a":1},{"b":2}]`, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202", rec.Code)
	}
	if len(w.events) != 2 {
		t.Fatalf("writer got %d events want 2", len(w.events))
	}
	if string(w.events[0].Payload) != `{"a":1}` {
		t.Fatalf("event 0 payload = %q want {\"a\":1}", w.events[0].Payload)
	}
	if string(w.events[1].Payload) != `{"b":2}` {
		t.Fatalf("event 1 payload = %q want {\"b\":2}", w.events[1].Payload)
	}
}

func TestWebhookNonArrayPayloadStaysSingleEvent(t *testing.T) {
	w := &stubWriter{}
	h := newHandler(w)

	rec := doRequest(t, h, "log123", `{"items":[1,2]}`, nil)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202", rec.Code)
	}
	if len(w.events) != 1 {
		t.Fatalf("writer got %d events want 1", len(w.events))
	}
	if string(w.events[0].Payload) != `{"items":[1,2]}` {
		t.Fatalf("payload = %q", w.events[0].Payload)
	}
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
	if e.LiveEventsUUID != "log123" || string(e.Payload) != `{"a":1}` {
		t.Fatalf("event = %+v", e)
	}
	if e.WebhookPath != "integration/webhook" {
		t.Fatalf("webhookPath = %q want integration/webhook", e.WebhookPath)
	}
	if e.EventID == "" {
		t.Fatal("eventId is empty")
	}
	var hdr map[string]string
	if err := json.Unmarshal(e.Headers, &hdr); err != nil {
		t.Fatalf("headers not json: %v", err)
	}
	if got := hdr["X-Event-Type"]; got != "issue_updated" {
		t.Fatalf("X-Event-Type = %q want issue_updated", got)
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

// captureHandler records slog records for assertions.
type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *captureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}
func (h *captureHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *captureHandler) WithGroup(string) slog.Handler      { return h }
func (h *captureHandler) payloadCount(payload string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	n := 0
	for _, r := range h.records {
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == "payload" && a.Value.String() == payload {
				n++
			}
			return true
		})
	}
	return n
}

func (h *captureHandler) attrValue(key string) string {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		var found string
		r.Attrs(func(a slog.Attr) bool {
			if a.Key == key {
				found = a.Value.String()
			}
			return true
		})
		if found != "" {
			return found
		}
	}
	return ""
}

func TestWriteLogsEventIDOnSuccess(t *testing.T) {
	logs := &captureHandler{}
	w := &stubWriter{}
	h := NewHandler(w, slog.New(logs), 2, time.Millisecond)
	e := &event.Event{EventID: event.NewID(), LiveEventsUUID: "log123", WebhookPath: "hook", Payload: []byte(`{}`)}
	if err := h.write(context.Background(), e); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := logs.attrValue("eventId"); got != e.EventID {
		t.Fatalf("eventId = %q want %q", got, e.EventID)
	}
}

func TestWriteLogsPayloadOnceOnRetrySuccess(t *testing.T) {
	logs := &captureHandler{}
	w := &stubWriter{failTimes: 2}
	h := NewHandler(w, slog.New(logs), 2, time.Millisecond)
	payload := `{"once":true}`
	e := &event.Event{EventID: event.NewID(), LiveEventsUUID: "log123", WebhookPath: "hook", Payload: []byte(payload)}
	if err := h.write(context.Background(), e); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := logs.payloadCount(payload); got != 1 {
		t.Fatalf("payload logged %d times want 1", got)
	}
}

func TestWriteLogsPayloadOnceOnFinalFailure(t *testing.T) {
	logs := &captureHandler{}
	w := &stubWriter{failTimes: 99}
	h := NewHandler(w, slog.New(logs), 2, time.Millisecond)
	payload := `{"once":true}`
	e := &event.Event{EventID: event.NewID(), LiveEventsUUID: "log123", WebhookPath: "hook", Payload: []byte(payload)}
	if err := h.write(context.Background(), e); err == nil {
		t.Fatal("write: want error")
	}
	if got := logs.payloadCount(payload); got != 1 {
		t.Fatalf("payload logged %d times want 1", got)
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
	wantPaths := []string{"integration/webhook", "custom/hook", ""}
	for i, e := range w.events {
		if e.LiveEventsUUID != "log123" {
			t.Fatalf("liveEventsUUID = %q want log123", e.LiveEventsUUID)
		}
		if e.WebhookPath != wantPaths[i] {
			t.Fatalf("event %d webhookPath = %q want %q", i, e.WebhookPath, wantPaths[i])
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
