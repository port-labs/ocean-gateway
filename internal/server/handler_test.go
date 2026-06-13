package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/port-labs/ocean-gateway/internal/queue"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

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

func TestWebhookSuccessCapturesPayloadAndHeaders(t *testing.T) {
	q := queue.New(1 << 20)
	h := NewHandler(q, quiet())

	rec := doRequest(t, h, "log123", `{"a":1}`, map[string]string{"X-Event-Type": "issue_updated"})
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d want 202", rec.Code)
	}

	e, err := q.Dequeue()
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if e.LogIngestID != "log123" {
		t.Fatalf("logIngestId = %q", e.LogIngestID)
	}
	if string(e.Payload) != `{"a":1}` {
		t.Fatalf("payload = %q", e.Payload)
	}
	// Headers are stored as a JSON object of canonical-cased header -> []string.
	var hdr map[string][]string
	if err := json.Unmarshal(e.Headers, &hdr); err != nil {
		t.Fatalf("headers not valid json: %v (%s)", err, e.Headers)
	}
	if got := hdr["X-Event-Type"]; len(got) != 1 || got[0] != "issue_updated" {
		t.Fatalf("X-Event-Type header = %v", got)
	}
}

func TestWebhookMissingLogIngestId(t *testing.T) {
	q := queue.New(1 << 20)
	h := NewHandler(q, quiet())
	// chi won't match an empty path segment, so this 404s at the router; assert
	// the handler itself rejects an empty value via a direct call.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}"))
	h.Webhook(rec, req) // no chi URL param set => empty logIngestId
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d want 400", rec.Code)
	}
}

func TestWebhookQueueFullReturns503(t *testing.T) {
	q := queue.New(1) // forces ErrFull on the second event
	h := NewHandler(q, quiet())

	if rec := doRequest(t, h, "log123", "data", nil); rec.Code != http.StatusAccepted {
		t.Fatalf("first status = %d want 202", rec.Code)
	}
	if rec := doRequest(t, h, "log123", "data", nil); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("second status = %d want 503", rec.Code)
	}
}
