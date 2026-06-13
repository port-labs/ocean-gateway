package server

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/port-labs/ocean-gateway/internal/event"
	"github.com/port-labs/ocean-gateway/internal/queue"
)

// maxBodyBytes caps a single live-event payload to protect memory.
const maxBodyBytes = 4 << 20 // 4 MiB

// Handler implements the live-event intake flow: it buffers the raw request
// (body + headers) onto the queue, tagged with the logIngestId. It does not
// inspect the payload or validate ownership — the consuming integration does
// that when it reads the stream.
type Handler struct {
	queue *queue.Queue
	log   *slog.Logger
}

// NewHandler constructs the intake handler.
func NewHandler(q *queue.Queue, log *slog.Logger) *Handler {
	return &Handler{queue: q, log: log}
}

// Webhook handles POST /live-events/{logIngestId}/integration/webhook.
func (h *Handler) Webhook(w http.ResponseWriter, r *http.Request) {
	logIngestID := chi.URLParam(r, "logIngestId")
	if logIngestID == "" {
		http.Error(w, "missing logIngestId", http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	headers, err := json.Marshal(r.Header)
	if err != nil {
		// http.Header marshals deterministically; treat a failure as a server bug.
		h.log.Error("failed to encode headers", "logIngestId", logIngestID, "err", err)
		http.Error(w, "failed to encode headers", http.StatusInternalServerError)
		return
	}

	e := &event.Event{
		LogIngestID: logIngestID,
		Payload:     body,
		Headers:     headers,
	}
	if err := h.queue.Enqueue(e); err != nil {
		// ErrFull (backpressure) or ErrClosed (shutting down).
		http.Error(w, "gateway buffer full", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
