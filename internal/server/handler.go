package server

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/port-labs/ocean-gateway/internal/event"
	"github.com/port-labs/ocean-gateway/internal/metrics"
)

// maxBodyBytes caps a single live-event payload to protect memory.
const maxBodyBytes = 4 << 20 // 4 MiB

// StreamWriter writes a single event to its Redis stream.
type StreamWriter interface {
	Add(ctx context.Context, e *event.Event) error
}

// Handler implements the live-event intake flow. It writes each request
// (body + headers) straight to a Redis stream keyed by the logIngestId and only
// returns 202 once the write succeeds — the gateway holds no buffer of its own,
// so a crash never loses an accepted event. On a persistent Redis failure it
// returns 503 so the producer retries (the producer is the buffer).
type Handler struct {
	writer     StreamWriter
	log        *slog.Logger
	maxRetries int
	backoff    time.Duration
}

// NewHandler constructs the intake handler.
func NewHandler(writer StreamWriter, log *slog.Logger, maxRetries int, backoff time.Duration) *Handler {
	return &Handler{writer: writer, log: log, maxRetries: maxRetries, backoff: backoff}
}

// Webhook handles POST /live-events/{logIngestId}/integration/webhook.
func (h *Handler) Webhook(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.InFlightRequests.Inc()
	defer metrics.InFlightRequests.Dec()

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

	e := &event.Event{LogIngestID: logIngestID, Payload: body, Headers: headers}

	writeStart := time.Now()
	err = h.write(r.Context(), e)
	metrics.RedisWriteSeconds.Observe(time.Since(writeStart).Seconds())

	if err != nil {
		metrics.EventsFailedTotal.Inc()
		h.log.Error("failed to write event to redis", "logIngestId", logIngestID, "err", err)
		http.Error(w, "failed to persist event, retry", http.StatusServiceUnavailable)
		return
	}

	metrics.EventsForwardedTotal.Inc()
	metrics.EventE2ESeconds.Observe(time.Since(start).Seconds())
	w.WriteHeader(http.StatusAccepted)
}

// write performs the XADD with bounded exponential backoff. Retries smooth over
// transient blips; a persistent failure surfaces to the caller as a 503.
func (h *Handler) write(ctx context.Context, e *event.Event) error {
	delay := h.backoff
	var err error
	for attempt := 0; attempt <= h.maxRetries; attempt++ {
		if err = h.writer.Add(ctx, e); err == nil {
			return nil
		}
		if attempt == h.maxRetries {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			delay *= 2
		}
	}
	return err
}
