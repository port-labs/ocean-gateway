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
// (body + headers) straight to a Redis stream keyed by the live-events UUID and only
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

// Webhook handles POST /live-events/{liveEventsUUID} and /live-events/{liveEventsUUID}/*.
func (h *Handler) Webhook(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	metrics.InFlightRequests.Inc()
	defer metrics.InFlightRequests.Dec()

	liveEventsUUID := chi.URLParam(r, "liveEventsUUID")
	if liveEventsUUID == "" {
		http.NotFound(w, r)
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	flatHeaders := make(map[string]string, len(r.Header))
	for k := range r.Header {
		flatHeaders[k] = r.Header.Get(k)
	}
	headers, err := json.Marshal(flatHeaders)
	if err != nil {
		// http.Header marshals deterministically; treat a failure as a server bug.
		h.log.Error("failed to encode headers", "liveEventsUUID", liveEventsUUID, "err", err)
		http.Error(w, "failed to encode headers", http.StatusInternalServerError)
		return
	}

	e := &event.Event{
		LiveEventsUUID: liveEventsUUID,
		WebhookPath:    chi.URLParam(r, "*"),
		Payload:        body,
		Headers:        headers,
	}

	writeStart := time.Now()
	err = h.write(r.Context(), e)
	metrics.RedisWriteSeconds.Observe(time.Since(writeStart).Seconds())

	if err != nil {
		metrics.EventsFailedTotal.Inc()
		h.log.Error("failed to write event to redis", "liveEventsUUID", liveEventsUUID, "err", err)
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
			h.log.Debug("event written to stream", "liveEventsUUID", e.LiveEventsUUID)
			return nil
		}
		if attempt == h.maxRetries {
			break
		}
		h.log.Warn("redis write failed, retrying",
			"liveEventsUUID", e.LiveEventsUUID,
			"attempt", attempt+1,
			"maxRetries", h.maxRetries,
			"backoff", delay.String(),
			"err", err,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
			delay *= 2
		}
	}
	return err
}
