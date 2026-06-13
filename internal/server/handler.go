package server

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/port-labs/ocean-gateway/internal/auth"
	"github.com/port-labs/ocean-gateway/internal/cache"
	"github.com/port-labs/ocean-gateway/internal/event"
	"github.com/port-labs/ocean-gateway/internal/port"
	"github.com/port-labs/ocean-gateway/internal/queue"
)

// maxBodyBytes caps a single live-event payload to protect memory.
const maxBodyBytes = 4 << 20 // 4 MiB

// IntegrationResolver resolves a logIngestId to its integration identity.
type IntegrationResolver interface {
	GetIntegrationByLogIngestID(ctx context.Context, logIngestID, token string) (port.Integration, error)
}

// Handler implements the live-event intake flow.
type Handler struct {
	cache    *cache.Cache
	resolver IntegrationResolver
	queue    *queue.Queue
	log      *slog.Logger
	cacheTTL time.Duration
	now      func() time.Time
}

// NewHandler constructs the intake handler.
func NewHandler(c *cache.Cache, resolver IntegrationResolver, q *queue.Queue, log *slog.Logger, cacheTTL time.Duration) *Handler {
	return &Handler{
		cache:    c,
		resolver: resolver,
		queue:    q,
		log:      log,
		cacheTTL: cacheTTL,
		now:      time.Now,
	}
}

// Webhook handles POST /live-events/{logIngestId}/integration/webhook.
func (h *Handler) Webhook(w http.ResponseWriter, r *http.Request) {
	logIngestID := chi.URLParam(r, "logIngestId")
	if logIngestID == "" {
		http.Error(w, "missing logIngestId", http.StatusBadRequest)
		return
	}

	token, err := auth.ExtractBearer(r)
	if err != nil {
		http.Error(w, "missing or invalid Authorization header", http.StatusUnauthorized)
		return
	}
	orgID, err := auth.OrgIDFromToken(token)
	if err != nil {
		http.Error(w, "invalid token", http.StatusUnauthorized)
		return
	}

	// Resolve the integration (cache first, then Port API).
	resolved, ok := h.cache.Get(logIngestID)
	if !ok {
		integ, err := h.resolver.GetIntegrationByLogIngestID(r.Context(), logIngestID, token)
		if errors.Is(err, port.ErrIntegrationNotFound) {
			http.Error(w, "Integration not found", http.StatusNotFound)
			return
		}
		if err != nil {
			h.log.Error("resolve integration failed", "logIngestId", logIngestID, "err", err)
			http.Error(w, "failed to resolve integration", http.StatusBadGateway)
			return
		}
		resolved = cache.Value{LiveEventsUUID: integ.LiveEventsUUID, OrgID: orgID}
		h.cache.Set(logIngestID, resolved, h.cacheTTL)
	}

	// Read the raw event payload.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	e := &event.Event{
		OrgID:          resolved.OrgID,
		LiveEventsUUID: resolved.LiveEventsUUID,
		Payload:        body,
		ReceivedAt:     h.now(),
	}
	if err := h.queue.Enqueue(e); err != nil {
		// ErrFull (backpressure) or ErrClosed (shutting down).
		http.Error(w, "gateway buffer full", http.StatusServiceUnavailable)
		return
	}

	w.WriteHeader(http.StatusAccepted)
}
