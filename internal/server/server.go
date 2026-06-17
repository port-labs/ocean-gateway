// Package server wires the HTTP intake onto a chi router.
package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PingFunc is a function that pings a dependency and returns any error.
// Wrap *redis.Client with: func(ctx context.Context) error { return rdb.Ping(ctx).Err() }
type PingFunc func(ctx context.Context) error

// New builds the HTTP handler with middleware and routes mounted.
func New(h *Handler, redisPing PingFunc, version, commit string, log *slog.Logger) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.RequestID)
	r.Use(middleware.Recoverer)
	r.Use(middleware.Timeout(30 * time.Second))
	r.Use(requestLogger(log))

	r.Get("/healthz", healthHandler(redisPing, version, commit))
	r.Get("/metrics", promhttp.Handler().ServeHTTP)

	r.Post("/live-events/{logIngestId}", h.Webhook)
	r.Post("/live-events/{logIngestId}/*", h.Webhook)

	return r
}

type componentStatus struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

type healthResponse struct {
	Status     string                     `json:"status"` // "ok" | "degraded"
	Components map[string]componentStatus `json:"components"`
	Version    string                     `json:"version"`
	Commit     string                     `json:"commit"`
	GoVersion  string                     `json:"goVersion"`
}

func healthHandler(redisPing PingFunc, version, commit string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		resp := healthResponse{
			Status:    "ok",
			Version:   version,
			Commit:    commit,
			GoVersion: runtime.Version(),
			Components: map[string]componentStatus{
				"gateway": {Status: "ok"},
				"redis":   {Status: "ok"},
			},
		}

		if err := redisPing(ctx); err != nil {
			resp.Status = "degraded"
			resp.Components["redis"] = componentStatus{Status: "error", Error: err.Error()}
		}

		code := http.StatusOK
		if resp.Status != "ok" {
			code = http.StatusServiceUnavailable
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(resp)
	}
}
