package server

import (
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"github.com/port-labs/ocean-gateway/internal/metrics"
)

// statusRecorder wraps http.ResponseWriter to capture the written status code.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Status() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

// requestLogger returns a chi middleware that emits a structured log line and
// records HTTP metrics for every request.
func requestLogger(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w}

			next.ServeHTTP(rec, r)

			status := rec.Status()
			latency := time.Since(start)

			// Use the matched route pattern as the label, not the raw path,
			// to avoid high cardinality from live-events UUID values.
			route := chi.RouteContext(r.Context()).RoutePattern()
			if route == "" {
				route = r.URL.Path
			}

			statusStr := fmt.Sprintf("%d", status)
			metrics.HTTPRequestsTotal.WithLabelValues(r.Method, route, statusStr).Inc()
			metrics.HTTPRequestDuration.WithLabelValues(r.Method, route, statusStr).Observe(latency.Seconds())

			log.Info("request",
				"method", r.Method,
				"path", r.URL.Path,
				"route", route,
				"status", status,
				"latency", latency.String(),
				"requestId", middleware.GetReqID(r.Context()),
			)
		})
	}
}
