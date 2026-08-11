// Package apm boots the New Relic Go APM agent, mirroring the pattern used
// across other port-labs Go services (see packages/go/metrics/newrelic.go in
// the port repo).
package apm

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/newrelic/go-agent/v3/newrelic"
)

// NewRelicApp boots the New Relic Go APM agent when both NEW_RELIC_LICENSE_KEY
// and NEW_RELIC_APP_NAME are set in the environment. Returns (nil, nil) when
// the agent is intentionally disabled (e.g. local dev, tests). Callers should
// defer app.Shutdown(timeout).
func NewRelicApp() (*newrelic.Application, error) {
	licenseKey := os.Getenv("NEW_RELIC_LICENSE_KEY")
	appName := os.Getenv("NEW_RELIC_APP_NAME")

	if licenseKey == "" || appName == "" {
		if os.Getenv("IS_TEST_SUITE") == "" {
			const msg = "NEW_RELIC_LICENSE_KEY and/or NEW_RELIC_APP_NAME environment variables are missing, New Relic disabled"
			slog.Info(msg, "NEW_RELIC_APP_NAME", appName)
		}
		return nil, nil
	}

	app, err := newrelic.NewApplication(
		newrelic.ConfigFromEnvironment(),
		newrelic.ConfigDistributedTracerEnabled(true),
	)
	if err != nil {
		return nil, fmt.Errorf("apm: init new relic: %w", err)
	}

	slog.Info("New Relic enabled", "NEW_RELIC_APP_NAME", appName)
	return app, nil
}
