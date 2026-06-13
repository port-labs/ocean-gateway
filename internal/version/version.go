// Package version holds build-time metadata injected via ldflags.
//
//	go build -ldflags "-X github.com/port-labs/ocean-gateway/internal/version.Version=v1.2.3 \
//	                   -X github.com/port-labs/ocean-gateway/internal/version.Commit=abc1234 \
//	                   -X github.com/port-labs/ocean-gateway/internal/version.Date=2026-06-13"
package version

import "runtime"

var (
	Version = "dev"
	Commit  = "none"
	Date    = "unknown"
)

// GoVersion returns the Go runtime version string.
func GoVersion() string { return runtime.Version() }
