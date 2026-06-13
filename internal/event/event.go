// Package event defines the live-event value buffered by the gateway.
package event

import "time"

// Event is a single live event tagged with the logIngestId it was received
// under. It travels from the HTTP intake handler through the in-memory queue to
// a Redis stream. The gateway does not interpret the payload or validate
// ownership — the consuming integration does that when it picks events up.
type Event struct {
	LogIngestID string
	Payload     []byte
	Headers     []byte    // request headers, JSON-encoded
	ReceivedAt  time.Time // set by the HTTP handler; anchors queue-delay and e2e metrics
}

// fieldOverhead is a rough per-event accounting allowance for the struct
// header, string headers, and metadata fields on top of the payload/headers
// bytes. Keeps the queue memory bound conservative rather than exact.
const fieldOverhead = 144 // 128 + 16 bytes for time.Time

// Size returns the approximate in-memory footprint of the event in bytes. It
// is used by the queue to enforce its memory bound.
func (e *Event) Size() int {
	return len(e.Payload) + len(e.Headers) + len(e.LogIngestID) + fieldOverhead
}
