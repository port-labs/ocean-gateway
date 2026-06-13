// Package event defines the live-event value buffered by the gateway.
package event

// Event is a single live event tagged with the logIngestId it was received
// under. It travels from the HTTP intake handler through the in-memory queue to
// a Redis stream. The gateway does not interpret the payload or validate
// ownership — the consuming integration does that when it picks events up.
type Event struct {
	LogIngestID string
	Payload     []byte
	Headers     []byte // request headers, JSON-encoded
}

// fieldOverhead is a rough per-event accounting allowance for the struct
// header, string headers, and metadata, on top of the payload/headers bytes.
// It keeps the queue's memory bound conservative rather than exact.
const fieldOverhead = 128

// Size returns the approximate in-memory footprint of the event in bytes. It
// is used by the queue to enforce its memory bound.
func (e *Event) Size() int {
	return len(e.Payload) + len(e.Headers) + len(e.LogIngestID) + fieldOverhead
}
