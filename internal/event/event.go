// Package event defines the live-event value buffered by the gateway.
package event

import "time"

// Event is a single live event resolved to its target integration. It travels
// from the HTTP intake handler through the in-memory queue to a Redis stream.
type Event struct {
	OrgID          string
	LiveEventsUUID string
	Payload        []byte
	ReceivedAt     time.Time
}

// fieldOverhead is a rough per-event accounting allowance for the struct
// header, string headers, and metadata fields, on top of the payload bytes.
// It keeps the queue's memory bound conservative rather than exact.
const fieldOverhead = 128

// Size returns the approximate in-memory footprint of the event in bytes. It
// is used by the queue to enforce its memory bound.
func (e *Event) Size() int {
	return len(e.Payload) + len(e.OrgID) + len(e.LiveEventsUUID) + fieldOverhead
}
