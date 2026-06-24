// Package event defines the live-event value forwarded by the gateway.
package event

// Event is a single live event tagged with the live-events UUID it was received
// under. The HTTP handler builds it and writes it straight to a Redis stream —
// the gateway holds no buffer of its own. The gateway does not interpret the
// payload or validate ownership; the consuming integration does that.
type Event struct {
	LiveEventsUUID string
	WebhookPath    string // path suffix after /live-events/{liveEventsUUID}/, empty when none
	Payload        []byte
	Headers        []byte // request headers, JSON-encoded
}
