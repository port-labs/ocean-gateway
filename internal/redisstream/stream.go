// Package redisstream forwards events to per-integration Redis streams.
package redisstream

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/event"
)

// payloadField is the stream entry field name holding the raw event bytes.
const payloadField = "payload"

// Writer appends events to Redis streams via XADD.
type Writer struct {
	rdb    redis.Cmdable
	maxLen int64 // 0 = uncapped
}

// NewWriter wraps a go-redis client. maxLen, when > 0, caps each stream with an
// approximate MAXLEN trim.
func NewWriter(rdb redis.Cmdable, maxLen int64) *Writer {
	return &Writer{rdb: rdb, maxLen: maxLen}
}

// StreamKey builds the per-integration raw-event stream key. The "raw" segment
// leaves room for other event classes under the same integration namespace.
func StreamKey(orgID, liveEventsUUID string) string {
	return fmt.Sprintf("%s/%s/live-events/raw/event-stream", orgID, liveEventsUUID)
}

// Add appends a payload to the stream for the given org/integration.
func (w *Writer) Add(ctx context.Context, orgID, liveEventsUUID string, payload []byte) error {
	return w.rdb.XAdd(ctx, w.argsFor(orgID, liveEventsUUID, payload)).Err()
}

// AddBatch appends a batch of events in a single pipelined round-trip. Events
// may target different streams. It returns a per-event error slice aligned with
// events (nil = success), so the caller can retry only the failures.
func (w *Writer) AddBatch(ctx context.Context, events []*event.Event) []error {
	if len(events) == 0 {
		return nil
	}
	pipe := w.rdb.Pipeline()
	cmds := make([]*redis.StringCmd, len(events))
	for i, e := range events {
		cmds[i] = pipe.XAdd(ctx, w.argsFor(e.OrgID, e.LiveEventsUUID, e.Payload))
	}
	// Exec returns the first command error; per-command status comes from each
	// cmd's Err(). A connection-level failure surfaces on every command.
	_, _ = pipe.Exec(ctx)
	errs := make([]error, len(events))
	for i, c := range cmds {
		errs[i] = c.Err()
	}
	return errs
}

func (w *Writer) argsFor(orgID, liveEventsUUID string, payload []byte) *redis.XAddArgs {
	args := &redis.XAddArgs{
		Stream: StreamKey(orgID, liveEventsUUID),
		Values: map[string]any{payloadField: payload},
	}
	if w.maxLen > 0 {
		args.MaxLen = w.maxLen
		args.Approx = true
	}
	return args
}
