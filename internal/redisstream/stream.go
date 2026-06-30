// Package redisstream forwards events to per-live-events-UUID Redis streams.
//
// Two retention controls are applied on every write:
//   - event TTL: entries older than eventTTL are trimmed via XADD MINID, so a
//     stream only ever holds roughly the last eventTTL of events.
//   - stream idle TTL: the stream key's expiry is refreshed to streamTTL on
//     every write, so a stream with no new events for streamTTL is deleted.
package redisstream

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/event"
)

// Stream entry field names.
const (
	payloadField     = "payload"
	webhookPathField = "webhookPath"
	headersField     = "headers"
	QueuedAtField    = "queuedAt"
)

// Writer appends events to Redis streams via XADD.
type Writer struct {
	rdb       redis.Cmdable
	maxLen    int64         // 0 = uncapped (size-based trim, ignored when eventTTL > 0)
	eventTTL  time.Duration // 0 = no age-based trim
	streamTTL time.Duration // 0 = no idle expiry
	now       func() time.Time
}

// NewWriter wraps a go-redis client.
//   - maxLen, when > 0 and eventTTL == 0, caps each stream with an approximate
//     MAXLEN trim.
//   - eventTTL, when > 0, trims entries older than it via MINID (takes
//     precedence over maxLen).
//   - streamTTL, when > 0, refreshes each stream key's expiry on every write so
//     idle streams are deleted.
func NewWriter(rdb redis.Cmdable, maxLen int64, eventTTL, streamTTL time.Duration) *Writer {
	return &Writer{
		rdb:       rdb,
		maxLen:    maxLen,
		eventTTL:  eventTTL,
		streamTTL: streamTTL,
		now:       time.Now,
	}
}

// StreamKey is the per-integration stream key. The "raw" segment leaves room
// for other event classes under the same live-events UUID namespace later.
func StreamKey(liveEventsUUID string) string {
	return fmt.Sprintf("%s/live-events/raw/event-stream", liveEventsUUID)
}

// Add appends a single event to its stream.
func (w *Writer) Add(ctx context.Context, e *event.Event) error {
	if w.streamTTL <= 0 {
		return w.rdb.XAdd(ctx, w.argsFor(e)).Err()
	}
	// XADD + EXPIRE together so the idle TTL is refreshed atomically-enough.
	pipe := w.rdb.Pipeline()
	add := pipe.XAdd(ctx, w.argsFor(e))
	pipe.Expire(ctx, StreamKey(e.LiveEventsUUID), w.streamTTL)
	_, _ = pipe.Exec(ctx)
	return add.Err()
}

func (w *Writer) argsFor(e *event.Event) *redis.XAddArgs {
	args := &redis.XAddArgs{
		Stream: StreamKey(e.LiveEventsUUID),
		Values: map[string]any{
			payloadField:     e.Payload,
			webhookPathField: e.WebhookPath,
			headersField:     e.Headers,
			QueuedAtField:    strconv.FormatInt(w.now().UnixNano(), 10),
		},
	}
	switch {
	case w.eventTTL > 0:
		// Trim entries whose ID (a millisecond timestamp) predates the window.
		minID := strconv.FormatInt(w.now().Add(-w.eventTTL).UnixMilli(), 10)
		args.MinID = minID
		args.Approx = true
	case w.maxLen > 0:
		args.MaxLen = w.maxLen
		args.Approx = true
	}
	return args
}
