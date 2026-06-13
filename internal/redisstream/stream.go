// Package redisstream forwards events to per-logIngestId Redis streams.
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
	payloadField = "payload"
	headersField = "headers"
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
// for other event classes under the same logIngestId namespace later.
func StreamKey(logIngestID string) string {
	return fmt.Sprintf("%s/live-events/raw/event-stream", logIngestID)
}

// Add appends a single event to its stream.
func (w *Writer) Add(ctx context.Context, e *event.Event) error {
	if w.streamTTL <= 0 {
		return w.rdb.XAdd(ctx, w.argsFor(e)).Err()
	}
	// XADD + EXPIRE together so the idle TTL is refreshed atomically-enough.
	pipe := w.rdb.Pipeline()
	add := pipe.XAdd(ctx, w.argsFor(e))
	pipe.Expire(ctx, StreamKey(e.LogIngestID), w.streamTTL)
	_, _ = pipe.Exec(ctx)
	return add.Err()
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
	seen := make(map[string]struct{}, len(events))
	for i, e := range events {
		cmds[i] = pipe.XAdd(ctx, w.argsFor(e))
		// Refresh each distinct stream's idle TTL once per batch.
		if w.streamTTL > 0 {
			key := StreamKey(e.LogIngestID)
			if _, ok := seen[key]; !ok {
				seen[key] = struct{}{}
				pipe.Expire(ctx, key, w.streamTTL)
			}
		}
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

func (w *Writer) argsFor(e *event.Event) *redis.XAddArgs {
	args := &redis.XAddArgs{
		Stream: StreamKey(e.LogIngestID),
		Values: map[string]any{
			payloadField: e.Payload,
			headersField: e.Headers,
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
