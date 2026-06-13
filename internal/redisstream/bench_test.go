package redisstream

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/port-labs/ocean-gateway/internal/event"
)

// TestDrainThroughput isolates the Redis write path (no gateway, no load
// generator competing for CPU) to compare single XADD vs pipelined AddBatch.
// It is skipped unless REDIS_BENCH_ADDR points at a real Redis, e.g.:
//
//	REDIS_BENCH_ADDR=localhost:6379 go test ./internal/redisstream/ -run TestDrainThroughput -v
func TestDrainThroughput(t *testing.T) {
	addr := os.Getenv("REDIS_BENCH_ADDR")
	if addr == "" {
		t.Skip("set REDIS_BENCH_ADDR to run the throughput benchmark")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()
	ctx := context.Background()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping %s: %v", addr, err)
	}

	const total = 200_000
	const batchSize = 500
	payload := []byte(`{"webhookEvent":"jira:issue_updated","issue":{"key":"PORT-1"}}`)

	mkEvents := func(uuid string) []*event.Event {
		out := make([]*event.Event, total)
		for i := range out {
			out[i] = &event.Event{OrgID: "org_bench", LiveEventsUUID: uuid, Payload: payload}
		}
		return out
	}

	// --- single XADD per event ---
	singleKey := StreamKey("org_bench", "single")
	rdb.Del(ctx, singleKey)
	w := NewWriter(rdb, 0)
	evs := mkEvents("single")
	t0 := time.Now()
	for _, e := range evs {
		if err := w.Add(ctx, e.OrgID, e.LiveEventsUUID, e.Payload); err != nil {
			t.Fatalf("add: %v", err)
		}
	}
	singleDur := time.Since(t0)

	// --- pipelined AddBatch (batchSize per round-trip) ---
	batchKey := StreamKey("org_bench", "batch")
	rdb.Del(ctx, batchKey)
	evs = mkEvents("batch")
	t0 = time.Now()
	for i := 0; i < len(evs); i += batchSize {
		end := i + batchSize
		if end > len(evs) {
			end = len(evs)
		}
		for _, err := range w.AddBatch(ctx, evs[i:end]) {
			if err != nil {
				t.Fatalf("addbatch: %v", err)
			}
		}
	}
	batchDur := time.Since(t0)

	rate := func(d time.Duration) float64 { return float64(total) / d.Seconds() }
	fmt.Printf("\n=== Redis write-path throughput (%d events, batch=%d) ===\n", total, batchSize)
	fmt.Printf("single XADD : %v  => %.0f events/sec\n", singleDur.Round(time.Millisecond), rate(singleDur))
	fmt.Printf("pipelined   : %v  => %.0f events/sec\n", batchDur.Round(time.Millisecond), rate(batchDur))
	fmt.Printf("speedup     : %.1fx\n", rate(batchDur)/rate(singleDur))

	rdb.Del(ctx, singleKey, batchKey)
}
