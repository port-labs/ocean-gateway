// Command loadtest fires a configurable burst of live events at a running
// gateway, spread across several orgs. Each org gets its own logIngestId and
// its own bearer JWT (orgId claim), because the gateway caches the
// logIngestId -> {liveEventsUuid, orgId} resolution: sharing a logIngestId
// across orgs would collapse every event into the first org's stream.
//
// Usage:
//
//	go run ./cmd/loadtest -url http://localhost:8080 -events 10000 -orgs 10
package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	var (
		baseURL      = flag.String("url", "http://localhost:8080", "gateway base URL")
		totalEvents  = flag.Int("events", 10000, "total number of events to send")
		orgs         = flag.Int("orgs", 10, "number of distinct orgs (each gets its own logIngestId + JWT)")
		concurrency  = flag.Int("concurrency", 50, "number of concurrent senders")
		ingestPrefix = flag.String("log-ingest-prefix", "loadtest-ingest-", "logIngestId prefix; org i uses <prefix><i>")
		timeout      = flag.Duration("timeout", 10*time.Second, "per-request timeout")
	)
	flag.Parse()

	if *orgs < 1 || *totalEvents < 1 || *concurrency < 1 {
		fmt.Fprintln(os.Stderr, "events, orgs and concurrency must all be >= 1")
		os.Exit(2)
	}

	senders := buildSenders(*orgs, *ingestPrefix)

	client := &http.Client{
		Timeout: *timeout,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
			MaxConnsPerHost:     *concurrency * 2,
		},
	}

	fmt.Printf("Load test: %d events across %d orgs, concurrency %d -> %s\n",
		*totalEvents, *orgs, *concurrency, *baseURL)

	agg := run(client, *baseURL, senders, *totalEvents, *concurrency)
	agg.report(*totalEvents, *orgs)
}

// sender holds the per-org request context.
type sender struct {
	orgID       string
	logIngestID string
	authHeader  string
}

func buildSenders(orgs int, ingestPrefix string) []sender {
	out := make([]sender, orgs)
	for i := 0; i < orgs; i++ {
		orgID := fmt.Sprintf("org_load_%d", i)
		out[i] = sender{
			orgID:       orgID,
			logIngestID: fmt.Sprintf("%s%d", ingestPrefix, i),
			authHeader:  "Bearer " + mintToken(orgID),
		}
	}
	return out
}

// mintToken creates a decode-only JWT carrying the orgId claim. The gateway
// does not verify the signature, so any signing key works.
func mintToken(orgID string) string {
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"orgId": orgID})
	s, err := tok.SignedString([]byte("loadtest"))
	if err != nil {
		panic(err)
	}
	return s
}

type aggregate struct {
	mu        sync.Mutex
	latencies []time.Duration
	status    map[int]int64
	errors    int64
	totalNs   int64
	wall      time.Duration
}

func run(client *http.Client, baseURL string, senders []sender, total, concurrency int) *aggregate {
	agg := &aggregate{status: make(map[int]int64)}
	var next int64 = -1 // atomically incremented to claim event indices

	start := time.Now()
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make([]time.Duration, 0, total/concurrency+1)
			localStatus := make(map[int]int64)
			var localErrs, localNs int64
			for {
				i := atomic.AddInt64(&next, 1)
				if int(i) >= total {
					break
				}
				s := senders[int(i)%len(senders)]
				lat, code, err := send(client, baseURL, s, int(i))
				local = append(local, lat)
				localNs += int64(lat)
				if err != nil {
					localErrs++
				} else {
					localStatus[code]++
				}
			}
			agg.mu.Lock()
			agg.latencies = append(agg.latencies, local...)
			for c, n := range localStatus {
				agg.status[c] += n
			}
			agg.errors += localErrs
			agg.totalNs += localNs
			agg.mu.Unlock()
		}()
	}
	wg.Wait()
	agg.wall = time.Since(start)
	return agg
}

func send(client *http.Client, baseURL string, s sender, seq int) (time.Duration, int, error) {
	url := fmt.Sprintf("%s/live-events/%s/integration/webhook", baseURL, s.logIngestID)
	body := fmt.Sprintf(`{"org":%q,"seq":%d,"event":"loadtest"}`, s.orgID, seq)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader([]byte(body)))
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", s.authHeader)
	req.Header.Set("Content-Type", "application/json")

	t0 := time.Now()
	resp, err := client.Do(req)
	lat := time.Since(t0)
	if err != nil {
		return lat, 0, err
	}
	// Drain and close so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return lat, resp.StatusCode, nil
}

func (a *aggregate) report(total, orgs int) {
	a.mu.Lock()
	defer a.mu.Unlock()

	fmt.Printf("\n=== Results ===\n")
	fmt.Printf("wall time:    %s\n", a.wall.Round(time.Millisecond))
	if a.wall > 0 {
		fmt.Printf("throughput:   %.0f events/sec\n", float64(total)/a.wall.Seconds())
	}
	fmt.Printf("status codes:\n")
	codes := make([]int, 0, len(a.status))
	for c := range a.status {
		codes = append(codes, c)
	}
	sort.Ints(codes)
	for _, c := range codes {
		fmt.Printf("  %d: %d\n", c, a.status[c])
	}
	if a.errors > 0 {
		fmt.Printf("  transport errors: %d\n", a.errors)
	}

	if len(a.latencies) > 0 {
		sort.Slice(a.latencies, func(i, j int) bool { return a.latencies[i] < a.latencies[j] })
		fmt.Printf("latency:\n")
		fmt.Printf("  min:  %s\n", a.latencies[0].Round(time.Microsecond))
		fmt.Printf("  p50:  %s\n", pct(a.latencies, 0.50).Round(time.Microsecond))
		fmt.Printf("  p95:  %s\n", pct(a.latencies, 0.95).Round(time.Microsecond))
		fmt.Printf("  p99:  %s\n", pct(a.latencies, 0.99).Round(time.Microsecond))
		fmt.Printf("  max:  %s\n", a.latencies[len(a.latencies)-1].Round(time.Microsecond))
		fmt.Printf("  avg:  %s\n", (time.Duration(a.totalNs / int64(len(a.latencies)))).Round(time.Microsecond))
	}
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)-1))
	return sorted[idx]
}
