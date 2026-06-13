# ocean-gateway

Stateless gateway for Ocean live events. It receives live-event webhooks and
writes them, untouched, **straight to** a per-`logIngestId` Redis stream that
the Ocean integration consumes. The gateway holds no buffer of its own — an
accepted event is durably in Redis before the `202` is returned, so a gateway
crash never loses data and pods scale horizontally with no coordination. It does
not authenticate, resolve, or validate the event; the consuming integration does
that when it reads the stream.

```mermaid
flowchart TD
    P["Live-event producer"] -->|"POST /live-events/{logIngestId}/integration/webhook"| H

    subgraph GW["Gateway pod (stateless)"]
        direction TB
        H["HTTP handler"] --> CAP["Capture body + request headers,<br/>tag with logIngestId"]
        CAP --> X["Synchronous XADD<br/>(bounded retry on failure)"]
        X -->|write fails| E503["503 (producer retries)"]
        X -->|write ok| OK["202 Accepted"]
    end

    X -->|"XADD &lt;logIngestId&gt;/live-events/raw/event-stream<br/>payload=&lt;body&gt; headers=&lt;json&gt;"| R[("Redis streams<br/>one per logIngestId")]
    R --> OI["Ocean integration<br/>(consumer: resolves + validates)"]
```

## Flow

`POST /live-events/{logIngestId}/integration/webhook`

1. Extract `logIngestId` from the path.
2. Read the raw request body and capture the request headers.
3. `XADD` the event to `<logIngestId>/live-events/raw/event-stream` (the `raw`
   segment leaves room for other event classes later). On a transient Redis
   error it retries with bounded backoff; on persistent failure it returns
   `503` so the producer retries. On success it returns `202`.

Each stream entry has two fields:

| Field | Contents |
|-------|----------|
| `payload` | the raw request body, byte-for-byte |
| `headers` | the request headers, JSON object (`{"Header-Name":["value", ...]}`) |

**Throughput** comes from concurrency, not an internal queue: each request does
its own `XADD` through the Redis connection pool, so many in-flight requests
parallelize naturally. Tune `REDIS_POOL_SIZE` to raise the concurrent-write
ceiling.

**Retention** — applied on every write:

- **Event TTL** (`EVENT_TTL`, default 1h): each `XADD` trims entries older than
  the TTL via `MINID`, so a stream holds roughly the last hour of events.
- **Stream idle TTL** (`STREAM_TTL`, default 1h): each write refreshes the
  stream key's `EXPIRE`, so a stream with no new events for the TTL is deleted
  entirely. Active streams never expire.

## Metrics

`GET /metrics` exposes Prometheus metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `gateway_events_forwarded_total` | Counter | Events successfully written to Redis (use `rate()` for handling rate) |
| `gateway_events_failed_total` | Counter | Events that failed to write after retries (caller got a 503) |
| `gateway_inflight_requests` | Gauge | Requests currently blocked on a Redis write (saturation signal) |
| `gateway_redis_write_seconds` | Histogram | Duration of the Redis write (XADD round-trip, incl. retries) |
| `gateway_event_e2e_seconds` | Histogram | End-to-end time from HTTP intake to successful Redis write |

## Run

```sh
REDIS_ADDR=localhost:6379 go run ./cmd/gateway
```

### Configuration (env)

| Var | Default | Description |
|-----|---------|-------------|
| `LISTEN_ADDR` | `:8080` | HTTP listen address |
| `REDIS_ADDR` | `localhost:6379` | Redis address |
| `REDIS_PASSWORD` | _(empty)_ | Redis password |
| `REDIS_DB` | `0` | Redis database |
| `REDIS_POOL_SIZE` | `0` | Redis connection pool size; bounds concurrent writes (`0` = go-redis default, 10×GOMAXPROCS) |
| `REDIS_STREAM_MAXLEN` | `0` | Approx `MAXLEN` per stream, size-based (ignored when `EVENT_TTL` > 0; `0` = uncapped) |
| `EVENT_TTL` | `1h` | Trim stream entries older than this via `MINID` (`0` = no age trim) |
| `STREAM_TTL` | `1h` | Idle stream key expiry, refreshed on each write (`0` = no expiry) |
| `WRITE_MAX_RETRIES` | `2` | Per-request XADD retries before returning 503 |
| `WRITE_BACKOFF_BASE` | `50ms` | Initial backoff (doubles per retry) |

## Test

```sh
go test ./... -race
```

## Load test

`cmd/loadtest` fires a burst of events at a running gateway, spread across N
distinct `logIngestId` streams. Each request carries a JSON body and a couple
of headers, exercising both the `payload` and `headers` fields written to the
stream.

```sh
go run ./cmd/loadtest -url http://localhost:8080 -events 10000 -streams 10 -concurrency 50
```

Flags: `-url`, `-events` (default 10000), `-streams` (default 10),
`-concurrency` (default 50), `-log-ingest-prefix` (default `loadtest-ingest-`),
`-timeout`. It reports throughput, status-code breakdown, and latency
percentiles. Stream `i` uses `logIngestId = <prefix><i>`. Inspect afterward
with:

```sh
redis-cli --scan --pattern 'loadtest-ingest-*/live-events/raw/event-stream'
```
