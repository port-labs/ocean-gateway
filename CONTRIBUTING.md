# Contributing to ocean-gateway

Thank you for your interest in contributing! This document covers how to get
set up, run tests, and submit changes.

## Prerequisites

- Go 1.25+
- Docker (for Redis and the load test)
- `redis-cli` (optional, for inspecting streams)

## Local setup

```sh
git clone https://github.com/port-labs/ocean-gateway.git
cd ocean-gateway

# Start Redis
docker run -d --name redis -p 6379:6379 redis:7-alpine

# Copy and adjust environment variables
cp .env.example .env

# Run the gateway
REDIS_OCEAN_LIVE_EVENTS_URL=localhost:6379 go run ./cmd/gateway
```

Or spin up both services with Docker Compose:

```sh
docker compose up
```

## Running tests

```sh
go test ./... -race
```

The test suite uses [miniredis](https://github.com/alicebob/miniredis) for an
in-process Redis, so no external Redis is required for unit/integration tests.

To run the Redis write-path throughput benchmark (requires a real Redis):

```sh
REDIS_BENCH_ADDR=localhost:6379 go test ./internal/redisstream/ -run TestDrainThroughput -v
```

## Running the load test

```sh
# Start Redis and the gateway first (see Local setup above)
./scripts/loadtest.sh -e 100000 -s 10 -c 100
```

See `./scripts/loadtest.sh -h` for all flags.

## Code style

- `gofmt` / `goimports` — run before committing
- `go vet` — must pass
- `golangci-lint run` — must pass (see `.golangci.yml` for active linters)

```sh
go vet ./...
golangci-lint run
```

## Submitting changes

1. Fork the repo and create a branch from `main`.
2. Make your changes, add tests for new behaviour.
3. Ensure `go test ./... -race` and `golangci-lint run` both pass.
4. Open a pull request — CI will run build, lint, tests, and a load test
   automatically.

Helm chart changes belong in
[port-labs/helm-charts](https://github.com/port-labs/helm-charts/tree/main/charts/ocean-gateway),
not in this repository.

## Architecture notes

**The gateway is intentionally stateless.** Each incoming webhook is written
synchronously to a Redis stream (`XADD`) before the `202` response is sent.
There is no in-memory queue. This means:

- A pod crash never loses an accepted event.
- Horizontal scaling requires no coordination — just add pods.
- The producer is the buffer: a `503` means "Redis is unavailable, retry."

**Consumer guidance.** Integrations reading from the streams should use Redis
Streams consumer groups (`XREADGROUP` + `XACK`) rather than plain `XREAD`.
This ensures:
- Events remain "pending" until explicitly acknowledged.
- On restart, unacknowledged messages are reclaimed with `XAUTOCLAIM`.
- At-least-once delivery is preserved even through integration crashes.

**Stream key format:**
```
<liveEventsUUID>/live-events/raw/event-stream
```
The `raw` segment is namespaced to leave room for other event classes later.

**Retention:** `EVENT_TTL` trims old entries via `XADD MINID`; `STREAM_TTL`
deletes idle streams via `EXPIRE` refreshed on each write. Both default to 1h.
