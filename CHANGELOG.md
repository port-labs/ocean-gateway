# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Changed
- `STREAM_TTL` default is now `720h` (30 days)
- `EVENT_TTL` default is now `6h` (age-based trim via `XADD MINID`)

### Added
- `queuedAt` stream entry field (Unix nanoseconds) stamped before each `XADD`
  so consumers can measure time-until-consumed (queue wait from write to first
  `XREADGROUP`)
- Stateless synchronous write-through architecture: each webhook is `XADD`'d
  to Redis before returning `202`, eliminating in-memory event loss on pod crash
- Stream key format: `<logIngestId>/live-events/raw/event-stream`
- Each stream entry carries a `payload` (raw body) and `headers` (JSON) field
- Event TTL via `XADD MINID` (`EVENT_TTL`, default 1h)
- Stream idle TTL via `EXPIRE` refresh (`STREAM_TTL`, default 1h)
- Prometheus metrics at `/metrics`: forwarded/failed counters, inflight gauge,
  redis write and e2e latency histograms, HTTP request metrics, Redis pool stats
- `/healthz` with live Redis ping and structured JSON response (`200` / `503`)
- `ocean_gateway_build_info` metric with version/commit/date/go_version labels
- Per-request structured JSON logging (method, route, status, latency, requestId)
- Retry logging (warn on each attempt, error on final failure)
- `REDIS_POOL_SIZE` config for tuning concurrent write capacity
- Multi-stage Docker image (`gcr.io/distroless/static:nonroot`, ~3.6 MB)
- Build-time version injection via ldflags (`VERSION`, `COMMIT`, `DATE`)
- `docker-compose.yml` for local development (gateway + Redis with AOF)
- `scripts/loadtest.sh` — load test runner with performance summary
- GitHub Actions CI: build, vet, race tests, golangci-lint, 1M-event load test
- MIT License
