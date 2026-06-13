FROM golang:1.25-alpine AS builder

WORKDIR /build

# Cache dependency downloads separately from source compilation.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=none
ARG DATE=unknown

# Build a statically linked binary — no libc required in the runtime image.
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w \
      -X github.com/port-labs/ocean-gateway/internal/version.Version=${VERSION} \
      -X github.com/port-labs/ocean-gateway/internal/version.Commit=${COMMIT} \
      -X github.com/port-labs/ocean-gateway/internal/version.Date=${DATE}" \
    -o /gateway \
    ./cmd/gateway

# ── runtime ───────────────────────────────────────────────────────────────────
FROM gcr.io/distroless/static:nonroot

COPY --from=builder /gateway /gateway

EXPOSE 8080

ENTRYPOINT ["/gateway"]
