#!/usr/bin/env bash
# loadtest.sh — run the ocean-gateway load test and print a performance summary.
#
# Usage:
#   ./scripts/loadtest.sh [options]
#
# Options:
#   -u URL          Gateway base URL          (default: http://localhost:8080)
#   -e EVENTS       Total events to send      (default: 10000)
#   -s STREAMS      Distinct live-events UUID streams (default: 10)
#   -c CONCURRENCY  Concurrent senders        (default: 50)
#   -p PREFIX       live-events UUID prefix   (default: loadtest-ingest-)
#   -r REDIS        Redis address             (default: localhost:6379)
#   -w WAIT         Max seconds to wait for drain (default: 120)
#   -f              Flush matching streams before run
#   -h              Show this help

set -euo pipefail

# ── defaults ─────────────────────────────────────────────────────────────────
GW_URL="http://localhost:8080"
EVENTS=10000
STREAMS=10
CONCURRENCY=50
PREFIX="loadtest-ingest-"
REDIS_URL="localhost:6379"
DRAIN_WAIT=120
FLUSH=0

# ── parse flags ───────────────────────────────────────────────────────────────
while getopts "u:e:s:c:p:r:w:fh" opt; do
  case $opt in
    u) GW_URL=$OPTARG ;;
    e) EVENTS=$OPTARG ;;
    s) STREAMS=$OPTARG ;;
    c) CONCURRENCY=$OPTARG ;;
    p) PREFIX=$OPTARG ;;
    r) REDIS_URL=$OPTARG ;;
    w) DRAIN_WAIT=$OPTARG ;;
    f) FLUSH=1 ;;
    h) sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "Unknown option -$OPTARG. Run with -h for help." >&2; exit 1 ;;
  esac
done

REDIS_HOST="${REDIS_URL%%:*}"
REDIS_PORT="${REDIS_URL##*:}"
RC="redis-cli -h $REDIS_HOST -p $REDIS_PORT"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
STREAM_SUFFIX="/live-events/raw/event-stream"
PATTERN="${PREFIX}*${STREAM_SUFFIX}"

bold()  { printf '\033[1m%s\033[0m' "$*"; }
green() { printf '\033[32m%s\033[0m' "$*"; }
red()   { printf '\033[31m%s\033[0m' "$*"; }
dim()   { printf '\033[2m%s\033[0m' "$*"; }
hr()    { printf '%0.s─' {1..60}; echo; }

# ── preflight ────────────────────────────────────────────────────────────────
echo
bold "Ocean Gateway Load Test"; echo
hr

echo "Checking prerequisites..."

if ! $RC PING >/dev/null 2>&1; then
  red "ERROR: Redis not reachable at $REDIS_URL"; echo; exit 1
fi
echo "  Redis $REDIS_URL … $(green OK)"

GW_STATUS=$(curl -s -o /dev/null -w "%{http_code}" "$GW_URL/healthz" 2>/dev/null || echo "000")
if [ "$GW_STATUS" != "200" ]; then
  red "ERROR: Gateway not healthy at $GW_URL (got HTTP $GW_STATUS)"; echo
  echo "  Start it with:  REDIS_OCEAN_GATEWAY_URL=$REDIS_URL go run ./cmd/gateway"
  exit 1
fi
echo "  Gateway $GW_URL … $(green OK)"
echo

# ── flush ────────────────────────────────────────────────────────────────────
if [ "$FLUSH" -eq 1 ]; then
  echo "Flushing existing test streams…"
  KEYS=$($RC --scan --pattern "$PATTERN" 2>/dev/null || true)
  if [ -n "$KEYS" ]; then
    COUNT=$(echo "$KEYS" | wc -l | tr -d ' ')
    echo "$KEYS" | xargs -r redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" DEL >/dev/null
    echo "  Deleted $COUNT stream(s)"
  else
    echo "  Nothing to flush"
  fi
  echo
fi

# ── run load test ────────────────────────────────────────────────────────────
RESULT_FILE=$(mktemp /tmp/loadtest-result.XXXXXX)
trap 'rm -f "$RESULT_FILE"' EXIT

bold "Running load test"; echo
printf "  events=%s  streams=%s  concurrency=%s\n" "$EVENTS" "$STREAMS" "$CONCURRENCY"
printf "  gateway=%s  prefix=%s\n" "$GW_URL" "$PREFIX"
echo

START_TS=$(date +%s)

go run "$REPO_ROOT/cmd/loadtest" \
  -url "$GW_URL" \
  -events "$EVENTS" \
  -streams "$STREAMS" \
  -concurrency "$CONCURRENCY" \
  -live-events-uuid-prefix "$PREFIX" \
  2>&1 | tee "$RESULT_FILE"

END_TS=$(date +%s)
TOTAL_WALL=$((END_TS - START_TS))

# ── parse load test output (awk, macOS-compatible) ───────────────────────────
field_after() { # field_after "label:" file
  awk -F': *' "/^[[:space:]]*$1/"'{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2; exit}' "$2"
}
code_count() { # code_count "202:" file
  awk "/^[[:space:]]*$1/"'{gsub(/^[[:space:]]+|[[:space:]]+$/, "", $2); print $2+0; exit}' "$2"
}

WALL=$(field_after "wall time" "$RESULT_FILE")
THROUGHPUT=$(field_after "throughput" "$RESULT_FILE")
CODE_202=$(code_count "202:" "$RESULT_FILE")
CODE_503=$(code_count "503:" "$RESULT_FILE")
LAT_MIN=$(field_after "min" "$RESULT_FILE")
LAT_P50=$(field_after "p50" "$RESULT_FILE")
LAT_P95=$(field_after "p95" "$RESULT_FILE")
LAT_P99=$(field_after "p99" "$RESULT_FILE")
LAT_MAX=$(field_after "max" "$RESULT_FILE")
LAT_AVG=$(field_after "avg" "$RESULT_FILE")
: "${WALL:=n/a}" "${THROUGHPUT:=n/a}" "${CODE_202:=0}" "${CODE_503:=0}"
: "${LAT_MIN:=n/a}" "${LAT_P50:=n/a}" "${LAT_P95:=n/a}" "${LAT_P99:=n/a}" "${LAT_MAX:=n/a}" "${LAT_AVG:=n/a}"

# ── wait for Redis drain ──────────────────────────────────────────────────────
echo
bold "Verifying Redis delivery"; echo
DRAIN_START=$(date +%s)
PREV=0
while true; do
  TOTAL=0
  for i in $(seq 0 $((STREAMS - 1))); do
    KEY="${PREFIX}${i}${STREAM_SUFFIX}"
    N=$($RC XLEN "$KEY" 2>/dev/null || echo 0)
    TOTAL=$((TOTAL + N))
  done
  ELAPSED=$(( $(date +%s) - DRAIN_START ))
  printf "\r  delivered=%d / accepted=%s  (%ds elapsed)" "$TOTAL" "$CODE_202" "$ELAPSED"
  if [ "$TOTAL" -ge "${CODE_202:-0}" ] 2>/dev/null; then
    echo; echo "  $(green "Fully drained")"; break
  fi
  if [ "$TOTAL" -eq "$PREV" ] && [ "$ELAPSED" -gt 5 ]; then
    echo; echo "  $(dim "Stable at $TOTAL (queue may still be draining in background)")"; break
  fi
  if [ "$ELAPSED" -ge "$DRAIN_WAIT" ]; then
    echo; echo "  $(red "Drain timeout after ${DRAIN_WAIT}s — $TOTAL/$CODE_202 delivered")"; break
  fi
  PREV=$TOTAL
  sleep 2
done

DRAIN_SECS=$(( $(date +%s) - END_TS ))
DRAIN_RATE=0
if [ "$DRAIN_SECS" -gt 0 ] && [ "${CODE_202:-0}" -gt 0 ]; then
  DRAIN_RATE=$(( CODE_202 / (DRAIN_SECS + TOTAL_WALL) ))
fi

# ── per-stream breakdown ──────────────────────────────────────────────────────
STREAM_TOTALS=()
STREAM_MAX=0
STREAM_MIN=999999999
for i in $(seq 0 $((STREAMS - 1))); do
  KEY="${PREFIX}${i}${STREAM_SUFFIX}"
  N=$($RC XLEN "$KEY" 2>/dev/null || echo 0)
  STREAM_TOTALS+=("$N")
  [ "$N" -gt "$STREAM_MAX" ] && STREAM_MAX=$N
  [ "$N" -lt "$STREAM_MIN" ] && STREAM_MIN=$N
done
FINAL_TOTAL=0
for n in "${STREAM_TOTALS[@]}"; do FINAL_TOTAL=$((FINAL_TOTAL + n)); done

# ── Redis stats ───────────────────────────────────────────────────────────────
REDIS_MEM=$($RC INFO memory 2>/dev/null | awk -F: '/^used_memory_human/{print $2}' | tr -d '\r ')
REDIS_KEYS=$($RC DBSIZE 2>/dev/null || echo "?")
# Sample TTL from first stream key
FIRST_KEY="${PREFIX}0${STREAM_SUFFIX}"
STREAM_TTL_VAL=$($RC TTL "$FIRST_KEY" 2>/dev/null || echo -1)
if [ "$STREAM_TTL_VAL" -gt 0 ] 2>/dev/null; then
  STREAM_TTL_FMT="${STREAM_TTL_VAL}s"
else
  STREAM_TTL_FMT="no expiry"
fi

# ── acceptance rate ───────────────────────────────────────────────────────────
ACCEPT_RATE="n/a"
if [ "${CODE_202:-0}" -gt 0 ] 2>/dev/null && [ "$EVENTS" -gt 0 ]; then
  ACCEPT_PCT=$(awk "BEGIN{printf \"%.1f\", $CODE_202/$EVENTS*100}")
  ACCEPT_RATE="${CODE_202} (${ACCEPT_PCT}%)"
fi
REJECT_RATE="n/a"
if [ "${CODE_503:-0}" -gt 0 ] 2>/dev/null; then
  REJECT_PCT=$(awk "BEGIN{printf \"%.1f\", ${CODE_503:-0}/$EVENTS*100}")
  REJECT_RATE="${CODE_503} (${REJECT_PCT}%)"
fi

# ── print summary ─────────────────────────────────────────────────────────────
echo
hr
bold "  LOAD TEST SUMMARY"; echo
hr
printf "  %-22s %s\n" "Events sent:"      "$EVENTS"
printf "  %-22s %s\n" "Streams:"          "$STREAMS"
printf "  %-22s %s\n" "Concurrency:"      "$CONCURRENCY"
echo
bold "  Intake (HTTP → Redis)"; echo
printf "  %-22s %s\n" "Wall time:"        "$WALL"
printf "  %-22s %s\n" "Throughput:"       "$THROUGHPUT"
printf "  %-22s %s\n" "Accepted (202):"   "$(green "$ACCEPT_RATE")"
if [ "${CODE_503:-0}" -gt 0 ]; then
  printf "  %-22s %s\n" "Rejected (503):"   "$(red "$REJECT_RATE")"
fi
echo
bold "  Latency (accepted path)"; echo
printf "  %-22s %s\n" "min:"  "$LAT_MIN"
printf "  %-22s %s\n" "p50:"  "$LAT_P50"
printf "  %-22s %s\n" "p95:"  "$LAT_P95"
printf "  %-22s %s\n" "p99:"  "$LAT_P99"
printf "  %-22s %s\n" "max:"  "$LAT_MAX"
printf "  %-22s %s\n" "avg:"  "$LAT_AVG"
echo
bold "  Redis delivery"; echo
printf "  %-22s %s\n" "Delivered:"        "$FINAL_TOTAL / ${CODE_202:-0}"
if [ "$FINAL_TOTAL" -eq "${CODE_202:-0}" ] 2>/dev/null; then
  printf "  %-22s %s\n" "Loss:"           "$(green "0 (zero loss)")"
else
  LOSS=$(( ${CODE_202:-0} - FINAL_TOTAL ))
  printf "  %-22s %s\n" "Loss:"           "$(red "$LOSS events still in queue/dropped")"
fi
printf "  %-22s events/stream (min=%s max=%s)\n" \
       "Distribution:"  "$STREAM_MIN" "$STREAM_MAX"
printf "  %-22s %s\n" "Stream TTL:"       "$STREAM_TTL_FMT"
printf "  %-22s %s\n" "Redis memory:"     "$REDIS_MEM"
printf "  %-22s %s\n" "Redis keys:"       "$REDIS_KEYS"
echo
bold "  Inspect streams"; echo
printf "  redis-cli --scan --pattern '%s'\n" "${PREFIX}*${STREAM_SUFFIX}"
printf "  redis-cli XRANGE '%s' - + COUNT 3\n" "$FIRST_KEY"
hr
