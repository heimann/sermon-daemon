#!/usr/bin/env bash
# Benchmark sermon-agent resource usage. Fails if limits exceeded.
set -euo pipefail

MAX_RSS_KB=51200  # 50MB
MAX_CPU=2.0       # percent (averaged over run)
DURATION=35       # seconds (3+ collection cycles at 10s interval)
DB="/tmp/sermon-bench-$$.db"

cleanup() { kill "$PID" 2>/dev/null; wait "$PID" 2>/dev/null; rm -f "$DB"; }
trap cleanup EXIT

echo "Starting agent (${DURATION}s measurement)..."
LD_LIBRARY_PATH=lib ./zig-out/bin/sermon-agent --db "$DB" --interval 10 &
PID=$!
sleep "$DURATION"

RSS=$(awk '/^VmRSS/ {print $2}' /proc/$PID/status)
CPU=$(ps -p $PID -o pcpu --no-headers | tr -d ' ')

echo "RSS: ${RSS}KB (limit: ${MAX_RSS_KB}KB)"
echo "CPU: ${CPU}% (limit: ${MAX_CPU}%)"

FAIL=0
if (( RSS > MAX_RSS_KB )); then
    echo "FAIL: RSS ${RSS}KB exceeds ${MAX_RSS_KB}KB"
    FAIL=1
fi
if (( $(echo "$CPU > $MAX_CPU" | bc -l) )); then
    echo "FAIL: CPU ${CPU}% exceeds ${MAX_CPU}%"
    FAIL=1
fi

if (( FAIL == 0 )); then
    echo "PASS"
fi
exit $FAIL
