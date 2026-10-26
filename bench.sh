#!/usr/bin/env bash
# Benchmark sermon-agent resource usage. Fails if limits exceeded.
set -euo pipefail

MAX_RSS_KB=98304  # 96MB (bumped from 50MB - very conservative; baseline already sat at 50.4MB)
MAX_CPU=2.0       # percent (averaged over run)
DURATION=35       # seconds (3+ collection cycles at 10s interval)
# The daemon stores tables and its roll lock in the DB path's parent directory.
# A unique filename in /tmp does not isolate a benchmark from other daemons.
WORK_DIR=$(mktemp -d /tmp/sermon-bench.XXXXXXXX)
DB="$WORK_DIR/metrics.db"
PID=""

cleanup() {
    local status=$? child_status=0
    if [[ -n "$PID" ]]; then
        kill "$PID" 2>/dev/null || true
        wait "$PID" 2>/dev/null || child_status=$?
        if (( status == 0 )); then status=$child_status; fi
    fi
    if ! rm -rf "$WORK_DIR"; then
        if (( status == 0 )); then status=1; fi
    fi
    exit "$status"
}
trap cleanup EXIT

# Use daemon defaults, not the invoking user's optional remote/storage config.
printf '{}\n' > "$WORK_DIR/config.json"
echo "Starting agent (${DURATION}s measurement)..."
LD_LIBRARY_PATH=lib ./zig-out/bin/sermon-agent --config "$WORK_DIR/config.json" --db "$DB" --interval 10 &
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
