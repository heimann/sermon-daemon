#!/usr/bin/env bash
#
# Buffer-pool regression bench for sermon-agent.
#
# Runs the daemon through many collection cycles and samples its resident
# memory once per cycle, then checks the peak against a ceiling derived
# from DuckDB's PRAGMA memory_limit. Pairs with PR #96, which leaked the
# DuckDB buffer pool and grew RSS ~5-9 MB/hour over a multi-hour run with
# no automated test to catch it. See scripts/bench/README.md for the
# threshold derivation.
#
# Modes (BENCH_MODE env var):
#   fast  - default. ~45 cycles at 1s interval, completes in well under a
#           minute. A CI smoke test: catches a fast leak (>= ~2.5 MB/cycle)
#           that breaches the ceiling quickly.
#   soak  - 1200 cycles at 1s interval (~20 min). The buffer pool fully
#           warms; a sustained PR #96-scale leak accumulates past the
#           ceiling. This is the real regression gate.
#
# Overridable env vars: BENCH_MODE, BENCH_CYCLES, BENCH_WARMUP,
# BENCH_CEILING_MB, BENCH_SLOPE_KB.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${DAEMON_ROOT}"

AGENT="./zig-out/bin/sermon-agent"
if [[ ! -x "${AGENT}" ]]; then
    echo "Missing ${AGENT} - run: zig build" >&2
    exit 1
fi

MODE="${BENCH_MODE:-fast}"
case "${MODE}" in
    fast)
        CYCLES="${BENCH_CYCLES:-45}"
        WARMUP="${BENCH_WARMUP:-10}"
        # Over 45 cycles the daemon is still in DuckDB buffer-pool warmup.
        # The slope here is the warmup rate, measured at 151-225 KB/cycle
        # across clean runs; 600 KB/cycle is ~2.7x that clean ceiling, so
        # fast mode catches a leak of roughly >= 375 KB/cycle.
        SLOPE_KB="${BENCH_SLOPE_KB:-600}"
        # Clean-code peak RSS over the first 45 cycles measured 48.5-50.5
        # MB; 90 MB is a coarse safety net (~1.8x). See README.md.
        DEFAULT_CEILING_MB=90
        ;;
    soak)
        CYCLES="${BENCH_CYCLES:-1200}"
        WARMUP="${BENCH_WARMUP:-900}"
        # Post-warmup clean-code slope measured 40-61 KB/cycle; 300
        # KB/cycle is ~5x that. This is the real regression gate.
        SLOPE_KB="${BENCH_SLOPE_KB:-300}"
        # Clean-code peak RSS over 1200 cycles measured 130 MB; 200 MB is
        # a coarse safety net (~1.5x). See README.md.
        DEFAULT_CEILING_MB=200
        ;;
    *)
        echo "Unknown BENCH_MODE '${MODE}' (expected: fast | soak)" >&2
        exit 2
        ;;
esac

# Ceiling is a coarse, mode-specific safety net. The daemon's RSS is not
# bounded by a flat plateau - DuckDB's buffer pool keeps creeping toward
# (and the glibc allocator holds pages past) the memory_limit, so peak
# RSS scales with cycle count. The slope gate above is the precise
# regression signal; the ceiling only catches a gross runaway.
CEILING_MB="${BENCH_CEILING_MB:-${DEFAULT_CEILING_MB}}"

DB="$(mktemp -u /tmp/sermon-bench-bufferpool-XXXXXX.db)"
SAMPLES="$(mktemp /tmp/sermon-bench-rss-XXXXXX.csv)"
PID=""

cleanup() {
    if [[ -n "${PID}" ]]; then
        kill -TERM "${PID}" 2>/dev/null || true
        wait "${PID}" 2>/dev/null || true
    fi
    rm -f "${DB}" "${DB}".wal "${SAMPLES}"
}
trap cleanup EXIT

echo "buffer-pool bench: mode=${MODE} cycles=${CYCLES} warmup=${WARMUP} ceiling=${CEILING_MB}MB"

# Start the daemon at the minimum (1s) interval so the bench exercises the
# collection / buffer-pool loop as fast as the daemon allows.
LD_LIBRARY_PATH=lib "${AGENT}" --db "${DB}" --interval 1 >/tmp/sermon-bench-daemon.log 2>&1 &

# Resolve the real daemon PID by its exact process name. We deliberately do
# not trust $!: under some shells the launcher is wrapped, so $! can be the
# wrapper rather than the agent. -n picks the newest match.
for _ in $(seq 1 20); do
    PID="$(pgrep -x -n sermon-agent || true)"
    [[ -n "${PID}" ]] && break
    sleep 0.5
done
if [[ -z "${PID}" ]]; then
    echo "FAIL: sermon-agent did not start" >&2
    cat /tmp/sermon-bench-daemon.log >&2 || true
    exit 1
fi
echo "daemon PID=${PID}, sampling RSS per cycle..."

# Sample VmRSS once per second (one collection cycle at --interval 1).
echo "n,rss_kb" >"${SAMPLES}"
for ((i = 1; i <= CYCLES; i++)); do
    if ! kill -0 "${PID}" 2>/dev/null; then
        echo "FAIL: daemon exited early at cycle ${i}" >&2
        cat /tmp/sermon-bench-daemon.log >&2 || true
        exit 1
    fi
    RSS="$(awk '/^VmRSS/ {print $2}' "/proc/${PID}/status" 2>/dev/null || echo 0)"
    echo "${i},${RSS}" >>"${SAMPLES}"
    sleep 1
done

kill -TERM "${PID}" 2>/dev/null || true
wait "${PID}" 2>/dev/null || true
PID=""

python3 "${SCRIPT_DIR}/analyze_rss.py" "${SAMPLES}" "${WARMUP}" "${CEILING_MB}" "${SLOPE_KB}"
