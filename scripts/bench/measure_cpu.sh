#!/usr/bin/env bash
#
# Measure sermon-agent CPU usage on a populated DuckDB.
#
# Usage: measure_cpu.sh <release_dir> <out_file> [duration_s] [db_path]
#
#   release_dir: directory containing bin/sermon-agent and lib/libduckdb.so
#                (e.g. an extracted sermon-v0.0.1-rc6-x86_64-linux-gnu/)
#   out_file:    where to write per-second CPU% / RSS samples
#   duration_s:  sample length (default 90)
#   db_path:     DuckDB file the daemon should use (default ./metrics.db)
#
# Pairs with populate_metrics_db.py to compare daemon builds against a
# heavy DuckDB. Used by the regression harness in PR #86.
set -euo pipefail

BIN_DIR="${1:?release_dir required}"
OUT="${2:?out_file required}"
DUR="${3:-90}"
DB_PATH="${4:-./metrics.db}"

# Stop any prior daemon
pkill -9 -f "bin/sermon-agent" 2>/dev/null || true
sleep 2

# Start daemon
cd "$BIN_DIR"
LD_LIBRARY_PATH=lib ./bin/sermon-agent --db "$DB_PATH" --interval 10 > /tmp/daemon.log 2>&1 &
PID=$!
sleep 5
echo "started daemon PID=$PID"

# Sample
> "$OUT"
for i in $(seq 1 "$DUR"); do
  if ! kill -0 "$PID" 2>/dev/null; then
    echo "daemon exited at sample $i" >&2
    break
  fi
  ps -p "$PID" -o %cpu=,rss= --no-headers >> "$OUT"
  sleep 1
done

kill -9 "$PID" 2>/dev/null || true
echo "samples written to $OUT"
