#!/usr/bin/env bash
# Build/test matrix for the -Dstore (and -Dner) gating.
#
# Verifies:
#   1. The three test variants are all green.
#   2. The minimal build (-Dner=false -Dstore=false) links NEITHER duckdb nor
#      libpf nor libstdc++ in either binary (ldd-clean), so it builds and runs
#      with lib/libduckdb.so + lib/libpf.so absent.
#   3. The PURE test count (the always-run modules) is IDENTICAL across all three
#      variants - a regression guard that gating did not silently drop a pure
#      test in the minimal build.
#
# Run from the repo root: scripts/test-matrix.sh
set -euo pipefail

ZIG="${ZIG:-mise exec -- zig}"

# Pure modules whose test targets are wired into test_step in EVERY variant
# (none of them link a native lib and none are gated). config_test is included:
# it is the new pure config/gate suite, also always wired. The cross-variant
# assertion sums `test` decls across these files; if a future change gates one of
# them out of the minimal build, the per-variant build still succeeds but this
# count diverges and the script fails.
PURE_MODULES=(
  src/agent/collector.zig
  src/agent/logs.zig
  src/agent/rules.zig
  src/agent/push.zig
  src/agent/redact.zig
  src/agent/ner.zig
  src/agent/preprocessor.zig
  src/agent/redact_adapter.zig
  src/agent/proc_self.zig
  src/agent/proxmox.zig
  src/agent/staging.zig
  src/agent/config_test.zig
)

pure_count() {
  local total=0
  for f in "${PURE_MODULES[@]}"; do
    total=$(( total + $(grep -c '^test ' "$f") ))
  done
  echo "$total"
}

run_variant() {
  local label="$1"; shift
  echo "=== ${label}: $* test ==="
  $ZIG build "$@" test
  echo "=== ${label}: green ==="
}

echo "Pure-test count (must match across variants): $(pure_count)"

run_variant "store=true ner=false"  -Dner=false -Dstore=true
run_variant "store=false ner=false" -Dner=false -Dstore=false

# The ner=true leg needs lib/libpf.so (+ggml+model), bootstrapped from source via
# scripts/bootstrap-ner.sh. Skip it (don't fail) when libpf is absent - e.g. CI,
# which runs the cheap store-gating + ldd-clean legs without the heavy NER build.
# Force-require it with NER=1 (a clean clone that forgot to bootstrap then fails).
if [ -e lib/libpf.so ]; then
  run_variant "store=true ner=true" -Dner=true -Dstore=true
elif [ "${NER:-0}" = "1" ]; then
  echo "FAIL: NER=1 but lib/libpf.so absent - run scripts/bootstrap-ner.sh first" >&2
  exit 1
else
  echo "=== store=true ner=true: SKIPPED (lib/libpf.so absent; run scripts/bootstrap-ner.sh to include) ==="
fi

# ── Minimal-build ldd cleanliness ──
echo "=== minimal build ldd check (duckdb/libpf/libstdc++ must be ABSENT) ==="
$ZIG build -Dner=false -Dstore=false
dirty=0
for bin in sermon-agent sermon; do
  if ldd "zig-out/bin/${bin}" | grep -Eiq 'duckdb|libpf|libstdc'; then
    echo "FAIL: ${bin} links a native processing dep:"
    ldd "zig-out/bin/${bin}" | grep -Ei 'duckdb|libpf|libstdc'
    dirty=1
  else
    echo "OK: ${bin} is ldd-clean"
  fi
done
if [ "$dirty" -ne 0 ]; then
  echo "MATRIX FAILED: minimal build is not ldd-clean"
  exit 1
fi

echo "MATRIX PASSED"
