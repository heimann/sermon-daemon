#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

required_zig="${REQUIRED_ZIG_VERSION:-0.15.2}"
actual_zig="$(zig version)"

if [[ "${actual_zig}" != "${required_zig}" ]]; then
  echo "Zig version mismatch: expected ${required_zig}, got ${actual_zig}" >&2
  exit 1
fi

if [[ ! -f "${DAEMON_ROOT}/lib/libduckdb.so" ]]; then
  echo "Missing ${DAEMON_ROOT}/lib/libduckdb.so" >&2
  echo "Run: ./scripts/bootstrap-duckdb.sh" >&2
  exit 1
fi

echo "daemon preflight ok"
