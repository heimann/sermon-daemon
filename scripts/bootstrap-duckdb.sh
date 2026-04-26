#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LIB_DIR="${DAEMON_ROOT}/lib"

usage() {
  cat <<'EOF'
Usage: bootstrap-duckdb.sh [--arch x86_64|aarch64]

Downloads the pinned DuckDB shared library for the requested Linux architecture.
Defaults to the current machine architecture.
EOF
}

DUCKDB_VERSION="${DUCKDB_VERSION:-1.2.1}"
REQUESTED_ARCH=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --arch)
      REQUESTED_ARCH="${2:-}"
      if [[ -z "${REQUESTED_ARCH}" ]]; then
        echo "Error: --arch requires a value" >&2
        exit 1
      fi
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

ARCH="${REQUESTED_ARCH:-$(uname -m)}"
case "${ARCH}" in
  x86_64|amd64|x86_64-linux-gnu)
    DUCKDB_ARCH="amd64"
    ;;
  aarch64|arm64|aarch64-linux-gnu)
    DUCKDB_ARCH="aarch64"
    ;;
  *)
    echo "Unsupported architecture: ${ARCH}" >&2
    exit 1
    ;;
esac

case "${DUCKDB_VERSION}:${DUCKDB_ARCH}" in
  "1.2.1:amd64")
    EXPECTED_SHA256="8dda081c84ef1da07f19f953ca95e1c6db9b6851e357444a751ad45be8a14d36"
    ;;
  "1.2.1:aarch64")
    EXPECTED_SHA256="882c451500f446f080eec42bf268a92a664a364ce0235ec257897de567e5d732"
    ;;
  *)
    echo "No pinned checksum for DuckDB v${DUCKDB_VERSION} (${DUCKDB_ARCH})" >&2
    echo "Update scripts/bootstrap-duckdb.sh with a verified checksum first." >&2
    exit 1
    ;;
esac

ZIP_NAME="libduckdb-linux-${DUCKDB_ARCH}.zip"
URL="https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/${ZIP_NAME}"

if ! command -v unzip >/dev/null 2>&1; then
  echo "Missing required command: unzip" >&2
  exit 1
fi

if ! command -v sha256sum >/dev/null 2>&1; then
  echo "Missing required command: sha256sum" >&2
  exit 1
fi

mkdir -p "${LIB_DIR}"

tmpdir="$(mktemp -d)"
trap 'rm -rf "${tmpdir}"' EXIT

zip_path="${tmpdir}/libduckdb.zip"

curl -fsSL "${URL}" -o "${zip_path}"

actual_sha256="$(sha256sum "${zip_path}" | awk '{print $1}')"
if [[ "${actual_sha256}" != "${EXPECTED_SHA256}" ]]; then
  echo "Checksum verification failed for ${ZIP_NAME}" >&2
  echo "Expected: ${EXPECTED_SHA256}" >&2
  echo "Actual:   ${actual_sha256}" >&2
  exit 1
fi

unzip -j -o "${zip_path}" libduckdb.so duckdb.h -d "${LIB_DIR}" >/dev/null

echo "Installed DuckDB v${DUCKDB_VERSION} to ${LIB_DIR}/libduckdb.so (checksum verified)"
