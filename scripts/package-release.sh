#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: package-release.sh --version <tag> --target <zig-target> [--arch <x86_64|aarch64>] [--out-dir <dir>]

Builds a ReleaseSafe Sermon Daemon tarball for a Linux glibc target.
EOF
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CALLER_DIR="$(pwd)"
HOST_ARCH="$(uname -m)"
STAGING_DIR=""

cleanup() {
  if [[ -n "${STAGING_DIR}" ]]; then
    rm -rf "${STAGING_DIR}"
  fi

  if [[ "${SERMON_RESTORE_HOST_DUCKDB:-1}" == "1" ]]; then
    "${SCRIPT_DIR}/bootstrap-duckdb.sh" --arch "${HOST_ARCH}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

VERSION=""
TARGET=""
ARCH=""
OUT_DIR="${OUT_DIR:-dist}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --target)
      TARGET="${2:-}"
      shift 2
      ;;
    --arch)
      ARCH="${2:-}"
      shift 2
      ;;
    --out-dir)
      OUT_DIR="${2:-}"
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

if [[ -z "${VERSION}" || -z "${TARGET}" ]]; then
  echo "Error: --version and --target are required" >&2
  usage >&2
  exit 1
fi

if [[ -z "${ARCH}" ]]; then
  case "${TARGET}" in
    x86_64-linux-gnu) ARCH="x86_64" ;;
    aarch64-linux-gnu) ARCH="aarch64" ;;
    *)
      echo "Error: cannot infer --arch from target ${TARGET}" >&2
      exit 1
      ;;
  esac
fi

case "${TARGET}" in
  x86_64-linux-gnu|aarch64-linux-gnu) ;;
  *)
    echo "Unsupported release target: ${TARGET}" >&2
    exit 1
    ;;
esac

if [[ "${OUT_DIR}" != /* ]]; then
  OUT_DIR="${CALLER_DIR}/${OUT_DIR}"
fi

cd "${DAEMON_ROOT}"

"${SCRIPT_DIR}/bootstrap-duckdb.sh" --arch "${ARCH}"
rm -rf zig-out
zig build -Dtarget="${TARGET}" -Doptimize=ReleaseSafe

mkdir -p "${OUT_DIR}"
STAGING_DIR="$(mktemp -d)"

package_name="sermon-${VERSION}-${TARGET}"
package_dir="${STAGING_DIR}/${package_name}"
mkdir -p "${package_dir}/bin" "${package_dir}/lib"

cp zig-out/bin/sermon zig-out/bin/sermon-agent "${package_dir}/bin/"
cp lib/libduckdb.so "${package_dir}/lib/"
cp install.sh "${package_dir}/install.sh"
cp README.md "${package_dir}/README.md"
chmod +x "${package_dir}/install.sh"

if [[ -f "${DAEMON_ROOT}/LICENSE" ]]; then
  cp "${DAEMON_ROOT}/LICENSE" "${package_dir}/LICENSE"
fi

archive_path="${OUT_DIR}/${package_name}.tar.gz"
tar -C "${STAGING_DIR}" -czf "${archive_path}" "${package_name}"
(
  cd "${OUT_DIR}"
  sha256sum "$(basename "${archive_path}")" > "$(basename "${archive_path}").sha256"
)

echo "Wrote ${archive_path}"
