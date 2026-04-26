#!/usr/bin/env bash
set -euo pipefail

GITHUB_REPO="heimann/sermon-daemon"
DEFAULT_SERVER_URL="https://sermon.fyi"
DEFAULT_RELEASE_BASE_URL=""

usage() {
  cat <<'EOF'
Usage: install.sh [--server-name <name>] [--ingestion-key <key>] [options]

Options:
  --server-name <name>          Human name for this server. Default: hostname.
  --ingestion-key <key>         Optional Sermon ingestion key. If omitted, installs local-only.
  --server-url <url>            Sermon web URL for remote push. Default: https://sermon.fyi
  --version <tag>               Release tag. Default: latest GitHub release.
  --release-base-url <url>      Base URL containing tarball + .sha256 files.
  --install-dir <path>          Install dir. Default: /opt/sermon or ~/.local/opt/sermon.
  --config-dir <path>           Config dir. Default: /etc/sermon or ~/.config/sermon.
  --db-path <path>              Metrics DB path. Default: /var/lib/sermon/metrics.db or ~/.local/share/sermon/metrics.db.
  --service-name <name>         systemd service name. Default: sermon-agent.
  -h, --help                    Show this help.

Local test example:
  SERMON_RELEASE_BASE_URL=http://localhost:8000 \
    bash install.sh --version v0.0.0-local --server-name test --ingestion-key serm_... --server-url http://localhost:4000
EOF
}

log() {
  printf 'sermon-install: %s\n' "$*"
}

fail() {
  printf 'sermon-install: error: %s\n' "$*" >&2
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

shell_quote() {
  printf "'%s'" "$(printf '%s' "$1" | sed "s/'/'\\''/g")"
}

json_escape() {
  printf '%s' "$1" | sed 's/\\/\\\\/g; s/"/\\"/g'
}

SERVER_NAME=""
INGESTION_KEY=""
SERVER_URL="${SERMON_SERVER_URL:-${DEFAULT_SERVER_URL}}"
VERSION="${SERMON_INSTALL_VERSION:-}"
RELEASE_BASE_URL="${SERMON_RELEASE_BASE_URL:-${DEFAULT_RELEASE_BASE_URL}}"
INSTALL_DIR=""
CONFIG_DIR_OVERRIDE=""
DB_PATH_OVERRIDE=""
SERVICE_NAME="${SERMON_SERVICE_NAME:-sermon-agent}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-name)
      SERVER_NAME="${2:-}"
      shift 2
      ;;
    --ingestion-key)
      INGESTION_KEY="${2:-}"
      shift 2
      ;;
    --server-url)
      SERVER_URL="${2:-}"
      shift 2
      ;;
    --version)
      VERSION="${2:-}"
      shift 2
      ;;
    --release-base-url)
      RELEASE_BASE_URL="${2:-}"
      shift 2
      ;;
    --install-dir)
      INSTALL_DIR="${2:-}"
      shift 2
      ;;
    --config-dir)
      CONFIG_DIR_OVERRIDE="${2:-}"
      shift 2
      ;;
    --db-path)
      DB_PATH_OVERRIDE="${2:-}"
      shift 2
      ;;
    --service-name)
      SERVICE_NAME="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      fail "unknown option: $1"
      ;;
  esac
done

if [[ -z "${SERVER_NAME}" ]]; then
  SERVER_NAME="$(hostname 2>/dev/null || printf 'sermon-host')"
fi
[[ -n "${SERVER_URL}" ]] || fail "--server-url cannot be empty"
[[ "${SERVICE_NAME}" =~ ^[A-Za-z0-9_.@-]+$ ]] || fail "invalid --service-name"
UNIT_NAME="${SERVICE_NAME}.service"

[[ "$(uname -s)" == "Linux" ]] || fail "Linux is required"

if command -v ldd >/dev/null 2>&1; then
  ldd_version="$(ldd --version 2>&1 || true)"
  case "${ldd_version}" in
    *musl*|*Musl*|*MUSL*)
      fail "glibc Linux is required for Sermon V1 binaries; musl/Alpine is not supported yet"
      ;;
  esac
fi
if command -v getconf >/dev/null 2>&1 && ! getconf GNU_LIBC_VERSION >/dev/null 2>&1; then
  fail "glibc Linux is required for Sermon V1 binaries"
fi

case "$(uname -m)" in
  x86_64|amd64)
    TARGET="x86_64-linux-gnu"
    ;;
  aarch64|arm64)
    TARGET="aarch64-linux-gnu"
    ;;
  *)
    fail "unsupported architecture: $(uname -m)"
    ;;
esac

need_cmd curl
need_cmd tar
need_cmd sha256sum
need_cmd sed
need_cmd awk
need_cmd systemctl

if [[ "$(id -u)" == "0" ]]; then
  ROOT_INSTALL=1
  INSTALL_DIR="${INSTALL_DIR:-/opt/sermon}"
  CONFIG_DIR="${CONFIG_DIR_OVERRIDE:-/etc/sermon}"
  DB_PATH="${DB_PATH_OVERRIDE:-/var/lib/sermon/metrics.db}"
  UNIT_DIR="/etc/systemd/system"
  UNIT_PATH="${UNIT_DIR}/${UNIT_NAME}"
  SYSTEMCTL=(systemctl)
  JOURNALCTL=(journalctl -u "${UNIT_NAME}")
else
  ROOT_INSTALL=0
  INSTALL_DIR="${INSTALL_DIR:-${HOME}/.local/opt/sermon}"
  CONFIG_DIR="${CONFIG_DIR_OVERRIDE:-${HOME}/.config/sermon}"
  DB_PATH="${DB_PATH_OVERRIDE:-${HOME}/.local/share/sermon/metrics.db}"
  UNIT_DIR="${HOME}/.config/systemd/user"
  UNIT_PATH="${UNIT_DIR}/${UNIT_NAME}"
  SYSTEMCTL=(systemctl --user)
  JOURNALCTL=(journalctl --user -u "${UNIT_NAME}")
fi

if [[ -z "${VERSION}" ]]; then
  need_cmd grep
  latest_json="$(curl -fsSL "https://api.github.com/repos/${GITHUB_REPO}/releases/latest")"
  VERSION="$(printf '%s\n' "${latest_json}" | grep -m1 '"tag_name"' | sed -E 's/.*"tag_name"[[:space:]]*:[[:space:]]*"([^"]+)".*/\1/')"
  [[ -n "${VERSION}" ]] || fail "could not resolve latest release version"
fi

ARCHIVE="sermon-${VERSION}-${TARGET}.tar.gz"
if [[ -n "${RELEASE_BASE_URL}" ]]; then
  DOWNLOAD_BASE="${RELEASE_BASE_URL%/}"
else
  DOWNLOAD_BASE="https://github.com/${GITHUB_REPO}/releases/download/${VERSION}"
fi
ARCHIVE_URL="${DOWNLOAD_BASE}/${ARCHIVE}"
SHA_URL="${ARCHIVE_URL}.sha256"

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

log "downloading ${ARCHIVE_URL}"
curl -fsSL "${ARCHIVE_URL}" -o "${tmpdir}/${ARCHIVE}"
curl -fsSL "${SHA_URL}" -o "${tmpdir}/${ARCHIVE}.sha256"
(
  cd "${tmpdir}"
  sha256sum -c "${ARCHIVE}.sha256"
)

new_dir="${INSTALL_DIR}.new"
old_dir="${INSTALL_DIR}.old"
rm -rf "${new_dir}" "${old_dir}"
mkdir -p "${new_dir}"
tar -xzf "${tmpdir}/${ARCHIVE}" -C "${new_dir}" --strip-components=1
chmod +x "${new_dir}/bin/sermon" "${new_dir}/bin/sermon-agent"

mkdir -p "$(dirname "${INSTALL_DIR}")"
if [[ -d "${INSTALL_DIR}" ]]; then
  mv "${INSTALL_DIR}" "${old_dir}"
fi
mv "${new_dir}" "${INSTALL_DIR}"
rm -rf "${old_dir}"

mkdir -p "${CONFIG_DIR}" "$(dirname "${DB_PATH}")" "${UNIT_DIR}"

config_tmp="${CONFIG_DIR}/config.json.tmp"
umask 077
cat >"${config_tmp}" <<EOF
{
  "db_path": "$(json_escape "${DB_PATH}")",
  "interval": 10,
  "retention": 604800
EOF
if [[ -n "${INGESTION_KEY}" ]]; then
  cat >>"${config_tmp}" <<EOF
  ,"server_url": "$(json_escape "${SERVER_URL}")",
  "api_key": "$(json_escape "${INGESTION_KEY}")"
EOF
fi
cat >>"${config_tmp}" <<EOF
}
EOF
mv "${config_tmp}" "${CONFIG_DIR}/config.json"
chmod 600 "${CONFIG_DIR}/config.json"

quoted_exec="$(shell_quote "${INSTALL_DIR}/bin/sermon-agent")"
quoted_config="$(shell_quote "${CONFIG_DIR}/config.json")"

if [[ "${ROOT_INSTALL}" == "1" ]]; then
  cat >"${UNIT_PATH}" <<EOF
[Unit]
Description=Sermon observability daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${quoted_exec} --config ${quoted_config}
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$(shell_quote "$(dirname "${DB_PATH}")")

[Install]
WantedBy=multi-user.target
EOF
else
  cat >"${UNIT_PATH}" <<EOF
[Unit]
Description=Sermon observability daemon
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=${quoted_exec} --config ${quoted_config}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
EOF
fi

log "starting systemd service"
"${SYSTEMCTL[@]}" daemon-reload
"${SYSTEMCTL[@]}" enable "${UNIT_NAME}"
if "${SYSTEMCTL[@]}" is-active --quiet "${UNIT_NAME}"; then
  log "restarting existing systemd service"
  "${SYSTEMCTL[@]}" restart "${UNIT_NAME}"
else
  "${SYSTEMCTL[@]}" start "${UNIT_NAME}"
fi

for _ in $(seq 1 10); do
  if "${SYSTEMCTL[@]}" is-active --quiet "${UNIT_NAME}"; then
    log "daemon running for ${SERVER_NAME}"
    if [[ -n "${INGESTION_KEY}" ]]; then
      log "dashboard: ${SERVER_URL%/}/dashboard"
    else
      log "local-only install; query with ${INSTALL_DIR}/bin/sermon --db ${DB_PATH} status"
    fi
    exit 0
  fi
  sleep 1
done

"${SYSTEMCTL[@]}" status "${UNIT_NAME}" --no-pager || true
"${JOURNALCTL[@]}" -n 20 --no-pager || true
fail "${UNIT_NAME} did not become active"
