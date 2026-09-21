#!/usr/bin/env bash
# Disposable real systemd install/start/upgrade check. Requires root in a test
# VM/orb, not a production host. Archives/checksums must already be downloaded.
set -euo pipefail
repo="$(cd "$(dirname "$0")/.." && pwd)"
archives="$(realpath "${1:?archive directory required}")"
baseline="${2:?published baseline tag required (e.g. v0.0.1-rc19)}"
candidate="${3:-v0.0.2}"
[[ $(id -u) == 0 && $(cat /proc/1/comm) == systemd ]] || { echo 'Requires root and running systemd in a disposable environment'; exit 1; }
work="$(mktemp -d "$repo/.zig-cache/install-XXXXXX")"
service="sermon-candidate-$$"
cleanup() {
  systemctl stop "$service" >/dev/null 2>&1 || true
  systemctl disable "$service" >/dev/null 2>&1 || true
  rm -f "/etc/systemd/system/$service.service"
  systemctl daemon-reload
  rm -rf "$work"
}
trap cleanup EXIT
install_version() {
  bash "$repo/install.sh" --version "$1" --release-base-url "file://$archives" \
    --install-dir "$work/install" --config-dir "$work/config" \
    --db-path "$work/data/metrics.db" --service-name "$service"
  sleep 12
  systemctl is-active --quiet "$service"
  [[ $(systemctl show -p NRestarts --value "$service") == 0 ]]
  env -u LD_LIBRARY_PATH "$work/install/bin/sermon" --db "$work/data/metrics.db" --format json status > "$work/status.json"
  python3 -m json.tool "$work/status.json" >/dev/null
  env -u LD_LIBRARY_PATH "$work/install/bin/sermon" --db "$work/data/metrics.db" --format json query \
    'SELECT min(timestamp) AS oldest, count(*) AS samples FROM metrics' > "$work/metrics.json"
  python3 -c 'import json,sys; rows=json.load(open(sys.argv[1])); assert int(rows[0]["samples"]) > 0' "$work/metrics.json"
}
# First, genuinely fresh install of the candidate.
install_version "$candidate"
echo "PASS clean $candidate install/start, live metrics, packaged library resolution"
systemctl stop "$service"
rm -rf "$work/install" "$work/config" "$work/data"
# Then install the published baseline and upgrade without supplying a new key.
install_version "$baseline"
cp "$work/metrics.json" "$work/baseline-metrics.json"
sha256sum "$work/config/config.json" > "$work/config.sha256"
mkdir -p "$work/data/_outbox"
printf 'private-local-upgrade-sentinel\n' > "$work/data/_outbox/upgrade.held"
sha256sum "$work/data/_outbox/upgrade.held" > "$work/outbox.sha256"
install_version "$candidate"
sha256sum -c "$work/config.sha256" "$work/outbox.sha256"
python3 - "$work/baseline-metrics.json" "$work/metrics.json" <<'PY'
import json, sys
before, after = [json.load(open(path))[0] for path in sys.argv[1:]]
assert before["oldest"] == after["oldest"] and int(after["samples"]) > int(before["samples"]), (before, after)
PY
echo "PASS $baseline -> $candidate real systemd restart; config, retained store and held outbox preserved"
