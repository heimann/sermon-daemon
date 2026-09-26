#!/usr/bin/env bash
set -euo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
work="$(mktemp -d "${TMPDIR:-/tmp}/sermon-install-attestation-XXXXXX")"
cleanup() {
  rm -rf "${work}"
}
trap cleanup EXIT

case "$(uname -m)" in
  x86_64|amd64) target="x86_64-linux-gnu" ;;
  aarch64|arm64) target="aarch64-linux-gnu" ;;
  *) echo "unsupported test architecture: $(uname -m)" >&2; exit 1 ;;
esac

version="v9.8.7-attestation-test"
archive="sermon-${version}-${target}.tar.gz"
package="${work}/package/sermon-${version}-${target}"
archives="${work}/archives"
fake_bin="${work}/bin"
mkdir -p "${package}/bin" "${archives}" "${fake_bin}" "${work}/home"

for binary in sermon sermon-agent; do
  printf '#!/usr/bin/env bash\nexit 0\n' > "${package}/bin/${binary}"
  chmod +x "${package}/bin/${binary}"
done
tar -czf "${archives}/${archive}" -C "${work}/package" "$(basename "${package}")"
(
  cd "${archives}"
  sha256sum "${archive}" > "${archive}.sha256"
)

cat > "${fake_bin}/gh" <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$@" > "${SERMON_TEST_GH_ARGS:?}"
[[ "${SERMON_TEST_GH_RESULT:-fail}" == pass ]]
EOF

cat > "${fake_bin}/id" <<'EOF'
#!/usr/bin/env bash
case "${1:-}" in
  -u) printf '1000\n' ;;
  -nG) printf 'users\n' ;;
  *) exec /usr/bin/id "$@" ;;
esac
EOF

cat > "${fake_bin}/systemctl" <<'EOF'
#!/usr/bin/env bash
exit 0
EOF
chmod +x "${fake_bin}/gh" "${fake_bin}/id" "${fake_bin}/systemctl"

install_dir="${work}/install"
config_dir="${work}/config"
db_path="${work}/data/metrics.db"
gh_args="${work}/gh.args"
common_args=(
  --version "${version}"
  --release-base-url "file://${archives}"
  --install-dir "${install_dir}"
  --config-dir "${config_dir}"
  --db-path "${db_path}"
  --service-name sermon-attestation-test
)

mkdir -p "${install_dir}/bin"
printf 'original install\n' > "${install_dir}/bin/sentinel"

if HOME="${work}/home" PATH="${fake_bin}:${PATH}" \
  SERMON_TEST_GH_ARGS="${gh_args}" SERMON_TEST_GH_RESULT=fail \
  bash "${repo}/install.sh" "${common_args[@]}" >/dev/null 2>&1; then
  echo "installer accepted an archive whose attestation verification failed" >&2
  exit 1
fi

test -f "${install_dir}/bin/sentinel"
test ! -e "${config_dir}/config.json"
test ! -e "${work}/home/.config/systemd/user/sermon-attestation-test.service"

# The temporary directory is intentionally unpredictable, so assert the policy
# arguments separately from the archive path recorded in argv[3].
mapfile -t recorded < "${gh_args}"
[[ "${recorded[0]}" == attestation ]]
[[ "${recorded[1]}" == verify ]]
[[ "${recorded[2]##*/}" == "${archive}" ]]
expected_policy=(
  --repo heimann/sermon-daemon
  --signer-workflow heimann/sermon-daemon/.github/workflows/release.yml
  --source-ref "refs/tags/${version}"
  --predicate-type https://slsa.dev/provenance/v1
  --deny-self-hosted-runners
)
for ((i = 0; i < ${#expected_policy[@]}; i++)); do
  [[ "${recorded[i + 3]}" == "${expected_policy[i]}" ]]
done
[[ "${#recorded[@]}" -eq 12 ]]

HOME="${work}/home" PATH="${fake_bin}:${PATH}" \
  SERMON_TEST_GH_ARGS="${gh_args}" SERMON_TEST_GH_RESULT=pass \
  bash "${repo}/install.sh" "${common_args[@]}" >/dev/null

test -x "${install_dir}/bin/sermon"
test -x "${install_dir}/bin/sermon-agent"
test ! -e "${install_dir}/bin/sentinel"
test -f "${config_dir}/config.json"
test -f "${work}/home/.config/systemd/user/sermon-attestation-test.service"

echo "install attestation verification ok"
