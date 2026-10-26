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
if [[ "$*" == "attestation verify --help" ]]; then
  if [[ "${SERMON_TEST_GH_HELP:-supported}" == supported ]]; then
    printf '%s\n' --repo --signer-workflow --source-ref --predicate-type --deny-self-hosted-runners |
      grep -Fxv -- "${SERMON_TEST_GH_MISSING:-none}"
    exit 0
  fi
  exit 1
fi
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
touch "${SERMON_TEST_SYSTEMCTL:?}"
exit 0
EOF
chmod +x "${fake_bin}/gh" "${fake_bin}/id" "${fake_bin}/systemctl"

install_dir="${work}/install"
config_dir="${work}/config"
db_path="${work}/data/metrics.db"
gh_args="${work}/gh.args"
export SERMON_TEST_SYSTEMCTL="${work}/systemctl.called"
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
  SERMON_TEST_GH_ARGS="${gh_args}" SERMON_TEST_GH_HELP=unsupported \
  bash "${repo}/install.sh" "${common_args[@]}"; then
  echo "installer accepted a GitHub CLI without attestation verification support" >&2
  exit 1
fi

test -f "${install_dir}/bin/sentinel"
test ! -e "${gh_args}"

for flag in --repo --signer-workflow --source-ref --predicate-type --deny-self-hosted-runners; do
  if HOME="${work}/home" PATH="${fake_bin}:${PATH}" \
    SERMON_TEST_GH_ARGS="${gh_args}" SERMON_TEST_GH_MISSING="${flag}" \
    bash "${repo}/install.sh" "${common_args[@]}"; then
    echo "installer accepted a verifier missing ${flag}" >&2
    exit 1
  fi
  test -f "${install_dir}/bin/sentinel"
  test ! -e "${gh_args}"
  test ! -e "${SERMON_TEST_SYSTEMCTL}"
done

if HOME="${work}/home" PATH="${fake_bin}:${PATH}" \
  SERMON_TEST_GH_ARGS="${gh_args}" SERMON_TEST_GH_RESULT=fail \
  bash "${repo}/install.sh" "${common_args[@]}"; then
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
[[ "${#recorded[@]}" -eq $((3 + ${#expected_policy[@]})) ]]

# These cases invoke the real installer; gh only stands in for the external
# cryptographic verifier. Refusal must preserve the installed tree and service.
reject_install() {
  local reason="$1"
  if HOME="${work}/home" PATH="${fake_bin}:${PATH}" \
    SERMON_TEST_GH_ARGS="${gh_args}" SERMON_TEST_GH_RESULT=pass \
    bash "${repo}/install.sh" "${common_args[@]}"; then
    echo "installer accepted ${reason}" >&2
    exit 1
  fi
  test "$(cat "${install_dir}/bin/sentinel")" = 'original install'
  test ! -e "${config_dir}/config.json"
  test ! -e "${SERMON_TEST_SYSTEMCTL}"
  echo "PASS refused ${reason} before replacement/service changes"
}

cp "${archives}/${archive}.sha256" "${work}/valid.sha256"
printf 'unrelated\n' > "${archives}/unrelated"
sha256sum "${archives}/unrelated" > "${archives}/${archive}.sha256"
rm "${gh_args}"
reject_install 'a checksum manifest for another file'
test ! -e "${gh_args}"
cp "${work}/valid.sha256" "${archives}/${archive}.sha256"

cp "${archives}/${archive}" "${work}/valid.tar.gz"
printf 'tampering' >> "${archives}/${archive}"
reject_install 'an archive with a mismatched checksum'
test ! -e "${gh_args}"
cp "${work}/valid.tar.gz" "${archives}/${archive}"

# A genuine release for the other platform can share the same tag provenance.
if [[ "${target}" == x86_64-linux-gnu ]]; then
  wrong_package="sermon-${version}-aarch64-linux-gnu"
else
  wrong_package="sermon-${version}-x86_64-linux-gnu"
fi
mv "${package}" "${work}/package/${wrong_package}"
tar -czf "${archives}/${archive}" -C "${work}/package" "${wrong_package}"
(cd "${archives}" && sha256sum "${archive}" > "${archive}.sha256")
reject_install 'a package with the wrong platform root'
cp "${work}/valid.tar.gz" "${archives}/${archive}"
cp "${work}/valid.sha256" "${archives}/${archive}.sha256"

# Hold the same transaction lock as a concurrent installer, without timing or
# sleeps. The contender must fail rather than touch shared .new/.old trees.
mkdir -p "${install_dir}.new" "${install_dir}.old"
printf 'staging\n' > "${install_dir}.new/sentinel"
printf 'rollback\n' > "${install_dir}.old/sentinel"
# Opening a preexisting lock must not truncate data, even through a symlink.
printf 'lock target\n' > "${work}/lock-target"
ln -s "${work}/lock-target" "${install_dir}.lock"
exec 9>>"${install_dir}.lock"
flock -n 9
reject_install 'a concurrent installation'
test "$(cat "${work}/lock-target")" = 'lock target'
test "$(cat "${install_dir}.new/sentinel")" = staging
test "$(cat "${install_dir}.old/sentinel")" = rollback
flock -u 9
exec 9>&-

HOME="${work}/home" PATH="${fake_bin}:${PATH}" \
  SERMON_TEST_GH_ARGS="${gh_args}" SERMON_TEST_GH_RESULT=pass \
  bash "${repo}/install.sh" "${common_args[@]}"

test -x "${install_dir}/bin/sermon"
test -x "${install_dir}/bin/sermon-agent"
test ! -e "${install_dir}/bin/sentinel"
test -f "${config_dir}/config.json"
test -f "${work}/home/.config/systemd/user/sermon-attestation-test.service"

echo "installer attestation policy/control-flow fixture ok (gh and systemd stubbed)"
