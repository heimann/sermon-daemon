#!/usr/bin/env bash
set -euo pipefail

# Bootstrap the optional model-backed NER preprocessor (-Dner=true). Unlike
# DuckDB (a pinned upstream release), privacy-filter.cpp ships no prebuilt
# shared library, so we build libpf + ggml from source at a pinned commit and
# fetch the published GGUF model. Outputs land in lib/ and models/, both
# gitignored. After this, `zig build -Dner=true` links and the daemon can load
# the model via config (enable_ner=true, ner_model_path).

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAEMON_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
LIB_DIR="${DAEMON_ROOT}/lib"
MODEL_DIR="${DAEMON_ROOT}/models"

# Pinned upstream: github.com/localai-org/privacy-filter.cpp. Override PF_REF to
# move the pin; ggml is a submodule, so the clone is --recursive.
PF_REPO="${PF_REPO:-https://github.com/localai-org/privacy-filter.cpp}"
PF_REF="${PF_REF:-61a30dc1f24f1a1b69a89d4996a3259060be0370}"
# Published f16 GGUF (verified HTTP 200). Override MODEL_URL for q8/multilingual.
MODEL_URL="${MODEL_URL:-https://huggingface.co/LocalAI-io/privacy-filter-GGUF/resolve/main/privacy-filter-f16.gguf}"
MODEL_NAME="${MODEL_NAME:-privacy-filter-f16.gguf}"

PF_SRC=""
SKIP_LIBS=0
SKIP_MODEL=0

usage() {
  cat <<'EOF'
Usage: bootstrap-ner.sh [--pf-src DIR] [--skip-libs] [--skip-model]

Builds libpf + ggml from source (pinned) into lib/, and downloads the GGUF
model into models/, so `zig build -Dner=true` can link and run.

  --pf-src DIR    Reuse an existing privacy-filter.cpp checkout instead of
                  cloning (must already have the ggml submodule). The build
                  still runs cmake in DIR/build/release.
  --skip-libs     Only fetch the model (libs already present).
  --skip-model    Only build the libs (model already present).

Env overrides: PF_REPO, PF_REF, MODEL_URL, MODEL_NAME.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --pf-src) PF_SRC="${2:-}"; [[ -z "${PF_SRC}" ]] && { echo "Error: --pf-src requires a value" >&2; exit 1; }; shift 2 ;;
    --skip-libs) SKIP_LIBS=1; shift ;;
    --skip-model) SKIP_MODEL=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage >&2; exit 1 ;;
  esac
done

mkdir -p "${LIB_DIR}" "${MODEL_DIR}"

if [[ "${SKIP_LIBS}" -eq 0 ]]; then
  for cmd in git cmake; do
    command -v "${cmd}" >/dev/null 2>&1 || { echo "Missing required command: ${cmd}" >&2; exit 1; }
  done

  cleanup_src=""
  if [[ -z "${PF_SRC}" ]]; then
    PF_SRC="$(mktemp -d)"
    cleanup_src="${PF_SRC}"
    # shellcheck disable=SC2064
    trap "rm -rf '${cleanup_src}'" EXIT
    echo "Cloning ${PF_REPO} @ ${PF_REF} (with ggml submodule)..."
    git clone --recursive "${PF_REPO}" "${PF_SRC}" >/dev/null
    git -C "${PF_SRC}" checkout --quiet "${PF_REF}"
    git -C "${PF_SRC}" submodule update --init --recursive >/dev/null
  else
    echo "Using existing privacy-filter.cpp checkout: ${PF_SRC}"
  fi

  echo "Building libpf (release)..."
  cmake -S "${PF_SRC}" -B "${PF_SRC}/build/release" -DCMAKE_BUILD_TYPE=Release >/dev/null
  cmake --build "${PF_SRC}/build/release" --target pf -j >/dev/null

  # Copy the versioned .so chain (real files + symlinks) preserving links.
  cp -a "${PF_SRC}"/build/release/libpf.so* "${LIB_DIR}/"
  cp -a "${PF_SRC}"/build/release/ggml/src/libggml*.so* "${LIB_DIR}/"
  echo "Installed libpf + ggml to ${LIB_DIR}"
fi

if [[ "${SKIP_MODEL}" -eq 0 ]]; then
  command -v curl >/dev/null 2>&1 || { echo "Missing required command: curl" >&2; exit 1; }
  dest="${MODEL_DIR}/${MODEL_NAME}"
  echo "Downloading model -> ${dest}"
  tmp="$(mktemp "${MODEL_DIR}/.${MODEL_NAME}.XXXXXX")"
  # shellcheck disable=SC2064
  trap "rm -f '${tmp}'" EXIT
  curl -fSL "${MODEL_URL}" -o "${tmp}"
  # Validate it is a real GGUF (magic "GGUF") and a plausible size, instead of
  # trusting the URL blindly. No fabricated checksum: pin one here once verified.
  magic="$(head -c 4 "${tmp}")"
  if [[ "${magic}" != "GGUF" ]]; then
    echo "Downloaded file is not a GGUF model (bad magic: '${magic}')" >&2
    exit 1
  fi
  size="$(stat -c '%s' "${tmp}")"
  if [[ "${size}" -lt 100000000 ]]; then
    echo "Downloaded model implausibly small (${size} bytes) - aborting" >&2
    exit 1
  fi
  mv "${tmp}" "${dest}"
  echo "Installed model: ${dest} (${size} bytes, GGUF magic OK)"
  echo "Set in config.json: \"enable_ner\": true, \"ner_model_path\": \"${dest}\""
fi
