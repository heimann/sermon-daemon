#!/usr/bin/env bash
# Install the versioned git hooks in this repo by pointing git at .githooks/.
# Idempotent. Run once per clone:  ./.githooks/install-hooks.sh
set -euo pipefail
repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
chmod +x .githooks/pre-push
git config core.hooksPath .githooks
echo "Installed: core.hooksPath -> .githooks"
echo "pre-push security review is now active. Bypass any push with: git push --no-verify"
