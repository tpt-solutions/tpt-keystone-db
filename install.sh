#!/usr/bin/env bash
# Build and install the `tpt` CLI (tpt-keystone-cli/) from source into ~/.cargo/bin.
# Requires a Rust toolchain (https://rustup.rs) — there is no prebuilt-binary
# release pipeline yet (see TODO.md Phase 7).
set -euo pipefail

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo not found. Install Rust first: https://rustup.rs" >&2
  exit 1
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Building tpt-keystone-cli in release mode..."
(cd "$repo_root/tpt-keystone-cli" && cargo install --path . --locked --force)

echo
echo "Installed. Run 'tpt --help' to get started (make sure ~/.cargo/bin is on your PATH)."
