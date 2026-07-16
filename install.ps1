# Build and install the `tpt` CLI (tpt-cli/) from source into %USERPROFILE%\.cargo\bin.
# Requires a Rust toolchain (https://rustup.rs) - there is no prebuilt-binary
# release pipeline yet (see TODO.md Phase 7).
$ErrorActionPreference = "Stop"

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Error "cargo not found. Install Rust first: https://rustup.rs"
    exit 1
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

Write-Host "Building tpt-cli in release mode..."
Push-Location (Join-Path $repoRoot "tpt-cli")
try {
    cargo install --path . --locked --force
} finally {
    Pop-Location
}

Write-Host ""
Write-Host "Installed. Run 'tpt --help' to get started (make sure %USERPROFILE%\.cargo\bin is on your PATH)."
