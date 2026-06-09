#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLASK_URL="${FLASK_URL:-http://127.0.0.1:5000}"

cd "$ROOT"

if ! command -v cargo >/dev/null 2>&1; then
  echo "[run_desktop_dev] ERROR: cargo not found. Install Rust from https://rustup.rs/" >&2
  exit 1
fi

if ! cargo tauri --version >/dev/null 2>&1; then
  echo "[run_desktop_dev] ERROR: 'cargo tauri' not available. Install: cargo install tauri-cli --locked" >&2
  exit 1
fi

echo "[run_desktop_dev] Checking Flask health at $FLASK_URL/health (optional — Tauri starts Flask automatically if needed) ..."
if curl -sfS --connect-timeout 2 "$FLASK_URL/health" >/dev/null 2>&1; then
  echo "[run_desktop_dev] Flask already running — skipping auto-start inside Tauri warm-up loop."
else
  echo "[run_desktop_dev] Flask not reachable — Tauri will spawn it (unless TAURI_SKIP_BACKEND=1)."
fi

echo "[run_desktop_dev] Launching Tauri (dev) → Flask at $FLASK_URL when ready"
cd "$ROOT/src-tauri"
# Tauri CLI 2.x can fail if CI is set to "1" (invalid --ci parsing). Dev script clears it.
exec env -u CI cargo tauri dev
