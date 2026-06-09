#!/usr/bin/env bash
# Copy the Flask/Python project into src-tauri/bundle-resources/job-pipeline for `cargo tauri build`.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DEST="$ROOT/src-tauri/bundle-resources/job-pipeline"

mkdir -p "$DEST"
echo "[sync_py_bundle_for_tauri] Syncing repo → $DEST"

# Includes `.venv` when present so the packaged .app can run without a global pip install.
rsync -a --delete \
  --exclude ".git/" \
  --exclude "__pycache__/" \
  --exclude "*.pyc" \
  --exclude ".env" \
  --exclude "instance/" \
  --exclude "*.db" \
  --exclude "*.sqlite" \
  --exclude "*.sqlite3" \
  --exclude "node_modules/" \
  --exclude "src-tauri/target/" \
  --exclude "src-tauri/bundle-resources/job-pipeline/" \
  "$ROOT/" "$DEST/"

echo "[sync_py_bundle_for_tauri] Done. Confirm: test -f $DEST/app.py"
echo "[sync_py_bundle_for_tauri] Tip: create .venv at repo root (python3 -m venv .venv && pip install -r requirements.txt) before sync so the desktop bundle is self-contained."
