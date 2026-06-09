#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FLASK_URL="${FLASK_URL:-http://127.0.0.1:5000}"
PORT="${PORT:-5000}"

ok=0
warn=0

step() { echo "[check_desktop] $*"; }

fail() { echo "[check_desktop] ERROR: $*" >&2; ok=1; }

python3 -V >/dev/null 2>&1 || fail "python3 not found"
[[ -f "$ROOT/requirements.txt" ]] || fail "requirements.txt missing at $ROOT"

if command -v rustc >/dev/null 2>&1; then
  step "Rust: $(rustc --version)"
else
  fail "rustc not found (install from https://rustup.rs/)"
fi

if command -v cargo >/dev/null 2>&1; then
  step "Cargo: $(cargo --version)"
else
  fail "cargo not found"
fi

if command -v node >/dev/null 2>&1; then
  step "Node: $(node --version)"
else
  fail "node not found (Tauri tooling expects Node/npm)"
fi

if command -v npm >/dev/null 2>&1; then
  step "npm: $(npm --version)"
else
  fail "npm not found"
fi

if cargo tauri --version >/dev/null 2>&1; then
  step "Tauri CLI: $(cargo tauri --version)"
else
  fail "cargo tauri missing — run: cargo install tauri-cli --locked"
fi

if [[ -d "$ROOT/.venv" ]]; then
  step "Virtualenv: .venv present"
else
  echo "[check_desktop] WARNING: no .venv; ensure dependencies are installed globally or create a venv." >&2
  warn=1
fi

if python3 -c "import flask" 2>/dev/null; then
  step "Python import: flask OK"
else
  echo "[check_desktop] WARNING: cannot import flask (pip install -r requirements.txt)." >&2
  warn=1
fi

if command -v lsof >/dev/null 2>&1; then
  if lsof -iTCP:"$PORT" -sTCP:LISTEN -n -P >/dev/null 2>&1; then
    step "Port $PORT: something is listening (Flask may already be running)"
  else
    step "Port $PORT: nothing listening (start Flask before cargo tauri dev)"
  fi
else
  step "Port $PORT: install lsof for listener check, or verify manually: $FLASK_URL"
fi

if curl -sfS --connect-timeout 1 "$FLASK_URL" >/dev/null 2>&1; then
  step "Flask probe: $FLASK_URL responds"
else
  step "Flask probe: $FLASK_URL not responding (expected if server is stopped)"
fi

if [[ "$ok" -ne 0 ]]; then
  exit 1
fi
if [[ "$warn" -ne 0 ]]; then
  exit 0
fi
step "All checks passed."
exit 0
