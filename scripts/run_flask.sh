#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -d ".venv" ]]; then
  # shellcheck source=/dev/null
  source ".venv/bin/activate"
  echo "[run_flask] Using virtualenv: .venv"
else
  echo "[run_flask] No .venv found; using python3 from PATH." >&2
  echo "[run_flask] Tip: python3 -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt" >&2
fi

if ! command -v python3 >/dev/null 2>&1; then
  echo "[run_flask] ERROR: python3 not found." >&2
  exit 1
fi

if [[ ! -f "requirements.txt" ]]; then
  echo "[run_flask] ERROR: requirements.txt missing in $ROOT" >&2
  exit 1
fi

# Optional: lightweight hint if Flask import likely missing (do not auto-pip: too many edge cases)
if ! python3 -c "import flask" 2>/dev/null; then
  echo "[run_flask] WARNING: Flask does not import. Install deps: pip install -r requirements.txt" >&2
fi

echo "[run_flask] Starting Flask (default http://127.0.0.1:5000; override with FLASK_RUN_PORT or PORT)..."
exec python3 app.py
