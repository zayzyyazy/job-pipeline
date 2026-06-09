Python backend copy for bundled macOS/Linux/Windows `.app`:

  ./scripts/sync_py_bundle_for_tauri.sh

Produces `bundle-resources/job-pipeline/` beside this file (ignored by git). Run the script before `cargo tauri build` so the packaged app includes `app.py`, `database/`, `services/`, `templates/`, `static/`, `config.py`, `requirements.txt`, etc.

After syncing, install dependencies inside the bundled copy (recommended: venv in that folder):

  cd src-tauri/bundle-resources/job-pipeline
  python3 -m venv .venv
  ./.venv/bin/pip install -r requirements.txt

The desktop shell prefers `.venv/bin/python3` when present; otherwise it falls back to the system `python3`/`python`.

Alternatively set env `JOB_PIPELINE_ROOT` to your full checkout (with `.env`, `credentials.json`, `token.json`, `instance/`, etc.) — the Rust launcher checks that directory first.

Optional: `TAURI_SKIP_BACKEND=1` — do not spawn Python from Tauri (use when Flask is already running for debugging).

Built artifacts (relative to `src-tauri/`): macOS `.app` is under `target/release/bundle/macos/`.
