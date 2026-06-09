# Job Pipeline — Tauri desktop shell

## What Tauri does here

[Tauri](https://tauri.app/) provides a **native desktop window** with an embedded webview. The project **does not replace Flask** or the Jinja/HTML UI. Tauri loads the same app as in the browser at `http://127.0.0.1:<port>/` (default port **5000**, overridable with `FLASK_RUN_PORT` / `PORT`).

## Standalone `.app` (macOS)

When you double-click **`Job Pipeline.app`**:

1. The window opens on **`splash.html`** (“Starting Job Pipeline…”).
2. Rust checks **`/health`**. If Flask is **already** running on that port, nothing new is spawned.
3. If not, Tauri starts **`python3 app.py`** (or **`bundle-resources/job-pipeline/.venv/bin/python3`** when present) from the **bundled** project copy, with `JOB_PIPELINE_EMBEDDED=1`.
4. SQLite, Gmail `token.json`, and optional `credentials.json` for OAuth use a **writable** folder: **`~/Library/Application Support/Job Pipeline/`** (set via `JOB_PIPELINE_DATA_DIR`).
5. When **`/health`** returns `{"ok":true}`, the webview navigates to the dashboard (Rust listener). The splash page also **polls `/health`** and redirects as a fallback.
6. On quit, if Tauri **started** that Python process, it is **killed**. If Flask was already running (e.g. from Terminal), it is **left running**.

**Dev mode** (`cargo tauri dev`): unchanged — backend auto-start still runs unless `TAURI_SKIP_BACKEND=1`.

## Prerequisites (build machine)

- **Python 3.10+** and a **`.venv`** in the repo root with `pip install -r requirements.txt` (recommended so the bundle is self-contained).
- **Rust** + **Tauri CLI** (`cargo tauri`).
- **Node** — required by the Tauri toolchain for this layout.

## Sync Python into the bundle

From the repo root:

```bash
./scripts/sync_py_bundle_for_tauri.sh
```

This rsyncs the project (including **`.venv`** when it exists) into `src-tauri/bundle-resources/job-pipeline/`.

## Build the Mac app

```bash
cd src-tauri
cargo tauri build
```

Typical output path:

`src-tauri/target/release/bundle/macos/Job Pipeline.app`

Copy that bundle to **`/Users/<you>/Desktop/Job Pipeline.app`** if you want the exact double-click path from the product brief.

## Run — desktop development

```bash
./scripts/run_desktop_dev.sh
```

Flask is started automatically if it is not already up (unless `TAURI_SKIP_BACKEND=1`).

## Job links, copy, and the system browser

See **`src-tauri/capabilities/default.json`** — `remote.urls` must include the Flask origin (default `http://127.0.0.1:*`).

## Gmail credentials in the packaged app

Place **`credentials.json`** in **`~/Library/Application Support/Job Pipeline/`** after first launch (or copy from the project). Embedded mode prefers that path when the file exists; otherwise it uses `credentials.json` next to the bundled code (read-only inside the `.app`).

## Limitations

- **Python is not embedded** in the binary: the bundle uses the **copied `.venv`** or falls back to **`python3` on PATH** (must have Flask, etc.).
- **Playwright** (Apply Assist) is not bundled separately; it uses whatever the venv provides.
- **Custom Flask port**: set `FLASK_RUN_PORT` before launching the `.app`, and ensure the splash can reach the same port (Rust injects `window.__JOB_PIPELINE_FLASK_PORT__` from the same env).

## Troubleshooting

### Blank window

Confirm `http://127.0.0.1:5000/health` in a normal browser. If the app never leaves splash, check Console for Python errors; release builds attach **stderr** to null unless you use a debug build.

### `cargo tauri`: `--ci` / `CI=1`

Use `env -u CI cargo tauri dev` or `./scripts/run_desktop_dev.sh`.
