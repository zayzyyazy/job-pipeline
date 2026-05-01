# Job Pipeline — Final local-first handbook

NRW-centric Gmail → SQLite automation desk with GPT enrichment, heuristic target fit,
location tagging, pinning, recycled deletes, categories, responsive card dashboard,
and optional Tauri shell.

## Prerequisites (macOS)

- **Python** 3.10+ (`python3 --version`).
- Recommended: a virtual env in this folder (`python3 -m venv .venv && source .venv/bin/activate`).
- **pip installs** matching your shipped `requirements.txt` (usually `pip install flask python-dotenv openai google-* etc.`).
- Desktop packaging extras (already satisfied on this machine if `rustc` / `cargo` / `node` exist):
  - Rust (`rustc --version`) — install via https://rustup.rs if missing.
  - Node.js for optional JS tooling (`node --version`) — install via https://nodejs.org if missing.
  - Tauri CLI ships with modern Rust toolchains: `cargo tauri --version`.

## Private files (never commit, never paste into chats)

| File | Purpose |
| --- | --- |
| `.env` | `OPENAI_API_KEY`, `FLASK_SECRET_KEY`, Gmail query tuning, feature flags, optional `FLASK_RUN_PORT` / `PORT`. |
| `credentials.json` | OAuth client secret JSON from Google Cloud (Desktop app). |
| `token.json` | Refresh token produced after your first Gmail login. |

Keep them beside `app.py` or point `GMAIL_CREDENTIALS_PATH` / `GMAIL_TOKEN_PATH` / `DB_PATH` via `.env`.

## Run the Flask web UI (primary workflow)

```bash
cd "/Users/zay/Desktop/Job-Pipeline-App"   # or your dev clone
source .venv/bin/activate                # if you use a venv
python3 app.py
```

Defaults listen on `http://127.0.0.1:5000` unless you export `FLASK_RUN_PORT` or `PORT`.

Smoke checklist:

1. Dashboard loads with **job cards** (pinned items first).
2. Filters: status, source, target fit, **location fit**, category, discovery, pinned-only, **search**, optional show-deleted.
3. Pin / unpin from card + detail.
4. Category editor locks AI overwrites when the checkbox stays on.
5. Soft delete hides jobs until “Show deleted” or restore on the detail page.
6. Manual paste enrichment + reprocess still refresh AI + discovery without nuking notes/pins/locks.

## NRW / Germany-remote logic

`services/location_fit.py` inspects normalized job + email blobs with regex ladders:

| `location_fit` | Meaning |
| --- | --- |
| `nrw` | Strong NRW / Ruhr wording (cities, NRW shorthand, Hybrid NRW). |
| `remote_germany` | Remote anchored to Germany, hybrid Deutschland, or GER-wide travel phrasing tolerant to NRW candidates. |
| `unclear` | Insufficient cues (still surfaced for manual triage). |
| `outside_target` | Sofia, Berlin, München, Hamburg, Frankfurt, Stuttgart, notable non-DE anchors, explicit “outside Germany”. |

Synced + reprocessed jobs **always persist** regardless of bucket — low priority is a label, not a delete.

Supporting fields:

- `location_reason`: short rationale string for UI + audits.
- `target_fit`, `discovery_status`, GPT outputs remain untouched by this scorer.

Data columns (see SQLite `jobs`): `location_fit`, `location_reason`.

## Pins, deletes, categories

| Feature | Mechanics |
| --- | --- |
| **Pin** | `jobs.pinned` (`0|1`). Toggled via POST `/job/<id>/pin`; ordering `pinned DESC, updated_at DESC`. |
| **Soft delete** | `jobs.deleted_at` ISO timestamp — dashboard hides rows unless “Show deleted”. Restore clears timestamp. Gmail rows untouched. |
| **Category** | `jobs.category` constrained to enums in `services/category_helper.py`. Heuristic seeds new mail, GPT suggests `job_category`, `update_job_category_if_unlocked` writes only when `category_locked = 0`. Manual saves set `category_locked = 1` by default via UI checkbox (uncheck to let AI refresh later). |

## Reprocess semantics

Batch + single paths re-run:

1. Heuristic target fit from stored job fields + originating email transcript.
2. NRW scorer (`location_fit` / `location_reason`).
3. Discovery stack + GPT enrichment.
4. GPT category suggestion respecting locks.

Preserves: workflow `status`, `pinned`, `deleted_at`, user-locked categories, manually edited notes via existing forms.

## Tauri desktop shell (optional thin wrapper)

The packaged tree at `/Users/zay/Desktop/Job-Pipeline-App` includes `src-tauri/` generated with **Tauri 2** tooling.

Because the UI is Flask-served HTML, **`cargo tauri dev` expects Flask already listening on `127.0.0.1:5000`** (see `src-tauri/tauri.conf.json → build.devUrl`).

### Everyday desktop flow

1. **Terminal A — Flask**

   ```bash
   cd "/Users/zay/Desktop/Job-Pipeline-App"
   source .venv/bin/activate
   export FLASK_RUN_PORT=5000   # default; change both here + devUrl if needed
   python3 app.py
   ```

2. **Terminal B — Tauri**

   ```bash
   cd "/Users/zay/Desktop/Job-Pipeline-App/src-tauri"
   cargo tauri dev
   ```

That opens an embedded WKWebView pointed at `http://127.0.0.1:5000`, so Gmail OAuth + OpenAI behave exactly like the browser build.

### When CLI dependencies are missing

Install commands (free tooling only):

```bash
# Rust toolchain
curl https://sh.rustup.rs -sSf | sh

# Xcode CLT on macOS (for Apple frameworks)
xcode-select --install
```

Restart the terminal, then rerun `cargo tauri dev`.

### Production build caveat

`cargo tauri bundle` expects the `frontendDist` folder (`../.static-placeholder`) for icons + packaging. Flask itself is **not bundled** automatically; distributing a `.app` that talks to SQLite still requires you to bundle Python (PyInstaller/etc.) separately or run Flask locally alongside the packaged shell. Until you automate that pairing, ship the Flask instructions above as your default “desktop mode”.

## Maintenance sync

Whenever you tweak code under `job-pipline/`, rerun:

```bash
rsync -a --exclude '.venv' --exclude '__pycache__' --exclude '*.pyc' \
  "/Users/zay/Desktop/02_PROJECTS_SOURCE/job-pipline/" \
  "/Users/zay/Desktop/Job-Pipeline-App/"
```

This refreshes Flask assets without clobbering your Desktop-only `instance/*.db`.

## Troubleshooting

- **`location_fit` blank after upgrade** — Launch once (`python3 app.py`) so migrations run (`init_db()` executes ALTERs).
- **Category never updates via AI** — Clear the “Lock category” checkbox or SQL `UPDATE jobs SET category_locked=0 WHERE id=?`.
- **Tauri loads blank screen** — Confirm Flask reachable in Safari/Chrome at the same URL + port printed in Flask logs.

## Support stance

Everything stays local/offline-capable minus Gmail/OpenAI outbound calls — perfect for tinkering offline with cached data once tokens exist.

Never share `.env`, `credentials.json`, or `token.json`.
