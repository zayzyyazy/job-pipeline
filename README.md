# Job Pipeline

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![Flask](https://img.shields.io/badge/Flask-Web%20UI-000?logo=flask&logoColor=white)
![SQLite](https://img.shields.io/badge/SQLite-Local-003B57?logo=sqlite&logoColor=white)
![Local-first](https://img.shields.io/badge/Privacy-Local--first-success)
![AI-powered](https://img.shields.io/badge/OpenAI-Relevance%20%26%20skills-purple)

**An AI-powered, local-first job research assistant that turns Gmail job alerts into structured, researched opportunities—not just another inbox summary.**

Job Pipeline pulls job-related email from Gmail, extracts each lead (title, company, location, link), optionally **surfaces real employer / ATS postings** when possible, and runs structured AI analysis: summary, skills, tooling, recommendation, and fit signals. Everything lives on your machine: **SQLite** storage, Flask UI, OAuth to Google and OpenAI from your own keys—**no SaaS backend** shipping your inbox or resumes to someone else’s cloud.

---

## Why this exists

Job boards scatter context; email alerts bury the JD in snippets. Job Pipeline treats **email as the signal**, not the source of truth: it organizes leads, prefers **verified posting text**, and writes clear takeaways so you can decide faster—**Saved**, **Applied**, or **Ignored**—without losing auditability.

---

## What makes it different

| Aspect | Typical flow | Job Pipeline |
| -------- | ------------- | ------------- |
| Input | Paste links or doom-scroll boards | Gmail job alerts synced locally |
| “What is this role?” | Skim recruiter blurbs | Structured AI + employer text when available |
| Posting depth | Snapshot only | **Source quality**: full posting vs partial vs email snapshot |
| Research | Manual tab hunting | **Research real posting** (local HTML search; no scraping farm) |
| Trust & privacy | Web apps hoover data | **Local-first**: DB and tokens on disk you control |

---

## Key features

- **Gmail integration** — OAuth (desktop-style client); ingest recent job mail with configurable query.
- **Pipeline & parsing** — Heuristic extraction with AI fallback when the email is ambiguous.
- **AI enrichment** — Summary, relevance, skills, tools, recommendation & score aligned to automation / AI / ops-style roles (configurable model via `.env`).
- **Job research** — User-triggered pass to locate public employer/ATS postings and refresh analysis when better text exists.
- **Source quality** — Transparent labels (e.g. full posting vs email snapshot); conservative behavior when full text isn’t verified.
- **Manual paste fallback** — Paste full JD when search doesn’t surface a page; overrides thin email context.
- **Dashboard & workflow** — Job cards with filters/pinning; detail view with debug-friendly panels; statuses and category.
- **Local-first architecture** — SQLite database path under `instance/` by default; no bespoke cloud API for your core data.

---

## How it works (non-technical)

1. **Connect Gmail** — One-time OAuth; tokens stored locally (`token.json`; git-ignored).
2. **Sync job emails** — Pull recent alerts matching your query.
3. **Extract leads** — Title, company, location, links come from structured parsing (+ AI fallback when needed).
4. **Research real postings** *(optional)* — Try to locate a fuller public job page before relying on snippets.
5. **AI analyzes** — Summaries, skills, tooling, recommendation—grounded by the best posting text available and source quality rules.
6. **Track decisions** — Mark jobs Saved / Applied / Ignored from dashboard or detail; keep NRW/target heuristics in view while you prioritize.

---

## Screens & UX (what you’ll see)

- **Dashboard** — Filterable grid of jobs: identity, NRW-ish location cues, category, recommendation/source-quality badges, quick actions (pin / status).
- **Job detail** — Full narrative: links (open/copy), AI summary & relevance, skill chips, tooling, recommendation, collapsible AI debug, paste + **force AI re-run** for recovery.
- **Research & enrichment** — Prominent **Research real job posting** on detail; textarea for **manual paste** when search fails.

_No screenshots are bundled here; add your own captures under `.github/` or `docs/` if you publish a showcase._

---

## Tech stack

| Layer | Choice |
| ----- | ------ |
| App server | Python **Flask** (`app.py`) |
| Persistence | **SQLite** (default `instance/job_pipeline.db`) |
| Mail | **Gmail API** (Google OAuth desktop client JSON) |
| Intelligence | **OpenAI** API (`OPENAI_MODEL`, default sensible mini model) |
| UI | **Jinja** templates + static **CSS** |
| Desktop *(optional)* | **Tauri** wraps the Flask URL in a native window when `src-tauri/` is present |

---

## Philosophy: local-first

- **Your data stays on your machine**: messages and analysis land in SQLite; secrets are `.env`, `credentials.json`, and `token.json`—never committed.
- **No multitenant backend**: you operate the Flask process; outbound calls are Gmail + OpenAI (and lightweight HTML discovery), which you explicitly configure.
- **You can inspect everything**: expandable debug on job detail complements logs for repeatable troubleshooting.

---

## Setup (step-by-step)

### 1. Clone

```bash
git clone https://github.com/<you>/job-pipeline.git
cd job-pipeline
```

### 2. Virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment file

```bash
cp .env.example .env
```

Edit `.env`:

- **`OPENAI_API_KEY`** — required for full enrichment (graceful degraded mode exists without key).
- **`FLASK_SECRET_KEY`** — production sessions; change from example.
- Optional: `OPENAI_MODEL`, `GMAIL_QUERY`, `FETCH_JOB_PAGES`, `ENABLE_DDG_DISCOVERY`, paths below.

Paths (defaults):

- `GMAIL_CREDENTIALS_PATH=credentials.json`
- `GMAIL_TOKEN_PATH=token.json`
- `DB_PATH=instance/job_pipeline.db`

### 5. Gmail API (important)

1. Open [Google Cloud Console](https://console.cloud.google.com/) and select/create a project.
2. **Enable Gmail API** for that project (**APIs & Services → Library**).
3. **OAuth consent screen** — configure as needed (often “External” + test users during development).
4. **Credentials → Create OAuth client ID → Application type: Desktop app** (the flow matches desktop token storage).
5. **Download JSON** → save as **`credentials.json`** at the repository root *(or path you set with `GMAIL_CREDENTIALS_PATH`)*.

On **first Gmail sync**, a browser opens; authorize once. **`token.json`** is written locally (`git-ignored`). Never commit **`credentials.json`**, **`token.json`**, or **`.env`**.

### 6. Run the web app

```bash
python app.py
```

On macOS/Linux you can use:

```bash
python3 app.py
```

Then open **`http://127.0.0.1:5000`** — or the port set by `FLASK_RUN_PORT` / `PORT`.

### 7. Everyday flow

Use **Sync** on the dashboard, open a job card, optionally **Research real job posting**, adjust status, repeat.

---

## Run as a desktop app (Tauri)

If your tree includes **`src-tauri/`** (Tauri + Rust wrapper loading the Flask site):

```bash
# Terminal 1 — keep Flask reachable (default http://127.0.0.1:5000)
python app.py

# Terminal 2 — desktop shell (from repo root once src-tauri exists)
cd src-tauri
cargo tauri dev
```

**Flask must stay running**: Tauri wraps a WebView pointing at your local URL. If `src-tauri/` is absent, use the browser workflow only or scaffold with `cargo tauri init`.

---

## Project structure

```
.
├── app.py              # Flask app, routes, dashboard & job detail orchestration
├── config.py           # Environment-backed settings (API keys paths, toggles)
├── requirements.txt    # Python dependencies
├── .env.example        # Safe template — copy to .env
├── database/
│   └── db.py           # SQLite schema, migrations, queries, upserts
├── services/
│   ├── pipeline.py     # Sync, reprocess, research, AI refresh flows
│   ├── gmail_service.py
│   ├── ai_service.py   # Prompting, guardrails, fallbacks
│   ├── parser.py       # Lead extraction (+ AI extraction fallback path)
│   ├── job_discovery.py
│   └── ...             # Filters, targeting, classification helpers
├── templates/          # HTML (Jinja) views
├── static/             # CSS & assets
└── instance/           # Default SQLite folder (usually git-ignored)
```

---

## Future improvements *(ideas)*

- Packaged installers (single `.dmg` / `.exe`) atop Tauri releases.
- Pluggable connectors beyond Gmail (filesystem drop, webhook).
- Smarter ATS coverage / locale-specific boards while staying scraping-policy friendly.
- Test harness with fixture emails for regressions.

---

## Why this project matters (portfolio framing)

Built to automate a **real personal workflow**, Job Pipeline mixes **product sense**—what to show busy job seekers—with **engineering**: OAuth, sync pipelines, heuristic + LLM enrichment, pragmatic privacy, and a UI oriented around decisions, not dashboards for their own sake. It demonstrates **end-to-end delivery**: APIs, persistence, presentation, and responsible AI use constrained by traceable inputs.

---

## License / credits

Specify your preferred license here (e.g. MIT). Attribution welcome if you reuse or fork.
