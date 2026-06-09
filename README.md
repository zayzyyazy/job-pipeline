# Job Pipeline

**AI-powered job pipeline for filtering, understanding, and applying smarter.**

## What it does

Job Pipeline is a local-first tool for turning noisy job intake into practical decisions and faster applications.

- Ingests jobs from **Gmail** and **manual input**
- Analyzes each role with **structured AI scoring** (not just free-form LLM text)
- Classifies roles (for example: AI workflow vs backend-heavy)
- Shows what you would actually do, required skills, and tool stack
- Supports action with **Apply Assist** and **Application Packet**
- Runs locally with **Flask + SQLite**, with a desktop shell for daily use

It is a decision tool first, not a generic tracker.

## Why this exists

Most job search pipelines fail for one reason: volume beats judgment.

- Too many jobs to review carefully
- “AI” in titles often hides mismatched backend or generic support work
- Requirements are unclear until late in the process
- People waste time applying to jobs that looked relevant but were not

Job Pipeline exists to force clarity early: what this role is, how it fits, and whether you should apply.

## Author

**zayzyyazy** — https://github.com/zayzyyazy

## Key features

- **Structured AI scoring** with explicit components and mismatch penalties
- **Match score + recommendation** with concise reasoning
- **What you would actually do** (task-focused breakdown)
- **Skills + tools** grouped into Required / Tools / Nice to have
- **Required skill match** indicator (`X / Y`)
- **Apply Assist** for safe form-filling support
- **Application Packet** generator for copy/paste applications
- **Gmail ingestion + deduplication**
- **Responsive UI** that stays usable in half-screen
- **Desktop app** flow: double-click and run locally

## How it works (architecture)

### Frontend

- Flask server-rendered templates (`templates/`)
- Lightweight static styling (`static/style.css`)

### Backend

- Flask app routes and page composition (`app.py`)
- SQLite persistence (`database/`)
- AI analysis + scoring/classification services (`services/`)

### Desktop

- Python-native desktop wrapper (`PyWebView`) in `Job-Pipeline-Desktop-CLEAN`
- Starts local Flask backend and opens `http://127.0.0.1:5000`

### Why PyWebView instead of Tauri now

We intentionally moved away from Tauri bundling for this phase.

- Avoids stale bundle/sync drift issues
- Removes Rust + Python packaging complexity for day-to-day iteration
- Keeps one runtime path (Python) and faster release loops

## Example output

**Job:** AI Automation Engineer  
**Match:** 68% (**Review**)

**What you would actually do**

- Build LLM workflows that turn support messages into structured actions
- Connect CRM/email/ticketing APIs so AI outputs update records automatically
- Debug prompts, tool calls, retries, and fallback behavior

**Skills / tools (highlights)**

- Required: Python, API integrations, workflow automation
- Tools: FastAPI, Docker, OpenAI API
- Nice to have: Kubernetes, SQL

This is the point of the product: quick, concrete decision context before you spend effort applying.

## Getting started

### Run locally

```bash
cd Job-Pipeline-App
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Then open: `http://127.0.0.1:5000`

### Desktop app

```bash
cd Job-Pipeline-Desktop-CLEAN
bash build_desktop.sh
open dist/Job\ Pipeline.app
```

## Project structure

- `Job-Pipeline-App/` — main Flask app (ingestion, analysis, UI, SQLite)
- `Job-Pipeline-Desktop-CLEAN/` — Python-native desktop wrapper/build project (PyWebView + PyInstaller)

## Limitations

- Output quality depends on source job text quality
- Desktop wrapper currently expects local app paths (not a fully path-agnostic standalone binary yet)
- Classification/scoring are structured but still heuristic

## Roadmap

- Better company understanding from sparse postings
- More precise skill matching against profile strength levels
- Cleaner standalone desktop packaging
- Further dashboard simplification for daily triage
