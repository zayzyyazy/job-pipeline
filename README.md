# Job Pipeline (Local-First)

Job Pipeline is a local Flask + SQLite app that reads job alert emails from Gmail, extracts job details, enriches each job with AI, and presents a clean dashboard to track decisions.

## Features
- Gmail OAuth desktop authentication
- Job email filtering (Indeed + LinkedIn)
- Rule-based extraction with AI fallback
- AI enrichment (summary, skills, recommendation, score)
- SQLite local storage with deduplication
- Dashboard + job detail + status history

## Project Structure

```text
.
├── app.py
├── config.py
├── requirements.txt
├── .env.example
├── database/
│   └── db.py
├── services/
│   ├── ai_service.py
│   ├── filtering.py
│   ├── gmail_service.py
│   ├── parser.py
│   └── pipeline.py
├── templates/
│   ├── base.html
│   ├── dashboard.html
│   └── job_detail.html
└── static/
    └── style.css
```

## Setup

1) Create and activate virtual environment
```bash
python3 -m venv .venv
source .venv/bin/activate
```

2) Install dependencies
```bash
pip install -r requirements.txt
```

3) Create env file
```bash
cp .env.example .env
```

4) Fill `.env`
- `OPENAI_API_KEY`
- optional `OPENAI_MODEL` (default: `gpt-4o-mini`)
- optional Gmail paths/query

## Gmail API Setup (Desktop OAuth)

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create/select project
3. Enable **Gmail API**
4. Configure OAuth consent screen
5. Create OAuth Client ID of type **Desktop app**
6. Download client JSON and save as `credentials.json` in project root

At first sync, browser auth will open and save `token.json` locally.

## Run

```bash
python app.py
```

Open: <http://127.0.0.1:5000>

## Usage Flow

1. Click **Sync Emails**
2. App fetches Gmail emails
3. Filters job-related messages
4. Extracts job data
5. Enriches with AI
6. Stores in `instance/job_pipeline.db`
7. Dashboard updates

## Notes

- If OpenAI key is missing, fallback analysis is used.
- Raw Gmail payload is stored in DB for debugging.
- Job dedupe key: `(title, company, job_link)`.
