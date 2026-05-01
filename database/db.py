import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from config import settings

VALID_STATUSES = {"New", "Saved", "Applied", "Ignored"}


def _utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _row_to_dict(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    """sqlite3.Row has no .get(); convert early for pipeline / service logic."""
    if row is None:
        return None
    return dict(row)


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def _migrate(conn: sqlite3.Connection) -> None:
    job_cols = set(_table_columns(conn, "jobs"))
    if "target_fit" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN target_fit TEXT")
    if "target_score" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN target_score INTEGER")
    if "target_matched_keywords" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN target_matched_keywords TEXT")
    if "job_page_text" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN job_page_text TEXT")
    if "discovered_url" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN discovered_url TEXT")
    if "discovered_source" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN discovered_source TEXT")
    if "discovered_text" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN discovered_text TEXT")
    if "discovery_status" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN discovery_status TEXT")
    if "discovery_reason" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN discovery_reason TEXT")
    if "quality_flag" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN quality_flag TEXT")
    if "location_fit" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN location_fit TEXT DEFAULT 'unclear'")
    if "location_reason" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN location_reason TEXT")
    if "category" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN category TEXT DEFAULT 'Other'")
    if "category_locked" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN category_locked INTEGER DEFAULT 0")
    if "pinned" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN pinned INTEGER DEFAULT 0")
    if "deleted_at" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN deleted_at TEXT")
    if "source_quality" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN source_quality TEXT DEFAULT 'email_snapshot'")

    conn.execute(
        """
        UPDATE jobs
        SET category = 'Other'
        WHERE category IS NULL OR TRIM(category) = ''
        """
    )
    conn.execute(
        """
        UPDATE jobs
        SET location_fit = 'unclear'
        WHERE location_fit IS NULL OR TRIM(location_fit) = ''
        """
    )

    ai_cols = set(_table_columns(conn, "ai_analysis"))
    if "clean_title" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN clean_title TEXT")
    if "remote" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN remote INTEGER")
    if "why_relevant" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN why_relevant TEXT")
    if "nice_to_have_skills" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN nice_to_have_skills TEXT")
    if "tools_technologies" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN tools_technologies TEXT")
    if "automation_ai_relevance" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN automation_ai_relevance TEXT")
    if "enrichment_sources_used" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN enrichment_sources_used TEXT")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS pipeline_rejections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email_id INTEGER,
            title TEXT,
            company TEXT,
            job_link TEXT,
            stage TEXT NOT NULL,
            reject_reason TEXT,
            target_fit TEXT,
            target_score INTEGER,
            detail_json TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY(email_id) REFERENCES emails(id)
        );
        """
    )


def init_db() -> None:
    db_path = Path(settings.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS emails (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                gmail_message_id TEXT NOT NULL UNIQUE,
                thread_id TEXT,
                sender TEXT,
                subject TEXT,
                snippet TEXT,
                body TEXT,
                source TEXT,
                received_at TEXT,
                raw_payload TEXT,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email_id INTEGER,
                source TEXT,
                title TEXT NOT NULL,
                company TEXT NOT NULL,
                location TEXT,
                job_link TEXT,
                description TEXT,
                status TEXT NOT NULL DEFAULT 'New',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(title, company, job_link),
                FOREIGN KEY(email_id) REFERENCES emails(id)
            );

            CREATE TABLE IF NOT EXISTS ai_analysis (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL UNIQUE,
                summary TEXT,
                skills TEXT,
                recommendation TEXT,
                score INTEGER,
                reasoning TEXT,
                raw_response TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS status_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                changed_at TEXT NOT NULL,
                note TEXT,
                FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
            );
            """
        )
        _migrate(conn)


@contextmanager
def get_conn():
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def insert_email_if_new(email: Dict[str, Any]) -> Optional[int]:
    now = _utc_now()
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM emails WHERE gmail_message_id = ?", (email["gmail_message_id"],)).fetchone()
        if row:
            return None
        cur = conn.execute(
            """
            INSERT INTO emails (gmail_message_id, thread_id, sender, subject, snippet, body, source, received_at, raw_payload, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                email.get("gmail_message_id"),
                email.get("thread_id"),
                email.get("sender"),
                email.get("subject"),
                email.get("snippet"),
                email.get("body"),
                email.get("source"),
                email.get("received_at"),
                json.dumps(email.get("raw_payload", {}), ensure_ascii=True),
                now,
            ),
        )
        return int(cur.lastrowid)


def upsert_job(job: Dict[str, Any]) -> int:
    now = _utc_now()
    t_keywords = job.get("target_matched_keywords")
    if isinstance(t_keywords, list):
        t_keywords = json.dumps(t_keywords, ensure_ascii=True)

    with get_conn() as conn:
        row = conn.execute(
            "SELECT id FROM jobs WHERE title = ? AND company = ? AND COALESCE(job_link, '') = COALESCE(?, '')",
            (job["title"], job["company"], job.get("job_link")),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE jobs SET
                    email_id = COALESCE(?, email_id),
                    source = COALESCE(?, source),
                    location = ?,
                    description = ?,
                    target_fit = ?,
                    target_score = ?,
                    target_matched_keywords = ?,
                    job_page_text = ?,
                    discovered_url = ?,
                    discovered_source = ?,
                    discovered_text = ?,
                    discovery_status = ?,
                    discovery_reason = ?,
                    quality_flag = ?,
                    location_fit = ?,
                    location_reason = ?,
                    source_quality = COALESCE(?, source_quality),
                    updated_at = ? WHERE id = ?
                """,
                (
                    job.get("email_id"),
                    job.get("source"),
                    job.get("location"),
                    job.get("description"),
                    job.get("target_fit"),
                    job.get("target_score"),
                    t_keywords,
                    job.get("job_page_text"),
                    job.get("discovered_url"),
                    job.get("discovered_source"),
                    job.get("discovered_text"),
                    job.get("discovery_status"),
                    job.get("discovery_reason"),
                    job.get("quality_flag"),
                    job.get("location_fit") or "unclear",
                    job.get("location_reason") or "",
                    job.get("source_quality"),
                    now,
                    int(row["id"]),
                ),
            )
            return int(row["id"])

        cur = conn.execute(
            """
            INSERT INTO jobs (
                email_id, source, title, company, location, job_link, description,
                target_fit, target_score, target_matched_keywords, job_page_text,
                discovered_url, discovered_source, discovered_text,
                discovery_status, discovery_reason, quality_flag,
                location_fit, location_reason, source_quality, category, category_locked, pinned, deleted_at,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'New', ?, ?)
            """,
            (
                job.get("email_id"),
                job.get("source"),
                job["title"],
                job["company"],
                job.get("location"),
                job.get("job_link"),
                job.get("description"),
                job.get("target_fit"),
                job.get("target_score"),
                t_keywords,
                job.get("job_page_text"),
                job.get("discovered_url"),
                job.get("discovered_source"),
                job.get("discovered_text"),
                job.get("discovery_status"),
                job.get("discovery_reason"),
                job.get("quality_flag"),
                job.get("location_fit") or "unclear",
                job.get("location_reason") or "",
                job.get("source_quality") or "email_snapshot",
                job.get("category") or "Other",
                int(job.get("category_locked") or 0),
                int(job.get("pinned") or 0),
                job.get("deleted_at"),
                now,
                now,
            ),
        )
        job_id = int(cur.lastrowid)
        conn.execute(
            "INSERT INTO status_history (job_id, old_status, new_status, changed_at, note) VALUES (?, ?, ?, ?, ?)",
            (job_id, None, "New", now, "Initial status"),
        )
        return job_id


def update_job_discovery(job_id: int, fields: Dict[str, Any]) -> None:
    allowed = (
        "job_page_text",
        "discovered_url",
        "discovered_source",
        "discovered_text",
        "discovery_status",
        "discovery_reason",
        "quality_flag",
        "source_quality",
    )
    now = _utc_now()
    sets = []
    vals: List[Any] = []
    for key in allowed:
        if key in fields:
            sets.append(f"{key} = ?")
            vals.append(fields[key])
    if not sets:
        return
    vals.extend([now, job_id])
    with get_conn() as conn:
        conn.execute(f"UPDATE jobs SET {', '.join(sets)}, updated_at = ? WHERE id = ?", vals)


def get_email_by_id(email_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if not email_id:
        return None
    with get_conn() as conn:
        return _row_to_dict(conn.execute("SELECT * FROM emails WHERE id = ?", (email_id,)).fetchone())


def list_job_ids_ordered(limit: Optional[int] = None) -> List[int]:
    """Non-deleted jobs only, pinned first for stable UX batches."""
    with get_conn() as conn:
        q = "SELECT id FROM jobs WHERE deleted_at IS NULL ORDER BY pinned DESC, updated_at DESC"
        params: tuple = ()
        if limit:
            q += " LIMIT ?"
            params = (int(limit),)
        return [int(r["id"]) for r in conn.execute(q, params).fetchall()]


def insert_pipeline_rejection(
    email_id: Optional[int],
    title: str,
    company: str,
    job_link: str,
    stage: str,
    reject_reason: str,
    target_fit: str,
    target_score: int,
    detail: Dict[str, Any],
) -> None:
    now = _utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO pipeline_rejections (
                email_id, title, company, job_link, stage, reject_reason,
                target_fit, target_score, detail_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                email_id,
                title,
                company,
                job_link or "",
                stage,
                reject_reason,
                target_fit,
                target_score,
                json.dumps(detail, ensure_ascii=True),
                now,
            ),
        )


def _normalized_json_skills(payload: Optional[Any]) -> str:
    if payload is None:
        return json.dumps([], ensure_ascii=True)
    if isinstance(payload, list):
        clean = [str(x).strip() for x in payload if str(x).strip()]
        return json.dumps(clean, ensure_ascii=True)
    if isinstance(payload, str):
        stripped = payload.strip()
        if not stripped:
            return json.dumps([], ensure_ascii=True)
        if stripped.startswith("["):
            try:
                data = json.loads(stripped)
                if isinstance(data, list):
                    clean = [str(x).strip() for x in data if str(x).strip()]
                    return json.dumps(clean, ensure_ascii=True)
            except json.JSONDecodeError:
                pass
        parts = []
        for chunk in stripped.replace("•", ";").replace("\n", ";").replace("|", ";").split(";"):
            chunk = chunk.strip()
            if not chunk:
                continue
            for piece in chunk.split(","):
                token = piece.strip()
                if token:
                    parts.append(token)
        return json.dumps(parts, ensure_ascii=True)
    return json.dumps([], ensure_ascii=True)


def upsert_ai_analysis(job_id: int, data: Dict[str, Any]) -> None:
    now = _utc_now()
    skills_payload = (
        data.get("skills") or data.get("required_skills") or data.get("must_have_skills")
    )
    nice_payload = (
        data.get("nice_to_have_skills") or data.get("nice_to_have")
    )
    tools_payload = (
        data.get("tools_technologies")
        or data.get("tools_or_technologies")
        or data.get("technologies")
        or data.get("tools")
    )

    skills = _normalized_json_skills(skills_payload)
    nice = _normalized_json_skills(nice_payload)
    tools = _normalized_json_skills(tools_payload)

    remote = data.get("remote")
    if remote is True:
        remote_val = 1
    elif remote is False:
        remote_val = 0
    else:
        remote_val = None

    payload = (
        data.get("summary"),
        skills,
        data.get("recommendation"),
        data.get("score"),
        data.get("reasoning"),
        json.dumps(data.get("raw_response", {}), ensure_ascii=True),
        data.get("clean_title"),
        remote_val,
        data.get("why_relevant"),
        nice,
        tools,
        data.get("automation_ai_relevance"),
        data.get("enrichment_sources_used"),
    )
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM ai_analysis WHERE job_id = ?", (job_id,)).fetchone()
        if row:
            conn.execute(
                """
                UPDATE ai_analysis SET
                    summary = ?, skills = ?, recommendation = ?, score = ?, reasoning = ?, raw_response = ?,
                    clean_title = ?, remote = ?, why_relevant = ?, nice_to_have_skills = ?,
                    tools_technologies = ?, automation_ai_relevance = ?, enrichment_sources_used = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (*payload, now, job_id),
            )
            return

        conn.execute(
            """
            INSERT INTO ai_analysis (
                job_id, summary, skills, recommendation, score, reasoning, raw_response,
                clean_title, remote, why_relevant, nice_to_have_skills, tools_technologies,
                automation_ai_relevance, enrichment_sources_used, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (job_id, *payload, now, now),
        )


def list_jobs(
    status: Optional[str] = None,
    source: Optional[str] = None,
    target_fit: Optional[str] = None,
    discovery_status: Optional[str] = None,
    location_fit: Optional[str] = None,
    category: Optional[str] = None,
    search: Optional[str] = None,
    pinned_only: bool = False,
    show_deleted: bool = False,
    strict_focus: bool = False,
) -> Iterable[sqlite3.Row]:
    query = """
        SELECT jobs.id, jobs.title, jobs.company, jobs.location, jobs.source, jobs.status, jobs.job_link, jobs.updated_at,
               jobs.target_fit, jobs.target_score, jobs.discovery_status, jobs.discovered_source,
               jobs.source_quality,
               jobs.location_fit, jobs.location_reason, jobs.category, jobs.pinned, jobs.deleted_at,
               ai_analysis.recommendation, ai_analysis.score AS score, ai_analysis.skills
        FROM jobs
        LEFT JOIN ai_analysis ON ai_analysis.job_id = jobs.id
        WHERE 1=1
    """
    params: List[Any] = []
    if strict_focus:
        query += " AND COALESCE(TRIM(jobs.status), '') != 'Ignored'"
        query += " AND COALESCE(TRIM(jobs.location_fit), 'unclear') IN ('nrw', 'remote_germany', 'unclear')"
        query += """ AND (
            jobs.target_fit IS NULL OR TRIM(COALESCE(jobs.target_fit, '')) = ''
            OR LOWER(TRIM(jobs.target_fit)) IN ('strong', 'medium')
        )"""
    if not show_deleted:
        query += " AND jobs.deleted_at IS NULL"
    if status:
        query += " AND jobs.status = ?"
        params.append(status)
    if source:
        query += " AND jobs.source = ?"
        params.append(source)
    if target_fit:
        query += " AND jobs.target_fit = ?"
        params.append(target_fit)
    if discovery_status:
        query += " AND COALESCE(jobs.discovery_status, '') = ?"
        params.append(discovery_status)
    if location_fit:
        query += " AND COALESCE(jobs.location_fit, 'unclear') = ?"
        params.append(location_fit)
    if category:
        query += " AND COALESCE(jobs.category, '') = ?"
        params.append(category)
    if pinned_only:
        query += " AND jobs.pinned = 1"
    if search:
        like = f"%{search.strip()}%"
        query += """ AND (
            jobs.title LIKE ? OR jobs.company LIKE ?
            OR jobs.location LIKE ? OR jobs.category LIKE ?
            OR COALESCE(ai_analysis.skills, '') LIKE ?
        )"""
        params.extend([like, like, like, like, like])
    query += " ORDER BY jobs.pinned DESC, jobs.updated_at DESC"
    with get_conn() as conn:
        return conn.execute(query, params).fetchall()


def get_job(job_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        return _row_to_dict(
            conn.execute(
                """
            SELECT
                jobs.id AS id,
                jobs.email_id AS email_id,
                jobs.source AS source,
                jobs.title AS title,
                jobs.company AS company,
                jobs.location AS location,
                jobs.job_link AS job_link,
                jobs.description AS description,
                jobs.status AS status,
                jobs.created_at AS created_at,
                jobs.updated_at AS updated_at,
                jobs.target_fit AS target_fit,
                jobs.target_score AS target_score,
                jobs.target_matched_keywords AS target_matched_keywords,
                jobs.job_page_text AS job_page_text,
                jobs.discovered_url AS discovered_url,
                jobs.discovered_source AS discovered_source,
                jobs.discovered_text AS discovered_text,
                jobs.discovery_status AS discovery_status,
                jobs.discovery_reason AS discovery_reason,
                jobs.quality_flag AS quality_flag,
                jobs.location_fit AS location_fit,
                jobs.location_reason AS location_reason,
                jobs.source_quality AS source_quality,
                jobs.category AS category,
                jobs.category_locked AS category_locked,
                jobs.pinned AS pinned,
                jobs.deleted_at AS deleted_at,
                emails.sender AS sender,
                emails.subject AS email_subject,
                emails.snippet AS email_snippet,
                emails.body AS email_body,
                emails.received_at AS received_at,
                ai_analysis.summary AS summary,
                ai_analysis.skills AS skills,
                ai_analysis.recommendation AS recommendation,
                ai_analysis.score AS score,
                ai_analysis.reasoning AS reasoning,
                ai_analysis.raw_response AS raw_response,
                ai_analysis.clean_title AS clean_title,
                ai_analysis.remote AS remote,
                ai_analysis.why_relevant AS why_relevant,
                ai_analysis.nice_to_have_skills AS nice_to_have_skills,
                ai_analysis.tools_technologies AS tools_technologies,
                ai_analysis.automation_ai_relevance AS automation_ai_relevance,
                ai_analysis.enrichment_sources_used AS enrichment_sources_used
            FROM jobs
            LEFT JOIN emails ON emails.id = jobs.email_id
            LEFT JOIN ai_analysis ON ai_analysis.job_id = jobs.id
            WHERE jobs.id = ?
                """,
                (job_id,),
            ).fetchone()
        )


def get_ai_analysis_row(job_id: int) -> Optional[Dict[str, Any]]:
    """Full ai_analysis row for merge / audits."""
    with get_conn() as conn:
        return _row_to_dict(conn.execute("SELECT * FROM ai_analysis WHERE job_id = ?", (job_id,)).fetchone())


def get_ai_clean_title(job_id: int) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute("SELECT clean_title FROM ai_analysis WHERE job_id = ?", (job_id,)).fetchone()
        if not row or not row["clean_title"]:
            return None
        s = str(row["clean_title"]).strip()
        return s if s and s.lower() != "unknown" else None


def get_job_plain(job_id: int) -> Optional[Dict[str, Any]]:
    """Jobs table columns only — avoids ambiguity from JOIN duplicates."""
    with get_conn() as conn:
        return _row_to_dict(conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone())


def bundle_job_for_discovery(
    job_row: Any, email_row: Optional[Any], extras: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    jr = dict(job_row)
    e = dict(email_row) if email_row else {}
    x = extras or {}
    bundle: Dict[str, Any] = {
        "job_id": jr.get("id"),
        "title": jr.get("title") or "",
        "clean_title_ai": str(x.get("clean_title_ai") or ""),
        "company": jr.get("company") or "",
        "location": jr.get("location") or "",
        "job_link": jr.get("job_link") or "",
        "description": jr.get("description") or "",
        "source": jr.get("source") or "",
        "job_page_text": jr.get("job_page_text"),
        "discovered_url": jr.get("discovered_url"),
        "discovered_text": jr.get("discovered_text"),
        "discovered_source": jr.get("discovered_source"),
        "discovery_status": jr.get("discovery_status"),
        "discovery_reason": jr.get("discovery_reason"),
        "source_quality": jr.get("source_quality"),
        "target_fit": jr.get("target_fit"),
        "target_score": jr.get("target_score"),
        "target_matched_keywords": jr.get("target_matched_keywords"),
        "email_subject": e.get("subject") or "",
        "email_snippet": e.get("snippet") or "",
        "email_body_excerpt": (e.get("body") or "")[:6000],
    }
    mk = jr.get("target_matched_keywords")
    if mk and isinstance(mk, str):
        try:
            bundle["target_matched_keywords_json"] = json.loads(mk)
        except json.JSONDecodeError:
            bundle["target_matched_keywords_json"] = []
    return bundle


def get_status_history(job_id: int) -> Iterable[sqlite3.Row]:
    with get_conn() as conn:
        return conn.execute(
            "SELECT old_status, new_status, changed_at, note FROM status_history WHERE job_id = ? ORDER BY changed_at DESC",
            (job_id,),
        ).fetchall()


def update_job_status(job_id: int, new_status: str, note: str = "") -> bool:
    if new_status not in VALID_STATUSES:
        return False
    now = _utc_now()
    with get_conn() as conn:
        job = conn.execute("SELECT status FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not job:
            return False
        old_status = job["status"]
        if old_status == new_status:
            return True
        conn.execute("UPDATE jobs SET status = ?, updated_at = ? WHERE id = ?", (new_status, now, job_id))
        conn.execute(
            "INSERT INTO status_history (job_id, old_status, new_status, changed_at, note) VALUES (?, ?, ?, ?, ?)",
            (job_id, old_status, new_status, now, note),
        )
        return True


def toggle_job_pin(job_id: int) -> Optional[int]:
    """Toggle pin for a non-deleted job. Returns new pinned value or None."""
    now = _utc_now()
    with get_conn() as conn:
        row = conn.execute("SELECT pinned FROM jobs WHERE id = ? AND deleted_at IS NULL", (job_id,)).fetchone()
        if not row:
            return None
        cur = int(row["pinned"] or 0)
        new_val = 0 if cur else 1
        conn.execute(
            "UPDATE jobs SET pinned = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL",
            (new_val, now, job_id),
        )
        return new_val


def soft_delete_job(job_id: int) -> bool:
    now = _utc_now()
    with get_conn() as conn:
        row = conn.execute("SELECT deleted_at FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row:
            return False
        if row["deleted_at"]:
            return True
        conn.execute("UPDATE jobs SET deleted_at = ?, updated_at = ? WHERE id = ?", (now, now, job_id))
        return True


def restore_job(job_id: int) -> bool:
    now = _utc_now()
    with get_conn() as conn:
        row = conn.execute("SELECT deleted_at FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if not row or not row["deleted_at"]:
            return False
        conn.execute("UPDATE jobs SET deleted_at = NULL, updated_at = ? WHERE id = ?", (now, job_id))
        return True


def update_job_user_category(job_id: int, category: str, *, lock_user_edit: bool = True) -> bool:
    now = _utc_now()
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM jobs WHERE id = ? AND deleted_at IS NULL", (job_id,)).fetchone()
        if not row:
            return False
        conn.execute(
            "UPDATE jobs SET category = ?, category_locked = ?, updated_at = ? WHERE id = ?",
            (category, 1 if lock_user_edit else 0, now, job_id),
        )
        return True


def update_job_category_if_unlocked(job_id: int, category: str) -> None:
    now = _utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE jobs
            SET category = ?, updated_at = ?
            WHERE id = ?
              AND COALESCE(category_locked, 0) = 0
              AND deleted_at IS NULL
            """,
            (category, now, job_id),
        )


_GARBAGE_TITLES_LOW = {
    "e",
    "no",
    "n/a",
    "roles",
    "role",
    "jobs",
    "job",
    "listing",
    "role opening",
    "new jobs",
    "neue jobs",
    "opening",
}


def cleanup_low_quality_jobs() -> Dict[str, int]:
    """
    Conservative soft-delete for obviously noisy rows. Honors pinned/Saved/Applied.
    Returns summary counters for UI feedback (no destructive deletes beyond deleted_at stamp).
    """
    summary = {
        "checked": 0,
        "soft_deleted": 0,
        "skipped_pinned": 0,
        "skipped_saved_or_applied": 0,
    }

    def _looks_garbage_title(title_raw: Optional[str]) -> bool:
        t = str(title_raw or "").strip().lower()
        if not t:
            return True
        if len(t) <= 2:
            return True
        collapsed = "".join(ch for ch in t if not ch.isdigit()).strip()
        if len(collapsed) <= 2:
            return True
        return t in _GARBAGE_TITLES_LOW

    query = """
        SELECT jobs.id AS id, jobs.title AS title, jobs.pinned AS pinned, jobs.status AS status,
               jobs.location_fit AS location_fit, jobs.target_fit AS target_fit,
               ai_analysis.recommendation AS recommendation, ai_analysis.score AS score
        FROM jobs
        LEFT JOIN ai_analysis ON ai_analysis.job_id = jobs.id
        WHERE jobs.deleted_at IS NULL
    """

    now = _utc_now()
    with get_conn() as conn:
        rows = conn.execute(query).fetchall()
        for row in rows:
            summary["checked"] += 1

            pinned = bool(int(row["pinned"] or 0))
            if pinned:
                summary["skipped_pinned"] += 1
                continue
            status = str(row["status"] or "").strip()
            if status in ("Saved", "Applied"):
                summary["skipped_saved_or_applied"] += 1
                continue

            lf = str(row["location_fit"] or "").strip()
            tf = str(row["target_fit"] or "").strip().lower()
            reco = str(row["recommendation"] or "").strip().lower()
            score = row["score"]

            should_delete = False

            if _looks_garbage_title(row["title"]):
                should_delete = True
            elif lf == "outside_target" and tf in ("weak", "reject", ""):
                should_delete = True
            elif reco == "skip" and isinstance(score, int) and score < 40:
                should_delete = True

            if should_delete:
                conn.execute(
                    "UPDATE jobs SET deleted_at = ?, updated_at = ? WHERE id = ? AND deleted_at IS NULL",
                    (now, now, int(row["id"])),
                )
                summary["soft_deleted"] += 1

    return summary
