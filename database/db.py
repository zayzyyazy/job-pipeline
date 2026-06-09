import json
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urlparse

from config import settings

VALID_STATUSES = {"New", "Saved", "Applied", "Ignored"}


def _utc_now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")


def _assert_sql_placeholders_match_params(sql: str, params: Sequence[Any]) -> None:
    """Fail fast when INSERT/UPDATE `?` count does not match the bound sequence length."""
    n = sql.count("?")
    if n != len(params):
        raise AssertionError(
            f"SQL bind mismatch: {n} placeholders vs {len(params)} params (statement starts: {sql[:120].strip()!r}…)"
        )


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
    if "job_deadline" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN job_deadline TEXT")
    if "job_status" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN job_status TEXT")
    if "career_fit" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN career_fit TEXT")
    if "career_fit_score" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN career_fit_score INTEGER")
    if "career_fit_reason" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN career_fit_reason TEXT")
    if "mismatch_reasons" not in job_cols:
        conn.execute("ALTER TABLE jobs ADD COLUMN mismatch_reasons TEXT")

    job_cols2 = set(_table_columns(conn, "jobs"))
    for col, ddl in (
        ("applied_detected_at", "ALTER TABLE jobs ADD COLUMN applied_detected_at TEXT"),
        ("application_confirmation_email_id", "ALTER TABLE jobs ADD COLUMN application_confirmation_email_id INTEGER"),
        ("application_confirmation_subject", "ALTER TABLE jobs ADD COLUMN application_confirmation_subject TEXT"),
        ("application_confirmation_sender", "ALTER TABLE jobs ADD COLUMN application_confirmation_sender TEXT"),
        ("application_match_confidence", "ALTER TABLE jobs ADD COLUMN application_match_confidence INTEGER"),
        ("application_match_reason", "ALTER TABLE jobs ADD COLUMN application_match_reason TEXT"),
        ("extraction_confidence", "ALTER TABLE jobs ADD COLUMN extraction_confidence INTEGER"),
        ("extraction_reason", "ALTER TABLE jobs ADD COLUMN extraction_reason TEXT"),
        ("is_multi_job_email", "ALTER TABLE jobs ADD COLUMN is_multi_job_email INTEGER DEFAULT 0"),
        ("needs_manual_review", "ALTER TABLE jobs ADD COLUMN needs_manual_review INTEGER DEFAULT 0"),
        ("job_link_kind", "ALTER TABLE jobs ADD COLUMN job_link_kind TEXT"),
    ):
        if col not in job_cols2:
            conn.execute(ddl)
            job_cols2.add(col)

    job_cols3 = set(_table_columns(conn, "jobs"))
    if "application_url" not in job_cols3:
        conn.execute("ALTER TABLE jobs ADD COLUMN application_url TEXT")
    if "duplicate_of" not in job_cols3:
        conn.execute("ALTER TABLE jobs ADD COLUMN duplicate_of INTEGER")
    if "duplicate_reason" not in job_cols3:
        conn.execute("ALTER TABLE jobs ADD COLUMN duplicate_reason TEXT")
    if "duplicate_confidence" not in job_cols3:
        conn.execute("ALTER TABLE jobs ADD COLUMN duplicate_confidence INTEGER")
    if "is_duplicate" not in job_cols3:
        conn.execute("ALTER TABLE jobs ADD COLUMN is_duplicate INTEGER DEFAULT 0")

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS apply_assist_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            status TEXT NOT NULL,
            url TEXT,
            fields_detected TEXT,
            fields_filled TEXT,
            fields_skipped TEXT,
            manual_required TEXT,
            error_message TEXT,
            FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE
        );
        """
    )

    email_cols = set(_table_columns(conn, "emails"))
    for col, ddl in (
        ("email_type", "ALTER TABLE emails ADD COLUMN email_type TEXT"),
        ("classification_confidence", "ALTER TABLE emails ADD COLUMN classification_confidence INTEGER"),
        ("classification_reason", "ALTER TABLE emails ADD COLUMN classification_reason TEXT"),
    ):
        if col not in email_cols:
            conn.execute(ddl)
            email_cols.add(col)

    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT,
            updated_at TEXT NOT NULL
        );
        """
    )

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
    if "what_the_job_is" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN what_the_job_is TEXT")
    if "key_responsibilities" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN key_responsibilities TEXT")
    if "business_context" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN business_context TEXT")
    if "why_this_role_exists" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN why_this_role_exists TEXT")
    if "seniority_level" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN seniority_level TEXT")
    if "job_classification" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN job_classification TEXT")
    if "scoring_breakdown" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN scoring_breakdown TEXT")
    if "career_level" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN career_level TEXT")
    if "recommendation_career" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN recommendation_career TEXT")
    if "reality_check" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN reality_check TEXT")
    if "why_not_fit" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN why_not_fit TEXT")
    if "engineering_level_required" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN engineering_level_required TEXT")
    if "work_type_actual" not in ai_cols:
        conn.execute("ALTER TABLE ai_analysis ADD COLUMN work_type_actual TEXT")

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
                application_url TEXT,
                status TEXT NOT NULL DEFAULT 'New',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                duplicate_of INTEGER,
                duplicate_reason TEXT,
                duplicate_confidence INTEGER,
                is_duplicate INTEGER DEFAULT 0,
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
            INSERT INTO emails (
                gmail_message_id, thread_id, sender, subject, snippet, body, source, received_at, raw_payload, created_at,
                email_type, classification_confidence, classification_reason
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                email.get("email_type"),
                email.get("classification_confidence"),
                email.get("classification_reason"),
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
                    title = COALESCE(?, title),
                    company = COALESCE(?, company),
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
                    job_deadline = ?,
                    job_status = ?,
                    extraction_confidence = COALESCE(?, extraction_confidence),
                    extraction_reason = COALESCE(?, extraction_reason),
                    is_multi_job_email = COALESCE(?, is_multi_job_email),
                    needs_manual_review = COALESCE(?, needs_manual_review),
                    job_link_kind = COALESCE(?, job_link_kind),
                    updated_at = ? WHERE id = ?
                """,
                (
                    job.get("email_id"),
                    job.get("source"),
                    job.get("title"),
                    job.get("company"),
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
                    job.get("job_deadline"),
                    job.get("job_status"),
                    job.get("extraction_confidence"),
                    job.get("extraction_reason"),
                    job.get("is_multi_job_email"),
                    job.get("needs_manual_review"),
                    job.get("job_link_kind"),
                    now,
                    int(row["id"]),
                ),
            )
            return int(row["id"])

        insert_sql = """
            INSERT INTO jobs (
                email_id, source, title, company, location, job_link, description,
                target_fit, target_score, target_matched_keywords, job_page_text,
                discovered_url, discovered_source, discovered_text,
                discovery_status, discovery_reason, quality_flag,
                location_fit, location_reason, source_quality, job_deadline, job_status,
                category, category_locked, pinned, deleted_at,
                extraction_confidence, extraction_reason, is_multi_job_email, needs_manual_review, job_link_kind,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        insert_params: Tuple[Any, ...] = (
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
            job.get("job_deadline"),
            job.get("job_status"),
            job.get("category") or "Other",
            int(job.get("category_locked") or 0),
            int(job.get("pinned") or 0),
            job.get("deleted_at"),
            job.get("extraction_confidence"),
            job.get("extraction_reason"),
            int(job.get("is_multi_job_email") or 0),
            int(job.get("needs_manual_review") or 0),
            job.get("job_link_kind"),
            job.get("status") or "New",
            now,
            now,
        )
        _assert_sql_placeholders_match_params(insert_sql, insert_params)
        cur = conn.execute(insert_sql, insert_params)
        job_id = int(cur.lastrowid)
        initial_status = job.get("status") or "New"
        conn.execute(
            "INSERT INTO status_history (job_id, old_status, new_status, changed_at, note) VALUES (?, ?, ?, ?, ?)",
            (
                job_id,
                None,
                initial_status,
                now,
                "Initial status" if initial_status == "New" else "Created with this status",
            ),
        )
        return job_id


def update_job_after_career_judge(
    job_id: int,
    *,
    target_fit: str,
    target_score: int,
    target_matched_keywords: Any,
    career_fit: str,
    career_fit_score: int,
    career_fit_reason: str,
    mismatch_reasons: List[str],
) -> None:
    now = _utc_now()
    t_kw = target_matched_keywords
    if isinstance(t_kw, list):
        t_kw = json.dumps(t_kw, ensure_ascii=True)
    mm = mismatch_reasons if isinstance(mismatch_reasons, list) else []
    mm_json = json.dumps(mm, ensure_ascii=True)
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE jobs SET
                target_fit = ?,
                target_score = ?,
                target_matched_keywords = ?,
                career_fit = ?,
                career_fit_score = ?,
                career_fit_reason = ?,
                mismatch_reasons = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                target_fit,
                int(target_score),
                t_kw,
                career_fit,
                int(career_fit_score),
                career_fit_reason,
                mm_json,
                now,
                int(job_id),
            ),
        )


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
    kr_payload = data.get("key_responsibilities") or data.get("responsibilities")
    key_resp = _normalized_json_skills(kr_payload)

    remote = data.get("remote")
    if remote is True:
        remote_val = 1
    elif remote is False:
        remote_val = 0
    else:
        remote_val = None

    def _nz_text(val: Any) -> Optional[str]:
        if val is None:
            return None
        s = str(val).strip()
        return s if s else None

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
        _nz_text(data.get("what_the_job_is")),
        key_resp,
        _nz_text(data.get("business_context")),
        _nz_text(data.get("why_this_role_exists")),
        _nz_text(data.get("seniority_level")),
        json.dumps(data.get("job_classification", {}), ensure_ascii=True),
        json.dumps(data.get("scoring_breakdown", {}), ensure_ascii=True),
        _nz_text(data.get("career_level")),
        _nz_text(data.get("recommendation_career")),
        _nz_text(data.get("reality_check")),
        _nz_text(data.get("why_not_fit")),
        _nz_text(data.get("engineering_level_required")),
        _nz_text(data.get("work_type_actual")),
    )
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM ai_analysis WHERE job_id = ?", (job_id,)).fetchone()
        if row:
            conn.execute(
                """
                UPDATE ai_analysis SET
                    summary = ?, skills = ?, recommendation = ?, score = ?, reasoning = ?, raw_response = ?,
                    clean_title = ?, remote = ?, why_relevant = ?, nice_to_have_skills = ?,
                    tools_technologies = ?, automation_ai_relevance = ?, enrichment_sources_used = ?,
                    what_the_job_is = ?, key_responsibilities = ?, business_context = ?,
                    why_this_role_exists = ?, seniority_level = ?, job_classification = ?, scoring_breakdown = ?,
                    career_level = ?, recommendation_career = ?, reality_check = ?, why_not_fit = ?,
                    engineering_level_required = ?, work_type_actual = ?, updated_at = ?
                WHERE job_id = ?
                """,
                (*payload, now, job_id),
            )
            return

        insert_sql = """
            INSERT INTO ai_analysis (
                job_id, summary, skills, recommendation, score, reasoning, raw_response,
                clean_title, remote, why_relevant, nice_to_have_skills, tools_technologies,
                automation_ai_relevance, enrichment_sources_used,
                what_the_job_is, key_responsibilities, business_context, why_this_role_exists, seniority_level,
                job_classification, scoring_breakdown, career_level, recommendation_career, reality_check, why_not_fit,
                engineering_level_required, work_type_actual,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
        insert_params = (job_id, *payload, now, now)
        assert insert_sql.count("?") == len(insert_params), (
            f"ai_analysis INSERT placeholder/param mismatch: "
            f"{insert_sql.count('?')} placeholders vs {len(insert_params)} params"
        )

        conn.execute(
            insert_sql,
            insert_params,
        )


# Default “daily review” cohort: prioritized opportunities, hides noise until user opts in.
_SQL_REVIEW_FOCUS = """
(
  COALESCE(TRIM(jobs.status), '') != 'Ignored'
  AND COALESCE(jobs.location_fit, 'unclear') IN ('nrw', 'remote_germany', 'unclear')
  AND (
        jobs.job_status IS NULL
     OR TRIM(jobs.job_status) = ''
     OR LOWER(TRIM(jobs.job_status)) IN ('fresh', 'active')
  )
  AND (
       jobs.pinned = 1
    OR jobs.status IN ('Saved', 'Applied')
    OR LOWER(TRIM(COALESCE(NULLIF(TRIM(jobs.career_fit), ''), jobs.target_fit, ''))) IN ('strong', 'medium')
  )
  AND (
       jobs.pinned = 1
    OR jobs.status IN ('Saved', 'Applied')
    OR NOT (
         COALESCE(jobs.needs_manual_review, 0) = 1
         AND COALESCE(jobs.extraction_confidence, 100) < 55
       )
  )
)
"""


def dashboard_review_counts() -> Dict[str, int]:
    """Snapshot counts for the dashboard header (non-deleted jobs only unless noted)."""
    out = {
        "strong_fit": 0,
        "medium_fit": 0,
        "saved": 0,
        "applied": 0,
        "needs_review": 0,
        "hidden_noisy": 0,
    }
    eff = "LOWER(TRIM(COALESCE(NULLIF(TRIM(jobs.career_fit), ''), jobs.target_fit, '')))"
    nd = "jobs.deleted_at IS NULL"
    with get_conn() as conn:
        out["strong_fit"] = int(
            conn.execute(f"SELECT COUNT(*) FROM jobs WHERE {nd} AND {eff} = 'strong'", ()).fetchone()[0]
        )
        out["medium_fit"] = int(
            conn.execute(f"SELECT COUNT(*) FROM jobs WHERE {nd} AND {eff} = 'medium'", ()).fetchone()[0]
        )
        out["saved"] = int(
            conn.execute(f"SELECT COUNT(*) FROM jobs WHERE {nd} AND jobs.status = 'Saved'", ()).fetchone()[0]
        )
        out["applied"] = int(
            conn.execute(f"SELECT COUNT(*) FROM jobs WHERE {nd} AND jobs.status = 'Applied'", ()).fetchone()[0]
        )
        out["detected_applications"] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {nd} AND TRIM(COALESCE(jobs.applied_detected_at, '')) != ''",
                (),
            ).fetchone()[0]
        )
        out["needs_review"] = int(
            conn.execute(
                f"SELECT COUNT(*) FROM jobs WHERE {nd} AND COALESCE(TRIM(jobs.status), '') = 'New'",
                (),
            ).fetchone()[0]
        )
        out["hidden_noisy"] = int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM jobs
                WHERE jobs.deleted_at IS NULL
                  AND NOT ({_SQL_REVIEW_FOCUS.strip()})
                """,
                (),
            ).fetchone()[0]
        )
    return out


def list_jobs(
    status: Optional[str] = None,
    source: Optional[str] = None,
    career_fit: Optional[str] = None,
    discovery_status: Optional[str] = None,
    location_fit: Optional[str] = None,
    category: Optional[str] = None,
    source_quality: Optional[str] = None,
    freshness: Optional[str] = None,
    search: Optional[str] = None,
    pinned_only: bool = False,
    show_deleted: bool = False,
    review_focus: bool = False,
    application_tracked: bool = False,
    applied_scope: Optional[str] = None,
    show_duplicates: bool = False,
) -> Iterable[sqlite3.Row]:
    query = """
        SELECT jobs.id, jobs.title, jobs.company, jobs.location, jobs.source, jobs.status, jobs.job_link, jobs.updated_at,
               jobs.target_fit, jobs.target_score, jobs.career_fit, jobs.career_fit_score,
               jobs.discovery_status, jobs.discovered_source,
               jobs.source_quality,
               jobs.location_fit, jobs.location_reason, jobs.category, jobs.pinned, jobs.deleted_at,
               jobs.job_status, jobs.job_deadline,
               jobs.career_fit_reason, jobs.mismatch_reasons,
               jobs.applied_detected_at, jobs.application_confirmation_sender,
               jobs.application_confirmation_subject, jobs.application_match_confidence, jobs.application_match_reason,
               jobs.extraction_confidence, jobs.needs_manual_review, jobs.job_link_kind,
               jobs.is_duplicate, jobs.duplicate_of, jobs.duplicate_reason, jobs.duplicate_confidence,
               ai_analysis.recommendation, ai_analysis.score AS score, ai_analysis.skills,
               ai_analysis.tools_technologies, ai_analysis.what_the_job_is, ai_analysis.clean_title,
               ai_analysis.raw_response
        FROM jobs
        LEFT JOIN ai_analysis ON ai_analysis.job_id = jobs.id
        WHERE 1=1
    """
    params: List[Any] = []

    eff_fit = "LOWER(TRIM(COALESCE(NULLIF(TRIM(jobs.career_fit), ''), jobs.target_fit, '')))"
    eff_score = "COALESCE(jobs.career_fit_score, jobs.target_score, 0)"
    eff_js = "LOWER(TRIM(COALESCE(jobs.job_status, '')))"

    if review_focus:
        query += f" AND {_SQL_REVIEW_FOCUS.strip()}"
    if not show_deleted:
        query += " AND jobs.deleted_at IS NULL"
    if not show_duplicates:
        query += " AND COALESCE(jobs.is_duplicate, 0) = 0"
    if status:
        query += " AND jobs.status = ?"
        params.append(status)
    if applied_scope == "applied":
        query += " AND jobs.status = 'Applied'"
    elif applied_scope == "open":
        query += " AND COALESCE(jobs.status, '') != 'Applied'"
    if source:
        query += " AND jobs.source = ?"
        params.append(source)
    if career_fit:
        query += f" AND {eff_fit} = LOWER(TRIM(?))"
        params.append(career_fit)
    if discovery_status:
        query += " AND COALESCE(jobs.discovery_status, '') = ?"
        params.append(discovery_status)
    if location_fit:
        query += " AND COALESCE(jobs.location_fit, 'unclear') = ?"
        params.append(location_fit)
    if category:
        query += " AND COALESCE(jobs.category, '') = ?"
        params.append(category)
    if source_quality:
        query += " AND COALESCE(jobs.source_quality, '') = ?"
        params.append(source_quality)
    if freshness:
        fz = str(freshness).strip().lower()
        if fz == "unknown":
            query += " AND (jobs.job_status IS NULL OR TRIM(jobs.job_status) = '')"
        else:
            query += f" AND {eff_js} = LOWER(?)"
            params.append(fz)
    if pinned_only:
        query += " AND jobs.pinned = 1"
    if application_tracked:
        query += " AND TRIM(COALESCE(jobs.applied_detected_at, '')) != ''"
    if search:
        like = f"%{search.strip()}%"
        query += """ AND (
            jobs.title LIKE ? OR COALESCE(ai_analysis.clean_title, '') LIKE ?
            OR jobs.company LIKE ?
            OR jobs.location LIKE ? OR jobs.category LIKE ?
            OR COALESCE(ai_analysis.skills, '') LIKE ?
            OR COALESCE(ai_analysis.tools_technologies, '') LIKE ?
            OR COALESCE(ai_analysis.what_the_job_is, '') LIKE ?
            OR COALESCE(jobs.career_fit_reason, '') LIKE ?
        )"""
        params.extend([like, like, like, like, like, like, like, like, like])
    query += f"""
        ORDER BY
            jobs.pinned DESC,
            CASE jobs.status
                WHEN 'Applied' THEN 1
                WHEN 'Saved' THEN 2
                WHEN 'New' THEN 3
                WHEN 'Ignored' THEN 4
                ELSE 5
            END,
            CASE {eff_fit}
                WHEN 'strong' THEN 1
                WHEN 'medium' THEN 2
                WHEN 'weak' THEN 3
                WHEN 'reject' THEN 4
                ELSE 5
            END,
            {eff_score} DESC,
            CASE
                WHEN {eff_js} = 'fresh' THEN 1
                WHEN {eff_js} = 'active' THEN 2
                WHEN jobs.job_status IS NULL OR TRIM(jobs.job_status) = '' THEN 3
                WHEN {eff_js} = 'old' THEN 4
                WHEN {eff_js} = 'likely_expired' THEN 5
                ELSE 3
            END,
            jobs.updated_at DESC
        """
    with get_conn() as conn:
        rows = conn.execute(query, params).fetchall()
        if application_tracked:
            rows = sorted(
                rows,
                key=lambda r: str(r["applied_detected_at"] or ""),
                reverse=True,
            )
        return rows


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
                jobs.job_deadline AS job_deadline,
                jobs.job_status AS job_status,
                jobs.career_fit AS career_fit,
                jobs.career_fit_score AS career_fit_score,
                jobs.career_fit_reason AS career_fit_reason,
                jobs.mismatch_reasons AS mismatch_reasons,
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
                ai_analysis.enrichment_sources_used AS enrichment_sources_used,
                ai_analysis.what_the_job_is AS what_the_job_is,
                ai_analysis.key_responsibilities AS key_responsibilities,
                ai_analysis.business_context AS business_context,
                ai_analysis.why_this_role_exists AS why_this_role_exists,
                ai_analysis.seniority_level AS seniority_level,
                ai_analysis.job_classification AS job_classification,
                ai_analysis.scoring_breakdown AS scoring_breakdown,
                ai_analysis.career_level AS career_level,
                ai_analysis.recommendation_career AS recommendation_career,
                ai_analysis.reality_check AS reality_check,
                ai_analysis.why_not_fit AS why_not_fit,
                ai_analysis.engineering_level_required AS engineering_level_required,
                ai_analysis.work_type_actual AS work_type_actual,
                jobs.applied_detected_at AS applied_detected_at,
                jobs.application_confirmation_email_id AS application_confirmation_email_id,
                jobs.application_confirmation_subject AS application_confirmation_subject,
                jobs.application_confirmation_sender AS application_confirmation_sender,
                jobs.application_match_confidence AS application_match_confidence,
                jobs.application_match_reason AS application_match_reason,
                jobs.extraction_confidence AS extraction_confidence,
                jobs.extraction_reason AS extraction_reason,
                jobs.is_multi_job_email AS is_multi_job_email,
                jobs.needs_manual_review AS needs_manual_review,
                jobs.job_link_kind AS job_link_kind,
                jobs.application_url AS application_url
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


def update_job_application_metadata(
    job_id: int,
    *,
    applied_detected_at: Optional[str] = None,
    application_confirmation_email_id: Optional[int] = None,
    application_confirmation_subject: Optional[str] = None,
    application_confirmation_sender: Optional[str] = None,
    application_match_confidence: Optional[int] = None,
    application_match_reason: Optional[str] = None,
) -> None:
    now = _utc_now()
    sets: List[str] = []
    vals: List[Any] = []
    if applied_detected_at is not None:
        sets.append("applied_detected_at = ?")
        vals.append(applied_detected_at)
    if application_confirmation_email_id is not None:
        sets.append("application_confirmation_email_id = ?")
        vals.append(application_confirmation_email_id)
    if application_confirmation_subject is not None:
        sets.append("application_confirmation_subject = ?")
        vals.append(application_confirmation_subject)
    if application_confirmation_sender is not None:
        sets.append("application_confirmation_sender = ?")
        vals.append(application_confirmation_sender)
    if application_match_confidence is not None:
        sets.append("application_match_confidence = ?")
        vals.append(application_match_confidence)
    if application_match_reason is not None:
        sets.append("application_match_reason = ?")
        vals.append(application_match_reason)
    if not sets:
        return
    sets.append("updated_at = ?")
    vals.append(now)
    vals.append(job_id)
    with get_conn() as conn:
        conn.execute(f"UPDATE jobs SET {', '.join(sets)} WHERE id = ?", vals)


def get_email_id_by_gmail_message_id(gmail_message_id: str) -> Optional[int]:
    with get_conn() as conn:
        row = conn.execute("SELECT id FROM emails WHERE gmail_message_id = ?", (gmail_message_id,)).fetchone()
        return int(row["id"]) if row else None


def search_jobs_for_application_match(
    company_hint: Optional[str],
    title_hint: Optional[str],
    url_hint: Optional[str],
    *,
    limit: int = 15,
) -> List[Dict[str, Any]]:
    """Non-deleted jobs only; recent first."""
    company_hint = (company_hint or "").strip().lower()
    title_hint = (title_hint or "").strip().lower()
    url_hint = (url_hint or "").strip()
    out: List[Dict[str, Any]] = []
    with get_conn() as conn:
        if url_hint and len(url_hint) > 8:
            rows = conn.execute(
                """
                SELECT id, title, company, job_link, status, updated_at, pinned
                FROM jobs
                WHERE deleted_at IS NULL
                  AND job_link IS NOT NULL AND TRIM(job_link) != ''
                  AND (job_link = ? OR ? LIKE '%' || job_link || '%' OR job_link LIKE '%' || ? || '%')
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (url_hint, url_hint, url_hint, limit),
            ).fetchall()
            out.extend(_row_to_dict(r) or {} for r in rows)
        if company_hint and len(company_hint) >= 2:
            like = f"%{company_hint}%"
            rows = conn.execute(
                """
                SELECT id, title, company, job_link, status, updated_at, pinned
                FROM jobs
                WHERE deleted_at IS NULL
                  AND LOWER(TRIM(company)) LIKE ?
                ORDER BY updated_at DESC
                LIMIT ?
                """,
                (like, limit),
            ).fetchall()
            for r in rows:
                d = _row_to_dict(r)
                if d and not any(x["id"] == d["id"] for x in out):
                    out.append(d)
        if title_hint and len(title_hint) >= 4:
            toks = [t for t in title_hint.replace("/", " ").split() if len(t) > 3][:4]
            for tok in toks:
                like = f"%{tok}%"
                rows = conn.execute(
                    """
                    SELECT id, title, company, job_link, status, updated_at, pinned
                    FROM jobs
                    WHERE deleted_at IS NULL
                      AND LOWER(TRIM(title)) LIKE ?
                      AND status IN ('New', 'Saved')
                    ORDER BY updated_at DESC
                    LIMIT ?
                    """,
                    (like, limit),
                ).fetchall()
                for r in rows:
                    d = _row_to_dict(r)
                    if d and not any(x["id"] == d["id"] for x in out):
                        out.append(d)
    return out[:limit]


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


def _norm_text(val: Optional[str]) -> str:
    s = str(val or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _norm_title_for_dupe(title: Optional[str]) -> str:
    t = _norm_text(title)
    t = re.sub(r"manual job\s*\([^)]*\)", "manual job", t)
    t = re.sub(r"unknown role from job alert", "", t)
    t = re.sub(r"unknown company", "", t)
    t = re.sub(r"[^a-z0-9\s/+.-]+", "", t)
    return t.strip()


def _norm_company_for_dupe(company: Optional[str]) -> str:
    c = _norm_text(company)
    c = re.sub(r"unknown company|company unknown|unknown", "", c)
    c = re.sub(r"[^a-z0-9\s/+.-]+", "", c)
    return c.strip()


def _url_domain(url: Optional[str]) -> str:
    u = str(url or "").strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u, flags=re.I):
        u = "https://" + u.lstrip("/")
    try:
        p = urlparse(u)
    except Exception:
        return ""
    host = (p.netloc or "").lower().strip()
    if host.startswith("www."):
        host = host[4:]
    return host


def _is_bad_title(title: Optional[str]) -> bool:
    t = _norm_text(title)
    return (
        not t
        or "manual job (" in t
        or t.startswith("manual job")
        or t == "unknown role from job alert"
        or t in _GARBAGE_TITLES_LOW
    )


def _is_bad_company(company: Optional[str]) -> bool:
    c = _norm_text(company)
    return not c or c in ("unknown company", "company unknown", "unknown")


def _is_unique_job_identity_available(
    conn: sqlite3.Connection,
    title: Optional[str],
    company: Optional[str],
    job_link: Optional[str],
    exclude_job_id: int,
) -> bool:
    row = conn.execute(
        """
        SELECT id
        FROM jobs
        WHERE title = ?
          AND company = ?
          AND COALESCE(job_link, '') = COALESCE(?, '')
          AND id != ?
          AND deleted_at IS NULL
        LIMIT 1
        """,
        (title, company, job_link, int(exclude_job_id)),
    ).fetchone()
    return row is None


def deduplicate_jobs() -> Dict[str, Any]:
    """
    Detect and soft-merge likely duplicates.
    Never hard-deletes rows. Canonical rows remain visible; duplicates are hidden by is_duplicate=1.
    """
    report: Dict[str, Any] = {
        "checked": 0,
        "duplicates_found": 0,
        "duplicates_hidden": 0,
        "merge_collisions_skipped": 0,
        "canonical_updates": 0,
        "groups": [],
    }
    now = _utc_now()

    def _status_rank(s: Optional[str]) -> int:
        st = str(s or "").strip()
        if st == "Applied":
            return 4
        if st == "Saved":
            return 3
        if st == "New":
            return 2
        return 1

    def _sq_rank(sq: Optional[str]) -> int:
        v = str(sq or "").strip()
        if v == "manual_paste":
            return 5
        if v == "full_posting":
            return 4
        if v == "partial_posting":
            return 3
        if v == "email_snapshot":
            return 2
        return 1

    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                jobs.id, jobs.title, jobs.company, jobs.location, jobs.job_link, jobs.discovered_url, jobs.application_url,
                jobs.description, jobs.discovered_text, jobs.job_page_text, jobs.source_quality, jobs.status, jobs.pinned,
                jobs.applied_detected_at, jobs.application_confirmation_email_id, jobs.application_confirmation_subject,
                jobs.application_confirmation_sender, jobs.application_match_confidence, jobs.application_match_reason,
                jobs.updated_at, jobs.is_duplicate,
                ai_analysis.clean_title, ai_analysis.summary, ai_analysis.reasoning, ai_analysis.score
            FROM jobs
            LEFT JOIN ai_analysis ON ai_analysis.job_id = jobs.id
            WHERE jobs.deleted_at IS NULL
            """
        ).fetchall()

        candidates = [dict(r) for r in rows]
        report["checked"] = len(candidates)

        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for r in candidates:
            title = _norm_title_for_dupe(r.get("clean_title") or r.get("title"))
            company = _norm_company_for_dupe(r.get("company"))
            domain = _url_domain(r.get("application_url")) or _url_domain(r.get("discovered_url")) or _url_domain(r.get("job_link"))
            if not title and not domain:
                continue
            primary = f"{title}|{company}|{domain}"
            buckets.setdefault(primary, []).append(r)
            # Secondary key catches common "manual placeholder vs improved real title/company" duplicates sharing domain.
            if domain:
                secondary = f"domain_only|{domain}"
                buckets.setdefault(secondary, []).append(r)

        handled_ids: set[int] = set()
        for _, grp in buckets.items():
            if len(grp) < 2:
                continue
            grp_ids = [int(x["id"]) for x in grp]
            if any(gid in handled_ids for gid in grp_ids):
                continue
            report["duplicates_found"] += len(grp) - 1

            def _score_row(x: Dict[str, Any]) -> Tuple[int, int, int, int]:
                text_len = len(str(x.get("discovered_text") or "")) + len(str(x.get("job_page_text") or "")) + len(str(x.get("description") or ""))
                return (
                    _status_rank(x.get("status")),
                    1 if int(x.get("pinned") or 0) else 0,
                    _sq_rank(x.get("source_quality")),
                    text_len,
                )

            grp_sorted = sorted(grp, key=_score_row, reverse=True)
            canonical = grp_sorted[0]
            dupes = grp_sorted[1:]
            can_id = int(canonical["id"])

            merged = dict(canonical)
            merged_changed = False
            for d in dupes:
                # Promote better identity fields.
                if _is_bad_title(merged.get("title")) and not _is_bad_title(d.get("clean_title") or d.get("title")):
                    merged["title"] = (d.get("clean_title") or d.get("title"))
                    merged_changed = True
                if _is_bad_company(merged.get("company")) and not _is_bad_company(d.get("company")):
                    merged["company"] = d.get("company")
                    merged_changed = True
                if (not str(merged.get("location") or "").strip()) and str(d.get("location") or "").strip():
                    merged["location"] = d.get("location")
                    merged_changed = True

                # Promote richer text/source quality.
                for fld in ("description", "discovered_text", "job_page_text"):
                    if len(str(d.get(fld) or "")) > len(str(merged.get(fld) or "")):
                        merged[fld] = d.get(fld)
                        merged_changed = True
                if _sq_rank(d.get("source_quality")) > _sq_rank(merged.get("source_quality")):
                    merged["source_quality"] = d.get("source_quality")
                    merged_changed = True

                # Preserve strongest status/application metadata.
                if _status_rank(d.get("status")) > _status_rank(merged.get("status")):
                    merged["status"] = d.get("status")
                    merged_changed = True
                for fld in (
                    "applied_detected_at",
                    "application_confirmation_email_id",
                    "application_confirmation_subject",
                    "application_confirmation_sender",
                    "application_match_confidence",
                    "application_match_reason",
                ):
                    if not merged.get(fld) and d.get(fld):
                        merged[fld] = d.get(fld)
                        merged_changed = True

            if merged_changed:
                # Uniqueness is on (title, company, job_link). Merge logic does not change job_link; use merged for clarity.
                identity_job_link = merged.get("job_link")
                safe_title = merged.get("title")
                safe_company = merged.get("company")
                if not _is_unique_job_identity_available(
                    conn,
                    safe_title,
                    safe_company,
                    identity_job_link,
                    can_id,
                ):
                    report["merge_collisions_skipped"] += 1
                    coll = conn.execute(
                        """
                        SELECT id FROM jobs
                        WHERE title = ?
                          AND company = ?
                          AND COALESCE(job_link, '') = COALESCE(?, '')
                          AND id != ?
                          AND deleted_at IS NULL
                        LIMIT 1
                        """,
                        (safe_title, safe_company, identity_job_link, can_id),
                    ).fetchone()
                    # Keep canonical identity stable when collision exists.
                    merged["title"] = canonical.get("title")
                    merged["company"] = canonical.get("company")
                    # If the colliding row is a likely duplicate, hide it safely.
                    dup_ids_in_group = {int(x["id"]) for x in dupes}
                    if coll:
                        coll_id = int(coll["id"])
                        conn.execute(
                            """
                            UPDATE jobs
                            SET is_duplicate = 1, duplicate_of = ?, duplicate_reason = ?, duplicate_confidence = ?, updated_at = ?
                            WHERE id = ?
                            """,
                            (can_id, "identity_collision_during_merge", 80, now, coll_id),
                        )
                        # Avoid double-counting if this row is also marked hidden in the dupe loop below.
                        if coll_id not in dup_ids_in_group:
                            report["duplicates_hidden"] += 1

                try:
                    conn.execute(
                        """
                        UPDATE jobs SET
                            title = ?, company = ?, location = ?, description = ?, discovered_text = ?, job_page_text = ?,
                            source_quality = ?, status = ?, applied_detected_at = ?, application_confirmation_email_id = ?,
                            application_confirmation_subject = ?, application_confirmation_sender = ?,
                            application_match_confidence = ?, application_match_reason = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            merged.get("title"),
                            merged.get("company"),
                            merged.get("location"),
                            merged.get("description"),
                            merged.get("discovered_text"),
                            merged.get("job_page_text"),
                            merged.get("source_quality"),
                            merged.get("status"),
                            merged.get("applied_detected_at"),
                            merged.get("application_confirmation_email_id"),
                            merged.get("application_confirmation_subject"),
                            merged.get("application_confirmation_sender"),
                            merged.get("application_match_confidence"),
                            merged.get("application_match_reason"),
                            now,
                            can_id,
                        ),
                    )
                    report["canonical_updates"] += 1
                except sqlite3.IntegrityError:
                    # Never crash cleanup on duplicate identity collisions.
                    report["merge_collisions_skipped"] += 1

            hidden_ids: List[int] = []
            for d in dupes:
                did = int(d["id"])
                conn.execute(
                    """
                    UPDATE jobs
                    SET is_duplicate = 1, duplicate_of = ?, duplicate_reason = ?, duplicate_confidence = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (can_id, "title+company+domain", 85, now, did),
                )
                report["duplicates_hidden"] += 1
                hidden_ids.append(did)
            # Ensure canonical is not marked duplicate.
            conn.execute(
                "UPDATE jobs SET is_duplicate = 0, duplicate_of = NULL, duplicate_reason = NULL, duplicate_confidence = NULL, updated_at = ? WHERE id = ?",
                (now, can_id),
            )

            report["groups"].append(
                {"canonical_job_id": can_id, "duplicate_job_ids": hidden_ids}
            )
            handled_ids.update(grp_ids)

    return report


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


def update_job_application_url(job_id: int, url: Optional[str]) -> None:
    """User-edited application / ATS page URL (optional override for Apply Assist)."""
    now = _utc_now()
    cleaned = (url or "").strip() or None
    with get_conn() as conn:
        conn.execute(
            "UPDATE jobs SET application_url = ?, updated_at = ? WHERE id = ?",
            (cleaned, now, int(job_id)),
        )


def insert_apply_assist_session(
    job_id: int,
    *,
    status: str,
    url: Optional[str],
    fields_detected: Optional[Any] = None,
    fields_filled: Optional[Any] = None,
    fields_skipped: Optional[Any] = None,
    manual_required: Optional[Any] = None,
    error_message: Optional[str] = None,
) -> int:
    now = _utc_now()

    def _j(x: Any) -> Optional[str]:
        if x is None:
            return None
        if isinstance(x, str):
            return x
        return json.dumps(x, ensure_ascii=True)

    with get_conn() as conn:
        cur = conn.execute(
            """
            INSERT INTO apply_assist_sessions (
                job_id, started_at, finished_at, status, url,
                fields_detected, fields_filled, fields_skipped, manual_required, error_message
            )
            VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(job_id),
                now,
                status,
                url,
                _j(fields_detected),
                _j(fields_filled),
                _j(fields_skipped),
                _j(manual_required),
                error_message,
            ),
        )
        return int(cur.lastrowid)


def update_apply_assist_session(
    session_id: int,
    *,
    status: Optional[str] = None,
    finished_at: Optional[str] = None,
    fields_detected: Any = None,
    fields_filled: Any = None,
    fields_skipped: Any = None,
    manual_required: Any = None,
    error_message: Optional[str] = None,
) -> None:
    sets: List[str] = []
    vals: List[Any] = []

    def _j(x: Any) -> Optional[str]:
        if x is None:
            return None
        if isinstance(x, str):
            return x
        return json.dumps(x, ensure_ascii=True)

    if status is not None:
        sets.append("status = ?")
        vals.append(status)
    if finished_at is not None:
        sets.append("finished_at = ?")
        vals.append(finished_at)
    if fields_detected is not None:
        sets.append("fields_detected = ?")
        vals.append(_j(fields_detected))
    if fields_filled is not None:
        sets.append("fields_filled = ?")
        vals.append(_j(fields_filled))
    if fields_skipped is not None:
        sets.append("fields_skipped = ?")
        vals.append(_j(fields_skipped))
    if manual_required is not None:
        sets.append("manual_required = ?")
        vals.append(_j(manual_required))
    if error_message is not None:
        sets.append("error_message = ?")
        vals.append(error_message)
    if not sets:
        return
    vals.append(int(session_id))
    with get_conn() as conn:
        conn.execute(
            f"UPDATE apply_assist_sessions SET {', '.join(sets)} WHERE id = ?",
            vals,
        )


def get_latest_apply_assist_session(job_id: int) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        return _row_to_dict(
            conn.execute(
                """
                SELECT * FROM apply_assist_sessions
                WHERE job_id = ?
                ORDER BY datetime(started_at) DESC, id DESC
                LIMIT 1
                """,
                (int(job_id),),
            ).fetchone()
        )
