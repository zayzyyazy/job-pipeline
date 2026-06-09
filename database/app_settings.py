"""SQLite-backed app settings (OpenAI, etc.). Keys mirror env names where applicable."""

from __future__ import annotations

from typing import Optional

from database.db import _utc_now, get_conn


def get_setting(key: str) -> Optional[str]:
    with get_conn() as conn:
        row = conn.execute("SELECT value FROM app_settings WHERE key = ?", (key,)).fetchone()
        if not row or row["value"] is None:
            return None
        return str(row["value"])


def set_setting(key: str, value: str) -> None:
    now = _utc_now()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO app_settings (key, value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """,
            (key, value, now),
        )


def delete_setting(key: str) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM app_settings WHERE key = ?", (key,))


def mask_api_key(raw: Optional[str]) -> str:
    if not raw or not str(raw).strip():
        return ""
    s = str(raw).strip()
    if len(s) <= 8:
        return "••••••••"
    if s.startswith("sk-"):
        return f"sk-…{s[-4:]}"
    return f"…{s[-4:]}"


def get_openai_key_for_display() -> tuple[bool, str]:
    """(configured_in_db, masked_or_empty)"""
    v = get_setting("OPENAI_API_KEY")
    if v and v.strip():
        return True, mask_api_key(v)
    return False, ""

