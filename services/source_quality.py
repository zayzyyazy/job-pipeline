"""Persisted research depth for UI + AI guardrails."""

from __future__ import annotations

from typing import Any, Dict, Optional

from services.job_discovery import content_looks_meaningful

VALID_SOURCE_QUALITY = (
    "full_posting",
    "partial_posting",
    "email_snapshot",
    "manual_paste",
    "not_found",
)


def is_manual_quality(job_row: Dict[str, Any]) -> bool:
    src = str(job_row.get("discovered_source") or "").lower()
    return ("manual_paste" in src) or ("manual_form" in src) or ("user_url_fetch" in src)


def infer_source_quality(job_row: Dict[str, Any]) -> str:
    """
    Derive source_quality from persisted job fields after discovery/research/sync.
    manual_paste is detected via discovered_source tagging from improve flows.
    """
    if is_manual_quality(job_row):
        return "manual_paste"

    disc_txt = str(job_row.get("discovered_text") or "").strip()
    orig_txt = str(job_row.get("job_page_text") or "").strip()
    main = disc_txt if len(disc_txt) >= len(orig_txt) else orig_txt
    if not main.strip():
        main = disc_txt or orig_txt

    status = str(job_row.get("discovery_status") or "").strip()

    if main and content_looks_meaningful(main):
        return "full_posting"
    if main and len(main) >= 900:
        return "partial_posting"

    if status == "found" or status == "original_only":
        if len(main.strip()) >= 400:
            return "partial_posting"
        return "email_snapshot"

    if status == "email_only":
        return "email_snapshot"

    if status == "failed":
        return "not_found"

    if len(main.strip()) >= 200:
        return "partial_posting"
    return "email_snapshot"


def label_for_quality(sq: Optional[str]) -> str:
    return {
        "full_posting": "Full posting",
        "partial_posting": "Partial posting",
        "email_snapshot": "Email snapshot",
        "manual_paste": "Manual paste",
        "not_found": "Not found",
    }.get(str(sq or ""), "Unknown")


def badge_class(sq: Optional[str]) -> str:
    return {
        "full_posting": "sq-full",
        "partial_posting": "sq-partial",
        "email_snapshot": "sq-email",
        "manual_paste": "sq-manual",
        "not_found": "sq-missing",
    }.get(str(sq or ""), "sq-unknown")
