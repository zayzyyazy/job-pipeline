"""
Match application-confirmation emails to existing jobs and update status / metadata.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from database.db import (
    _utc_now,
    get_job_plain,
    search_jobs_for_application_match,
    update_job_application_metadata,
    update_job_status,
    upsert_job,
)
from services.target_fit import evaluate_target_fit


def _norm(s: Optional[str]) -> str:
    return re.sub(r"\s+", " ", (s or "").strip().lower())


def _extract_url(text: str) -> Optional[str]:
    m = re.search(r"https?://[^\s<>\"']+", text or "")
    return m.group(0).strip().rstrip(").,;") if m else None


def _extract_title_company(text: str) -> Tuple[Optional[str], Optional[str]]:
    """Lightweight extraction from confirmation bodies (DE/EN)."""
    t = text or ""
    title: Optional[str] = None
    company: Optional[str] = None

    m = re.search(
        r"(?:application|bewerbung)\s+(?:for|als|zur|für)\s+(.{3,120}?)(?:\s+at\s+|\s+bei\s+|\s+to\s+)(.{2,80}?)(?:\.|,|\n|$)",
        t,
        re.I | re.S,
    )
    if m:
        title = m.group(1).strip(" \n\r\t-–—")
        company = m.group(2).strip(" \n\r\t-–—")

    m2 = re.search(
        r"bewerbung\s+als\s+(.{3,120}?)\s+bei\s+(.{2,80}?)(?:\.|,|\n|$)",
        t,
        re.I | re.S,
    )
    if m2 and not title:
        title = m2.group(1).strip()
        company = m2.group(2).strip()

    m3 = re.search(r"thank\s+you\s+for\s+your\s+application\s+to\s+(.{2,80}?)(?:\.|,|\n)", t, re.I)
    if m3 and not company:
        company = m3.group(1).strip()

    if title and len(title) > 160:
        title = title[:160].rsplit(" ", 1)[0]
    if company and len(company) > 120:
        company = company[:120].rsplit(" ", 1)[0]
    return title, company


def _sender_domain(sender: str) -> Optional[str]:
    m = re.search(r"[\w.-]+@([\w.-]+)", sender or "")
    return m.group(1).lower() if m else None


def _score_match(job: Dict[str, Any], title_guess: Optional[str], company_guess: Optional[str]) -> Tuple[int, str]:
    parts: List[str] = []
    score = 0
    jt = _norm(job.get("title"))
    jc = _norm(job.get("company"))
    tg = _norm(title_guess)
    cg = _norm(company_guess)

    if cg and jc and (cg in jc or jc in cg):
        score += 55
        parts.append("company_strong")
    elif cg and jc:
        cg_tokens = set(cg.split()) - {"gmbh", "ag", "inc", "llc", "ltd", "the", "gmbh."}
        jc_tokens = set(jc.split()) - {"gmbh", "ag", "inc", "llc", "ltd", "the"}
        if cg_tokens & jc_tokens:
            score += 35
            parts.append("company_token_overlap")

    if tg and jt:
        if tg in jt or jt in tg:
            score += 40
            parts.append("title_substring")
        else:
            overlap = len(set(tg.split()) & set(jt.split()))
            if overlap >= 2:
                score += min(30, overlap * 8)
                parts.append("title_token_overlap")

    reason = ",".join(parts) if parts else "weak"
    return score, reason


def process_application_confirmation_email(
    email_id: int,
    email: Dict[str, Any],
    classification: Dict[str, Any],
    result_counters: Any,
) -> None:
    """
    Mutates result_counters (SyncResult) for applications_matched / applications_created.
    """
    subject = str(email.get("subject") or "")
    sender = str(email.get("sender") or "")
    body = str(email.get("body") or "")
    blob = "\n".join([subject, email.get("snippet") or "", body[:16000]])

    title_guess, company_guess = _extract_title_company(blob)
    if not company_guess:
        company_guess = _sender_domain(sender)
    if not title_guess:
        title_guess = subject[:120] if subject else None

    url_hint = _extract_url(blob)
    candidates = search_jobs_for_application_match(company_guess, title_guess, url_hint or "", limit=18)

    best: Optional[Tuple[int, Dict[str, Any], str]] = None
    for job in candidates:
        sc, reason = _score_match(job, title_guess, company_guess)
        if url_hint and job.get("job_link"):
            jl = str(job.get("job_link")).strip()
            if jl and (jl in url_hint or url_hint in jl):
                sc += 45
                reason += ",url_match"
        if best is None or sc > best[0]:
            best = (sc, job, reason)

    now = _utc_now()
    meta = {
        "applied_detected_at": now,
        "application_confirmation_email_id": email_id,
        "application_confirmation_subject": subject[:520],
        "application_confirmation_sender": sender[:520],
    }

    threshold = 48
    if best and best[0] >= threshold:
        job_id = int(best[1]["id"])
        jr = get_job_plain(job_id)
        if not jr:
            return
        if str(jr.get("status") or "") != "Applied":
            update_job_status(
                job_id,
                "Applied",
                note="Marked Applied from Gmail application confirmation.",
            )
        update_job_application_metadata(
            job_id,
            application_match_confidence=min(100, best[0]),
            application_match_reason=str(best[2])[:900],
            **meta,
        )
        result_counters.applications_matched += 1
        return

    # Create Applied-only job row
    disp_title = (title_guess or "Unknown applied role").strip()[:200]
    disp_company = (company_guess or "Unknown company").strip()[:200]
    if disp_company.lower() in ("unknown company", "unknown"):
        sd = _sender_domain(sender)
        if sd:
            disp_company = sd.split(".")[0].title() if sd else disp_company

    synthetic_link = f"gmail-app://confirm-{email_id}"
    parsed: Dict[str, Any] = {
        "email_id": email_id,
        "source": "Gmail Application Confirmation",
        "title": disp_title,
        "company": disp_company,
        "location": "",
        "job_link": synthetic_link,
        "description": (blob[:8000]),
        "status": "Applied",
        "category": "Other",
        "category_locked": 0,
        "pinned": 0,
        "deleted_at": None,
        "discovery_status": "email_only",
        "discovery_reason": "Created from Gmail application confirmation (no prior job card).",
        "source_quality": "email_snapshot",
        "extraction_confidence": 72,
        "extraction_reason": "gmail_application_confirmation",
        "is_multi_job_email": 0,
        "needs_manual_review": 0,
        "job_link_kind": "none",
    }
    fit = evaluate_target_fit(
        {
            "title": disp_title,
            "company": disp_company,
            "location": "",
            "description": parsed["description"],
            "job_link": synthetic_link,
            "source": parsed["source"],
        },
        {
            "subject": subject,
            "snippet": email.get("snippet") or "",
            "body": body[:4000],
        },
    )
    parsed["target_fit"] = fit["target_fit"]
    parsed["target_score"] = fit["target_score"]
    parsed["target_matched_keywords"] = fit["matched_keywords"]

    job_id = upsert_job(parsed)
    update_job_application_metadata(
        job_id,
        application_match_confidence=best[0] if best else 0,
        application_match_reason=(best[2] if best else "no_match_new_row")[:900],
        **meta,
    )
    result_counters.applications_created += 1
