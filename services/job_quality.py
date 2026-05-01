"""
Normalize and validate job titles/companies parsed from email.
Flags garbage titles; extracts company from common German / English patterns.
"""

import html
import re
from typing import Any, Dict, Optional, Tuple

_WS = re.compile(r"\s+")

# Lowercase stripped titles we reject outright (exact match after normalize)
_REJECT_EXACT_TITLE = frozenset(
    {
        "e",
        "no",
        "yes",
        "ok",
        "job",
        "jobs",
        "new",
        "hi",
        "neue jobs",
        "your application",
        "application",
        "bewerbung",
        "confirmation",
        "terminbestätigung",
    }
)

# Substrings that strongly suggest not a job title line
_REJECT_SUBSTRING = (
    "your application",
    "neue jobs",
    "new jobs matched",
)

_BEI_PATTERN = re.compile(
    r"\bbei\s+(?P<co>[^|,–—\-:|]+)",
    flags=re.IGNORECASE,
)
_AT_PATTERN = re.compile(
    r"\bat\s+(?P<co>[^|,–—\-:|]+)",
    flags=re.IGNORECASE,
)
_VON_PATTERN = re.compile(
    r"\bvon\s+(?P<co>[^|,–—\-:|]+)",
    flags=re.IGNORECASE,
)
_FUER_PATTERN = re.compile(
    r"\bfür\s+(?P<co>[^|,–—\-:|]+)",
    flags=re.IGNORECASE,
)


def clean_text_basic(value: Optional[str]) -> str:
    if not value:
        return ""
    t = html.unescape(str(value))
    t = t.replace("\xa0", " ")
    t = _WS.sub(" ", t).strip()
    return t


def normalize_job_fields(parsed: Dict[str, Any]) -> Dict[str, Any]:
    """Mutates copy-friendly dict: cleans title/company/description/location."""
    out = dict(parsed)
    for key in ("title", "company", "location", "description"):
        if out.get(key) is not None:
            out[key] = clean_text_basic(out[key])
    return out


def try_extract_company_from_title(title: str) -> Tuple[str, Optional[str]]:
    """
    Split 'Role bei Company …' patterns: return (clean_title, company_or_None).
    """
    t = title
    company = None
    for pat in (_BEI_PATTERN, _AT_PATTERN, _VON_PATTERN, _FUER_PATTERN):
        m = pat.search(t)
        if m:
            cand = clean_text_basic(m.group("co"))
            if len(cand) >= 2 and len(cand) < 160:
                company = cand
                role = clean_text_basic(t[: m.start()])
                role = role.rstrip(" ,–-|")
                if len(role) >= 3:
                    t = role
            break
    return t or title, company


def assess_title_quality(title: str, allow_short_codenames: Tuple[str, ...] = ()) -> Tuple[bool, Optional[str]]:
    """
    Returns (ok, rejection_reason_if_not_ok).
    Titles shorter than 4 chars are rejected unless in allow_short_codenames or digits.
    """
    t = clean_text_basic(title)
    if not t:
        return False, "empty_title"
    lowered = t.lower()
    if len(t) < 4:
        if t in allow_short_codenames or (t.isalnum() and any(c.isdigit() for c in t)):
            return True, None
        return False, f"title_too_short:{t!r}"
    if lowered in _REJECT_EXACT_TITLE:
        return False, f"disallowed_title_exact:{lowered}"
    for fragment in _REJECT_SUBSTRING:
        if fragment in lowered:
            return False, f"disallowed_title_contains:{fragment}"
    if len(lowered.replace(" ", "")) < 3:
        return False, "title_degenerate"
    return True, None


def apply_company_from_title(parsed: Dict[str, Any]) -> Dict[str, Any]:
    parsed = normalize_job_fields(parsed)
    tit = parsed.get("title") or ""
    new_title, extracted = try_extract_company_from_title(tit)
    if extracted:
        parsed["title"] = new_title or parsed["title"]
        co = parsed.get("company") or ""
        if not co.strip() or co.strip().lower() in ("unknown company", "unknown"):
            parsed["company"] = extracted
    return parsed


def prepare_parsed_job_for_pipeline(parsed: Dict[str, Any]) -> Tuple[Dict[str, Any], Optional[str]]:
    """
    Full cleanup; returns (parsed, title_reject_reason or None).
    """
    parsed = apply_company_from_title(parsed)
    ok, reason = assess_title_quality(parsed.get("title", ""))
    if not ok:
        return parsed, reason
    return parsed, None
