"""
Email → job lead extraction. Treats Gmail as a lead source, not a verified JD.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

from services.job_quality import clean_text_basic

_LINK_RX = re.compile(r"https?://[^\s\]>\)\"'<>]+", re.I)

# Generic / alert subject lines — not real job titles (substring match on normalized title)
_GENERIC_TITLE_FRAGMENTS = (
    "neue jobs für dich",
    "neue jobs",
    "new jobs for you",
    "new jobs matching",
    "new jobs",
    "your job alert",
    "job alert",
    "jobs for you",
    "recommended jobs",
    "recommended for you",
    "indeed job alert",
    "linkedin jobs",
    "empfohlene jobs",
    "matching jobs",
    "stellenausschreibung",  # too generic alone
)

# One-word / noise titles
_COMPANY_PRONOUN_STOP = frozenset(
    {
        "dich",
        "dir",
        "mich",
        "uns",
        "euch",
        "ihr",
        "ihnen",
        "sie",
        "mir",
        "you",
        "me",
        "us",
        "them",
        "everyone",
        "all",
        "alle",
        "diese",
        "denen",
    }
)

_REJECT_SINGLE_TOKEN = frozenset(
    {
        "e",
        "job",
        "jobs",
        "hi",
        "new",
        "alert",
        "role",
        "position",
        "bewerbung",
        "application",
    }
)

_TITLE_PREFIX_STRIP = re.compile(
    r"^\s*(new\s*job\s*:|new\s*:|job\s*:|jobs\s*:|alert\s*:|careers\s*:|"
    r"neue\s*stelle\s*:|stellenangebot\s*:|position\s*:|role\s*:)\s*",
    re.I,
)

# Company from surrounding text (not only title)
_COMPANY_AT = re.compile(
    r"\b(?:at|bei|von|für)\s+(?P<co>[A-Za-z0-9&][A-Za-z0-9&\s.'\-]{1,120}?)(?:\s*[–|,\n]|$|(?:\s+in\s+)|(?:\s+-\s+))",
    re.I,
)
_COMPANY_SUCHT = re.compile(
    r"\b(?P<co>[A-Za-z0-9&][A-Za-z0-9&\s.'\-]{1,100}?)\s+sucht\b",
    re.I,
)

# "Werkstudent X bei Company – City"
_ROLE_BEI_LOC = re.compile(
    r"^(?P<title>.+?)\s+bei\s+(?P<co>[A-Za-z0-9&][^–—\-|,\n]{1,120}?)\s*[–—\-]\s*(?P<loc>.+)$",
    re.I,
)
_BEWERBUNG_ALS_BEI = re.compile(
    r"(?:bewerbung|application)\s+als\s+(?P<title>.+?)\s+bei\s+(?P<co>[A-Za-z0-9&][^\.\n,]{1,120})",
    re.I,
)

_DE_CITIES = (
    "düsseldorf",
    "duesseldorf",
    "köln",
    "koeln",
    "cologne",
    "bonn",
    "essen",
    "dortmund",
    "berlin",
    "hamburg",
    "münchen",
    "muenchen",
    "munich",
    "frankfurt",
    "stuttgart",
    "hannover",
    "leipzig",
    "nürnberg",
    "nuernberg",
    "aachen",
    "bremen",
    "remote",
    "hybrid",
    "home office",
    "teilzeit",
    "vollzeit",
)

_TRACKING_SUBSTR = (
    "unsubscribe",
    "preferences",
    "optout",
    "opt-out",
    "/clk?",
    "click?",
    "utm_",
    "trk=",
    "tracking",
    "mailchi.mp",
    "list-manage.com",
    "sendgrid.net/wf",
    "doubleclick",
    "google.com/url",
    "lnkd.in/",
    "bit.ly/",
    "t.co/",
    "email.linkedin.com",
    "safelinks.protection",
)

_STRONG_HOST = (
    "greenhouse.io",
    "lever.co",
    "myworkdayjobs.com",
    "ashbyhq.com",
    "ashbyjobs.com",
    "smartrecruiters.com",
    "workable.com",
    "personio.de",
    "jobs.personio.de",
)


def _strip_title_prefixes(raw: str) -> str:
    t = clean_text_basic(raw)
    prev = None
    while prev != t:
        prev = t
        t = _TITLE_PREFIX_STRIP.sub("", t).strip()
    return t


def _is_generic_title(title: str) -> bool:
    low = clean_text_basic(title).lower()
    if not low:
        return True
    if low in _GENERIC_TITLE_FRAGMENTS:
        return True
    for frag in _GENERIC_TITLE_FRAGMENTS:
        if frag in low and len(low) <= len(frag) + 12:
            return True
    return False


def _is_bad_token_title(title: str) -> bool:
    t = clean_text_basic(title)
    if not t:
        return True
    if len(t) <= 1:
        return True
    tokens = t.split()
    if len(tokens) == 1 and tokens[0].lower() in _REJECT_SINGLE_TOKEN:
        return True
    if len(tokens) == 1 and len(t) < 4 and not any(c.isdigit() for c in t):
        return True
    return False


def _classify_link(url: str) -> str:
    u = url.lower().strip()
    if u.startswith("mailto:"):
        return "skip"
    if any(b in u for b in _TRACKING_SUBSTR):
        return "tracking"
    if "facebook.com" in u or "twitter.com" in u or "instagram.com" in u:
        return "skip"
    return "direct"


def _score_link(url: str) -> int:
    kind = _classify_link(url)
    if kind == "skip":
        return -999
    u = url.lower()
    sc = 30
    for h in _STRONG_HOST:
        if h in u:
            sc += 50
            break
    if "indeed.com" in u and ("viewjob" in u or "rc/clk" in u):
        sc += 25 if "viewjob" in u else 8
    if "linkedin.com/jobs" in u and ("view" in u or "collections" in u):
        sc += 22
    if kind == "tracking":
        sc -= 12
    return sc


def _pick_best_link(links: List[str]) -> Tuple[str, str]:
    """Returns (url_or_empty, kind: direct|tracking|none)."""
    best = ("", "none", -10_000)
    for raw in links:
        u = raw.rstrip(").,;]")
        if not u.startswith("http"):
            continue
        kind = _classify_link(u)
        if kind == "skip":
            continue
        sc = _score_link(u)
        k = "tracking" if kind == "tracking" else "direct"
        if sc > best[2]:
            best = (u, k, sc)
    return best[0], best[1]


def _extract_company_from_blob(blob: str, title: str) -> str:
    blob = blob[:12000]
    for pat in (_COMPANY_AT, _COMPANY_SUCHT):
        m = pat.search(blob)
        if m:
            co = clean_text_basic(m.group("co"))
            if len(co) >= 2 and co.lower() not in ("the", "a", "an", "die", "der", "das", "you", "your"):
                if co.lower() in _COMPANY_PRONOUN_STOP:
                    continue
                # avoid capturing title tail
                if title and co.lower() in title.lower():
                    continue
                return co[:160]
    return ""


def _extract_location(blob: str) -> str:
    low = blob.lower()
    hits: List[str] = []
    for city in _DE_CITIES:
        if city in low:
            hits.append(city.replace("duesseldorf", "Düsseldorf").title())
    if "remote" in low or "home office" in low:
        if "hybrid" in low:
            hits.append("Hybrid")
        elif not any(h.lower() == "remote" for h in hits):
            hits.append("Remote")
    if hits:
        return ", ".join(dict.fromkeys(hits))[:180]
    m = re.search(r"\b(?:in|at|near)\s+([A-ZÄÖÜ][a-zäöüß\-]{2,40})\b", blob[:4000])
    if m:
        return m.group(1)
    return ""


def _best_description(subject: str, snippet: str, body: str) -> str:
    parts: List[str] = []
    sub = clean_text_basic(subject)
    if sub:
        parts.append(f"Subject: {sub}")
    body = body or ""
    snippet = clean_text_basic(snippet)
    chunk = ""
    if len(body.strip()) > max(len(snippet), 80):
        for para in re.split(r"\n{2,}", body[:20000]):
            p = clean_text_basic(para)
            if len(p) > 140:
                chunk = p[:8000]
                break
        if not chunk:
            chunk = clean_text_basic(body[:8000])
    else:
        chunk = snippet or clean_text_basic(body[:4000])
    if chunk:
        parts.append(chunk)
    return "\n\n".join(parts).strip()[:12000]


def _first_nonempty_line(text: str) -> str:
    for line in (text or "").split("\n"):
        s = clean_text_basic(line)
        if len(s) >= 8:
            return s
    return ""


def _count_job_like_links(links: List[str]) -> int:
    domains = set()
    for u in links:
        if _classify_link(u) == "skip":
            continue
        try:
            host = re.sub(r"^https?://([^/]+)/?.*$", r"\1", u.lower())
            domains.add(host[:120])
        except Exception:
            continue
    return len(domains)


def extract_email_lead(email: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract structured lead from a Gmail-like dict.
    Returns keys: title, company, location, job_link, job_link_kind, description,
    is_multi_job_email, extraction_confidence, extraction_reason (plus legacy shape for pipeline).
    """
    subject = str(email.get("subject") or "")
    body = str(email.get("body") or "")
    snippet = str(email.get("snippet") or "")
    sender = str(email.get("sender") or "")

    merged = "\n".join([subject, snippet, body[:25000]])
    links = _LINK_RX.findall(merged)
    links = [l.rstrip(").,;]") for l in links]

    extraction_reason_parts: List[str] = ["email_lead"]

    multi = False
    if len(links) >= 5 and _count_job_like_links(links) >= 4:
        multi = True
    if re.search(r"\b(\d+)\s+(new\s+)?jobs?\b", merged, re.I) and len(links) >= 3:
        multi = True

    title = ""
    company = ""
    location = ""

    # Structured DE line: subject first, then first substantial line of snippet/body
    subj_line = _first_nonempty_line(subject) or clean_text_basic(subject)
    head_candidates = [subj_line]
    for chunk in (snippet, body):
        fl = _first_nonempty_line(chunk)
        if fl and fl not in head_candidates:
            head_candidates.append(fl)

    m = None
    for cand in head_candidates:
        m = _ROLE_BEI_LOC.search(cand)
        if m:
            break
    if m:
        title = clean_text_basic(m.group("title"))
        company = clean_text_basic(m.group("co"))
        location = clean_text_basic(m.group("loc"))
        extraction_reason_parts.append("role_bei_loc_line")

    if not title:
        m2 = _BEWERBUNG_ALS_BEI.search(merged[:4000])
        if m2:
            title = clean_text_basic(m2.group("title"))
            company = clean_text_basic(m2.group("co"))
            extraction_reason_parts.append("bewerbung_als_pattern")

    # Title | Company patterns
    if not title:
        for pat in (
            re.compile(r"(?P<title>[^\n|–—\-]+?)\s*[|–—]\s*(?P<co>[^\n|]+)", re.I),
            re.compile(r"(?P<title>[^\n]+?)\s+at\s+(?P<co>[^\n,|–—]+)", re.I),
        ):
            mm = pat.search(subject) or pat.search(snippet + "\n" + body[:2000])
            if mm:
                title = clean_text_basic(mm.group("title"))
                company = clean_text_basic(mm.group("co"))
                break

    if not title and subject:
        title = _strip_title_prefixes(subject.split("\n")[0].split("|")[0])[:160]

    title = _strip_title_prefixes(title)
    blob_for_co = merged

    if not company or company.lower() in ("unknown", "unknown company"):
        co2 = _extract_company_from_blob(blob_for_co, title)
        if co2:
            company = co2

    if not location:
        location = _extract_location(merged)

    job_link, job_link_kind = _pick_best_link(links)

    if not company.strip():
        company = "Unknown Company"

    # Sanitize title display
    display_title = title
    if _is_bad_token_title(display_title) or _is_generic_title(display_title):
        display_title = "Unknown role from job alert"
        extraction_reason_parts.append("generic_or_thin_title")
        if company and company.lower() in _COMPANY_PRONOUN_STOP:
            company = "Unknown Company"

    # Confidence 0–100
    conf = 42
    if display_title and display_title != "Unknown role from job alert":
        conf += 22
        if len(display_title) >= 12:
            conf += 8
    if company and company.lower() not in ("unknown company", "unknown"):
        conf += 18
    if job_link:
        conf += 12 if job_link_kind == "direct" else 4
    if location and len(location) > 2:
        conf += 6
    desc_blob = _best_description(subject, snippet, body)
    if len(desc_blob) > 900:
        conf += 10
    elif len(desc_blob) < 350:
        conf -= 18
        extraction_reason_parts.append("shallow_text")
    if multi:
        conf -= 25
        extraction_reason_parts.append("multi_job_email")
    if job_link_kind == "tracking" and job_link:
        extraction_reason_parts.append("tracking_link")
    conf = max(0, min(100, conf))

    extraction_reason = ";".join(dict.fromkeys(extraction_reason_parts))

    return {
        "title": display_title,
        "company": company,
        "location": location,
        "job_link": job_link,
        "job_link_kind": job_link_kind,
        "description": desc_blob,
        "is_multi_job_email": multi,
        "extraction_confidence": conf,
        "extraction_reason": extraction_reason,
        "links_found": links[:24],
    }


def extract_email_sample(raw_text: str) -> Dict[str, Any]:
    """Debug helper: run extraction on arbitrary pasted email/JD text."""
    text = (raw_text or "").strip()
    email = {
        "subject": "",
        "snippet": text[:500],
        "body": text,
        "sender": "",
    }
    out = extract_email_lead(email)
    ec = int(out.get("extraction_confidence") or 0)
    needs = bool(
        ec < 55
        or out.get("is_multi_job_email")
        or "generic_or_thin_title" in str(out.get("extraction_reason") or "")
    )
    return {
        "title": out["title"],
        "company": out["company"],
        "location": out["location"],
        "job_link": out["job_link"],
        "job_link_kind": out["job_link_kind"],
        "links_found": out.get("links_found") or [],
        "confidence": ec,
        "reason": out["extraction_reason"],
        "is_multi_job_email": out["is_multi_job_email"],
        "needs_manual_review": needs,
    }


def parse_job_from_email(email: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Backward-compatible: returns job dict + extraction metadata for the pipeline."""
    lead = extract_email_lead(email)
    if not lead.get("title"):
        return None
    return {
        "title": lead["title"],
        "company": lead["company"],
        "location": lead.get("location") or "",
        "job_link": lead.get("job_link") or "",
        "description": lead.get("description") or "",
        "job_link_kind": lead.get("job_link_kind") or "none",
        "is_multi_job_email": bool(lead.get("is_multi_job_email")),
        "extraction_confidence": int(lead.get("extraction_confidence") or 0),
        "extraction_reason": str(lead.get("extraction_reason") or ""),
    }
