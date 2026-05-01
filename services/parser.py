import re
from typing import Any, Dict, Optional

TITLE_COMPANY_PATTERNS = [
    re.compile(r"(?P<title>[^\n\-|]+)\s[-|@]\s(?P<company>[^\n\-|]+)", re.IGNORECASE),
    re.compile(r"(?P<title>[^\n]+)\sat\s(?P<company>[^\n]+)", re.IGNORECASE),
]
LOCATION_PATTERNS = [
    re.compile(r"location[:\s]+(?P<location>[^\n,]+)", re.IGNORECASE),
    re.compile(r"\b(remote|hybrid|on-site|onsite)\b", re.IGNORECASE),
]
LINK_PATTERN = re.compile(r"https?://[^\s\]>\)\"']+")


def _clean(value: Optional[str]) -> str:
    return (value or "").strip(" \n\t-|")


def parse_job_from_email(email: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    subject = email.get("subject", "")
    body = email.get("body", "")
    snippet = email.get("snippet", "")

    title = ""
    company = ""
    for chunk in [subject, body[:500]]:
        for pattern in TITLE_COMPANY_PATTERNS:
            m = pattern.search(chunk)
            if m:
                title = _clean(m.group("title"))
                company = _clean(m.group("company"))
                break
        if title:
            break

    merged = "\n".join([subject, snippet, body])
    location = ""
    for pattern in LOCATION_PATTERNS:
        m = pattern.search(merged)
        if m:
            location = _clean(m.groupdict().get("location") or m.group(0))
            break

    links = LINK_PATTERN.findall(merged)
    job_link = ""
    for link in links:
        if "linkedin.com/jobs" in link.lower() or "indeed.com" in link.lower():
            job_link = link
            break
    if not job_link and links:
        job_link = links[0]

    if not title and subject:
        title = _clean(subject.split("-")[0])[:120]
    if not company:
        company = "Unknown Company"
    if not title:
        return None

    return {
        "title": title,
        "company": company,
        "location": location,
        "job_link": job_link,
        "description": _clean(snippet) or _clean(body[:240]),
    }
