"""Deterministic skill/tool phrases when AI returns empty but JD text exists."""

from __future__ import annotations

import re
from typing import Dict, List, Sequence, Set, Tuple


def _dedupe_ordered(items: Sequence[str]) -> List[str]:
    seen: Set[str] = set()
    out: List[str] = []
    for raw in items:
        s = str(raw).strip()
        low = s.lower()
        if not s or low in seen:
            continue
        seen.add(low)
        out.append(s)
    return out


def _phrase_in(text_lc: str, needle_lc: str) -> bool:
    if not needle_lc:
        return False
    # Multi-word or long snippets: naive substring avoids missing "REST APIs".
    if " " in needle_lc or len(needle_lc) >= 6:
        return needle_lc in text_lc
    return re.search(rf"(?<![a-z0-9]){re.escape(needle_lc)}(?![a-z0-9])", text_lc) is not None


def extract_skills_fallback(text: str) -> Dict[str, List[str]]:
    """
    Lightweight extractor for pasted/discovered postings (keyword-based, not NLP).
    """
    blank = {"required_skills": [], "nice_to_have_skills": [], "tools_technologies": []}
    if not text or not text.strip():
        return dict(blank)

    parts = re.split(r"(?is)\bnice\s+to\s+have\s*:?", text, maxsplit=1)
    main_lc = parts[0].lower()
    nice_lc = parts[1].lower() if len(parts) > 1 else ""

    req_defs: Sequence[Tuple[str, str]] = (
        ("workflow automation", "workflow automation"),
        ("process automation", "process automation"),
        ("requirements gathering", "requirements gathering"),
        ("stakeholder management", "stakeholder management"),
        ("documenting workflows", "documenting workflows"),
        ("business process automation", "business process automation"),
        ("data analysis", "data analysis"),
        ("project management", "project management"),
        ("Python", "python"),
        ("SQL", "sql"),
        ("Excel", "excel"),
    )

    tool_defs: Sequence[Tuple[str, str]] = (
        ("Google Sheets", "google sheets"),
        ("REST APIs", "rest apis"),
        ("REST API", "rest api"),
        ("ChatGPT", "chatgpt"),
        ("Salesforce", "salesforce"),
        ("HubSpot", "hubspot"),
        ("Power BI", "power bi"),
        ("Tableau", "tableau"),
        ("Airtable", "airtable"),
        ("Slack", "slack"),
        ("Notion", "notion"),
        ("Zapier", "zapier"),
        ("Make", r"(?<![a-z0-9])make(?![a-z0-9\.])"),
        ("n8n", r"(?<![a-z0-9])n8n(?![a-z0-9])"),
        ("Jira", "jira"),
        ("Confluence", "confluence"),
        ("OpenAI", "openai"),
        ("LLM", r"(?<![a-z0-9])llm(?![a-z0-9])"),
        ("AI agents", "ai agents"),
        ("APIs", r"(?<![a-z0-9])apis(?![a-z0-9])"),
        ("CRM", r"(?<![a-z0-9])crm(?![a-z0-9\.])"),
    )

    nice_defs: Sequence[Tuple[str, str]] = (
        ("product management", "product management"),
        ("Power BI", "power bi"),
        ("Tableau", "tableau"),
        ("HubSpot", "hubspot"),
        ("Salesforce", "salesforce"),
        ("CRM", r"(?<![a-z0-9])crm(?![a-z0-9\.])"),
        ("low-code", "low-code"),
        ("no-code", "no-code"),
    )

    required: List[str] = []

    # Longer needles first reduces duplicate granularity for overlapping phrases we keep both via dedupe? workflow vs business process automation both fine
    for label, needle in sorted(req_defs, key=lambda kv: len(kv[1]), reverse=True):
        if needle.startswith("(?"):
            hit = re.search(needle, main_lc) is not None
        else:
            hit = _phrase_in(main_lc, needle)
        if hit:
            required.append(label)

    tools: List[str] = []
    for label, needle in sorted(tool_defs, key=lambda kv: len(kv[1]), reverse=True):
        if needle.startswith("(?"):
            hit = re.search(needle, main_lc) is not None
        else:
            hit = _phrase_in(main_lc, needle)
        if label == "APIs" and ("rest apis" in main_lc or "rest api" in main_lc):
            continue
        if hit:
            tools.append(label)

    if "REST APIs" in tools:
        tools = [t for t in tools if t != "REST API"]

    nice_bucket = nice_lc if nice_lc.strip() else ""
    nice_hits: List[str] = []
    if nice_bucket.strip():
        for label, needle in sorted(nice_defs, key=lambda kv: len(kv[1]), reverse=True):
            if needle.startswith("(?"):
                hit = re.search(needle, nice_bucket) is not None
            else:
                hit = _phrase_in(nice_bucket, needle)
            if hit:
                nice_hits.append(label)

    # Tone down noisy bare automation duplicates when specific automation phrases matched.
    if any(x.lower().startswith(("workflow automation", "process automation")) for x in required):
        required = [x for x in required if x.lower() != "automation"]

    return {
        "required_skills": _dedupe_ordered(required),
        "nice_to_have_skills": _dedupe_ordered(nice_hits),
        "tools_technologies": _dedupe_ordered(tools),
    }
