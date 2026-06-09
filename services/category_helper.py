"""Heuristic job category + normalization for dashboard filters."""

from typing import Any, Dict, List

VALID_CATEGORIES = [
    "AI Automation",
    "AI Agent / Workflow",
    "Product / Startup",
    "No-code / Low-code",
    "Operations Automation",
    "Data / Analytics",
    "Other",
]

_KEYWORDS: Dict[str, List[str]] = {
    "AI Automation": ["automation engineer", "ai automation", "prozessautomation", "rpa ", " robotic process automation", "automatisierung"],
    "AI Agent / Workflow": ["ai agent", "llm workflow", "genai agent", "ai workflow", "orchestration", "langchain"],
    "Product / Startup": ["product ops", "product operations", "startup", "technical product", "chief of staff"],
    "No-code / Low-code": ["no-code", "nocode", "low-code", "low code", "zapier", "make.com", "n8n", "bubble.", "bubble.io"],
    "Operations Automation": ["operations automation", "business process", "bpo automation", "ops automation"],
    "Data / Analytics": ["data analyst", "analytics engineer", "bi developer", "data engineer", "sql ", " tableau", "semantic layer"],
}


def normalize_ai_category(ai_value: Any) -> str:
    raw = str(ai_value or "").strip()
    if not raw or raw.lower() == "unknown":
        return "Other"
    simplified = raw.replace("_", "/").strip()
    for cat in VALID_CATEGORIES:
        if cat.lower() == simplified.lower():
            return cat
    low = simplified.lower()
    for cat in VALID_CATEGORIES[:-1]:
        if cat.lower() in low:
            return cat
    return "Other"


def heuristic_category(blob: Dict[str, str]) -> str:
    text = " ".join(
        filter(
            None,
            [
                blob.get("title"),
                blob.get("description"),
                blob.get("email_subject"),
                blob.get("email_snippet"),
                (blob.get("email_body_excerpt") or "")[:2400],
            ],
        )
    ).lower()

    hits: Dict[str, int] = {}
    for cat, needles in _KEYWORDS.items():
        score = sum(1 for needle in needles if needle in text)
        if score:
            hits[cat] = score
    if not hits:
        return "Other"
    return sorted(hits.keys(), key=lambda k: hits[k], reverse=True)[0]
