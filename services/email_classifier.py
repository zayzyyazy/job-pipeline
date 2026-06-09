"""
Heuristic Gmail message classification for job alerts vs application confirmations.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Tuple

# German application confirmations
_DE_APP = [
    r"vielen\s+dank\s+für\s+(deine|ihre)\s+bewerbung",
    r"danke\s+für\s+(deine|ihre)\s+bewerbung",
    r"wir\s+haben\s+(deine|ihre)\s+bewerbung\s+erhalten",
    r"(deine|ihre)\s+bewerbung\s+ist\s+eingegangen",
    r"bestätigung\s+(deiner|ihrer)\s+bewerbung",
    r"eingangsbestätigung\s+bewerbung",
    r"bewerbung\s+erhalten",
    r"bewerbung\s+als\s+",
]

# English application confirmations
_EN_APP = [
    r"thank\s+you\s+for\s+your\s+application",
    r"thanks\s+for\s+applying",
    r"we\s+received\s+your\s+application",
    r"your\s+application\s+has\s+been\s+received",
    r"application\s+received",
    r"application\s+confirmation",
    r"thanks\s+for\s+your\s+interest",
    r"thank\s+you\s+for\s+applying",
    r"your\s+application\s+to\s+",
    r"application\s+for\s+",
]

# Job suggestion / alert signals (higher priority only when not app confirmation)
_JOB_ALERT = [
    r"neue\s+jobs\s+für\s+dich",
    r"new\s+jobs\s+(for\s+you|matching)",
    r"indeed\s+job\s+alert",
    r"linkedin\s+jobs?:\s*recommended",
    r"job\s+alert",
    r"empfohlene\s+stellen",
    r"matching\s+jobs",
    r"jobs?\s+you\s+might",
    r"recommended\s+for\s+you",
]

_COMPILED_DE = [(p, re.compile(p, re.I)) for p in _DE_APP]
_COMPILED_EN = [(p, re.compile(p, re.I)) for p in _EN_APP]
_COMPILED_JOB = [(p, re.compile(p, re.I)) for p in _JOB_ALERT]


def _blob(email: Dict[str, Any]) -> str:
    parts = [
        str(email.get("sender") or ""),
        str(email.get("subject") or ""),
        str(email.get("snippet") or ""),
        str(email.get("body") or "")[:12000],
    ]
    return "\n".join(parts)


def classify_email(email: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns:
      email_type: job_suggestion | application_confirmation | other
      confidence: 0-100
      matched_terms: list[str]
      reason: str
    """
    text = _blob(email)
    low = text.lower()
    matched: List[str] = []

    for label, rx in _COMPILED_DE + _COMPILED_EN:
        if rx.search(text):
            matched.append(label)
            conf = min(100, 78 + min(12, len(matched) * 4))
            return {
                "email_type": "application_confirmation",
                "confidence": conf,
                "matched_terms": matched[:12],
                "reason": "Matched application-confirmation phrase (DE/EN).",
            }

    for label, rx in _COMPILED_JOB:
        if rx.search(text):
            matched.append(label)

    if matched:
        return {
            "email_type": "job_suggestion",
            "confidence": min(95, 65 + len(matched) * 5),
            "matched_terms": matched[:12],
            "reason": "Matched job-alert / digest style phrase.",
        }

    # Weak heuristics: "Bewerbung" in subject without thank-you might still be HR mail
    if re.search(r"\bbewerbung\b", low) and re.search(r"\b(erhalten|eingang|bestätigung|confirmation)\b", low):
        return {
            "email_type": "application_confirmation",
            "confidence": 55,
            "matched_terms": ["bewerbung+status"],
            "reason": "German Bewerbung + receipt/confirmation wording.",
        }

    if "application" in low and any(
        w in low for w in ("received", "submitted", "confirmation", "thank you", "thanks")
    ):
        return {
            "email_type": "application_confirmation",
            "confidence": 52,
            "matched_terms": ["application+keyword"],
            "reason": "Application keyword with receipt/thanks context.",
        }

    if any(w in low for w in ("job alert", "job alert:", "new job", "neue stelle", "stellenangebot")):
        return {
            "email_type": "job_suggestion",
            "confidence": 58,
            "matched_terms": ["job_marketing"],
            "reason": "Generic job marketing language.",
        }

    return {
        "email_type": "other",
        "confidence": 20,
        "matched_terms": [],
        "reason": "No strong job-alert or application-confirmation signals.",
    }


def classify_text_blob(text: str) -> Dict[str, Any]:
    """Debug helper: classify arbitrary pasted text as if it were an email body."""
    return classify_email(
        {
            "sender": "",
            "subject": "",
            "snippet": "",
            "body": text or "",
        }
    )
