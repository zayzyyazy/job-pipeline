"""
NRW-focused location scoring for local job pipeline.
Marks jobs nrw | remote_germany | unclear | outside_target — does not discard rows.
"""

from __future__ import annotations

import re
from typing import Any, Dict

_NRW_PATTERNS = [
    r"\bd[uü]sseldorf\b",
    r"\bduesseldorf\b",
    r"\bduisburg\b",
    r"\bessen\b(?!\w)",
    r"\bk[oö]ln\b",
    r"\bcologne\b",
    r"\bbonn\b",
    r"\bdortmund\b",
    r"\bbochum\b",
    r"\bm[uü]nster\b",
    r"\bwuppertal\b",
    r"\bbielefeld\b",
    r"\baachen\b",
    r"\bgelsenkirchen\b",
    r"\boberhausen\b",
    r"\bkrefeld\b",
    r"\bm[oö]nchengladbach\b",
    r"\bmonchengladbach\b",
    r"\bleverkusen\b",
    r"\bneuss\b",
    r"\bratingen\b",
    r"\bhilden\b",
    r"\bsolingen\b",
    r"\bremscheid\b",
    r"\b(nrw)\b",
    r"\bnordrhein[-\s]?westfalen\b",
    r"\bnorth[\s-]rhine[-\s]?westphalia\b",
    r"\bruhr(gebiet)?\b",
]

_REMOTE_GERMANY_PATTERNS = [
    r"\b(remote|fully[\s\-]remote)[^\n]{0,40}(germany|deutschland)\b",
    r"\b(germany|deutschland)[^\n]{0,40}\b(remote|home[\s\-]office)\b",
    r"\bremote[^\n]{0,10}\b(?:de\b|\(de\)|deutschland|germany)\b",
    r"\banywhere[^\n]{0,20}(germany|deutschland)\b",
]

_HYBRID_NRW_PATTERNS = [
    r"\bhybrid[^\n]{0,30}nrw\b",
    r"\bhybrid[\s\-]nrw\b",
    r"\bteilweise[^\n]{0,30}vor[\s\-]ort[^\n]{0,40}nrw\b",
]


_OUSTIDE_REGEX = [
    # Non-target hubs / overseas (case-insensitive)
    (r"\bsofia\b", "Sofia"),
    (r"\bberlin\b", "Berlin"),
    (r"\bhamburg\b", "Hamburg"),
    (r"\bmünchen\b|munich|muenchen\b", "Munich/München"),
    (r"\bfrankfurt\b", "Frankfurt"),
    (r"\bstuttgart\b", "Stuttgart"),
    (r"\blondon\b", "London"),
    (r"\bparis\b", "Paris"),
    (r"\bamsterdam\b", "Amsterdam"),
    (r"\bwien\b|vienna\b", "Wien/Vienna"),
    (r"\b(warsaw|krak[wóo]w|poland|prague)\b", "outside Germany (CEE hub)"),
    (r"\b(united\s+states|\busa\b|san\s+francisco|new\s+york)\b", "outside Germany (USA)"),
    (r"outside\s+germany|au(er|ß)halb\s+deutschlands", "explicit non-Germany"),
]


_COMPILED_NRW = [re.compile(p, re.I) for p in _NRW_PATTERNS]
_COMPILED_REMOTE_DE = [re.compile(p, re.I) for p in _REMOTE_GERMANY_PATTERNS]
_COMPILED_HYBRID_NRW = [re.compile(p, re.I) for p in _HYBRID_NRW_PATTERNS]
_COMPILED_OUTSIDE = [(re.compile(p, re.I), label) for p, label in _OUSTIDE_REGEX]


def _norm_blob(parts: Dict[str, str]) -> str:
    chunks = []
    for key in (
        "location",
        "title",
        "email_subject",
        "email_snippet",
        "email_body_excerpt",
        "posting_excerpt",
        "manual_priority_excerpt",
    ):
        chunks.append(parts.get(key) or "")
    text = "\n".join(chunks)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def evaluate_location_fit(context: Dict[str, Any]) -> Dict[str, str]:
    manual_pri = ""
    mp = context.get("manual_priority_text") or ""
    if mp:
        manual_pri = str(mp)[:4000]
    post_ex = ""
    for key in ("discovered_text", "job_page_text"):
        blk = context.get(key) or ""
        if blk:
            post_ex = str(blk)[:6000]
            break
    blob = _norm_blob(
        {
            "location": context.get("location") or "",
            "title": context.get("title") or "",
            "email_subject": context.get("email_subject") or "",
            "email_snippet": context.get("email_snippet") or "",
            "email_body_excerpt": (context.get("email_body_excerpt") or "")[:4000],
            "posting_excerpt": post_ex,
            "manual_priority_excerpt": manual_pri,
        }
    )

    has_nrw = any(pat.search(blob) for pat in _COMPILED_NRW)
    hybrid_nrw = any(pat.search(blob) for pat in _COMPILED_HYBRID_NRW)
    remote_de = any(pat.search(blob) for pat in _COMPILED_REMOTE_DE)

    hybrid_general = bool(re.search(r"\bhybrid\b", blob, flags=re.I))
    german_anchor = bool(re.search(r"\b(?:germany|deutschland)\b", blob, flags=re.I))

    germany_wide_travel = bool(
        re.search(r"\breisebereitschaft\b|\bgermany\b.*\btravel|\btravel\b.*\bgermany\b", blob, flags=re.I | re.DOTALL)
    )

    outside_hits = [lbl for rx, lbl in _COMPILED_OUTSIDE if rx.search(blob)]

    outside_clear = bool(outside_hits)
    if has_nrw or hybrid_nrw:
        # NRW specificity wins over stray generic city mentions
        outside_clear = False

    location_fit = "unclear"
    reason_parts: list[str] = []

    if outside_clear:
        location_fit = "outside_target"
        uniq = sorted(set(outside_hits))[:4]
        reason_parts.append(f"Detected non-target location cues ({', '.join(uniq)}).")

    elif has_nrw or hybrid_nrw:
        location_fit = "nrw"
        tags = []
        if has_nrw:
            tags.append("NRW / Ruhr city")
        if hybrid_nrw:
            tags.append("hybrid NRW wording")
        reason_parts.append(" | ".join(tags) + " match.")

    elif remote_de:
        location_fit = "remote_germany"
        reason_parts.append("Remote role anchored to Germany.")

    elif hybrid_general and german_anchor:
        location_fit = "remote_germany"
        reason_parts.append("Hybrid within Germany framing (assume acceptable from NRW).")

    elif germany_wide_travel:
        location_fit = "remote_germany"
        reason_parts.append("Germany-wide wording with travel mention.")

    else:
        location_fit = "unclear"
        reason_parts.append("Insufficient NRW / Germany-remote evidence.")

    return {
        "location_fit": location_fit,
        "location_reason": " ".join(reason_parts)[:500],
    }
