"""
Heuristic target-fit scoring for automation / AI / product / tool-building roles.
Runs before AI enrichment; only strong/medium pass the pipeline gate.
"""

from typing import Any, Dict, List, Tuple

from config import settings

# Positive signals (career direction)
POSITIVE_TERMS: List[Tuple[str, int]] = [
    ("automation", 14),
    ("workflow automation", 18),
    ("business process automation", 20),
    ("process automation", 16),
    ("ai agent", 18),
    ("ai agents", 18),
    ("llm", 14),
    ("genai", 14),
    ("generative ai", 16),
    ("machine learning", 14),
    ("ml engineer", 16),
    ("data science", 10),
    ("no-code", 12),
    ("low-code", 12),
    ("nocode", 10),
    ("internal tools", 16),
    ("developer tools", 14),
    ("product operations", 16),
    ("technical product", 16),
    ("product ops", 14),
    ("ai implementation", 18),
    ("ai workflow", 16),
    ("operations", 6),
    ("startup", 8),
    ("pipeline", 10),
    ("orchestration", 12),
    ("rpa", 14),
    ("api integration", 12),
    ("zapier", 10),
    ("make.com", 10),
    ("n8n", 12),
    ("vibe coding", 14),
    ("tooling", 10),
    ("copilot", 8),
    ("prompt engineering", 12),
    ("retrieval augmented", 12),
    ("software engineer", 8),
    ("softwareentwickler", 8),
    ("python", 6),
    ("typescript", 6),
    ("kubernetes", 6),
    ("cloud", 4),
    ("künstliche intelligenz", 14),
    ("prozessoptimierung", 10),
    ("digital transformation", 10),
    ("early-career", 4),
    ("early career", 4),
]

# Strong negatives unless matched with clear automation/AI/tool-building signals
NEGATIVE_TERMS: List[Tuple[str, int]] = [
    ("call center", 35),
    ("inbound call", 30),
    ("outbound call", 30),
    ("telefonist", 30),
    ("kundenservice", 28),
    ("customer support", 32),
    ("support agent", 28),
    ("chat agent", 22),
    ("help desk", 22),
    ("warehouse", 28),
    ("lagerhelfer", 28),
    ("kommissionierer", 24),
    ("forklift", 24),
    ("stapler", 20),
    ("verkäufer", 28),
    ("verkaufer", 28),
    ("einzelhandel", 24),
    ("retail", 24),
    ("cashier", 24),
    ("kassierer", 22),
    ("driver", 22),
    ("fahrer", 22),
    ("lieferfahrer", 22),
    ("cleaning", 24),
    ("reinigung", 24),
    ("putzkraft", 24),
    ("housekeeping", 22),
    ("hospitality", 20),
    ("gastronom", 18),
    ("kellner", 20),
    ("barista", 18),
    ("security guard", 18),
    ("pflegehelfer", 18),
    ("pflegekraft", 14),
]

# Generic low-skill / broad student roles (lighter penalty)
SOFT_NEGATIVE_TERMS: List[Tuple[str, int]] = [
    ("aushilfe", 12),
    ("minijob", 10),
    ("450 euro", 8),
    ("hilfskraft", 10),
    ("packer", 14),
    ("montage", 10),
]

STRONG_MIN_DEFAULT = 72


def _normalize_blob(job: Dict[str, Any], email: Dict[str, Any]) -> str:
    parts = [
        str(job.get("title", "")),
        str(job.get("company", "")),
        str(job.get("description", "")),
        str(job.get("source", "")),
        str(email.get("subject", "")),
        str(email.get("snippet", "")),
        str(email.get("body", ""))[:8000],
    ]
    return " ".join(parts).lower()


def _collect_matches(text: str, terms: List[Tuple[str, int]]) -> Tuple[int, List[str]]:
    score = 0
    matched: List[str] = []
    for phrase, weight in terms:
        if phrase in text:
            score += weight
            matched.append(phrase)
    return score, matched


def evaluate_target_fit(job: Dict[str, Any], email: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns:
      target_fit: strong | medium | weak | reject
      target_score: 0-100
      matched_keywords: list of matched positive (and soft tags for negatives in reject_reason)
      reject_reason: string when weak/reject
    """
    text = _normalize_blob(job, email)
    title_lower = str(job.get("title", "")).lower()

    pos_score, pos_kw = _collect_matches(text, POSITIVE_TERMS)
    neg_score, neg_kw = _collect_matches(text, NEGATIVE_TERMS)
    soft_score, soft_kw = _collect_matches(text, SOFT_NEGATIVE_TERMS)

    # Title emphasis: double-count positive hits that appear in title
    title_bonus = 0
    title_hits: List[str] = []
    for phrase, weight in POSITIVE_TERMS:
        if phrase in title_lower:
            title_bonus += min(weight, 12)
            if phrase not in pos_kw:
                title_hits.append(f"title:{phrase}")
    pos_score += title_bonus

    raw = 45 + pos_score - neg_score - soft_score
    target_score = max(0, min(100, raw))

    strong_min = STRONG_MIN_DEFAULT
    min_medium = settings.target_min_score

    matched_keywords: List[str] = sorted(set(pos_kw + title_hits))

    # Hard reject: strong negative with no meaningful positive signal
    if neg_kw and pos_score < 18:
        return {
            "target_fit": "reject",
            "target_score": target_score,
            "matched_keywords": matched_keywords,
            "reject_reason": f"Role type suggests non-target work ({', '.join(neg_kw[:4])}) without automation/AI/tool-building signals.",
        }

    # Heavy negative stack even with some positives
    if neg_score >= 40 and pos_score < 35:
        return {
            "target_fit": "reject",
            "target_score": target_score,
            "matched_keywords": matched_keywords,
            "reject_reason": "Dominant support/manual/retail signals vs. weak automation/AI relevance.",
        }

    if target_score >= strong_min:
        return {
            "target_fit": "strong",
            "target_score": target_score,
            "matched_keywords": matched_keywords,
            "reject_reason": "",
        }

    if target_score >= min_medium:
        return {
            "target_fit": "medium",
            "target_score": target_score,
            "matched_keywords": matched_keywords,
            "reject_reason": "",
        }

    if target_score >= 28:
        return {
            "target_fit": "weak",
            "target_score": target_score,
            "matched_keywords": matched_keywords,
            "reject_reason": f"Below target threshold ({min_medium}); partial match only. Negatives: {neg_kw[:3] or 'none'}; soft: {soft_kw[:3] or 'none'}.",
        }

    return {
        "target_fit": "reject",
        "target_score": target_score,
        "matched_keywords": matched_keywords,
        "reject_reason": "Score too low for automation/AI/product/tool-building focus.",
    }


def passes_target_gate(fit_result: Dict[str, Any]) -> bool:
    return fit_result["target_fit"] in ("strong", "medium")
