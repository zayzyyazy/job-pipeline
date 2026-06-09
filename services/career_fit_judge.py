"""
Structured career-fit classifier and scorer for job alignment.
Focuses on realistic role fit for applied AI automation workflow work.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Dict, List, Sequence, Tuple

from openai import OpenAI

from config import settings

_LOGGER = logging.getLogger(__name__)

_USER_TOOL_TERMS = [
    "openai",
    "claude",
    "llm",
    "api",
    "automation",
    "workflow",
    "integration",
    "n8n",
    "zapier",
    "make.com",
    "airtable",
    "gmail",
    "crm",
    "oauth",
    "python",
]

_JOB_TYPE_PATTERNS: Dict[str, List[str]] = {
    "ai_workflow": [
        "workflow automation",
        "business process automation",
        "ai automation",
        "agent workflow",
        "integration automation",
        "internal tooling",
        "product ops automation",
        "n8n",
        "zapier",
        "make.com",
        "ai operations",
    ],
    "backend_engineering": [
        "distributed systems",
        "microservices",
        "backend engineer",
        "backend developer",
        "system architecture",
        "high performance",
        "low latency",
        "kubernetes",
        "go ",
        "rust",
    ],
    "data_science": [
        "machine learning engineer",
        "data scientist",
        "statistical model",
        "deep learning",
        "ml ops",
        "feature engineering",
    ],
    "product_ai": [
        "ai product manager",
        "technical product manager",
        "product ai",
        "product operations",
        "ai strategy",
        "ai enablement",
    ],
}

_DEPTH_HIGH = [
    "distributed systems",
    "kubernetes",
    "microservices",
    "scalability",
    "low-latency",
    "compiler",
    "high performance",
    "deep architecture",
    "advanced algorithms",
]
_DEPTH_MEDIUM = [
    "backend",
    "api development",
    "python",
    "data pipelines",
    "oauth",
    "integration",
]
_DEPTH_LOW = [
    "workflow",
    "automation",
    "low-code",
    "no-code",
    "tools",
    "orchestration",
]

_NEGATIVE_MISMATCH = [
    "clinical",
    "patient support",
    "call center",
    "customer support",
    "help desk",
    "sales representative",
]

_AI_OVERRIDE_TERMS = [
    "llm",
    "ai",
    "agent",
    "automation",
    "workflow",
]


def _contains_ai_workflow_keywords(text: str) -> bool:
    t = (text or "").lower()
    if not t.strip():
        return False
    if "llm" in t:
        return True
    if "artificial intelligence" in t:
        return True
    # avoid false positives for "ai" inside words (best effort)
    if re.search(r"\bai\b", t):
        return True
    for term in ("agent", "automation", "workflow"):
        if term in t:
            return True
    return False


def _safe_int_score(val: Any, default: int = 50) -> int:
    try:
        return max(0, min(100, int(val)))
    except (TypeError, ValueError):
        return default


def _corp(work: Dict[str, Any]) -> str:
    parts = [
        str(work.get("title") or ""),
        str(work.get("clean_title") or work.get("clean_title_ai") or ""),
        str(work.get("company") or ""),
        str(work.get("description") or ""),
        str(work.get("discovered_text") or ""),
        str(work.get("job_page_text") or ""),
        str(work.get("email_subject") or ""),
        str(work.get("email_snippet") or ""),
        str(work.get("email_body_excerpt") or "")[:6000],
    ]
    blob = "\n".join(parts)
    return blob[:28000]


def _count_hits(text: str, terms: Sequence[str]) -> int:
    low = text.lower()
    return sum(1 for t in terms if t and t.lower() in low)


def _extract_list_tokens(ai_data: Dict[str, Any]) -> List[str]:
    out: List[str] = []
    for key in ("required_skills", "skills", "tools_technologies", "tools_or_technologies"):
        val = ai_data.get(key) or []
        if isinstance(val, str):
            try:
                parsed = json.loads(val)
                if isinstance(parsed, list):
                    val = parsed
            except json.JSONDecodeError:
                val = [x.strip() for x in re.split(r"[,;\n|]+", val) if x.strip()]
        if isinstance(val, list):
            out.extend(str(x).strip().lower() for x in val if str(x).strip())
    return out


def _classify_job_type(text: str) -> str:
    scores = {k: _count_hits(text, v) for k, v in _JOB_TYPE_PATTERNS.items()}
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    top_key, top_score = ranked[0]
    return top_key if top_score > 0 else "unknown"


def _engineering_depth(text: str) -> str:
    hi = _count_hits(text, _DEPTH_HIGH)
    med = _count_hits(text, _DEPTH_MEDIUM)
    low = _count_hits(text, _DEPTH_LOW)
    if hi >= 2:
        return "high"
    if med >= 2 or (hi >= 1 and low == 0):
        return "medium"
    if low >= 1:
        return "low"
    return "medium"


def _core_work(job_type: str, text: str) -> str:
    if job_type == "ai_workflow":
        return "Build and run AI/automation workflows, integrations, and internal tools."
    if job_type == "backend_engineering":
        return "Design and maintain backend systems and platform-level services."
    if job_type == "data_science":
        return "Modeling, analytics, experimentation, and data-science-heavy delivery."
    if job_type == "product_ai":
        return "Coordinate AI product direction and cross-functional delivery."
    short = text.strip()[:180]
    return short or "Unknown"


def _fit_components(work: Dict[str, Any], ai_data: Dict[str, Any]) -> Tuple[Dict[str, int], List[str], List[str], str]:
    corpus = _corp(work)
    low = corpus.lower()
    tokens = _extract_list_tokens(ai_data)
    tokens_blob = " ".join(tokens)
    merged = f"{low}\n{tokens_blob}"

    ai_override = _contains_ai_workflow_keywords(merged)
    job_type = _classify_job_type(merged)
    if ai_override:
        # Critical accuracy fix: AI/LLM/agent/workflow roles should never be treated as pure backend engineering.
        job_type = "ai_workflow"
    depth = _engineering_depth(merged)

    good_fit: List[str] = []
    concerns: List[str] = []

    if job_type == "ai_workflow":
        role_alignment = 34
        good_fit.extend(
            [
                "Build and test LLM/agent workflows that route inputs and call tools.",
                "Integrate APIs/tools (email/CRM/ticketing/automation endpoints) into automated steps.",
                "Debug automation reliability (prompts/tool schemas, retries, fallbacks).",
            ]
        )
    elif job_type == "product_ai":
        role_alignment = 26
        good_fit.extend(
            [
                "Coordinate AI product direction and operationalize it into delivery workflows.",
                "Work with tooling/integrations to ship usable AI features.",
            ]
        )
    elif job_type == "data_science":
        role_alignment = 14
        concerns.append("Role leans data-science heavy rather than hands-on workflow automation execution.")
    elif job_type == "backend_engineering":
        role_alignment = 8
        concerns.append("Role is backend-heavy vs your workflow/automation strengths.")
    else:
        role_alignment = 18
        concerns.append("Role type is unclear; alignment confidence is limited.")

    tool_hits = _count_hits(merged, _USER_TOOL_TERMS)
    tool_overlap = max(0, min(20, tool_hits * 3))
    if tool_overlap >= 12:
        good_fit.append("Posting includes concrete overlap with your API/automation toolset.")
    elif tool_overlap <= 6:
        concerns.append("Tool overlap with your day-to-day stack is limited.")

    if depth == "low":
        complexity_match = 18
    elif depth == "medium":
        complexity_match = 14
    else:
        complexity_match = 4
        concerns.append("Engineering depth appears high and may over-index on deep backend work.")

    growth_potential = 8 if job_type in ("ai_workflow", "product_ai") else 5
    if "ownership" in merged or "lead" in merged or "build from scratch" in merged:
        growth_potential = min(10, growth_potential + 1)

    mismatch_penalty = 0
    if job_type == "backend_engineering" and depth == "high":
        mismatch_penalty = max(mismatch_penalty, 34)
        concerns.append("High-depth backend engineering mismatch penalty applied.")
    elif job_type == "backend_engineering":
        mismatch_penalty = max(mismatch_penalty, 24)
    elif job_type == "data_science" and depth in ("medium", "high"):
        mismatch_penalty = max(mismatch_penalty, 18)

    neg_hits = _count_hits(merged, _NEGATIVE_MISMATCH)
    if neg_hits:
        mismatch_penalty = min(40, mismatch_penalty + min(12, neg_hits * 4))
        concerns.append("Role includes signals outside your preferred AI automation track.")

    # If the description is clearly AI/LLM/agent/workflow, drastically reduce mismatch penalty.
    # This prevents the common failure mode: AI jobs misclassified as backend engineering with ~0-20 scores.
    if ai_override:
        mismatch_penalty = min(mismatch_penalty, 12)
        if mismatch_penalty >= 25:
            mismatch_penalty = 12

    base = role_alignment + tool_overlap + complexity_match + growth_potential - mismatch_penalty
    score = max(0, min(100, base))

    if score >= 70:
        level = "strong"
        recommendation = "apply"
    elif score >= 45:
        level = "medium"
        recommendation = "review"
    else:
        level = "weak"
        recommendation = "ignore"

    classification = {
        "job_type": job_type,
        "engineering_depth": depth,
        "core_work": _core_work(job_type, merged),
        "alignment_risk": "high" if mismatch_penalty >= 25 else ("medium" if mismatch_penalty >= 12 else "low"),
    }

    ai_data["job_classification"] = classification
    ai_data["scoring_breakdown"] = {
        "role_alignment": role_alignment,
        "tool_overlap": tool_overlap,
        "complexity_match": complexity_match,
        "growth_potential": growth_potential,
        "mismatch_penalty": mismatch_penalty,
    }
    ai_data["score"] = score
    ai_data["career_level"] = level
    ai_data["reality_check"] = (
        "Good alignment signal for workflow/agent engineering. Still verify hands-on automation ownership vs reporting/support."
        if level in ("strong", "medium")
        else "Risk of mismatch: the role may be automation-adjacent but not hands-on workflow engineering. Confirm ownership and tooling depth."
    )
    ai_data["why_not_fit"] = "; ".join(concerns[:4]) if concerns else "No major mismatch risks detected."
    ai_data["engineering_level_required"] = depth
    ai_data["work_type_actual"] = classification["core_work"]

    ai_data["reasoning"] = (
        f"Level: {level}. "
        f"Reality check: {ai_data['reality_check']} "
        f"Concerns: {ai_data['why_not_fit']}"
    )
    ai_data["reasoning_struct"] = {
        "good_fit": good_fit[:6],
        "concerns": concerns[:6],
        "reality_check": ai_data["reality_check"],
        "recommendation": recommendation,
    }
    return ai_data["scoring_breakdown"], good_fit, concerns, recommendation


def _kw_list(heuristic_kw: Sequence[str]) -> List[str]:
    if not heuristic_kw:
        return []
    if isinstance(heuristic_kw, str):
        try:
            data = json.loads(heuristic_kw)
            if isinstance(data, list):
                return [str(x) for x in data if str(x).strip()]
        except json.JSONDecodeError:
            pass
        return [heuristic_kw] if heuristic_kw.strip() else []
    return [str(x) for x in heuristic_kw if str(x).strip()]


def _normalize_verdict(parsed: Dict[str, Any]) -> Dict[str, Any]:
    cf = str(parsed.get("career_fit") or "weak").strip().lower()
    if cf not in ("strong", "medium", "weak", "reject"):
        cf = "weak"

    cfs = _safe_int_score(parsed.get("career_fit_score"), 35)
    if cf == "reject":
        cfs = min(cfs, 32)
        if cfs > 28:
            cfs = min(26, cfs)
    elif cf == "weak":
        cfs = min(cfs, 48)
    elif cf == "medium":
        cfs = max(45, min(74, cfs))
    elif cf == "strong":
        cfs = max(72, min(100, cfs))

    reason = str(parsed.get("career_fit_reason") or "").strip() or (
        "Not enough evidence for automation/AI/product tooling work."
        if cf in ("weak", "reject")
        else "Role aligns with automation/AI/product tooling path."
    )

    mismatches = parsed.get("mismatch_reasons")
    mm: List[str] = []
    if isinstance(mismatches, list):
        mm = [str(x).strip() for x in mismatches if str(x).strip()][:12]
    elif isinstance(mismatches, str) and mismatches.strip():
        mm = [mismatches.strip()[:300]]

    return {
        "career_fit": cf,
        "career_fit_score": cfs,
        "career_fit_reason": reason[:1200],
        "mismatch_reasons": mm,
    }


def heuristic_career_verdict(work: Dict[str, Any], ai_data: Dict[str, Any]) -> Dict[str, Any]:
    """Deterministic structured verdict with weighted mismatch-aware scoring."""
    _, _good, concerns, recommendation = _fit_components(work, ai_data)
    score = int(ai_data.get("score") or 0)
    level = str(ai_data.get("career_level") or "weak").lower()
    fit = {"strong": "strong", "medium": "medium", "weak": "weak"}.get(level, "reject")
    if fit == "weak" and score < 35:
        fit = "reject"
    return _normalize_verdict(
        {
            "career_fit": fit,
            "career_fit_score": score,
            "career_fit_reason": str(ai_data.get("reality_check") or ai_data.get("reasoning") or "")[:1200],
            "mismatch_reasons": concerns[:8],
            "recommendation": recommendation,
        }
    )


def merge_target_keyword_entries(heuristic_kw: Sequence[str], verdict: Dict[str, Any]) -> List[str]:
    base = list(_kw_list(heuristic_kw))
    out: List[str] = []

    cf_r = str(verdict.get("career_fit_reason") or "").strip()
    if cf_r:
        out.append(cf_r[:240])

    for m in verdict.get("mismatch_reasons") or []:
        ms = str(m).strip()
        if ms and ms not in out:
            out.append(ms[:200])

    tag = verdict.get("career_fit")
    if isinstance(tag, str) and tag:
        tier = f"career_fit_judge:{tag}"
        if tier not in out:
            out.append(tier)

    for term in base:
        if term and term not in out:
            out.append(term[:160])
        if len(out) >= 32:
            break
    return out[:32]


def apply_career_verdict_to_ai_data(ai_data: Dict[str, Any], verdict: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(ai_data)
    cf = verdict["career_fit"]
    cfs = int(verdict["career_fit_score"])

    raw = dict(out.get("raw_response") or {})
    if not isinstance(raw, dict):
        raw = {}
    raw["career_fit_judge"] = verdict
    out["raw_response"] = raw

    out["score"] = cfs
    reco = str(out.get("recommendation") or "review").lower().strip()
    if "reasoning_struct" in out and isinstance(out["reasoning_struct"], dict):
        out["reasoning"] = json.dumps(out["reasoning_struct"], ensure_ascii=True)

    if cf == "reject":
        out["recommendation"] = "skip"
    elif cf == "weak":
        out["recommendation"] = "review" if reco != "skip" else "skip"
    elif cf == "medium":
        if reco == "apply":
            out["recommendation"] = "review"
    # strong: leave recommendation unless contradictory
    if cf == "strong" and reco == "skip":
        out["recommendation"] = "review"

    # Persist user-facing recommendation flavor too.
    out["recommendation_career"] = (
        "apply" if out["recommendation"] == "apply" else ("review" if out["recommendation"] == "review" else "ignore")
    )
    return out


class CareerFitJudgeService:
    def __init__(self) -> None:
        self.enabled = bool(settings.openai_api_key)
        self.client = OpenAI(api_key=settings.openai_api_key) if self.enabled else None

    def judge(
        self,
        work: Dict[str, Any],
        ai_data: Dict[str, Any],
        heuristic_kw: Sequence[str],
    ) -> Dict[str, Any]:
        # Deterministic scoring is the source of truth to avoid optimistic keyword drift.
        verdict = heuristic_career_verdict(work, ai_data)
        verdict["judged_via"] = "structured_heuristic_v2"
        return verdict

    @staticmethod
    def _enforce_contradictions(verdict: Dict[str, Any], ai_data: Dict[str, Any]) -> None:
        """Clamp obvious contradictions between judge + AI narrative."""
        why = (
            str(ai_data.get("why_relevant") or "")
            + str(ai_data.get("automation_ai_relevance") or "")
            + str(ai_data.get("reasoning") or "")
        ).lower()
        cfs = int(verdict.get("career_fit_score") or 0)

        contradictory = False
        for phrase in (
            "patient support",
            "not centered on automation",
            "not centered",
            "not automation",
            "customer support-heavy",
            "support-heavy",
            "clinical",
            "medical administration",
            "bedside",
        ):
            if phrase in why and cfs >= 62:
                contradictory = True
                break

        if contradictory:
            verdict["career_fit"] = "weak"
            verdict["career_fit_score"] = min(cfs, 44)
            if str(verdict.get("career_fit_reason")):
                verdict["career_fit_reason"] = (
                    verdict["career_fit_reason"][:880]
                    + " (adjusted: AI narrative did not center automation/tooling versus high keyword score)."
                )


def run_career_judge_for_job(job_id: int, work: Dict[str, Any], ai_data: Dict[str, Any]) -> Dict[str, Any]:
    """Judge career fit, UPDATE jobs targeting columns + return merged AI payload for ai_analysis UPSERT."""
    from database.db import update_job_after_career_judge

    hk = _kw_list(work.get("target_matched_keywords") or [])
    srv = CareerFitJudgeService()
    verdict = srv.judge(work, ai_data, hk)
    merged_ai = apply_career_verdict_to_ai_data(ai_data, verdict)
    merged_kw = merge_target_keyword_entries(hk, verdict)
    update_job_after_career_judge(
        job_id,
        target_fit=verdict["career_fit"],
        target_score=int(verdict["career_fit_score"]),
        target_matched_keywords=merged_kw,
        career_fit=verdict["career_fit"],
        career_fit_score=int(verdict["career_fit_score"]),
        career_fit_reason=str(verdict["career_fit_reason"] or ""),
        mismatch_reasons=verdict.get("mismatch_reasons") or [],
    )
    return merged_ai


def dry_run_heuristic(work: Dict[str, Any], ai_data: Dict[str, Any]) -> Dict[str, Any]:
    """Local tests without persistence."""
    v = heuristic_career_verdict(work, ai_data)
    v["judged_via"] = "heuristic_only"
    return v
