import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
from urllib.parse import urlparse

from flask import Flask, Response, flash, jsonify, redirect, render_template, request, url_for

from config import reload_runtime_settings, settings
from database import application_profile as application_profile_store
from database.db import (
    VALID_STATUSES,
    cleanup_low_quality_jobs,
    deduplicate_jobs,
    dashboard_review_counts,
    get_job,
    get_latest_apply_assist_session,
    get_status_history,
    init_db,
    insert_apply_assist_session,
    list_jobs,
    restore_job,
    soft_delete_job,
    toggle_job_pin,
    update_job_application_url,
    update_job_user_category,
    update_job_status,
)
from services.apply_assist import (
    build_application_packet,
    mark_session_completed_manually,
    resolve_apply_url,
    start_apply_assist_background,
    verify_playwright_chromium,
)
from services.ai_service import resolve_effective_source_quality
from services.ai_skills_fallback import extract_skills_fallback
from services.category_helper import VALID_CATEGORIES
from services.pipeline import JobPipeline
from services.source_quality import VALID_SOURCE_QUALITY, badge_class, label_for_quality

DISCOVERY_OPTIONS = ["found", "original_only", "email_only", "failed"]
CAREER_FIT_FILTERS = ["strong", "medium", "weak", "reject"]
LOCATION_FIT_FILTERS = ["nrw", "remote_germany", "unclear", "outside_target"]
SOURCE_QUALITY_FILTERS = list(VALID_SOURCE_QUALITY)
FRESHNESS_FILTERS = [
    ("fresh", "Fresh"),
    ("active", "Active"),
    ("unknown", "Unknown / unset"),
    ("old", "Older"),
    ("likely_expired", "Likely expired"),
]

# Flask run port (desktop / Tauri doc references this host)
DEFAULT_PORT = int(os.getenv("FLASK_RUN_PORT") or os.getenv("PORT") or "5000")
_LOGGER = logging.getLogger(__name__)


def _redirect_back(default_endpoint: str = "dashboard"):
    candidate = request.form.get("next") or request.args.get("next") or ""
    parsed = urlparse(candidate)
    if parsed.scheme or parsed.netloc:
        flash("Ignoring unsafe redirect target.", "error")
        return redirect(url_for(default_endpoint))
    if candidate.startswith("/"):
        return redirect(candidate)
    return redirect(url_for(default_endpoint))


def _discovery_short(status: Optional[str]) -> str:
    return {
        "found": "Found",
        "original_only": "Orig",
        "email_only": "Email",
        "failed": "Fail",
        None: "—",
        "": "—",
    }.get(status or "", status or "—")


def _json_list(raw: Optional[str]) -> List[str]:
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(x) for x in raw if str(x).strip()]
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            return [str(x).strip() for x in data if str(x).strip()]
    except json.JSONDecodeError:
        pass
    return [raw] if isinstance(raw, str) and raw.strip() else []


def _skills_preview(raw: Optional[str], max_items: int = 3, max_len: int = 72) -> str:
    skills = _json_list(raw)
    if not skills:
        return "—"
    joined = ", ".join(skills[:max_items])
    if len(joined) > max_len:
        return joined[: max_len - 1].rstrip() + "…"
    return joined


def _effective_career_fit(row: Dict[str, Any]) -> str:
    c = str(row.get("career_fit") or "").strip()
    if c:
        return c
    return str(row.get("target_fit") or "").strip()


def _eff_fit_slug(eff_fit: str) -> str:
    s = str(eff_fit or "").strip().lower()
    return s if s else "na"


def _effective_career_score(row: Dict[str, Any]) -> Optional[int]:
    cs = row.get("career_fit_score")
    if cs is not None:
        try:
            return int(cs)
        except (TypeError, ValueError):
            pass
    ts = row.get("target_score")
    if ts is not None:
        try:
            return int(ts)
        except (TypeError, ValueError):
            pass
    return None


def _dash_top_skills_tools(skills_raw: Any, tools_raw: Any, limit: int = 3) -> List[str]:
    sk = list(_json_list(skills_raw))
    tl = list(_json_list(tools_raw))
    out: List[str] = []
    seen = set()
    for seq in (sk, tl):
        for x in seq:
            tok = str(x).strip()
            low = tok.lower()
            if not tok or low in seen:
                continue
            seen.add(low)
            out.append(tok[:48])
            if len(out) >= limit:
                return out[:limit]
    return out[:limit]


def _remote_label(remote_val: Optional[int]) -> str:
    if remote_val == 1:
        return "Remote (likely)"
    if remote_val == 0:
        return "Not remote"
    return "Unknown"


def _intel_plain(val: Optional[str]) -> str:
    t = str(val or "").strip()
    return t if t and t.lower() != "unknown" else "—"


def _skills_explanation_when_empty(job: Dict[str, Any]) -> Optional[str]:
    has_any = bool(
        (job.get("required_skills_list") or [])
        or (job.get("nice_to_have_list") or [])
        or (job.get("tools_list") or [])
    )
    if has_any:
        return None
    tf = (job.get("target_fit") or "").lower()
    cf = str(job.get("career_fit_reason") or "").strip()
    mism = job.get("mismatch_lines") or []

    if tf in ("reject", "weak"):
        parts = [
            "No automation, AI-workflow, or internal-tooling skill profile is surfaced here because "
            "this role does not score as relevant to your automation/AI/product path."
        ]
        if cf:
            parts.append(cf[:520])
        if mism:
            parts.append("Signals: " + "; ".join(mism[:5])[:400])
        return " ".join(parts)[:980]

    if str(job.get("source_quality") or "").strip() in ("email_snapshot", "not_found"):
        return (
            "Only a Gmail snapshot — skills and tools are withheld until you paste or fetch "
            "the full posting."
        )

    if cf:
        return (
            "No discrete skills/tools were enumerated in the sourced text despite a usable posting excerpt. "
            + cf[:400]
        )
    return (
        "No discrete skills/tools were enumerated in sourced text yet — paste the employer posting "
        "or run research to extract lists."
    )


def _match_score_line(row: Dict[str, Any]) -> str:
    eff = (_effective_career_fit(row) or "").strip()
    sc = _effective_career_score(row)
    if eff and sc is not None:
        return f"{eff.capitalize()} · Match score {sc}"
    if eff:
        return eff.capitalize()
    if sc is not None:
        return f"Match score {sc}"
    return "—"


def _reco_human_line(recommendation: Optional[str], score: Any) -> str:
    r = (recommendation or "review").strip().lower()
    label = {"apply": "Apply", "review": "Review", "skip": "Skip"}.get(r, "Review")
    try:
        sc_int = int(score) if score is not None and str(score).strip() != "" else None
    except (TypeError, ValueError):
        sc_int = None
    if sc_int is not None:
        return f"Recommended: {label} (score {sc_int})"
    return f"Recommended: {label}"


def _decision_what_paragraph(job: Dict[str, Any]) -> str:
    wti = _intel_plain(job.get("what_the_job_is"))
    if wti != "—":
        return wti
    summ = str(job.get("summary") or "").strip()
    if summ and summ.lower() not in ("unknown",):
        return summ[:1400]
    krs = job.get("key_responsibilities_list") or []
    if krs:
        return "From the posting:\n" + "\n".join(f"• {x}" for x in krs[:8])
    return (
        "Paste the full job description or use Research below — there isn’t enough text to describe the role yet."
    )


def _worth_apply_combined(job: Dict[str, Any]) -> str:
    worth = str(job.get("fit_path_worth") or "").strip()
    reasoning = str(job.get("reasoning") or "").strip()
    parts = []
    if worth:
        parts.append(worth)
    if reasoning and reasoning.lower() not in ("unknown", ""):
        parts.append(reasoning)
    return "\n\n".join(parts) if parts else "—"


def _json_obj(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def _truncate_blurb(text: str, max_chars: int = 220) -> str:
    s = " ".join(text.split())
    if len(s) <= max_chars:
        return s
    clipped = s[: max_chars - 1].rsplit(" ", 1)[0].strip()
    return clipped + "…" if clipped else s[:max_chars] + "…"


def _reasoning_struct_from_job(job: Dict[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = dict(_json_obj(job.get("reasoning")))
    rp = job.get("raw_response")
    if isinstance(rp, str) and rp.strip():
        try:
            raw = json.loads(rp)
        except json.JSONDecodeError:
            raw = {}
        if isinstance(raw, dict):
            rs = raw.get("reasoning_struct")
            if isinstance(rs, dict):
                for key, val in rs.items():
                    cur = merged.get(key)
                    if cur in (None, [], "") and val not in (None, [], ""):
                        merged[key] = val
    return merged


def _normalize_bullet_list(val: Any, max_items: int = 8) -> List[str]:
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip()][:max_items]
    return []


def _company_does_line(job: Dict[str, Any]) -> str:
    for key in ("business_context_display", "why_this_role_exists_display"):
        raw = str(job.get(key) or "").strip()
        if raw and raw.lower() not in ("—", "unknown"):
            return _truncate_blurb(raw, 220)
    summ = str(job.get("summary") or "").strip()
    if summ and summ.lower() not in ("unknown", ""):
        low = summ.lower()
        if any(k in low for k in ("platform", "software", "tool", "product", "service", "customers", "teams")):
            return _truncate_blurb(summ, 220)
    return "Not clear from current posting — search/company research needed."


def _actually_do_bullets(job: Dict[str, Any]) -> List[str]:
    classification = _json_obj(job.get("job_classification"))
    job_type = str(classification.get("job_type") or "").strip().lower()
    depth = str(classification.get("engineering_depth") or "").strip().lower()

    # Concrete, role-based task templates.
    # This replaces generic summaries with "what you'd actually do" bullets.
    if job_type == "ai_workflow":
        bullets = [
            "You will build and test LLM/agent workflows that turn inputs into concrete actions.",
            "You will connect CRM/email/ticketing APIs so AI outputs can create or update records.",
            "You will debug prompts, tool calls, retries, and fallbacks so automations do not fail silently.",
            "You will add monitoring and guardrails around workflow runs and errors.",
        ]
    elif job_type == "backend_engineering":
        bullets = [
            "You will build and maintain backend APIs/services used by product workflows.",
            "You will improve reliability and performance (queues, retries, caching, observability).",
            "You will implement data/storage flows needed by production services.",
            "You will ship backend changes and support them in production.",
        ]
    elif job_type == "product_ai":
        bullets = [
            "You will translate AI product goals into concrete workflow deliverables.",
            "You will define tooling/integration requirements with engineering.",
            "You will coordinate experiments and rollouts, then track real usage outcomes.",
            "You will keep stakeholders aligned on what shipped and what still blocks delivery.",
        ]
    elif job_type == "data_science":
        bullets = [
            "You will build and evaluate models/analytics pipelines for measurable outcomes.",
            "You will design experiments and quantify impact against business KPIs.",
            "You will implement data pipelines and data-quality checks.",
            "You will operationalize model outputs so teams can use them in production.",
        ]
    else:
        bullets = [
            "Confirm the role’s core work (automation vs backend vs data).",
            "Identify the required tooling and decide if you want hands-on ownership.",
            "Check whether tasks are execution-heavy vs coordination-heavy.",
            "If in doubt, paste the full job posting and run Research for accuracy.",
        ]

    # Prefer concrete, action-based extracted responsibilities when they look useful.
    action_verbs = (
        "build",
        "implement",
        "integrate",
        "design",
        "develop",
        "maintain",
        "debug",
        "operate",
        "deploy",
        "ship",
        "optimize",
        "monitor",
        "scale",
        "own",
        "test",
    )

    extracted: List[str] = []
    for x in job.get("key_responsibilities_list") or []:
        s = str(x).strip()
        if not s:
            continue
        low = s.lower()
        if any(v in low for v in action_verbs) and len(s) <= 170:
            extracted.append(s)

    # Hybrid enhancement: if it's an AI workflow role but depth looks high + backend keywords appear, include a backend-leaning twist.
    corpus_hint = " ".join(
        str(job.get(k) or "")
        for k in ("description", "discovered_text", "job_page_text", "summary", "automation_ai_relevance")
    ).lower()
    backend_hint = any(k in corpus_hint for k in ("kubernetes", "microservices", "distributed", "database", "service", "api"))
    if job_type == "ai_workflow" and depth in ("high", "medium") and backend_hint:
        bullets[2] = "Debug and maintain the automation stack, including backend APIs it depends on."

    if extracted:
        # Keep templates but replace the last one/two bullets with extracted action items for more specificity.
        bullets = bullets[:3] + extracted[:2]

    return bullets[:5]


def _compute_required_skills_match_counts(job: Dict[str, Any]) -> Tuple[int, int]:
    required = job.get("required_skills_list") or []
    if not isinstance(required, list):
        return 0, 0
    required_skills = [str(x).strip() for x in required if str(x).strip()]
    required_count = len(required_skills)
    if required_count == 0:
        return 0, 0

    # Match clarity: compare extracted required skills to your AI automation/tooling profile keywords.
    # (This is a heuristic overlap, not a database of your actual skill inventory.)
    user_terms = [
        "llm",
        "ai",
        "agent",
        "automation",
        "workflow",
        "python",
        "api",
        "integration",
        "oauth",
        "n8n",
        "zapier",
        "make.com",
        "airtable",
        "crm",
        "gmail",
        "sql",
        "fastapi",
        "docker",
        "kubernetes",
    ]
    matched = 0
    for skill in required_skills:
        s = skill.lower()
        if any(t in s for t in user_terms):
            matched += 1
    return required_count, matched


def _tool_usage_explanations(job: Dict[str, Any], max_items: int = 6) -> List[str]:
    tool_map = {
        "docker": "likely used to package and deploy services consistently",
        "fastapi": "likely used to expose API endpoints around AI/workflow logic",
        "rag": "likely used to retrieve knowledge/context before LLM responses",
        "openai": "likely used for LLM calls, prompt execution, and structured outputs",
        "claude": "likely used for LLM inference and content/tool orchestration",
        "kubernetes": "likely used to run and scale services in production",
        "sql": "likely used to store/query workflow and application data",
        "postgres": "likely used as the primary relational database",
        "redis": "likely used for caching, queues, or short-lived workflow state",
        "airflow": "likely used to orchestrate scheduled pipelines/jobs",
        "n8n": "likely used to orchestrate automation workflows and external tool steps",
        "zapier": "likely used to connect SaaS tools quickly for workflow automation",
        "make.com": "likely used to build no-code automation scenarios",
    }
    out: List[str] = []
    seen: set[str] = set()
    for tool in job.get("tools_list") or []:
        t = str(tool).strip()
        if not t:
            continue
        low = t.lower()
        explanation = None
        for k, v in tool_map.items():
            if k in low:
                explanation = f"{t} — {v}"
                break
        if explanation and explanation.lower() not in seen:
            out.append(explanation)
            seen.add(explanation.lower())
        if len(out) >= max_items:
            break
    return out


def _fit_good_and_gap_bullets(job: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    st = _reasoning_struct_from_job(job)
    good = _normalize_bullet_list(st.get("good_fit"), 6)
    gaps = _normalize_bullet_list(st.get("concerns"), 8)

    if not good:
        auto = str(job.get("automation_ai_relevance") or "").strip()
        if auto and auto.lower() not in ("unknown", ""):
            for part in auto.replace(";", ".").split("."):
                chunk = part.strip()
                if len(chunk) > 16:
                    good.append(_truncate_blurb(chunk, 160))
                if len(good) >= 3:
                    break

    if not gaps:
        wn = str(job.get("why_not_fit") or "").strip()
        if wn and wn.lower() not in ("unknown", ""):
            for part in wn.split(";"):
                chunk = part.strip()
                if chunk:
                    gaps.append(_truncate_blurb(chunk, 160))
    if len(gaps) < 2:
        for m in job.get("mismatch_lines") or []:
            ms = str(m).strip()
            if ms and ms.lower() not in {g.lower() for g in gaps}:
                gaps.append(_truncate_blurb(ms, 180))
            if len(gaps) >= 5:
                break

    # Keep gap bullets practical/actionable.
    practical_gaps: List[str] = []
    for g in gaps:
        gl = g.lower()
        if "backend" in gl and "stretch" not in gl:
            practical_gaps.append("You can position this as AI-assisted workflow building, but backend ownership may be a stretch.")
        elif "unclear" in gl or "mixed" in gl:
            practical_gaps.append("Ask whether this is mainly workflow automation or production backend engineering.")
        else:
            practical_gaps.append(g)
    gaps = practical_gaps

    return good[:5], gaps[:5]


def _header_recommendation_label(rec: Optional[str]) -> str:
    r = (rec or "review").strip().lower()
    return {"apply": "Apply", "review": "Review", "skip": "Skip"}.get(r, "Review")


def _reco_ui_class(rec: Optional[str]) -> str:
    r = (rec or "review").strip().lower()
    return r if r in ("apply", "review", "skip") else "review"


def _best_apply_href(job: Dict[str, Any]) -> Optional[str]:
    return job.get("href_application_url") or job.get("href_job_link") or job.get("href_discovered")


def _best_apply_copy_value(job: Dict[str, Any]) -> str:
    return (
        (job.get("application_url_raw") or "").strip()
        or (job.get("copy_job_link_value") or "").strip()
        or (job.get("copy_discovered_value") or "").strip()
    )


def _job_search_copy_line(job: Dict[str, Any]) -> str:
    co = str(job.get("ai_company") or job.get("company") or "").strip()
    ti = str(job.get("display_title") or job.get("title") or "").strip()
    return f"Search: {co} {ti}".strip()


def _is_generic_title(value: Any) -> bool:
    s = str(value or "").strip()
    if not s:
        return True
    sl = s.lower()
    if sl in {"unknown", "unknown role", "unknown role from job alert"}:
        return True
    if sl.startswith("manual job ("):
        return True
    if "application received" in sl or "application confirmation" in sl:
        return True
    return False


def _is_generic_company(value: Any) -> bool:
    s = str(value or "").strip()
    if not s:
        return True
    return s.lower() in {"unknown company", "company unknown", "unknown"}


def _ai_raw_company(row: Dict[str, Any]) -> str:
    raw = row.get("raw_response")
    if not raw:
        return ""
    parsed: Dict[str, Any] = {}
    if isinstance(raw, dict):
        parsed = raw
    elif isinstance(raw, str):
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                parsed = obj
        except json.JSONDecodeError:
            parsed = {}
    return str(parsed.get("company") or "").strip()


def _best_display_title(row: Dict[str, Any]) -> str:
    ai_title = str(row.get("clean_title") or "").strip()
    db_title = str(row.get("title") or "").strip()
    if ai_title and not _is_generic_title(ai_title):
        return ai_title
    if db_title and not _is_generic_title(db_title):
        return db_title
    return ai_title or db_title or "Untitled"


def _best_display_company(row: Dict[str, Any]) -> str:
    ai_company = str(row.get("ai_company") or "").strip() or _ai_raw_company(row)
    db_company = str(row.get("company") or "").strip()
    if ai_company and not _is_generic_company(ai_company):
        return ai_company
    if db_company and not _is_generic_company(db_company):
        return db_company
    return ai_company or db_company or "Company unknown"


def _fit_path_sections(job: Dict[str, Any]) -> None:
    tf_raw = str(job.get("target_fit") or "").lower().strip()
    tf = tf_raw if tf_raw else "unset"
    sc = job.get("target_score")
    sc_display = sc if isinstance(sc, int) else sc
    head = str(job.get("career_fit_reason") or "").strip()
    if not head:
        head = (
            f"No career-judge rationale stored yet — target fit reads {job.get('target_fit') or 'unset'} ({sc_display}). "
            'Use “Force re-run AI” to refresh scoring.'
        )

    reco = str(job.get("recommendation") or "review").lower()
    mism = job.get("mismatch_lines") or []

    if tf in ("reject", "weak"):
        worth = (
            "Not a priority for your AI/automation/product tooling track unless you are "
            "deliberately changing industries."
        )
    elif tf == "medium":
        worth = "Optional to review — check whether tooling ownership is real vs. rebranded admin work."
    else:
        worth = (
            "Worth time to read the full posting — judge sees strong alignment cues with automation / tooling work."
            if reco != "skip"
            else "Posting looks broadly aligned, but the model suggested skipping — read the rationale below."
        )

    if tf in ("reject", "weak"):
        skills_gain = (
            "Unlikely to focus your résumé on scalable automation, agent systems, or internal product tooling."
        )
    elif not (job.get("required_skills_list") or []) and not (job.get("tools_list") or []):
        skills_gain = (
            "Skill upside is unclear from extracted bullets — inspect the JD for stacks (Make, n8n, Python, SQL, CRM automation)."
        )
    else:
        parts = []
        rs = job.get("required_skills_list") or []
        tl = job.get("tools_list") or []
        if rs:
            parts.append(", ".join(rs[:6]))
        if tl:
            parts.append("Tools: " + ", ".join(tl[:8]))
        glue = " · ".join(parts)
        skills_gain = "Could reinforce: " + glue[:420] + ("…" if len(glue) > 420 else "")

    if tf in ("reject", "weak"):
        missing = "Anything mentioning patient support volume, ticketing SLAs without automation scope, or zero mention of tooling ownership."
    elif tf == "medium":
        missing = "Clarity on how much is hands-on automation build vs coordination / documentation-only."
    else:
        missing = (
            "How much autonomy you get on stack choices, infra access, stakeholders, plus realistic on-call/Support load."
        )

    job["fit_path_headline"] = head[:1200]
    job["fit_path_worth"] = worth[:800]
    job["fit_path_skills_gain"] = skills_gain[:820]
    job["fit_path_missing"] = missing[:820]
    job["fit_path_tf_label"] = str(job.get("target_fit") or "unset").capitalize()
    job["mismatch_bullets"] = mism[:10]


def _job_freshness_ui(raw: Optional[str]) -> Tuple[str, str]:
    if raw is None or not str(raw).strip():
        return "Not set yet", "job-freshness-unknown"
    k = str(raw).strip().lower().replace("-", "_")
    labels = {
        "fresh": "Fresh listing",
        "active": "Active listing",
        "old": "Older listing",
        "likely_expired": "Likely expired",
    }
    css = {
        "fresh": "job-freshness-fresh",
        "active": "job-freshness-active",
        "old": "job-freshness-old",
        "likely_expired": "job-freshness-likely_expired",
    }
    return labels.get(k, str(raw)), css.get(k, "job-freshness-unknown")


def _seniority_display(val: Optional[str]) -> str:
    m = {"junior": "Junior", "mid": "Mid-level", "senior": "Senior", "unknown": "Unknown"}
    return m.get(str(val or "").strip().lower(), "Unknown")


def _card_tone(target_fit: Optional[str], recommendation: Optional[str]) -> str:
    tf = (target_fit or "").lower()
    if tf == "strong":
        return "tone-strong"
    if tf == "medium":
        return "tone-medium"
    if tf in {"weak", "reject"}:
        return "tone-weak"
    reco = (recommendation or "").lower().strip()
    if reco == "apply":
        return "tone-strong"
    if reco == "skip":
        return "tone-weak"
    return "tone-muted"


_APPLY_ASSIST_LABELS = {
    "starting": "Starting…",
    "browser_launched": "Browser launched",
    "ready": "Ready (safe fields filled)",
    "manual_review_required": "Manual review required",
    "completed_manually": "Completed manually",
    "error_playwright_missing": "Playwright / Chromium missing",
    "error_failed": "Stopped with error",
}


def _parse_apply_assist_json_list(raw: Any) -> List[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            out = json.loads(raw)
            return out if isinstance(out, list) else []
        except json.JSONDecodeError:
            return []
    return []


def _apply_assist_session_view(row: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not row:
        return None
    st = str(row.get("status") or "")
    return {
        "id": row.get("id"),
        "started_at": row.get("started_at"),
        "finished_at": row.get("finished_at"),
        "status": st,
        "status_label": _APPLY_ASSIST_LABELS.get(st, st.replace("_", " ").title() or "Unknown"),
        "url": row.get("url"),
        "fields_filled": _parse_apply_assist_json_list(row.get("fields_filled")),
        "fields_skipped": _parse_apply_assist_json_list(row.get("fields_skipped")),
        "manual_required": _parse_apply_assist_json_list(row.get("manual_required")),
        "fields_detected_count": len(_parse_apply_assist_json_list(row.get("fields_detected"))),
        "error_message": row.get("error_message"),
    }


def _browser_href(canonical: Optional[str], raw: Optional[str]) -> Optional[str]:
    if canonical:
        return canonical
    cand = str(raw or "").strip()
    if not cand:
        return None
    lowered = cand.lower()
    if lowered.startswith("http://") or lowered.startswith("https://"):
        return cand
    if lowered.startswith("//"):
        return f"https:{cand}"
    return f"https://{cand.lstrip('/')}"


def _location_badge_cls(location_fit: Optional[str]) -> str:
    return {
        "nrw": "loc-nrw",
        "remote_germany": "loc-remote",
        "unclear": "loc-unclear",
        "outside_target": "loc-outside",
    }.get((location_fit or "unclear"), "loc-unclear")


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["SECRET_KEY"] = settings.flask_secret_key
    init_db()
    reload_runtime_settings()

    @app.route("/health")
    def health():
        return jsonify({"ok": True})

    @app.route("/debug/storage-path")
    def debug_storage_path():
        """Non-secret paths for verifying embedded vs dev SQLite (local troubleshooting)."""
        return jsonify(
            {
                "db_path": settings.db_path,
                "job_pipeline_embedded": os.getenv("JOB_PIPELINE_EMBEDDED") == "1",
                "job_pipeline_data_dir": os.getenv("JOB_PIPELINE_DATA_DIR") or None,
            }
        )

    @app.route("/")
    def dashboard():
        status_filter = request.args.get("status", "")
        source_filter = request.args.get("source", "")
        career_fit_filter = (
            request.args.get("career_fit", "").strip()
            or request.args.get("target_fit", "").strip()
        )
        discovery_filter = request.args.get("discovery_status", "")
        location_fit_filter = request.args.get("location_fit", "")
        category_filter = request.args.get("category", "")
        source_quality_filter = request.args.get("source_quality", "").strip()
        freshness_filter = request.args.get("freshness", "").strip()
        search_q = request.args.get("q", "").strip()
        pinned_only = request.args.get("pinned_only", "") == "1"
        show_deleted = request.args.get("show_deleted", "") == "1"
        show_hidden = request.args.get("show_hidden", "") == "1" or request.args.get("relax", "") == "1"
        review_focus = not show_hidden
        application_tracked = request.args.get("application_tracked", "") == "1"
        list_review_focus = review_focus and not application_tracked
        raw_scope = (request.args.get("applied_scope") or "").strip().lower()
        applied_scope = raw_scope if raw_scope in ("applied", "open") else None

        review_stats = dashboard_review_counts()

        rows = list_jobs(
            status=status_filter or None,
            source=source_filter or None,
            career_fit=career_fit_filter or None,
            discovery_status=discovery_filter or None,
            location_fit=location_fit_filter or None,
            category=category_filter or None,
            source_quality=source_quality_filter or None,
            freshness=freshness_filter or None,
            search=search_q or None,
            pinned_only=pinned_only,
            show_deleted=show_deleted,
            review_focus=list_review_focus,
            application_tracked=application_tracked,
            applied_scope=applied_scope,
            show_duplicates=show_hidden,
        )
        jobs: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            eff_fit = _effective_career_fit(d)
            escore = _effective_career_score(d)
            d["eff_fit_slug"] = _eff_fit_slug(eff_fit)
            tf_show = eff_fit.capitalize() if eff_fit else "—"
            d["tgt_fit_display"] = tf_show
            d["tgt_score_display"] = escore if escore is not None else "—"
            cfr = str(d.get("career_fit_reason") or "").strip()
            d["career_fit_line"] = (cfr[:220] + "…") if len(cfr) > 220 else (cfr or None)
            d["dashboard_muted_low_fit"] = (eff_fit or "").lower() in ("weak", "reject")
            reco = (d.get("recommendation") or "—").lower()
            d["match_card_line"] = _match_score_line(d)
            d["reco_display"] = _reco_human_line(d.get("recommendation"), d.get("score"))
            sq = str(d.get("source_quality") or "email_snapshot").strip()
            d["source_quality"] = sq
            d["sq_label"] = label_for_quality(sq)
            d["sq_class"] = badge_class(sq)
            d["card_tone"] = _card_tone(eff_fit or d.get("target_fit"), d.get("recommendation"))
            d["location_badge"] = _location_badge_cls(d.get("location_fit"))
            loc_label = str(d.get("location_fit") or "unclear").replace("_", " ").title()
            d["loc_fit_short"] = loc_label[:18]
            d["pinned_flag"] = bool(int(d.get("pinned") or 0))
            d["category_display"] = d.get("category") or "Other"
            d["display_title"] = _best_display_title(d)
            d["display_company"] = _best_display_company(d)
            d["title_clamp"] = str(d["display_title"])[:160]
            d["reco_class"] = reco if reco in {"apply", "review", "skip"} else "review"
            d["dash_chips"] = _dash_top_skills_tools(d.get("skills"), d.get("tools_technologies"), 3)
            flabel, fcls = _job_freshness_ui(d.get("job_status"))
            d["fresh_label"] = "Unset" if flabel.startswith("Not set") else flabel
            d["fresh_class"] = fcls
            ad = str(d.get("applied_detected_at") or "").strip()
            d["application_tracked"] = bool(ad)
            acs = str(d.get("application_confirmation_sender") or "")
            d["app_confirm_sender_short"] = (acs[:72] + "…") if len(acs) > 72 else acs
            d["applied_detected_at_short"] = ad[:19] if ad else "—"
            d["app_match_conf_display"] = (
                d.get("application_match_confidence") if d.get("application_match_confidence") is not None else "—"
            )
            try:
                ec_dash = int(d.get("extraction_confidence")) if d.get("extraction_confidence") is not None else None
            except (TypeError, ValueError):
                ec_dash = None
            d["needs_review_badge"] = bool(int(d.get("needs_manual_review") or 0)) or (
                ec_dash is not None and ec_dash < 55
            )
            jobs.append(d)

        return render_template(
            "dashboard.html",
            jobs=jobs,
            statuses=sorted(list(VALID_STATUSES)),
            sources=["Indeed", "LinkedIn", "Other Job Source", "Gmail Application Confirmation"],
            categories=VALID_CATEGORIES,
            status_filter=status_filter,
            source_filter=source_filter,
            career_fit_filter=career_fit_filter,
            discovery_filter=discovery_filter,
            location_fit_filter=location_fit_filter,
            category_filter=category_filter,
            source_quality_filter=source_quality_filter,
            freshness_filter=freshness_filter,
            search_q=search_q,
            pinned_only=pinned_only,
            show_deleted=show_deleted,
            show_hidden=show_hidden,
            review_focus=review_focus,
            application_tracked=application_tracked,
            applied_scope=applied_scope or "",
            review_stats=review_stats,
            career_fit_choices=CAREER_FIT_FILTERS,
            discovery_choices=DISCOVERY_OPTIONS,
            location_fit_choices=LOCATION_FIT_FILTERS,
            source_quality_choices=SOURCE_QUALITY_FILTERS,
            freshness_choices=FRESHNESS_FILTERS,
            flask_port=DEFAULT_PORT,
        )

    @app.route("/jobs/manual/new", methods=["GET", "POST"])
    def manual_job_new():
        if request.method == "POST":
            title = (request.form.get("job_title") or "").strip() or None
            company = (request.form.get("company") or "").strip() or None
            location = (request.form.get("location") or "").strip() or None
            job_url = (request.form.get("job_url") or "").strip() or None
            full_description = (request.form.get("full_description") or "").strip()
            ok, err, job_id = JobPipeline().create_manual_job_and_analyze(
                pasted_description=full_description,
                job_title=title,
                company=company,
                location=location,
                job_url=job_url,
            )
            if not ok or not job_id:
                flash(err or "Could not save manual job.", "error")
                return (
                    render_template(
                        "manual_job_new.html",
                        form_title=title or "",
                        form_company=company or "",
                        form_location=location or "",
                        form_url=job_url or "",
                        form_description=full_description,
                        flask_port=DEFAULT_PORT,
                    ),
                    400,
                )
            flash("Manual job saved and analyzed.", "success")
            return redirect(url_for("job_detail", job_id=job_id))

        return render_template(
            "manual_job_new.html",
            form_title="",
            form_company="",
            form_location="",
            form_url="",
            form_description="",
            flask_port=DEFAULT_PORT,
        )

    @app.route("/sync", methods=["POST"])
    def sync_emails():
        pipeline = JobPipeline()
        try:
            result = pipeline.sync_emails(max_results=50)
            flash(
                (
                    f"Sync complete: fetched={result.fetched}, job_suggestions={result.job_suggestions}, "
                    f"app_confirmations={result.application_confirmations}, apps_matched={result.applications_matched}, "
                    f"apps_created={result.applications_created}, classified_other={result.classified_other}, "
                    f"passed_email_filter={result.passed_filter}, rejected_target_fit={result.rejected_target_fit}, "
                    f"rejected_title={result.rejected_title_quality}, saved_jobs={result.jobs_created_or_updated}, "
                    f"ai_enriched={result.ai_enriched}, "
                    f"discovery(found/orig/email/failed)={result.discovery_found}/{result.discovery_original_only}/"
                    f"{result.discovery_email_only}/{result.discovery_failed}, ddg_attempts={result.discovery_search_attempted}, "
                    f"ignored_total={result.ignored}"
                ),
                "success",
            )
        except Exception as exc:
            flash(f"Sync failed: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.route("/reprocess", methods=["POST"])
    def reprocess_jobs():
        try:
            limit = int(request.form.get("limit", "80"))
            limit = max(1, min(limit, 500))
        except ValueError:
            limit = 80
        try:
            result = JobPipeline().reprocess_jobs(limit=limit)
            flash(
                (
                    f"Reprocess batch done ({limit} max). Discovery counts — found={result.discovery_found}, "
                    f"original_only={result.discovery_original_only}, email_only={result.discovery_email_only}, "
                    f"failed={result.discovery_failed}, ddg_attempts={result.discovery_search_attempted}"
                ),
                "success",
            )
        except Exception as exc:
            flash(f"Reprocess failed: {exc}", "error")
        return redirect(url_for("dashboard"))

    @app.route("/debug/fetched-preview")
    def fetched_preview():
        pipeline = JobPipeline()
        try:
            preview = pipeline.fetch_preview(max_results=10)
            return jsonify({"count": len(preview), "emails": preview})
        except Exception as exc:
            return jsonify({"error": str(exc)}), 500

    @app.route("/job/<int:job_id>")
    def job_detail(job_id: int):
        row = get_job(job_id)
        if not row:
            flash("Job not found", "error")
            return redirect(url_for("dashboard"))

        job = dict(row)
        sq_raw = str(job.get("source_quality") or "email_snapshot").strip()
        job["required_skills_list"] = _json_list(job.get("skills"))
        job["nice_to_have_list"] = _json_list(job.get("nice_to_have_skills"))
        job["tools_list"] = _json_list(job.get("tools_technologies"))
        job["key_responsibilities_list"] = _json_list(job.get("key_responsibilities"))
        req_count, matched_count = _compute_required_skills_match_counts(job)
        job["required_skills_count"] = req_count
        job["matched_skills_count"] = matched_count
        flab, fcls = _job_freshness_ui(job.get("job_status"))
        job["freshness_label"] = flab
        job["freshness_class"] = fcls
        job["job_deadline_display"] = (str(job.get("job_deadline") or "").strip() or None)
        job["what_the_job_is_display"] = _intel_plain(job.get("what_the_job_is"))
        job["business_context_display"] = _intel_plain(job.get("business_context"))
        job["why_this_role_exists_display"] = _intel_plain(job.get("why_this_role_exists"))
        job["seniority_display"] = _seniority_display(job.get("seniority_level"))
        job["mismatch_lines"] = _json_list(job.get("mismatch_reasons"))
        classification = _json_obj(job.get("job_classification"))
        scoring = _json_obj(job.get("scoring_breakdown"))
        job["engineering_level_required_display"] = (
            str(job.get("engineering_level_required") or classification.get("engineering_depth") or "unknown").strip()
        )
        job["work_type_actual_display"] = str(
            job.get("work_type_actual") or classification.get("core_work") or job.get("what_the_job_is_display") or "—"
        ).strip()
        job["score_breakdown_technical"] = (
            f"alignment {scoring.get('role_alignment', 0)} + tools {scoring.get('tool_overlap', 0)} + "
            f"complexity {scoring.get('complexity_match', 0)} + growth {scoring.get('growth_potential', 0)} "
            f"- mismatch {scoring.get('mismatch_penalty', 0)}"
            if scoring
            else ""
        )

        desc_blob = "\n\n".join(
            str(chunk).strip()
            for chunk in (job.get("description"), job.get("discovered_text"), job.get("job_page_text"))
            if chunk and str(chunk).strip()
        ).strip()
        job["effective_source_quality"] = resolve_effective_source_quality(job)
        job["ai_debug_desc_len"] = len(desc_blob)

        enrichment_line = str(job.get("enrichment_sources_used") or "")
        ai_keyword_fallback = "keyword_fallback_v1" in enrichment_line
        good_skill_sources = sq_raw in ("manual_paste", "full_posting", "partial_posting")
        skills_empty = not job["required_skills_list"] and not job["nice_to_have_list"] and not job["tools_list"]
        eff_fit_slug = (_effective_career_fit(job) or "").strip().lower()
        allow_skill_ui_fallback = eff_fit_slug in ("strong", "medium")
        job["ai_debug_ui_fallback"] = False
        if good_skill_sources and skills_empty and len(desc_blob) > 300 and allow_skill_ui_fallback:
            fb = extract_skills_fallback(desc_blob)
            job["required_skills_list"] = list(fb.get("required_skills") or [])
            job["nice_to_have_list"] = list(fb.get("nice_to_have_skills") or [])
            job["tools_list"] = list(fb.get("tools_technologies") or [])
            job["ai_debug_ui_fallback"] = bool(
                job["required_skills_list"] or job["nice_to_have_list"] or job["tools_list"]
            )

        skills_empty_final = (
            not job["required_skills_list"] and not job["nice_to_have_list"] and not job["tools_list"]
        )
        job["skills_path_explanation"] = _skills_explanation_when_empty(job)
        _fit_path_sections(job)
        job["match_score_line"] = _match_score_line(job)
        job["reco_human_line"] = _reco_human_line(job.get("recommendation"), job.get("score"))
        job["decision_what"] = _decision_what_paragraph(job)
        job["worth_apply_combined"] = _worth_apply_combined(job)
        job["show_nice_skills"] = bool(job.get("nice_to_have_list"))
        job["ai_debug_fallback_used"] = bool(ai_keyword_fallback or job["ai_debug_ui_fallback"])
        job["ai_debug_req_count"] = len(job["required_skills_list"])
        job["ai_debug_nice_count"] = len(job["nice_to_have_list"])
        job["ai_debug_tools_count"] = len(job["tools_list"])
        job["skills_debug_hint"] = bool(good_skill_sources and skills_empty_final)
        try:
            ec_job = int(job.get("extraction_confidence")) if job.get("extraction_confidence") is not None else None
        except (TypeError, ValueError):
            ec_job = None
        job["needs_review_badge"] = bool(int(job.get("needs_manual_review") or 0)) or (
            ec_job is not None and ec_job < 55
        )
        job["show_improve_job_box"] = bool(skills_empty_final) and (
            sq_raw in ("email_snapshot", "not_found")
            or bool(int(job.get("needs_manual_review") or 0))
            or (ec_job is not None and ec_job < 55)
        )
        job["display_title"] = _best_display_title(job)
        job["remote_label"] = _remote_label(job.get("remote"))
        job["application_confirmed"] = bool(str(job.get("applied_detected_at") or "").strip())
        amc = job.get("application_match_confidence")
        job["application_match_conf_display"] = amc if amc is not None else "—"

        eb = job.get("email_body") or ""
        job["email_body_preview"] = eb[:2400] + ("…" if len(eb) > 2400 else "")

        raw_ai: Dict[str, Any] = {}
        raw_payload = job.get("raw_response")
        if raw_payload:
            try:
                raw_ai = json.loads(raw_payload)
            except json.JSONDecodeError:
                raw_ai = {}

        def _ai_str(val: Any) -> Optional[str]:
            if not isinstance(val, str):
                return None
            s = val.strip()
            if not s or s.lower() == "unknown":
                return None
            return s

        job["ai_company"] = _ai_str(raw_ai.get("company")) or job.get("company")
        job["display_company"] = _best_display_company(job)
        job["ai_location"] = _ai_str(raw_ai.get("location")) or job.get("location")

        job["discovery_label"] = _discovery_short(job.get("discovery_status"))
        job["discovery_class"] = (job.get("discovery_status") or "na").lower()
        job["source_quality"] = sq_raw
        job["sq_label"] = label_for_quality(sq_raw)
        job["sq_class"] = badge_class(sq_raw)
        job["missing_posting_warning"] = sq_raw in ("email_snapshot", "not_found")
        job["location_badge"] = _location_badge_cls(job.get("location_fit"))
        job["category_display"] = job.get("category") or "Other"
        job["category_locked_bool"] = bool(int(job.get("category_locked") or 0))
        job["pinned_flag"] = bool(int(job.get("pinned") or 0))
        job["is_deleted"] = bool(job.get("deleted_at"))

        job["canonical_job_link"] = JobPipeline._normalize_posting_url(job.get("job_link"))
        job["canonical_discovered_link"] = JobPipeline._normalize_posting_url(job.get("discovered_url"))
        job["copy_job_link_value"] = (job["canonical_job_link"] or str(job.get("job_link") or "").strip()) or ""
        job["copy_discovered_value"] = (job["canonical_discovered_link"] or str(job.get("discovered_url") or "").strip()) or ""
        jl_raw = str(job.get("job_link") or "").strip()
        job["href_job_link"] = _browser_href(job["canonical_job_link"], job.get("job_link"))
        job["job_link_present"] = bool(jl_raw)
        job["job_link_broken"] = bool(job.get("canonical_job_link") is None and jl_raw)
        job["href_discovered"] = _browser_href(job["canonical_discovered_link"], job.get("discovered_url"))
        job["discovered_link_present"] = bool(str(job.get("discovered_url") or "").strip())
        du = job.get("discovered_url")
        job["discovered_link_broken"] = bool(du and str(du).strip() and job.get("canonical_discovered_link") is None)
        manual_source = (job.get("discovered_source") or "").lower()
        manualish = "manual" in manual_source
        has_public_link = bool(job["copy_job_link_value"] or job["copy_discovered_value"])
        job["manual_paste_notice"] = bool(manualish and not has_public_link)

        jpt = job.get("job_page_text") or ""
        job["job_page_preview"] = jpt[:4000] + ("…" if len(jpt) > 4000 else "")
        disc_blob = job.get("discovered_text") or ""
        job["discovered_preview"] = disc_blob[:4000] + ("…" if len(disc_blob) > 4000 else "")

        canonical_app_url = JobPipeline._normalize_posting_url(job.get("application_url"))
        job["application_url_raw"] = str(job.get("application_url") or "").strip()
        job["href_application_url"] = _browser_href(canonical_app_url, job.get("application_url"))
        job["apply_assist_resolved_url"] = (resolve_apply_url(job) or "").strip()
        sess_row = get_latest_apply_assist_session(job_id)
        job["apply_assist_session"] = _apply_assist_session_view(dict(sess_row) if sess_row else None)
        _prof = application_profile_store.get_application_profile()
        job["application_profile_ready"] = application_profile_store.profile_ready_for_assist(_prof)
        job["application_packet"] = build_application_packet(job, _prof)
        job["match_score_pct"] = _effective_career_score(job)
        job["header_reco_label"] = _header_recommendation_label(job.get("recommendation"))
        job["reco_ui_class"] = _reco_ui_class(job.get("recommendation"))
        job["company_does_line"] = _company_does_line(job)
        job["actually_do_bullets"] = _actually_do_bullets(job)
        job["fit_good_bullets"], job["fit_gap_bullets"] = _fit_good_and_gap_bullets(job)
        job["tool_explanations"] = _tool_usage_explanations(job)
        job["best_apply_href"] = _best_apply_href(job)
        job["best_apply_copy_value"] = _best_apply_copy_value(job)
        job["job_search_copy_line"] = _job_search_copy_line(job)
        job["persist_debug"] = {
            "db_path": settings.db_path,
            "embedded": os.getenv("JOB_PIPELINE_EMBEDDED") == "1",
            "data_dir": os.getenv("JOB_PIPELINE_DATA_DIR", "") or None,
            "source_quality": sq_raw,
            "discovered_source": (str(job.get("discovered_source") or "").strip() or None),
            "discovered_text_len": len(str(job.get("discovered_text") or "")),
            "job_page_text_len": len(str(job.get("job_page_text") or "")),
        }

        return render_template(
            "job_detail.html",
            job=job,
            history=get_status_history(job_id),
            statuses=sorted(list(VALID_STATUSES)),
            categories=VALID_CATEGORIES,
            flask_port=DEFAULT_PORT,
        )

    @app.route("/job/<int:job_id>/status", methods=["POST"])
    def set_job_status(job_id: int):
        if update_job_status(job_id, request.form.get("status", ""), note=request.form.get("note", "")):
            flash("Status updated", "success")
        else:
            flash("Could not update status", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/quick-status", methods=["POST"])
    def quick_status(job_id: int):
        status = request.form.get("status", "")
        note = request.form.get("note", "").strip()
        if status not in VALID_STATUSES:
            flash("Unsupported quick status.", "error")
            return _redirect_back()
        if update_job_status(job_id, status, note=note or f"Marked {status} from dashboard"):
            flash(f"Status set to {status}", "success")
        else:
            flash("Unable to quick-update status.", "error")
        return _redirect_back()

    @app.route("/job/<int:job_id>/pin", methods=["POST"])
    def toggle_pin_route(job_id: int):
        if toggle_job_pin(job_id) is None:
            flash("Unable to toggle pin.", "error")
        else:
            flash("Pin updated", "success")
        return _redirect_back()

    @app.route("/job/<int:job_id>/delete", methods=["POST"])
    def delete_job_route(job_id: int):
        if soft_delete_job(job_id):
            flash("Job moved to recycle bin.", "success")
            return redirect(url_for("dashboard"))
        flash("Could not delete job.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/restore", methods=["POST"])
    def restore_job_route(job_id: int):
        if restore_job(job_id):
            flash("Job restored", "success")
        else:
            flash("Nothing to restore", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/category", methods=["POST"])
    def update_category_route(job_id: int):
        incoming = request.form.get("category", "")
        lock_user = request.form.get("category_lock") == "on"
        if incoming not in VALID_CATEGORIES:
            flash("Invalid category.", "error")
            return redirect(url_for("job_detail", job_id=job_id))

        updated = update_job_user_category(job_id, incoming, lock_user_edit=lock_user)
        if updated:
            flash("Category saved", "success")
        else:
            flash("Could not update category.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/application-url", methods=["POST"])
    def save_job_application_url(job_id: int):
        if not get_job(job_id):
            flash("Job not found", "error")
            return redirect(url_for("dashboard"))
        raw = (request.form.get("application_url") or "").strip()
        update_job_application_url(job_id, raw or None)
        flash("Application URL saved.", "success")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/apply-assist/start", methods=["POST"])
    def apply_assist_start(job_id: int):
        row = get_job(job_id)
        if not row or row.get("deleted_at"):
            flash("Job not found.", "error")
            return redirect(url_for("dashboard"))
        profile = application_profile_store.get_application_profile()
        if not application_profile_store.profile_ready_for_assist(profile):
            flash("Add your full name and email under Settings → Application Profile first.", "error")
            return redirect(url_for("job_detail", job_id=job_id))
        job_d = dict(row or {})
        url = resolve_apply_url(job_d)
        if not url:
            flash("Add a job application link or paste the application page URL.", "error")
            return redirect(url_for("job_detail", job_id=job_id))
        ok, hint = verify_playwright_chromium()
        if not ok:
            flash(hint or "Playwright is not ready.", "error")
            return redirect(url_for("job_detail", job_id=job_id))
        sid = insert_apply_assist_session(job_id, status="starting", url=url)
        start_apply_assist_background(sid, job_id, url, profile)
        flash(
            "Apply Assist started. A Chromium window should open. "
            "Review every field yourself — nothing is submitted automatically.",
            "success",
        )
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/apply-assist/complete", methods=["POST"])
    def apply_assist_complete(job_id: int):
        if mark_session_completed_manually(job_id):
            flash("Marked this Apply Assist session as completed manually.", "success")
        else:
            flash("No Apply Assist session to update yet.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/improve-posting", methods=["POST"])
    def improve_posting(job_id: int):
        url = request.form.get("job_url") or ""
        desc = request.form.get("job_description") or ""
        ok, error = JobPipeline().improve_job_posting(job_id, url.strip() or None, desc.strip() or None)
        if ok:
            flash("Updated analysis using your pasted posting or fetched page.", "success")
        else:
            flash(error or "Could not update AI analysis.", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/force-ai-refresh", methods=["POST"])
    def force_ai_refresh_route(job_id: int):
        try:
            ok, err = JobPipeline().force_ai_refresh(job_id)
            if ok:
                flash("AI analysis re-run complete (full overwrite of stored analysis).", "success")
            else:
                flash(err or "Could not re-run AI.", "error")
        except Exception as exc:  # pragma: no cover
            _LOGGER.exception("force_ai_refresh_route job_id=%s", job_id)
            flash(f"Force AI refresh failed: {exc}", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/job/<int:job_id>/research", methods=["POST"])
    def research_job_posting_route(job_id: int):
        try:
            ok, msg = JobPipeline().research_job(job_id)
            flash(msg, "success" if ok else "error")
        except Exception as exc:  # pragma: no cover - defensive
            _LOGGER.exception("research_job_posting_route job_id=%s", job_id)
            flash(f"Research failed unexpectedly: {exc}", "error")
        return redirect(url_for("job_detail", job_id=job_id))

    @app.route("/settings")
    def app_settings():
        from database import app_settings as aps

        from services.gmail_service import GmailService

        snap = GmailService().gmail_connection_snapshot()
        db_key_set, masked = aps.get_openai_key_for_display()
        model_val = (aps.get_setting("OPENAI_MODEL") or "").strip() or settings.openai_model
        return render_template(
            "settings.html",
            flask_port=DEFAULT_PORT,
            openai_model_value=model_val,
            openai_key_configured=db_key_set,
            openai_key_masked=masked,
            gmail=snap,
        )

    @app.route("/settings/application-profile", methods=["GET", "POST"])
    def settings_application_profile():
        if request.method == "POST":
            payload = {k: (request.form.get(k) or "").strip() for k in application_profile_store.PROFILE_FIELDS}
            application_profile_store.save_application_profile(payload)
            flash("Application profile saved locally.", "success")
            return redirect(url_for("settings_application_profile"))
        profile = application_profile_store.get_application_profile()
        return render_template(
            "settings_application_profile.html",
            profile=profile,
            profile_ready=application_profile_store.profile_ready_for_assist(profile),
            flask_port=DEFAULT_PORT,
        )

    @app.route("/settings/openai", methods=["POST"])
    def settings_openai_save():
        from database import app_settings as aps

        model = (request.form.get("openai_model") or "").strip()
        key = (request.form.get("openai_api_key") or "").strip()
        if model:
            aps.set_setting("OPENAI_MODEL", model)
        if key:
            aps.set_setting("OPENAI_API_KEY", key)
        reload_runtime_settings()
        flash("OpenAI settings saved", "success")
        return redirect(url_for("app_settings"))

    @app.route("/settings/gmail/connect", methods=["POST"])
    def settings_gmail_connect():
        from services.gmail_service import GmailService

        try:
            GmailService().run_interactive_oauth(force_reauth=False)
            flash("Gmail connected — token saved locally.", "success")
        except FileNotFoundError:
            flash(
                "Download Google OAuth Desktop credentials and save as credentials.json in the project root.",
                "error",
            )
        except Exception as exc:
            flash(f"Gmail connect did not complete: {exc}", "error")
        return redirect(url_for("app_settings"))

    @app.route("/settings/gmail/reconnect", methods=["POST"])
    def settings_gmail_reconnect():
        from services.gmail_service import GmailService

        try:
            GmailService().run_interactive_oauth(force_reauth=True)
            flash("Gmail reconnected — new token saved locally.", "success")
        except FileNotFoundError:
            flash(
                "Download Google OAuth Desktop credentials and save as credentials.json in the project root.",
                "error",
            )
        except Exception as exc:
            flash(f"Gmail reconnect did not complete: {exc}", "error")
        return redirect(url_for("app_settings"))

    @app.route("/settings/gmail/disconnect", methods=["POST"])
    def settings_gmail_disconnect():
        from services.gmail_service import GmailService

        GmailService().disconnect()
        flash("Gmail disconnected", "success")
        return redirect(url_for("app_settings"))

    @app.route("/debug/classify-email-sample")
    def debug_classify_email_sample():
        from services.email_classifier import classify_text_blob

        text = request.args.get("text", "")
        return jsonify(classify_text_blob(text))

    @app.route("/debug/extract-email-sample")
    def debug_extract_email_sample():
        from services.parser import extract_email_sample

        text = request.args.get("text", "")
        return jsonify(extract_email_sample(text))

    @app.route("/debug/apply-assist-form")
    def debug_apply_assist_form():
        """Local-only test page with common field names (Apply Assist should fill but never press submit)."""
        html = """<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><title>Apply Assist test form</title></head>
<body>
<h1>Apply Assist local test</h1>
<p>Use the job detail “Start Apply Assist” with this URL, or open this file URL in Apply Assist.</p>
<form method="get" action="#">
  <p><label>First name <input type="text" name="first_name" id="first_name"></label></p>
  <p><label>Last name <input type="text" name="last_name" id="last_name"></label></p>
  <p><label>Email <input type="email" name="email" id="email"></label></p>
  <p><label>Phone <input type="tel" name="phone" id="phone"></label></p>
  <p><label>LinkedIn <input type="url" name="linkedin" id="linkedin"></label></p>
  <p><label>Cover letter <textarea name="cover_letter" id="cover_letter" rows="4"></textarea></label></p>
  <p><label>Salary expectation <input type="text" name="salary" id="salary" placeholder="Do not auto-fill"></label></p>
  <p><button type="submit" name="final" value="1">Submit application</button></p>
</form>
</body></html>"""
        return Response(html, mimetype="text/html")

    @app.route("/admin/cleanup-low-quality", methods=["POST"])
    def admin_cleanup():
        stats = cleanup_low_quality_jobs()
        drep = deduplicate_jobs()
        flash(
            (
                "Cleanup run — checked={checked}, soft_deleted={soft_deleted}, skipped_pinned={skipped_pinned}, "
                "skipped_saved_or_applied={skipped_saved_or_applied}".format(**stats)
            ),
            "success",
        )
        flash(
            (
                "Deduplicate run — checked={checked}, duplicates_found={duplicates_found}, "
                "duplicates_hidden={duplicates_hidden}, merge_collisions_skipped={merge_collisions_skipped}, "
                "canonical_updates={canonical_updates}".format(**drep)
            ),
            "success",
        )
        return redirect(url_for("dashboard"))

    return app


if __name__ == "__main__":
    _embedded = os.getenv("JOB_PIPELINE_EMBEDDED") == "1"
    _app_obj = create_app()
    # Embedded (Tauri child process): stable single-process server; interactive dev stays on debug/reloader below.
    if _embedded:
        _app_obj.run(
            debug=False,
            host="127.0.0.1",
            port=DEFAULT_PORT,
            use_reloader=False,
            threaded=True,
        )
    else:
        _app_obj.run(debug=True, host="127.0.0.1", port=DEFAULT_PORT, threaded=True)
