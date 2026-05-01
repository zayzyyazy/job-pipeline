import json
import logging
import re
from typing import Any, Dict, List, Optional, Sequence, Tuple

from openai import OpenAI

from config import settings
from services.ai_skills_fallback import extract_skills_fallback
from services.category_helper import VALID_CATEGORIES, heuristic_category, normalize_ai_category

_LOGGER = logging.getLogger(__name__)


def _as_list(val: Any) -> List[str]:
    if val is None or val == "Unknown":
        return []
    if isinstance(val, list):
        return [str(x).strip() for x in val if str(x).strip() and str(x).strip().lower() != "unknown"]
    if isinstance(val, str):
        s = val.strip()
        if not s or s.lower() == "unknown":
            return []
        if s.startswith("["):
            try:
                data = json.loads(s)
                if isinstance(data, list):
                    return [str(x).strip() for x in data if str(x).strip()]
            except json.JSONDecodeError:
                pass
        parts = re.split(r"[,;•\n|]+", s)
        chunk = [p.strip() for p in parts if p.strip() and p.strip().lower() != "unknown"]
        return chunk if len(chunk) > 1 else [s]
    return []


def _merge_parsed_skill_fields(parsed: Dict[str, Any]) -> Tuple[List[str], List[str], List[str]]:
    """Normalize alternate JSON keys from the model into three buckets."""
    required: List[str] = []
    for key in ("required_skills", "skills", "must_have_skills"):
        required.extend(_as_list(parsed.get(key)))

    nice: List[str] = []
    for key in ("nice_to_have_skills", "nice_to_have"):
        nice.extend(_as_list(parsed.get(key)))

    tools: List[str] = []
    for key in ("tools_technologies", "tools_or_technologies", "technologies", "tools"):
        tools.extend(_as_list(parsed.get(key)))

    def _dedupe(seq: Sequence[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for item in seq:
            low = item.strip().lower()
            if not low or low in seen:
                continue
            seen.add(low)
            out.append(item.strip())
        return out

    return _dedupe(required), _dedupe(nice), _dedupe(tools)


def _coerce_remote(val: Any) -> Optional[bool]:
    if val is None or val == "Unknown":
        return None
    if isinstance(val, bool):
        return val
    s = str(val).strip().lower()
    if s in ("true", "yes", "remote", "fully remote"):
        return True
    if s in ("false", "no", "onsite", "on-site", "hybrid"):
        return False
    return None


def _safe_int_score(val: Any, default: int = 50) -> int:
    try:
        return int(val)
    except (TypeError, ValueError):
        return default


def _effective_source_quality(job: Dict[str, Any], has_manual_text: bool) -> str:
    """Long manual paste should never be treated as a thin email snapshot."""
    raw = str(job.get("source_quality") or "email_snapshot").strip()
    manual = (job.get("manual_priority_text") or "").strip()
    if has_manual_text and len(manual) >= 120:
        return "manual_paste"
    return raw


def resolve_effective_source_quality(job_like: Dict[str, Any]) -> str:
    """UI/analytics helper: derive the same manual-paste uplift when paste lives on discovered_text."""
    patched = dict(job_like)
    manual = str(patched.get("manual_priority_text") or "").strip()
    if len(manual) < 120:
        src = str(patched.get("discovered_source") or "").lower()
        if any(x in src for x in ("manual_paste", "manual_form", "user_url_fetch")):
            manual = str(patched.get("discovered_text") or "").strip()
            if manual:
                patched["manual_priority_text"] = manual
    has_manual = len(str(patched.get("manual_priority_text") or "").strip()) >= 120
    return _effective_source_quality(patched, has_manual)


def _apply_source_quality_guardrails(job: Dict[str, Any], out: Dict[str, Any]) -> None:
    """Only down-rank / strip skills for unverified sources (email_snapshot, not_found)."""
    sq = str(job.get("source_quality") or "email_snapshot").strip()
    snapshot_notice = "Only email snapshot available — analysis may be incomplete."
    email_banner = "Full job posting not found. This analysis is based only on the email."

    if sq in ("manual_paste", "full_posting", "partial_posting"):
        return

    if sq in ("email_snapshot", "not_found"):
        out["required_skills"] = []
        out["nice_to_have_skills"] = []
        out["tools_or_technologies"] = []
        out["tools_technologies"] = []
        out["skills"] = []
        out["recommendation"] = "review" if out.get("recommendation") == "apply" else out.get("recommendation")
        cap = 50 if sq == "not_found" else 55
        out["score"] = min(_safe_int_score(out.get("score"), 48), cap)
        sm = str(out.get("summary") or "").strip()
        if snapshot_notice not in sm[:220]:
            out["summary"] = f"{snapshot_notice}\n\n{sm}".strip() if sm else snapshot_notice
        wy = str(out.get("why_relevant") or "").strip()
        if email_banner not in wy[:260]:
            out["why_relevant"] = (email_banner + ("\n\n" + wy if wy and wy.lower() != "unknown" else "")).strip()
        extras = "| email_snapshot_guardrails" if sq == "email_snapshot" else "| not_found_guardrails"
        esu = str(out.get("enrichment_sources_used") or "").strip()
        if extras.strip("| ") not in esu:
            out["enrichment_sources_used"] = f"{esu} {extras}".strip()


def _truncate_block(label: str, text: Optional[str], max_chars: int) -> str:
    t = (text or "").strip()
    if not t:
        return f"{label}\n(No content supplied.)\n"
    clipped = t[:max_chars]
    return f"{label}\n---\n{clipped}\n"


def _category_blob(job: Dict[str, Any]) -> Dict[str, str]:
    return {
        "title": job.get("title"),
        "description": job.get("description"),
        "email_subject": job.get("email_subject"),
        "email_snippet": job.get("email_snippet"),
        "email_body_excerpt": job.get("email_body_excerpt"),
    }


def _combined_jd_text(manual: str, discovered_used: str, posting: str) -> str:
    return "\n\n".join(part.strip() for part in (manual, discovered_used, posting) if part and part.strip())


def _finalize_skill_tool_outputs(out: Dict[str, Any]) -> None:
    tools = out.get("tools_technologies")
    if not tools:
        tools = out.get("tools_or_technologies") or []
    out["tools_technologies"] = list(tools or [])
    out["tools_or_technologies"] = list(tools or [])
    out["skills"] = list(out.get("required_skills") or [])


def _maybe_keyword_fallback(
    effective_sq: str,
    combined_jd: str,
    out: Dict[str, Any],
) -> None:
    if effective_sq not in ("manual_paste", "full_posting", "partial_posting"):
        return
    if len(combined_jd.strip()) <= 300:
        return
    needs = (not out.get("required_skills")) and (not out.get("tools_technologies")) and (not out.get("tools_or_technologies"))
    if not needs:
        return
    fb = extract_skills_fallback(combined_jd)
    touched = False
    if fb.get("required_skills"):
        out["required_skills"] = fb["required_skills"]
        touched = True
    if fb.get("nice_to_have_skills"):
        out["nice_to_have_skills"] = fb["nice_to_have_skills"]
        touched = True
    if fb.get("tools_technologies"):
        out["tools_technologies"] = fb["tools_technologies"]
        out["tools_or_technologies"] = fb["tools_technologies"]
        touched = True
    if touched:
        out["skills"] = list(out.get("required_skills") or [])
        esu = str(out.get("enrichment_sources_used") or "").strip()
        suffix = "| keyword_fallback_v1"
        if "keyword_fallback_v1" not in esu:
            out["enrichment_sources_used"] = f"{esu} {suffix}".strip()


def _log_enrichment_snapshot(
    phase: str,
    *,
    job: Dict[str, Any],
    effective_sq: str,
    manual_len: int,
    disc_len: int,
    prompt_chars: int,
    parsed_keys: Optional[List[str]] = None,
    out: Optional[Dict[str, Any]] = None,
    before_guardrails: Optional[Dict[str, int]] = None,
    after_guardrails: Optional[Dict[str, int]] = None,
    final_lens: Optional[Dict[str, int]] = None,
) -> None:
    snapshot = (
        "[AI_ENRICH] phase=%s job_id=%s raw_sq=%s effective_sq=%s manual_len=%s discovered_len=%s prompt_chars=%s "
        "json_keys=%s before_guard=%s after_guard=%s final_counts=%s"
        % (
            phase,
            job.get("job_id"),
            str(job.get("source_quality")),
            effective_sq,
            manual_len,
            disc_len,
            prompt_chars,
            ",".join(parsed_keys or []),
            before_guardrails or {},
            after_guardrails or {},
            final_lens or {},
        )
    )
    _LOGGER.info(snapshot)


def _build_sources_line(job: Dict[str, Any], has_manual: bool, has_disc: bool, has_orig: bool) -> str:
    parts: List[str] = []
    if has_manual:
        parts.append("manual_priority_text")
    if has_disc:
        ds = job.get("discovered_source") or "discovered_public_page"
        parts.append(f"discovered[{ds}]")
    if has_orig:
        parts.append("original_job_link_fetch")
    parts.append("email_text")
    return " > ".join(parts)


class AIService:
    def __init__(self) -> None:
        self.enabled = bool(settings.openai_api_key)
        self.client = OpenAI(api_key=settings.openai_api_key) if self.enabled else None

    @staticmethod
    def _fallback(job: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "clean_title": str(job.get("title", "Unknown") or "Unknown"),
            "company": str(job.get("company", "Unknown") or "Unknown"),
            "location": str(job.get("location", "Unknown") or "Unknown"),
            "remote": None,
            "summary": f"{job.get('title', 'Job')} at {job.get('company', 'Unknown')}. OPENAI_API_KEY not configured; heuristic summary only.",
            "why_relevant": "Unknown",
            "required_skills": [],
            "nice_to_have_skills": [],
            "tools_or_technologies": [],
            "tools_technologies": [],
            "automation_ai_relevance": "Unknown",
            "recommendation": "review",
            "score": 50,
            "reasoning": "OPENAI_API_KEY not configured; fallback analysis used.",
            "job_category": heuristic_category(_category_blob(job)),
            "raw_response": {},
            "enrichment_sources_used": "no_openai",
        }

    def enrich_job(self, job: Dict[str, Any]) -> Dict[str, Any]:
        manual = (job.get("manual_priority_text") or "").strip()
        posting = (job.get("job_page_text") or "").strip()
        discovered = (job.get("discovered_text") or "").strip()

        if manual and discovered and manual.strip()[:200] == discovered.strip()[:200]:
            discovered_used = ""
        elif manual:
            discovered_used = discovered
        else:
            discovered_used = discovered

        has_manual = bool(manual)
        has_disc = bool(discovered_used.strip())
        has_orig = bool(posting.strip())
        sources_line = _build_sources_line(job, has_manual, has_disc, has_orig)
        combined_jd = _combined_jd_text(manual, discovered_used, posting)
        effective_sq = _effective_source_quality(job, has_manual)
        job_guard_ctx = dict(job)
        job_guard_ctx["source_quality"] = effective_sq

        manual_len = len(manual)
        disc_blob_len = len(discovered_used or "")

        if not self.enabled or not self.client:
            out = self._fallback(job)
            _finalize_skill_tool_outputs(out)
            out["job_category"] = normalize_ai_category(out.get("job_category"))
            _maybe_keyword_fallback(effective_sq, combined_jd, out)
            _finalize_skill_tool_outputs(out)
            _apply_source_quality_guardrails(job_guard_ctx, out)
            _finalize_skill_tool_outputs(out)
            _log_enrichment_snapshot(
                "disabled_client",
                job=job,
                effective_sq=effective_sq,
                manual_len=manual_len,
                disc_len=disc_blob_len,
                prompt_chars=0,
                parsed_keys=[],
                out=out,
                before_guardrails={"req": len(out["required_skills"]), "nice": len(out["nice_to_have_skills"]), "tools": len(out["tools_technologies"])},
                after_guardrails={"req": len(out["required_skills"]), "nice": len(out["nice_to_have_skills"]), "tools": len(out["tools_technologies"])},
                final_lens={
                    "req": len(out["required_skills"]),
                    "nice": len(out["nice_to_have_skills"]),
                    "tools": len(out["tools_technologies"]),
                },
            )
            return out

        source_quality = effective_sq

        email_block = _truncate_block(
            "SOURCE 1 — EMAIL (lowest factual priority)",
            "\n".join(
                [
                    f"subject: {job.get('email_subject', '')}",
                    f"snippet: {job.get('email_snippet', '')}",
                    f"body_excerpt:\n{job.get('email_body_excerpt', '')}",
                ]
            ),
            9000,
        )

        orig_block = (
            _truncate_block("SOURCE 2 — ORIGINAL JOB LINK FETCH (tracked link)", posting, 12000)
            if has_orig
            else "SOURCE 2 — ORIGINAL JOB LINK FETCH\n(No usable fetch text)\n"
        )

        discovered_block = (
            _truncate_block(
                "SOURCE 3 — DISCOVERED PUBLIC JOB PAGE (ATS / careers site)",
                discovered_used,
                14000,
            )
            if has_disc
            else "SOURCE 3 — DISCOVERED PUBLIC JOB PAGE\n(None)\n"
        )

        manual_block = (
            _truncate_block(
                "SOURCE 0 — USER PASTE (absolute highest reliability if present)",
                manual,
                120000,
            )
            if has_manual
            else ""
        )

        rule_parse = f"""
Rule/email parser hints (often partial or wrong — fix only when a higher source proves it):
Parsed title: {job.get('title', '')}
Parsed company: {job.get('company', '')}
Parsed location: {job.get('location', '')}
Parsed description: {job.get('description', '')}
Job link: {job.get('job_link', '')}
Email source label: {job.get('source', '')}
Discovery status: {job.get('discovery_status', '')} | reason: {(job.get('discovery_reason') or '')[:420]}
Public discovered URL hint: {job.get('discovered_url', '')}
Target heuristic: {job.get('target_fit', '')} ({job.get('target_score', '')})
""".strip()

        prompt = f"""
You analyze job postings for a candidate focused on: automation, AI agents/tools/workflows, no-code/low-code,
internal tools, business process automation, pipeline building, product operations, startup/technical product roles,
and roles where building tools or AI implementation is central.

AUTHORITATIVE PIPELINE SOURCE QUALITY (effective): {source_quality}
Meaning — obey strictly before taking creative license:
- manual_paste: SOURCE 0 is a full pasted JD. Extract concrete skills/tools only when stated there.
- full_posting: You have ATS/careers-class text via SOURCE 3/2. Extract concrete bullets only when clearly written there.
  Explain fit vs automation / AI / product / startup workflows with evidence from posting text.
- partial_posting: Text is thinner; prefer concrete facts but allow hedging (“not stated”).
- email_snapshot OR not_found: Gmail + snippets only. You MUST treat this as UNVERIFIED. Do NOT pretend the email is the full JD.
  required_skills, nice_to_have_skills, and tools_or_technologies MUST be EMPTY arrays unless a skill/tool name is verbatim in SOURCE 1.
  Recommendation should almost always be \"review\", not \"apply\". Score must stay conservative (prefer under 56).
  State clearly when key info is unknown.

Priority for facts/details (skills, responsibilities, tooling, employer name, office location): 
SOURCE 0 (manual paste if present), then SOURCE 3 discovered public page text, then SOURCE 2 original link fetch text,
finally SOURCE 1 email text — only verbatim facts, never guesses.

STRICT hallucination guard:
- NEVER invent bullets for required_skills, nice_to_have_skills, or tools_or_technologies unless wording appears in Sources 0–3.
  If postings do not enumerate skills explicitly, output EMPTY arrays AND explain succinctly inside reasoning/summary.

Be STRICT about employer facts. Unknown → string literal "Unknown" for textual fields.

Penalize customer support/manual labor-heavy roles unless Sources clearly show automation/AI/tool-building centrality.


{manual_block}

{discovered_block}

{orig_block}

{email_block}

RULE / PARSED HINTS
---
{rule_parse}
---

Planned enrichment path (for auditing): {sources_line}

Approved category labels for this user (pick exactly ONE for job_category): {', '.join(VALID_CATEGORIES)}

Return STRICT JSON only, preferring EXACTLY these canonical keys:
- clean_title (string; use Unknown if unclear)
- company (string; use Unknown if unclear)
- location (string; use Unknown if unclear)
- remote (true | false | null)  // null if not enough information
- summary (string; 2–4 lines)
- why_relevant (string; tie to automation/AI/tools/product ops or explain lack of fit)
- required_skills (array of strings)
- nice_to_have_skills (array of strings)
- tools_or_technologies (array of strings)
- automation_ai_relevance (string; one short paragraph)
- recommendation (one of: apply, review, skip)
- score (integer 0–100; align with recommendation and target focus)
- reasoning (string; concise, cite snippets / section labels you used)
- enrichment_sources_used (string; summarise which sources materially informed your output)
- job_category (string; MUST be exactly one of the Approved category labels)
Additional aliases you may ALSO use instead of arrays above must stay consistent:
- skills / must_have_skills for required bullets
- nice_to_have aliases
- technologies / tools for tooling lists alongside tools_or_technologies
Use arrays of short strings wherever possible — never prose inside arrays.
""".strip()

        prompt_len = len(prompt)

        try:
            response = self.client.chat.completions.create(
                model=settings.openai_model,
                response_format={"type": "json_object"},
                messages=[
                    {
                        "role": "system",
                        "content": "You produce only valid JSON for job analysis. Never hallucinate specifics.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.15,
            )
            parsed = json.loads(response.choices[0].message.content or "{}")

            parsed_keys_sorted = sorted(str(k) for k in parsed.keys())
            req_pre, nice_pre, tools_pre = _merge_parsed_skill_fields(parsed)
            skill_counts_before = {"req": len(req_pre), "nice": len(nice_pre), "tools": len(tools_pre)}
            _log_enrichment_snapshot(
                "parsed_model",
                job=job,
                effective_sq=effective_sq,
                manual_len=manual_len,
                disc_len=disc_blob_len,
                prompt_chars=prompt_len,
                parsed_keys=parsed_keys_sorted,
                before_guardrails=skill_counts_before,
                after_guardrails=None,
            )

            clean_title = str(parsed.get("clean_title", "Unknown") or "Unknown").strip()
            company = str(parsed.get("company", "Unknown") or "Unknown").strip()
            location = str(parsed.get("location", "Unknown") or "Unknown").strip()
            required_skills = list(req_pre)
            nice = list(nice_pre)
            tools = list(tools_pre)

            hinted_category = normalize_ai_category(parsed.get("job_category"))

            out = {
                "clean_title": clean_title,
                "company": company,
                "location": location,
                "remote": _coerce_remote(parsed.get("remote")),
                "summary": str(parsed.get("summary", "")).strip() or "Unknown",
                "why_relevant": str(parsed.get("why_relevant", "")).strip() or "Unknown",
                "required_skills": required_skills,
                "nice_to_have_skills": nice,
                "tools_technologies": tools,
                "tools_or_technologies": tools,
                "automation_ai_relevance": str(parsed.get("automation_ai_relevance", "")).strip() or "Unknown",
                "recommendation": str(parsed.get("recommendation", "review")).lower().strip(),
                "score": _safe_int_score(parsed.get("score"), 50),
                "reasoning": str(parsed.get("reasoning", "")).strip() or "Unknown",
                "job_category": hinted_category if hinted_category != "Other" else heuristic_category(_category_blob(job)),
                "raw_response": parsed,
                "enrichment_sources_used": str(parsed.get("enrichment_sources_used") or "").strip() or sources_line,
            }
            if out["recommendation"] not in ("apply", "review", "skip"):
                out["recommendation"] = "review"

            if out["job_category"] == "Other":
                fallback_cat = heuristic_category(_category_blob(job))
                out["job_category"] = fallback_cat

            _finalize_skill_tool_outputs(out)

            counts_after_parse = {"req": len(out["required_skills"]), "nice": len(out["nice_to_have_skills"]), "tools": len(out["tools_technologies"])}

            _maybe_keyword_fallback(effective_sq, combined_jd, out)
            _finalize_skill_tool_outputs(out)

            counts_before_guard = {
                "req": len(out["required_skills"]),
                "nice": len(out["nice_to_have_skills"]),
                "tools": len(out["tools_technologies"]),
            }

            _apply_source_quality_guardrails(job_guard_ctx, out)
            counts_after_guard = {"req": len(out["required_skills"]), "nice": len(out["nice_to_have_skills"]), "tools": len(out["tools_technologies"])}
            _finalize_skill_tool_outputs(out)

            _log_enrichment_snapshot(
                "post_guardrails",
                job=job,
                effective_sq=effective_sq,
                manual_len=manual_len,
                disc_len=disc_blob_len,
                prompt_chars=prompt_len,
                parsed_keys=parsed_keys_sorted,
                out=out,
                before_guardrails=counts_before_guard,
                after_guardrails=counts_after_guard,
                final_lens={
                    "req": len(out["required_skills"]),
                    "nice": len(out["nice_to_have_skills"]),
                    "tools": len(out["tools_technologies"]),
                },
            )
            _finalize_skill_tool_outputs(out)
            return out
        except Exception as exc:
            fallback = self._fallback(job)
            fallback["reasoning"] = f"AI fallback due to error: {exc}"
            fallback["job_category"] = normalize_ai_category(fallback.get("job_category"))
            fallback["enrichment_sources_used"] = sources_line + " | error_fallback"
            _finalize_skill_tool_outputs(fallback)
            _maybe_keyword_fallback(effective_sq, combined_jd, fallback)
            _finalize_skill_tool_outputs(fallback)
            _apply_source_quality_guardrails(job_guard_ctx, fallback)
            _finalize_skill_tool_outputs(fallback)
            _log_enrichment_snapshot(
                "exception_fallback",
                job=job,
                effective_sq=effective_sq,
                manual_len=manual_len,
                disc_len=disc_blob_len,
                prompt_chars=prompt_len,
                parsed_keys=[],
                out=fallback,
                before_guardrails={"req": len(fallback["required_skills"]), "nice": len(fallback["nice_to_have_skills"]), "tools": len(fallback["tools_technologies"])},
                after_guardrails={"req": len(fallback["required_skills"]), "nice": len(fallback["nice_to_have_skills"]), "tools": len(fallback["tools_technologies"])},
                final_lens={
                    "req": len(fallback["required_skills"]),
                    "nice": len(fallback["nice_to_have_skills"]),
                    "tools": len(fallback["tools_technologies"]),
                },
            )
            _finalize_skill_tool_outputs(fallback)
            return fallback

    def extract_job_when_missing(self, email_text: str) -> Dict[str, Any]:
        if not self.enabled or not self.client:
            return {}
        prompt = f"Extract title, company, location, job_link, description as strict JSON from this email text:\n{email_text[:5000]}"
        try:
            response = self.client.chat.completions.create(
                model=settings.openai_model,
                response_format={"type": "json_object"},
                messages=[
                    {"role": "system", "content": "You extract job fields from text."},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )
            parsed = json.loads(response.choices[0].message.content or "{}")
            return {
                "title": str(parsed.get("title", "")).strip(),
                "company": str(parsed.get("company", "")).strip() or "Unknown Company",
                "location": str(parsed.get("location", "")).strip(),
                "job_link": str(parsed.get("job_link", "")).strip(),
                "description": str(parsed.get("description", "")).strip(),
            }
        except Exception:
            return {}
