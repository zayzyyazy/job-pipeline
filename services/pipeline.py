from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import json
import logging
import re

from database.db import (
    bundle_job_for_discovery,
    get_ai_analysis_row,
    get_ai_clean_title,
    get_email_by_id,
    get_job_plain,
    insert_email_if_new,
    insert_pipeline_rejection,
    list_job_ids_ordered,
    update_job_category_if_unlocked,
    update_job_discovery,
    upsert_ai_analysis,
    upsert_job,
)
from services.ai_service import AIService
from services.category_helper import heuristic_category, normalize_ai_category
from services.filtering import evaluate_email_filter
from services.gmail_service import GmailService
from services.job_discovery import content_looks_meaningful, discover_job_details
from services.job_page_fetcher import fetch_job_posting_text
from services.source_quality import infer_source_quality, is_manual_quality
from services.job_quality import prepare_parsed_job_for_pipeline
from services.location_fit import evaluate_location_fit
from services.parser import parse_job_from_email
from services.target_fit import evaluate_target_fit, passes_target_gate

_LOGGER = logging.getLogger(__name__)


@dataclass
class SyncResult:
    fetched: int = 0
    processed: int = 0
    ignored: int = 0
    jobs_created_or_updated: int = 0
    ai_enriched: int = 0
    passed_filter: int = 0
    parsing_failures: int = 0
    rejected_target_fit: int = 0
    rejected_title_quality: int = 0
    job_pages_fetched_ok: int = 0
    job_pages_fetch_failed: int = 0
    discovery_found: int = 0
    discovery_original_only: int = 0
    discovery_email_only: int = 0
    discovery_failed: int = 0
    discovery_search_attempted: int = 0
    ignored_reasons: Dict[str, int] = field(default_factory=dict)
    target_rejection_reasons: Dict[str, int] = field(default_factory=dict)


def _skills_count_from_db(raw_skills: Any) -> int:
    if not raw_skills:
        return 0
    if isinstance(raw_skills, list):
        return len([x for x in raw_skills if str(x).strip()])
    try:
        data = json.loads(raw_skills)
        if isinstance(data, list):
            return len([x for x in data if str(x).strip()])
    except json.JSONDecodeError:
        pass
    return 1 if str(raw_skills).strip() else 0


def _pick_richer_text(candidate: Optional[str], previous: Optional[str]) -> Optional[str]:
    cand = str(candidate or "").strip()
    prev = str(previous or "").strip()
    if cand and cand.lower() not in {"", "unknown", "n/a", "none"}:
        return cand
    return prev if prev else cand


class JobPipeline:
    def __init__(self) -> None:
        self.gmail = GmailService()
        self.ai = AIService()

    @staticmethod
    def _merge_discovery_keep_manual_prior(
        jr: Dict[str, Any], discovery_out: Dict[str, Any]
    ) -> Dict[str, Any]:
        """If the job already relied on manual paste, do not wipe it when research finds nothing stronger."""
        if not (is_manual_quality(jr) or str(jr.get("source_quality") or "") == "manual_paste"):
            return discovery_out
        new_txt = str(discovery_out.get("discovered_text") or "").strip()
        new_ok = bool(
            discovery_out.get("discovery_status") == "found"
            and new_txt
            and content_looks_meaningful(new_txt)
        )
        if new_ok:
            return discovery_out
        merged = dict(discovery_out)
        merged["discovered_url"] = jr.get("discovered_url")
        merged["discovered_text"] = jr.get("discovered_text")
        merged["discovered_source"] = jr.get("discovered_source")
        merged["discovery_status"] = jr.get("discovery_status")
        merged["discovery_reason"] = (
            str(discovery_out.get("discovery_reason") or "").strip()
            + " | kept_manual_paste_after_research"
        )[-780:]
        return merged

    @staticmethod
    def _safe_trim(text: str, limit: int = 120) -> str:
        compact = (text or "").replace("\n", " ").strip()
        return compact if len(compact) <= limit else f"{compact[:limit]}..."

    @staticmethod
    def _bucket_reason(reason: str, max_len: int = 140) -> str:
        r = (reason or "").strip()
        return r if len(r) <= max_len else r[:max_len] + "…"

    def fetch_preview(self, max_results: int = 10) -> List[Dict[str, str]]:
        emails = self.gmail.fetch_recent_messages(max_results=max_results)
        preview: List[Dict[str, str]] = []
        for email in emails[:10]:
            preview.append(
                {
                    "sender": self._safe_trim(email.get("sender", "")),
                    "subject": self._safe_trim(email.get("subject", "")),
                    "date": email.get("received_at", ""),
                }
            )
        return preview

    def _apply_discovery_counters(self, result: SyncResult, status: Optional[str]) -> None:
        if status == "found":
            result.discovery_found += 1
        elif status == "original_only":
            result.discovery_original_only += 1
        elif status == "email_only":
            result.discovery_email_only += 1
        elif status == "failed":
            result.discovery_failed += 1

    def _persist_ai_category_if_unlocked(self, job_id: int, ai_data: Dict[str, Any]) -> None:
        hinted = normalize_ai_category(ai_data.get("job_category"))
        update_job_category_if_unlocked(job_id, hinted)

    def _category_hints(self, work: Dict[str, Any]) -> str:
        return heuristic_category(
            {
                "title": work.get("title"),
                "description": work.get("description"),
                "email_subject": work.get("email_subject"),
                "email_snippet": work.get("email_snippet"),
                "email_body_excerpt": work.get("email_body_excerpt"),
            }
        )

    def reprocess_job(self, job_id: int, result: Optional[SyncResult] = None) -> bool:
        """Re-run discovery + AI for an existing persisted job."""
        jr = get_job_plain(job_id)
        if not jr or jr.get("deleted_at"):
            return False
        jr = dict(jr)
        em = get_email_by_id(jr["email_id"])
        extras = {"clean_title_ai": get_ai_clean_title(job_id) or ""}
        work = bundle_job_for_discovery(jr, em, extras)

        email_for_fit: Dict[str, Any] = {}
        if em:
            emd = dict(em)
            email_for_fit = {
                "subject": emd.get("subject", ""),
                "snippet": emd.get("snippet", ""),
                "body": emd.get("body", ""),
            }

        parsed_like = {
            "title": jr["title"],
            "company": jr["company"],
            "location": jr["location"],
            "description": jr["description"] or "",
            "job_link": jr["job_link"],
            "source": jr["source"],
        }
        fit_result = evaluate_target_fit(parsed_like, email_for_fit)
        work["target_fit"] = fit_result["target_fit"]
        work["target_score"] = fit_result["target_score"]
        work["target_matched_keywords"] = fit_result["matched_keywords"]
        loc = evaluate_location_fit(work)
        work.update(loc)

        rr = result or SyncResult()
        print(f"[REPROCESS] job_id={job_id} title={self._safe_trim(work.get('title', ''))}")

        disc = discover_job_details(work, log_prefix="[DISCOVERY][RE]")
        work["job_page_text"] = disc.get("job_page_text")
        work["discovered_url"] = disc.get("discovered_url")
        work["discovered_text"] = disc.get("discovered_text")
        work["discovered_source"] = disc.get("discovered_source")
        work["discovery_status"] = disc.get("discovery_status")
        work["discovery_reason"] = disc.get("discovery_reason")
        self._apply_discovery_counters(rr, work.get("discovery_status"))
        if disc.get("search_attempted"):
            rr.discovery_search_attempted += 1
        if disc.get("direct_fetch_success"):
            rr.job_pages_fetched_ok += 1

        merged = dict(jr)
        merged.update(work)
        merged["source_quality"] = infer_source_quality(merged)
        work["source_quality"] = merged["source_quality"]
        upsert_job(merged)

        ai_data = self.ai.enrich_job(work)
        upsert_ai_analysis(job_id, ai_data)
        self._persist_ai_category_if_unlocked(job_id, ai_data)
        if result is None:
            print(
                "[REPROCESS][DONE] discovery_status=%s discovered_url=%s"
                % (work.get("discovery_status"), self._safe_trim(str(work.get("discovered_url") or ""), 100))
            )
        return True

    def research_job(self, job_id: int) -> Tuple[bool, str]:
        """
        User-triggered deep discovery (DuckDuckGo + employer/ATS bias), then AI re-run.
        Returns (ok, user_message) for flash display.
        """
        jr = get_job_plain(job_id)
        if not jr or jr.get("deleted_at"):
            return False, "Job unavailable or archived."
        jr = dict(jr)
        em = get_email_by_id(jr["email_id"])
        extras = {"clean_title_ai": get_ai_clean_title(job_id) or ""}
        work = bundle_job_for_discovery(jr, em, extras)

        email_for_fit: Dict[str, Any] = {}
        if em:
            emd = dict(em)
            email_for_fit = {
                "subject": emd.get("subject", ""),
                "snippet": emd.get("snippet", ""),
                "body": emd.get("body", ""),
            }

        try:
            disc = discover_job_details(work, log_prefix="[DISCOVERY][RESEARCH]", force_research=True)
            disc = JobPipeline._merge_discovery_keep_manual_prior(jr, disc)
            work["job_page_text"] = disc.get("job_page_text")
            work["discovered_url"] = disc.get("discovered_url")
            work["discovered_text"] = disc.get("discovered_text")
            work["discovered_source"] = disc.get("discovered_source")
            work["discovery_status"] = disc.get("discovery_status")
            work["discovery_reason"] = disc.get("discovery_reason")

            merged = dict(jr)
            merged.update(work)
            merged["source_quality"] = infer_source_quality(merged)
            work["source_quality"] = merged["source_quality"]

            fit_result = evaluate_target_fit(
                {
                    "title": merged["title"],
                    "company": merged["company"],
                    "location": merged["location"],
                    "description": str(merged.get("description") or "")
                    + "\n\n"
                    + str(merged.get("discovered_text") or ""),
                    "job_link": merged["job_link"],
                    "source": merged["source"],
                },
                email_for_fit,
            )
            work["target_fit"] = fit_result["target_fit"]
            work["target_score"] = fit_result["target_score"]
            work["target_matched_keywords"] = fit_result["matched_keywords"]
            loc = evaluate_location_fit(work)
            work.update(loc)
            merged.update(work)
            merged["source_quality"] = infer_source_quality(merged)
            work["source_quality"] = merged["source_quality"]

            upsert_job(merged)

            ai_data = self.ai.enrich_job(work)
            upsert_ai_analysis(job_id, ai_data)
            self._persist_ai_category_if_unlocked(job_id, ai_data)

            sq = str(merged.get("source_quality") or "")
            if sq in ("full_posting", "partial_posting"):
                return True, "Found employer posting and updated analysis."
            if sq == "manual_paste":
                return True, "Kept your pasted posting as the primary source."
            return True, "Could not find full posting. Using email snapshot only."
        except Exception as exc:
            _LOGGER.exception("research_job failed job_id=%s", job_id)
            return False, f"Research failed ({type(exc).__name__}). Existing data was not changed."

    def force_ai_refresh(self, job_id: int) -> Tuple[bool, str]:
        """Re-run AI enrichment against current persisted job/email row (does not redo discovery search)."""
        jr = get_job_plain(job_id)
        if not jr or jr.get("deleted_at"):
            return False, "Job unavailable or archived."
        jr = dict(jr)
        em = get_email_by_id(jr["email_id"])
        extras = {"clean_title_ai": get_ai_clean_title(job_id) or ""}
        work = bundle_job_for_discovery(jr, em, extras)
        ds_l = str(jr.get("discovered_source") or "").lower()
        if any(tag in ds_l for tag in ("manual_paste", "manual_form", "user_url_fetch")):
            blob = (jr.get("discovered_text") or "").strip()
            if blob:
                work["manual_priority_text"] = blob[:120000]
        work["source_quality"] = jr.get("source_quality") or infer_source_quality(jr)
        prev_ai = get_ai_analysis_row(job_id)
        try:
            _LOGGER.info(
                "[FORCE_AI_REFRESH] job_id=%s sq=%s manual_priority_len=%s discovered_len=%s",
                job_id,
                work.get("source_quality"),
                len(work.get("manual_priority_text") or ""),
                len(jr.get("discovered_text") or ""),
            )
            ai_data = self.ai.enrich_job(work)
            self._fuse_ai_identity(ai_data, prev_ai)
            upsert_ai_analysis(job_id, ai_data)
            self._persist_ai_category_if_unlocked(job_id, ai_data)
            return True, ""
        except Exception as exc:  # pragma: no cover
            _LOGGER.exception("force_ai_refresh failed job_id=%s", job_id)
            return False, str(exc)

    def reprocess_jobs(self, limit: int = 100) -> SyncResult:
        result = SyncResult()
        ids = list_job_ids_ordered(limit=limit)
        print(f"[REPROCESS] batch count={len(ids)}")
        for jid in ids:
            self.reprocess_job(jid, result=result)
        print(
            "[REPROCESS][SUMMARY] found=%s original_only=%s email_only=%s failed=%s search_attempted=%s"
            % (
                result.discovery_found,
                result.discovery_original_only,
                result.discovery_email_only,
                result.discovery_failed,
                result.discovery_search_attempted,
            )
        )
        return result

    @staticmethod
    def _normalize_posting_url(url: Optional[str]) -> Optional[str]:
        if not url:
            return None
        candidate = url.strip()
        if not candidate:
            return None
        if candidate.startswith("//"):
            candidate = "https:" + candidate
        if not re.match(r"^https?://", candidate, flags=re.I):
            candidate = "https://" + candidate
        parsed = urlparse(candidate)
        return candidate if parsed.scheme in ("http", "https") and parsed.netloc else None

    @staticmethod
    def _fuse_ai_identity(ai_data: Dict[str, Any], prev_row: Optional[Any]) -> None:
        if not ai_data:
            return
        prev_raw: Dict[str, Any] = {}
        if prev_row and prev_row["raw_response"]:
            try:
                prev_raw = json.loads(prev_row["raw_response"]) or {}
            except json.JSONDecodeError:
                prev_raw = {}

        new_raw = dict(ai_data.get("raw_response") or {})
        merged = dict(prev_raw)
        merged.update(new_raw)
        for field in ("company", "location", "clean_title"):
            merged[field] = _pick_richer_text(new_raw.get(field), prev_raw.get(field))
        ai_data["raw_response"] = merged

        prev_clean_title = ""
        if prev_row and prev_row["clean_title"]:
            prev_clean_title = str(prev_row["clean_title"]).strip()
        ai_data["clean_title"] = _pick_richer_text(ai_data.get("clean_title"), prev_clean_title)

    def improve_job_posting(
        self,
        job_id: int,
        posting_url: Optional[str],
        pasted_description: Optional[str],
    ) -> Tuple[bool, Optional[str]]:
        """
        Highest-priority manual improvement path combining optional URL fetch + textarea paste.
        Recomputes target/location scoring from the pasted/fetched corpus, refreshes all AI_analysis fields,
        then optionally locks category respecting category_locked semantics handled downstream.
        """
        url_in = posting_url.strip() if posting_url else None
        desc_text = pasted_description.strip() if pasted_description else None
        jr = get_job_plain(job_id)
        if not jr or jr.get("deleted_at"):
            return False, "Job unavailable or archived."
        jr = dict(jr)

        prev_ai = get_ai_analysis_row(job_id)
        prev_skill_count = _skills_count_from_db(prev_ai["skills"] if prev_ai else None)

        fetch_text = None
        fetch_error = None
        normalized_url = self._normalize_posting_url(url_in)
        if url_in:
            if not normalized_url:
                fetch_error = "Job URL looked invalid."
            elif not normalized_url.startswith("http"):
                fetch_error = "Job URL needs http/https."
            else:
                fetch_text, fetch_error = fetch_job_posting_text(normalized_url)

        blocks: List[str] = []
        if desc_text:
            blocks.append(desc_text.strip())
        if fetch_text:
            stripped_fetch = fetch_text.strip()
            if stripped_fetch:
                blocks.append(("--- Employer page fetched from pasted URL ---\n" + stripped_fetch).strip())

        combined = "\n\n".join([b for b in blocks if b]).strip()

        min_len = 80
        if len(combined) < min_len:
            if normalized_url and not fetch_text:
                hint = fetch_error or "download failed"
                return False, f"Could not retrieve text from URL ({hint}). Paste the description too or fix the URL."
            if normalized_url or desc_text:
                return False, "Need at least ~80 characters overall after combining URL fetch + pasted text."

        jr = get_job_plain(job_id)
        if not jr:
            return False, "Could not reload job."
        jr = dict(jr)

        reason_bits: List[str] = []
        discovery_source_parts: List[str] = []
        if desc_text:
            discovery_source_parts.append("manual_paste")
            reason_bits.append("User pasted JD text")
        if fetch_text:
            discovery_source_parts.append("user_url_fetch")
            reason_bits.append("Fetched ATS page from supplied URL")

        discovery_source = "+".join(dict.fromkeys(discovery_source_parts)) or "manual_form"
        discovery_reason_summary = "; ".join(reason_bits)[:500]

        discovered_url_field = normalized_url if fetch_text else None
        discovery_fields = {
            "discovered_text": combined[:200000],
            "discovered_source": discovery_source,
            "discovered_url": discovered_url_field,
            "discovery_status": "found",
            "discovery_reason": discovery_reason_summary or "Manual posting improvement",
            "source_quality": "manual_paste",
        }
        update_job_discovery(job_id, discovery_fields)

        jr = get_job_plain(job_id)
        if not jr:
            return False, "Job missing after persistence."
        jr = dict(jr)

        em = get_email_by_id(jr["email_id"])
        extras = {"clean_title_ai": get_ai_clean_title(job_id) or ""}
        work = bundle_job_for_discovery(jr, em, extras)
        work["manual_priority_text"] = combined[:120000]

        email_for_fit: Dict[str, Any] = {}
        if em:
            emd = dict(em)
            email_for_fit = {
                "subject": emd.get("subject", ""),
                "snippet": emd.get("snippet", ""),
                "body": emd.get("body", ""),
            }

        corp_for_fit = "\n".join(
            filter(
                None,
                [
                    str(jr["description"] or ""),
                    combined,
                ],
            )
        )[:22000]

        parsed_like = {
            "title": jr["title"],
            "company": jr["company"],
            "location": jr["location"],
            "description": corp_for_fit,
            "job_link": jr["job_link"],
            "source": jr["source"],
        }
        fit_result = evaluate_target_fit(parsed_like, email_for_fit)
        work["target_fit"] = fit_result["target_fit"]
        work["target_score"] = fit_result["target_score"]
        work["target_matched_keywords"] = fit_result["matched_keywords"]

        loc_refresh = evaluate_location_fit(work)
        work.update(loc_refresh)

        merged = dict(jr)
        merged.update(work)
        merged["source_quality"] = infer_source_quality(merged)
        work["source_quality"] = merged["source_quality"]
        upsert_job(merged)

        print(
            f"[MANUAL_IMPROVE] job_id={job_id} desc_len={len(desc_text or '')} "
            f"fetch_len={len(fetch_text or '')} combined_len={len(combined)} "
            f"category_locked={int(jr.get('category_locked') or 0)}"
        )

        ai_ok = True
        ai_error = None
        try:
            ai_data = self.ai.enrich_job(work)
            _LOGGER.info(
                "[MANUAL_IMPROVE][AI_DEBUG_PRE_UPSERT] job_id=%s persisted_sq=%s manual_priority_len=%s "
                "discovered_text_len=%s combined_desc_len=%s req=%s nice=%s tools=%s raw_ai_keys_sample=%s",
                job_id,
                work.get("source_quality"),
                len(work.get("manual_priority_text") or ""),
                len(jr.get("discovered_text") or ""),
                len(combined),
                len(ai_data.get("required_skills") or []) if isinstance(ai_data.get("required_skills"), list) else 0,
                len(ai_data.get("nice_to_have_skills") or []) if isinstance(ai_data.get("nice_to_have_skills"), list) else 0,
                len(ai_data.get("tools_technologies") or []) if isinstance(ai_data.get("tools_technologies"), list) else 0,
                sorted(list((ai_data.get("raw_response") or {}).keys())[:24]),
            )
            self._fuse_ai_identity(ai_data, prev_ai)
            upsert_ai_analysis(job_id, ai_data)
            self._persist_ai_category_if_unlocked(job_id, ai_data)
            new_skill_count = _skills_count_from_db(ai_data.get("skills"))
            print(
                "[MANUAL_IMPROVE][AI_OK] job_id=%s fields=%s skills_old=%s skills_new=%s reco=%s score=%s"
                % (
                    job_id,
                    "summary,why_relevant,automation_ai_relevance,skills,nice,tools,reco,score,reasoning,clean_title",
                    prev_skill_count,
                    new_skill_count,
                    ai_data.get("recommendation"),
                    ai_data.get("score"),
                )
            )
        except Exception as exc:  # pragma: no cover - defensive
            ai_ok = False
            ai_error = str(exc)
            print(f"[MANUAL_IMPROVE][AI_FAIL] job_id={job_id} error={exc}")

        if not ai_ok:
            return False, f"AI enrichment failed after storing posting text: {ai_error}"

        return True, None

    def manual_enrich(self, job_id: int, pasted_text: str) -> bool:
        """Backward-compatible helper for legacy callers — URL-less paste only."""
        ok, _ = self.improve_job_posting(job_id, None, pasted_text)
        return ok

    def sync_emails(self, max_results: int = 50) -> SyncResult:
        result = SyncResult()
        emails = self.gmail.fetch_recent_messages(max_results=max_results)
        result.fetched = len(emails)

        print(f"[SYNC] fetched={result.fetched}")
        print("[SYNC] first fetched email previews (up to 10):")
        for idx, email in enumerate(emails[:10], start=1):
            print(
                f"[SYNC][PREVIEW {idx}] sender={self._safe_trim(email.get('sender', ''))} | "
                f"subject={self._safe_trim(email.get('subject', ''))} | "
                f"date={email.get('received_at', '(missing)')}"
            )

        for idx, email in enumerate(emails, start=1):
            eval_result = evaluate_email_filter(email)
            passed = bool(eval_result["passed"])
            source = eval_result["source"]
            reason = str(eval_result["reason"])

            print(
                f"[SYNC][EMAIL {idx}] sender={self._safe_trim(email.get('sender', ''))} | "
                f"subject={self._safe_trim(email.get('subject', ''))} | "
                f"date={email.get('received_at', '(missing)')} | "
                f"passed_filter={passed} | reason={reason}"
            )

            if not passed:
                result.ignored += 1
                result.ignored_reasons[reason] = result.ignored_reasons.get(reason, 0) + 1
                continue

            result.passed_filter += 1
            email["source"] = source

            email_id = insert_email_if_new(email)
            if email_id is None:
                duplicate_reason = "duplicate email already stored"
                result.ignored_reasons[duplicate_reason] = result.ignored_reasons.get(duplicate_reason, 0) + 1
                continue

            result.processed += 1
            parsed = parse_job_from_email(email)
            if not parsed:
                guess = self.ai.extract_job_when_missing(
                    "\n".join([email.get("subject", ""), email.get("snippet", ""), email.get("body", "")])
                )
                parsed = guess if guess.get("title") else None

            if not parsed:
                result.ignored += 1
                result.parsing_failures += 1
                parse_reason = "parsing failed (rule + AI fallback)"
                result.ignored_reasons[parse_reason] = result.ignored_reasons.get(parse_reason, 0) + 1
                print(
                    f"[SYNC][PARSE_FAIL] sender={self._safe_trim(email.get('sender', ''))} | "
                    f"subject={self._safe_trim(email.get('subject', ''))}"
                )
                continue

            parsed, title_reject = prepare_parsed_job_for_pipeline(parsed)
            if title_reject:
                result.ignored += 1
                result.rejected_title_quality += 1
                tr = f"title_quality:{title_reject}"
                result.ignored_reasons[tr] = result.ignored_reasons.get(tr, 0) + 1
                print(f"[SYNC][TITLE_REJECT] {title_reject} | raw={parsed.get('title')!r}")
                insert_pipeline_rejection(
                    email_id=email_id,
                    title=parsed.get("title") or "",
                    company=parsed.get("company") or "",
                    job_link=parsed.get("job_link") or "",
                    stage="title_quality",
                    reject_reason=str(title_reject),
                    target_fit="n/a",
                    target_score=0,
                    detail={"parsed": parsed},
                )
                continue

            fit_result = evaluate_target_fit(parsed, email)
            if not passes_target_gate(fit_result):
                result.ignored += 1
                result.rejected_target_fit += 1
                rkey = self._bucket_reason(fit_result.get("reject_reason") or f"fit={fit_result['target_fit']}")
                result.target_rejection_reasons[rkey] = result.target_rejection_reasons.get(rkey, 0) + 1
                gate_reason = f"target_fit:{fit_result['target_fit']}"
                result.ignored_reasons[gate_reason] = result.ignored_reasons.get(gate_reason, 0) + 1
                insert_pipeline_rejection(
                    email_id=email_id,
                    title=parsed.get("title") or "",
                    company=parsed.get("company") or "",
                    job_link=parsed.get("job_link") or "",
                    stage="target_fit",
                    reject_reason=str(fit_result.get("reject_reason") or ""),
                    target_fit=str(fit_result["target_fit"]),
                    target_score=int(fit_result["target_score"]),
                    detail={"matched_keywords": fit_result.get("matched_keywords", []), "parsed": parsed},
                )
                continue

            parsed.update(
                {
                    "email_id": email_id,
                    "source": source,
                    "target_fit": fit_result["target_fit"],
                    "target_score": fit_result["target_score"],
                    "target_matched_keywords": fit_result["matched_keywords"],
                    "email_subject": email.get("subject", ""),
                    "email_snippet": email.get("snippet", ""),
                    "email_body_excerpt": (email.get("body") or "")[:6000],
                    "clean_title_ai": "",
                    "quality_flag": None,
                }
            )

            loc_bundle = evaluate_location_fit(parsed)
            parsed.update(loc_bundle)
            parsed["category"] = self._category_hints(parsed)
            parsed["category_locked"] = 0

            disc = discover_job_details(parsed)
            parsed["job_page_text"] = disc.get("job_page_text")
            parsed["discovered_url"] = disc.get("discovered_url")
            parsed["discovered_text"] = disc.get("discovered_text")
            parsed["discovered_source"] = disc.get("discovered_source")
            parsed["discovery_status"] = disc.get("discovery_status")
            parsed["discovery_reason"] = disc.get("discovery_reason")
            parsed["source_quality"] = infer_source_quality(parsed)
            self._apply_discovery_counters(result, parsed.get("discovery_status"))
            if disc.get("search_attempted"):
                result.discovery_search_attempted += 1
            if disc.get("direct_fetch_success"):
                result.job_pages_fetched_ok += 1

            job_id = upsert_job(parsed)
            result.jobs_created_or_updated += 1

            ai_data = self.ai.enrich_job(parsed)
            upsert_ai_analysis(job_id, ai_data)
            self._persist_ai_category_if_unlocked(job_id, ai_data)
            result.ai_enriched += 1

        print(
            "[SYNC][SUMMARY] "
            f"fetched={result.fetched}, passed_email_filter={result.passed_filter}, "
            f"rejected_target_fit={result.rejected_target_fit}, rejected_title_quality={result.rejected_title_quality}, "
            f"parsed_processed={result.processed}, saved_jobs={result.jobs_created_or_updated}, ai_enriched={result.ai_enriched}, "
            f"ignored_total={result.ignored}, parsing_failures={result.parsing_failures}, "
            f"discovery(found/o/email/failed)={result.discovery_found}/{result.discovery_original_only}/"
            f"{result.discovery_email_only}/{result.discovery_failed}, search_attempted={result.discovery_search_attempted}"
        )

        return result
