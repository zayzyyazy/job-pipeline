import json
import logging
import os
from typing import Any, Dict, List, Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
from urllib.parse import urlparse

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

from config import settings
from database.db import (
    VALID_STATUSES,
    cleanup_low_quality_jobs,
    get_job,
    get_status_history,
    init_db,
    list_jobs,
    restore_job,
    soft_delete_job,
    toggle_job_pin,
    update_job_user_category,
    update_job_status,
)
from services.ai_service import resolve_effective_source_quality
from services.ai_skills_fallback import extract_skills_fallback
from services.category_helper import VALID_CATEGORIES
from services.pipeline import JobPipeline
from services.source_quality import badge_class, label_for_quality

DISCOVERY_OPTIONS = ["found", "original_only", "email_only", "failed"]
TARGET_FIT_FILTERS = ["strong", "medium", "weak", "reject"]
LOCATION_FIT_FILTERS = ["nrw", "remote_germany", "unclear", "outside_target"]

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


def _remote_label(remote_val: Optional[int]) -> str:
    if remote_val == 1:
        return "Remote (likely)"
    if remote_val == 0:
        return "Not remote"
    return "Unknown"


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

    @app.route("/")
    def dashboard():
        status_filter = request.args.get("status", "")
        source_filter = request.args.get("source", "")
        target_fit_filter = request.args.get("target_fit", "")
        discovery_filter = request.args.get("discovery_status", "")
        location_fit_filter = request.args.get("location_fit", "")
        category_filter = request.args.get("category", "")
        search_q = request.args.get("q", "").strip()
        pinned_only = request.args.get("pinned_only", "") == "1"
        show_deleted = request.args.get("show_deleted", "") == "1"
        relaxed_view = request.args.get("relax", "") == "1"

        has_custom_filters = any(
            [
                bool(status_filter),
                bool(source_filter),
                bool(target_fit_filter),
                bool(discovery_filter),
                bool(location_fit_filter),
                bool(category_filter),
                bool(search_q),
                show_deleted,
            ]
        )
        strict_focus = (not relaxed_view) and (not has_custom_filters)

        rows = list_jobs(
            status=status_filter or None,
            source=source_filter or None,
            target_fit=target_fit_filter or None,
            discovery_status=discovery_filter or None,
            location_fit=location_fit_filter or None,
            category=category_filter or None,
            search=search_q or None,
            pinned_only=pinned_only,
            show_deleted=show_deleted,
            strict_focus=strict_focus,
        )
        jobs: List[Dict[str, Any]] = []
        for row in rows:
            d = dict(row)
            d["skills_preview"] = _skills_preview(d.get("skills"))
            d["tgt_score_display"] = d.get("target_score") if d.get("target_score") is not None else "—"
            d["tgt_fit_display"] = d.get("target_fit") or "—"
            reco = (d.get("recommendation") or "—").lower()
            d["reco_display"] = (d.get("recommendation") or "—") + (
                f" ({d.get('score')})" if d.get("score") is not None else ""
            )
            sq = str(d.get("source_quality") or "email_snapshot").strip()
            d["source_quality"] = sq
            d["sq_label"] = label_for_quality(sq)
            d["sq_class"] = badge_class(sq)
            d["card_tone"] = _card_tone(d.get("target_fit"), d.get("recommendation"))
            d["location_badge"] = _location_badge_cls(d.get("location_fit"))
            loc_label = str(d.get("location_fit") or "unclear").replace("_", " ").title()
            d["loc_fit_short"] = loc_label[:18]
            d["pinned_flag"] = bool(int(d.get("pinned") or 0))
            d["category_display"] = d.get("category") or "Other"
            d["title_clamp"] = (d.get("title") or "Untitled")[:160]
            d["reco_class"] = reco if reco in {"apply", "review", "skip"} else "review"
            jobs.append(d)

        return render_template(
            "dashboard.html",
            jobs=jobs,
            statuses=sorted(list(VALID_STATUSES)),
            sources=["Indeed", "LinkedIn", "Other Job Source"],
            categories=VALID_CATEGORIES,
            status_filter=status_filter,
            source_filter=source_filter,
            target_fit_filter=target_fit_filter,
            discovery_filter=discovery_filter,
            location_fit_filter=location_fit_filter,
            category_filter=category_filter,
            search_q=search_q,
            pinned_only=pinned_only,
            show_deleted=show_deleted,
            relaxed_view=relaxed_view,
            strict_focus=strict_focus,
            target_fit_choices=TARGET_FIT_FILTERS,
            discovery_choices=DISCOVERY_OPTIONS,
            location_fit_choices=LOCATION_FIT_FILTERS,
            flask_port=DEFAULT_PORT,
        )

    @app.route("/sync", methods=["POST"])
    def sync_emails():
        pipeline = JobPipeline()
        try:
            result = pipeline.sync_emails(max_results=50)
            flash(
                (
                    f"Sync complete: fetched={result.fetched}, passed_email_filter={result.passed_filter}, "
                    f"rejected_target_fit={result.rejected_target_fit}, rejected_title={result.rejected_title_quality}, "
                    f"saved_jobs={result.jobs_created_or_updated}, ai_enriched={result.ai_enriched}, "
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
        job["ai_debug_ui_fallback"] = False
        if good_skill_sources and skills_empty and len(desc_blob) > 300:
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
        job["ai_debug_fallback_used"] = bool(ai_keyword_fallback or job["ai_debug_ui_fallback"])
        job["ai_debug_req_count"] = len(job["required_skills_list"])
        job["ai_debug_nice_count"] = len(job["nice_to_have_list"])
        job["ai_debug_tools_count"] = len(job["tools_list"])
        job["skills_debug_hint"] = bool(good_skill_sources and skills_empty_final)
        job["display_title"] = (job.get("clean_title") and job["clean_title"] != "Unknown" and job.get("clean_title")) or job.get(
            "title"
        )
        job["remote_label"] = _remote_label(job.get("remote"))

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

    @app.route("/admin/cleanup-low-quality", methods=["POST"])
    def admin_cleanup():
        stats = cleanup_low_quality_jobs()
        flash(
            (
                "Cleanup run — checked={checked}, soft_deleted={soft_deleted}, skipped_pinned={skipped_pinned}, "
                "skipped_saved_or_applied={skipped_saved_or_applied}".format(**stats)
            ),
            "success",
        )
        return redirect(url_for("dashboard"))

    return app


if __name__ == "__main__":
    create_app().run(debug=True, host="127.0.0.1", port=DEFAULT_PORT)
