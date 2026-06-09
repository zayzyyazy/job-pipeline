"""
Apply Assist: local Playwright helper to pre-fill safe application fields only.
Never submits forms or clicks final apply/send buttons.
"""

from __future__ import annotations

import json
import logging
import re
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from database import db as dbmod


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

_LOGGER = logging.getLogger(__name__)

PLAYWRIGHT_INSTALL_HINT = (
    "Playwright browser is not installed. Run: python -m playwright install chromium"
)

_JS_FIELD_META = """
(el) => {
  const txt = (n) => (n && (n.innerText || n.textContent) || "").trim().slice(0, 500);
  const aria = el.getAttribute("aria-label") || "";
  const ph = el.placeholder || "";
  const name = el.name || "";
  const id = el.id || "";
  const typ = (el.type || "text").toLowerCase();
  let lab = "";
  if (id) {
    try {
      const esc = (typeof CSS !== "undefined" && CSS.escape) ? CSS.escape(id) : id.replace(/"/g, "");
      const l = document.querySelector("label[for=\\"" + esc + "\\"]");
      if (l) lab = txt(l);
    } catch (e) {}
  }
  if (!lab && el.labels && el.labels.length) {
    lab = Array.from(el.labels).map((l) => txt(l)).join(" ");
  }
  return { aria, ph, name, id, typ, tag: el.tagName.toLowerCase() };
}
"""

_SENSITIVE_COMBINED = re.compile(
    r"salary|compensation|equity|stock|bonus|pay\s*range|expected\s*(pay|salary)|"
    r"gender|race|ethnicity|disability|veteran|eeo|divers(?:ity|e)|lgbtq?|"
    r"pronoun|sexual\s*orientation|marital|religion|nationality|birth|date\s*of\s*birth|\bdob\b|"
    r"\bvisa\b|sponsorship|work\s*authorization|authorized\s*to\s*work|"
    r"criminal|conviction|felony|eligible\s*to\s*work|legally\s*authorized|"
    r"ethnic\s*background|race\s*or|veteran\s*status",
    re.I,
)

_COMPANY_WEBSITE = re.compile(r"company\s*website|employer\s*url|corporate\s*site", re.I)


def resolve_apply_url(job: Dict[str, Any], pasted_url: Optional[str] = None) -> Optional[str]:
    candidates: List[str] = []
    if pasted_url and str(pasted_url).strip():
        candidates.append(str(pasted_url).strip())
    for k in ("application_url", "discovered_url", "job_link"):
        v = job.get(k)
        if v and str(v).strip():
            candidates.append(str(v).strip())
    for u in candidates:
        low = u.lower()
        if low.startswith(("http://", "https://", "file://")):
            return u
    return None


def profile_display_lines(profile: Dict[str, str]) -> List[Tuple[str, str]]:
    labels = [
        ("Full name", "full_name"),
        ("Email", "email"),
        ("Phone", "phone"),
        ("City / location", "city"),
        ("LinkedIn", "linkedin_url"),
        ("GitHub", "github_url"),
        ("Portfolio", "portfolio_url"),
        ("Resume path (local file)", "resume_path"),
        ("Default cover letter", "cover_letter_default"),
        ("Work authorization note", "work_authorization_note"),
        ("Availability note", "availability_note"),
        ("Short about me", "short_about"),
    ]
    out: List[Tuple[str, str]] = []
    for lab, k in labels:
        v = (profile.get(k) or "").strip()
        if v:
            out.append((lab, v))
    return out


def build_application_packet(job: Dict[str, Any], profile: Dict[str, str]) -> Dict[str, Any]:
    """Copy-ready text blocks (no network; no logging of contents)."""
    title = (job.get("clean_title") or job.get("title") or "").strip()
    company = (job.get("company") or "").strip()
    skills_raw = job.get("skills") or "[]"
    try:
        skills = json.loads(skills_raw) if isinstance(skills_raw, str) else list(skills_raw or [])
    except json.JSONDecodeError:
        skills = []
    if not isinstance(skills, list):
        skills = []
    skills = [str(s).strip() for s in skills if str(s).strip()][:12]
    top = skills[0] if skills else "the technical stack described in the posting"

    interest = (
        f"I am interested in the {title} role at {company}. "
        f"My background aligns with {top}. "
        f"I would welcome a conversation about how I can contribute."
    ).strip()

    skills_match = ""
    if skills:
        skills_match = "Skills to highlight from your profile vs this posting: " + ", ".join(skills[:8]) + "."
    else:
        skills_match = "Add or refresh job posting text on this card to extract clearer skills for tailoring."

    cover = (profile.get("cover_letter_default") or "").strip()
    if not cover:
        cover = (
            f"Dear Hiring Team,\n\n"
            f"I am writing to express interest in the {title} position at {company}.\n\n"
            f"[Add your experience and fit here — save a default in Application Profile.]\n\n"
            f"Best regards,\n{(profile.get('full_name') or '[Your name]').strip()}"
        )

    return {
        "profile_lines": profile_display_lines(profile),
        "interest_suggestion": interest,
        "skills_match": skills_match,
        "cover_letter_block": cover,
        "short_about_block": (profile.get("short_about") or "").strip(),
    }


def _haystack(meta: Dict[str, str]) -> str:
    parts = [meta.get("aria") or "", meta.get("ph") or "", meta.get("name") or "", meta.get("id") or ""]
    return " | ".join(parts).lower()


def _classify_slot(hay: str) -> Tuple[str, str]:
    """Return (action, detail) where action is fill slot name, skip_sensitive, skip_control, or unknown."""
    if _SENSITIVE_COMBINED.search(hay):
        return "skip_sensitive", "sensitive_or_high_risk_topic"
    if _COMPANY_WEBSITE.search(hay):
        return "skip_sensitive", "company_site_field"

    rules: List[Tuple[str, re.Pattern[str]]] = [
        ("first_name", re.compile(r"first[\s_]*name|given[\s_]*name|vorname|\bfname\b", re.I)),
        ("last_name", re.compile(r"last[\s_]*name|surname|family[\s_]*name|nachname|\blname\b", re.I)),
        ("full_name", re.compile(r"full[\s_]*name|^name$|\byour\s*name\b|applicant\s*name|legal\s*name", re.I)),
        ("email", re.compile(r"\bemail\b|e-mail|mail\s*address", re.I)),
        ("phone", re.compile(r"\bphone\b|telefon|mobile|tel\b|cell", re.I)),
        ("city", re.compile(r"\bcity\b|town|location(?!\s*preference)|ort\b|wohnort", re.I)),
        ("linkedin", re.compile(r"linkedin", re.I)),
        ("github", re.compile(r"github|git\s*hub", re.I)),
        ("portfolio", re.compile(r"portfolio|personal\s*site|personal\s*url", re.I)),
        ("website", re.compile(r"\bwebsite\b|web\s*site|homepage|url(?!.*linkedin)", re.I)),
        ("resume", re.compile(r"\bresume\b|\bcv\b|lebenslauf|curriculum|upload\s*cv", re.I)),
        ("cover", re.compile(r"cover\s*letter|anschreiben|motivation(?!.*salary)", re.I)),
        ("about", re.compile(r"about\s*you|tell\s*us\s*about|summary|bio|introduction", re.I)),
        ("interest", re.compile(r"why\s*(are\s*you|do\s*you)|interest\s*in|what\s*attracts", re.I)),
    ]
    for slot, rx in rules:
        if rx.search(hay):
            return "fill", slot
    return "unknown", ""


def _split_name(full: str) -> Tuple[str, str]:
    full = (full or "").strip()
    if not full:
        return "", ""
    parts = full.split(None, 1)
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], parts[1]


def _value_for_slot(slot: str, profile: Dict[str, str], consumed: Dict[str, bool]) -> Optional[str]:
    fn = (profile.get("full_name") or "").strip()
    first, last = _split_name(fn)
    if slot == "first_name":
        if consumed.get("first_name"):
            return None
        consumed["first_name"] = True
        return first or None
    if slot == "last_name":
        if consumed.get("last_name"):
            return None
        consumed["last_name"] = True
        return last or first or None
    if slot == "full_name":
        if consumed.get("full_name"):
            return None
        consumed["full_name"] = True
        return fn or None
    if slot == "email":
        if consumed.get("email"):
            return None
        consumed["email"] = True
        return (profile.get("email") or "").strip() or None
    if slot == "phone":
        if consumed.get("phone"):
            return None
        consumed["phone"] = True
        return (profile.get("phone") or "").strip() or None
    if slot == "city":
        if consumed.get("city"):
            return None
        consumed["city"] = True
        return (profile.get("city") or "").strip() or None
    if slot == "linkedin":
        if consumed.get("linkedin"):
            return None
        consumed["linkedin"] = True
        return (profile.get("linkedin_url") or "").strip() or None
    if slot == "github":
        if consumed.get("github"):
            return None
        consumed["github"] = True
        return (profile.get("github_url") or "").strip() or None
    if slot == "portfolio":
        if consumed.get("portfolio"):
            return None
        consumed["portfolio"] = True
        return (profile.get("portfolio_url") or "").strip() or None
    if slot == "website":
        if consumed.get("website"):
            return None
        consumed["website"] = True
        return (profile.get("portfolio_url") or "").strip() or None
    if slot == "cover":
        if consumed.get("cover"):
            return None
        consumed["cover"] = True
        return (profile.get("cover_letter_default") or "").strip() or None
    if slot == "about":
        if consumed.get("about"):
            return None
        consumed["about"] = True
        base = (profile.get("short_about") or "").strip()
        if base:
            return base
        avail = (profile.get("availability_note") or "").strip()
        return avail or None
    if slot == "interest":
        if consumed.get("interest"):
            return None
        consumed["interest"] = True
        return (profile.get("short_about") or "").strip() or None
    return None


def _resume_path(profile: Dict[str, str]) -> Optional[Path]:
    raw = (profile.get("resume_path") or "").strip()
    if not raw:
        return None
    p = Path(raw).expanduser()
    try:
        if p.is_file():
            return p
    except OSError:
        return None
    return None


def verify_playwright_chromium() -> Tuple[bool, Optional[str]]:
    try:
        from playwright.sync_api import sync_playwright  # noqa: F401
    except ImportError:
        return False, "Playwright is not installed. Run: pip install playwright"
    try:
        from playwright.sync_api import sync_playwright

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            browser.close()
        return True, None
    except Exception as exc:  # pragma: no cover - environment specific
        err = str(exc).lower()
        if "executable" in err or "browser" in err or "chromium" in err:
            return False, PLAYWRIGHT_INSTALL_HINT
        return False, f"Playwright could not start Chromium: {exc}"


def _run_playwright_session(session_id: int, job_id: int, url: str, profile: Dict[str, str]) -> None:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        dbmod.update_apply_assist_session(
            session_id,
            status="error_playwright_missing",
            finished_at=_now_iso(),
            error_message="Playwright is not installed. Run: pip install playwright",
            manual_required=["Install the Playwright package, then install Chromium."],
        )
        return

    detected: List[Dict[str, Any]] = []
    filled_labels: List[str] = []
    skipped: List[str] = []
    manual: List[str] = []

    consumed: Dict[str, bool] = {}
    resume_path = _resume_path(profile)

    try:
        with sync_playwright() as p:
            try:
                browser = p.chromium.launch(headless=False)
            except Exception as launch_exc:
                err_l = str(launch_exc).lower()
                if "executable" in err_l or "browser" in err_l or "chromium" in err_l:
                    dbmod.update_apply_assist_session(
                        session_id,
                        status="error_playwright_missing",
                        finished_at=_now_iso(),
                        error_message=PLAYWRIGHT_INSTALL_HINT,
                        manual_required=["Manual answer required: install Playwright Chromium."],
                    )
                else:
                    dbmod.update_apply_assist_session(
                        session_id,
                        status="error_failed",
                        finished_at=_now_iso(),
                        error_message=str(launch_exc)[:500],
                    )
                return

            dbmod.update_apply_assist_session(session_id, status="browser_launched")
            context = browser.new_context()
            page = context.new_page()
            page.set_default_timeout(60_000)
            page.goto(url, wait_until="domcontentloaded")

            handles = page.locator("input, textarea").all()
            for el in handles:
                try:
                    meta = el.evaluate(_JS_FIELD_META)
                except Exception:
                    continue
                tag = meta.get("tag") or ""
                typ = (meta.get("typ") or "text").lower()
                if typ in ("hidden", "submit", "button", "image", "reset"):
                    continue
                if tag == "input" and typ in ("checkbox", "radio"):
                    manual.append(f"checkbox/radio: {_haystack(meta)[:120]}")
                    continue

                hay = _haystack(meta)
                action, detail = _classify_slot(hay)
                detected.append({"hay": hay[:200], "type": typ, "tag": tag})

                if action == "skip_sensitive":
                    manual.append(f"Manual answer required ({detail}): {hay[:100]}")
                    skipped.append(hay[:120])
                    continue
                if action == "unknown":
                    if tag == "textarea" or typ in ("text", "email", "tel", "url"):
                        manual.append(f"Manual answer required (unclear field): {hay[:120]}")
                    continue

                slot = detail
                if slot == "resume":
                    if typ != "file" or not resume_path:
                        manual.append("Resume file: set a valid local path in Application Profile.")
                        continue
                    try:
                        el.set_input_files(str(resume_path))
                        filled_labels.append("resume file")
                    except Exception:
                        manual.append("Resume upload could not be completed automatically.")
                    continue

                if typ == "file" and slot != "resume":
                    manual.append(f"Manual answer required (file field): {hay[:100]}")
                    continue

                val = _value_for_slot(slot, profile, consumed)
                if not val:
                    skipped.append(f"{slot}: no profile value")
                    continue

                try:
                    el.scroll_into_view_if_needed()
                    if tag == "textarea" or typ in ("text", "email", "tel", "url"):
                        el.fill("")
                        el.fill(val)
                        filled_labels.append(f"{slot}")
                except Exception:
                    manual.append(f"Could not fill automatically: {hay[:100]}")

            try:
                for sel in page.locator("select").all():
                    try:
                        hay = ((sel.get_attribute("name") or "") + " " + (sel.get_attribute("id") or "")).strip()
                        manual.append(f"Manual answer required (dropdown): {hay[:120]}")
                    except Exception:
                        continue
            except Exception:
                pass

            # Never click submit-like controls (belt-and-suspenders: do not query buttons).
            final_status = "manual_review_required" if manual else "ready"
            if not filled_labels and manual:
                final_status = "manual_review_required"

            dbmod.update_apply_assist_session(
                session_id,
                status=final_status,
                fields_detected=detected[:80],
                fields_filled=filled_labels,
                fields_skipped=skipped[:80],
                manual_required=manual[:120],
            )

            _LOGGER.info(
                "apply_assist finished session_id=%s job_id=%s filled=%s manual=%s",
                session_id,
                job_id,
                len(filled_labels),
                len(manual),
            )

            while browser.is_connected():
                time.sleep(0.35)

            dbmod.update_apply_assist_session(session_id, finished_at=_now_iso())
            try:
                browser.close()
            except Exception:
                pass
    except Exception as exc:  # pragma: no cover
        _LOGGER.exception("apply_assist session_id=%s failed", session_id)
        dbmod.update_apply_assist_session(
            session_id,
            status="error_failed",
            finished_at=_now_iso(),
            error_message=str(exc)[:500],
        )


def start_apply_assist_background(session_id: int, job_id: int, url: str, profile: Dict[str, str]) -> None:
    """Run Playwright in a daemon thread so the Flask request can return immediately."""

    def _wrap() -> None:
        try:
            _run_playwright_session(session_id, job_id, url, profile)
        except Exception as exc:  # pragma: no cover
            _LOGGER.exception("apply_assist thread crash session_id=%s", session_id)
            dbmod.update_apply_assist_session(
                session_id,
                status="error_failed",
                finished_at=_now_iso(),
                error_message=str(exc)[:500],
            )

    threading.Thread(target=_wrap, daemon=True).start()


def mark_session_completed_manually(job_id: int) -> bool:
    row = dbmod.get_latest_apply_assist_session(job_id)
    if not row:
        return False
    dbmod.update_apply_assist_session(
        int(row["id"]),
        status="completed_manually",
        finished_at=_now_iso(),
    )
    return True
