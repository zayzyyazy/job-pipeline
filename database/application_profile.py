"""Local-only applicant profile for Apply Assist (stored as JSON in app_settings)."""

from __future__ import annotations

import json
from typing import Any, Dict, Tuple

from database.app_settings import get_setting, set_setting

_PROFILE_KEY = "APPLICATION_PROFILE_JSON"

_DEFAULT: Dict[str, str] = {
    "full_name": "",
    "email": "",
    "phone": "",
    "city": "",
    "linkedin_url": "",
    "github_url": "",
    "portfolio_url": "",
    "resume_path": "",
    "cover_letter_default": "",
    "work_authorization_note": "",
    "availability_note": "",
    "short_about": "",
}

PROFILE_FIELDS: Tuple[str, ...] = tuple(_DEFAULT.keys())


def get_application_profile() -> Dict[str, str]:
    raw = get_setting(_PROFILE_KEY)
    if not raw or not str(raw).strip():
        return dict(_DEFAULT)
    try:
        data = json.loads(raw)
        if not isinstance(data, dict):
            return dict(_DEFAULT)
        out = dict(_DEFAULT)
        for k in _DEFAULT:
            v = data.get(k)
            out[k] = str(v).strip() if v is not None else ""
        return out
    except json.JSONDecodeError:
        return dict(_DEFAULT)


def save_application_profile(data: Dict[str, Any]) -> None:
    out = dict(_DEFAULT)
    for k in _DEFAULT:
        v = data.get(k)
        out[k] = str(v).strip() if v is not None else ""
    set_setting(_PROFILE_KEY, json.dumps(out, ensure_ascii=True))


def profile_ready_for_assist(profile: Dict[str, str]) -> bool:
    """Minimum data so Apply Assist is meaningful (never log these checks)."""
    fn = (profile.get("full_name") or "").strip()
    em = (profile.get("email") or "").strip()
    return len(fn) >= 2 and len(em) >= 3 and "@" in em
