"""
Fetch public job posting HTML and extract readable text for AI enrichment.
Fails gracefully when blocked or unreachable.
"""

import re
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup

DEFAULT_TIMEOUT_SEC = 12
MAX_CHARS = 20000


def fetch_job_posting_text(url: str, timeout_sec: float = DEFAULT_TIMEOUT_SEC) -> Tuple[Optional[str], Optional[str]]:
    """
    Returns (text, error_message). text is None on failure.
    """
    if not url or not url.startswith("http"):
        return None, "invalid_or_missing_url"

    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; JobPipeline/1.0; +local personal tool)",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout_sec, allow_redirects=True)
        if resp.status_code >= 400:
            return None, f"http_{resp.status_code}"
        ctype = resp.headers.get("Content-Type", "")
        if "html" not in ctype.lower() and resp.text.strip().startswith("<") is False:
            return None, "non_html"
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()
        text = soup.get_text(separator="\n")
        lines = [re.sub(r"\s+", " ", ln).strip() for ln in text.splitlines()]
        collapsed = "\n".join(ln for ln in lines if ln)
        collapsed = collapsed[:MAX_CHARS]
        if len(collapsed) < 80:
            return None, "content_too_short"
        return collapsed, None
    except requests.exceptions.Timeout:
        return None, "timeout"
    except requests.exceptions.RequestException as exc:
        return None, f"request_error:{type(exc).__name__}"
    except Exception as exc:
        return None, f"parse_error:{type(exc).__name__}"
