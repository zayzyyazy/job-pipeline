"""
Multi-strategy job detail discovery without paid APIs.
A: direct fetch of original job_link
B: DuckDuckGo HTML search (free — may rate-limit or break)
C: fetch & score ATS / career URLs from search results
"""

import re
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, urlparse, unquote

import requests
from bs4 import BeautifulSoup

from config import settings
from services.job_page_fetcher import MAX_CHARS, fetch_job_posting_text

JOB_SECTION_HINTS_EN = (
    "responsibilities",
    "requirements",
    "qualifications",
    "skills",
    "experience",
    "benefits",
    "what you'll do",
    "what we look for",
)

JOB_SECTION_HINTS_DE = (
    "ihre aufgaben",
    "profil",
    "anforderungen",
    "qualifikationen",
    "voraussetzungen",
    "kenntnisse",
)

PREFERRED_DOMAIN_PARTS = (
    "greenhouse.io",
    "lever.co",
    "smartrecruiters.com",
    "workable.com",
    "personio.",
    "ashbyhq.com",
    "ashbyjobs.com",
    "myworkdayjobs.com",
)

AVOID_ALWAYS_SUBSTRINGS = (
    "facebook.com",
    "instagram.com",
    "twitter.com",
    "tiktok.com",
    "linkedin.com/",
    "indeed.",
    "glassdoor.",
    "ziprecruiter",
    "simplyhired",
)

_DD_TIMEOUT = 16
_DD_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9,de;q=0.8",
    "Referer": "https://duckduckgo.com/",
}

_WS = re.compile(r"\s+")

# Require both length hints and a few section-like hits for "meaningful" posting text
_SUBSTANTIAL_MIN_LEN = 450


def content_looks_meaningful(text: Optional[str]) -> bool:
    if not text:
        return False
    stripped = text.strip()
    if len(stripped) < _SUBSTANTIAL_MIN_LEN:
        return False
    lowered = stripped.lower()
    hits = sum(1 for h in JOB_SECTION_HINTS_EN + JOB_SECTION_HINTS_DE if h in lowered)
    word_count = len(lowered.split())
    return hits >= 2 and word_count >= 72


def clean_query(q: str) -> str:
    return _WS.sub(" ", (q or "")).strip()


def _extract_href_urls_from_ddg(html_text: str) -> List[str]:
    soup = BeautifulSoup(html_text, "html.parser")
    urls: List[str] = []
    for a in soup.find_all("a", class_=re.compile(r"result__a")):
        href = (a.get("href") or "").strip()
        if href:
            urls.append(href)
    if not urls:
        for a in soup.select("a[href*='uddg']"):
            href = (a.get("href") or "").strip()
            if href:
                urls.append(href)
    return urls


def _expand_ddg_redirect(href: str) -> Optional[str]:
    try:
        parsed = urlparse(href)
        if "duckduckgo.com" in (parsed.netloc or "") and parsed.path.startswith("/l/"):
            qs = parse_qs(parsed.query)
            ulist = qs.get("uddg")
            if ulist:
                return unquote(ulist[0])
        if href.startswith("http"):
            return href
    except Exception:
        return None
    return None


def duckduckgo_search_urls(query: str, max_collect: int = 20) -> Tuple[List[str], Optional[str]]:
    try:
        body = {"q": query, "b": ""}
        resp = requests.post(
            "https://html.duckduckgo.com/html/",
            data=body,
            headers=_DD_HEADERS,
            timeout=_DD_TIMEOUT,
        )
        if resp.status_code >= 400:
            return [], f"ddg_http_{resp.status_code}"
        raw_links = _extract_href_urls_from_ddg(resp.text)
        out: List[str] = []
        seen = set()
        for h in raw_links:
            real = _expand_ddg_redirect(h)
            target = real or (h if h.startswith("http") else None)
            if target and target not in seen:
                seen.add(target)
                out.append(target)
            if len(out) >= max_collect:
                break
        return out, None
    except requests.exceptions.Timeout:
        return [], "ddg_timeout"
    except requests.exceptions.RequestException as exc:
        return [], f"ddg_request:{type(exc).__name__}"
    except Exception as exc:
        return [], f"ddg_error:{type(exc).__name__}"


def _score_url_candidates(urls: Sequence[str]) -> List[Tuple[int, str]]:
    scored_loc: List[Tuple[int, str]] = []
    for u in urls:
        low = u.lower()
        if any(av in low for av in AVOID_ALWAYS_SUBSTRINGS):
            continue
        score = 35
        for frag in PREFERRED_DOMAIN_PARTS:
            if frag in low:
                score += 115
                break
        if any(x in low for x in ("/jobs", "/job/", "/job?", "/career", "/careers", "/karriere", "/stellen")):
            score += 35
        if score <= 60:
            continue
        scored_loc.append((score, u))
    scored_loc.sort(reverse=True)
    uniq: List[str] = []
    ordered: List[Tuple[int, str]] = []
    for s, u in scored_loc:
        if u not in uniq:
            uniq.append(u)
            ordered.append((s, u))
    return ordered[:12]


def _token_overlap_words(hay_low: str, reference: str) -> int:
    ref = {re.sub(r"\W+", "", w.lower()) for w in reference.split() if len(w) > 2}
    cand_words = hay_low.split()
    cand = {re.sub(r"\W+", "", w.lower()) for w in cand_words if len(w) > 2}
    if not ref or not cand:
        return 0
    return len(ref & cand)


def score_page_quality(text: Optional[str], job_title: str, job_company: str) -> int:
    if not text:
        return -10_000
    low = text.lower()
    score = min(len(text), MAX_CHARS // 3)
    hints = JOB_SECTION_HINTS_EN + JOB_SECTION_HINTS_DE
    score += 100 * sum(1 for h in hints if h in low)
    head = low[:5000]
    score += _token_overlap_words(head, job_title or "") * 26
    score += _token_overlap_words(low[:9000], job_company or "") * 22
    if job_company.strip() and job_company.strip().lower() not in ("unknown company", "unknown"):
        slug = job_company.strip().lower()[:140]
        if slug and slug in low[:14000]:
            score += 160
    return score


def _build_product_research_queries(
    title_raw: str,
    title_for_match: str,
    company: str,
    location: Optional[str],
) -> List[str]:
    """Explicit employer/ATS-oriented queries when user taps “Research real job posting”."""
    t = (
        title_for_match.strip()
        if (title_for_match or "").strip()
        else (title_raw or "").strip()
    )
    comp = ""
    cl = (company or "").strip().lower()
    if company and cl not in ("unknown company", "unknown"):
        comp = company.strip()
    loc = (location or "").strip()
    qs: List[str] = []
    if t and comp:
        qs.extend(
            [
                clean_query(f'"{t}" "{comp}" careers'),
                clean_query(f'"{t}" "{comp}" job'),
            ]
        )
    if t and loc and comp:
        qs.append(clean_query(f'"{t}" "{loc}" "{comp}"'))
    if t and comp:
        qs.extend(
            [
                clean_query(f'site:greenhouse.io "{t}" "{comp}"'),
                clean_query(f'site:lever.co "{t}" "{comp}"'),
                clean_query(f'site:personio.de "{t}" "{comp}"'),
                clean_query(f'site:personio.com "{t}" "{comp}"'),
                clean_query(f'site:workable.com "{t}" "{comp}"'),
                clean_query(f'site:ashbyhq.com "{t}" "{comp}"'),
                clean_query(f'site:smartrecruiters.com "{t}" "{comp}"'),
                clean_query(f'site:myworkdayjobs.com "{t}" "{comp}"'),
            ]
        )
    uniq: List[str] = []
    for q in qs:
        if q and q not in uniq:
            uniq.append(q)
    return uniq[:14]


def _build_queries(title_raw: str, title_clean: str, company: str, location: Optional[str], source: Optional[str]) -> List[str]:
    t_primary = (title_clean.strip() if (title_clean or "").strip() else (title_raw or "").strip()) or ""
    comp = ""
    cl = (company or "").strip().lower()
    if company and cl not in ("unknown company", "unknown"):
        comp = company.strip()
    loc = (location or "").strip()
    qs: List[str] = []
    if t_primary and comp:
        qs.extend(
            [
                f'"{t_primary}" "{comp}" jobs',
                f'"{t_primary}" "{comp}" careers',
            ]
        )
    if t_primary and loc:
        qs.append(f'"{t_primary}" "{loc}" jobs')
    if t_primary and comp:
        qs.extend(
            [
                f'site:greenhouse.io "{t_primary}" "{comp}"',
                f'site:lever.co "{t_primary}" "{comp}"',
                f'site:workable.com "{t_primary}" "{comp}"',
                f'site:personio.de "{t_primary}" "{comp}"',
            ]
        )
    if len(qs) < 3 and t_primary:
        qs.append(f"{t_primary} job openings")
        if source:
            qs.append(f"{t_primary} careers {source}")
    uniq = []
    for q in qs:
        cq = clean_query(q)
        if cq and cq not in uniq:
            uniq.append(cq)
    return uniq[:10]


def discover_job_details(
    job_context: Dict[str, Any],
    log_prefix: str = "[DISCOVERY]",
    *,
    force_research: bool = False,
) -> Dict[str, Any]:
    """Return merged fields: job_page_text, discovery_*, logs via print."""
    link = (job_context.get("job_link") or "").strip()
    title_raw = str(job_context.get("title") or "").strip()
    title_clean = str(job_context.get("clean_title_ai") or "").strip()
    title_for_match = title_clean or title_raw
    company = str(job_context.get("company") or "").strip()
    location = (job_context.get("location") or "").strip()
    source = (job_context.get("source") or "").strip()

    out: Dict[str, Any] = {
        "job_page_text": job_context.get("job_page_text"),
        "direct_fetch_success": False,
        "direct_fetch_reason": None,
        "search_attempted": False,
        "discovered_url": None,
        "discovered_source": None,
        "discovered_text": None,
        "discovery_status": "email_only",
        "discovery_reason": "",
        "discovery_log": "",
    }

    notes: List[str] = []

    # --- Strategy A: direct ---
    original_text: Optional[str] = None
    original_err: Optional[str] = None
    direct_meaningful = False
    if settings.fetch_job_pages and link.startswith("http"):
        print(f"{log_prefix} strategy_a fetch original link: {link[:120]}")
        original_text, original_err = fetch_job_posting_text(link)
        if original_text:
            direct_meaningful = content_looks_meaningful(original_text)
            out["job_page_text"] = original_text
            out["direct_fetch_success"] = True
            out["direct_fetch_reason"] = "meaningful" if direct_meaningful else f"weak_len_{len(original_text)}"
            print(f"{log_prefix} direct_fetch_success length={len(original_text)} meaningful={direct_meaningful}")
        else:
            out["direct_fetch_success"] = False
            out["direct_fetch_reason"] = original_err or "unknown"
            print(f"{log_prefix} direct_fetch_fail reason={original_err}")
    elif not link.startswith("http"):
        out["direct_fetch_reason"] = "no_link"
        notes.append("no_original_link")

    notes.append(f"direct_meaningful={direct_meaningful}")

    discovered_best_score = -10_000
    discovered_url: Optional[str] = None
    discovered_text: Optional[str] = None
    discovery_src_note = ""
    attempted = False
    ddg_errors: List[str] = []

    # --- Strategies B+C ---
    skip_search = bool(direct_meaningful) and not force_research

    if not settings.enable_ddg_discovery:
        notes.append("ddg_disabled")

    elif skip_search:
        notes.append("skip_search_strong_original")

    else:
        if force_research:
            queries = _build_product_research_queries(title_raw, title_for_match, company, location)
            remaining = [
                q
                for q in _build_queries(title_raw, title_for_match, company, location, source)
                if q not in queries
            ]
            queries.extend(remaining[:4])
        else:
            queries = _build_queries(title_raw, title_for_match, company, location, source)
        if not queries:
            ddg_errors.append("no_queries_built")

        pooled_urls: List[str] = []

        attempted = False
        for q_idx, qry in enumerate(queries):
            attempted = True
            print(f"{log_prefix} ddg_attempt query#{q_idx+1}=(len {len(qry)})")
            urls, err = duckduckgo_search_urls(qry)
            if err:
                ddg_errors.append(err)
                print(f"{log_prefix} ddg_error={err}")
            if urls:
                print(f"{log_prefix} got {len(urls)} raw urls from DDG")

            pooled_urls.extend(urls)
            ranked = _score_url_candidates(list(dict.fromkeys(pooled_urls)))
            if ranked:
                # stop early once we found strong ATS-domain hits
                if any("greenhouse" in u.lower() or "lever.co" in u.lower() for _, u in ranked[:5]):
                    break

        attempted = attempted and bool(queries)
        out["search_attempted"] = attempted

        ranked = _score_url_candidates(list(dict.fromkeys(pooled_urls)))

        max_try = 12 if force_research else 5
        for _prio, cand_url in ranked[:max_try]:
            norm_link = (link or "").rstrip("/")
            if norm_link and cand_url.rstrip("/") == norm_link:
                continue
            print(f"{log_prefix} fetch_candidate_url={cand_url[:160]}")
            txt, ferr = fetch_job_posting_text(cand_url)
            if not txt:
                print(f"{log_prefix} candidate_fetch_fail={ferr}")
                continue
            sc = score_page_quality(txt, title_for_match or title_raw, company)
            print(f"{log_prefix} candidate_score={sc} length={len(txt)}")
            if sc > discovered_best_score:
                discovered_best_score = sc
                discovered_url = cand_url
                discovered_text = txt
                host = urlparse(cand_url).netloc.lower()
                if any(
                    p in host for p in ("greenhouse", "lever", "workable", "personio", "ashby", "smartrecruiters")
                ):
                    discovery_src_note = f"ats_via_ddg->{host}"
                else:
                    discovery_src_note = f"career_via_ddg->{host}"

    # --- Decide final discovery_status ---
    notes.extend([f"ddg_err:{e}" for e in ddg_errors[:6]])

    orig_score = score_page_quality(original_text, title_for_match or title_raw, company) if original_text else -10_000
    disc_score = discovered_best_score
    meaningful_disc = bool(discovered_text and content_looks_meaningful(discovered_text))

    pick_discovered = meaningful_disc and (not direct_meaningful or disc_score >= orig_score)
    # User-initiated deep research prefers a meaningful employer posting over a strong inbox/link capture.
    if force_research and meaningful_disc and discovered_url and discovered_text:
        pick_discovered = True

    if pick_discovered and discovered_url and discovered_text:
        out["discovered_url"] = discovered_url
        out["discovered_text"] = discovered_text[:MAX_CHARS]
        out["discovered_source"] = discovery_src_note or "duckduckgo_search"
        out["discovery_status"] = "found"
        notes.append("picked_discovered_public_page")

    elif direct_meaningful:
        out["discovery_status"] = "original_only"
        notes.append("picked_original_meaningful")
        if meaningful_disc:
            notes.append("discarded_discovered_original_strong")

    elif original_text and len(original_text.strip()) >= _SUBSTANTIAL_MIN_LEN:
        out["discovery_status"] = "original_only"
        notes.append(f"picked_original_weak_len={len(original_text)}")

    elif (
        discovered_text
        and discovered_url
        and len(discovered_text.strip()) >= 900
        and (not original_text or len((original_text or "").strip()) < _SUBSTANTIAL_MIN_LEN)
    ):
        out["discovered_url"] = discovered_url
        out["discovered_text"] = discovered_text[:MAX_CHARS]
        out["discovered_source"] = discovery_src_note or "duckduckgo_search"
        out["discovery_status"] = "found"
        notes.append("picked_discovered_heuristic_fallback")

    else:
        notes.append("no_usable_public_posting")
        if link.startswith("http") and settings.fetch_job_pages:
            out["discovery_status"] = "failed"
        else:
            out["discovery_status"] = "email_only"

    summary_line = "; ".join(notes)[:780]
    out["discovery_reason"] = summary_line
    out["discovery_log"] = summary_line

    print(
        f"{log_prefix} final_status={out['discovery_status']} discovered_url={out.get('discovered_url')} "
        f"search_attempted={out['search_attempted']} direct_reason={out.get('direct_fetch_reason')}"
    )

    return out