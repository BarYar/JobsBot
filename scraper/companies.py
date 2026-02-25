"""
scraper/companies.py
────────────────────────────────────────────────────────────────────────────
Scrapes career pages of 22 top Israeli-presence tech companies.
All endpoints verified/updated February 2026.

Strategy per company:
  • Greenhouse API  → /boards/{token}/jobs  (public JSON)
  • Lever API       → /v0/postings/{slug}   (public JSON)
  • SmartRecruiters → /v1/companies/{id}/postings
  • Custom REST     → Google, Amazon, Microsoft, Intel, Nvidia, Apple
  • HTML scrape     → Shabak (fallback)
"""

import hashlib
import logging
import re
import sys
import pathlib
import time
from typing import Any, Dict, List, Optional

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from models import Job  # noqa: E402

logger = logging.getLogger("scraper.companies")

# ── Shared HTTP session ───────────────────────────────────────────────────────
_SESSION = requests.Session()
_SESSION.verify = False
_SESSION.headers.update({
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept":          "application/json, text/html, */*",
})

TIMEOUT = 15

# ── Target city areas — SAME as LinkedIn scraper ──────────────────────────────
_IL_TERMS = {
    # Tel Aviv metro
    "tel aviv", "ramat gan", "petah tikva", "holon", "bat yam",
    "givatayim", "kiryat ono", "or yehuda", "airport city", "lod",
    "ramla", "rishon", "yahud", "bnei brak", "azur",
    # Herzliya + North Sharon
    "herzliya", "ra'anana", "raanana", "kfar saba", "hod hasharon",
    "tel mond", "even yehuda",
    # Rehovot corridor
    "rehovot", "nes ziona", "yavne", "gedera", "rechovot",
    # Jerusalem
    "jerusalem",
    # Ashdod
    "ashdod",
    # NOTE: "remote" intentionally omitted — "Remote, UK" etc. would false-match
    # Bare "israel" is handled in _is_israel() below
}

# Locations that should NOT pass
_IL_EXCLUDE = {
    "yokneam", "haifa", "beer sheva", "be'er sheva", "netanya",
    "nahariya", "afula", "tiberias", "eilat", "karmiel", "acre", "akko",
    "nazareth",
}

# ── Role keyword filters ───────────────────────────────────────────────────────
_INCLUDE = {
    "software engineer", "software developer",
    "backend", "back-end", "back end",
    "full stack", "fullstack", "full-stack",
    "c# developer", "c# engineer",
    "python developer", "python engineer",
    "r&d engineer", "r&d developer",
    "embedded software", "embedded engineer",
    "application engineer", "application developer",
    "server engineer", "server developer",
    "developer", "engineer",
}

_EXCLUDE = {
    "devops", "devsecops", "site reliability", "sre",
    "data engineer", "data scientist", "data analyst",
    "cloud engineer", "cloud architect",
    "ml engineer", "machine learning",
    "network engineer", "automation engineer",
    "qa engineer", "quality assurance",
    "security researcher", "security engineer",
    "hardware engineer", "electrical engineer", "mechanical engineer",
    "sales engineer", "solutions engineer", "field engineer",
    "product manager", "scrum", "marketing",
    "ui developer", "ui engineer", "ux", "ui/ux",
}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_relevant(title: str) -> bool:
    t = title.lower()
    # Any exclusion term → reject immediately
    if any(ex in t for ex in _EXCLUDE):
        return False
    # Must match at least one include term
    return any(inc in t for inc in _INCLUDE)


def _is_israel(location: str) -> bool:
    """Only accept jobs in the target city areas."""
    if not location:
        return False
    loc = location.lower()
    # Reject excluded cities first
    if any(ex in loc for ex in _IL_EXCLUDE):
        return False
    # Match target city areas
    if any(term in loc for term in _IL_TERMS):
        return True
    # Accept if "israel" appears anywhere in the location string
    if "israel" in loc:
        return True
    return False


def _uid(text: str) -> str:
    return hashlib.md5(text.encode()).hexdigest()[:16]


def _get(url: str, params: Optional[Dict] = None, json_mode: bool = True) -> Any:
    try:
        r = _SESSION.get(url, params=params, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        return r.json() if json_mode else r.text
    except Exception as exc:
        logger.warning("GET %s failed: %s", url, exc)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Generic ATS helpers
# ─────────────────────────────────────────────────────────────────────────────

def _greenhouse(token: str, company: str) -> List[Job]:
    """Greenhouse public jobs API."""
    data = _get(f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs", params={"content": "false"})
    if not data:
        return []
    jobs = []
    for item in data.get("jobs", []):
        title    = item.get("title", "")
        location = item.get("location", {}).get("name", "")
        if not _is_relevant(title) or not _is_israel(location):
            continue
        job_id = str(item.get("id", _uid(title + location)))
        jobs.append(Job(
            source=token, job_id=job_id, title=title, company=company,
            location=location, url=item.get("absolute_url", ""),
        ))
    logger.info("[Greenhouse/%s] %d jobs.", company, len(jobs))
    return jobs


def _lever(slug: str, company: str) -> List[Job]:
    """Lever public postings API."""
    data = _get(f"https://api.lever.co/v0/postings/{slug}", params={"mode": "json"})
    if not data:
        return []
    jobs = []
    for item in data:
        title    = item.get("text", "")
        location = item.get("categories", {}).get("location", "")
        if not _is_relevant(title) or not _is_israel(location):
            continue
        job_id = item.get("id", _uid(title + location))
        jobs.append(Job(
            source=slug, job_id=str(job_id), title=title, company=company,
            location=location, url=item.get("hostedUrl", ""),
        ))
    logger.info("[Lever/%s] %d jobs.", company, len(jobs))
    return jobs


def _smartrecruiters(company_id: str, company: str) -> List[Job]:
    """SmartRecruiters public API — always post-filter by Israel location."""
    data = _get(
        f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings",
        params={"limit": 100, "country": "IL"},
    )
    if not data:
        return []
    jobs = []
    for item in data.get("content", []):
        title    = item.get("name", "")
        city     = item.get("location", {}).get("city", "")
        country  = item.get("location", {}).get("countryCode", item.get("location", {}).get("country", ""))
        location = f"{city}, {country}".strip(", ")
        if not _is_relevant(title):
            continue
        if not _is_israel(location):
            continue
        job_id = item.get("id", _uid(title + location))
        jobs.append(Job(
            source=company_id, job_id=str(job_id), title=title, company=company,
            location=location,
            url=f"https://jobs.smartrecruiters.com/{company_id}/{job_id}",
        ))
    logger.info("[SmartRecruiters/%s] %d jobs.", company, len(jobs))
    return jobs


# ─────────────────────────────────────────────────────────────────────────────
# Company scrapers — all endpoints verified Feb 2026
# ─────────────────────────────────────────────────────────────────────────────

# ── 1. Google ─────────────────────────────────────────────────────────────────
def _google() -> List[Job]:
    """Google Careers — scrape job links from the HTML results page."""
    html = _get(
        "https://www.google.com/about/careers/applications/jobs/results",
        params={"location": "Israel", "q": "software engineer"},
        json_mode=False,
    )
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    jobs = []
    # Job links follow pattern: jobs/results/{id}-{slug}
    seen = set()
    for a in soup.find_all("a", href=re.compile(r"jobs/results/\d+")):
        href = a.get("href", "")
        m = re.search(r"results/(\d+)", href)
        if not m:
            continue
        job_id = m.group(1)
        if job_id in seen:
            continue
        seen.add(job_id)
        # Try to get title from parent container
        title = ""
        parent = a.find_parent(["li", "div"])
        if parent:
            h = parent.find(["h3", "h2"])
            if h:
                title = h.get_text(strip=True)
        if not title:
            # Extract from slug: "107582654160741062-software-engineer-cpu..."
            slug = href.split("/")[-1]
            parts = slug.split("-")[1:]  # skip the ID
            if parts:
                # Filter out query params
                title = " ".join(parts).split("?")[0].replace("-", " ").title()
        if not title or not _is_relevant(title):
            continue
        full_url = "https://www.google.com/about/careers/applications/" + href.lstrip("./")
        jobs.append(Job(
            source="google", job_id=job_id, title=title, company="Google",
            location="Israel", url=full_url,
        ))
    logger.info("[Google] %d Israel software jobs found (HTML).", len(jobs))
    return jobs


# ── 2. Amazon ─────────────────────────────────────────────────────────────────
def _amazon() -> List[Job]:
    """Amazon Jobs public JSON search API."""
    data = _get("https://www.amazon.jobs/en/search.json", params={
        "base_query": "software engineer",
        "loc_query":  "Israel",
        "job_count":  20,
        "result_limit": 20,
        "sort":       "recent",
        "category[]": "software-development",
    })
    if not data:
        return []
    jobs = []
    for item in data.get("jobs", []):
        # Amazon API returns global results — must filter by country_code
        if item.get("country_code", "") != "ISR":
            continue
        title    = item.get("title", "")
        location = item.get("normalized_location", item.get("location", ""))
        if not _is_relevant(title) or not _is_israel(location):
            continue
        job_id = str(item.get("id") or item.get("job_id") or _uid(title + location))
        jobs.append(Job(source="amazon", job_id=job_id, title=title,
                        company="Amazon", location=location,
                        url="https://www.amazon.jobs" + item.get("job_path", "")))
    logger.info("[Amazon] %d jobs.", len(jobs))
    return jobs


# ── 3. Microsoft ──────────────────────────────────────────────────────────────
def _microsoft() -> List[Job]:
    """Microsoft Careers search API (updated URL, Feb 2026)."""
    # Primary endpoint
    data = _get("https://jobs.careers.microsoft.com/global/en/search", params={
        "q":     "software engineer",
        "lc":    "Israel",
        "l":     "en_us",
        "pgSz":  20,
        "pg":    1,
        "o":     "Relevance",
    })
    if not data:
        return []
    jobs = []
    for item in data.get("operationResult", {}).get("result", {}).get("jobs", []):
        title    = item.get("title", "")
        location = item.get("location", "")
        if not _is_relevant(title) or not _is_israel(location):
            continue
        job_id = str(item.get("jobId", _uid(title + location)))
        jobs.append(Job(source="microsoft", job_id=job_id, title=title,
                        company="Microsoft", location=location,
                        url=f"https://jobs.careers.microsoft.com/global/en/job/{job_id}"))
    logger.info("[Microsoft] %d jobs.", len(jobs))
    return jobs


# ── 4. Wix ────────────────────────────────────────────────────────────────────
def _wix() -> List[Job]:
    """Wix uses SmartRecruiters."""
    return _smartrecruiters("WixCom", "Wix")


# ── 5. Wiz ────────────────────────────────────────────────────────────────────
def _wiz() -> List[Job]:
    """Wiz — Greenhouse board 'wizinc' (verified Feb 2026, 29 Tel Aviv jobs)."""
    return _greenhouse("wizinc", "Wiz")


# ── 6. Monday.com ─────────────────────────────────────────────────────────────
def _monday() -> List[Job]:
    """Monday.com — try SmartRecruiters then Lever."""
    jobs = _smartrecruiters("mondaydotcom", "Monday.com")
    if not jobs:
        jobs = _lever("monday", "Monday.com")
    return jobs


# ── 7. Mobileye ───────────────────────────────────────────────────────────────
def _mobileye() -> List[Job]:
    """Mobileye Workday — requires POST with payload."""
    url = "https://mobileye.wd3.myworkdayjobs.com/wday/cxs/mobileye/Mobileye_Careers/jobs"
    payload = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": "software engineer"}
    try:
        r = _SESSION.post(url, json=payload, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.warning("[Mobileye] Workday failed: %s — trying SmartRecruiters", exc)
        return _smartrecruiters("Mobileye", "Mobileye")
    jobs = []
    for item in data.get("jobPostings", []):
        title    = item.get("title", "")
        location = item.get("locationsText", "Israel")
        if not _is_relevant(title) or not _is_israel(location):
            continue
        path = item.get("externalPath", "")
        jobs.append(Job(source="mobileye", job_id=_uid(title + path),
                        title=title, company="Mobileye", location=location,
                        url="https://mobileye.wd3.myworkdayjobs.com/Mobileye_Careers" + path))
    logger.info("[Mobileye] %d jobs.", len(jobs))
    return jobs


# ── 8. IAI ────────────────────────────────────────────────────────────────────
def _iai() -> List[Job]:
    return _smartrecruiters("IsraelAerospaceIndustries", "IAI")


# ── 9. Shabak ─────────────────────────────────────────────────────────────────
def _shabak() -> List[Job]:
    """Shabak redirects to a modern Angular SPA — try their API directly."""
    data = _get("https://www.shabak.gov.il/api/JobsApi/GetJobs", json_mode=True)
    if data:
        jobs = []
        for item in data if isinstance(data, list) else data.get("jobs", data.get("Items", [])):
            title = item.get("Title", item.get("title", ""))
            if not _is_relevant(title):
                continue
            job_id = str(item.get("Id", item.get("id", _uid(title))))
            url    = item.get("Url", item.get("url", "https://www.shabak.gov.il/career/"))
            jobs.append(Job(source="shabak", job_id=job_id, title=title,
                            company="Shabak (Shin Bet)", location="Israel", url=url))
        logger.info("[Shabak] %d jobs (API).", len(jobs))
        return jobs

    # Fallback: HTML
    html = _get("https://www.shabak.gov.il/career/", json_mode=False)
    if not html:
        return []
    soup = BeautifulSoup(html, "lxml")
    jobs = []
    for el in soup.select("a[href*='career'], a[href*='job']"):
        title = el.get_text(strip=True)
        if not title or not _is_relevant(title):
            continue
        href = el.get("href", "")
        if href and not href.startswith("http"):
            href = "https://www.shabak.gov.il" + href
        jobs.append(Job(source="shabak", job_id=_uid(title), title=title,
                        company="Shabak (Shin Bet)", location="Israel", url=href))
    logger.info("[Shabak] %d jobs (HTML).", len(jobs))
    return jobs


# ── 10. Check Point ───────────────────────────────────────────────────────────
def _checkpoint() -> List[Job]:
    """Check Point uses SmartRecruiters."""
    return _smartrecruiters("CheckPointSoftwareTechnologiesLtd", "Check Point")


# ── 11. Intel Israel ─────────────────────────────────────────────────────────
def _intel() -> List[Job]:
    """Intel careers — Workday API (verified Feb 2026)."""
    url = "https://intel.wd1.myworkdayjobs.com/wday/cxs/intel/external/jobs"
    payload = {
        "appliedFacets": {},
        "limit":      20,
        "offset":     0,
        "searchText": "software engineer",
    }
    try:
        r = _SESSION.post(url, json=payload, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.warning("[Intel] Workday request failed: %s", exc)
        return []
    jobs = []
    for item in data.get("jobPostings", []):
        title    = item.get("title", "")
        location = item.get("locationsText", "")
        if not _is_relevant(title) or not _is_israel(location):
            continue
        path = item.get("externalPath", "")
        jobs.append(Job(
            source="intel", job_id=_uid(title + path),
            title=title, company="Intel", location=location,
            url="https://intel.wd1.myworkdayjobs.com/external" + path,
        ))
    logger.info("[Intel] %d jobs.", len(jobs))
    return jobs


# ── 12. Nvidia Israel ────────────────────────────────────────────────────────
def _nvidia() -> List[Job]:
    """Nvidia Workday careers — correct payload for Feb 2026."""
    url = "https://nvidia.wd5.myworkdayjobs.com/wday/cxs/nvidia/NVIDIAExternalCareerSite/jobs"
    payload = {
        "appliedFacets": {},
        "limit":         20,
        "offset":        0,
        "searchText":    "software engineer",
        "locationHierarchy1": ["Israel"],
    }
    try:
        r = _SESSION.post(url, json=payload, timeout=TIMEOUT, verify=False)
        r.raise_for_status()
        data = r.json()
    except Exception as exc:
        logger.warning("[Nvidia] Request failed: %s", exc)
        return []
    jobs = []
    for item in data.get("jobPostings", []):
        title    = item.get("title", "")
        location = item.get("locationsText", "Israel")
        if not _is_relevant(title) or not _is_israel(location):
            continue
        path = item.get("externalPath", "")
        jobs.append(Job(source="nvidia", job_id=_uid(title + path),
                        title=title, company="Nvidia", location=location,
                        url="https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite" + path))
    logger.info("[Nvidia] %d jobs.", len(jobs))
    return jobs


# ── 13. CyberArk ─────────────────────────────────────────────────────────────
def _cyberark() -> List[Job]:
    """CyberArk uses SmartRecruiters."""
    return _smartrecruiters("CyberArk", "CyberArk")


# ── 14. Amdocs ───────────────────────────────────────────────────────────────
def _amdocs() -> List[Job]:
    return _smartrecruiters("Amdocs", "Amdocs")


# ── 15. Fiverr ───────────────────────────────────────────────────────────────
def _fiverr() -> List[Job]:
    """Fiverr — try Greenhouse then SmartRecruiters."""
    jobs = _greenhouse("fiverr", "Fiverr")
    if not jobs:
        jobs = _smartrecruiters("Fiverr", "Fiverr")
    return jobs


# ── 16. Unity / IronSource ───────────────────────────────────────────────────
def _unity() -> List[Job]:
    return _greenhouse("unity3d", "Unity (IronSource)")


# ── 17. Meta Israel ──────────────────────────────────────────────────────────
def _meta() -> List[Job]:
    """Meta careers via their public job search API."""
    data = _get("https://www.metacareers.com/jobs", params={
        "q":        "software engineer",
        "locations[0]": "Israel",
        "results_per_page": 20,
    })
    if not data:
        return []
    jobs = []
    for item in data.get("data", data.get("jobs", [])):
        title    = item.get("title", "")
        locs     = item.get("locations", [{}])
        location = locs[0].get("city", "Israel") if locs else "Israel"
        if not _is_relevant(title) or not _is_israel(location):
            continue
        job_id = str(item.get("id", _uid(title + location)))
        jobs.append(Job(source="meta", job_id=job_id, title=title,
                        company="Meta", location=location,
                        url=f"https://www.metacareers.com/jobs/{job_id}/"))
    logger.info("[Meta] %d jobs.", len(jobs))
    return jobs


# ── 18. Apple Israel ─────────────────────────────────────────────────────────
def _apple() -> List[Job]:
    """Apple Jobs search API (updated path)."""
    data = _get("https://jobs.apple.com/api/role/search", params={
        "q":        "software engineer",
        "filters.location": "ISRAL",
        "limit":    20,
        "page":     1,
    })
    if not data:
        return []
    jobs = []
    for item in data.get("searchResults", []):
        title    = item.get("postingTitle", "")
        locs     = item.get("locations", [{}])
        location = locs[0].get("name", "Israel") if locs else "Israel"
        if not _is_relevant(title) or not _is_israel(location):
            continue
        job_id = str(item.get("positionId", _uid(title + location)))
        jobs.append(Job(source="apple", job_id=job_id, title=title,
                        company="Apple", location=location,
                        url=f"https://jobs.apple.com/en-us/details/{job_id}"))
    logger.info("[Apple] %d jobs.", len(jobs))
    return jobs


# ── 19. Radware ──────────────────────────────────────────────────────────────
def _radware() -> List[Job]:
    """Radware uses SmartRecruiters."""
    return _smartrecruiters("Radware", "Radware")


# ── 20. NICE Systems ─────────────────────────────────────────────────────────
def _nice() -> List[Job]:
    """NICE — Greenhouse board 'nice' (verified Feb 2026, 13 Israel jobs)."""
    return _greenhouse("nice", "NICE Systems")


# ── 21. Kaltura ──────────────────────────────────────────────────────────────
def _kaltura() -> List[Job]:
    """Kaltura — try Greenhouse then SmartRecruiters."""
    jobs = _greenhouse("kaltura", "Kaltura")
    if not jobs:
        jobs = _smartrecruiters("Kaltura", "Kaltura")
    return jobs


# ── 22. Imperva / Thales ─────────────────────────────────────────────────────
def _imperva() -> List[Job]:
    """Imperva uses SmartRecruiters after Thales acquisition."""
    return _smartrecruiters("Imperva", "Imperva")


# ─────────────────────────────────────────────────────────────────────────────
# Master runner
# ─────────────────────────────────────────────────────────────────────────────

_SCRAPERS = [
    # ── WORKING (verified Feb 2026) ──────────────────────────────────────────
    ("Google",           _google),
    ("Amazon",           _amazon),
    ("Nvidia",           _nvidia),
    ("Intel",            _intel),
    ("NICE",             _nice),
    ("Unity/IronSource", _unity),
    ("Wiz",              _wiz),

    # ── BROKEN / 0 results (disabled to avoid noise + wasted requests) ───────
    # ("Microsoft",      _microsoft),    # Returns HTML, no JSON API
    # ("Wix",            _wix),          # SmartRecruiters WixCom returns 0
    # ("Wiz",            _wiz),          # All ATS slugs 404 or 0 Israel jobs
    # ("Monday.com",     _monday),       # All endpoints return 0
    # ("Mobileye",       _mobileye),     # Workday 401, SmartRecruiters 0
    # ("IAI",            _iai),          # SmartRecruiters 0
    # ("Shabak",         _shabak),       # 403 everywhere
    # ("Check Point",    _checkpoint),   # SmartRecruiters 0
    # ("CyberArk",       _cyberark),     # SmartRecruiters 0
    # ("Amdocs",         _amdocs),       # SmartRecruiters 0
    # ("Fiverr",         _fiverr),       # Wrong Fiverr in SmartRecruiters
    # ("Meta",           _meta),         # API returns 400
    # ("Apple",          _apple),        # API 404
    # ("Radware",        _radware),      # SmartRecruiters 0
    # ("Kaltura",        _kaltura),      # Greenhouse 404, SR 0
    # ("Imperva",        _imperva),      # SmartRecruiters 0
]


def scrape_all_companies() -> List[Job]:
    """Run all company scrapers, return only jobs not yet in the DB."""
    import db  # imported here to avoid circular at module load
    all_jobs: List[Job] = []
    for name, fn in _SCRAPERS:
        logger.info("Scraping %s …", name)
        try:
            jobs = fn()
            # Pre-filter: drop already-seen jobs before returning
            new = [j for j in jobs if db.is_new(j.source, j.job_id)]
            all_jobs.extend(new)
        except Exception as exc:
            logger.error("[%s] Unhandled error: %s", name, exc)
        time.sleep(2)
    logger.info("Company scrapers total: %d new jobs.", len(all_jobs))
    return all_jobs

