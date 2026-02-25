"""
scraper/linkedin.py
────────────────────────────────────────────────────────────────────────────
Guest API scraper — general keyword search only.
Runs broad keyword searches (sortBy=DD) across all of Israel (geoId),
filters to target cities, and sends results to Telegram.
"""
import logging
import re
import sys
import pathlib
import time
from datetime import datetime, timedelta, timezone
from typing import List
from urllib.parse import urlencode

import requests
import urllib3
from bs4 import BeautifulSoup

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
from models import Job   # noqa: E402
import db                # noqa: E402

logger = logging.getLogger("scraper.linkedin")

# ══════════════════════════════════════════════════════════════════════════════
#  Configuration
# ══════════════════════════════════════════════════════════════════════════════
# Broad keywords — each surfaces a different slice of LinkedIn's index.
# "developer" and "engineer" alone are too broad and return irrelevant roles,
# so we use specific compound terms to get targeted results.
_KEYWORDS = [
    # Core titles
    "software developer",
    "software engineer",
    "backend developer",
    "backend engineer",
    "full stack developer",
    "full stack engineer",
    # Language-specific
    "C# developer",
    ".NET developer",
    "java developer",
    "python developer",
    # Other common Israeli titles
    "R&D engineer",
    "R&D developer",
    "application developer",
    "web developer",
]

# Israel country-level geoId
_ISRAEL_GEO_ID = "101620260"

# City-level geoIds for targeted searches that surface local-only postings
_CITY_GEO_IDS = {
    "Tel Aviv":  "106920490",
    "Jerusalem": "104769292",
    "Herzliya":  "101715498",
    "Rehovot":   "100667795",
    "Ashdod":    "102545485",
}

# Time windows: 24h first (freshest), then 7d to catch stragglers
_TIME_WINDOWS = [
    ("24h", "r86400"),
    ("7d",  "r604800"),
]

_TARGET_LOCATIONS = {
    "tel aviv", "ramat gan", "petah tikva", "holon", "bat yam",
    "givatayim", "kiryat ono", "or yehuda", "airport city", "lod",
    "ramla", "rishon", "yahud", "bnei brak", "azur",
    "herzliya", "ra'anana", "raanana", "kfar saba", "hod hasharon",
    "tel mond", "even yehuda",
    "rehovot", "nes ziona", "yavne", "gedera", "rechovot",
    "jerusalem",
    "ashdod",
    "remote",
}

_EXCLUDE_LOCATIONS = {
    "yokneam", "haifa", "beer sheva", "be'er sheva", "netanya",
    "nahariya", "afula", "tiberias", "eilat", "karmiel", "acre", "akko",
    "nazareth", "rosh haayin", "modiin",
}

_INCLUDE_TERMS = {
    "software engineer", "software developer",
    "backend", "back-end", "back end",
    "full stack", "fullstack", "full-stack",
    "c# developer", "c# engineer",
    "python developer", "python engineer",
    "java developer", "java engineer",
    "r&d engineer", "r&d developer",
    "embedded software", "embedded engineer",
    "application engineer", "application developer",
    "server engineer", "server developer",
    "developer", "engineer",
}

_EXCLUDE_TERMS = {
    "frontend", "front-end", "front end",
    "ui developer", "ui engineer", "ux ", "ui/ux",
    "devops", "devsecops", "site reliability", "sre",
    "data engineer", "data scientist", "data analyst",
    "cloud engineer", "cloud architect",
    "ml engineer", "machine learning",
    "network engineer", "automation engineer",
    "qa engineer", "quality assurance",
    "security researcher", "security engineer",
    "hardware engineer", "electrical engineer", "mechanical engineer",
    "sales engineer", "solutions engineer", "field engineer",
}


def _is_target_location(location: str) -> bool:
    loc = location.lower().strip()
    if any(ex in loc for ex in _EXCLUDE_LOCATIONS):
        return False
    if any(term in loc for term in _TARGET_LOCATIONS):
        return True
    if loc.strip() in ("israel", "israel area", "israel region"):
        return True
    return False


def _is_relevant(title: str) -> bool:
    t = title.lower()
    if any(ex in t for ex in _EXCLUDE_TERMS):
        return False
    return any(inc in t for inc in _INCLUDE_TERMS)


# ══════════════════════════════════════════════════════════════════════════════
#  Guest API — HTTP helpers
# ══════════════════════════════════════════════════════════════════════════════
_GUEST_URL = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

_session: requests.Session | None = None


def _get_session() -> requests.Session:
    """Return a module-level requests.Session with rotated User-Agent."""
    import random
    global _session
    if _session is None:
        s = requests.Session()
        s.verify = False
        s.headers.update({
            "User-Agent": random.choice(_USER_AGENTS),
            "Accept-Language": "en-US,en;q=0.9",
        })
        _session = s
    return _session


def _parse_relative_date(text: str) -> datetime | None:
    now = datetime.now(timezone.utc)
    text = text.lower().strip()
    m = re.search(r'(\d+)\s+(second|minute|hour|day|week|month)', text)
    if not m:
        return None
    n, unit = int(m.group(1)), m.group(2)
    deltas = {
        "second": timedelta(seconds=n), "minute": timedelta(minutes=n),
        "hour": timedelta(hours=n), "day": timedelta(days=n),
        "week": timedelta(weeks=n), "month": timedelta(days=n * 30),
    }
    return now - deltas[unit]


def _fetch_job_detail(job_id: str) -> tuple[str, datetime | None]:
    """Fetch description + posted_at from the guest jobPosting page."""
    url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    try:
        session = _get_session()
        resp = session.get(url, timeout=12, verify=False)
        if resp.status_code != 200:
            return "", None
        soup = BeautifulSoup(resp.text, "lxml")
        posted_at = None
        time_el = soup.find("span", class_=re.compile(r"posted-time-ago__text"))
        if time_el:
            posted_at = _parse_relative_date(time_el.get_text(strip=True))
        desc_el = (soup.find("div", class_="description__text")
                   or soup.find("div", class_=re.compile(r"description")))
        description = desc_el.get_text(separator=" ", strip=True)[:800] if desc_el else ""
        return description, posted_at
    except Exception:
        return "", None


def _fetch_guest_page(
    keyword: str,
    start: int = 0,
    geo_id: str = _ISRAEL_GEO_ID,
    time_range: str = "r86400",
) -> List[BeautifulSoup]:
    """Fetch one page from the guest API."""
    params = {
        "keywords": keyword,
        "geoId": geo_id,
        "f_E": "2,3",          # Entry-level + Associate (0-5 yrs)
        "f_JT": "F",           # Full-time
        "sortBy": "DD",        # Most recent first
        "f_TPR": time_range,   # Time posted range
        "start": str(start),
    }
    url = _GUEST_URL + "?" + urlencode(params)
    backoff = 60
    session = _get_session()
    for _ in range(3):
        try:
            resp = session.get(url, timeout=15, verify=False)
            if resp.status_code == 429:
                logger.warning("[LinkedIn] 429 — sleeping %ds", backoff)
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            if resp.status_code in (401, 403):
                return []
            resp.raise_for_status()
            return BeautifulSoup(resp.text, "lxml").find_all("li")
        except requests.RequestException as exc:
            logger.debug("[LinkedIn] Request failed (kw=%s): %s", keyword, exc)
            return []
    return []


def _parse_card(card: BeautifulSoup) -> tuple | None:
    """Parse an <li> card → (job_id, title, company, location, url) or None."""
    base = card.find("div", {"data-entity-urn": True})
    if not base:
        return None
    job_id = base["data-entity-urn"].split(":")[-1]
    title_el = card.find("h3", class_="base-search-card__title")
    company_el = card.find("h4", class_="base-search-card__subtitle")
    loc_el = card.find("span", class_="job-search-card__location")
    link_el = card.find("a", class_="base-card__full-link")

    title = title_el.get_text(strip=True) if title_el else ""
    company = company_el.get_text(strip=True) if company_el else ""
    location = loc_el.get_text(strip=True) if loc_el else "Israel"
    url = (link_el["href"].split("?")[0] if link_el
           else f"https://www.linkedin.com/jobs/view/{job_id}")
    return job_id, title, company, location, url


# ══════════════════════════════════════════════════════════════════════════════
#  Internal helpers
# ══════════════════════════════════════════════════════════════════════════════

# Max pages per single (keyword, geo, window) combination
_MAX_PAGES = 10  # 10 × 25 = 250 cards max per combo


def _scrape_combo(
    keyword: str,
    geo_id: str,
    geo_label: str,
    time_range: str,
    time_label: str,
    results: List[Job],
    seen_ids: set,
) -> int:
    """Scrape one (keyword, geoId, timeWindow) combination with pagination.
    Returns number of new jobs found."""
    found = 0
    raw = 0
    for page in range(_MAX_PAGES):
        cards = _fetch_guest_page(keyword, start=page * 25, geo_id=geo_id, time_range=time_range)
        if not cards:
            break
        raw += len(cards)

        for card in cards:
            parsed = _parse_card(card)
            if not parsed:
                continue
            job_id, title, company, location, url = parsed

            if not title or not _is_relevant(title) or not _is_target_location(location):
                continue

            if job_id in seen_ids:
                continue
            seen_ids.add(job_id)

            if not db.is_new("linkedin", job_id):
                continue

            desc, posted_at = _fetch_job_detail(job_id)
            results.append(Job(
                source="linkedin", job_id=job_id, title=title,
                company=company, location=location, url=url,
                description_snippet=desc, posted_at=posted_at,
            ))
            found += 1

        if len(cards) < 25:
            break
        time.sleep(2)

    if raw > 0:
        logger.info("[LinkedIn] kw='%s' geo=%s window=%s: %d raw → %d new",
                    keyword, geo_label, time_label, raw, found)
    return found


# ══════════════════════════════════════════════════════════════════════════════
#  Public API
# ══════════════════════════════════════════════════════════════════════════════

def scrape_linkedin() -> List[Job]:
    """
    Multi-pass LinkedIn search:

    Pass 1 — All keywords × Israel-wide geoId × 24h window (deep pagination)
             This is the main sweep, sorted by most recent.

    Pass 2 — All keywords × Israel-wide geoId × 7d window
             Catches jobs missed in previous cycles (posted >24h ago but
             not seen yet — e.g. bot was down, or job appeared late).

    Pass 3 — Top keywords × city-specific geoIds × 24h window
             LinkedIn sometimes hides jobs under city geos that don't appear
             under the country geo. This catches Tel Aviv / Jerusalem / etc.
             specific postings.

    Deduplication via seen_ids set prevents sending the same job twice.
    """
    t_start = time.time()
    results: List[Job] = []
    seen_ids: set = set()
    total_new = 0

    # ── Pass 1: All keywords × Israel × 24h ──────────────────────────────────
    logger.info("[LinkedIn] Pass 1: %d keywords × Israel × 24h …", len(_KEYWORDS))
    for kw in _KEYWORDS:
        total_new += _scrape_combo(kw, _ISRAEL_GEO_ID, "Israel", "r86400", "24h",
                                   results, seen_ids)
        time.sleep(1)

    # ── Pass 2: All keywords × Israel × 7d ───────────────────────────────────
    logger.info("[LinkedIn] Pass 2: %d keywords × Israel × 7d …", len(_KEYWORDS))
    for kw in _KEYWORDS:
        total_new += _scrape_combo(kw, _ISRAEL_GEO_ID, "Israel", "r604800", "7d",
                                   results, seen_ids)
        time.sleep(1)

    # ── Pass 3: Top keywords × city geoIds × 24h ─────────────────────────────
    _CITY_KEYWORDS = [
        "software developer",
        "software engineer",
        "backend developer",
        "full stack developer",
    ]
    logger.info("[LinkedIn] Pass 3: %d keywords × %d cities × 24h …",
                len(_CITY_KEYWORDS), len(_CITY_GEO_IDS))
    for kw in _CITY_KEYWORDS:
        for city_name, city_geo in _CITY_GEO_IDS.items():
            total_new += _scrape_combo(kw, city_geo, city_name, "r86400", "24h",
                                       results, seen_ids)
            time.sleep(1)

    elapsed = time.time() - t_start
    logger.info("[LinkedIn] DONE: %d new unique jobs | %.1fs elapsed", total_new, elapsed)

    # Sort oldest-first so oldest unseen jobs are sent first
    _FALLBACK = datetime.max.replace(tzinfo=timezone.utc)

    def _sort_key(j: Job):
        dt = j.posted_at
        if dt is None:
            return _FALLBACK
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)

    results.sort(key=_sort_key)
    return results

