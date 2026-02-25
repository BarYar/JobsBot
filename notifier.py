"""
notifier.py – sends Telegram messages for new job postings.
"""
import asyncio
import html
import logging
import re
from datetime import datetime, timezone
from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
from telegram.request import HTTPXRequest

import config
from models import Job

logger = logging.getLogger(__name__)

# ── One persistent event loop ─────────────────────────────────────────────────
_LOOP = asyncio.new_event_loop()
asyncio.set_event_loop(_LOOP)

# ── One persistent bot instance ───────────────────────────────────────────────
_REQUEST = HTTPXRequest(
    connection_pool_size=8,
    http_version="1.1",
    httpx_kwargs={"verify": False},
)
_BOT = Bot(token=config.TELEGRAM_BOT_TOKEN, request=_REQUEST)
_LOOP.run_until_complete(_BOT.initialize())


def _get_bot() -> Bot:
    return _BOT


# Source → emoji mapping
_SOURCE_EMOJI: dict[str, str] = {
    "linkedin":     "💼",
    "google":       "🔍",
    "amazon":       "📦",
    "microsoft":    "🪟",
    "WixCom":       "🎨",
    "wiz-security": "🛡️",
    "mondaydotcom": "📅",
    "mobileye":     "🚗",
    "IsraelAerospaceIndustries": "✈️",
    "shabak":       "🔐",
    "CheckPointSoftwareTechnologiesLtd": "🔒",
    "intel":        "💻",
    "nvidia":       "🎮",
    "CyberArk":     "🔑",
    "Amdocs":       "📡",
    "fiverr":       "💚",
    "unity3d":      "🎯",
    "meta":         "🌐",
    "apple":        "🍎",
    "Radware":      "🌊",
    "NICE":         "📞",
    "Kaltura":      "🎬",
    "Imperva":      "🛡",
}

# Terms to bold in the title (HTML <b> tags)
_BOLD_TERMS = [
    "C#", "Python", "Backend", "Back-End", "Back End",
    "Full Stack", "FullStack", "Full-Stack",
]


def _e(text: str) -> str:
    """HTML-escape a plain string so it's safe inside Telegram HTML messages."""
    return html.escape(str(text), quote=False)


# ── Tech stack detection ───────────────────────────────────────────────────────
_TECH_PATTERNS = [
    # Languages / runtimes
    (r"\bC#\b",             "C#"),
    (r"\b\.NET\b",          ".NET"),
    (r"\bPython\b",         "Python"),
    (r"\bJava\b(?!Script)", "Java"),
    (r"\bJavaScript\b|JS\b","JavaScript"),
    (r"\bTypeScript\b",     "TypeScript"),
    (r"\bNode\.?js\b",      "Node.js"),
    (r"\bGo\b|Golang\b",    "Go"),
    (r"\bRust\b",           "Rust"),
    (r"\bC\+\+\b",          "C++"),
    (r"\bRuby\b",           "Ruby"),
    (r"\bPHP\b",            "PHP"),
    (r"\bScala\b",          "Scala"),
    (r"\bKotlin\b",         "Kotlin"),
    (r"\bSwift\b",          "Swift"),
    # Frameworks
    (r"\bReact\b",          "React"),
    (r"\bAngular\b",        "Angular"),
    (r"\bVue\b",            "Vue"),
    (r"\bSpring\b",         "Spring"),
    (r"\bDjango\b",         "Django"),
    (r"\bFastAPI\b",        "FastAPI"),
    (r"\bFlask\b",          "Flask"),
    (r"\bASP\.NET\b",       "ASP.NET"),
    # DBs / infra keywords relevant to dev roles
    (r"\bPostgreSQL\b|Postgres\b", "PostgreSQL"),
    (r"\bMySQL\b",          "MySQL"),
    (r"\bMongoDB\b",        "MongoDB"),
    (r"\bRedis\b",          "Redis"),
    (r"\bkubernetes\b|k8s\b", "Kubernetes", re.IGNORECASE),
    (r"\bDocker\b",         "Docker"),
    (r"\bAWS\b",            "AWS"),
    (r"\bAzure\b",          "Azure"),
    (r"\bGCP\b",            "GCP"),
]

# ── Experience year extraction ─────────────────────────────────────────────────
# Matches: "3+ years", "2-4 years", "0-3 years experience", "at least 2 years" etc.
_EXP_RE = re.compile(
    r"(\d+)\s*[-–]\s*(\d+)\s*\+?\s*years?|"   # "2-4 years"
    r"(\d+)\s*\+\s*years?|"                    # "3+ years"
    r"at\s+least\s+(\d+)\s*years?|"            # "at least 2 years"
    r"minimum\s+(\d+)\s*years?|"               # "minimum 2 years"
    r"(\d+)\s*years?\s+(?:of\s+)?experience",  # "3 years experience"
    re.IGNORECASE,
)


def _detect_tech(text: str) -> list[str]:
    """Return list of detected tech stacks found in text."""
    found = []
    seen  = set()
    for entry in _TECH_PATTERNS:
        pattern, label = entry[0], entry[1]
        flags = entry[2] if len(entry) > 2 else 0
        if re.search(pattern, text, flags):
            if label not in seen:
                seen.add(label)
                found.append(label)
    return found


def _detect_years(title: str, description: str = "") -> str | None:
    """Return a human-readable experience requirement string, or None.
    Seniority keywords are only checked in the TITLE to avoid false blocks
    from phrases like 'work alongside senior engineers' in descriptions.
    """
    # Explicit year ranges from description or title (most reliable)
    combined = title + " " + description
    m = _EXP_RE.search(combined)
    if m:
        groups = m.groups()
        if groups[0] and groups[1]:
            return f"{groups[0]}-{groups[1]} yrs"
        if groups[2]:
            return f"{groups[2]}+ yrs"
        if groups[3]:
            return f"{groups[3]}+ yrs"
        if groups[4]:
            return f"{groups[4]}+ yrs"
        if groups[5]:
            return f"{groups[5]}+ yrs"

    # Seniority heuristic — title ONLY (not description)
    t = title.lower()
    if re.search(r"\bjunior\b|\bentry.?level\b|\bjr\.?\b", t):
        return "0-2 yrs (Junior)"
    if re.search(r"\bmid.?level\b|\bintermediate\b", t):
        return "2-5 yrs (Mid)"
    if re.search(r"\bsenior\b|\bstaff\b|\blead\b|\bprincipal\b|\b(ii|iii|iv|v)\b", t):
        return "5+ yrs (Senior)"
    return None


def _is_csharp_junior(tech: list[str], years_str: str | None) -> bool:
    """True if this is a C# job requiring 0-4 years experience."""
    if "C#" not in tech and ".NET" not in tech:
        return False
    if years_str is None:
        return True   # no years mentioned → assume junior-friendly
    # Parse the lower bound
    m = re.match(r"(\d+)", years_str)
    if m and int(m.group(1)) <= 4:
        return True
    return False


def _exceeds_4_years(years_str: str | None) -> bool:
    """Return True if the detected experience requirement is clearly > 4 years.
    When unknown/unspecified we let it through (return False)."""
    if years_str is None:
        return False
    # "5+ yrs (Senior)", "5+ yrs", "6-10 yrs" → skip
    # "0-2 yrs (Junior)", "2-5 yrs (Mid)", "3+ yrs", "4-6 yrs" → check lower bound
    if "Senior" in years_str or "senior" in years_str:
        return True
    m = re.match(r"(\d+)", years_str)
    if m and int(m.group(1)) >= 5:
        return True
    return False


def _posted_label(posted_at: datetime | None) -> str | None:
    """Return 'DD MMM YYYY · X ago' — absolute date + relative time.
    posted_at is already accurate (derived from LinkedIn's own 'X hours ago' text).
    """
    if posted_at is None:
        return None
    now = datetime.now(timezone.utc)
    dt  = posted_at if posted_at.tzinfo else posted_at.replace(tzinfo=timezone.utc)

    # Absolute date (Windows-safe — strip leading zero from day manually)
    date_str = dt.strftime("%d %b %Y").lstrip("0")

    # Relative part
    diff = now - dt
    total_minutes = max(0, int(diff.total_seconds() / 60))
    if total_minutes < 60:
        rel = f"{total_minutes}m ago" if total_minutes > 0 else "just now"
    elif total_minutes < 1440:
        rel = f"{total_minutes // 60}h ago"
    elif total_minutes < 10080:
        rel = f"{total_minutes // 1440}d ago"
    else:
        rel = f"{total_minutes // 10080}w ago"

    return f"{date_str} · {rel}"


def _highlight_title(title: str) -> str:
    """Wrap known tech keywords in <b> tags (HTML mode)."""
    safe = _e(title)
    for term in _BOLD_TERMS:
        safe = re.sub(
            re.escape(term),
            lambda m: f"<b>{m.group(0)}</b>",
            safe,
            flags=re.IGNORECASE,
        )
    return safe


def _format_job(job: Job) -> str | None:
    """Build an HTML-formatted Telegram message for one job.
    Returns None if the job should be skipped (e.g. requires > 4 years)."""
    emoji    = _SOURCE_EMOJI.get(job.source, "🏢")
    title    = _highlight_title(job.title)
    company  = _e(job.company)
    location = _e(job.location)
    url      = job.url or "https://www.linkedin.com/jobs/"

    # Detect tech stack and experience from title + description
    search_text = job.title + " " + job.description_snippet
    tech  = _detect_tech(search_text)
    years = _detect_years(job.title, job.description_snippet)

    # Skip jobs that clearly require more than 4 years
    if _exceeds_4_years(years):
        logger.debug("[Telegram] Skipping '%s' — over-experience: %s", job.title, years)
        return None

    # Special highlight for C# / .NET junior roles (0-4 yrs)
    csharp_junior = _is_csharp_junior(tech, years)
    header_prefix = "🔥 " if csharp_junior else ""

    lines = [
        f"{header_prefix}{emoji} <b>{title}</b>",
        f"🏢 {company}",
        f"📍 {location}",
    ]

    # When posted
    ago = _posted_label(job.posted_at)
    if ago:
        lines.append(f"🕐 {ago}")

    # Tech stack line
    if tech:
        lines.append("💻 " + " · ".join(tech))

    # Experience line
    if years:
        lines.append(f"📅 {years} experience")
    elif job.experience_level:
        lines.append(f"📅 {_e(job.experience_level)}")

    if csharp_junior:
        lines.append("⭐ <b>C# Junior-friendly!</b>")

    lines.append(f'🔗 <a href="{url}">Apply here</a>')
    return "\n".join(lines)


async def _send_async(text: str) -> None:
    await _get_bot().send_message(
        chat_id=config.TELEGRAM_CHAT_ID,
        text=text,
        parse_mode=ParseMode.HTML,          # HTML is far more reliable than Markdown
        disable_web_page_preview=True,      # don't expand link previews — keeps feed clean
    )


def _send(text: str) -> None:
    _LOOP.run_until_complete(_send_async(text))


def send_job(job: Job) -> bool:
    """Send a job notification. Returns False if the job was filtered out."""
    text = _format_job(job)
    if text is None:
        return False   # filtered (e.g. over-experience)
    try:
        _send(text)
        logger.info("[Telegram] ✓ %s @ %s", job.title, job.company)
        return True
    except TelegramError as exc:
        logger.error("[Telegram] Failed to send job %s: %s", job.job_id, exc)
        return False


def send_alert(message: str) -> None:
    try:
        _send(f"⚠️ <b>JobsBot Alert</b>\n{_e(message)}")
    except TelegramError as exc:
        logger.error("[Telegram] Failed to send alert: %s", exc)


def send_startup_message() -> None:
    try:
        _send("🤖 <b>JobsBot started!</b>\nSending Software / Backend / Full Stack jobs in Israel every ~23 min. Good luck! 🍀")
    except TelegramError as exc:
        logger.error("[Telegram] Failed to send startup message: %s", exc)
