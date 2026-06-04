"""
config.py – all settings, loaded from environment variables.

Filtering can be customised via env vars (comma-separated strings).
Set them in AWS Lambda → Configuration → Environment variables.
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(f"Missing required environment variable: {key}")
    return val


def _csv_list(key: str, default: list[str]) -> list[str]:
    val = os.getenv(key, "")
    if val.strip():
        return [v.strip().lower() for v in val.split(",") if v.strip()]
    return default


def _csv_set(key: str, default: list[str]) -> set[str]:
    return set(_csv_list(key, default))


# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = _require("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: str = _require("TELEGRAM_CHAT_ID")

# ── AWS ───────────────────────────────────────────────────────────────────────
DYNAMODB_TABLE: str = os.getenv("DYNAMODB_TABLE", "jobsbot-seen-jobs")

# ── Experience filter ─────────────────────────────────────────────────────────
# Jobs requiring more than this many years will be silently dropped.
MAX_EXPERIENCE_YEARS: int = int(os.getenv("MAX_EXPERIENCE_YEARS", "4"))

# ── LinkedIn search keywords ──────────────────────────────────────────────────
# What to search for on LinkedIn. Each keyword is a separate API call.
LINKEDIN_SEARCH_KEYWORDS: list[str] = _csv_list("LINKEDIN_SEARCH_KEYWORDS", [
    "software developer",
    "software engineer",
    "backend developer",
    "backend engineer",
    "full stack developer",
    "full stack engineer",
    "C# developer",
    ".NET developer",
    "java developer",
    "python developer",
    "R&D engineer",
    "R&D developer",
    "application developer",
    "web developer",
])

# ── Job title filters (applied to both LinkedIn and company scrapers) ─────────
# A job must match at least one INCLUDE term and zero EXCLUDE terms.
INCLUDE_TITLE_TERMS: set[str] = _csv_set("INCLUDE_TITLE_TERMS", [
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
])

EXCLUDE_TITLE_TERMS: set[str] = _csv_set("EXCLUDE_TITLE_TERMS", [
    "frontend", "front-end", "front end",
    "ui developer", "ui engineer", "ux", "ui/ux",
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
])

# ── Location filters ──────────────────────────────────────────────────────────
TARGET_LOCATIONS: set[str] = _csv_set("TARGET_LOCATIONS", [
    "tel aviv", "ramat gan", "petah tikva", "holon", "bat yam",
    "givatayim", "kiryat ono", "or yehuda", "airport city", "lod",
    "ramla", "rishon", "yahud", "bnei brak", "azur",
    "herzliya", "ra'anana", "raanana", "kfar saba", "hod hasharon",
    "tel mond", "even yehuda",
    "rehovot", "nes ziona", "yavne", "gedera", "rechovot",
    "jerusalem", "ashdod", "remote",
])

EXCLUDE_LOCATIONS: set[str] = _csv_set("EXCLUDE_LOCATIONS", [
    "yokneam", "haifa", "beer sheva", "be'er sheva", "netanya",
    "nahariya", "afula", "tiberias", "eilat", "karmiel", "acre", "akko",
    "nazareth", "rosh haayin", "modiin",
])
