"""
config.py – central place for all settings loaded from .env
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(
            f"Missing required environment variable: {key}\n"
            f"Copy .env.example → .env and fill in the values."
        )
    return val


# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = _require("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID: str = _require("TELEGRAM_CHAT_ID")

# ── Tuning ────────────────────────────────────────────────────────────────────
MAX_NEW_JOBS_PER_SOURCE: int = int(os.getenv("MAX_NEW_JOBS_PER_SOURCE", "50"))


