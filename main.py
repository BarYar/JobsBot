"""
main.py – entry point for JobsBot.
"""
import logging
import random
import time
from typing import List

import schedule

import db
import notifier
from models import Job
from scraper.linkedin import scrape_linkedin
from scraper.companies import _SCRAPERS

_log_fmt = "%(asctime)s  %(levelname)-8s  %(name)s – %(message)s"
_log_datefmt = "%Y-%m-%d %H:%M:%S"


class _FlushingFileHandler(logging.FileHandler):
    """FileHandler that flushes + os.fsync after every emit."""
    import os as _os

    def emit(self, record):
        super().emit(record)
        self.flush()
        if hasattr(self.stream, "fileno"):
            try:
                self._os.fsync(self.stream.fileno())
            except Exception:
                pass


logging.basicConfig(
    level=logging.INFO,
    format=_log_fmt,
    datefmt=_log_datefmt,
)

# Add file handler that flushes immediately after each log line
_fh = _FlushingFileHandler("jobsbot.log", mode="a", encoding="utf-8")
_fh.setLevel(logging.INFO)
_fh.setFormatter(logging.Formatter(_log_fmt, datefmt=_log_datefmt))
logging.getLogger().addHandler(_fh)
logger = logging.getLogger("main")

# ── Cycle counter — companies run every 2nd LinkedIn cycle ───────────────────
_cycle_count = 0


def _notify_jobs(jobs: List[Job], label: str) -> int:
    """Send Telegram message for each new job immediately, then mark seen."""
    sent = 0
    filtered = 0
    for job in jobs:
        if not db.is_new(job.source, job.job_id):
            continue
        sent_ok = notifier.send_job(job)
        db.mark_seen(job.source, job.job_id)
        if sent_ok:
            sent += 1
        else:
            filtered += 1
    if sent or filtered:
        logger.info("[%s] %d sent, %d filtered (>4 yrs).", label, sent, filtered)
    return sent


def run_pipeline() -> None:
    global _cycle_count
    _cycle_count += 1
    logger.info("━━━━  Pipeline run #%d starting  ━━━━", _cycle_count)
    total = 0

    # ── LinkedIn general keyword search (every cycle) ─────────────────────────
    try:
        li_jobs = scrape_linkedin()
        logger.info("[LinkedIn] %d jobs to send to Telegram.", len(li_jobs))
        total += _notify_jobs(li_jobs, "LinkedIn")
    except Exception as exc:
        logger.error("LinkedIn scraper failed: %s", exc)
        notifier.send_alert(f"LinkedIn scraper failed: {exc}")

    # ── Company career-page scrapers (every 2nd cycle) ────────────────────────
    if _cycle_count % 2 == 0:
        logger.info("Cycle #%d — running company scrapers …", _cycle_count)
        for name, fn in _SCRAPERS:
            try:
                logger.info("Scraping %s …", name)
                jobs = fn()
                new_jobs = [j for j in jobs if db.is_new(j.source, j.job_id)]
                total += _notify_jobs(new_jobs, name)
            except Exception as exc:
                logger.error("[%s] scraper failed: %s", name, exc)
            time.sleep(2)
    else:
        logger.info("Cycle #%d — skipping company scrapers (runs every 2nd cycle).", _cycle_count)

    logger.info("━━━━  Pipeline done — %d total new jobs sent  ━━━━", total)


# ── Scheduler — fix: cancel old job before adding new one ────────────────────
_scheduled_job = None


def _schedule_with_jitter() -> None:
    global _scheduled_job
    base   = 23  # ~23 minutes between runs
    jitter = random.uniform(-2, 2)
    nxt    = max(5, base + jitter)
    if _scheduled_job is not None:
        try:
            schedule.cancel_job(_scheduled_job)
        except Exception:
            pass
    _scheduled_job = schedule.every(nxt).minutes.do(_jitter_run)
    logger.info("Next run in %.1f minutes.", nxt)


def _jitter_run() -> None:
    run_pipeline()
    _schedule_with_jitter()


if __name__ == "__main__":
    logger.info("🤖  JobsBot starting up …")

    notifier.send_startup_message()
    run_pipeline()
    _schedule_with_jitter()
    logger.info("Scheduler running. Press Ctrl+C to stop.")
    while True:
        schedule.run_pending()
        time.sleep(10)
