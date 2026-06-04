"""
main.py – Lambda entry point for JobsBot.
"""
import logging
import time
from typing import List

import db
import notifier
from models import Job
from scraper.linkedin import scrape_linkedin
from scraper.companies import _SCRAPERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s – %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


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
    logger.info("━━━━  Pipeline starting  ━━━━")
    total = 0

    try:
        li_jobs = scrape_linkedin()
        logger.info("[LinkedIn] %d jobs to send to Telegram.", len(li_jobs))
        total += _notify_jobs(li_jobs, "LinkedIn")
    except Exception as exc:
        logger.error("LinkedIn scraper failed: %s", exc)
        notifier.send_alert(f"LinkedIn scraper failed: {exc}")

    for name, fn in _SCRAPERS:
        try:
            logger.info("Scraping %s …", name)
            jobs = fn()
            new_jobs = [j for j in jobs if db.is_new(j.source, j.job_id)]
            total += _notify_jobs(new_jobs, name)
        except Exception as exc:
            logger.error("[%s] scraper failed: %s", name, exc)
        time.sleep(2)

    logger.info("━━━━  Pipeline done — %d total new jobs sent  ━━━━", total)


def lambda_handler(event, context):
    run_pipeline()
    return {"statusCode": 200}


if __name__ == "__main__":
    notifier.send_startup_message()
    run_pipeline()
