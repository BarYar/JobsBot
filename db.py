"""
db.py – SQLite-backed deduplication store.

Tracks which (source, job_id) pairs have already been sent to Telegram
so we never send the same job twice.
"""
import sqlite3
import pathlib
import threading
from typing import List

_DB_PATH = pathlib.Path(__file__).parent / "seen_jobs.db"
_db_lock = threading.Lock()


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS seen_jobs (
            source  TEXT NOT NULL,
            job_id  TEXT NOT NULL,
            seen_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (source, job_id)
        )
        """
    )
    conn.commit()
    return conn


def is_new(source: str, job_id: str) -> bool:
    """Return True if this (source, job_id) pair has NOT been seen before."""
    with _db_lock:
        with _get_conn() as conn:
            row = conn.execute(
                "SELECT 1 FROM seen_jobs WHERE source=? AND job_id=?",
                (source, job_id),
            ).fetchone()
            return row is None


def mark_seen(source: str, job_id: str) -> None:
    """Record that we have already notified about this job."""
    with _db_lock:
        with _get_conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO seen_jobs (source, job_id) VALUES (?, ?)",
                (source, job_id),
            )
            conn.commit()


