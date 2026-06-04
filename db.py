"""
db.py – DynamoDB-backed deduplication store.

Tracks which (source, job_id) pairs have already been sent to Telegram
so we never send the same job twice.

Table schema:
  PK: job_key  (String)  →  "{source}#{job_id}"
"""
import os
from datetime import datetime, timezone

import boto3

_TABLE_NAME = os.getenv("DYNAMODB_TABLE", "jobsbot-seen-jobs")
_table = boto3.resource("dynamodb").Table(_TABLE_NAME)


def is_new(source: str, job_id: str) -> bool:
    """Return True if this (source, job_id) pair has NOT been seen before."""
    resp = _table.get_item(Key={"job_key": f"{source}#{job_id}"})
    return "Item" not in resp


def mark_seen(source: str, job_id: str) -> None:
    """Record that we have already notified about this job."""
    _table.put_item(Item={
        "job_key": f"{source}#{job_id}",
        "seen_at": datetime.now(timezone.utc).isoformat(),
    })
