"""
models.py – shared dataclass for a scraped job posting.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Job:
    source: str          # e.g. "linkedin", "google", "monday"
    job_id: str          # unique within the source
    title: str
    company: str
    location: str
    url: str
    description_snippet: str = ""
    experience_level: str = ""
    employment_type: str = ""
    posted_at: Optional[datetime] = None

