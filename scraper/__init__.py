# scraper/__init__.py
import sys
import pathlib
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))

from .linkedin import scrape_linkedin
from .companies import scrape_all_companies, _SCRAPERS

__all__ = ["scrape_linkedin", "scrape_all_companies", "_SCRAPERS"]
