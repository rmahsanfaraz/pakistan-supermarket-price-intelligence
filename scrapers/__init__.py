"""
scrapers — one module per supermarket chain.

Public API:
    from scrapers import MetroScraper, ImtiazScraper, NaheedScraper, AlFatahScraper, ChaseUpScraper
"""

from scrapers.metro_scraper import MetroScraper
from scrapers.imtiaz_scraper import ImtiazScraper
from scrapers.naheed_scraper import NaheedScraper
from scrapers.alfatah_scraper import AlFatahScraper
from scrapers.chaseup_scraper import ChaseUpScraper

__all__ = ["MetroScraper", "ImtiazScraper", "NaheedScraper", "AlFatahScraper", "ChaseUpScraper"]
