"""
Global configuration for the Pakistani supermarket scraping pipeline.

All paths, timeouts, store URLs, selectors, and output preferences live here.
Every module in the project imports from this single source of truth.
"""

from pathlib import Path

# ─── Paths ───────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
MATCHED_DIR = DATA_DIR / "matched"
VALIDATION_DIR = DATA_DIR / "validation"
ANALYSIS_DIR = DATA_DIR / "analysis"
LOG_DIR = BASE_DIR / "logs"

for _d in (RAW_DIR, PROCESSED_DIR, MATCHED_DIR, VALIDATION_DIR, ANALYSIS_DIR, LOG_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ─── Scraping behaviour ─────────────────────────────────────────────────────
REQUEST_TIMEOUT = 30          # seconds per HTTP request
MAX_RETRIES = 5               # retry attempts per request
BACKOFF_BASE = 2              # exponential backoff multiplier (seconds)
BACKOFF_MAX = 60              # ceiling for backoff sleep
RATE_LIMIT_DELAY = 1.5        # min seconds between consecutive requests
SCRAPER_WORKERS = 4           # parallel category-scraping threads per store

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# ─── Store-specific settings ────────────────────────────────────────────────

METRO_CONFIG = {
    "store_name": "Metro",
    "base_url": "https://www.metro-online.pk",
    # Metro Online serves one national catalogue — each city is a separate
    # price-observation record (matching how the retailer ships nationwide).
    "cities": [
        "Karachi", "Lahore", "Islamabad", "Faisalabad", "Multan",
        "Rawalpindi", "Hyderabad", "Peshawar",
        "Quetta", "Gujranwala", "Sialkot", "Sukkur",
    ],
    "currency": "PKR",
    "api_base": "https://www.metro-online.pk",
    "categories_url": "https://www.metro-online.pk",
    "page_size": 48,
    "city_url_map": {
        "Karachi":     "https://www.metro-online.pk",
        "Lahore":      "https://www.metro-online.pk",
        "Islamabad":   "https://www.metro-online.pk",
        "Faisalabad":  "https://www.metro-online.pk",
        "Multan":      "https://www.metro-online.pk",
        "Rawalpindi":  "https://www.metro-online.pk",
        "Hyderabad":   "https://www.metro-online.pk",
        "Peshawar":    "https://www.metro-online.pk",
        "Quetta":      "https://www.metro-online.pk",
        "Gujranwala":  "https://www.metro-online.pk",
        "Sialkot":     "https://www.metro-online.pk",
        "Sukkur":      "https://www.metro-online.pk",
    },
}

IMTIAZ_CONFIG = {
    "store_name": "Imtiaz",
    "base_url": "https://shop.imtiaz.com.pk",
    "cities": ["Karachi", "Hyderabad", "Faisalabad"],
    "currency": "PKR",
    "categories_url": "https://shop.imtiaz.com.pk/api/menu-section",
    "page_size": 48,  # was 24 — doubled to halve the number of pagination requests
    "city_url_map": {
        "Karachi":    "https://shop.imtiaz.com.pk",
        "Hyderabad":  "https://shop.imtiaz.com.pk",
        "Faisalabad": "https://shop.imtiaz.com.pk",
    },
}

NAHEED_CONFIG = {
    "store_name": "Naheed",
    "base_url": "https://www.naheed.pk",
    # Naheed serves one national catalogue via Magento — each city is a
    # separate price-observation record for cross-city analysis.
    "cities": [
        "Karachi", "Lahore", "Islamabad", "Rawalpindi",
        "Faisalabad", "Peshawar", "Hyderabad", "Multan",
        "Quetta", "Gujranwala", "Sialkot", "Sukkur",
    ],
    "currency": "PKR",
    "categories_url": "https://www.naheed.pk/graphql",
    "page_size": 48,
    "city_url_map": {
        "Karachi":     "https://www.naheed.pk",
        "Lahore":      "https://www.naheed.pk",
        "Islamabad":   "https://www.naheed.pk",
        "Rawalpindi":  "https://www.naheed.pk",
        "Faisalabad":  "https://www.naheed.pk",
        "Peshawar":    "https://www.naheed.pk",
        "Hyderabad":   "https://www.naheed.pk",
        "Multan":      "https://www.naheed.pk",
        "Quetta":      "https://www.naheed.pk",
        "Gujranwala":  "https://www.naheed.pk",
        "Sialkot":     "https://www.naheed.pk",
        "Sukkur":      "https://www.naheed.pk",
    },
}

ALFATAH_CONFIG = {
    "store_name": "AlFatah",
    "base_url": "https://www.alfatah.com.pk",
    "cities": ["Lahore", "Islamabad"],
    "currency": "PKR",
    "categories_url": "https://www.alfatah.com.pk",
    "page_size": 24,
    "city_url_map": {
        "Lahore":    "https://www.alfatah.com.pk",
        "Islamabad": "https://www.alfatah.com.pk",
    },
}

CHASEUP_CONFIG = {
    "store_name": "ChaseUp",
    "base_url": "https://www.chaseupgrocery.com",
    "cities": ["Karachi"],
    "currency": "PKR",
    "categories_url": "https://www.chaseupgrocery.com/api/menu",
    "page_size": 48,
    "city_url_map": {
        "Karachi": "https://www.chaseupgrocery.com",
    },
}

# ─── Output format ──────────────────────────────────────────────────────────
OUTPUT_FORMAT = "csv"          # "csv" or "parquet"
CSV_ENCODING = "utf-8-sig"

# ─── Scale targets ───────────────────────────────────────────────────────────
# Set high enough that scrapers cycle through ALL cities before stopping.
# Naheed: ~36k products × 12 cities = ~432k rows; Metro: ~12k × 12 = ~144k rows.
TARGET_ROWS_PER_STORE = 500_000  # rows per chain before stopping

# ─── Matching thresholds ────────────────────────────────────────────────────
FUZZY_THRESHOLD = 88          # RapidFuzz token_sort_ratio cut-off
MIN_CROSS_STORE = 2           # a match group must span ≥ N stores
MIN_MATCHED_PRODUCTS = 10_000 # §5 constraint: ≥ 10,000 matched products
MIN_TOTAL_CLEAN_ROWS = 500_000  # §5 constraint: ≥ 500,000 cleaned rows

# ─── Canonical raw CSV schema ────────────────────────────────────────────────
# All scrapers MUST produce exactly these columns in this order.
# Missing fields are filled with None; extra fields are dropped.
RAW_COLUMNS = [
    "store_name",
    "city",
    "product_name",
    "brand",
    "category",
    "price",
    "original_price",
    "currency",
    "size",
    "product_url",
    "image_url",
    "in_stock",
    "scrape_timestamp",
]
