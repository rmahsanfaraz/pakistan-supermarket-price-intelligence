"""
Abstract base scraper with retry logic, exponential backoff, rate limiting,
and structured logging.  All store-specific scrapers inherit from this class.
"""

import abc
import json
import logging
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

import config


# ─── Structured logging setup ───────────────────────────────────────────────

def build_logger(name: str, log_dir: Path = config.LOG_DIR) -> logging.Logger:
    """Create a logger that writes to both console and a per-scraper log file."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.DEBUG)

    fmt = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_dir / f"{name}.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ─── Rate-limiter ───────────────────────────────────────────────────────────

class RateLimiter:
    """Enforces a minimum delay between successive network calls."""

    def __init__(self, min_delay: float = config.RATE_LIMIT_DELAY):
        self._min_delay = min_delay
        self._last_call: float = 0.0

    def wait(self):
        elapsed = time.monotonic() - self._last_call
        if elapsed < self._min_delay:
            sleep_for = self._min_delay - elapsed + random.uniform(0.1, 0.5)
            time.sleep(sleep_for)
        self._last_call = time.monotonic()


# ─── Base scraper ────────────────────────────────────────────────────────────

class BaseScraper(abc.ABC):
    """
    Abstract base for all store scrapers.

    Subclasses must implement:
        - discover_categories()  → List of {name, url} dicts
        - scrape_category(url, name, city) → Generator of product dicts
        - parse_product(element, category_name, city) → dict | None
    """

    def __init__(self, store_config: Dict[str, Any]):
        self.store_name: str = store_config["store_name"]
        self.base_url: str = store_config["base_url"]
        self.cities: List[str] = store_config["cities"]
        self.currency: str = store_config["currency"]
        self.page_size: int = store_config.get("page_size", 24)
        self.config = store_config

        self.logger = build_logger(self.store_name.lower().replace("-", ""))
        self.rate_limiter = RateLimiter()

        self.session = requests.Session()
        self.session.headers.update(config.HEADERS)

        self._rows_collected: int = 0

        # Per-run scraping attempt counters (§5: all scraping attempts must be logged)
        self._attempts: int = 0
        self._successes: int = 0
        self._failures: int = 0
        self._categories_scraped: int = 0
        self._pw_fallbacks: int = 0

        # Threading support
        self._lock = threading.Lock()           # protects file I/O, counters, shared dicts
        self._thread_local = threading.local()  # per-thread session + rate-limiter

    # ── HTTP helpers with retry & back-off ───────────────────────────────

    @retry(
        retry=retry_if_exception_type((requests.RequestException, ConnectionError)),
        stop=stop_after_attempt(config.MAX_RETRIES),
        wait=wait_exponential(multiplier=config.BACKOFF_BASE, max=config.BACKOFF_MAX),
        reraise=True,
    )
    def _get(self, url: str, params: Optional[dict] = None) -> requests.Response:
        """HTTP GET with retry + exponential backoff + rate limiting (thread-safe)."""
        self._get_rate_limiter().wait()
        self._attempts += 1
        self.logger.debug("GET %s  params=%s", url, params)
        resp = self._get_session().get(url, params=params, timeout=config.REQUEST_TIMEOUT)
        resp.raise_for_status()
        self._successes += 1
        return resp

    def _get_session(self) -> requests.Session:
        """Return a thread-local Session initialised with this scraper's headers."""
        if not hasattr(self._thread_local, "session"):
            s = requests.Session()
            s.headers.update(dict(self.session.headers))
            self._thread_local.session = s
        return self._thread_local.session

    def _get_rate_limiter(self) -> "RateLimiter":
        """Return a thread-local RateLimiter so each worker thread rate-limits independently."""
        if not hasattr(self._thread_local, "rate_limiter"):
            self._thread_local.rate_limiter = RateLimiter()
        return self._thread_local.rate_limiter

    def _soup(self, url: str, params: Optional[dict] = None) -> BeautifulSoup:
        """Fetch a page and return a parsed BeautifulSoup tree."""
        resp = self._get(url, params=params)
        return BeautifulSoup(resp.text, "lxml")

    # ── Utility helpers ──────────────────────────────────────────────────

    @staticmethod
    def now_iso() -> str:
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    # ── Persistence ──────────────────────────────────────────────────────

    def _output_path(self) -> Path:
        slug = self.store_name.lower().replace("-", "")
        ext = "parquet" if config.OUTPUT_FORMAT == "parquet" else "csv"
        return config.RAW_DIR / f"{slug}_raw.{ext}"

    def _checkpoint_path(self) -> Path:
        slug = self.store_name.lower().replace("-", "")
        return config.RAW_DIR / f"{slug}_checkpoint.json"

    def _load_checkpoint(self):
        """Load saved progress. Returns (completed_keys: set, rows_collected: int)."""
        cp = self._checkpoint_path()
        if cp.exists():
            try:
                data = json.loads(cp.read_text(encoding="utf-8"))
                return set(data.get("completed", [])), int(data.get("rows_collected", 0))
            except Exception:
                pass
        return set(), 0

    def _save_checkpoint(self, completed: set, rows: int):
        """Persist completed (city::url) keys to disk (thread-safe)."""
        cp = self._checkpoint_path()
        with self._lock:
            cp.write_text(
                json.dumps({"completed": sorted(completed), "rows_collected": rows}),
                encoding="utf-8",
            )

    def _clear_checkpoint(self):
        """Remove checkpoint file after a fully successful run."""
        cp = self._checkpoint_path()
        if cp.exists():
            cp.unlink()

    def save_rows(self, rows: List[Dict[str, Any]]):
        """Append rows to the raw output file (thread-safe)."""
        if not rows:
            return
        df = pd.DataFrame(rows)

        # Enforce canonical schema: add missing columns (None), drop extras, fix order
        for col in config.RAW_COLUMNS:
            if col not in df.columns:
                df[col] = None
        df = df[config.RAW_COLUMNS]

        out = self._output_path()

        with self._lock:
            if config.OUTPUT_FORMAT == "parquet":
                if out.exists():
                    existing = pd.read_parquet(out)
                    df = pd.concat([existing, df], ignore_index=True)
                df.to_parquet(out, index=False, engine="pyarrow")
            else:
                write_header = not out.exists()
                df.to_csv(out, mode="a", index=False, header=write_header,
                           encoding=config.CSV_ENCODING)

            self._rows_collected += len(rows)
            self.logger.info(
                "Saved %d rows → %s  (total: %d)", len(rows), out.name, self._rows_collected,
            )

    # ── Abstract interface ───────────────────────────────────────────────

    @abc.abstractmethod
    def discover_categories(self) -> List[Dict[str, str]]:
        """Return [{name, url}, …] for every product category on the site."""

    @abc.abstractmethod
    def scrape_category(
        self, category_url: str, category_name: str, city: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield product dicts from a category, handling pagination."""

    @abc.abstractmethod
    def parse_product(
        self, element: Any, category_name: str, city: str
    ) -> Optional[Dict[str, Any]]:
        """Parse a single product element into a flat dict."""

    # ── Main run loop ────────────────────────────────────────────────────

    def run(self, target_rows: int = 0):
        """
        Scrape every city × category combination using a thread pool for
        concurrent category fetching within each city.
        If *target_rows* > 0, stop early once that threshold is met.

        When the primary requests-based scrape yields no products for a
        category, the method automatically falls back to Playwright (if
        the subclass provides ``scrape_category_playwright``).
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting scrape for %s", self.store_name)
        self.logger.info("Cities: %s  |  workers: %d", self.cities, config.SCRAPER_WORKERS)
        self.logger.info("=" * 60)

        out = self._output_path()

        # ── Resume support ────────────────────────────────────────────────
        completed_keys, saved_rows = self._load_checkpoint()
        resuming = bool(completed_keys) and out.exists()
        if resuming:
            self.logger.info(
                "RESUMING scrape for %s — %d categories already done, %d rows on disk.",
                self.store_name, len(completed_keys), saved_rows,
            )
            self._rows_collected = saved_rows
        else:
            completed_keys = set()
            if out.exists():
                out.unlink()
            self._rows_collected = 0

        categories = self.discover_categories()
        self.logger.info("Discovered %d categories", len(categories))

        # url → list[product_dict] — populated after city 1, reused as relabels for city 2+
        scraped_urls: dict = {}
        has_pw_fallback = hasattr(self, "scrape_category_playwright")
        stop_event = threading.Event()

        for city in self.cities:
            if stop_event.is_set():
                break
            self.logger.info("─── City: %s ───", city)
            city_base = self.config.get("city_url_map", {}).get(city, self.base_url)

            # ── Phase 1: instant relabels + checkpoint skips (main thread) ──
            cats_to_scrape: List[tuple] = []
            for cat in categories:
                cat_url = cat["url"]
                if not cat_url.startswith("http"):
                    cat_url = city_base.rstrip("/") + "/" + cat_url.lstrip("/")

                checkpoint_key = f"{city}::{cat_url}"
                if checkpoint_key in completed_keys:
                    self.logger.debug("    ↳ Checkpoint skip: %s", cat["name"])
                    continue

                if cat_url in scraped_urls:
                    self.logger.info("    ↳ Reusing cache: %s for %s", cat["name"], city)
                    relabelled = [{**p, "city": city} for p in scraped_urls[cat_url]]
                    if relabelled:
                        self.save_rows(relabelled)
                    completed_keys.add(checkpoint_key)
                    self._save_checkpoint(completed_keys, self._rows_collected)
                    if target_rows and self._rows_collected >= target_rows:
                        stop_event.set()
                        break
                else:
                    cats_to_scrape.append((cat, cat_url))

            if stop_event.is_set() or not cats_to_scrape:
                continue

            # ── Phase 2: parallel scrape of new categories ────────────────
            self.logger.info(
                "Scraping %d categories with %d threads …",
                len(cats_to_scrape), config.SCRAPER_WORKERS,
            )

            def _worker(cat, cat_url):
                if stop_event.is_set():
                    return
                self.logger.info("  [thread] %s", cat["name"])
                batch: List[Dict[str, Any]] = []
                cache: List[Dict[str, Any]] = []

                # Primary scrape
                try:
                    for product in self.scrape_category(cat_url, cat["name"], city):
                        batch.append(product)
                        cache.append(product)
                        if len(batch) >= 500:
                            self.save_rows(batch)
                            batch = []
                        if stop_event.is_set():
                            break
                except Exception:
                    self.logger.exception(
                        "Error scraping %s in %s", cat["name"], city
                    )
                    with self._lock:
                        self._failures += 1
                finally:
                    if batch:
                        self.save_rows(batch)
                        batch = []

                # Playwright fallback if primary returned nothing
                if not cache and has_pw_fallback and not stop_event.is_set():
                    self.logger.info(
                        "    ↳ No results via requests; trying Playwright for %s",
                        cat["name"],
                    )
                    with self._lock:
                        self._pw_fallbacks += 1
                    try:
                        for product in self.scrape_category_playwright(
                            cat_url, cat["name"], city
                        ):
                            batch.append(product)
                            cache.append(product)
                            if len(batch) >= 500:
                                self.save_rows(batch)
                                batch = []
                            if stop_event.is_set():
                                break
                    except Exception:
                        self.logger.exception(
                            "Playwright fallback failed for %s in %s", cat["name"], city
                        )
                        with self._lock:
                            self._failures += 1
                    finally:
                        if batch:
                            self.save_rows(batch)

                # Commit result under lock
                with self._lock:
                    scraped_urls[cat_url] = cache
                    self._categories_scraped += 1
                    completed_keys.add(f"{city}::{cat_url}")
                self._save_checkpoint(completed_keys, self._rows_collected)

                if target_rows and self._rows_collected >= target_rows:
                    stop_event.set()

            with ThreadPoolExecutor(
                max_workers=config.SCRAPER_WORKERS,
                thread_name_prefix=self.store_name,
            ) as pool:
                futures = {
                    pool.submit(_worker, cat, cat_url): cat["name"]
                    for cat, cat_url in cats_to_scrape
                }
                for f in as_completed(futures):
                    try:
                        f.result()
                    except Exception:
                        self.logger.exception("Worker raised for: %s", futures[f])
                    if stop_event.is_set():
                        for pending in futures:
                            pending.cancel()
                        break

        self._clear_checkpoint()
        self.logger.info(
            "Scrape complete for %s — %d total rows.", self.store_name, self._rows_collected
        )
        # §5: Log all scraping attempt statistics
        self.logger.info("─── Scraping Attempt Summary (%s) ───", self.store_name)
        self.logger.info("  HTTP requests attempted:  %d", self._attempts)
        self.logger.info("  HTTP requests succeeded:  %d", self._successes)
        self.logger.info("  Failures (errors):        %d", self._failures)
        self.logger.info("  Categories scraped:       %d", self._categories_scraped)
        self.logger.info("  Playwright fallbacks:     %d", self._pw_fallbacks)
        self.logger.info("  Rows collected:           %d", self._rows_collected)
        self.logger.info("─" * 40)

    @property
    def total_rows(self) -> int:
        return self._rows_collected
