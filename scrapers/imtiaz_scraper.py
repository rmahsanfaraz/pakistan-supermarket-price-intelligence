"""
Imtiaz Super Market (shop.imtiaz.com.pk) scraper.

Imtiaz is a major Sindh-based chain with stores in Karachi and Hyderabad.
Their site at shop.imtiaz.com.pk runs on the BlinkCo.io platform (Next.js)
and exposes a REST API.  This scraper fetches products directly from the
JSON API, bypassing the JS-rendered frontend entirely.
"""

import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, Generator, List, Optional

import config
from scrapers.base_scraper import BaseScraper, RateLimiter
from scrapers.helpers import extract_brand, extract_size


class ImtiazScraper(BaseScraper):
    """Scraper for https://shop.imtiaz.com.pk via BlinkCo REST API."""

    API_BASE = "https://shop.imtiaz.com.pk/api"
    REST_ID = "55126"

    # Branch IDs per city – each branch carries a slightly different catalogue.
    BRANCH_MAP = {
        "Karachi":    "54943",   # Clifton branch
        "Hyderabad":  "54934",   # Korangi Warehouse branch (different catalogue)
        "Faisalabad": "54940",   # Global branch (third distinct catalogue)
    }
    DEFAULT_BRANCH_ID = "54943"

    # Custom headers required by the BlinkCo platform
    _API_HEADERS = {
        "app-name": "imtiazsuperstore",
        "rest-id": "55126",
    }

    # Imtiaz exposes a REST/JSON API — safe to poll faster than HTML scrapers
    _API_RATE_LIMIT_DELAY = 0.5  # seconds between requests per thread

    def __init__(self):
        super().__init__(config.IMTIAZ_CONFIG)
        self.session.headers.update(self._API_HEADERS)
        # Active branch id – updated per city in run()
        self._active_branch: str = self.DEFAULT_BRANCH_ID

    def _get_rate_limiter(self) -> RateLimiter:
        """Use a faster per-thread rate-limiter suited for a REST API."""
        if not hasattr(self._thread_local, "rate_limiter"):
            self._thread_local.rate_limiter = RateLimiter(
                min_delay=self._API_RATE_LIMIT_DELAY
            )
        return self._thread_local.rate_limiter

    # ── API helpers ──────────────────────────────────────────────────────

    def _api_params(self, **extra) -> dict:
        """Return base query parameters for Imtiaz API calls."""
        params = {
            "restId": self.REST_ID,
            "rest_brId": self._active_branch,
            "delivery_type": "0",
            "source": "",
        }
        params.update(extra)
        return params

    # ── Category discovery (via API) ─────────────────────────────────────

    def discover_categories(self) -> List[Dict[str, str]]:
        """Fetch menu sections and their sub-sections from the API.

        Returns a flat list where each entry represents a sub-section
        (the level at which products are listed).  The ``url`` field
        stores the dish_sub_section id for use in scrape_category.
        """
        categories: List[Dict[str, str]] = []
        try:
            resp = self._get(
                f"{self.API_BASE}/menu-section",
                params=self._api_params(),
            )
            sections = resp.json().get("data", [])
            for section in sections:
                section_name = (section.get("name") or "").strip()
                if not section_name:
                    continue

                # menu-section response includes inner 'section' list
                inner_subs = section.get("section", [])
                for inner in inner_subs:
                    inner_id = inner.get("id")
                    inner_name = (inner.get("name") or "").strip()
                    if not inner_id:
                        continue

                    # Fetch dish_sub_sections for this subsection
                    try:
                        sub_resp = self._get(
                            f"{self.API_BASE}/sub-section",
                            params=self._api_params(sectionId=str(inner_id)),
                        )
                        sub_data = sub_resp.json().get("data", [])
                    except Exception:
                        self.logger.warning(
                            "Failed to fetch sub-sections for %s > %s",
                            section_name, inner_name,
                        )
                        continue

                    for sub_group in sub_data:
                        dish_subs = sub_group.get("dish_sub_sections", [])
                        if dish_subs:
                            for ds in dish_subs:
                                ds_id = ds.get("id")
                                ds_name = (ds.get("name") or "").strip()
                                if ds_id and ds_name:
                                    categories.append({
                                        "name": f"{section_name} > {inner_name} > {ds_name}",
                                        "url": str(ds_id),
                                    })
                        else:
                            sg_id = sub_group.get("id")
                            sg_name = (sub_group.get("name") or "").strip()
                            if sg_id and sg_name:
                                categories.append({
                                    "name": f"{section_name} > {sg_name}",
                                    "url": str(sg_id),
                                })
        except Exception:
            self.logger.warning("API category discovery failed.", exc_info=True)
            self._failures += 1

        self.logger.info("Discovered %d categories via API.", len(categories))
        return categories

    # ── Product parsing (from API JSON) ──────────────────────────────────

    def parse_product(
        self, item: Any, category_name: str, city: str
    ) -> Optional[Dict[str, Any]]:
        """Convert an API product JSON object into our standard dict."""
        if not isinstance(item, dict):
            return None

        product_name = (item.get("name") or "").strip()
        if not product_name:
            return None

        price = item.get("price")
        try:
            price = float(price) if price is not None else None
        except (ValueError, TypeError):
            price = None

        discount_price = item.get("discount_price")
        try:
            discount_price = float(discount_price) if discount_price else None
        except (ValueError, TypeError):
            discount_price = None

        brand = item.get("brand_name") or extract_brand(product_name)
        image_url = item.get("img_url") or ""
        # BlinkCo API returns slug=None for most products; prefer numeric id,
        # then fall back to a slugified name (both URL formats return 200).
        slug = item.get("slug") or ""
        prod_id = item.get("id")
        if slug:
            product_url = f"https://shop.imtiaz.com.pk/product/{slug}"
        elif prod_id:
            product_url = f"https://shop.imtiaz.com.pk/product/{prod_id}"
        else:
            import re as _re
            name_slug = _re.sub(r"[^a-z0-9]+", "-", product_name.lower()).strip("-")
            product_url = f"https://shop.imtiaz.com.pk/product/{name_slug}" if name_slug else ""

        return {
            "store_name": self.store_name,
            "city": city,
            "product_name": product_name,
            "brand": brand,
            "category": category_name,
            "price": discount_price if discount_price else price,
            "original_price": price if discount_price else None,
            "currency": self.currency,
            "size": extract_size(product_name),
            "product_url": product_url,
            "image_url": image_url,
            "in_stock": bool(item.get("availability", 1)),
            "scrape_timestamp": self.now_iso(),
        }

    # ── Paginated category scraper (API) ─────────────────────────────────

    def scrape_category(
        self, category_url: str, category_name: str, city: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Fetch products for a sub-section via the API with pagination.

        ``category_url`` holds the sub_section_id (set by discover_categories).
        """
        # The base class prepends the city URL, so extract the trailing ID
        sub_section_id = category_url.rstrip("/").rsplit("/", 1)[-1]
        page_no = 1
        per_page = self.page_size

        while True:
            try:
                resp = self._get(
                    f"{self.API_BASE}/items-by-subsection",
                    params=self._api_params(
                        sub_section_id=sub_section_id,
                        brand_name="",
                        min_price="0",
                        max_price="",
                        sort_by="",
                        sort="",
                        page_no=str(page_no),
                        per_page=str(per_page),
                        start=str((page_no - 1) * per_page),
                        limit=str(per_page),
                    ),
                )
                data = resp.json()
            except Exception:
                self.logger.warning(
                    "Failed API page %d of %s", page_no, category_name
                )
                break

            items = data.get("data", [])
            if not items:
                break

            for item in items:
                product = self.parse_product(item, category_name, city)
                if product:
                    yield product

            # If fewer items than requested, we've hit the last page
            if len(items) < per_page:
                break
            page_no += 1

    # ── Overridden run loop (per-city branch switching) ──────────────────

    def run(self, target_rows: int = 0):
        """Scrape each city with its own branch ID using a thread pool per city."""
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
                "RESUMING scrape for %s — %d categories done, %d rows on disk.",
                self.store_name, len(completed_keys), saved_rows,
            )
            self._rows_collected = saved_rows
        else:
            completed_keys = set()
            if out.exists():
                out.unlink()
            self._rows_collected = 0

        stop_event = threading.Event()
        # Track whether the run finished fully so we know whether to clear the checkpoint
        fully_done = False

        for city in self.cities:
            if stop_event.is_set():
                break
            self._active_branch = self.BRANCH_MAP.get(city, self.DEFAULT_BRANCH_ID)
            self.logger.info("─── City: %s  (branch %s) ───", city, self._active_branch)

            # Discover categories for this branch.  Skip the HTTP round-trips entirely
            # when every category for this city is already recorded in the checkpoint.
            categories = self.discover_categories()
            if not categories:
                self.logger.warning("No categories discovered for %s — skipping city.", city)
                continue
            if all(f"{city}::{cat['url']}" in completed_keys for cat in categories):
                self.logger.info("All %d categories for %s already checkpointed — skipping discovery HTTP overhead.", len(categories), city)
                continue
            self.logger.info("Discovered %d categories for %s", len(categories), city)

            cats_to_scrape = [
                (cat, cat["url"])
                for cat in categories
                if f"{city}::{cat['url']}" not in completed_keys
            ]
            if not cats_to_scrape:
                self.logger.info("All categories for %s already in checkpoint — skipping.", city)
                continue

            self.logger.info(
                "Scraping %d categories with %d threads …",
                len(cats_to_scrape), config.SCRAPER_WORKERS,
            )

            def _worker(cat, cat_url):
                if stop_event.is_set():
                    return
                self.logger.info("  [thread] %s", cat["name"])
                batch: list = []
                try:
                    for product in self.scrape_category(cat_url, cat["name"], city):
                        batch.append(product)
                        if len(batch) >= 500:
                            self.save_rows(batch)
                            batch = []
                        if stop_event.is_set():
                            break
                except Exception:
                    self.logger.exception(
                        "Error scraping category %s in %s", cat["name"], city
                    )
                    with self._lock:
                        self._failures += 1
                finally:
                    if batch:
                        self.save_rows(batch)

                with self._lock:
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

        # Only wipe the checkpoint when every category in every city was processed.
        # If stop_event fired because target_rows was reached, keep the checkpoint so
        # the next run can resume from where this one left off.
        if not stop_event.is_set():
            fully_done = True
            self._clear_checkpoint()
        else:
            self.logger.info(
                "Scrape stopped early (target_rows=%d reached). "
                "Checkpoint retained — next run will resume from here.",
                target_rows,
            )

        self.logger.info(
            "Scrape %s for %s — %d total rows.",
            "complete" if fully_done else "paused",
            self.store_name,
            self._rows_collected,
        )
        self.logger.info("─── Scraping Attempt Summary (%s) ───", self.store_name)
        self.logger.info("  HTTP requests attempted:  %d", self._attempts)
        self.logger.info("  HTTP requests succeeded:  %d", self._successes)
        self.logger.info("  Failures (errors):        %d", self._failures)
        self.logger.info("  Categories scraped:       %d", self._categories_scraped)
        self.logger.info("  Rows collected:           %d", self._rows_collected)
        self.logger.info("─" * 40)


if __name__ == "__main__":
    scraper = ImtiazScraper()
    scraper.run()
    print(f"Imtiaz scrape finished — {scraper.total_rows} rows collected.")
