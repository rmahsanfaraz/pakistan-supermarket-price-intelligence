"""
Chase Up (chaseupgrocery.com) scraper.

Chase Up is a Pakistani grocery and general merchandise retail chain
based primarily in Karachi.  Their site at chaseupgrocery.com runs on the
BlinkCo.io ordering platform (Next.js frontend) and exposes a REST API.
The full product catalogue is returned in a single /api/menu call, so this
scraper fetches all products in one request per branch and parses the nested
JSON hierarchy: menu → section → sub_section → dish.
"""

import re
import threading
from typing import Any, Dict, Generator, List, Optional

import config
from scrapers.base_scraper import BaseScraper, RateLimiter
from scrapers.helpers import extract_brand, extract_size


class ChaseUpScraper(BaseScraper):
    """Scraper for https://www.chaseupgrocery.com via BlinkCo REST API."""

    API_BASE = "https://www.chaseupgrocery.com/api"
    REST_ID = "55525"

    # Branch IDs per city — discovered from the geofence / splash APIs.
    # All branches carry a nearly identical catalogue (~6 700 products).
    BRANCH_MAP = {
        "Karachi": "56249",
    }
    DEFAULT_BRANCH_ID = "56249"

    # Custom headers required by the BlinkCo platform
    _API_HEADERS = {
        "app-name": "chaseup",
        "rest-id": "55525",
        "timezone": "Asia/Karachi",
    }

    _API_RATE_LIMIT_DELAY = 0.5

    def __init__(self):
        super().__init__(config.CHASEUP_CONFIG)
        self.session.headers.update(self._API_HEADERS)
        self._active_branch: str = self.DEFAULT_BRANCH_ID

    def _get_rate_limiter(self) -> RateLimiter:
        if not hasattr(self._thread_local, "rate_limiter"):
            self._thread_local.rate_limiter = RateLimiter(
                min_delay=self._API_RATE_LIMIT_DELAY
            )
        return self._thread_local.rate_limiter

    # ── Category discovery (from /api/menu bulk response) ────────────────

    def discover_categories(self) -> List[Dict[str, str]]:
        """Fetch the full menu and return a flat list of category entries.

        Each entry's ``url`` field stores the top-level menu section id
        (used later by ``scrape_category`` to filter the cached response).
        """
        categories: List[Dict[str, str]] = []
        try:
            resp = self._get(
                f"{self.API_BASE}/menu",
                params={"rest_brId": self._active_branch},
            )
            menu_data = resp.json().get("data", [])
            # Cache the full menu so scrape_category doesn't re-fetch
            self._cached_menu = menu_data
            for section in menu_data:
                name = (section.get("name") or "").strip()
                sec_id = section.get("id")
                if not name or not sec_id:
                    continue
                # Only include sections that actually contain products
                has_dishes = False
                for inner in section.get("all_section", []):
                    for subsec in inner.get("all_sub_section", []):
                        if subsec.get("dish"):
                            has_dishes = True
                            break
                    if has_dishes:
                        break
                if has_dishes:
                    categories.append({"name": name, "url": str(sec_id)})
        except Exception:
            self.logger.warning("API menu fetch failed.", exc_info=True)
            self._failures += 1
            self._cached_menu = []

        self.logger.info("Discovered %d categories via API.", len(categories))
        return categories

    # ── Product parsing (from API JSON dish object) ──────────────────────

    def parse_product(
        self, item: Any, category_name: str, city: str
    ) -> Optional[Dict[str, Any]]:
        """Convert a BlinkCo dish JSON object into the canonical dict."""
        if not isinstance(item, dict):
            return None

        product_name = (item.get("name") or "").strip()
        if not product_name:
            return None

        # Skip inactive or unavailable dishes
        if not item.get("status", 1):
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

        # Branch-level price override
        branch_info = item.get("dish_branch_status")
        if isinstance(branch_info, dict) and branch_info.get("price"):
            try:
                branch_price = float(branch_info["price"])
                if branch_price > 0:
                    price = branch_price
            except (ValueError, TypeError):
                pass
            try:
                branch_disc = float(branch_info.get("discount_price") or 0)
                if branch_disc > 0:
                    discount_price = branch_disc
            except (ValueError, TypeError):
                pass

        brand = (item.get("brand_name") or "").strip() or extract_brand(product_name)
        image_url = item.get("img_url") or ""

        slug = item.get("slug") or ""
        prod_id = item.get("id")
        if slug:
            product_url = f"{self.base_url}/product/{slug}-{prod_id}"
        elif prod_id:
            name_slug = re.sub(r"[^a-z0-9]+", "-", product_name.lower()).strip("-")
            product_url = f"{self.base_url}/product/{name_slug}-{prod_id}"
        else:
            product_url = ""

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

    # ── Category scraper (uses cached menu data) ─────────────────────────

    def scrape_category(
        self, category_url: str, category_name: str, city: str
    ) -> Generator[Dict[str, Any], None, None]:
        """Yield product dicts from the cached menu for a given section id.

        ``category_url`` holds the menu section id set by discover_categories.
        """
        section_id = category_url.rstrip("/").rsplit("/", 1)[-1]

        menu_data = getattr(self, "_cached_menu", [])
        for section in menu_data:
            if str(section.get("id")) != section_id:
                continue
            for inner in section.get("all_section", []):
                inner_name = (inner.get("name") or "").strip()
                for subsec in inner.get("all_sub_section", []):
                    subsec_name = (subsec.get("name") or "").strip()
                    full_cat = f"{category_name} > {inner_name} > {subsec_name}"
                    for dish in subsec.get("dish", []):
                        product = self.parse_product(dish, full_cat, city)
                        if product:
                            yield product

    # ── Overridden run loop (per-city branch switching) ──────────────────

    def run(self, target_rows: int = 0):
        """Scrape each city with its own branch ID."""
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
        fully_done = False

        for city in self.cities:
            if stop_event.is_set():
                break
            self._active_branch = self.BRANCH_MAP.get(city, self.DEFAULT_BRANCH_ID)
            self.logger.info("─── City: %s  (branch %s) ───", city, self._active_branch)

            categories = self.discover_categories()
            if not categories:
                self.logger.warning("No categories discovered for %s — skipping.", city)
                continue

            for cat in categories:
                if stop_event.is_set():
                    break
                checkpoint_key = f"{city}::{cat['url']}"
                if checkpoint_key in completed_keys:
                    self.logger.debug("    ↳ Checkpoint skip: %s", cat["name"])
                    continue

                self.logger.info("  %s", cat["name"])
                batch: list = []
                try:
                    for product in self.scrape_category(cat["url"], cat["name"], city):
                        batch.append(product)
                        if len(batch) >= 500:
                            self.save_rows(batch)
                            batch = []
                        if stop_event.is_set():
                            break
                except Exception:
                    self.logger.exception(
                        "Error scraping %s in %s", cat["name"], city
                    )
                    self._failures += 1
                finally:
                    if batch:
                        self.save_rows(batch)

                with self._lock:
                    self._categories_scraped += 1
                    completed_keys.add(checkpoint_key)
                self._save_checkpoint(completed_keys, self._rows_collected)

                if target_rows and self._rows_collected >= target_rows:
                    stop_event.set()

        if not stop_event.is_set():
            fully_done = True
            self._clear_checkpoint()

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
    scraper = ChaseUpScraper()
    scraper.run()
    print(f"Chase Up scrape finished — {scraper._rows_collected} rows collected.")
