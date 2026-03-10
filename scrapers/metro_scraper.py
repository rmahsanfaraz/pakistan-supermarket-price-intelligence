"""
Metro Online (metro-online.pk) scraper.

Metro's online catalogue is a Next.js SPA backed by a REST API at
admin.metro-online.pk.  This scraper fetches products directly from the
JSON API, which is far more reliable than parsing the JS-rendered HTML.
"""

import json
from typing import Any, Dict, Generator, List, Optional

import config
from scrapers.base_scraper import BaseScraper
from scrapers.helpers import extract_brand, extract_size


class MetroScraper(BaseScraper):
    """Scraper for https://www.metro-online.pk via admin REST API."""

    API_BASE = "https://admin.metro-online.pk"
    STORE_ID = "10"  # Metro Pakistan store ID

    def __init__(self):
        super().__init__(config.METRO_CONFIG)

    # ── Category discovery (via API) ─────────────────────────────────────

    def discover_categories(self) -> List[Dict[str, str]]:
        """Fetch categories from the Metro admin API."""
        categories: List[Dict[str, str]] = []
        try:
            self.rate_limiter.wait()
            self._attempts += 1
            resp = self.session.get(
                f"{self.API_BASE}/api/read/Categories",
                params={"filter": "storeId", "filterValue": self.STORE_ID},
                timeout=config.REQUEST_TIMEOUT,
            )
            resp.raise_for_status()
            self._successes += 1
            data = resp.json().get("data", [])
            for cat in data:
                cat_id = cat.get("id")
                cat_name = cat.get("category_name", "").strip()
                if cat_id and cat_name:
                    categories.append({
                        "name": cat_name,
                        "url": str(cat_id),  # store category ID as "url"
                    })
        except Exception:
            self.logger.warning("API category discovery failed.", exc_info=True)
            self._failures += 1

        self.logger.info("Discovered %d categories via API.", len(categories))
        return categories

    # ── Product parsing (from API JSON) ──────────────────────────────────

    def _parse_api_product(self, item: dict, category_name: str, city: str) -> Optional[Dict[str, Any]]:
        """Convert an API product JSON object into our standard dict."""
        product_name = (item.get("product_name") or "").strip()
        if not product_name:
            return None

        price = item.get("sell_price") or item.get("price")
        try:
            price = float(price) if price is not None else None
        except (ValueError, TypeError):
            price = None

        brand = item.get("brand_name") or extract_brand(product_name)
        image_url = item.get("url", "")  # API stores image URL in 'url'
        # seo_url_slug is always empty in the Metro API; prefer product_code_app, then
        # internal id, then extract short numeric ID from the image filename.
        product_code = item.get("product_code_app") or item.get("id") or ""
        if not product_code and image_url:
            import re as _re
            _m = _re.search(r'/products_images_new/(\d+)-\d+-[A-Z]\.', image_url, _re.IGNORECASE)
            if not _m:
                _m = _re.search(r'/Products/(\d{1,9})\.(png|jpg|jpeg)', image_url, _re.IGNORECASE)
            product_code = _m.group(1) if _m else ""
        product_url = f"https://www.metro-online.pk/detail/{product_code}" if product_code else ""

        # Build category from tier hierarchy if available
        tier_names = [
            item.get("teir1Name", ""),
            item.get("tier2Name", ""),
            item.get("tier3Name", ""),
            item.get("tier4Name", ""),
        ]
        api_category = " > ".join(n for n in tier_names if n) or category_name

        return {
            "store_name": self.store_name,
            "city": city,
            "product_name": product_name,
            "brand": brand,
            "category": api_category,
            "price": price,
            "original_price": None,
            "currency": self.currency,
            "size": item.get("weight", "") or extract_size(product_name),
            "product_url": product_url,
            "image_url": image_url,
            "in_stock": None,
            "scrape_timestamp": self.now_iso(),
        }

    # ── API-based category scraper ───────────────────────────────────────

    def scrape_category(
        self, category_url: str, category_name: str, city: str
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Fetch products from the admin API.  *category_url* is actually
        the category ID string returned by discover_categories().
        We paginate using offset/limit.
        """
        offset = 0
        limit = self.page_size  # 48
        while True:
            try:
                self.rate_limiter.wait()
                self._attempts += 1
                resp = self.session.get(
                    f"{self.API_BASE}/api/read/Products",
                    params={
                        "type": "Products_nd_associated_Brands",
                        "order": "product_scoring__DESC",
                        "filter": "storeId",
                        "filterValue": self.STORE_ID,
                        "offset": str(offset),
                        "limit": str(limit),
                    },
                    timeout=config.REQUEST_TIMEOUT,
                )
                resp.raise_for_status()
                self._successes += 1
            except Exception:
                self.logger.warning(
                    "API request failed at offset %d for %s.", offset, category_name,
                )
                self._failures += 1
                break

            payload = resp.json()
            products = payload.get("data", [])
            if not products:
                break

            for item in products:
                parsed = self._parse_api_product(item, category_name, city)
                if parsed:
                    yield parsed

            total = payload.get("total_count", 0)
            offset += limit
            if offset >= total:
                break

    # ── Overrides (not needed for API-based scraping) ────────────────────

    def parse_product(self, element: Any, category_name: str, city: str) -> Optional[Dict[str, Any]]:
        """Not used for API-based scraping; kept for interface compliance."""
        return None

    def run(self, target_rows: int = 0):
        """
        Override the base run() to do a single API-paginated fetch
        across all products (the API doesn't easily filter by category,
        so we fetch all products and label them with their tier hierarchy).
        """
        self.logger.info("=" * 60)
        self.logger.info("Starting API scrape for %s", self.store_name)
        self.logger.info("Cities: %s", self.cities)
        self.logger.info("=" * 60)

        out = self._output_path()
        if out.exists():
            out.unlink()
        self._rows_collected = 0

        # Fetch all products from the API (single catalogue for all cities)
        # The API has a hard pagination cap (~6 pages) so we use a single
        # large-limit request to retrieve the full catalogue at once.
        all_products: List[Dict[str, Any]] = []

        try:
            self.rate_limiter.wait()
            self._attempts += 1
            resp = self.session.get(
                f"{self.API_BASE}/api/read/Products",
                params={
                    "type": "Products_nd_associated_Brands",
                    "order": "product_scoring__DESC",
                    "filter": "storeId",
                    "filterValue": self.STORE_ID,
                    "offset": "0",
                    "limit": "15000",
                },
                timeout=config.REQUEST_TIMEOUT * 3,
            )
            resp.raise_for_status()
            self._successes += 1
        except Exception:
            self.logger.warning("API request failed.")
            self._failures += 1
            self._log_summary()
            return

        payload = resp.json()
        products = payload.get("data", [])
        total = payload.get("total_count", 0)
        self.logger.info(
            "Fetched %d of %d products in single request", len(products), total,
        )

        if products:
            for city in self.cities:
                batch: List[Dict[str, Any]] = []
                for item in products:
                    parsed = self._parse_api_product(item, "", city)
                    if parsed:
                        batch.append(parsed)
                if batch:
                    self.save_rows(batch)

        self._log_summary()

    def _log_summary(self):
        self.logger.info("Scrape complete for %s — %d total rows.", self.store_name, self._rows_collected)
        self.logger.info("─── Scraping Attempt Summary (%s) ───", self.store_name)
        self.logger.info("  HTTP requests attempted:  %d", self._attempts)
        self.logger.info("  HTTP requests succeeded:  %d", self._successes)
        self.logger.info("  Failures (errors):        %d", self._failures)
        self.logger.info("  Rows collected:           %d", self._rows_collected)
        self.logger.info("─" * 40)


if __name__ == "__main__":
    scraper = MetroScraper()
    scraper.run()
    print(f"Metro scrape finished — {scraper.total_rows} rows collected.")
