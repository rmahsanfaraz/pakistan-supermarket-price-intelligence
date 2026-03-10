"""
Naheed (naheed.pk) scraper.

Naheed is a Karachi-based supermarket chain with two physical stores
(Bahadurabad and Malir Cantonment) and an e-commerce site built on
Magento 2.  This scraper uses the public Magento GraphQL API at
``https://www.naheed.pk/graphql`` to fetch ALL product categories.
"""

from typing import Any, Dict, Generator, List, Optional

import config
from scrapers.base_scraper import BaseScraper
from scrapers.helpers import extract_brand, extract_size


# Magento default root category – queries all departments, not just grocery.
_ROOT_CATEGORY_ID = 2

_GRAPHQL_URL = "https://www.naheed.pk/graphql"

_CATEGORY_QUERY = """
{
  categoryList(filters: {ids: {eq: "%s"}}) {
    id
    name
    product_count
    children {
      id
      name
      url_key
      product_count
      children {
        id
        name
        url_key
        product_count
      }
    }
  }
}
"""

_PRODUCTS_QUERY = """
{
  products(
    filter: {category_id: {eq: "%s"}}
    pageSize: %d
    currentPage: %d
  ) {
    total_count
    items {
      name
      sku
      url_key
      price_range {
        minimum_price {
          regular_price { value currency }
          final_price { value currency }
          discount { amount_off percent_off }
        }
      }
      image { url label }
    }
    page_info {
      total_pages
      current_page
      page_size
    }
  }
}
"""


class NaheedScraper(BaseScraper):
    """Scraper for https://www.naheed.pk using the Magento 2 GraphQL API."""

    def __init__(self):
        super().__init__(config.NAHEED_CONFIG)

    # ── GraphQL helper ───────────────────────────────────────────────────

    def _graphql(self, query: str) -> dict:
        """Execute a GraphQL query against Naheed's Magento endpoint (thread-safe)."""
        self._get_rate_limiter().wait()
        self._attempts += 1
        self.logger.debug("POST %s", _GRAPHQL_URL)
        resp = self._get_session().post(
            _GRAPHQL_URL,
            json={"query": query},
            headers={"Content-Type": "application/json"},
            timeout=config.REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        self._successes += 1
        return resp.json()

    # ── Category discovery ───────────────────────────────────────────────

    def discover_categories(self) -> List[Dict[str, str]]:
        """Fetch ALL product sub-categories via GraphQL (root id=2)."""
        query = _CATEGORY_QUERY % _ROOT_CATEGORY_ID
        try:
            data = self._graphql(query)
        except Exception:
            self.logger.exception("Failed to fetch categories via GraphQL.")
            return []

        categories: List[Dict[str, str]] = []
        for root in data.get("data", {}).get("categoryList", []):
            for dept in root.get("children", []):
                dept_name = dept.get("name", "").strip()
                children = dept.get("children", [])
                if children:
                    # Use leaf sub-categories for finer granularity
                    for child in children:
                        pc = child.get("product_count", 0)
                        if pc <= 0:
                            continue
                        cat_id = str(child["id"])
                        cat_name = f"{dept_name} > {child['name']}"
                        categories.append({"name": cat_name, "url": cat_id})
                        self.logger.debug(
                            "Category: %s (id=%s, products=%d)",
                            cat_name, cat_id, pc,
                        )
                elif dept.get("product_count", 0) > 0:
                    # Department with products but no children
                    cat_id = str(dept["id"])
                    categories.append({"name": dept_name, "url": cat_id})
                    self.logger.debug(
                        "Category: %s (id=%s, products=%d)",
                        dept_name, cat_id, dept["product_count"],
                    )

        self.logger.info("Discovered %d sub-categories across all departments.", len(categories))
        return categories

    # ── Product parsing ──────────────────────────────────────────────────

    def parse_product(
        self, element: Any, category_name: str, city: str
    ) -> Optional[Dict[str, Any]]:
        """Convert a GraphQL product node into a flat dict."""
        try:
            name = element.get("name", "").strip()
            if not name:
                return None

            price_range = element.get("price_range", {})
            min_price = price_range.get("minimum_price", {})
            final = min_price.get("final_price", {}).get("value")

            url_key = element.get("url_key", "")
            product_url = f"https://www.naheed.pk/{url_key}" if url_key else ""

            image_info = element.get("image") or {}
            image_url = image_info.get("url", "")

            return {
                "store_name": self.store_name,
                "city": city,
                "product_name": name,
                "brand": extract_brand(name),
                "category": category_name,
                "price": final,
                "original_price": None,
                "currency": self.currency,
                "size": extract_size(name),
                "product_url": product_url,
                "image_url": image_url,
                "in_stock": None,
                "scrape_timestamp": self.now_iso(),
            }
        except Exception:
            self.logger.debug("Failed to parse product element.", exc_info=True)
            return None

    # ── Pagination-aware category scraper ────────────────────────────────

    def scrape_category(
        self, category_url: str, category_name: str, city: str
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Paginate through all products in a category via GraphQL.

        ``category_url`` here is the Magento category **id** —
        possibly prefixed with the base URL by the parent ``run()``
        method (e.g. ``"https://www.naheed.pk/310"``).
        """
        # Extract the numeric category id regardless of URL wrapping
        cat_id = category_url.rstrip("/").rsplit("/", 1)[-1]
        page = 1
        total_pages = 1  # updated after first response

        while page <= total_pages:
            query = _PRODUCTS_QUERY % (cat_id, self.page_size, page)
            try:
                data = self._graphql(query)
            except Exception:
                self.logger.warning(
                    "GraphQL request failed for %s page %d.", category_name, page,
                )
                break

            products_data = data.get("data", {}).get("products", {})
            items = products_data.get("items", [])

            if not items:
                break

            page_info = products_data.get("page_info", {})
            total_pages = page_info.get("total_pages", 1)

            for item in items:
                product = self.parse_product(item, category_name, city)
                if product:
                    yield product

            self.logger.debug(
                "  %s page %d/%d — %d items", category_name, page, total_pages, len(items),
            )
            page += 1
