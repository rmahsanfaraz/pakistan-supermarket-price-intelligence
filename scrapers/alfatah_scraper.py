"""
Al-Fatah (alfatah.com.pk) scraper.

Al-Fatah is a Punjab-based retail chain (Lahore, Islamabad, Rawalpindi).
Their online store at alfatah.com.pk is a WooCommerce site focused on
electronics and home appliances.
"""

import asyncio
import json
from typing import Any, Dict, Generator, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup, Tag

import config
from scrapers.base_scraper import BaseScraper
from scrapers.helpers import parse_price, extract_brand, extract_size


class AlFatahScraper(BaseScraper):
    """Scraper for https://www.alfatah.com.pk"""

    # Real product-category URLs discovered from the live alfatah.com.pk site
    _SEED_CATEGORIES = [
        {"name": "Air Conditioners", "url": "/product-category/online-shop-for-split-air-conditioner-in-pakistan/"},
        {"name": "Refrigerators", "url": "/product-category/online-shop-for-refrigerators-in-pakistan/"},
        {"name": "Washing Machines", "url": "/product-category/online-shop-for-washing-machine-in-pakistan/"},
        {"name": "Microwave Ovens", "url": "/product-category/microwave-oven/"},
        {"name": "Cooking Range", "url": "/product-category/online-shop-for-3-burners-cooking-range-in-pakistan/"},
        {"name": "LED TVs", "url": "/product-category/online-shop-for-led-tv-in-pakistan/"},
        {"name": "Water Dispensers", "url": "/product-category/online-shop-for-water-dispenser-in-pakistan/"},
        {"name": "Heaters", "url": "/product-category/online-shop-for-heaters-in-pakistan/"},
        {"name": "Air Purifiers", "url": "/product-category/air-purifier/"},
        {"name": "Deep Freezers", "url": "/product-category/online-shop-for-deep-freezer-in-pakistan/"},
        {"name": "Kitchen Appliances", "url": "/product-category/online-shop-for-small-kitchen-items-in-pakistan/"},
        {"name": "Air Fryers", "url": "/product-category/online-shop-for-small-kitchen-items-in-pakistan/air-fryer/"},
        {"name": "Mobile & Car Accessories", "url": "/product-category/mobile-and-car-accessories/"},
        {"name": "Iron & Garment Care", "url": "/product-category/online-shop-for-iron-in-pakistan/"},
        {"name": "Personal Care", "url": "/product-category/online-shop-for-personal-care-items-in-pakistan/"},
        {"name": "Kitchen Hob", "url": "/product-category/online-shop-for-kitchen-hob-in-pakistan/"},
        {"name": "Kitchen Hood", "url": "/product-category/online-shop-for-kitchen-hood-in-pakistan/"},
        {"name": "Vacuum Cleaners", "url": "/product-category/online-shop-for-vaccume-cleaner-in-pakistan/"},
        {"name": "Geysers", "url": "/product-category/online-shop-for-geyser-in-pakistan/"},
        {"name": "Fans", "url": "/product-category/online-shop-for-fans-in-pakistan/"},
        {"name": "UPS & Stabilizers", "url": "/product-category/online-shop-for-ups-stabilizer-in-pakistan/"},
        {"name": "Dish Washers", "url": "/product-category/online-shop-for-dishwasher-in-pakistan/"},
        {"name": "Water Coolers", "url": "/product-category/online-shop-for-water-cooler-in-pakistan/"},
        {"name": "Air Coolers", "url": "/product-category/air-cooler/"},
        {"name": "All Products", "url": "/shop/"},
    ]

    SEL_PRODUCT_CARD = (
        "div.product-wrapper, li.product, div.product-card, div.product-item, "
        "div.product-grid-item, article.product, "
        "div[data-product], div.col-product, div.product-box"
    )
    SEL_PRODUCT_NAME = (
        "h3.product-title a, h3, h2.woocommerce-loop-product__title, "
        "h2.product-title a, a.woocommerce-LoopProduct-link h2, "
        ".product-title, a.product-name, span.product-name, div.product-title a, h2 a, h3 a"
    )
    # NOTE: deliberately excludes span.price and div.price span — those are
    # WooCommerce *container* elements that hold BOTH the crossed-out original
    # price AND the sale price as children.  Selecting them via get_text()
    # concatenates both numbers into nonsense like "350000280000".
    SEL_PRICE_INNER = (
        "bdi, span.woocommerce-Price-amount, "
        "span.current-price, span.price-amount"
    )
    SEL_PAGINATION_NEXT = (
        "a.next.page-numbers, a.next, li.next a, "
        "a[rel='next'], nav.woocommerce-pagination a.next"
    )
    SEL_CATEGORY_LINK = (
        "ul.product-categories a, li.cat-item a, "
        "nav.product-categories a, div.category-card a, "
        "ul.category-list a, a.category-link, div.product-category a"
    )

    def __init__(self):
        super().__init__(config.ALFATAH_CONFIG)

    # ── Category discovery ───────────────────────────────────────────────

    def discover_categories(self) -> List[Dict[str, str]]:
        categories: List[Dict[str, str]] = []
        try:
            soup = self._soup(self.base_url)
            for a in soup.select(self.SEL_CATEGORY_LINK):
                href = a.get("href", "")
                name = a.get_text(strip=True)
                if href and name and "product-category" in href:
                    full = href if href.startswith("http") else urljoin(self.base_url, href)
                    categories.append({"name": name, "url": full})

            for a in soup.select("nav a, ul.menu a, ul.main-menu a"):
                href = a.get("href", "")
                name = a.get_text(strip=True)
                if href and name and ("product-category" in href or "/shop/" in href):
                    full = href if href.startswith("http") else urljoin(self.base_url, href)
                    if full not in {c["url"] for c in categories}:
                        categories.append({"name": name, "url": full})

            for script in soup.find_all("script", type="application/ld+json"):
                try:
                    data = json.loads(script.string or "")
                    if isinstance(data, dict) and "itemListElement" in data:
                        for item in data["itemListElement"]:
                            if "item" in item:
                                url = item["item"]
                                if not url.startswith("http"):
                                    url = urljoin(self.base_url, url)
                                categories.append({"name": item.get("name", ""), "url": url})
                except (json.JSONDecodeError, KeyError):
                    continue
        except Exception:
            self.logger.warning("Live category discovery failed; using seeds.")

        if not categories:
            self.logger.info("Using seed categories (%d).", len(self._SEED_CATEGORIES))
            categories = [
                {"name": c["name"], "url": urljoin(self.base_url, c["url"])}
                for c in self._SEED_CATEGORIES
            ]

        seen, unique = set(), []
        for c in categories:
            if c["url"] not in seen:
                seen.add(c["url"])
                unique.append(c)
        self.logger.info("Total unique categories: %d", len(unique))
        return unique

    # ── Product parsing ──────────────────────────────────────────────────

    def parse_product(self, element: Tag, category_name: str, city: str) -> Optional[Dict[str, Any]]:
        try:
            name_el = element.select_one(self.SEL_PRODUCT_NAME)
            if not name_el:
                return None
            product_name = name_el.get_text(strip=True)
            if not product_name:
                return None

            product_url = ""
            link_el = element.select_one("a.woocommerce-LoopProduct-link, a[href]")
            if link_el:
                href = link_el.get("href", "")
                product_url = href if href.startswith("http") else urljoin(self.base_url, href)

            # WooCommerce sale layout: <del>original</del><ins>sale</ins>
            # 1. Prefer the discounted price inside <ins>
            price_el = element.select_one("ins bdi, ins span.woocommerce-Price-amount")
            if not price_el:
                # 2. Fall back to any price element NOT inside a <del> tag
                for sel in self.SEL_PRICE_INNER.split(","):
                    sel = sel.strip()
                    for candidate in element.select(sel):
                        if not candidate.find_parent("del"):
                            price_el = candidate
                            break
                    if price_el:
                        break
            raw_price = price_el.get_text(strip=True) if price_el else ""

            img_el = element.select_one("img")
            # Prefer data-src (lazy-load real URL) over src (may be SVG placeholder)
            image_url = (img_el.get("data-src") or img_el.get("src", "")) if img_el else ""

            return {
                "store_name": self.store_name,
                "city": city,
                "product_name": product_name,
                "brand": extract_brand(product_name),
                "category": category_name,
                "price": parse_price(raw_price),
                "original_price": None,
                "currency": self.currency,
                "size": extract_size(product_name),
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
        page_num = 1
        while page_num <= 200:
            url = self._page_url(category_url, page_num)
            try:
                soup = self._soup(url)
            except Exception:
                self.logger.warning("Failed page %d of %s.", page_num, category_name)
                break

            cards = soup.select(self.SEL_PRODUCT_CARD)
            if not cards:
                break

            for card in cards:
                product = self.parse_product(card, category_name, city)
                if product:
                    yield product

            if not self._has_next(soup):
                break
            page_num += 1

    # ── Playwright fallback ──────────────────────────────────────────────

    def scrape_category_playwright(
        self, category_url: str, category_name: str, city: str
    ) -> Generator[Dict[str, Any], None, None]:
        from scrapers.playwright_helper import browser_context, get_page_content

        async def _scrape():
            products = []
            async with browser_context() as ctx:
                page_num = 1
                while page_num <= 200:
                    url = self._page_url(category_url, page_num)
                    try:
                        # No wait_selector: the page uses varied CSS across categories
                        # (li.product, div.product-wrapper, article.product, …).
                        # We rely on domcontentloaded + 2 s settle in get_page_content
                        # and then parse with the full SEL_PRODUCT_CARD selector set.
                        html = await get_page_content(ctx, url)
                    except Exception:
                        self.logger.warning("Playwright failed page %d of %s", page_num, category_name)
                        break
                    soup = BeautifulSoup(html, "lxml")
                    cards = soup.select(self.SEL_PRODUCT_CARD)
                    if not cards:
                        break
                    for card in cards:
                        product = self.parse_product(card, category_name, city)
                        if product:
                            products.append(product)
                    if not self._has_next(soup):
                        break
                    page_num += 1
            return products

        for product in asyncio.run(_scrape()):
            yield product

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _page_url(base: str, page: int) -> str:
        if page == 1:
            return base
        return f"{base.rstrip('/')}/page/{page}/"

    def _has_next(self, soup: BeautifulSoup) -> bool:
        return bool(soup.select_one(self.SEL_PAGINATION_NEXT))


if __name__ == "__main__":
    scraper = AlFatahScraper()
    scraper.run()
    print(f"Al-Fatah scrape finished — {scraper.total_rows} rows collected.")
