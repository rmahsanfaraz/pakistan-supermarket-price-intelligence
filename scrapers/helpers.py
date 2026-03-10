"""
Shared helpers used by multiple scrapers (brand extraction, size parsing, price
cleaning).  Kept here to avoid duplication across store modules.
"""

import re
from typing import Optional

# Brands common in Pakistani grocery retail
KNOWN_BRANDS = [
    "Nestle", "Olpers", "Tapal", "Lipton", "Shan", "National",
    "Unilever", "K&N", "K&Ns", "Dawn", "Knorr", "Rafhan",
    "Dalda", "Sufi", "Habib", "Nurpur", "Haleeb", "Pakola",
    "Pepsi", "Coca-Cola", "Sprite", "Fanta", "7Up", "Mountain Dew",
    "Lays", "Kurkure", "Pringles", "Colgate", "Surf", "Ariel",
    "Dettol", "Safeguard", "Lux", "Dove", "Head & Shoulders",
    "Pantene", "Sunsilk", "Garnier", "Nivea", "Fair & Lovely",
    "Mezan", "Eva", "Engro", "Gourmet", "Peek Freans", "LU",
    "Bisconni", "Candyland", "Hilal", "Mitchell", "Shangrila",
    "Young's", "PK", "Rooh Afza", "Tang", "Milo", "Nido",
    "Everyday", "Tarang", "Good Milk", "Millac", "Adams",
]

_SIZE_RE = re.compile(
    r"(\d+(?:\.\d+)?\s*"
    r"(?:g|gm|gms|gram|grams|kg|kgs|ml|l|ltr|litre|litres|liter|liters"
    r"|oz|pcs|pack|rolls|sheets|tablets|caps)s?)\b",
    re.IGNORECASE,
)


def parse_price(raw: str) -> Optional[float]:
    """Extract numeric price from strings like 'Rs. 1,250.00' or '₨ 35,000'.

    Handles:
    - Currency symbols and words (Rs, Rs., ₨, PKR)
    - Thousands commas
    - Stray periods left by currency abbreviations ("Rs." → remaining ".")
    - Multiple decimal separators (keeps only the last one)
    """
    if not raw:
        return None
    # Remove commas used as thousands separators
    s = raw.replace(",", "")
    # Strip everything except digits and dots
    s = re.sub(r"[^\d.]", "", s)
    # If multiple dots remain (e.g. ".1250.00" from "Rs. 1250.00")
    # treat everything before the LAST dot as the integer part
    parts = s.split(".")
    if len(parts) > 2:
        s = "".join(parts[:-1]) + "." + parts[-1]
    s = s.strip(".")
    if not s:
        return None
    try:
        v = float(s)
        return v if v > 0 else None
    except ValueError:
        return None


def extract_brand(product_name: str) -> str:
    """Return the first known brand found in *product_name*, else first word."""
    name_lower = product_name.lower()
    for brand in KNOWN_BRANDS:
        if brand.lower() in name_lower:
            return brand
    parts = product_name.split()
    return parts[0] if parts else ""


def extract_size(product_name: str) -> str:
    """Pull size/weight info from the product name (e.g. '500g', '1.5L')."""
    match = _SIZE_RE.search(product_name)
    return match.group(1).strip() if match else ""
