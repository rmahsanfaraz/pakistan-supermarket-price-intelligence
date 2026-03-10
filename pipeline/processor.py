"""
Data Cleaning & Normalisation Pipeline
=======================================

Reads raw Parquet / CSV files produced by the scrapers, applies eight
transformation stages, and writes a clean dataset to data/processed/.

Pipeline stages (in order):
    1. Load & initial inspection
    2. Convert all prices to numeric format
   2b. Flag placeholder prices & mark availability status
    3. Normalize currency formats
    4. Extract quantity (value + unit) via regex
    5. Standardize product sizes (convert to base units)
    6. Normalize brand names (merge spelling variants)
    7. Handle missing values (column-aware imputation)
    8. Remove duplicate records (multi-key dedup)
    9. Compute price-per-unit
   10. Build a cleaned product-name column

Each function is self-contained so it can be unit-tested or reused
independently.  The public entry-point is ``process_all()``.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Optional

import pandas as pd

import config
from scrapers.base_scraper import build_logger

logger = build_logger("processor")


# =====================================================================
# REFERENCE TABLES
# =====================================================================

# --- Unit synonym map (raw token → canonical unit) -------------------
UNIT_MAP: dict[str, str] = {
    # weight
    "g": "g", "gm": "g", "gms": "g", "gram": "g", "grams": "g", "grm": "g",
    "kg": "kg", "kgs": "kg", "kilo": "kg", "kilos": "kg", "kilogram": "kg",
    "kilograms": "kg",
    # volume
    "ml": "ml", "millilitre": "ml", "milliliter": "ml",
    "l": "l", "ltr": "l", "ltrs": "l", "litre": "l", "litres": "l",
    "liter": "l", "liters": "l",
    # imperial
    "oz": "oz", "fl oz": "oz", "lb": "lb", "lbs": "lb",
    # countable
    "pcs": "pcs", "pc": "pcs", "piece": "pcs", "pieces": "pcs",
    "pack": "pack", "packs": "pack", "pk": "pack",
    "sachet": "sachet", "sachets": "sachet",
    "rolls": "rolls", "roll": "rolls",
    "sheets": "sheets", "sheet": "sheets",
    "tablets": "tablets", "tablet": "tablets", "tab": "tablets", "tabs": "tablets",
    "cap": "caps", "caps": "caps", "capsule": "caps", "capsules": "caps",
}

# --- Conversion factors to a single base unit -----------------------
#     weight → grams,  volume → millilitres
WEIGHT_TO_GRAMS: dict[str, float] = {
    "g": 1.0,
    "kg": 1_000.0,
    "oz": 28.3495,
    "lb": 453.592,
}

VOLUME_TO_ML: dict[str, float] = {
    "ml": 1.0,
    "l": 1_000.0,
}

# --- Brand normalisation alias table --------------------------------
#     Maps every known spelling variant (lowercased) to the canonical form.
BRAND_ALIASES: dict[str, str] = {
    # Nestle variants
    "nestle": "Nestle", "nestlé": "Nestle", "nestlè": "Nestle",
    "nestle's": "Nestle", "nestlé's": "Nestle", "néstle": "Nestle",
    # K&Ns variants
    "k&n": "K&Ns", "k&n's": "K&Ns", "k&ns": "K&Ns", "k & n": "K&Ns",
    "k and n": "K&Ns", "k&ns's": "K&Ns", "kandns": "K&Ns",
    # Coca-Cola variants
    "coca-cola": "Coca-Cola", "coca cola": "Coca-Cola", "cocacola": "Coca-Cola",
    "coke": "Coca-Cola",
    # Pepsi variants
    "pepsi": "Pepsi", "pepsi co": "Pepsi", "pepsico": "Pepsi",
    # Unilever variants
    "unilever": "Unilever", "uni lever": "Unilever",
    # Head & Shoulders
    "head & shoulders": "Head & Shoulders", "head and shoulders": "Head & Shoulders",
    "head&shoulders": "Head & Shoulders",
    # Fair & Lovely → Glow & Lovely (rebranded)
    "fair & lovely": "Glow & Lovely", "fair and lovely": "Glow & Lovely",
    "glow & lovely": "Glow & Lovely", "glow and lovely": "Glow & Lovely",
    # Peek Freans
    "peek freans": "Peek Freans", "peek freens": "Peek Freans",
    "peekfreans": "Peek Freans",
    # Shangrila variants
    "shangrila": "Shangrila", "shangri la": "Shangrila", "shangri-la": "Shangrila",
    # Mountain Dew
    "mountain dew": "Mountain Dew", "mtn dew": "Mountain Dew",
    # Rooh Afza
    "rooh afza": "Rooh Afza", "roohafza": "Rooh Afza", "rooh-afza": "Rooh Afza",
    # 7Up
    "7up": "7Up", "7 up": "7Up", "seven up": "7Up",
}

# Reference list of canonical brand names for first-word fallback matching
KNOWN_BRANDS = [
    "Nestle", "Olpers", "Tapal", "Lipton", "Shan", "National",
    "Unilever", "K&Ns", "Dawn", "Knorr", "Rafhan", "Dalda",
    "Sufi", "Habib", "Nurpur", "Haleeb", "Pakola", "Pepsi",
    "Coca-Cola", "Sprite", "Fanta", "7Up", "Mountain Dew",
    "Lays", "Kurkure", "Pringles", "Colgate", "Surf", "Ariel",
    "Dettol", "Safeguard", "Lux", "Dove", "Head & Shoulders",
    "Pantene", "Sunsilk", "Garnier", "Nivea", "Glow & Lovely",
    "Mezan", "Eva", "Engro", "Gourmet", "Peek Freans", "LU",
    "Bisconni", "Candyland", "Hilal", "Mitchell", "Shangrila",
    "Young's", "PK", "Rooh Afza", "Tang", "Milo", "Nido",
    "Everyday", "Tarang", "Good Milk", "Millac", "Adams",
]

# --- Currency token map → canonical string --------------------------
CURRENCY_ALIASES: dict[str, str] = {
    "rs": "PKR", "rs.": "PKR", "pkr": "PKR", "rupees": "PKR",
    "rupee": "PKR", "/-": "PKR", "pak rs": "PKR", "pak rs.": "PKR",
    "\u20a8": "PKR",    # ₨  Rupee sign
    "\u20a8.": "PKR",
    "rs/-": "PKR", "rs /-": "PKR",
}

# --- Regex for extracting quantity + unit from free text -------------
_QTY_RE = re.compile(
    r"""
    (\d+(?:\.\d+)?)          # numeric value
    \s*[xX×]?\s*             # optional multiplier sign
    (g|gm|gms|gram|grams|grm
    |kg|kgs|kilo|kilos|kilogram|kilograms
    |ml|millilitre|milliliter
    |l|ltr|ltrs|litre|litres|liter|liters
    |oz|fl\s?oz|lb|lbs
    |pcs|pc|piece|pieces
    |pack|packs|pk
    |sachet|sachets
    |rolls?|sheets?|tablets?|tabs?|caps?|capsules?)
    s?                        # optional trailing 's'
    \b
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Optional "x N" multiplier AFTER the unit  (e.g. "250ml x 12")
_MULTI_RE = re.compile(
    r"""
    (\d+(?:\.\d+)?)          # quantity
    \s*
    (g|gm|gms|gram|grams|grm|kg|kgs|ml|l|ltr|ltrs|litre|litres|liter|liters
     |oz|lb|lbs|pcs|pc|pack|packs|pk|sachet|sachets|rolls?|sheets?
     |tablets?|tabs?|caps?|capsules?)
    s?\s*[xX×]\s*
    (\d+)                    # pack count
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Pre-multiplier "N x Qty Unit"  (e.g. "12 x 250ml", "6x500g")
_PRE_MULTI_RE = re.compile(
    r"""
    (\d+)                    # pack count
    \s*[xX×]\s*
    (\d+(?:\.\d+)?)          # quantity per unit
    \s*
    (g|gm|gms|gram|grams|grm|kg|kgs|ml|l|ltr|ltrs|litre|litres|liter|liters
     |oz|lb|lbs|pcs|pc|pack|packs|pk|sachet|sachets|rolls?|sheets?
     |tablets?|tabs?|caps?|capsules?)
    s?\b
    """,
    re.IGNORECASE | re.VERBOSE,
)


# =====================================================================
# STAGE 1  –  LOAD RAW FILES
# =====================================================================

def _load_raw_files() -> pd.DataFrame:
    """Read every Parquet and CSV in data/raw/ into one DataFrame."""
    frames: list[pd.DataFrame] = []
    for pattern in ("*.parquet", "*.csv"):
        for path in config.RAW_DIR.glob(pattern):
            logger.info("Loading %s", path.name)
            if path.suffix == ".parquet":
                frames.append(pd.read_parquet(path))
            else:
                frames.append(pd.read_csv(path, encoding=config.CSV_ENCODING))
    if not frames:
        logger.error("No raw data files found in %s", config.RAW_DIR)
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    logger.info("Loaded %d raw rows from %d file(s).", len(df), len(frames))
    return df


# =====================================================================
# STAGE 2  –  CONVERT PRICES TO NUMERIC  (Task 8)
# =====================================================================

def _strip_accents(text: str) -> str:
    """Remove diacritics / accent marks from text (Nestlé → Nestle)."""
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nfkd if not unicodedata.combining(ch))


def _clean_text(s) -> str:
    """Collapse whitespace, strip leading/trailing blanks."""
    if not isinstance(s, str):
        return ""
    return re.sub(r"\s+", " ", s).strip()


def convert_prices_to_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 8 — Convert all prices to numeric format.

    Raw scraped prices may contain currency symbols, commas, or text
    (``Rs. 1,250.00``, ``PKR 899/-``).  This step strips everything
    except digits and the decimal point, then casts to float.
    Rows with un-parseable or non-positive prices are dropped.
    """
    if "price" not in df.columns:
        return df

    def _to_float(val) -> Optional[float]:
        if pd.isna(val):
            return None
        s = str(val).replace(",", "")
        # Remove currency words / symbols but keep digits and dots
        s = re.sub(r"[^\d.]", "", s)
        # Handle multiple dots (e.g. "Rs." leaves a stray dot → ".1250.00")
        # Keep only the rightmost dot as the decimal separator
        parts = s.split(".")
        if len(parts) > 2:
            # Rejoin: everything before the last dot is integer part
            s = "".join(parts[:-1]) + "." + parts[-1]
        s = s.strip(".")
        if not s:
            return None
        try:
            v = float(s)
            return v if v > 0 else None
        except ValueError:
            return None

    before = len(df)
    df["price"] = df["price"].apply(_to_float)
    df = df.dropna(subset=["price"])
    logger.info(
        "Stage 2 — prices → numeric: %d → %d rows (%d dropped as non-positive/unparseable).",
        before, len(df), before - len(df),
    )
    return df


# =====================================================================
# STAGE 2b –  FLAG PLACEHOLDER PRICES & MARK AVAILABILITY STATUS
# =====================================================================

# Hard ceiling: any price at or below this is treated as an API
# placeholder (e.g. Metro returns sell_price=1 for sold-out items).
# These are NEVER real retail prices.
PLACEHOLDER_PRICE_CEILING: float = 1.0

# Soft floor: prices above the ceiling but at or below this value are
# checked against category-aware rules.  Legitimate cheap items (small
# sachets, bags, per-250 g produce) live in this band.
SUSPICIOUS_PRICE_FLOOR: float = 10.0

# Categories whose products can genuinely cost <= SUSPICIOUS_PRICE_FLOOR.
# Matching is case-insensitive substring search on the category column.
_LOW_PRICE_CATEGORIES = {
    "vegetables", "fruits", "fresh food",       # per-250 g produce
    "shopping bag", "reusable bag",             # store bags
    "sachet",                                   # single-use sachets
    "pos", "service fee",                       # service entries
    "biscuit", "jelly", "detergent",            # tiny single packs
    "snacks",                                   # small chips packs
}


def flag_placeholder_prices(df: pd.DataFrame) -> pd.DataFrame:
    """
    Stage 2b - Detect placeholder / anomalous prices and add an
    ``availability_status`` column.

    Logic (applied per row):

    1. **price <= 1 PKR** → always a placeholder.  Marked as
       ``"Sold Out / Unavailable"`` and **removed** from the
       clean dataset.

    2. **1 < price <= 10 PKR** → checked against category-aware
       rules.  If the product's category or name does NOT match any
       known low-price category, it is flagged as
       ``"Suspect - Placeholder"`` and removed.  Otherwise it is
       kept as ``"Available"``.

    3. **price > 10 PKR** → accepted, marked ``"Available"``.

    Removed rows are logged at WARNING level so they can be audited.
    The cleaned dataset returned by this function contains only rows
    with realistic, analysis-ready prices.
    """
    if "price" not in df.columns:
        return df

    before = len(df)

    # --- helper: does this row's category / name belong to a
    #     legitimately cheap product group? ---
    def _matches_low_price_category(row) -> bool:
        text = " ".join([
            str(row.get("category", "")),
            str(row.get("product_name", "")),
        ]).lower()
        return any(cat in text for cat in _LOW_PRICE_CATEGORIES)

    # --- classify every row ---
    status = pd.Series("Available", index=df.index)

    # Rule 1: hard placeholder (price <= 1)
    is_placeholder = df["price"] <= PLACEHOLDER_PRICE_CEILING
    status[is_placeholder] = "Sold Out / Unavailable"

    # Rule 2: suspicious band (1 < price <= 10) without a matching
    #         cheap-product category
    in_suspect_band = (df["price"] > PLACEHOLDER_PRICE_CEILING) & (
        df["price"] <= SUSPICIOUS_PRICE_FLOOR
    )
    if in_suspect_band.any():
        has_legit_category = df[in_suspect_band].apply(
            _matches_low_price_category, axis=1
        )
        suspect_idx = has_legit_category[~has_legit_category].index
        status[suspect_idx] = "Suspect - Placeholder"

    df["availability_status"] = status

    # --- log flagged rows ---
    flagged_mask = df["availability_status"] != "Available"
    flagged = df[flagged_mask]
    if not flagged.empty:
        logger.warning(
            "Stage 2b — flagged %d rows as placeholder / unavailable:",
            len(flagged),
        )
        for _, row in (
            flagged[["store_name", "product_name", "price", "availability_status"]]
            .drop_duplicates()
            .iterrows()
        ):
            logger.warning(
                "  [%s] %s | Rs. %.1f | %s",
                row["availability_status"],
                row.get("store_name", "?"),
                row["price"],
                row["product_name"],
            )

    # --- remove flagged rows from the clean dataset ---
    df = df[~flagged_mask]
    after = len(df)
    logger.info(
        "Stage 2b — placeholder price filter: %d -> %d rows "
        "(%d removed: %d sold-out/unavailable, %d suspect).",
        before, after, before - after,
        is_placeholder.sum(),
        (status == "Suspect - Placeholder").sum(),
    )
    return df


# =====================================================================
# STAGE 3  –  NORMALIZE CURRENCY FORMATS  (Task 6)
# =====================================================================

def normalize_currency(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 6 — Normalize currency formats.

    Converts every representation of Pakistani Rupee
    (``Rs``, ``Rs.``, ``Rupees``, ``/-``) to the ISO code ``PKR``.
    If the column is missing entirely it is created and filled.
    """
    if "currency" not in df.columns:
        df["currency"] = "PKR"
        logger.info("Stage 3 — currency column created, set to PKR for all rows.")
        return df

    def _norm_currency(val) -> str:
        if pd.isna(val) or str(val).strip() == "":
            return "PKR"
        token = str(val).strip().lower()
        return CURRENCY_ALIASES.get(token, str(val).strip().upper())

    df["currency"] = df["currency"].apply(_norm_currency)
    counts = df["currency"].value_counts().to_dict()
    logger.info("Stage 3 — currency normalised. Distribution: %s", counts)
    return df


# =====================================================================
# STAGE 4  –  EXTRACT QUANTITY VIA REGEX  (Task 2)
# =====================================================================

def extract_quantity(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 2 — Extract quantity using regex.

    Scans both the ``size`` and ``product_name`` columns for patterns
    like ``500g``, ``1.5 ltr``, ``250 ml x 12``.  When a pack-multiplier
    is found (e.g. "×12") the total quantity is computed.

    New columns:
        qty_value   — numeric quantity  (float)
        qty_unit    — raw unit token as found in the text  (str)
        pack_count  — multiplier if present, else 1  (int)
        total_qty   — qty_value × pack_count  (float)
    """
    qty_values: list[Optional[float]] = []
    qty_units: list[str] = []
    pack_counts: list[int] = []

    # Prioritise the ``size`` column; fall back to ``product_name``
    has_size = "size" in df.columns
    has_name = "product_name" in df.columns

    for idx in range(len(df)):
        text_sources = []
        if has_size:
            text_sources.append(str(df.iat[idx, df.columns.get_loc("size")]))
        if has_name:
            text_sources.append(str(df.iat[idx, df.columns.get_loc("product_name")]))

        found_val: Optional[float] = None
        found_unit: str = ""
        found_pack: int = 1

        for text in text_sources:
            if found_val is not None:
                break

            # Try "250ml x 12" pattern first  (post-multiplier)
            mm = _MULTI_RE.search(text)
            if mm:
                found_val = float(mm.group(1))
                found_unit = mm.group(2).lower().rstrip("s")
                found_pack = int(mm.group(3))
                break

            # Try "12 x 250ml" pattern  (pre-multiplier)
            pm = _PRE_MULTI_RE.search(text)
            if pm:
                found_pack = int(pm.group(1))
                found_val = float(pm.group(2))
                found_unit = pm.group(3).lower().rstrip("s")
                break

            # Simple "500g" pattern
            m = _QTY_RE.search(text)
            if m:
                found_val = float(m.group(1))
                found_unit = m.group(2).lower().rstrip("s")
                break

        qty_values.append(found_val)
        qty_units.append(found_unit)
        pack_counts.append(found_pack)

    df["qty_value"] = qty_values
    df["qty_unit"] = qty_units
    df["pack_count"] = pack_counts
    df["total_qty"] = df["qty_value"].fillna(0) * df["pack_count"]
    df.loc[df["total_qty"] == 0, "total_qty"] = None

    extracted = df["qty_value"].notna().sum()
    logger.info(
        "Stage 4 — quantity extracted for %d / %d rows (%.1f%%).",
        extracted, len(df), 100 * extracted / max(len(df), 1),
    )
    return df


# =====================================================================
# STAGE 5  –  STANDARDIZE PRODUCT SIZES  (Task 1)
# =====================================================================

def standardize_sizes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 1 — Standardize product sizes.

    Converts every extracted quantity to a canonical **base unit**:
        • weight → grams   (500 g → 500 g;  1 kg → 1000 g;  8 oz → 226.8 g)
        • volume → ml      (1 L → 1000 ml;  250 ml → 250 ml)
        • countable units are kept as-is (pcs, pack, sachet, etc.)

    New columns:
        size_unit_canonical  — the canonical unit after synonym mapping
        size_base_value      — quantity converted to base grams or ml
        size_base_unit       — ``g`` | ``ml`` | original countable unit
    """
    canonical_units: list[str] = []
    base_values: list[Optional[float]] = []
    base_units: list[str] = []

    qty_vals = df["qty_value"] if "qty_value" in df.columns else pd.Series([None] * len(df))
    qty_raw_units = df["qty_unit"] if "qty_unit" in df.columns else pd.Series([""] * len(df))
    pack_counts = df["pack_count"] if "pack_count" in df.columns else pd.Series([1] * len(df))

    for i in range(len(df)):
        raw_u = str(qty_raw_units.iat[i]).lower().rstrip("s") if pd.notna(qty_raw_units.iat[i]) else ""
        canon = UNIT_MAP.get(raw_u, raw_u)
        canonical_units.append(canon)

        val = qty_vals.iat[i]
        pc = pack_counts.iat[i] if pd.notna(pack_counts.iat[i]) else 1
        total = val * pc if pd.notna(val) and val > 0 else None

        if total is None:
            base_values.append(None)
            base_units.append("")
        elif canon in WEIGHT_TO_GRAMS:
            base_values.append(round(total * WEIGHT_TO_GRAMS[canon], 4))
            base_units.append("g")
        elif canon in VOLUME_TO_ML:
            base_values.append(round(total * VOLUME_TO_ML[canon], 4))
            base_units.append("ml")
        else:
            base_values.append(total)
            base_units.append(canon)

    df["size_unit_canonical"] = canonical_units
    df["size_base_value"] = base_values
    df["size_base_unit"] = base_units

    converted = sum(1 for v in base_values if v is not None)
    logger.info(
        "Stage 5 — sizes standardised for %d / %d rows. "
        "Examples: 1 kg → 1000 g, 1 L → 1000 ml.",
        converted, len(df),
    )
    return df


# =====================================================================
# STAGE 6  –  NORMALIZE BRAND NAMES  (Task 3)
# =====================================================================

def normalize_brands(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 3 — Normalize brand names.

    Three-step strategy:
      1. Strip diacritics and lowercase → look up in ``BRAND_ALIASES``.
      2. If no alias hit, do a case-insensitive substring search against
         ``KNOWN_BRANDS``.
      3. If still unmatched, Title-Case the raw value as a best effort.

    New column:  ``brand_normalised``
    """
    if "brand" not in df.columns:
        logger.warning("Stage 6 — no 'brand' column; skipping.")
        return df

    def _norm(raw) -> str:
        if pd.isna(raw) or str(raw).strip() == "":
            return ""
        text = _strip_accents(str(raw)).strip()
        key = text.lower()

        # 1) Exact alias lookup
        if key in BRAND_ALIASES:
            return BRAND_ALIASES[key]

        # 2) Substring match against known brands
        for brand in KNOWN_BRANDS:
            if brand.lower() in key or key in brand.lower():
                return brand

        # 3) Fallback — Title-Case to unify capitalisation
        return text.title()

    df["brand_normalised"] = df["brand"].apply(_norm)

    n_aliased = (df["brand_normalised"] != df["brand"].fillna("").str.strip().str.title()).sum()
    logger.info(
        "Stage 6 — brands normalised. %d rows had their brand corrected via alias/match.",
        n_aliased,
    )
    return df


# =====================================================================
# STAGE 7  –  HANDLE MISSING VALUES  (Task 5)
# =====================================================================

def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 5 — Handle missing values.

    Strategy per column:
        store_name   → drop row (critical identifier)
        product_name → drop row (unusable without a name)
        price        → drop row (already handled in Stage 2)
        city         → fill with "Unknown"
        brand / brand_normalised → fill with "Unknown"
        category     → fill with "Uncategorised"
        currency     → fill with "PKR"
        size, qty_value, size_base_value → leave as NaN (legitimately absent)
        product_url, image_url → fill with ""
        scrape_timestamp → fill with current UTC
    """
    before = len(df)

    # ── Detect & report missing values per column ───────────────────
    logger.info("Stage 7 — Missing value detection (before handling):")
    for col in df.columns:
        n_miss = df[col].isna().sum()
        n_blank = (df[col].astype(str).str.strip() == "").sum() if df[col].dtype == object else 0
        total = n_miss + n_blank
        if total > 0:
            logger.info(
                "  %-25s  %d missing + %d blank  (%.1f%%)",
                col, n_miss, n_blank, 100 * total / max(len(df), 1),
            )

    # Drop rows missing critical fields
    critical = [c for c in ("store_name", "product_name") if c in df.columns]
    if critical:
        df = df.dropna(subset=critical)
        df = df[~df[critical].apply(lambda col: col.astype(str).str.strip().eq("")).any(axis=1)]

    # Column-specific fills
    fill_map = {
        "city": "Unknown",
        "brand": "Unknown",
        "brand_normalised": "Unknown",
        "category": "Uncategorised",
        "currency": "PKR",
        "product_url": "",
        "image_url": "",
    }
    for col, fill_val in fill_map.items():
        if col in df.columns:
            df[col] = df[col].fillna(fill_val)
            # Also fill blank strings
            df.loc[df[col].astype(str).str.strip() == "", col] = fill_val

    if "scrape_timestamp" in df.columns:
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat(timespec="seconds")
        df["scrape_timestamp"] = df["scrape_timestamp"].fillna(now)

    after = len(df)
    logger.info(
        "Stage 7 — missing values handled. %d rows dropped (missing name/store), "
        "%d rows retained.",
        before - after, after,
    )
    return df


# =====================================================================
# STAGE 8  –  REMOVE DUPLICATE RECORDS  (Task 4)
# =====================================================================

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 4 — Remove duplicate records.

    Two-pass deduplication:

    Pass 1 — **Exact duplicates** on the composite key
             (store_name, city, product_name, price).  Keeps the first
             occurrence.

    Pass 2 — **Near duplicates** where only the cleaned product name,
             store, and city match (catches price-formatting differences
             within the same scrape run).  Keeps the row with the lowest
             price (best deal).
    """
    before = len(df)

    # Pass 1: exact key dedup
    exact_cols = [c for c in ("store_name", "city", "product_name", "price") if c in df.columns]
    if exact_cols:
        df = df.drop_duplicates(subset=exact_cols, keep="first")
    after_p1 = len(df)

    # Pass 2: near-dedup on cleaned name
    if "product_name" in df.columns:
        df["_dedup_key"] = (
            df["product_name"]
            .apply(lambda s: _strip_accents(str(s)) if pd.notna(s) else "")
            .str.lower()
            .str.replace(r"[^a-z0-9]", "", regex=True)
        )
        near_cols = [c for c in ("store_name", "city", "_dedup_key") if c in df.columns]
        if "price" in df.columns:
            df = df.sort_values("price", ascending=True)
        df = df.drop_duplicates(subset=near_cols, keep="first")
        df = df.drop(columns=["_dedup_key"])
    after_p2 = len(df)

    logger.info(
        "Stage 8 — deduplication: %d → %d → %d rows "
        "(pass 1 removed %d exact, pass 2 removed %d near-dupes).",
        before, after_p1, after_p2,
        before - after_p1, after_p1 - after_p2,
    )
    return df


# =====================================================================
# STAGE 9  –  COMPUTE PRICE PER UNIT  (Task 7)
# =====================================================================

def compute_price_per_unit(df: pd.DataFrame) -> pd.DataFrame:
    """
    Task 7 — Compute price per unit.

    Uses the standardised base quantity from Stage 5 to compute
    ``price_per_unit`` = price ÷ size_base_value.

    For weight products this yields **PKR per gram**; for volume products
    **PKR per ml**.  A separate ``price_per_kg_or_litre`` column scales
    this up to per-kg / per-litre for easier human comparison.
    """
    needed = ("price", "size_base_value", "size_base_unit")
    if not all(c in df.columns for c in needed):
        logger.warning("Stage 9 — missing columns for price-per-unit; skipping.")
        return df

    def _ppu(row):
        p = row["price"]
        q = row["size_base_value"]
        if pd.isna(p) or pd.isna(q) or q <= 0:
            return None
        return round(p / q, 4)

    def _ppu_scaled(row):
        ppu = row.get("price_per_unit")
        unit = row.get("size_base_unit", "")
        if pd.isna(ppu) or ppu is None:
            return None
        if unit == "g":
            return round(ppu * 1000, 2)   # → PKR per kg
        if unit == "ml":
            return round(ppu * 1000, 2)   # → PKR per litre
        return round(ppu, 2)

    df["price_per_unit"] = df.apply(_ppu, axis=1)
    df["price_per_kg_or_litre"] = df.apply(_ppu_scaled, axis=1)

    computed = df["price_per_unit"].notna().sum()
    logger.info(
        "Stage 9 — price-per-unit computed for %d / %d rows (%.1f%%).",
        computed, len(df), 100 * computed / max(len(df), 1),
    )
    return df


# =====================================================================
# STAGE 10  –  CLEANED PRODUCT-NAME COLUMN
# =====================================================================

def build_clean_product_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a lowercased, punctuation-stripped ``product_name_clean``
    column used downstream for fuzzy matching and analysis.
    """
    if "product_name" not in df.columns:
        return df
    df["product_name_clean"] = (
        df["product_name"]
        .apply(lambda s: _strip_accents(str(s)) if pd.notna(s) else "")
        .str.lower()
        .str.replace(r"[^a-z0-9\s]", " ", regex=True)
        .str.replace(r"\s+", " ", regex=True)
        .str.strip()
    )
    return df


# =====================================================================
# ORCHESTRATION
# =====================================================================

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run all cleaning stages in order.  Returns the cleaned DataFrame.

    Pipeline:
        Stage 2  → convert_prices_to_numeric
        Stage 3  → normalize_currency
             (clean text fields)
        Stage 4  → extract_quantity
        Stage 5  → standardize_sizes
        Stage 6  → normalize_brands
        Stage 7  → handle_missing_values
        Stage 8  → remove_duplicates
        Stage 9  → compute_price_per_unit
        Stage 10 → build_clean_product_name
    """
    original = len(df)
    logger.info("=" * 60)
    logger.info("CLEANING PIPELINE START — %d raw rows", original)
    logger.info("=" * 60)

    # ── pre-clean text columns ──────────────────────────────────────
    for col in ("product_name", "brand", "category", "size"):
        if col in df.columns:
            df[col] = df[col].fillna("").apply(_clean_text)

    # ── Stage 2: prices → numeric ──────────────────────────────────
    df = convert_prices_to_numeric(df)

    # ── Stage 2b: flag placeholder prices & mark availability ────
    df = flag_placeholder_prices(df)

    # ── Stage 3: currency normalisation ────────────────────────────
    df = normalize_currency(df)

    # ── Stage 4: extract quantity via regex ────────────────────────
    df = extract_quantity(df)

    # ── Stage 5: standardize sizes ────────────────────────────────
    df = standardize_sizes(df)

    # ── Stage 6: normalize brands ─────────────────────────────────
    df = normalize_brands(df)

    # ── Stage 7: handle missing values ────────────────────────────
    df = handle_missing_values(df)

    # ── Stage 8: remove duplicates ────────────────────────────────
    df = remove_duplicates(df)

    # ── Stage 9: price per unit ───────────────────────────────────
    df = compute_price_per_unit(df)

    # ── Stage 10: cleaned product name ────────────────────────────
    df = build_clean_product_name(df)

    df = df.reset_index(drop=True)

    logger.info("=" * 60)
    logger.info(
        "CLEANING PIPELINE DONE — %d → %d rows (%.1f%% kept)",
        original, len(df), 100 * len(df) / max(original, 1),
    )
    logger.info("Columns: %s", list(df.columns))
    logger.info("=" * 60)
    return df


def save_processed(df: pd.DataFrame):
    """Write the cleaned DataFrame to data/processed/."""
    if config.OUTPUT_FORMAT == "parquet":
        out = config.PROCESSED_DIR / "products_processed.parquet"
        df.to_parquet(out, index=False, engine="pyarrow")
    else:
        out = config.PROCESSED_DIR / "products_processed.csv"
        df.to_csv(out, index=False, encoding=config.CSV_ENCODING)
    logger.info("Processed data → %s  (%d rows)", out.name, len(df))


def process_all() -> pd.DataFrame:
    """Load raw → clean → save processed → return DataFrame."""
    df = _load_raw_files()
    if df.empty:
        return df
    df = clean_dataframe(df)
    save_processed(df)
    return df


# =====================================================================
# CLI
# =====================================================================

if __name__ == "__main__":
    result = process_all()
    print(f"Processing done — {len(result)} clean rows.")
    if not result.empty:
        print("\nColumn dtypes:")
        print(result.dtypes.to_string())
        print(f"\nSample (first 5 rows):\n{result.head()}")

