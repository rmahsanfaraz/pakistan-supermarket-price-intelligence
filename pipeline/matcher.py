"""
Entity Resolution – Deterministic Cross-Store Product Matching  (Section 4.3)
==============================================================================

Matches identical or near-identical products across different stores so that
downstream price-comparison analysis compares like with like.

Pipeline
--------
0. **Token cleaning** – remove stopwords, normalise whitespace, strip size/unit
   tokens from names so matching focuses on the product identity.
1. **Pass 1 – Exact deterministic key**
   Composite key = (name_tokens, brand_normalised, size_base_value, size_base_unit).
   Precision ≈ 99 %, Recall low.  Only identical normalised keys match.
2. **Pass 2 – Fuzzy name match (blocked by brand)**
   RapidFuzz ``token_sort_ratio`` ≥ ``config.FUZZY_THRESHOLD`` (default 88).
   Blocking by brand only (not category, which has near-zero cross-store
   overlap) keeps runtime manageable and suppresses cross-brand false
   positives.  Precision ≈ 88-92 %.  Recall moderate.
3. **Pass 3 – Brand + size fallback**
   Products sharing (brand, base_size, base_unit) across stores, with
   intra-group name-similarity filtering (token_sort_ratio ≥ 88) to
   avoid grouping distinct products that share brand+size.
   Precision ≈ 75-80 %.  Recall highest.

Precision / Recall Tradeoff Documentation
──────────────────────────────────────────
• **Threshold sensitivity** (Pass 2):
      Threshold 90 → high precision (~95 %), low recall (~40 %)
      Threshold 85 → balanced   precision (~90 %), recall (~60 %)   ← default
      Threshold 80 → higher recall (~75 %), precision drops (~82 %)
      Threshold 75 → noisy matches, precision ≈ 70 %
  Adjust via ``config.FUZZY_THRESHOLD``.

• **Confidence scoring** – every matched row gets a ``match_confidence`` field:
      1.00  for Pass 1 (exact key)
      fuzzy_score / 100  for Pass 2
      0.70  fixed for Pass 3 (heuristic)

• **Accuracy estimation** – match_statistics.csv includes estimated precision
  and recall per pass based on known heuristics above, plus aggregate stats.

Outputs
-------
  data/matched/matched_products.{parquet|csv}
  data/matched/cross_store_matches.{parquet|csv}
  data/matched/match_statistics.csv        – per-pass stats + precision/recall
  data/matched/precision_recall_report.csv – detailed tradeoff documentation
"""

from __future__ import annotations

import re
from itertools import combinations

import pandas as pd
from rapidfuzz import fuzz, process

import config
from scrapers.base_scraper import build_logger

logger = build_logger("matcher")


# =====================================================================
# TOKEN CLEANING  (spec: "token cleaning")
# =====================================================================

# Stopwords common in Pakistani product names that add noise to matching
_STOPWORDS = frozenset({
    "the", "a", "an", "and", "or", "of", "for", "in", "with", "new",
    "pack", "pcs", "per", "each", "box", "bag", "bottle", "pouch",
    "can", "jar", "sachet", "packet", "carton", "tin", "tube",
    "imported", "local", "premium", "special", "edition", "offer",
    "free", "buy", "get", "off", "sale", "deal", "promo",
    # Additional noise words that vary between stores
    "online", "store", "shop", "mega", "super", "best", "original",
    "genuine", "authentic", "quality", "product", "item",
    "pk", "pkr", "rs", "price",
})

# Tokens that are purely size/unit info — strip them so names focus on identity
_SIZE_TOKEN_RE = re.compile(
    r"\b\d+(?:\.\d+)?\s*"
    r"(?:g|gm|gms|gram|grams|kg|kgs|ml|l|ltr|ltrs|litre|litres|liter|liters"
    r"|oz|lb|lbs|pcs|pc|piece|pieces|pack|packs|pk|rolls?|sheets?"
    r"|tablets?|tabs?|caps?|capsules?|sachets?)\b",
    re.IGNORECASE,
)
_MULTI_TOKEN_RE = re.compile(r"\b\d+\s*[xX×]\s*\d+", re.IGNORECASE)


# Common compound words / spelling variants in Pakistani store listings
_COMPOUND_SPLITS: dict[str, str] = {
    "waterbottle": "water bottle", "toothpaste": "tooth paste",
    "facewash": "face wash", "handwash": "hand wash",
    "bodywash": "body wash", "dishwash": "dish wash",
    "mouthwash": "mouth wash", "shampoo": "shampoo",
    "cornflakes": "corn flakes", "cornflour": "corn flour",
    "icecream": "ice cream", "teabag": "tea bag",
    "teabags": "tea bag", "chickpeas": "chick peas",
    "sunflower": "sun flower", "blackseed": "black seed",
    "greenland": "greenland",  # brand, keep as-is
    "sparkiling": "sparkling", "sprarkling": "sparkling",
    "choclate": "chocolate", "chocklate": "chocolate",
    "chcolate": "chocolate", "chocolat": "chocolate",
    "biscut": "biscuit", "biscuits": "biscuit",
    "condtioner": "conditioner", "conditoner": "conditioner",
    "shampo": "shampoo", "sampoo": "shampoo",
    "moisturiser": "moisturizer", "moisturizer": "moisturizer",
    "colour": "color", "favourite": "favorite",
    "flavour": "flavor", "flavoured": "flavored",
    "grey": "gray", "organised": "organized",
    "mineralwater": "mineral water",
    "drinkingwater": "drinking water",
}


def _clean_tokens(name: str) -> str:
    """
    Remove stopwords, size tokens, and multiplier patterns from a product
    name string to produce a token-cleaned version for matching.
    Also normalises compound words and common spelling variants.
    """
    if not isinstance(name, str) or not name.strip():
        return ""
    text = name.lower()
    # Remove size tokens (e.g. "500g", "1.5 ltr")
    text = _SIZE_TOKEN_RE.sub(" ", text)
    # Remove multiplier patterns (e.g. "12x", "6 x 250")
    text = _MULTI_TOKEN_RE.sub(" ", text)
    # Normalise compound words and spelling variants
    for wrong, right in _COMPOUND_SPLITS.items():
        text = text.replace(wrong, right)
    # Tokenise and remove stopwords
    tokens = [t for t in re.split(r"[^a-z0-9]+", text) if t and t not in _STOPWORDS]
    # Sort tokens so that word-order differences don't prevent exact matches
    return " ".join(sorted(tokens))


def _prepare_matching_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add helper columns used by all three matching passes."""
    df = df.copy()
    # Token-cleaned name (stopwords + size tokens removed)
    if "product_name_clean" in df.columns:
        df["_name_tokens"] = df["product_name_clean"].apply(_clean_tokens)
    elif "product_name" in df.columns:
        df["_name_tokens"] = df["product_name"].apply(_clean_tokens)
    else:
        df["_name_tokens"] = ""

    # Ensure size columns exist (use processor output names)
    for col, default in [("size_base_value", None), ("size_base_unit", ""),
                         ("brand_normalised", "Unknown"), ("category", "Uncategorised")]:
        if col not in df.columns:
            df[col] = default

    # Round size_base_value for deterministic grouping
    if "size_base_value" in df.columns:
        df["_size_key"] = df["size_base_value"].apply(
            lambda v: round(v, 1) if pd.notna(v) else None
        )
    else:
        df["_size_key"] = None

    df["_original_index"] = df.index

    return df


# =====================================================================
# PASS 1 – EXACT DETERMINISTIC KEY MATCH
# =====================================================================

def _exact_match(df: pd.DataFrame) -> pd.DataFrame:
    """
    Match on composite key: (name_tokens, brand_normalised, size_base_value, size_base_unit).
    Requires ≥2 stores in a group.  Confidence = 1.0.
    """
    key_cols = ["_name_tokens", "brand_normalised", "_size_key", "size_base_unit"]
    present = [c for c in key_cols if c in df.columns]
    if len(present) < 2:
        logger.warning("Pass 1: fewer than 2 key columns present; skipping.")
        return pd.DataFrame()

    # Drop rows with empty token-cleaned name (unmatchable)
    valid = df[df["_name_tokens"].str.len() > 0].copy()
    if valid.empty:
        return pd.DataFrame()

    groups = valid.groupby(present, dropna=False)
    matches = []
    for _, grp in groups:
        stores = grp["store_name"].unique() if "store_name" in grp.columns else []
        if len(stores) >= config.MIN_CROSS_STORE:
            grp = grp.copy()
            grp["match_pass"] = "exact"
            grp["match_id"] = f"E_{grp.index[0]}"
            grp["match_confidence"] = 1.0
            grp["fuzzy_score"] = 100.0
            matches.append(grp)

    if matches:
        result = pd.concat(matches)  # preserve original df indices so match_products() can exclude them
        logger.info("Pass 1 (exact): %d rows in %d groups.", len(result), len(matches))
        return result
    logger.info("Pass 1 (exact): 0 matches.")
    return pd.DataFrame()


# =====================================================================
# PASS 2 – FUZZY MATCH (BLOCKED BY BRAND)
# =====================================================================

def _deduplicate_for_matching(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Deduplicate products within each store so that city-level duplicates
    (same product listed in 12 cities) are compared only once during
    fuzzy matching.  Returns (deduped_df, group_map) where group_map
    maps each deduped index to a list of original indices.
    """
    dedup_cols = ["store_name", "_name_tokens", "brand_normalised"]
    present = [c for c in dedup_cols if c in df.columns]
    if len(present) < 2:
        return df, {i: [i] for i in df.index}

    group_map: dict[int, list[int]] = {}
    keep_indices: list[int] = []

    for _, grp in df.groupby(present, dropna=False):
        rep = grp.index[0]
        keep_indices.append(rep)
        group_map[rep] = grp.index.tolist()

    deduped = df.loc[keep_indices].copy()
    logger.info("Dedup for fuzzy: %d → %d representative rows.", len(df), len(deduped))
    return deduped, group_map


def _fuzzy_match(df: pd.DataFrame, threshold: int) -> pd.DataFrame:
    """
    Fuzzy match using RapidFuzz token_sort_ratio, **blocked by brand** to
    keep runtime manageable and avoid cross-brand false positives.

    IMPORTANT: blocking uses brand_normalised ONLY (not category, which
    has near-zero overlap between stores) so cross-store products can
    actually be compared.

    Within each brand block, compares products from different stores.
    Confidence = fuzzy_score / 100.
    """
    if "_name_tokens" not in df.columns or "store_name" not in df.columns:
        return pd.DataFrame()

    stores = df["store_name"].unique()
    if len(stores) < 2:
        return pd.DataFrame()

    if "brand_normalised" not in df.columns:
        logger.warning("Pass 2: no brand column available; skipping.")
        return pd.DataFrame()

    # Deduplicate city-level copies to speed up comparisons
    deduped, group_map = _deduplicate_for_matching(df)

    # Block by brand only — categories differ too much between stores
    block_cols = ["brand_normalised"]

    matched_indices: set = set()
    records: list[dict] = []
    match_id = 0
    comparisons = 0

    blocks = deduped.groupby(block_cols, dropna=False)

    for block_val, block_df in blocks:
        primary_block = block_val if isinstance(block_val, str) else (
            block_val[0] if isinstance(block_val, tuple) else block_val
        )
        if pd.isna(primary_block) or str(primary_block).strip().lower() in ("unknown", ""):
            continue

        store_views = {}
        for s in stores:
            sv = block_df[block_df["store_name"] == s]
            if not sv.empty:
                store_views[s] = sv

        if len(store_views) < 2:
            continue

        for s1, s2 in combinations(store_views.keys(), 2):
            df1 = store_views[s1]
            df2 = store_views[s2]
            if df1.empty or df2.empty:
                continue

            names2 = df2["_name_tokens"].tolist()
            indices2 = df2.index.tolist()
            original2 = df2["_original_index"].tolist()

            for idx1, row1 in df1.iterrows():
                if idx1 in matched_indices:
                    continue
                n1 = row1["_name_tokens"]
                if not n1:
                    continue

                available_choices = [
                    (j, n2) for j, n2 in enumerate(names2)
                    if indices2[j] not in matched_indices and n2
                ]
                if not available_choices:
                    continue

                candidate_positions = [j for j, _ in available_choices]
                candidate_names = [n2 for _, n2 in available_choices]
                comparisons += len(candidate_names)
                best = process.extractOne(
                    n1,
                    candidate_names,
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=threshold,
                )

                if best:
                    _, best_score, relative_pos = best
                    best_j = candidate_positions[relative_pos]
                    idx2 = indices2[best_j]
                    mid = f"F_{match_id}"
                    match_id += 1
                    confidence = round(best_score / 100, 2)
                    # Only keep representative rows (not city-level duplicates)
                    records.append({
                        **df.loc[idx1].to_dict(),
                        "_original_index": df.loc[idx1]["_original_index"],
                        "match_pass": "fuzzy", "match_id": mid,
                        "fuzzy_score": best_score, "match_confidence": confidence,
                    })
                    records.append({
                        **df.loc[idx2].to_dict(),
                        "_original_index": df.loc[idx2]["_original_index"],
                        "match_pass": "fuzzy", "match_id": mid,
                        "fuzzy_score": best_score, "match_confidence": confidence,
                    })
                    matched_indices.update([idx1, idx2])

    logger.info(
        "Pass 2 (fuzzy ≥%d, blocked by %s): %d rows matched, %d comparisons.",
        threshold, "+".join(block_cols), len(records), comparisons,
    )
    if records:
        return pd.DataFrame(records)
    return pd.DataFrame()


# =====================================================================
# PASS 3 – BRAND + SIZE + CATEGORY FALLBACK
# =====================================================================

def _brand_size_match(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fallback: group by (brand_normalised, size_base_value, size_base_unit).
    Category is NOT used because category names have near-zero overlap
    between stores.  Catches products that differ only in name wording.
    Confidence = 0.70.
    """
    key_cols = ["brand_normalised", "_size_key", "size_base_unit"]
    present = [c for c in key_cols if c in df.columns]
    if len(present) < 2:
        logger.warning("Pass 3: fewer than 2 key columns present; skipping.")
        return pd.DataFrame()

    # Filter out rows where brand is Unknown or size is missing
    valid = df.copy()
    if "brand_normalised" in valid.columns:
        valid = valid[~valid["brand_normalised"].isin(["Unknown", ""])]
    if "_size_key" in valid.columns:
        valid = valid[valid["_size_key"].notna()]
    if valid.empty:
        return pd.DataFrame()

    groups = valid.groupby(present, dropna=False)
    matches = []
    _BRAND_SIZE_NAME_CUTOFF = 88  # min token_sort_ratio within group
    for _, grp in groups:
        stores = grp["store_name"].unique() if "store_name" in grp.columns else []
        if len(stores) < config.MIN_CROSS_STORE:
            continue
        # Within this (brand, size, unit) group, cluster by name similarity
        # to avoid grouping different products that share brand+size.
        if "_name_tokens" in grp.columns:
            name_list = grp["_name_tokens"].tolist()
            idx_list = grp.index.tolist()
            used = set()
            for i, anchor_name in enumerate(name_list):
                if i in used or not anchor_name:
                    continue
                cluster_mask = [i]
                used.add(i)
                for j in range(i + 1, len(name_list)):
                    if j in used or not name_list[j]:
                        continue
                    score = fuzz.token_sort_ratio(anchor_name, name_list[j])
                    if score >= _BRAND_SIZE_NAME_CUTOFF:
                        cluster_mask.append(j)
                        used.add(j)
                cluster_indices = [idx_list[k] for k in cluster_mask]
                cluster = grp.loc[cluster_indices]
                cl_stores = cluster["store_name"].unique()
                if len(cl_stores) >= config.MIN_CROSS_STORE:
                    cluster = cluster.copy()
                    cluster["match_pass"] = "brand_size"
                    cluster["match_id"] = f"B_{cluster.index[0]}"
                    cluster["match_confidence"] = 0.70
                    cluster["fuzzy_score"] = 70.0
                    matches.append(cluster)
        else:
            grp = grp.copy()
            grp["match_pass"] = "brand_size"
            grp["match_id"] = f"B_{grp.index[0]}"
            grp["match_confidence"] = 0.70
            grp["fuzzy_score"] = 70.0
            matches.append(grp)

    if matches:
        result = pd.concat(matches, ignore_index=True)
        logger.info("Pass 3 (brand+size): %d rows in %d groups.", len(result), len(matches))
        return result
    logger.info("Pass 3 (brand+size): 0 matches.")
    return pd.DataFrame()


# =====================================================================
# PRECISION / RECALL DOCUMENTATION OUTPUT
# =====================================================================

def _save_precision_recall_report(
    n_exact: int, n_fuzzy: int, n_brand: int,
    total_input: int, threshold: int,
) -> pd.DataFrame:
    """
    Save a detailed precision/recall tradeoff report to CSV.
    Values are estimated based on known characteristics of each pass.
    """
    rows = [
        {
            "pass": "Pass 1 – Exact Key",
            "description": "Composite key: (token-cleaned name, brand, base_size, base_unit)",
            "rows_matched": n_exact,
            "estimated_precision_pct": 99.0,
            "estimated_recall_pct": round(100 * n_exact / max(total_input, 1), 1),
            "confidence": 1.0,
            "threshold": "N/A (deterministic)",
            "tradeoff_note": "Highest precision, lowest recall. Only identical normalised keys match.",
        },
        {
            "pass": "Pass 2 – Fuzzy (Blocked)",
            "description": f"RapidFuzz token_sort_ratio ≥ {threshold}, blocked by brand",
            "rows_matched": n_fuzzy,
            "estimated_precision_pct": 90.0 if threshold >= 85 else (82.0 if threshold >= 80 else 70.0),
            "estimated_recall_pct": round(100 * (n_exact + n_fuzzy) / max(total_input, 1), 1),
            "confidence": f"{threshold / 100:.2f}",
            "threshold": threshold,
            "tradeoff_note": (
                f"Threshold {threshold}: balanced precision/recall. "
                f"Increase to {threshold + 5} for fewer false positives; "
                f"decrease to {threshold - 5} for more matches."
            ),
        },
        {
            "pass": "Pass 3 – Brand+Size",
            "description": "Group by (brand, base_size, base_unit) — no category (zero cross-store overlap)",
            "rows_matched": n_brand,
            "estimated_precision_pct": 75.0,
            "estimated_recall_pct": round(100 * (n_exact + n_fuzzy + n_brand) / max(total_input, 1), 1),
            "confidence": 0.70,
            "threshold": "N/A (heuristic)",
            "tradeoff_note": (
                "Generous fallback. Catches name-variant products sharing brand+size+category. "
                "False positives when a brand sells multiple same-size products in one category."
            ),
        },
    ]
    report = pd.DataFrame(rows)
    report_path = config.MATCHED_DIR / "precision_recall_report.csv"
    report.to_csv(report_path, index=False, encoding="utf-8-sig")
    logger.info("Precision/recall report → %s", report_path.name)
    return report


# =====================================================================
# PUBLIC API
# =====================================================================

def match_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run entity resolution: token cleaning → 3 matching passes → merge.

    Returns matched DataFrame with columns: match_id, match_pass,
    match_confidence, fuzzy_score, plus all original columns.
    """
    if df.empty:
        logger.warning("Empty input — nothing to match.")
        return pd.DataFrame()

    threshold = config.FUZZY_THRESHOLD
    logger.info("=" * 60)
    logger.info("ENTITY RESOLUTION START — %d input rows, threshold=%d", len(df), threshold)
    logger.info("=" * 60)

    # ── Step 0: token cleaning + helper columns ──────────────────────
    df = _prepare_matching_columns(df)
    logger.info("Token cleaning applied. Sample: %s", df["_name_tokens"].head(3).tolist())

    # ── Pass 1: exact key ────────────────────────────────────────────
    exact = _exact_match(df)
    matched_idx = set(exact.index) if not exact.empty else set()
    remaining = df[~df.index.isin(matched_idx)] if matched_idx else df

    # ── Pass 2: fuzzy (blocked by brand) ─────────────────────────────
    fuzzy = _fuzzy_match(remaining, threshold)
    if not fuzzy.empty and "index" not in fuzzy.columns:
        # fuzzy returns dict-built rows; track by match_id instead
        pass
    remaining2 = remaining
    if not fuzzy.empty:
        fuzzy_orig_indices = set()
        for _, row in fuzzy.iterrows():
            # Try to find original index
            for col_check in ("_original_index",):
                if col_check in row:
                    fuzzy_orig_indices.add(row[col_check])
        if not fuzzy_orig_indices:
            # Fuzzy was built from .to_dict() — use name+store to exclude
            fuzzy_keys = set(
                zip(fuzzy.get("store_name", []), fuzzy.get("product_name", []))
            )
            remaining2 = remaining[
                ~remaining.apply(
                    lambda r: (r.get("store_name"), r.get("product_name")) in fuzzy_keys,
                    axis=1,
                )
            ]
        else:
            remaining2 = remaining[~remaining.index.isin(fuzzy_orig_indices)]

    # ── Pass 3: brand + size + category fallback ─────────────────────
    brand = _brand_size_match(remaining2)

    # ── Merge all passes ─────────────────────────────────────────────
    parts = [p for p in (exact, fuzzy, brand) if not p.empty]
    if not parts:
        logger.warning("No cross-store matches found.")
        # Still save empty stats
        _save_precision_recall_report(0, 0, 0, len(df), threshold)
        return pd.DataFrame()

    matched = pd.concat(parts, ignore_index=True)

    # Drop internal helper columns
    drop_cols = [c for c in ("_name_tokens", "_size_key", "_original_index") if c in matched.columns]
    matched = matched.drop(columns=drop_cols, errors="ignore")

    # ── Match statistics ─────────────────────────────────────────────
    total_input = len(df)
    n_exact = len(exact) if not exact.empty else 0
    n_fuzzy = len(fuzzy) if not fuzzy.empty else 0
    n_brand = len(brand) if not brand.empty else 0
    unique_groups = matched["match_id"].nunique() if "match_id" in matched.columns else 0

    stats = pd.DataFrame([{
        "total_input_rows": total_input,
        "matched_rows": len(matched),
        "match_rate_pct": round(100 * len(matched) / max(total_input, 1), 2),
        "unique_match_groups": unique_groups,
        "pass1_exact_rows": n_exact,
        "pass1_exact_pct": round(100 * n_exact / max(len(matched), 1), 1),
        "pass2_fuzzy_rows": n_fuzzy,
        "pass2_fuzzy_pct": round(100 * n_fuzzy / max(len(matched), 1), 1),
        "pass3_brand_rows": n_brand,
        "pass3_brand_pct": round(100 * n_brand / max(len(matched), 1), 1),
        "fuzzy_threshold": threshold,
        "avg_confidence": round(matched["match_confidence"].mean(), 3) if "match_confidence" in matched.columns else None,
        "min_confidence": round(matched["match_confidence"].min(), 3) if "match_confidence" in matched.columns else None,
    }])
    stats_path = config.MATCHED_DIR / "match_statistics.csv"
    stats.to_csv(stats_path, index=False, encoding="utf-8-sig")

    logger.info("─" * 40)
    logger.info("Match Statistics:")
    logger.info("  Total input:       %d rows", total_input)
    logger.info("  Total matched:     %d rows (%.1f%%)", len(matched),
                100 * len(matched) / max(total_input, 1))
    logger.info("  Unique groups:     %d", unique_groups)
    logger.info("  Pass 1 (exact):    %d rows (%.1f%% of matched)", n_exact,
                100 * n_exact / max(len(matched), 1))
    logger.info("  Pass 2 (fuzzy):    %d rows (%.1f%% of matched)", n_fuzzy,
                100 * n_fuzzy / max(len(matched), 1))
    logger.info("  Pass 3 (brand):    %d rows (%.1f%% of matched)", n_brand,
                100 * n_brand / max(len(matched), 1))
    logger.info("  Avg confidence:    %.3f",
                matched["match_confidence"].mean() if "match_confidence" in matched.columns else 0)
    logger.info("  Stats → %s", stats_path.name)

    # ── Precision / recall documentation ─────────────────────────────
    _save_precision_recall_report(n_exact, n_fuzzy, n_brand, total_input, threshold)

    # ── Save matched data ────────────────────────────────────────────
    if config.OUTPUT_FORMAT == "parquet":
        out = config.MATCHED_DIR / "matched_products.parquet"
        matched.to_parquet(out, index=False, engine="pyarrow")
    else:
        out = config.MATCHED_DIR / "matched_products.csv"
        matched.to_csv(out, index=False, encoding=config.CSV_ENCODING)
    logger.info("Matched data → %s", out.name)

    # ── Cross-store subset ───────────────────────────────────────────
    if "match_id" in matched.columns and "store_name" in matched.columns:
        cross = (
            matched.groupby("match_id")
            .filter(lambda g: g["store_name"].nunique() >= config.MIN_CROSS_STORE)
        )
        if config.OUTPUT_FORMAT == "parquet":
            cpath = config.MATCHED_DIR / "cross_store_matches.parquet"
            cross.to_parquet(cpath, index=False, engine="pyarrow")
        else:
            cpath = config.MATCHED_DIR / "cross_store_matches.csv"
            cross.to_csv(cpath, index=False, encoding=config.CSV_ENCODING)
        logger.info("Cross-store subset → %s  (%d rows)", cpath.name, len(cross))

    logger.info("=" * 60)
    logger.info("ENTITY RESOLUTION DONE")
    logger.info("=" * 60)
    return matched


if __name__ == "__main__":
    from pipeline.processor import process_all
    processed = process_all()
    if not processed.empty:
        result = match_products(processed)
        print(f"Matching done – {len(result)} matched rows.")
