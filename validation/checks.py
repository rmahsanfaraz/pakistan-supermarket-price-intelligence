"""
Validation Layer — Automated Data-Quality Checks  (Section 4.4)
================================================================

Checks implemented (per assignment spec):
    1. Missing value percentage thresholds (per column)
    2. Duplicate detection
    3. Unit consistency validation
    4. Outlier detection (Z-score AND IQR method)
    5. Price sanity checks (price-per-unit bounds)
    6. Row-count, store-coverage, city-coverage (assignment constraints)

Every check produces a PASS / WARN / FAIL verdict.
Results are written to  data/validation_report.csv .
"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

import config
from scrapers.base_scraper import build_logger

logger = build_logger("validation")

# Columns that MUST be present
REQUIRED_COLUMNS = [
    "store_name", "city", "product_name", "price", "category",
    "brand", "currency", "scrape_timestamp",
]

# Maximum acceptable null % per column before FAIL
NULL_THRESHOLDS: dict[str, float] = {
    "store_name": 0.0,
    "product_name": 0.0,
    "price": 0.0,
    "city": 5.0,
    "category": 10.0,
    "brand": 15.0,
    "currency": 1.0,
}

# Valid canonical units (from processor.py UNIT_MAP values)
VALID_UNITS = {"g", "kg", "ml", "l", "oz", "lb", "pcs", "pack", "sachet",
               "rolls", "sheets", "tablets", "caps", ""}

# Price-per-unit sanity bounds (PKR per gram / ml)
PPU_MIN = 0.001      # anything below is likely a data error
PPU_MAX = 10_000.0   # luxury cosmetics / electronics can reach high per-unit prices


# ─── Individual checks ──────────────────────────────────────────────────────

def _check_row_count(df: pd.DataFrame) -> Dict[str, Any]:
    """Assignment requires >= 500 000 cleaned rows."""
    n = len(df)
    status = "PASS" if n >= 500_000 else "WARN" if n >= 100_000 else "FAIL"
    return {"check": "row_count_500k", "status": status,
            "detail": f"{n:,} rows (target ≥500,000)"}


def _check_required_columns(df: pd.DataFrame) -> Dict[str, Any]:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if not missing:
        return {"check": "required_columns", "status": "PASS", "detail": "All present"}
    return {"check": "required_columns", "status": "FAIL",
            "detail": f"Missing: {missing}"}


# ── 1. Missing value percentage thresholds ───────────────────────────────────

def _check_missing_pct(df: pd.DataFrame) -> List[Dict[str, Any]]:
    results = []
    for col, threshold in NULL_THRESHOLDS.items():
        if col not in df.columns:
            results.append({"check": f"missing_{col}", "status": "SKIP",
                            "detail": f"Column '{col}' absent"})
            continue
        pct = df[col].isna().mean() * 100
        # Also count blank strings as missing
        if df[col].dtype == object:
            blank = (df[col].astype(str).str.strip() == "").mean() * 100
            pct = max(pct, blank)
        if pct <= threshold:
            status = "PASS"
        elif pct <= threshold + 5:
            status = "WARN"
        else:
            status = "FAIL"
        results.append({"check": f"missing_{col}", "status": status,
                        "detail": f"{pct:.2f}% (threshold ≤{threshold}%)"})
    return results


# ── 2. Duplicate detection ───────────────────────────────────────────────────

def _check_duplicates(df: pd.DataFrame) -> Dict[str, Any]:
    dup_cols = [c for c in ("store_name", "city", "product_name", "price")
                if c in df.columns]
    if len(dup_cols) < 2:
        return {"check": "duplicates", "status": "SKIP",
                "detail": "Insufficient columns for dedup check"}
    n_dup = df.duplicated(subset=dup_cols).sum()
    pct = 100 * n_dup / max(len(df), 1)
    status = "PASS" if pct == 0 else "WARN" if pct < 2 else "FAIL"
    return {"check": "duplicates", "status": status,
            "detail": f"{n_dup:,} duplicates ({pct:.2f}%)"}


# ── 3. Unit consistency validation ───────────────────────────────────────────

def _check_unit_consistency(df: pd.DataFrame) -> Dict[str, Any]:
    col = "size_unit_canonical" if "size_unit_canonical" in df.columns else (
          "size_base_unit" if "size_base_unit" in df.columns else None)
    if col is None:
        return {"check": "unit_consistency", "status": "SKIP",
                "detail": "No unit column found"}
    units_found = set(df[col].dropna().unique())
    invalid = units_found - VALID_UNITS
    if not invalid:
        return {"check": "unit_consistency", "status": "PASS",
                "detail": f"All {len(units_found)} unit types valid: {sorted(units_found)}"}
    return {"check": "unit_consistency", "status": "WARN",
            "detail": f"Unknown units: {sorted(invalid)}"}


# ── 4. Outlier detection — IQR method ────────────────────────────────────────

def _check_outliers_iqr(df: pd.DataFrame) -> Dict[str, Any]:
    if "price" not in df.columns:
        return {"check": "outliers_iqr", "status": "SKIP", "detail": "No price column"}
    q1 = df["price"].quantile(0.25)
    q3 = df["price"].quantile(0.75)
    iqr = q3 - q1
    lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    outliers = ((df["price"] < lower) | (df["price"] > upper)).sum()
    pct = 100 * outliers / max(len(df), 1)
    status = "PASS" if pct < 5 else "WARN" if pct < 20 else "FAIL"
    return {"check": "outliers_iqr", "status": status,
            "detail": f"{outliers:,} outliers ({pct:.2f}%) outside [{lower:.2f}, {upper:.2f}]"}


# ── 4b. Outlier detection — Z-score method ───────────────────────────────────

def _check_outliers_zscore(df: pd.DataFrame) -> Dict[str, Any]:
    if "price" not in df.columns:
        return {"check": "outliers_zscore", "status": "SKIP", "detail": "No price column"}
    mean, std = df["price"].mean(), df["price"].std()
    if std == 0:
        return {"check": "outliers_zscore", "status": "PASS", "detail": "Zero variance"}
    z = ((df["price"] - mean) / std).abs()
    outliers = (z > 3).sum()
    pct = 100 * outliers / max(len(df), 1)
    status = "PASS" if pct < 2 else "WARN" if pct < 10 else "FAIL"
    return {"check": "outliers_zscore", "status": status,
            "detail": f"{outliers:,} rows with |Z| > 3 ({pct:.2f}%)"}


# ── 5. Price sanity — price per unit bounds ──────────────────────────────────

def _check_price_per_unit_bounds(df: pd.DataFrame) -> Dict[str, Any]:
    col = "price_per_unit"
    if col not in df.columns:
        return {"check": "price_per_unit_bounds", "status": "SKIP",
                "detail": "No price_per_unit column"}
    valid = df[col].dropna()
    if valid.empty:
        return {"check": "price_per_unit_bounds", "status": "SKIP",
                "detail": "All price_per_unit values are NaN"}
    too_low = (valid < PPU_MIN).sum()
    too_high = (valid > PPU_MAX).sum()
    bad = too_low + too_high
    pct = 100 * bad / max(len(valid), 1)
    status = "PASS" if pct < 2 else "WARN" if pct < 10 else "FAIL"
    return {"check": "price_per_unit_bounds", "status": status,
            "detail": f"{too_low} below {PPU_MIN}, {too_high} above {PPU_MAX} "
                      f"({pct:.2f}% of {len(valid):,} priced rows)"}


# ── 6. Price basic range ─────────────────────────────────────────────────────

def _check_price_range(df: pd.DataFrame) -> Dict[str, Any]:
    if "price" not in df.columns:
        return {"check": "price_range", "status": "SKIP", "detail": "No price column"}
    neg = (df["price"] <= 0).sum()
    extreme = (df["price"] > 2_500_000).sum()
    issues = neg + extreme
    pct = 100 * issues / max(len(df), 1)
    status = "PASS" if pct < 0.5 else "WARN" if pct < 2 else "FAIL"
    return {"check": "price_range", "status": status,
            "detail": f"{neg} non-positive, {extreme} extreme (>2.5M)"}


# ── 7. Store coverage (≥3 chains) ───────────────────────────────────────────

def _check_store_coverage(df: pd.DataFrame) -> Dict[str, Any]:
    if "store_name" not in df.columns:
        return {"check": "store_coverage", "status": "SKIP", "detail": "No store_name"}
    stores = df["store_name"].nunique()
    status = "PASS" if stores >= 3 else "WARN" if stores >= 2 else "FAIL"
    return {"check": "store_coverage", "status": status,
            "detail": f"{stores} store(s): {sorted(df['store_name'].unique())}"}


# ── 8. City coverage (≥2 per chain) ─────────────────────────────────────────

def _check_city_coverage(df: pd.DataFrame) -> List[Dict[str, Any]]:
    results = []
    if "store_name" not in df.columns or "city" not in df.columns:
        results.append({"check": "city_coverage", "status": "SKIP",
                        "detail": "Missing store_name or city columns"})
        return results
    for store, grp in df.groupby("store_name"):
        cities = grp["city"].nunique()
        status = "PASS" if cities >= 2 else "WARN"
        results.append({"check": f"city_coverage_{store}", "status": status,
                        "detail": f"{cities} cities: {sorted(grp['city'].unique())}"})
    return results


# ── 9. Matched products count (§5: ≥10,000 matched) ───────────────────

def _check_matched_count(df: pd.DataFrame) -> Dict[str, Any]:
    """Verify the matched layer has ≥ MIN_MATCHED_PRODUCTS rows."""
    target = getattr(config, "MIN_MATCHED_PRODUCTS", 10_000)
    if "match_id" not in df.columns:
        return {"check": "matched_product_count", "status": "SKIP",
                "detail": "No match_id column (not a matched dataset)"}
    n_rows = len(df)
    n_groups = df["match_id"].nunique()
    status = "PASS" if n_rows >= target else "WARN" if n_rows >= target // 2 else "FAIL"
    return {"check": "matched_product_count", "status": status,
            "detail": f"{n_rows:,} matched rows ({n_groups:,} groups) (target ≥{target:,})"}


# ── 10. Minimum total cleaned rows (§5: ≥500,000) ───────────────────

def _check_min_stores(df: pd.DataFrame) -> Dict[str, Any]:
    """Verify ≥3 supermarket chains are present."""
    if "store_name" not in df.columns:
        return {"check": "min_3_chains", "status": "SKIP", "detail": "No store_name"}
    n = df["store_name"].nunique()
    status = "PASS" if n >= 3 else "FAIL"
    return {"check": "min_3_chains", "status": status,
            "detail": f"{n} chains: {sorted(df['store_name'].unique())}"}


# =====================================================================
# PUBLIC API
# =====================================================================

def run_all_checks(df: pd.DataFrame, layer: str = "processed") -> pd.DataFrame:
    """Run every validation check. *layer* labels the report (raw/processed/matched)."""
    results: List[Dict[str, Any]] = []
    logger.info("Running validation checks on '%s' layer (%d rows)...", layer, len(df))

    if layer != "matched":
        results.append(_check_row_count(df))
    results.append(_check_required_columns(df))
    results.extend(_check_missing_pct(df))          # §4.4 missing-value thresholds
    results.append(_check_duplicates(df))            # §4.4 duplicate detection
    results.append(_check_unit_consistency(df))      # §4.4 unit consistency
    results.append(_check_outliers_iqr(df))          # §4.4 outlier (IQR)
    results.append(_check_outliers_zscore(df))       # §4.4 outlier (Z-score)
    results.append(_check_price_per_unit_bounds(df)) # §4.4 price sanity
    results.append(_check_price_range(df))
    results.append(_check_store_coverage(df))        # §5 ≥3 chains
    results.append(_check_min_stores(df))              # §5 ≥3 chains (explicit)
    results.extend(_check_city_coverage(df))           # §5 ≥2 cities/chain
    results.append(_check_matched_count(df))           # §5 ≥10,000 matched

    report = pd.DataFrame(results)

    # Log every check
    for _, row in report.iterrows():
        lvl = {"PASS": "info", "WARN": "warning", "FAIL": "error", "SKIP": "info"}
        getattr(logger, lvl.get(row["status"], "info"))(
            "[%s] %s — %s", row["status"], row["check"], row["detail"])

    passed = (report["status"] == "PASS").sum()
    total = len(report)
    logger.info("Validation summary: %d/%d PASS, %d WARN, %d FAIL.",
                passed,
                total,
                (report["status"] == "WARN").sum(),
                (report["status"] == "FAIL").sum())

    out_dir = config.VALIDATION_DIR if hasattr(config, "VALIDATION_DIR") else config.DATA_DIR
    out = out_dir / f"validation_report_{layer}.csv"
    report.to_csv(out, index=False, encoding="utf-8-sig")
    logger.info("Report → %s", out)
    return report


if __name__ == "__main__":
    from pipeline.processor import process_all
    processed = process_all()
    if not processed.empty:
        rpt = run_all_checks(processed)
        print(rpt.to_string(index=False))
