"""
Technical Report Generator — Section 6 Deliverables
=====================================================

Reads pipeline outputs (processed data, matched data, validation reports,
analysis results) and auto-generates a comprehensive Markdown technical
report covering:

    1. Architecture overview
    2. Data collection methodology
    3. Cleaning & normalisation pipeline
    4. Entity resolution approach
    5. Validation results
    6. Analysis results with interpretation
    7. Challenges & solutions
    8. Deliverables checklist
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

import config
from scrapers.base_scraper import build_logger

logger = build_logger("report_generator")

REPORT_DIR = config.DATA_DIR / "report"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _load_parquet_safe(path: Path) -> pd.DataFrame:
    """Load a Parquet file, returning empty DataFrame on failure."""
    try:
        return pd.read_parquet(path)
    except Exception:
        return pd.DataFrame()


def _load_csv_safe(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="utf-8-sig")
    except Exception:
        return pd.DataFrame()


def _load_analysis(name: str) -> pd.DataFrame:
    """Load an analysis output file respecting config.OUTPUT_FORMAT."""
    ext = config.OUTPUT_FORMAT
    path = config.ANALYSIS_DIR / f"{name}.{ext}"
    if ext == "parquet":
        return _load_parquet_safe(path)
    return _load_csv_safe(path)


def _fmt(n, decimals=2) -> str:
    """Format a number with commas."""
    if isinstance(n, float):
        return f"{n:,.{decimals}f}"
    return f"{n:,}"


def _file_exists(path: Path) -> str:
    """Return a checkmark or cross for file existence."""
    return "YES" if path.exists() and (path.is_file() or any(path.iterdir())) else "NO"


def _count_files(directory: Path, ext: str = "") -> int:
    if not directory.exists():
        return 0
    if ext:
        return sum(1 for f in directory.iterdir() if f.suffix == f".{ext}")
    return sum(1 for f in directory.iterdir() if f.is_file())


# ─── Section builders ───────────────────────────────────────────────────────

def _build_header() -> str:
    return (
        "# Technical Report: Pakistan Supermarket Price Intelligence Pipeline\n\n"
        f"**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        f"**Course:** CS4048 — Data Engineering (Spring 2026, FAST-NUCES)\n\n"
        "---\n\n"
    )


def _build_executive_summary(proc_df: pd.DataFrame, matched_df: pd.DataFrame,
                             validation: pd.DataFrame) -> str:
    lines = ["## 1. Executive Summary\n\n"]

    n_proc = len(proc_df)
    n_matched = len(matched_df)
    stores = proc_df["store_name"].nunique() if "store_name" in proc_df.columns else 0
    store_list = sorted(proc_df["store_name"].unique()) if "store_name" in proc_df.columns else []
    cities = proc_df["city"].nunique() if "city" in proc_df.columns else 0
    match_groups = matched_df["match_id"].nunique() if "match_id" in matched_df.columns else 0

    lines.append(f"This pipeline scraped, cleaned, matched, and analysed product data from "
                 f"**{stores}** Pakistani supermarket chains ({', '.join(store_list)}) across "
                 f"**{cities}** cities.\n\n")

    lines.append("| Metric | Value |\n|--------|-------|\n")
    lines.append(f"| Total cleaned rows | {_fmt(n_proc, 0)} |\n")
    lines.append(f"| Matched dataset rows | {_fmt(n_matched, 0)} |\n")
    lines.append(f"| Unique match groups | {_fmt(match_groups, 0)} |\n")
    lines.append(f"| Supermarket chains | {stores} |\n")
    lines.append(f"| Cities covered | {cities} |\n")

    if not validation.empty:
        passed = (validation["status"] == "PASS").sum()
        total = len(validation)
        lines.append(f"| Validation checks passed | {passed}/{total} |\n")

    lines.append(f"| Analysis outputs | {_count_files(config.ANALYSIS_DIR, config.OUTPUT_FORMAT)} {config.OUTPUT_FORMAT.upper()} files |\n")
    lines.append("\n")
    return "".join(lines)


def _build_architecture() -> str:
    return (
        "## 2. Architecture Overview\n\n"
        "The pipeline follows a layered architecture with clear separation of concerns:\n\n"
        "```\n"
        "┌─────────────┐    ┌──────────────┐    ┌─────────────┐    ┌──────────┐    ┌──────────┐\n"
        "│  Scrapers   │───>│  Processor   │───>│  Matcher    │───>│ Validator│───>│ Analysis │\n"
        "│  (Layer 1)  │    │  (Layer 2)   │    │  (Layer 3)  │    │          │    │          │\n"
        "└─────────────┘    └──────────────┘    └─────────────┘    └──────────┘    └──────────┘\n"
        "     │                   │                   │                  │              │\n"
        "     ▼                   ▼                   ▼                  ▼              ▼\n"
        "  data/raw/        data/processed/     data/matched/    data/validation/  data/analysis/\n"
        "```\n\n"
        "### Storage Layers\n\n"
        "| Layer | Directory | Format | Description |\n"
        "|-------|-----------|--------|-------------|\n"
        f"| Raw | `data/raw/` | {config.OUTPUT_FORMAT} | Original scraped data, one file per store |\n"
        f"| Processed | `data/processed/` | {config.OUTPUT_FORMAT} | Cleaned, normalised, deduplicated |\n"
        f"| Matched | `data/matched/` | {config.OUTPUT_FORMAT} | Cross-store entity resolution output |\n"
        "| Validation | `data/validation/` | CSV | Pass/Warn/Fail check reports |\n"
        f"| Analysis | `data/analysis/` | {config.OUTPUT_FORMAT} | Analytical output tables |\n\n"
        "### Key Design Decisions\n\n"
        "- **Single config.py**: All thresholds, URLs, and paths in one place.\n"
        "- **Re-runnability**: `_clean_layer()` wipes downstream outputs before each run.\n"
        f"- **{config.OUTPUT_FORMAT.upper()} storage**: All pipeline outputs stored in {config.OUTPUT_FORMAT.upper()} format.\n"
        "- **Per-scraper logging**: Each scraper has its own log file + console output.\n\n"
    )


def _build_data_collection() -> str:
    lines = [
        "## 3. Data Collection Methodology\n\n",
        "### 3.1 Scraping Strategy\n\n",
        "Each scraper extends `BaseScraper` which provides:\n\n",
        "- **Retry logic**: Exponential backoff via `tenacity` "
        f"(max {config.MAX_RETRIES} retries, backoff base {config.BACKOFF_BASE}s, "
        f"ceiling {config.BACKOFF_MAX}s)\n",
        f"- **Rate limiting**: Minimum {config.RATE_LIMIT_DELAY}s between requests\n",
        f"- **Request timeout**: {config.REQUEST_TIMEOUT}s\n",
        "- **Playwright fallback**: Automatically switches to headless browser for "
        "JavaScript-rendered pages\n",
        "- **Batch persistence**: Saves data in batches to prevent data loss\n",
        "- **Attempt logging**: Every HTTP request is counted "
        "(`_attempts`, `_successes`, `_failures`)\n\n",
        "### 3.2 Target Stores\n\n",
        "| Store | Base URL | Cities |\n",
        "|-------|----------|--------|\n",
        f"| Metro | {config.METRO_CONFIG['base_url']} | {', '.join(config.METRO_CONFIG['cities'])} |\n",
        f"| Imtiaz | {config.IMTIAZ_CONFIG['base_url']} | {', '.join(config.IMTIAZ_CONFIG['cities'])} |\n",
        f"| Naheed | {config.NAHEED_CONFIG['base_url']} | {', '.join(config.NAHEED_CONFIG['cities'])} |\n\n",
        f"### 3.3 Scale Target\n\n",
        f"- **Target rows per store**: {_fmt(config.TARGET_ROWS_PER_STORE, 0)}\n",
        f"- **Minimum total cleaned rows**: {_fmt(config.MIN_TOTAL_CLEAN_ROWS, 0)}\n\n",
    ]
    return "".join(lines)


def _build_cleaning_pipeline() -> str:
    return (
        "## 4. Cleaning & Normalisation Pipeline\n\n"
        "The `pipeline/processor.py` module applies a 10-stage transformation:\n\n"
        "| Stage | Operation |\n"
        "|-------|-----------|\n"
        "| 1 | Convert prices to numeric, coerce errors |\n"
        "| 2 | Standardise currency symbols (Rs, Rs., ₨ → PKR) |\n"
        "| 3 | Extract quantity via regex (e.g., '500g', '2×250ml') |\n"
        "| 4 | Normalise size units (kg→g, l→ml, oz→g, lb→g) |\n"
        "| 5 | Normalise brand names via alias dictionary (60+ known brands) |\n"
        "| 6 | Log per-column missing value percentages |\n"
        "| 7 | Pass 1 dedup — exact duplicates on (store, city, product, price) |\n"
        "| 8 | Pass 2 dedup — near-duplicates (same product, similar price within 2%) |\n"
        "| 9 | Compute price-per-unit (price ÷ size_value_grams_or_ml) |\n"
        "| 10 | Generate clean product name (lowercase, stripped, standardised) |\n\n"
    )


def _build_entity_resolution() -> str:
    return (
        "## 5. Entity Resolution (Cross-Store Matching)\n\n"
        "The `pipeline/matcher.py` module uses a 3-pass approach:\n\n"
        "| Pass | Method | Confidence | Description |\n"
        "|------|--------|------------|-------------|\n"
        "| 1 | Exact key match | 1.0 | Identical `product_name_clean + brand + size` |\n"
        f"| 2 | Fuzzy matching | ≥0.{config.FUZZY_THRESHOLD} | RapidFuzz `token_sort_ratio` blocked by brand |\n"
        "| 3 | Brand+Size+Category | 0.70 | Fallback grouping for remaining products |\n\n"
        f"- **Minimum cross-store requirement**: Products must appear in ≥{config.MIN_CROSS_STORE} stores\n"
        f"- **Minimum matched products**: {_fmt(config.MIN_MATCHED_PRODUCTS, 0)} unique match groups\n"
        "- **Precision/recall**: Documented in `match_statistics.csv` and `precision_recall_report.csv`\n\n"
    )


def _build_validation_results(validation: pd.DataFrame) -> str:
    lines = ["## 6. Validation Results\n\n"]

    if validation.empty:
        lines.append("*No validation report found.*\n\n")
        return "".join(lines)

    passed = (validation["status"] == "PASS").sum()
    warned = (validation["status"] == "WARN").sum()
    failed = (validation["status"] == "FAIL").sum()
    skipped = (validation["status"] == "SKIP").sum()
    total = len(validation)

    lines.append(f"**Summary**: {passed} PASS, {warned} WARN, {failed} FAIL, "
                 f"{skipped} SKIP out of {total} checks.\n\n")
    lines.append("| Check | Status | Detail |\n|-------|--------|--------|\n")
    for _, row in validation.iterrows():
        status_icon = {"PASS": "PASS", "WARN": "WARN", "FAIL": "**FAIL**",
                       "SKIP": "SKIP"}.get(row["status"], row["status"])
        lines.append(f"| {row['check']} | {status_icon} | {row['detail']} |\n")
    lines.append("\n")
    return "".join(lines)


def _build_analysis_results() -> str:
    lines = ["## 7. Analysis Results with Interpretation\n\n"]

    # §3.1 Price Dispersion
    lines.append("### 7.1 Price Dispersion Metrics (§3.1)\n\n")
    disp = _load_analysis("price_dispersion")
    if not disp.empty:
        lines.append(f"Computed dispersion metrics for **{len(disp):,}** product groups.\n\n")
        if "cv" in disp.columns:
            avg_cv = disp["cv"].mean()
            lines.append(f"- **Average Coefficient of Variation**: {avg_cv:.4f} "
                         f"({'high' if avg_cv > 0.15 else 'moderate' if avg_cv > 0.05 else 'low'} "
                         f"price dispersion across stores)\n")
        if "price_spread_ratio" in disp.columns:
            avg_spread = disp["price_spread_ratio"].mean()
            lines.append(f"- **Average Price Spread Ratio**: {avg_spread:.4f}\n")
        if "iqr" in disp.columns:
            avg_iqr = disp["iqr"].mean()
            lines.append(f"- **Average IQR**: {avg_iqr:.2f} PKR\n")
        lines.append("\n")
    else:
        lines.append("*No price dispersion data available.*\n\n")

    # RPPI
    rppi = _load_analysis("relative_price_position_index")
    if not rppi.empty:
        lines.append("**Relative Price Position Index (RPPI)** — aggregated by store:\n\n")
        lines.append("| Store | RPPI |\n|-------|------|\n")
        for _, row in rppi.iterrows():
            store = row.get("store_name", "?")
            val = row.get("rppi", row.get("mean_rppi", 0))
            interpretation = "premium" if val > 1.05 else "discount" if val < 0.95 else "average"
            lines.append(f"| {store} | {val:.4f} ({interpretation}) |\n")
        lines.append("\n")

    # §3.2 Store-Level Metrics
    lines.append("### 7.2 Store-Level Aggregated Metrics (§3.2)\n\n")
    slm = _load_analysis("store_level_metrics")
    if not slm.empty:
        lines.append(f"Metrics computed for **{len(slm)}** store-city combinations.\n\n")
        cols_to_show = [c for c in ["store_name", "city", "avg_category_price_index",
                                     "median_price_deviation", "price_volatility_score",
                                     "price_leadership_freq"] if c in slm.columns]
        if cols_to_show:
            lines.append("| " + " | ".join(cols_to_show) + " |\n")
            lines.append("| " + " | ".join(["---"] * len(cols_to_show)) + " |\n")
            for _, row in slm.iterrows():
                vals = []
                for c in cols_to_show:
                    v = row[c]
                    vals.append(f"{v:.4f}" if isinstance(v, float) else str(v))
                lines.append("| " + " | ".join(vals) + " |\n")
            lines.append("\n")
    else:
        lines.append("*No store-level metrics available.*\n\n")

    # §3.3 Leader Dominance Index
    lines.append("### 7.3 Leader Dominance Index (§3.3)\n\n")
    ldi = _load_analysis("leader_dominance_index")
    if not ldi.empty:
        lines.append("| Store | LDI | Weighted LDI |\n|-------|-----|-------------|\n")
        for _, row in ldi.iterrows():
            store = row.get("store_name", "?")
            ldi_val = row.get("ldi", 0)
            wldi = row.get("weighted_ldi", 0)
            lines.append(f"| {store} | {ldi_val:.4f} | {wldi:.4f} |\n")
        lines.append("\n")
        lines.append("**Interpretation**: Higher LDI indicates the store is the price leader "
                     "(lowest price) for a larger share of products.\n\n")
    else:
        lines.append("*No LDI data available.*\n\n")

    # Category-wise LDI
    cat_ldi = _load_analysis("category_wise_ldi")
    if not cat_ldi.empty:
        n_cats = cat_ldi["category"].nunique() if "category" in cat_ldi.columns else len(cat_ldi)
        lines.append(f"Category-wise LDI computed across **{n_cats}** categories.\n\n")

    # §3.4 Correlation analyses
    lines.append("### 7.4 Correlation & Competition Analysis (§3.4)\n\n")

    for name, label in [
        ("corr_size_vs_dispersion", "Product Size vs. Price Dispersion"),
        ("corr_competitors_vs_spread", "Number of Competitors vs. Price Spread"),
        ("corr_brand_tier_vs_volatility", "Brand Tier vs. Price Volatility"),
    ]:
        df = _load_analysis(name)
        if not df.empty:
            lines.append(f"**{label}**\n\n")
            for _, row in df.iterrows():
                r_val = row.get("r", row.get("correlation", None))
                p_val = row.get("p_value", None)
                interp = row.get("interpretation", "")
                if r_val is not None:
                    lines.append(f"- r = {r_val:.4f}")
                    if p_val is not None:
                        sig = "significant" if p_val < 0.05 else "not significant"
                        lines.append(f", p = {p_val:.4f} ({sig})")
                    lines.append("\n")
                if interp:
                    lines.append(f"- {interp}\n")
            lines.append("\n")

    # Cross-store sync
    sync = _load_analysis("cross_store_sync_summary")
    if not sync.empty:
        lines.append("**Cross-Store Price Synchronization**\n\n")
        for _, row in sync.iterrows():
            if "overall_sync_score" in row.index:
                score = row["overall_sync_score"]
                level = "high" if score > 0.7 else "moderate" if score > 0.4 else "low"
                lines.append(f"- Overall synchronization score: **{score:.4f}** ({level})\n")
            if "interpretation" in row.index:
                lines.append(f"- {row['interpretation']}\n")
        lines.append("\n")

    # City correlation
    pearson = _load_analysis("city_corr_pearson")
    if not pearson.empty:
        lines.append("**City-wise Price Correlation (Pearson)**\n\n")
        cols = list(pearson.columns)
        lines.append("| | " + " | ".join(str(c) for c in cols) + " |\n")
        lines.append("| --- | " + " | ".join(["---"] * len(cols)) + " |\n")
        for idx, row in pearson.iterrows():
            vals = " | ".join(f"{v:.4f}" if isinstance(v, float) else str(v)
                              for v in row)
            lines.append(f"| {idx} | {vals} |\n")
        lines.append("\n")

    return "".join(lines)


def _build_challenges() -> str:
    return (
        "## 8. Challenges & Solutions\n\n"
        "| Challenge | Solution |\n"
        "|-----------|----------|\n"
        "| JavaScript-rendered pages | Playwright fallback with headless Chromium |\n"
        "| Rate limiting / IP blocking | Exponential backoff, configurable delays, "
        "rotating user-agent headers |\n"
        "| Inconsistent product naming | 3-pass entity resolution: exact → fuzzy → brand+size |\n"
        "| Mixed currency symbols (Rs, ₨, Rs.) | CURRENCY_ALIASES normalisation map |\n"
        "| Inconsistent units (kg/g/oz/lb) | Unit conversion to canonical grams/ml |\n"
        "| Duplicate products | 2-pass dedup: exact + near-duplicate (2% price tolerance) |\n"
        "| Missing/null values | Per-column thresholds with PASS/WARN/FAIL grading |\n"
        "| Scale (500k+ rows) | Parquet columnar storage, batch persistence, streaming scrape |\n"
        "| Re-runnability | `_clean_layer()` wipes downstream dirs before each run |\n"
        "| Price outliers | Dual detection: IQR method + Z-score (|Z|>3) |\n\n"
    )


def _build_deliverables_checklist(proc_df: pd.DataFrame, matched_df: pd.DataFrame) -> str:
    lines = ["## 9. Deliverables Checklist\n\n"]
    lines.append("| # | Deliverable | Status | Detail |\n")
    lines.append("|---|------------|--------|--------|\n")

    # 1. Complete source code
    py_files = list(config.BASE_DIR.rglob("*.py"))
    py_count = len([f for f in py_files if "__pycache__" not in str(f)])
    lines.append(f"| 1 | Complete source code (modular & documented) | YES | "
                 f"{py_count} Python files across 5 packages |\n")

    # 2. Final cleaned dataset
    proc_files = _count_files(config.PROCESSED_DIR, config.OUTPUT_FORMAT)
    n_proc = len(proc_df)
    status_2 = "YES" if n_proc >= config.MIN_TOTAL_CLEAN_ROWS else "PARTIAL"
    lines.append(f"| 2 | Final cleaned dataset (500k+ rows) | {status_2} | "
                 f"{_fmt(n_proc, 0)} rows in {proc_files} file(s) |\n")

    # 3. Matched cross-store dataset
    matched_files = _count_files(config.MATCHED_DIR, config.OUTPUT_FORMAT)
    n_matched = len(matched_df)
    match_groups = matched_df["match_id"].nunique() if "match_id" in matched_df.columns else 0
    status_3 = "YES" if match_groups >= config.MIN_MATCHED_PRODUCTS else "PARTIAL"
    lines.append(f"| 3 | Matched cross-store dataset | {status_3} | "
                 f"{_fmt(n_matched, 0)} rows, {_fmt(match_groups, 0)} match groups |\n")

    # 4. Analysis results with interpretation
    analysis_files = _count_files(config.ANALYSIS_DIR, config.OUTPUT_FORMAT)
    status_4 = "YES" if analysis_files >= 10 else "PARTIAL"
    lines.append(f"| 4 | Analysis results with interpretation | {status_4} | "
                 f"{analysis_files} output files |\n")

    # 5. Technical report
    report_path = REPORT_DIR / "technical_report.md"
    lines.append(f"| 5 | Technical report | {'YES' if report_path.exists() else 'GENERATING'} | "
                 f"This document |\n")

    lines.append("\n")
    return "".join(lines)


# =====================================================================
#  PUBLIC API
# =====================================================================

def generate_technical_report(
    processed_df: pd.DataFrame | None = None,
    matched_df: pd.DataFrame | None = None,
) -> Path:
    """
    Generate a comprehensive Markdown technical report from pipeline outputs.

    If DataFrames are not provided, attempts to load them from disk.
    Returns the path to the generated report.
    """
    logger.info("Generating technical report...")

    # Load data if not provided
    if processed_df is None:
        proc_files = sorted(config.PROCESSED_DIR.glob(f"*.{config.OUTPUT_FORMAT}"))
        if proc_files:
            loader = _load_parquet_safe if config.OUTPUT_FORMAT == "parquet" else _load_csv_safe
            processed_df = pd.concat([loader(f) for f in proc_files],
                                     ignore_index=True)
        else:
            processed_df = pd.DataFrame()

    if matched_df is None:
        match_files = sorted(config.MATCHED_DIR.glob(f"*.{config.OUTPUT_FORMAT}"))
        if match_files:
            loader = _load_parquet_safe if config.OUTPUT_FORMAT == "parquet" else _load_csv_safe
            matched_df = pd.concat([loader(f) for f in match_files],
                                   ignore_index=True)
        else:
            matched_df = pd.DataFrame()

    # Load validation report (prefer matched, fall back to processed)
    val_path = config.VALIDATION_DIR / "validation_report_matched.csv"
    if not val_path.exists():
        val_path = config.VALIDATION_DIR / "validation_report_processed.csv"
    validation = _load_csv_safe(val_path)

    # Build report sections
    sections = [
        _build_header(),
        _build_executive_summary(processed_df, matched_df, validation),
        _build_architecture(),
        _build_data_collection(),
        _build_cleaning_pipeline(),
        _build_entity_resolution(),
        _build_validation_results(validation),
        _build_analysis_results(),
        _build_challenges(),
        _build_deliverables_checklist(processed_df, matched_df),
    ]

    report_text = "".join(sections)
    report_path = REPORT_DIR / "technical_report.md"
    report_path.write_text(report_text, encoding="utf-8")
    logger.info("Technical report saved → %s (%d chars)", report_path, len(report_text))

    return report_path


def verify_deliverables() -> Dict[str, bool]:
    """
    Check that all 5 required deliverables exist.
    Returns dict mapping deliverable name → exists (bool).
    """
    results = {}

    # 1. Source code
    py_files = [f for f in config.BASE_DIR.rglob("*.py") if "__pycache__" not in str(f)]
    results["source_code"] = len(py_files) >= 5

    # 2. Cleaned dataset
    proc_files = list(config.PROCESSED_DIR.glob(f"*.{config.OUTPUT_FORMAT}"))
    results["cleaned_dataset"] = len(proc_files) > 0

    # 3. Matched dataset
    match_files = list(config.MATCHED_DIR.glob(f"*.{config.OUTPUT_FORMAT}"))
    results["matched_dataset"] = len(match_files) > 0

    # 4. Analysis results
    analysis_files = list(config.ANALYSIS_DIR.glob(f"*.{config.OUTPUT_FORMAT}"))
    results["analysis_results"] = len(analysis_files) >= 10

    # 5. Technical report
    results["technical_report"] = (REPORT_DIR / "technical_report.md").exists()

    for name, ok in results.items():
        logger.info("Deliverable %-20s: %s", name, "PRESENT" if ok else "MISSING")

    return results
