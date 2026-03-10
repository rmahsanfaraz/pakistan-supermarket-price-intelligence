"""
Analytical Reports — Sections 3.1 – 3.4 of the NUCES Assignment
=================================================================

Section 3.1  Price Dispersion Metrics (product-level)
    Mean, Median, Std, CV, Range, IQR, Price Spread Ratio,
    Relative Price Position Index

Section 3.2  Store-Level Aggregated Metrics
    Avg Category Price Index, Median Price Deviation,
    Price Volatility Score, Price Leadership Frequency

Section 3.3  Leader Dominance Index (LDI)
    LDI, Weighted LDI (by category size), Category-wise LDI

Section 3.4  Correlation & Competition Analysis
    Size vs Dispersion, Competitors vs Spread, Brand Tier vs Volatility,
    City-wise Correlation Matrix (Pearson & Spearman),
    Cross-Store Price Synchronization Score

Every public function accepts a DataFrame and returns a DataFrame.
Outputs are saved to  data/analysis/ .
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy import stats as sp_stats

import config
from scrapers.base_scraper import build_logger

logger = build_logger("analysis")

ANALYSIS_DIR = config.ANALYSIS_DIR if hasattr(config, "ANALYSIS_DIR") else config.DATA_DIR / "analysis"
ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)


def _save(df: pd.DataFrame, name: str):
    if config.OUTPUT_FORMAT == "parquet":
        path = ANALYSIS_DIR / f"{name}.parquet"
        df.to_parquet(path, index=False, engine="pyarrow")
    else:
        path = ANALYSIS_DIR / f"{name}.csv"
        df.to_csv(path, index=False, encoding=config.CSV_ENCODING)
    logger.info("Saved %s → %s (%d rows)", name, path.name, len(df))


def _brand_col(df: pd.DataFrame) -> str:
    return "brand_normalised" if "brand_normalised" in df.columns else "brand"


def _product_col(df: pd.DataFrame) -> str:
    return "product_name_clean" if "product_name_clean" in df.columns else "product_name"


def _group_col(df: pd.DataFrame) -> str:
    """Use match_id for matched data, otherwise fall back to product name."""
    return "match_id" if "match_id" in df.columns else _product_col(df)


def _interpret_r(r: float, var_a: str, var_b: str) -> str:
    """Return a plain-English interpretation of a correlation coefficient."""
    if pd.isna(r):
        return f"correlation between {var_a} and {var_b} is undefined (insufficient variance)"
    abs_r = abs(r)
    if abs_r < 0.1:
        strength = "negligible"
    elif abs_r < 0.3:
        strength = "weak"
    elif abs_r < 0.5:
        strength = "moderate"
    elif abs_r < 0.7:
        strength = "strong"
    else:
        strength = "very strong"
    direction = "positive" if r >= 0 else "negative"
    return f"{strength} {direction} correlation between {var_a} and {var_b} (r={r:.3f})"


# =====================================================================
#  SECTION 3.1 — Price Dispersion Metrics (product-level)
# =====================================================================

def price_comparison(df: pd.DataFrame) -> pd.DataFrame:
    """Average price per product per store."""
    pcol = _product_col(df)
    if not {pcol, "store_name", "price"}.issubset(df.columns):
        return pd.DataFrame()
    result = (
        df.groupby([pcol, "store_name"])["price"]
        .agg(["mean", "min", "max", "count"])
        .reset_index()
        .rename(columns={"mean": "avg_price", "count": "n_listings"})
    )
    _save(result, "price_comparison")
    return result


def price_dispersion(df: pd.DataFrame) -> pd.DataFrame:
    """
    §3.1 — Full dispersion metrics per matched product:
        Mean, Median, Std, CV, Range, IQR,
        Price Spread Ratio (max/min).

    Groups by match_id when available (matched layer), otherwise by
    product name (processed layer).
    """
    gcol = _group_col(df)
    if not {gcol, "store_name", "price"}.issubset(df.columns):
        return pd.DataFrame()
    agg = (
        df.groupby(gcol)["price"]
        .agg(
            mean_price="mean",
            median_price="median",
            std_price="std",
            min_price="min",
            max_price="max",
            q25=lambda x: x.quantile(0.25),
            q75=lambda x: x.quantile(0.75),
        )
    )
    # store count from store_name column
    store_counts = df.groupby(gcol)["store_name"].nunique().rename("stores")
    agg = agg.join(store_counts)

    agg["cv"] = (agg["std_price"] / agg["mean_price"]).round(4)
    agg["price_range"] = agg["max_price"] - agg["min_price"]
    agg["iqr"] = agg["q75"] - agg["q25"]
    agg["price_spread_ratio"] = (agg["max_price"] / agg["min_price"].replace(0, np.nan)).round(4)

    # Attach a representative product name for readability
    if gcol == "match_id" and "product_name" in df.columns:
        rep_name = df.groupby(gcol)["product_name"].first().rename("product_name")
        agg = agg.join(rep_name)

    agg = agg.reset_index().sort_values("cv", ascending=False)
    _save(agg, "price_dispersion")
    return agg


def relative_price_position_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    §3.1 — Relative Price Position Index  =  store_price / category_mean

    Two outputs:
      1. Per-product-per-store RPPI
         (rppi_per_product.parquet / rppi_per_product.csv)
      2. Aggregated (store × category) average RPPI
         (relative_price_position_index.parquet / relative_price_position_index.csv)
    """
    if not {"store_name", "category", "price"}.issubset(df.columns):
        return pd.DataFrame()

    gcol = _group_col(df)
    cat_mean = df.groupby("category")["price"].mean().rename("category_mean")
    merged = df.merge(cat_mean, on="category", how="left")
    merged["rppi"] = (merged["price"] / merged["category_mean"]).round(4)

    # ── Per-product-per-store RPPI ───────────────────────────────────
    group_keys = [gcol, "store_name", "category"]
    group_keys = [k for k in group_keys if k in merged.columns]
    per_product = (
        merged.groupby(group_keys)
        .agg(avg_price=("price", "mean"), rppi=("rppi", "mean"),
             category_mean=("category_mean", "first"))
        .reset_index()
        .round(4)
    )
    _save(per_product, "rppi_per_product")

    # ── Aggregated (store × category) RPPI ───────────────────────────
    result = (
        merged.groupby(["store_name", "category"])["rppi"]
        .mean().reset_index()
        .rename(columns={"rppi": "avg_rppi"})
        .sort_values(["category", "avg_rppi"])
    )
    _save(result, "relative_price_position_index")
    return result


# =====================================================================
#  SECTION 3.2 — Store-Level Aggregated Metrics
# =====================================================================

def store_level_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    §3.2 — For each store **and** city compute:
        1. Average Category Price Index  (store-city cat avg / overall cat avg)
        2. Median Price Deviation from market average
        3. Price Volatility Score  (average CV across products)
        4. Price Leadership Frequency  (% of products where store-city has lowest price)

    Uses match_id as the product grouping key when available.
    """
    gcol = _group_col(df)
    if not {gcol, "store_name", "price"}.issubset(df.columns):
        return pd.DataFrame()

    has_city = "city" in df.columns
    key = ["store_name", "city"] if has_city else ["store_name"]

    cat_col = "category" if "category" in df.columns else None

    # ── pre-compute helpers ──────────────────────────────────────────────

    # 1a  Overall category average (market-wide)
    if cat_col:
        overall_cat_avg = df.groupby(cat_col)["price"].mean().rename("overall_cat_avg")
        # Store-city category average
        sc_cat_avg = (
            df.groupby(key + [cat_col])["price"].mean()
            .reset_index(name="sc_cat_avg")
            .merge(overall_cat_avg, on=cat_col, how="left")
        )
        sc_cat_avg["cat_idx"] = sc_cat_avg["sc_cat_avg"] / sc_cat_avg["overall_cat_avg"]
        avg_cat_idx = sc_cat_avg.groupby(key)["cat_idx"].mean()
    else:
        avg_cat_idx = pd.Series(dtype=float)

    # 2a  Market-average price per product
    mkt_avg = df.groupby(gcol)["price"].mean().rename("mkt_avg")

    # 3a  Per-product CV (across all stores/cities)
    prod_stats = df.groupby(gcol)["price"].agg(["mean", "std"])
    prod_stats["cv"] = prod_stats["std"] / prod_stats["mean"]
    prod_cv = prod_stats["cv"].rename("product_cv")

    # 4a  Cheapest store-city per product
    sc_prod_avg = df.groupby([gcol] + key)["price"].mean().reset_index()
    cheapest = sc_prod_avg.loc[
        sc_prod_avg.groupby(gcol)["price"].idxmin()
    ]
    leadership_counts = cheapest.groupby(key).size()
    total_products = df[gcol].nunique()

    # ── iterate over each (store, city) combination ──────────────────────
    combos = df[key].drop_duplicates().values.tolist()
    records = []

    for combo in combos:
        vals = combo if isinstance(combo, list) else [combo]
        filt = pd.Series(True, index=df.index)
        row = {}
        for k, v in zip(key, vals):
            filt &= df[k] == v
            row[k] = v
        sc_df = df[filt]

        # 1. Avg category price index
        idx_key = tuple(vals) if has_city else vals[0]
        cpi = avg_cat_idx.get(idx_key, np.nan) if not avg_cat_idx.empty else np.nan

        # 2. Median price deviation from market average
        merged = sc_df[[gcol, "price"]].merge(mkt_avg, left_on=gcol, right_index=True, how="left")
        merged["dev"] = merged["price"] - merged["mkt_avg"]
        median_dev = merged["dev"].median()

        # 3. Price Volatility Score = avg CV of products this store-city sells
        prods_sold = sc_df[gcol].unique()
        vol_score = prod_cv.reindex(prods_sold).mean()

        # 4. Price Leadership Frequency
        lc = leadership_counts.get(idx_key, 0) if not leadership_counts.empty else 0
        lead_freq = lc / max(total_products, 1) * 100

        row.update({
            "avg_category_price_index": round(cpi, 4) if pd.notna(cpi) else None,
            "median_price_deviation": round(median_dev, 2) if pd.notna(median_dev) else None,
            "price_volatility_score": round(vol_score, 4) if pd.notna(vol_score) else None,
            "price_leadership_freq_pct": round(lead_freq, 2),
        })
        records.append(row)

    result = pd.DataFrame(records)
    _save(result, "store_level_metrics")
    return result


# =====================================================================
#  SECTION 3.3 — Leader Dominance Index (LDI)
# =====================================================================

def leader_dominance_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    §3.3 — Leader Dominance Index.

    LDI_store = (# products where store has lowest price) / total matched products
    Weighted LDI = same ratio but each "win" weighted by its category size
    Category-wise LDI = LDI computed within each category separately
    """
    gcol = _group_col(df)
    if not {gcol, "store_name", "price"}.issubset(df.columns):
        return pd.DataFrame()

    has_cat = "category" in df.columns

    # Average price per product × store  →  cheapest store per product
    avg = df.groupby([gcol, "store_name"])["price"].mean().reset_index()
    cheapest = avg.loc[avg.groupby(gcol)["price"].idxmin()]
    total_products = avg[gcol].nunique()

    # Pre-compute category size (# unique products per category)
    if has_cat:
        prod_cat = df[[gcol, "category"]].drop_duplicates()
        cat_size = prod_cat.groupby("category")[gcol].nunique().rename("cat_size")
        total_cat_weight = cat_size.sum()
        # Attach category to cheapest rows
        cheapest_with_cat = cheapest.merge(prod_cat, on=gcol, how="left")

    stores = sorted(df["store_name"].unique())
    records = []

    for st in stores:
        wins = cheapest["store_name"] == st
        n_wins = wins.sum()
        ldi = n_wins / max(total_products, 1)

        # Weighted LDI: each win weighted by its category's size.
        # Denominator = sum(cat_size²) so that wldi ∈ [0, 1].
        wldi = np.nan
        if has_cat:
            st_wins = cheapest_with_cat[cheapest_with_cat["store_name"] == st]
            if not st_wins.empty:
                st_wins = st_wins.merge(cat_size, on="category", how="left")
                weighted_sum = st_wins["cat_size"].sum()
                max_possible = (cat_size ** 2).sum()  # if one store won everything
                wldi = weighted_sum / max(max_possible, 1)
            else:
                wldi = 0.0

        records.append({
            "store_name": st,
            "products_lowest_price": int(n_wins),
            "total_matched_products": int(total_products),
            "ldi": round(ldi, 4),
            "weighted_ldi": round(wldi, 4) if pd.notna(wldi) else None,
        })

    ldi_df = pd.DataFrame(records).sort_values("ldi", ascending=False)
    _save(ldi_df, "leader_dominance_index")

    # Category-wise LDI
    if has_cat:
        avg_with_cat = avg.merge(prod_cat, on=gcol, how="left")
        cat_ldi_records = []
        for cat, g in avg_with_cat.groupby("category"):
            cat_total = g[gcol].nunique()
            cat_cheapest = g.loc[g.groupby(gcol)["price"].idxmin()]
            for st in stores:
                n = (cat_cheapest["store_name"] == st).sum()
                cat_ldi_records.append({
                    "category": cat,
                    "store_name": st,
                    "products_lowest": int(n),
                    "category_total": int(cat_total),
                    "category_ldi": round(n / max(cat_total, 1), 4),
                })
        cat_ldi_df = pd.DataFrame(cat_ldi_records)
        _save(cat_ldi_df, "category_wise_ldi")

    return ldi_df


# =====================================================================
#  SECTION 3.4 — Correlation & Competition Analysis
# =====================================================================

def correlation_size_vs_dispersion(df: pd.DataFrame) -> pd.DataFrame:
    """§3.4 — Correlation between product size and price dispersion (CV)."""
    gcol = _group_col(df)
    # Processor writes size_base_value; accept either name for robustness
    size_col = next(
        (c for c in ("size_base_value", "size_value", "qty_value") if c in df.columns),
        None,
    )
    if size_col is None or gcol not in df.columns or "price" not in df.columns:
        return pd.DataFrame()

    prod_stats = df.groupby(gcol).agg(
        avg_size=(size_col, "mean"),
        cv=("price", lambda x: x.std() / x.mean() if x.mean() else np.nan),
    ).dropna()

    if len(prod_stats) < 3:
        return pd.DataFrame()

    pearson_r, pearson_p = sp_stats.pearsonr(prod_stats["avg_size"], prod_stats["cv"])
    spearman_r, spearman_p = sp_stats.spearmanr(prod_stats["avg_size"], prod_stats["cv"])

    result = pd.DataFrame([{
        "metric": "size_vs_dispersion",
        "pearson_r": round(pearson_r, 4), "pearson_p": round(pearson_p, 6),
        "spearman_r": round(spearman_r, 4), "spearman_p": round(spearman_p, 6),
        "n_products": len(prod_stats),
        "interpretation": _interpret_r(pearson_r, "product size", "price dispersion"),
    }])
    _save(result, "corr_size_vs_dispersion")
    return result


def correlation_competitors_vs_spread(df: pd.DataFrame) -> pd.DataFrame:
    """§3.4 — Correlation between number of competitors (stores) and price spread."""
    gcol = _group_col(df)
    if not {gcol, "store_name", "price"}.issubset(df.columns):
        return pd.DataFrame()

    prod = df.groupby(gcol).agg(
        n_stores=("store_name", "nunique"),
        price_range=("price", lambda x: x.max() - x.min()),
    ).dropna()

    if len(prod) < 3:
        return pd.DataFrame()

    pearson_r, pearson_p = sp_stats.pearsonr(prod["n_stores"], prod["price_range"])
    spearman_r, spearman_p = sp_stats.spearmanr(prod["n_stores"], prod["price_range"])

    result = pd.DataFrame([{
        "metric": "competitors_vs_spread",
        "pearson_r": round(pearson_r, 4), "pearson_p": round(pearson_p, 6),
        "spearman_r": round(spearman_r, 4), "spearman_p": round(spearman_p, 6),
        "n_products": len(prod),
        "interpretation": _interpret_r(pearson_r, "#competitors", "price spread"),
    }])
    _save(result, "corr_competitors_vs_spread")
    return result


def correlation_brand_tier_vs_volatility(df: pd.DataFrame) -> pd.DataFrame:
    """
    §3.4 — Correlation between brand tier (premium vs economy) and price volatility.

    Heuristic: classify brands by their median price into tiers
    (Top-25% = Premium, Bottom-25% = Economy, rest = Mid).
    Then compute avg CV per tier and a numeric correlation.
    """
    gcol = _group_col(df)
    bcol = _brand_col(df)
    if not {gcol, bcol, "price"}.issubset(df.columns):
        return pd.DataFrame()

    brand_med = df.groupby(bcol)["price"].median()
    q25, q75 = brand_med.quantile(0.25), brand_med.quantile(0.75)

    def tier(p):
        if p >= q75:
            return "premium"
        if p <= q25:
            return "economy"
        return "mid"

    tier_map = {"economy": 1, "mid": 2, "premium": 3}
    brand_tier = brand_med.apply(tier).rename("brand_tier")
    merged = df.merge(brand_tier, left_on=bcol, right_index=True, how="left")

    # Per-product CV and tier
    prod_cv = (
        merged.groupby([gcol, "brand_tier"])["price"]
        .agg(["mean", "std"])
        .assign(cv=lambda x: x["std"] / x["mean"])
        .dropna()
        .reset_index()
    )

    # Tier-level summary
    tier_cv = (
        prod_cv.groupby("brand_tier")["cv"]
        .agg(["mean", "count"])
        .rename(columns={"mean": "avg_cv", "count": "n_products"})
        .reset_index()
    )

    # Numeric correlation (tier rank vs CV) if enough data
    if len(prod_cv) >= 3:
        prod_cv["tier_num"] = prod_cv["brand_tier"].map(tier_map)
        r, p = sp_stats.spearmanr(prod_cv["tier_num"], prod_cv["cv"])
        tier_cv["spearman_r_tier_vs_cv"] = round(r, 4)
        tier_cv["spearman_p"] = round(p, 6)
        tier_cv["interpretation"] = _interpret_r(r, "brand tier", "price volatility")

    _save(tier_cv, "corr_brand_tier_vs_volatility")
    return tier_cv


def city_price_correlation_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """
    §3.4 — City-wise price correlation matrix (Pearson & Spearman).

    Pivot: rows = products, columns = cities, values = avg price.
    Then compute pairwise correlations for both methods.
    """
    gcol = _group_col(df)
    if not {gcol, "city", "price"}.issubset(df.columns):
        return pd.DataFrame()

    pivot = df.pivot_table(index=gcol, columns="city", values="price", aggfunc="mean")
    if pivot.shape[1] < 2:
        return pd.DataFrame()

    pearson = pivot.corr(method="pearson")
    _save(pearson.reset_index(), "city_corr_pearson")

    spearman = pivot.corr(method="spearman")
    _save(spearman.reset_index(), "city_corr_spearman")

    return pearson


def cross_store_price_synchronization(df: pd.DataFrame) -> pd.DataFrame:
    """
    §3.4 — Cross-store price synchronization score.

    Pivot: rows = products, columns = stores, values = avg price.
    Pairwise Pearson correlation between stores + overall sync score.
    """
    gcol = _group_col(df)
    if not {gcol, "store_name", "price"}.issubset(df.columns):
        return pd.DataFrame()

    pivot = df.pivot_table(index=gcol, columns="store_name", values="price", aggfunc="mean")
    if pivot.shape[1] < 2:
        return pd.DataFrame()

    sync = pivot.corr(method="pearson")
    _save(sync.reset_index(), "cross_store_price_sync")

    # Overall synchronization score = mean of off-diagonal correlations
    # Use nanmean because some store pairs may lack overlapping products
    mask = np.ones(sync.shape, dtype=bool)
    np.fill_diagonal(mask, False)
    off_diag = sync.values[mask]
    overall_score = np.nanmean(off_diag) if np.any(np.isfinite(off_diag)) else np.nan
    # Count products that appear in at least 2 stores (pairwise-usable)
    n_pairwise = (pivot.notna().sum(axis=1) >= 2).sum()
    summary = pd.DataFrame([{
        "overall_sync_score": round(overall_score, 4) if pd.notna(overall_score) else None,
        "n_stores": pivot.shape[1],
        "n_products_used": int(n_pairwise),
        "interpretation": _interpret_r(overall_score, "store prices", "store prices"),
    }])
    _save(summary, "cross_store_sync_summary")

    return sync


# =====================================================================
#  LEGACY / CONVENIENCE FUNCTIONS (kept for backward compatibility)
# =====================================================================

def cheapest_store(df: pd.DataFrame) -> pd.DataFrame:
    """For each product, show which store has the lowest average price."""
    pcol = _product_col(df)
    if not {pcol, "store_name", "price"}.issubset(df.columns):
        return pd.DataFrame()
    avg = df.groupby([pcol, "store_name"])["price"].mean().reset_index()
    idx = avg.groupby(pcol)["price"].idxmin()
    result = avg.loc[idx].rename(columns={"price": "lowest_avg_price"})
    _save(result, "cheapest_store")
    return result


def brand_competition(df: pd.DataFrame) -> pd.DataFrame:
    """Number of products per brand per store."""
    bcol = _brand_col(df)
    if bcol not in df.columns or "store_name" not in df.columns:
        return pd.DataFrame()
    result = (
        df.groupby([bcol, "store_name"])
        .size().reset_index(name="product_count")
        .sort_values("product_count", ascending=False)
    )
    _save(result, "brand_competition")
    return result


def city_price_index(df: pd.DataFrame) -> pd.DataFrame:
    """Average basket price by city (relative to overall mean)."""
    if not {"city", "price"}.issubset(df.columns):
        return pd.DataFrame()
    city_avg = df.groupby("city")["price"].mean().reset_index(name="avg_price")
    overall = df["price"].mean()
    city_avg["price_index"] = (city_avg["avg_price"] / overall * 100).round(2)
    city_avg = city_avg.sort_values("price_index", ascending=False)
    _save(city_avg, "city_price_index")
    return city_avg


def summary_stats(df: pd.DataFrame) -> pd.DataFrame:
    """High-level summary statistics."""
    pcol = _product_col(df)
    bcol = _brand_col(df)
    rows = {
        "total_products": len(df),
        "unique_products": df[pcol].nunique() if pcol in df.columns else None,
        "stores": df["store_name"].nunique() if "store_name" in df.columns else None,
        "cities": df["city"].nunique() if "city" in df.columns else None,
        "categories": df["category"].nunique() if "category" in df.columns else None,
        "brands": df[bcol].nunique() if bcol in df.columns else None,
        "avg_price": round(df["price"].mean(), 2) if "price" in df.columns else None,
        "median_price": round(df["price"].median(), 2) if "price" in df.columns else None,
    }
    result = pd.DataFrame([rows])
    _save(result, "summary_stats")
    return result


# =====================================================================
#  RUN EVERYTHING
# =====================================================================

def run_full_analysis(df: pd.DataFrame, matched_df: pd.DataFrame | None = None) -> dict:
    """
    Execute ALL analyses (Sections 3.1 – 3.4) and return dict of DataFrames.

    Parameters
    ----------
    df : pd.DataFrame
        Processed / cleaned dataset.
    matched_df : pd.DataFrame | None
        Cross-store matched dataset. If provided, used for LDI and
        dispersion metrics; otherwise falls back to *df*.
    """
    m = matched_df if matched_df is not None and not matched_df.empty else df

    analyses = [
        # legacy
        ("price_comparison",       lambda: price_comparison(df)),
        ("summary_stats",          lambda: summary_stats(df)),
        ("cheapest_store",         lambda: cheapest_store(m)),
        ("brand_competition",      lambda: brand_competition(df)),
        ("city_price_index",       lambda: city_price_index(df)),
        # §3.1
        ("price_dispersion",       lambda: price_dispersion(m)),
        ("rppi",                   lambda: relative_price_position_index(m)),
        # §3.2
        ("store_level_metrics",    lambda: store_level_metrics(m)),
        # §3.3
        ("ldi",                    lambda: leader_dominance_index(m)),
        # §3.4
        ("corr_size_disp",        lambda: correlation_size_vs_dispersion(m)),
        ("corr_comp_spread",      lambda: correlation_competitors_vs_spread(m)),
        ("corr_tier_vol",         lambda: correlation_brand_tier_vs_volatility(m)),
        ("city_corr_matrix",      lambda: city_price_correlation_matrix(m)),
        ("cross_store_sync",      lambda: cross_store_price_synchronization(m)),
    ]

    results = {}
    for name, fn in analyses:
        logger.info("Running %s …", name)
        try:
            results[name] = fn()
        except Exception:
            logger.exception("Analysis '%s' failed.", name)
            results[name] = pd.DataFrame()

    logger.info("All analyses complete (%d outputs).", len(results))
    return results


if __name__ == "__main__":
    from pipeline.processor import process_all
    processed = process_all()
    if not processed.empty:
        run_full_analysis(processed)
        print("Analysis complete — check data/analysis/")
