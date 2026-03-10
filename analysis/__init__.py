from analysis.report_generator import generate_technical_report, verify_deliverables
from analysis.reports import (
    # §3.1
    price_comparison,
    price_dispersion,
    relative_price_position_index,
    # §3.2
    store_level_metrics,
    # §3.3
    leader_dominance_index,
    # §3.4
    correlation_size_vs_dispersion,
    correlation_competitors_vs_spread,
    correlation_brand_tier_vs_volatility,
    city_price_correlation_matrix,
    cross_store_price_synchronization,
    # legacy / convenience
    cheapest_store,
    brand_competition,
    city_price_index,
    summary_stats,
    run_full_analysis,
)

__all__ = [
    "price_comparison",
    "price_dispersion",
    "relative_price_position_index",
    "store_level_metrics",
    "leader_dominance_index",
    "correlation_size_vs_dispersion",
    "correlation_competitors_vs_spread",
    "correlation_brand_tier_vs_volatility",
    "city_price_correlation_matrix",
    "cross_store_price_synchronization",
    "cheapest_store",
    "brand_competition",
    "city_price_index",
    "summary_stats",
    "run_full_analysis",
]
