#!/usr/bin/env python3
"""
CLI entry-point for the Pakistan Supermarket Scraping Pipeline.

Usage examples
--------------
  python run_scrapers.py                        # scrape all 3 stores, then process
  python run_scrapers.py --stores metro imtiaz  # only Metro & Imtiaz
  python run_scrapers.py --target-rows 100000   # stop after ~100 k rows
  python run_scrapers.py --no-pipeline          # scrape only, skip processing
  python run_scrapers.py --pipeline-only        # skip scraping, run pipeline on existing raw data
"""

import argparse
import sys

import config
from scrapers.base_scraper import build_logger

logger = build_logger("main")

STORE_MAP = {
    "metro":   ("scrapers.metro_scraper",   "MetroScraper"),
    "imtiaz":  ("scrapers.imtiaz_scraper",  "ImtiazScraper"),
    "naheed":  ("scrapers.naheed_scraper",  "NaheedScraper"),
    "alfatah": ("scrapers.alfatah_scraper", "AlFatahScraper"),
    "chaseup": ("scrapers.chaseup_scraper", "ChaseUpScraper"),
}


def _import_scraper(module_path: str, class_name: str):
    import importlib
    mod = importlib.import_module(module_path)
    return getattr(mod, class_name)


def _run_scrapers(stores: list, target_rows: int):
    for name in stores:
        if name not in STORE_MAP:
            logger.error("Unknown store: %s (choose from %s)", name, list(STORE_MAP))
            continue
        mod_path, cls_name = STORE_MAP[name]
        logger.info("── Starting %s ──", cls_name)
        ScraperCls = _import_scraper(mod_path, cls_name)
        scraper = ScraperCls()
        scraper.run(target_rows=target_rows)
        logger.info("── %s finished ──", cls_name)


def _clean_layer(directory):
    """Remove all files in *directory* so the pipeline is fully re-runnable."""
    import pathlib
    d = pathlib.Path(directory)
    if not d.exists():
        return
    for f in d.iterdir():
        if f.is_file():
            f.unlink()
            logger.debug("Removed stale output: %s", f)


def _run_pipeline():
    from pipeline.processor import process_all
    from pipeline.matcher import match_products
    from validation.checks import run_all_checks
    from analysis.reports import run_full_analysis
    from analysis.report_generator import generate_technical_report, verify_deliverables

    # §5: Pipeline must be fully re-runnable — clean previous outputs
    _clean_layer(config.PROCESSED_DIR)
    _clean_layer(config.MATCHED_DIR)
    _clean_layer(config.VALIDATION_DIR)
    _clean_layer(config.ANALYSIS_DIR)

    logger.info("═══ Processing raw data ═══")
    df = process_all()
    if df.empty:
        logger.error("No data to process.")
        return

    logger.info("═══ Validation (processed layer) ═══")
    run_all_checks(df, layer="processed")

    logger.info("═══ Matching ═══")
    matched = match_products(df)

    if not matched.empty:
        logger.info("═══ Validation (matched layer) ═══")
        run_all_checks(matched, layer="matched")

    logger.info("═══ Analysis ═══")
    # Pass both processed and matched DataFrames so that
    # dispersion / LDI / correlation analyses use matched data.
    run_full_analysis(df, matched_df=matched)

    # §6: Generate technical report & verify all deliverables
    logger.info("═══ Technical Report ═══")
    generate_technical_report(processed_df=df, matched_df=matched)

    logger.info("═══ Deliverables Verification ═══")
    deliverables = verify_deliverables()
    all_ok = all(deliverables.values())
    logger.info("Deliverables: %s", "ALL PRESENT" if all_ok else "SOME MISSING")

    logger.info("Pipeline complete.")


def main():
    parser = argparse.ArgumentParser(
        description="Pakistan Supermarket Scraping & Analysis Pipeline")
    parser.add_argument(
        "--stores", nargs="+", default=list(STORE_MAP),
        help="Store(s) to scrape (default: all)")
    parser.add_argument(
        "--target-rows", type=int, default=config.TARGET_ROWS_PER_STORE,
        help="Target rows per store (default: %(default)s)")
    parser.add_argument(
        "--no-pipeline", action="store_true",
        help="Skip the processing / matching / analysis pipeline")
    parser.add_argument(
        "--pipeline-only", action="store_true",
        help="Skip scraping; run pipeline on existing raw data")

    args = parser.parse_args()

    # Ensure directories exist
    for d in (config.RAW_DIR, config.PROCESSED_DIR, config.MATCHED_DIR, config.LOG_DIR):
        d.mkdir(parents=True, exist_ok=True)

    if not args.pipeline_only:
        _run_scrapers([s.lower() for s in args.stores], args.target_rows)

    if not args.no_pipeline:
        _run_pipeline()


if __name__ == "__main__":
    main()
