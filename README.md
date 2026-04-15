# Pakistan Supermarket Price Intelligence Pipeline

A data engineering pipeline that scrapes product data from **five** major Pakistani supermarket chains — **Metro**, **Imtiaz Super Market**, **Naheed**, **AlFatah**, and **ChaseUp** — then cleans, normalises, matches products across stores, validates data quality, and performs structured price dispersion and market competition analysis.

**Course:** CS4048 — Data Science (Spring 2026, FAST-NUCES)

---

## Table of Contents

1. [Project Overview](#project-overview)
2. [Technologies and Tools](#technologies-and-tools)
3. [Project Score by Criteria](#project-score-by-criteria)
4. [Project Architecture and Folder Structure](#project-architecture-and-folder-structure)
5. [Data Scraping Process](#data-scraping-process)
6. [Data Cleaning and Normalisation](#data-cleaning-and-normalisation)
7. [Entity Matching Methodology](#entity-matching-methodology)
8. [Validation Checks](#validation-checks)
9. [Price Dispersion and Market Structure Analysis](#price-dispersion-and-market-structure-analysis)
10. [Installation](#installation)
11. [How to Run the Project](#how-to-run-the-project)
12. [Output Files and Results](#output-files-and-results)
13. [Troubleshooting](#troubleshooting)

---

## Project Overview

This project implements an end-to-end automated pipeline for collecting, cleaning, matching, validating, and analysing grocery product prices across five Pakistani supermarket chains. The goal is to enable cross-store price comparison and market structure analysis at scale.

**Key metrics (current dataset):**

| Metric | Value |
|--------|-------|
| Total cleaned rows | 525,704 |
| Supermarket chains | 5 (Metro, Imtiaz, Naheed, AlFatah, ChaseUp) |
| Cross-store matched rows | 18,731 |
| Match groups | 2,217 |
| Average match confidence | 0.925 |
| Validation checks passed | 0 FAIL on both layers |

The pipeline is **fully re-runnable** — each execution wipes downstream output directories and regenerates all results from scratch.

---

## Technologies and Tools

| Technology | Version | Purpose |
|------------|---------|---------|
| **Python** | 3.10+ | Core programming language |
| **requests** | ≥ 2.31 | HTTP requests for REST API scraping |
| **BeautifulSoup4** | ≥ 4.12 | HTML parsing for WooCommerce sites |
| **lxml** | ≥ 4.9 | Fast HTML/XML parser backend |
| **Playwright** | ≥ 1.40 | Headless browser for JS-rendered pages (fallback) |
| **pandas** | ≥ 2.1 | DataFrame processing, cleaning, and aggregation |
| **pyarrow** | ≥ 14.0 | Parquet file read/write support |
| **RapidFuzz** | ≥ 3.5 | Fuzzy string matching for entity resolution |
| **tqdm** | ≥ 4.66 | Progress bars for long-running operations |
| **tenacity** | ≥ 8.2 | Retry logic with exponential backoff |
| **SciPy** | ≥ 1.11 | Statistical functions (Pearson/Spearman correlation, Z-scores) |

---

## Project Score by Criteria

Scored out of **100** based on current implementation quality, reproducibility, and analytical coverage.

| Criteria | Weight | Score |
|----------|--------|-------|
| End-to-end pipeline completeness | 20 | 19 |
| Data quality and validation rigor | 20 | 18 |
| Entity matching quality and confidence | 15 | 13 |
| Analytical depth and market insights | 15 | 14 |
| Code structure and maintainability | 15 | 13 |
| Documentation and usability | 15 | 14 |
| **Overall** | **100** | **91/100** |

---

## Project Architecture and Folder Structure

```
dataAssignment/
│
├── config.py                    # Single source of truth for all settings
├── run_scrapers.py              # CLI entry-point — runs the full pipeline
├── requirements.txt             # Python dependencies
├── README.md                    # This file
│
├── scrapers/                    # Layer 1 — Web Scraping
│   ├── __init__.py
│   ├── base_scraper.py          # Abstract base class (retry, rate-limit, logging)
│   ├── helpers.py               # Shared utilities (parse_price, extract_brand, extract_size)
│   ├── playwright_helper.py     # Async Playwright browser context for JS-rendered pages
│   ├── metro_scraper.py         # Metro Online — REST API scraper
│   ├── imtiaz_scraper.py        # Imtiaz Super Market — BlinkCo REST API scraper
│   ├── naheed_scraper.py        # Naheed — Magento 2 GraphQL API scraper
│   ├── alfatah_scraper.py       # AlFatah — WooCommerce HTML scraper + Playwright fallback
│   └── chaseup_scraper.py       # ChaseUp — BlinkCo REST API scraper
│
├── pipeline/                    # Layer 2 & 3 — Processing & Matching
│   ├── __init__.py
│   ├── processor.py             # 10-stage cleaning & normalisation pipeline
│   └── matcher.py               # 3-pass deterministic cross-store product matching
│
├── validation/                  # Layer 4 — Data Quality Checks
│   ├── __init__.py
│   └── checks.py                # 13 automated PASS / WARN / FAIL checks
│
├── analysis/                    # Layer 5 — Analytical Reports
│   ├── __init__.py
│   ├── reports.py               # All §3.1–§3.4 analyses (dispersion, LDI, correlation)
│   └── report_generator.py      # Auto-generates Markdown technical report
│
├── data/                        # Output data (created automatically at runtime)
│   ├── raw/                     # Raw scraped data (one CSV per store)
│   ├── processed/               # Cleaned & normalised dataset
│   ├── matched/                 # Cross-store matched products + statistics
│   ├── validation/              # Validation report CSVs
│   ├── analysis/                # 14 analytical output CSV files
│   └── report/                  # Auto-generated technical report (Markdown)
│
└── logs/                        # Per-module log files (created at runtime)
```

The pipeline follows a **five-layer architecture**:

```
Scraping  →  Cleaning & Normalisation  →  Entity Resolution  →  Validation  →  Storage & Analysis
```

---

## Data Scraping Process

All five scrapers inherit from `BaseScraper`, which provides:

- **Exponential backoff retry** — up to 5 attempts per request via `tenacity`
- **Rate limiting** — minimum 1.5 seconds between consecutive requests
- **Playwright fallback** — automatic headless browser fallback for JS-rendered pages
- **Batch persistence** — saves every 500 rows to prevent data loss on failure
- **Structured logging** — timestamped log files in `logs/`
- **Thread pool** — parallel category scraping (4 threads per store)

### Store Details

| Store | Website | Technique | Cities | Approx. Rows |
|-------|---------|-----------|--------|--------------|
| **Metro** | metro-online.pk | REST API (JSON) | Karachi, Lahore, Islamabad + 9 more | ~136,000 |
| **Naheed** | naheed.pk | Magento 2 GraphQL API | Karachi, Lahore, Islamabad + 9 more | ~375,000 |
| **Imtiaz** | shop.imtiaz.com.pk | BlinkCo REST API | Karachi, Hyderabad, Faisalabad | ~4,200 |
| **ChaseUp** | chaseupgrocery.com | BlinkCo REST API | Karachi | ~6,500 |
| **AlFatah** | alfatah.com.pk | WooCommerce HTML + Playwright | Lahore, Islamabad | ~2,900 |

Each scraper produces a CSV file in `data/raw/` with a standardised 13-column schema:

```
store_name, city, product_name, brand, category, price, original_price,
currency, size, product_url, image_url, in_stock, scrape_timestamp
```

### Scraper-Specific Details

**Metro** — Bypasses the Next.js frontend and hits the admin REST API at `admin.metro-online.pk`. Categories are discovered via `/api/read/Categories`, and products are fetched in paginated batches of 48. Products are expanded across 12 cities since Metro serves one national catalogue.

**Naheed** — Uses the Magento 2 public GraphQL endpoint at `naheed.pk/graphql`. Discovers grocery sub-categories under root category ID 46, then paginates products per category. Products are expanded across 12 cities (national catalogue).

**Imtiaz** — Uses the BlinkCo.io platform REST API with custom headers (`app-name: imtiazsuperstore`, `rest-id: 55126`). Discovers categories via `/api/menu-section`, then fetches products per subsection with pagination.

**ChaseUp** — Also uses the BlinkCo platform (`app-name: chaseup`, `rest-id: 55525`). The full product catalogue (~6,700 products) is returned in a single `/api/menu` call per branch, parsed from a nested JSON hierarchy.

**AlFatah** — A WooCommerce site scraped via HTML parsing with BeautifulSoup. Uses hardcoded seed category URLs. When HTML parsing yields no results, falls back to Playwright headless browser rendering.

---

## Data Cleaning and Normalisation

The cleaning pipeline (`pipeline/processor.py`) applies 10 sequential stages:

| Stage | Operation | Details |
|-------|-----------|---------|
| 1 | **Price conversion** | Coerce string prices to numeric; flag placeholders (0, 0.01) |
| 2 | **Currency normalisation** | Map `Rs`, `Rs.`, `₨` → `PKR` |
| 3 | **Quantity extraction** | Regex extraction of value + unit from product names (e.g., `500g`, `2×250ml`) |
| 4 | **Size standardisation** | Convert all units to base form: `kg→g`, `l→ml`, `oz→g`, `lb→g` |
| 5 | **Brand normalisation** | 60+ brand alias dictionary merges spelling variants (e.g., `K&N's` / `K&Ns` / `KnN`) |
| 6 | **Missing value logging** | Log per-column null percentages for diagnostics |
| 7 | **Exact deduplication** | Remove rows identical on `(store, city, product_name, price)` |
| 8 | **Near-duplicate removal** | Same product with price within 2% is collapsed |
| 9 | **Price-per-unit** | Compute `price ÷ size_in_base_unit` for cross-product comparison |
| 10 | **Clean product name** | Lowercase, strip special characters, standardise whitespace |

**Output:** `data/processed/products_processed.csv` (525,704 rows)

---

## Entity Matching Methodology

The entity resolution engine (`pipeline/matcher.py`) uses a **3-pass deterministic approach** to match identical or near-identical products across different stores:

### Pass 1 — Exact Key Match (Confidence: 1.00)
- Composite key: `(product_name_clean, brand_normalised, size_base_value, size_base_unit)`
- Only identical normalised keys match
- Estimated precision: ~99%

### Pass 2 — Fuzzy Name Match (Confidence: score/100)
- Algorithm: RapidFuzz `token_sort_ratio`
- Threshold: ≥ 88 (configurable via `config.FUZZY_THRESHOLD`)
- Blocking: by brand only (categories have near-zero cross-store overlap)
- Token cleaning: stopwords removed, compound words split, size tokens stripped
- Estimated precision: ~88–92%

### Pass 3 — Brand + Size Fallback (Confidence: 0.70)
- Groups products sharing `(brand, base_size, base_unit)` across stores
- Intra-group name similarity filtering (token_sort_ratio ≥ 88) prevents grouping distinct products
- Estimated precision: ~75–80%

### Matching Results

| Pass | Method | Matched Rows |
|------|--------|-------------|
| 1 | Exact key | 12,218 |
| 2 | Fuzzy match | 2,288 |
| 3 | Brand + size | 4,225 |
| **Total** | | **18,731** |

**Outputs in `data/matched/`:**

| File | Description |
|------|-------------|
| `matched_products.csv` | All matched rows with `match_id` and `match_confidence` |
| `cross_store_matches.csv` | Subset of matches appearing in ≥ 2 stores |
| `match_statistics.csv` | Per-pass counts, precision, and recall estimates |
| `precision_recall_report.csv` | Detailed threshold sensitivity documentation |

---

## Validation Checks

The validation layer (`validation/checks.py`) runs **13 automated data quality checks** on both the processed and matched datasets. Results are saved as CSV reports.

| Check | What It Verifies | Threshold |
|-------|-----------------|-----------|
| `row_count_500k` | ≥ 500,000 total cleaned rows | 500,000 |
| `required_columns` | All mandatory columns present | 8 columns |
| `missing_store_name` | Null % for store_name | 0% |
| `missing_product_name` | Null % for product_name | 0% |
| `missing_price` | Null % for price | 0% |
| `missing_city` | Null % for city | ≤ 5% |
| `missing_category` | Null % for category | ≤ 10% |
| `missing_brand` | Null % for brand | ≤ 15% |
| `missing_currency` | Null % for currency | ≤ 1% |
| `duplicates` | Duplicate row percentage | < 5% |
| `unit_consistency` | All unit values in valid canonical set | 100% valid |
| `outliers_iqr` | Price outliers via IQR method | < 20% |
| `outliers_zscore` | Price outliers via Z-score (\|Z\| > 3) | < 5% |
| `price_per_unit_bounds` | Price-per-unit within sane range | 0.001–50,000 PKR |
| `price_range` | No negative or extreme (> 500K) prices | 0–500,000 PKR |
| `store_coverage` | ≥ 3 supermarket chains present | 3+ stores |
| `city_coverage_*` | ≥ 2 cities per chain (where applicable) | 2+ cities |
| `matched_product_count` | ≥ 10,000 matched product rows | 10,000 |

Each check produces one of: **PASS**, **WARN**, **FAIL**, or **SKIP**.

**Output reports:**
- `data/validation/validation_report_processed.csv`
- `data/validation/validation_report_matched.csv`

---

## Price Dispersion and Market Structure Analysis

The analysis module (`analysis/reports.py`) implements all computations from Sections 3.1–3.4 of the assignment specification.

### §3.1 — Price Dispersion Metrics (Product-Level)

| Metric | Description |
|--------|-------------|
| Mean, Median, Std | Central tendency and spread per matched product |
| Coefficient of Variation (CV) | Std / Mean — normalised measure of price dispersion |
| Price Range | Max − Min across stores |
| Interquartile Range (IQR) | Q75 − Q25 |
| Price Spread Ratio | Max / Min — how much more the most expensive store charges |
| Relative Price Position Index (RPPI) | Store price / category mean — shows if a store is above or below market average |

**Output files:** `price_dispersion.csv`, `rppi_per_product.csv`, `relative_price_position_index.csv`

### §3.2 — Store-Level Aggregated Metrics

For each store (and city combination):

| Metric | Description |
|--------|-------------|
| Average Category Price Index | Store-city category average / overall category average |
| Median Price Deviation | Median difference from market-average price |
| Price Volatility Score | Average CV across all products the store sells |
| Price Leadership Frequency | % of products where the store has the lowest price |

**Output file:** `store_level_metrics.csv`

### §3.3 — Leader Dominance Index (LDI)

| Metric | Description |
|--------|-------------|
| LDI | Fraction of products where a store has the lowest price |
| Weighted LDI | LDI weighted by category size (normalised to [0, 1]) |
| Category-wise LDI | LDI computed within each product category separately |

**Output files:** `leader_dominance_index.csv`, `category_wise_ldi.csv`

### §3.4 — Correlation and Competition Analysis

| Analysis | Method | Output File |
|----------|--------|-------------|
| Size vs. Price Dispersion | Pearson + Spearman correlation | `corr_size_vs_dispersion.csv` |
| Number of Competitors vs. Spread | Pearson + Spearman correlation | `corr_competitors_vs_spread.csv` |
| Brand Tier vs. Volatility | Spearman correlation (economy/mid/premium tiers) | `corr_brand_tier_vs_volatility.csv` |
| City-wise Price Correlation | Pearson & Spearman matrices | `city_corr_pearson.csv`, `city_corr_spearman.csv` |
| Cross-Store Price Synchronisation | Pairwise Pearson between store price vectors | `cross_store_price_sync.csv`, `cross_store_sync_summary.csv` |

Each correlation output includes the computed r-value, p-value, sample size, and a plain-English interpretation.

### Summary Statistics

`summary_stats.csv` — High-level dataset statistics (total products, unique products, stores, cities, categories, brands, average/median price).

---

## Installation

### 1. Navigate to the project directory

```bash
cd "d:\uni data\Assignment new\dataAssignment"
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Install Playwright browser

```bash
python -m playwright install chromium
```

This downloads a headless Chromium binary used by the AlFatah scraper as a fallback for JS-rendered pages.

### Prerequisites

| Requirement | Details |
|-------------|---------|
| **Python** | 3.10 or higher |
| **pip** | Python package manager |
| **Internet** | Required for scraping live supermarket websites |
| **OS** | Windows, macOS, or Linux |

---

## How to Run the Project

### Step 1 — Run the full pipeline

```bash
python run_scrapers.py
```

This executes the complete pipeline in order:
1. **Scrape** all 5 stores → `data/raw/`
2. **Clean & normalise** → `data/processed/`
3. **Validate** processed data → `data/validation/`
4. **Match** products across stores → `data/matched/`
5. **Validate** matched data → `data/validation/`
6. **Analyse** (§3.1–§3.4) → `data/analysis/`
7. **Generate** technical report → `data/report/`
8. **Verify** all deliverables are present

### Step 2 — Review results

After the pipeline completes, check:
- `data/validation/validation_report_processed.csv` — processed data quality
- `data/validation/validation_report_matched.csv` — matched data quality
- `data/analysis/` — all analytical CSV files
- `data/report/technical_report.md` — comprehensive technical report

### Common CLI Options

| Command | Description |
|---------|-------------|
| `python run_scrapers.py` | Run full pipeline for all 5 stores |
| `python run_scrapers.py --stores metro imtiaz` | Scrape only Metro and Imtiaz |
| `python run_scrapers.py --stores naheed chaseup` | Scrape only Naheed and ChaseUp |
| `python run_scrapers.py --target-rows 50000` | Stop after ~50K rows per store |
| `python run_scrapers.py --no-pipeline` | Scrape only, skip processing/matching/analysis |
| `python run_scrapers.py --pipeline-only` | Skip scraping, run pipeline on existing raw data |

### Pipeline Execution Flow

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  1. SCRAPE   │───▶│ 2. PROCESS  │───▶│ 3. VALIDATE │───▶│  4. MATCH   │
│  5 stores    │    │  Clean &    │    │  13 checks  │    │  3-pass     │
│  REST/HTML   │    │  normalise  │    │  processed  │    │  entity     │
│  GraphQL     │    │  10 stages  │    │  layer      │    │  resolution │
└─────────────┘    └─────────────┘    └─────────────┘    └──────┬──────┘
                                                                │
┌─────────────┐    ┌─────────────┐    ┌─────────────┐          │
│  8. VERIFY   │◀──│ 7. REPORT   │◀──│ 6. ANALYSE  │◀──┌──────┴──────┐
│  Deliverable │    │  Technical  │    │  §3.1–§3.4  │    │ 5. VALIDATE │
│  checklist   │    │  Markdown   │    │  14 outputs │    │  matched    │
└─────────────┘    └─────────────┘    └─────────────┘    │  layer      │
                                                          └─────────────┘
```

---

## Output Files and Results

### Data Layers

| Layer | Directory | File(s) | Description |
|-------|-----------|---------|-------------|
| **Raw** | `data/raw/` | `metro_raw.csv`, `imtiaz_raw.csv`, `naheed_raw.csv`, `alfatah_raw.csv`, `chaseup_raw.csv` | Original scraped data — one file per store |
| **Processed** | `data/processed/` | `products_processed.csv` | Cleaned, normalised, deduplicated dataset (525,704 rows) |
| **Matched** | `data/matched/` | `matched_products.csv`, `cross_store_matches.csv`, `match_statistics.csv`, `precision_recall_report.csv` | Cross-store matched products with confidence scores |
| **Validation** | `data/validation/` | `validation_report_processed.csv`, `validation_report_matched.csv` | PASS/WARN/FAIL quality check results |
| **Analysis** | `data/analysis/` | 14 CSV files (see below) | All §3.1–§3.4 analytical outputs |
| **Report** | `data/report/` | `technical_report.md` | Auto-generated comprehensive technical report |

### Analysis Output Files

| File | Section | Description |
|------|---------|-------------|
| `summary_stats.csv` | — | High-level dataset statistics |
| `price_dispersion.csv` | §3.1 | 8 dispersion metrics per matched product |
| `rppi_per_product.csv` | §3.1 | Per-product Relative Price Position Index |
| `relative_price_position_index.csv` | §3.1 | Aggregated RPPI by store × category |
| `store_level_metrics.csv` | §3.2 | 4 metrics per (store, city) combination |
| `leader_dominance_index.csv` | §3.3 | LDI and Weighted LDI per store |
| `category_wise_ldi.csv` | §3.3 | LDI broken down by product category |
| `corr_size_vs_dispersion.csv` | §3.4 | Size ↔ dispersion correlation |
| `corr_competitors_vs_spread.csv` | §3.4 | No. of competitors ↔ price spread correlation |
| `corr_brand_tier_vs_volatility.csv` | §3.4 | Brand tier ↔ price volatility correlation |
| `city_corr_pearson.csv` | §3.4 | City × city Pearson correlation matrix |
| `city_corr_spearman.csv` | §3.4 | City × city Spearman correlation matrix |
| `cross_store_price_sync.csv` | §3.4 | Pairwise store price synchronisation matrix |
| `cross_store_sync_summary.csv` | §3.4 | Overall synchronisation score + interpretation |

### Log Files

Log files are generated in `logs/` during pipeline execution:

| Log File | Source Module |
|----------|--------------|
| `main.log` | Pipeline orchestrator |
| `metro.log` | Metro scraper |
| `imtiaz.log` | Imtiaz scraper |
| `naheed.log` | Naheed scraper |
| `alfatah.log` | AlFatah scraper |
| `chaseup.log` | ChaseUp scraper |
| `processor.log` | Cleaning pipeline |
| `matcher.log` | Entity resolution |
| `validation.log` | Data quality checks |
| `analysis.log` | Analysis functions |
| `report_generator.log` | Technical report generation |

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `ModuleNotFoundError` | Run `pip install -r requirements.txt` |
| Playwright browser not found | Run `python -m playwright install chromium` |
| No data after scraping | Check `logs/` for error details; websites may be temporarily down |
| All validation checks FAIL | Ensure raw data exists in `data/raw/`; run full pipeline |
| Imtiaz returns 0 products | Imtiaz BlinkCo API may be temporarily offline; check `logs/imtiaz.log` |
| Naheed returns 0 products | Naheed GraphQL API may be temporarily down; check `logs/naheed.log` |
| Rate-limited by a website | Increase `RATE_LIMIT_DELAY` in `config.py` (default: 1.5s) |
| Want CSV instead of Parquet | Set `OUTPUT_FORMAT = "csv"` in `config.py` (already the default) |

---

## License

This project is an academic assignment for FAST-NUCES CS4048 — Data Science (Spring 2026).
