"""
pipeline — data processing and cross-store product matching.

Public API:
    from pipeline import process_all, match_products
"""

from pipeline.processor import process_all
from pipeline.matcher import match_products

__all__ = ["process_all", "match_products"]
