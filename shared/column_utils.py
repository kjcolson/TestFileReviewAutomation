"""
shared/column_utils.py

Utility functions for Phase 3+ column resolution.
"""

from __future__ import annotations


def resolve_column(column_mappings: list[dict], staging_col: str) -> str | None:
    """
    Return the raw column name for a given staging column, or None if unmapped.

    Iterates Phase 1's column_mappings list (each entry has 'raw_col' and
    'staging_col' keys) and returns the first raw_col that matches staging_col.
    """
    for m in column_mappings:
        if m.get("staging_col") == staging_col or staging_col in m.get("staging_cols", []):
            raw = m.get("raw_col")
            if raw:
                return raw
    return None
