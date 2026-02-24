"""
phase2/unrecognized_columns.py

Flags source columns that Phase 1 could not map to any staging column
(UNMAPPED) and surfaces fuzzy-matched columns needing human review.

Near-miss detection uses rapidfuzz to score UNMAPPED column names against
all Source_Column values in the staging table.  Scores 60-84 (below Phase 1's
FUZZY_THRESHOLD of 85 but above noise) are flagged as potential near-misses.
"""

from __future__ import annotations

import re
from typing import Any

from rapidfuzz import fuzz

_NEAR_MISS_LOW  = 60
_NEAR_MISS_HIGH = 84   # below Phase 1's FUZZY_THRESHOLD of 85

# Column name fragments that suggest system/internal fields (LOW severity)
_SYSTEM_FRAGMENTS = {
    "rowid", "row_id", "rownum", "seq", "sequence",
    "lastupdated", "lastupdatedby", "lastmodified", "lastmodifiedby",
    "created_by", "createdby", "updated_by", "updatedby",
    "extract", "extractdate", "extracttime",
    "import", "importdate", "importid",
    "systemid", "systemfield", "internal", "etl",
    "batchid", "batchnum", "jobid",
}


def flag(
    file_data: dict[str, Any],
    source: str,
    staging_table: str | None,
) -> dict[str, Any]:
    """
    Identify UNMAPPED and FUZZY columns for one file.

    Returns
    -------
    dict with keys:
        "unrecognized_findings": list[dict]
        "fuzzy_review_list":     list[dict]
    """
    from shared import staging_meta

    column_mappings: list[dict] = file_data.get("column_mappings", [])

    # Staging column names for this table (for near-miss scoring)
    all_staging_cols: list[str] = []
    if staging_table and not staging_table.startswith("("):
        all_staging_cols = staging_meta.get_all_source_columns(staging_table)

    unrecognized: list[dict[str, Any]] = []
    fuzzy_review: list[dict[str, Any]] = []

    for rec in column_mappings:
        confidence = rec.get("confidence", "")
        raw_col    = rec.get("raw_col", "")

        if confidence == "UNMAPPED":
            entry = _classify_unmapped(raw_col, all_staging_cols)
            unrecognized.append(entry)

        elif confidence.startswith("FUZZY"):
            stg_col = rec.get("staging_col") or ""
            fuzzy_review.append({
                "raw_column":        raw_col,
                "mapped_to_staging": stg_col,
                "confidence":        confidence,
                "notes": (
                    f"Fuzzy match — confirm '{raw_col}' contains data "
                    f"for '{stg_col}'"
                ),
            })

    return {
        "unrecognized_findings": unrecognized,
        "fuzzy_review_list":     fuzzy_review,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _classify_unmapped(
    raw_col: str,
    all_staging_cols: list[str],
) -> dict[str, Any]:
    """Score an UNMAPPED column and assign severity / nearest match."""
    norm_raw = _normalize(raw_col)

    # Check for system-field patterns (LOW severity)
    if _is_system_field(norm_raw):
        return {
            "raw_column":          raw_col,
            "severity":            "LOW",
            "nearest_staging_match": None,
            "nearest_score":       0,
            "notes": "No close match found — likely a system/internal field",
        }

    # Near-miss scoring
    best_col   = None
    best_score = 0
    for stg_col in all_staging_cols:
        score = fuzz.token_sort_ratio(norm_raw, _normalize(stg_col))
        if score > best_score:
            best_score = score
            best_col   = stg_col

    if best_score >= _NEAR_MISS_LOW:
        if best_score >= 85:
            # Shouldn't happen (Phase 1 would have matched it), but be safe
            severity = "HIGH"
            note = (
                f"Very close to staging column '{best_col}' "
                f"(similarity: {best_score}%) — confirm or add alias to mapping table"
            )
        elif best_score >= _NEAR_MISS_LOW:
            severity = "MEDIUM"
            note = (
                f"Possible match to '{best_col}' "
                f"(similarity: {best_score}%) — confirm with client"
            )
        return {
            "raw_column":          raw_col,
            "severity":            severity,
            "nearest_staging_match": best_col,
            "nearest_score":       best_score,
            "notes": note,
        }

    return {
        "raw_column":          raw_col,
        "severity":            "LOW",
        "nearest_staging_match": None,
        "nearest_score":       best_score,
        "notes": "No close match found — client-specific or extra column",
    }


def _is_system_field(norm_col: str) -> bool:
    """Return True if the normalized column name looks like a system field."""
    for fragment in _SYSTEM_FRAGMENTS:
        if fragment in norm_col:
            return True
    return False


def _normalize(s: str) -> str:
    """Lowercase, strip spaces/underscores/hyphens/slashes/dots/parens."""
    return re.sub(r"[\s_\-/\.\(\)#]", "", s).lower()
