"""
phase1/source_detection.py

Identifies which PIVOT data source each parsed file represents by
fingerprint-matching its column headers against known distinctive column sets.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Fingerprint definitions  (spec §1.4)
# Keys used throughout the project as source_name values.
# ---------------------------------------------------------------------------

SOURCE_FINGERPRINTS: dict[str, list[str]] = {
    "billing_combined": [
        "Transaction Type",
        "Transaction Description",
        "Amount",
        "CPT-4 Code",
        "Work RVUs",
    ],
    "billing_charges": [
        "Charge Amount",
        "CPT-4 Code",
        "Work RVUs",
    ],
    "billing_transactions": [
        "Transaction ID",
        "Payment Amount",
        "Adjustment Amount",
        "Refund Amount",
    ],
    "scheduling": [
        "Appt ID",
        "Appt Date",
        "Appt Status",
        "Appt Type",
        "Scheduled Length",
    ],
    "payroll": [
        "Employee ID",
        "Job Code ID",
        "Earnings Code",
        "Pay Period Start Date",
        "Pay Period End Date",
    ],
    "gl": [
        "Cost Center Number",
        "Cost Center Name",
        "Account #",
        "Account Description",
    ],
    "quality": [
        "Measure Number",
        "Is_Inverse",
        "Denominator",
        "Numerator",
        "Performance Rate",
    ],
    "patient_satisfaction": [
        "Survey Date Range Start",
        "Survey Date Range End",
        "Survey Question Full",
        "Question Order",
        "Score",
    ],
}

# Staging table that corresponds to each source
SOURCE_TO_STAGING: dict[str, str] = {
    "billing_combined":       "#staging_billing",
    "billing_charges":        "#staging_charges",
    "billing_transactions":   "#staging_transactions",
    "scheduling":             "#staging_scheduling",
    "payroll":                "#staging_payroll",
    "gl":                     "#staging_gl",
    "quality":                "(no staging table)",
    "patient_satisfaction":   "(no staging table)",
}

# Minimum fingerprint column hits to make an assignment
MIN_HITS = 2


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

_VALID_SOURCES = set(SOURCE_FINGERPRINTS.keys())


def load_source_overrides(input_dir: str | Path) -> dict[str, str]:
    """
    Load manual source assignments from sources.csv in the input directory.

    Expected format (CSV with header row):
        Filename,Source
        payroll_file.txt,payroll
        scheduling_file.txt,scheduling

    Returns dict[filename, source_name]. Returns {} if file not found.
    Invalid source values are skipped with a printed warning.
    """
    csv_path = Path(input_dir) / "sources.csv"
    if not csv_path.exists():
        return {}

    import csv
    overrides: dict[str, str] = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            filename = (row.get("Filename") or "").strip()
            source   = (row.get("Source")   or "").strip()
            if not filename or not source:
                continue
            if source not in _VALID_SOURCES:
                print(f"  WARNING: sources.csv — unknown source '{source}' for '{filename}', skipping.")
                continue
            overrides[filename] = source
    return overrides


def detect_sources(
    file_dict: dict[str, dict[str, Any]],
    overrides: dict[str, str] | None = None,
) -> dict[str, str]:
    """
    Map each filename to its detected source name.

    If *overrides* contains an entry for a filename, that value is used
    directly and auto-detection is skipped for that file.

    Returns
    -------
    dict[filename, source_name]
        source_name is one of the keys in SOURCE_FINGERPRINTS, or "unknown".
    """
    _overrides = overrides or {}
    results: dict[str, str] = {}
    for filename, meta in file_dict.items():
        # Priority 1: explicit sources.csv override
        if filename in _overrides:
            results[filename] = _overrides[filename]
            continue
        # Priority 2: source subfolder name (e.g. input/Client/billing_charges/file.csv)
        folder_src = meta.get("source_folder")
        if folder_src and folder_src in _VALID_SOURCES:
            results[filename] = folder_src
            continue
        # Priority 3: column-fingerprint auto-detection
        df: pd.DataFrame | None = meta.get("df")
        if df is None:
            results[filename] = "unknown"
            continue
        results[filename] = _identify_source(df.columns.tolist())
    return results


def staging_table_for(source_name: str) -> str:
    """Return the staging table name for a given source."""
    return SOURCE_TO_STAGING.get(source_name, "(unknown)")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize_col(col: str) -> str:
    """Lowercase and strip spaces/underscores/hyphens for flexible comparison."""
    return re.sub(r"[\s_\-]", "", col).lower()


def _identify_source(columns: list[str]) -> str:
    norm_cols = {_normalize_col(c) for c in columns}

    scores: dict[str, int] = {}
    for source, fingerprints in SOURCE_FINGERPRINTS.items():
        hits = sum(
            1 for fp in fingerprints
            if _normalize_col(fp) in norm_cols
        )
        scores[source] = hits

    best_source = max(scores, key=lambda s: scores[s])
    best_score = scores[best_source]

    if best_score < MIN_HITS:
        return "unknown"

    # Check for ties
    tied = [s for s, v in scores.items() if v == best_score]
    if len(tied) > 1:
        # billing_combined vs billing_charges ambiguity: if "Transaction Type" is
        # present it is combined; otherwise charges.
        if set(tied) == {"billing_combined", "billing_charges"}:
            combined_hit = _normalize_col("Transaction Type") in norm_cols
            return "billing_combined" if combined_hit else "billing_charges"
        # General tie → unknown
        return "unknown"

    return best_source
