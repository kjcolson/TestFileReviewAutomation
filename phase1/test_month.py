"""
phase1/test_month.py

Identifies the test month from each file's filter date field and
checks that all core files are aligned to the same calendar month.
(spec §1.8)
"""

from __future__ import annotations

from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Filter staging column per source  (spec §1.8)
# ---------------------------------------------------------------------------

SOURCE_FILTER_COL: dict[str, str] = {
    "billing_combined":     "PostDate",
    "billing_charges":      "PostDate",
    "billing_transactions": "PostDate",
    "scheduling":           "ApptDate",
    "payroll":              "PayPeriodEndDate",
    "gl":                   "YearMonth",
    "quality":              "MeasurementPeriodEndDate",
    "patient_satisfaction": "SurveyDateRangeEnd",
}

# For sources without a staging table, fall back to matching these
# raw column name fragments (case-insensitive substring).
RAW_DATE_FALLBACKS: dict[str, list[str]] = {
    "quality":              ["measurement period end", "period end"],
    "patient_satisfaction": ["survey date range end", "survey end"],
}

# Core sources whose alignment is considered mandatory
CORE_SOURCES = {
    "billing_combined", "billing_charges", "billing_transactions",
    "scheduling", "payroll", "gl",
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def identify_test_month(
    file_dict:          dict[str, dict[str, Any]],
    mapping_results:    dict[str, list[dict[str, Any]]],
    source_assignments: dict[str, str],
) -> dict[str, Any]:
    """
    Identify the test month for each file and check alignment.

    Returns
    -------
    {
        "test_month":  "YYYY-MM" | None,   # consensus month
        "aligned":     bool,
        "per_file": {
            filename: {
                "source":         str,
                "filter_field":   str | None,   # raw col name used
                "min_date":       str | None,
                "max_date":       str | None,
                "implied_month":  "YYYY-MM" | None,
                "note":           str,
            }
        }
    }
    """
    per_file: dict[str, dict[str, Any]] = {}

    for filename, meta in file_dict.items():
        df: pd.DataFrame | None = meta.get("df")
        source = source_assignments.get(filename, "unknown")
        if df is None or source == "unknown":
            per_file[filename] = _empty_result(source, "File not parsed or source unknown")
            continue

        per_file[filename] = _analyse_file(df, source, mapping_results.get(filename, []))

    # Determine consensus test month from core sources
    core_months = [
        info["implied_month"]
        for fn, info in per_file.items()
        if source_assignments.get(fn) in CORE_SOURCES
        and info.get("implied_month")
    ]

    if not core_months:
        return {"test_month": None, "aligned": False, "per_file": per_file}

    month_counts: dict[str, int] = {}
    for m in core_months:
        month_counts[m] = month_counts.get(m, 0) + 1

    consensus = max(month_counts, key=lambda m: month_counts[m])
    aligned = all(m == consensus for m in core_months)

    return {
        "test_month": consensus,
        "aligned":    aligned,
        "per_file":   per_file,
    }


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _empty_result(source: str, note: str) -> dict[str, Any]:
    return {
        "source":        source,
        "filter_field":  None,
        "min_date":      None,
        "max_date":      None,
        "implied_month": None,
        "note":          note,
    }


def _analyse_file(
    df: pd.DataFrame,
    source: str,
    mapping_records: list[dict[str, Any]],
) -> dict[str, Any]:
    """Extract date range and implied month from the appropriate column."""
    filter_staging_col = SOURCE_FILTER_COL.get(source)
    raw_col = _find_raw_col(filter_staging_col, mapping_records, df, source)

    if not raw_col:
        return _empty_result(source, f"Filter date column not found (expected staging: {filter_staging_col})")

    # Special handling for GL — YearMonth is stored as YYYYMM integer
    if source == "gl":
        return _handle_gl_yearmonth(df, raw_col, source)

    date_series = _parse_dates(df[raw_col])
    if date_series.isna().all():
        return _empty_result(source, f"Could not parse dates in column '{raw_col}'")

    min_date = date_series.min()
    max_date = date_series.max()
    implied  = _infer_month(date_series, source)

    return {
        "source":        source,
        "filter_field":  raw_col,
        "min_date":      min_date.strftime("%Y-%m-%d") if pd.notna(min_date) else None,
        "max_date":      max_date.strftime("%Y-%m-%d") if pd.notna(max_date) else None,
        "implied_month": implied,
        "note":          "",
    }


def _find_raw_col(
    staging_col: str | None,
    mapping_records: list[dict[str, Any]],
    df: pd.DataFrame,
    source: str,
) -> str | None:
    """Find the raw column name that maps to *staging_col*."""
    if staging_col:
        for rec in mapping_records:
            if staging_col in rec.get("staging_cols", []):
                return rec["raw_col"]

    # Fallback: case-insensitive substring search in actual column names
    fallback_terms = RAW_DATE_FALLBACKS.get(source, [])
    for col in df.columns:
        col_lower = col.lower()
        if staging_col and staging_col.lower() in col_lower:
            return col
        for term in fallback_terms:
            if term.lower() in col_lower:
                return col
    return None


def _parse_dates(series: pd.Series) -> pd.Series:
    """Attempt to parse a string series as dates."""
    return pd.to_datetime(series, errors="coerce")


def _handle_gl_yearmonth(df: pd.DataFrame, raw_col: str, source: str) -> dict[str, Any]:
    """GL YearMonth — try YYYYMM integer first, fall back to date parsing."""
    raw_series = df[raw_col].dropna()
    if raw_series.empty:
        return _empty_result(source, f"No values in '{raw_col}'")

    numeric_vals = pd.to_numeric(raw_series, errors="coerce")
    non_null_numeric = numeric_vals.dropna()

    def ym_to_str(ym: int) -> str:
        year, month = divmod(ym, 100)
        return f"{year:04d}-{month:02d}"

    # If most values parse as numbers in YYYYMM range, treat as YYYYMM integers
    if len(non_null_numeric) > 0 and len(non_null_numeric) / len(raw_series) > 0.5:
        int_vals = non_null_numeric.astype(int)
        in_range = int_vals.between(200001, 203012)
        if in_range.mean() > 0.5:
            min_ym = ym_to_str(int(int_vals.min()))
            max_ym = ym_to_str(int(int_vals.max()))
            mode_ym = int_vals.mode().iloc[0]
            implied = ym_to_str(int(mode_ym))
            return {
                "source":        source,
                "filter_field":  raw_col,
                "min_date":      min_ym,
                "max_date":      max_ym,
                "implied_month": implied,
                "note":          "YearMonth (YYYYMM) format",
            }

    # Fall back to date parsing — GL Report Period is not in requested YYYYMM format
    date_series = pd.to_datetime(raw_series, errors="coerce")
    valid_dates = date_series.dropna()
    if valid_dates.empty:
        return _empty_result(source, f"No parseable date or YearMonth values in '{raw_col}'")

    # Detect the original format for the note
    sample_val = str(raw_series.iloc[0]).strip()

    # Convert to YYYYMM for analysis
    ym_series = valid_dates.dt.year * 100 + valid_dates.dt.month
    min_ym = ym_to_str(int(ym_series.min()))
    max_ym = ym_to_str(int(ym_series.max()))
    mode_ym = ym_series.mode().iloc[0]
    implied = ym_to_str(int(mode_ym))

    return {
        "source":        source,
        "filter_field":  raw_col,
        "min_date":      min_ym,
        "max_date":      max_ym,
        "implied_month": implied,
        "note":          f"GL Report Period is not in the requested YYYYMM format (e.g. '{sample_val}') — converted to YYYYMM for analysis",
    }


def _infer_month(date_series: pd.Series, source: str) -> str | None:
    """
    Determine the single implied calendar month for this file.

    For billing, the window is the 15th of prior month → 14th of current month,
    so majority-month logic is applied.  For all others, the modal month is used.
    """
    clean = date_series.dropna()
    if clean.empty:
        return None
    months = clean.dt.to_period("M")
    mode_month = months.mode()
    if mode_month.empty:
        return None
    return str(mode_month.iloc[0])
