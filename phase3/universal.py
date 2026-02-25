"""
phase3/universal.py

Universal data quality checks that apply to every data source.
Checks 1-6: null/blank, duplicates, date range, numeric range, placeholder, encoding.
"""

from __future__ import annotations

import re
from datetime import datetime, date

import pandas as pd
import numpy as np

from shared.column_utils import resolve_column
from shared import staging_meta

# Charge-conditional columns: only null-checked on charge rows in billing_combined
_CHARGE_CONDITIONAL_COLS = {
    "CptCode", "Units", "WorkRvuOriginal", "PrimaryIcdCode", "SecondaryIcdCodes",
}

# Placeholder values to detect (exact match, case-insensitive)
_PLACEHOLDER_EXACT = {
    "test", "testing", "xxx", "zzz", "tbd", "n/a", "na", "null", "none",
    "dummy", "sample", "fake", "placeholder", "default", "todo", "temp",
    "unknown", "john doe", "jane doe", "test patient", "mickey mouse",
    "donald duck", "0000000000", "1111111111", "9999999999", "aaaa",
    "1234567890", "000000", "999999", "test001", "patient1",
}

# Mojibake patterns indicating encoding problems
_MOJIBAKE_PATTERNS = re.compile(r"Ã©|Ã¡|â€™|Ã¶|Â|Ã[^A-Z]|\ufffd")


def _build_charge_mask(df: pd.DataFrame, column_mappings: list[dict]) -> pd.Series | None:
    """Return boolean Series identifying charge rows; None if cannot determine."""
    charge_codes, charge_descs = staging_meta.get_charge_type_sets()
    tt_col = resolve_column(column_mappings, "TransactionType")
    ttd_col = resolve_column(column_mappings, "TransactionTypeDesc")

    mask = pd.Series([False] * len(df), index=df.index)
    if tt_col and tt_col in df.columns and charge_codes:
        mask |= df[tt_col].astype(str).str.strip().isin(charge_codes)
    if ttd_col and ttd_col in df.columns and charge_descs:
        mask |= df[ttd_col].astype(str).str.strip().str.lower().isin(charge_descs)
    return mask if mask.any() else None


def _sev(requirement_level: str, missing_pct: float) -> str | None:
    """Return severity for a null finding based on requirement level and missing %."""
    rl = requirement_level.lower()
    if rl == "required":
        if missing_pct > 50:
            return "CRITICAL"
        if missing_pct > 0:
            return "HIGH"
    elif rl == "recommended":
        if missing_pct > 10:
            return "MEDIUM"
    elif rl == "optional":
        if missing_pct > 25:
            return "INFO"
    return None


def check_null_blank(
    df: pd.DataFrame,
    column_mappings: list[dict],
    source: str,
) -> list[dict]:
    """Check 1: Null / blank values in Required and Recommended fields."""
    findings = []
    charge_mask: pd.Series | None = None
    if source == "billing_combined":
        charge_mask = _build_charge_mask(df, column_mappings)

    for mapping in column_mappings:
        raw_col = mapping.get("raw_col", "")
        staging_col = mapping.get("staging_col", "")
        req_level = mapping.get("requirement_level", "Optional")

        if not raw_col or raw_col not in df.columns:
            continue
        if req_level.lower() not in ("required", "recommended", "optional"):
            continue

        series = df[raw_col]
        use_mask = (
            source == "billing_combined"
            and staging_col in _CHARGE_CONDITIONAL_COLS
        )
        if use_mask:
            if charge_mask is None:
                continue  # no charge rows identified; skip
            series = series[charge_mask]

        total = len(series)
        if total == 0:
            continue

        null_count = int(series.isna().sum())
        blank_count = int(
            (series.dropna().astype(str).str.strip() == "").sum()
        )
        total_missing = null_count + blank_count
        if total_missing == 0:
            continue

        missing_pct = total_missing / total * 100
        severity = _sev(req_level, missing_pct)
        if severity is None:
            continue

        # Collect sample row indices (up to 20)
        null_mask = series.isna() | (series.astype(str).str.strip() == "")
        sample_rows = [int(i) for i in null_mask[null_mask].index[:20]]

        findings.append({
            "check": "null_blank",
            "raw_column": raw_col,
            "staging_column": staging_col,
            "requirement_level": req_level,
            "null_count": null_count,
            "blank_count": blank_count,
            "total_missing": total_missing,
            "missing_pct": round(missing_pct, 2),
            "charge_rows_only": use_mask,
            "sample_rows": sample_rows,
            "severity": severity,
            "message": (
                f"{req_level} field '{raw_col}' has {total_missing:,} missing values "
                f"({missing_pct:.1f}%)"
                + (" [charge rows only]" if use_mask else "")
            ),
        })

    return findings


def check_duplicates(
    df: pd.DataFrame,
    column_mappings: list[dict],
    source: str,
) -> list[dict]:
    """Check 2: Full-row duplicate records."""
    findings = []
    full_dup_mask = df.duplicated(keep=False)
    full_dup_count = int(full_dup_mask.sum())
    if full_dup_count > 0:
        findings.append({
            "check": "full_row_duplicates",
            "duplicate_row_count": full_dup_count,
            "severity": "CRITICAL",
            "message": f"{full_dup_count:,} fully identical rows found (all columns match)",
        })
    return findings


# Primary date staging column per source type
_SOURCE_DATE_COL: dict[str, str] = {
    "billing_combined":     "PostDate",
    "billing_charges":      "PostDate",
    "billing_transactions": "PostDate",
    "scheduling":           "ApptDate",
    "payroll":              "PayPeriodEndDate",
    "gl":                   "YearMonth",
}


def check_date_range(
    df: pd.DataFrame,
    column_mappings: list[dict],
    source: str,
    test_month: str,
    date_range: tuple[date, date] | None = None,
) -> list[dict]:
    """Check 3: Date range alignment.

    Window check only runs when *date_range* is explicitly provided (set via
    --date-start / --date-end in run_phase1.py and stored in phase1_findings.json).
    Invalid-date and epoch-date checks always run regardless.
    """
    findings = []

    filter_staging = _SOURCE_DATE_COL.get(source)

    # ── Window check (only when date_range is provided) ───────────────────────
    if filter_staging and date_range is not None:
        window_start, window_end = date_range
        filter_raw = resolve_column(column_mappings, filter_staging)
        if filter_raw and filter_raw in df.columns:
            parsed = pd.to_datetime(df[filter_raw], errors="coerce")
            valid_dates = parsed.dropna()
            if len(valid_dates) > 0:
                outside = (
                    (valid_dates.dt.date < window_start)
                    | (valid_dates.dt.date > window_end)
                ).sum()
                pct = outside / len(df) * 100
                if pct > 5:
                    sev = "HIGH"
                elif pct > 0:
                    sev = "MEDIUM"
                else:
                    sev = None

                if sev:
                    findings.append({
                        "check": "date_range",
                        "raw_column": filter_raw,
                        "staging_column": filter_staging,
                        "expected_window": f"{window_start} to {window_end}",
                        "outside_count": int(outside),
                        "outside_pct": round(pct, 2),
                        "severity": sev,
                        "message": (
                            f"'{filter_raw}': {outside:,} rows ({pct:.1f}%) outside expected "
                            f"window {window_start} to {window_end}"
                        ),
                    })
                else:
                    findings.append({
                        "check": "date_range",
                        "raw_column": filter_raw,
                        "staging_column": filter_staging,
                        "expected_window": f"{window_start} to {window_end}",
                        "outside_count": 0,
                        "severity": "INFO",
                        "message": (
                            f"'{filter_raw}': date range aligned with expected window "
                            f"{window_start} to {window_end}"
                        ),
                    })

    # ── Always: scan date columns for obviously invalid / epoch values ─────────
    invalid_min = date(1900, 1, 1)
    invalid_max = date(2099, 12, 31)
    epoch = date(1970, 1, 1)

    for mapping in column_mappings:
        raw_col = mapping.get("raw_col", "")
        staging_col = mapping.get("staging_col", "")
        if not raw_col or raw_col not in df.columns:
            continue
        type_info = staging_meta.get_column_type(
            mapping.get("staging_table", ""), staging_col
        )
        sql_type = (type_info.get("sql_type") or "").lower()
        if "date" not in sql_type and "time" not in sql_type:
            continue

        parsed = pd.to_datetime(df[raw_col], errors="coerce")
        valid_dates = parsed.dropna()
        if len(valid_dates) == 0:
            continue

        out_of_range = (
            (valid_dates.dt.date < invalid_min) | (valid_dates.dt.date > invalid_max)
        ).sum()
        epoch_count = (valid_dates.dt.date == epoch).sum()

        if out_of_range > 0:
            findings.append({
                "check": "invalid_dates",
                "raw_column": raw_col,
                "staging_column": staging_col,
                "invalid_count": int(out_of_range),
                "severity": "MEDIUM",
                "message": f"'{raw_col}': {out_of_range:,} dates outside valid range (1900–2099)",
            })
        if epoch_count > 0:
            findings.append({
                "check": "epoch_dates",
                "raw_column": raw_col,
                "staging_column": staging_col,
                "epoch_count": int(epoch_count),
                "severity": "MEDIUM",
                "message": f"'{raw_col}': {epoch_count:,} values equal to 1970-01-01 (likely parse failure)",
            })

    return findings


def check_numeric_range(
    df: pd.DataFrame,
    column_mappings: list[dict],
    source: str,
) -> list[dict]:
    """Check 4: Numeric value range and outlier detection."""
    findings = []

    for mapping in column_mappings:
        raw_col = mapping.get("raw_col", "")
        staging_col = mapping.get("staging_col", "")
        staging_table = mapping.get("staging_table", "")
        if not raw_col or raw_col not in df.columns:
            continue

        type_info = staging_meta.get_column_type(staging_table, staging_col)
        sql_type = (type_info.get("sql_type") or "").lower()
        if not any(t in sql_type for t in ("int", "decimal", "numeric", "float", "money")):
            continue

        series = pd.to_numeric(df[raw_col], errors="coerce").dropna()
        if len(series) < 10:
            continue

        q1 = float(series.quantile(0.25))
        q3 = float(series.quantile(0.75))
        iqr = q3 - q1

        outlier_count = 0
        if iqr > 0:
            lower = q1 - 3 * iqr
            upper = q3 + 3 * iqr
            outlier_count = int(((series < lower) | (series > upper)).sum())

        if outlier_count > 0:
            findings.append({
                "check": "numeric_outliers",
                "raw_column": raw_col,
                "staging_column": staging_col,
                "outlier_count": outlier_count,
                "q1": round(q1, 4),
                "q3": round(q3, 4),
                "min": round(float(series.min()), 4),
                "max": round(float(series.max()), 4),
                "severity": "MEDIUM",
                "message": (
                    f"'{raw_col}': {outlier_count:,} extreme outliers "
                    f"(beyond Q1-3*IQR or Q3+3*IQR; range {series.min():.2f}–{series.max():.2f})"
                ),
            })

    return findings


def check_placeholder(
    df: pd.DataFrame,
    column_mappings: list[dict],
) -> list[dict]:
    """Check 5: Test / placeholder data detection."""
    findings = []

    for mapping in column_mappings:
        raw_col = mapping.get("raw_col", "")
        staging_col = mapping.get("staging_col", "")
        req_level = mapping.get("requirement_level", "Optional")
        if not raw_col or raw_col not in df.columns:
            continue

        str_series = df[raw_col].dropna().astype(str).str.strip()
        if len(str_series) == 0:
            continue

        # Exact match only (case-insensitive)
        matched = str_series[str_series.str.lower().isin(_PLACEHOLDER_EXACT)]
        count = len(matched)
        if count == 0:
            continue

        rl = req_level.lower()
        if rl == "required":
            sev = "HIGH"
        elif rl == "recommended":
            sev = "MEDIUM"
        else:
            sev = "LOW"

        sample_values = matched.value_counts().head(5).to_dict()
        sample_values = {str(k): int(v) for k, v in sample_values.items()}

        findings.append({
            "check": "placeholder_data",
            "raw_column": raw_col,
            "staging_column": staging_col,
            "requirement_level": req_level,
            "placeholder_count": count,
            "sample_values": sample_values,
            "severity": sev,
            "message": (
                f"'{raw_col}': {count:,} placeholder/test values detected "
                f"({req_level} field)"
            ),
        })

    return findings


def check_encoding(
    df: pd.DataFrame,
    column_mappings: list[dict],
) -> list[dict]:
    """Check 6: Encoding / character issues."""
    findings = []
    control_chars = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")

    for mapping in column_mappings:
        raw_col = mapping.get("raw_col", "")
        staging_col = mapping.get("staging_col", "")
        staging_table = mapping.get("staging_table", "")
        if not raw_col or raw_col not in df.columns:
            continue

        type_info = staging_meta.get_column_type(staging_table, staging_col)
        sql_type = (type_info.get("sql_type") or "").lower()
        # Only check text columns
        if sql_type and not any(t in sql_type for t in ("char", "varchar", "nchar", "nvarchar", "text")):
            continue

        str_series = df[raw_col].dropna().astype(str)
        if len(str_series) == 0:
            continue

        mojibake_count = int(str_series.str.contains(_MOJIBAKE_PATTERNS).sum())
        control_count = int(str_series.str.contains(control_chars).sum())

        total = mojibake_count + control_count
        if total > 0:
            findings.append({
                "check": "encoding_issues",
                "raw_column": raw_col,
                "staging_column": staging_col,
                "mojibake_count": mojibake_count,
                "control_char_count": control_count,
                "total_affected": total,
                "severity": "MEDIUM",
                "message": (
                    f"'{raw_col}': {total:,} rows with encoding issues "
                    f"(mojibake: {mojibake_count}, control chars: {control_count})"
                ),
            })

    return findings


def run_all_checks(
    df: pd.DataFrame,
    column_mappings: list[dict],
    source: str,
    test_month: str,
    date_range: tuple[date, date] | None = None,
) -> list[dict]:
    """Run all 6 universal checks and return combined findings list."""
    findings = []
    findings.extend(check_null_blank(df, column_mappings, source))
    findings.extend(check_duplicates(df, column_mappings, source))
    findings.extend(check_date_range(df, column_mappings, source, test_month, date_range))
    findings.extend(check_numeric_range(df, column_mappings, source))
    findings.extend(check_placeholder(df, column_mappings))
    findings.extend(check_encoding(df, column_mappings))
    return findings
