"""
phase3/quality.py

Source-specific data quality checks for Quality measure files.
Checks Q1-Q5.

Quality has no staging table; columns are resolved via raw column names from Phase 1 mappings.
"""

from __future__ import annotations

import re

import pandas as pd

from shared.column_utils import resolve_column

_MEASURE_PATTERNS = [
    re.compile(r"^CMS\d+v\d+$", re.I),       # eCQM format: CMS122v12
    re.compile(r"^CMS-?\d+$", re.I),           # CMS format: CMS-122 or CMS122
    re.compile(r"^QPP-?\d+$", re.I),           # QPP format
    re.compile(r"^MIPS-?\d+$", re.I),          # MIPS format
    re.compile(r"^\d+$"),                       # Standalone number (MIPS measure #)
    re.compile(r"^NQF-?\d+$", re.I),           # NQF format
]

_INVERSE_VALID = {"y", "yes", "1", "true", "n", "no", "0", "false"}

# Known inverse measures (partial list)
_KNOWN_INVERSE_MEASURES = {"CMS122", "CMS122v"}


def q1_performance_rate_range(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """Q1: Performance rate must be 0-100."""
    findings = []
    rate_col = resolve_column(column_mappings, "Performance Rate")
    if not rate_col or rate_col not in df.columns:
        return findings

    rates = pd.to_numeric(df[rate_col], errors="coerce")
    null_count = int(rates.isna().sum())
    out_of_range = int(((rates < 0) | (rates > 100)).sum())

    if out_of_range > 0:
        findings.append({
            "check": "Q1",
            "raw_column": rate_col,
            "out_of_range_count": out_of_range,
            "severity": "HIGH",
            "message": f"Q1: {out_of_range:,} Performance Rate values outside 0-100 range",
        })

    if null_count > 0:
        findings.append({
            "check": "Q1",
            "raw_column": rate_col,
            "null_count": null_count,
            "severity": "HIGH",
            "message": f"Q1: {null_count:,} null Performance Rate values",
        })

    if out_of_range == 0 and null_count == 0:
        findings.append({
            "check": "Q1",
            "raw_column": rate_col,
            "severity": "INFO",
            "message": f"Q1: All Performance Rates are within valid range (0-100)",
        })

    return findings


def q2_numerator_denominator(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """Q2: Numerator <= Denominator logic."""
    findings = []
    num_col = resolve_column(column_mappings, "Numerator")
    den_col = resolve_column(column_mappings, "Denominator")
    exc_col = resolve_column(column_mappings, "Exclusions/Exceptions")
    rate_col = resolve_column(column_mappings, "Performance Rate")

    if not num_col or num_col not in df.columns:
        return findings
    if not den_col or den_col not in df.columns:
        return findings

    numerator = pd.to_numeric(df[num_col], errors="coerce")
    denominator = pd.to_numeric(df[den_col], errors="coerce")
    exclusions = pd.to_numeric(df[exc_col], errors="coerce").fillna(0) if (exc_col and exc_col in df.columns) else pd.Series([0] * len(df), index=df.index)

    effective_denom = denominator - exclusions
    both_valid = numerator.notna() & denominator.notna()

    num_gt_denom = int((both_valid & (numerator > effective_denom)).sum())
    zero_denom = int((denominator == 0).sum())

    if num_gt_denom > 0:
        findings.append({
            "check": "Q2",
            "numerator_gt_denominator_count": num_gt_denom,
            "severity": "HIGH",
            "message": f"Q2: {num_gt_denom:,} rows where Numerator > (Denominator - Exclusions) — logically impossible",
        })

    if zero_denom > 0:
        findings.append({
            "check": "Q2",
            "zero_denominator_count": zero_denom,
            "severity": "MEDIUM",
            "message": f"Q2: {zero_denom:,} rows with Denominator = 0 (no eligible patients for measure)",
        })

    # Verify calculated rate matches reported rate
    if rate_col and rate_col in df.columns:
        reported_rate = pd.to_numeric(df[rate_col], errors="coerce")
        calc_mask = both_valid & (effective_denom > 0)
        calc_rate = (numerator[calc_mask] / effective_denom[calc_mask]) * 100
        rep_rate = reported_rate[calc_mask]
        mismatch = ((calc_rate - rep_rate).abs() > 1).sum()
        if int(mismatch) > 0:
            findings.append({
                "check": "Q2",
                "rate_mismatch_count": int(mismatch),
                "severity": "MEDIUM",
                "message": f"Q2: {mismatch:,} rows where calculated rate differs from reported Performance Rate by > 1%",
            })

    return findings


def q3_is_inverse_validation(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """Q3: Is_Inverse field validation."""
    findings = []
    inv_col = resolve_column(column_mappings, "Is_Inverse")
    measure_col = resolve_column(column_mappings, "Measure Number")

    if not inv_col or inv_col not in df.columns:
        return findings

    inv_series = df[inv_col].astype(str).str.strip()
    non_standard = inv_series[~inv_series.str.lower().isin(_INVERSE_VALID | {"", "nan"})]

    if len(non_standard) > 0:
        findings.append({
            "check": "Q3",
            "raw_column": inv_col,
            "non_standard_count": len(non_standard),
            "sample_values": list(non_standard.value_counts().index[:5]),
            "severity": "MEDIUM",
            "message": f"Q3: {len(non_standard):,} non-standard Is_Inverse values (expected Y/N, 1/0, True/False)",
        })

    # Check known inverse measures
    if measure_col and measure_col in df.columns:
        for measure_prefix in _KNOWN_INVERSE_MEASURES:
            mask = df[measure_col].astype(str).str.startswith(measure_prefix)
            if mask.sum() > 0:
                blank_inv = mask & (inv_series.str.lower().isin({"", "nan", "n", "no", "0", "false"}))
                if blank_inv.sum() > 0:
                    findings.append({
                        "check": "Q3",
                        "severity": "INFO",
                        "message": (
                            f"Q3: {blank_inv.sum()} rows with measure '{measure_prefix}' "
                            f"have Is_Inverse = N/blank — verify this measure is not an inverse measure"
                        ),
                    })

    findings.append({
        "check": "Q3",
        "raw_column": inv_col,
        "severity": "INFO",
        "message": f"Q3: Is_Inverse distribution: {inv_series.value_counts().to_dict()}",
    })

    return findings


def q4_measure_number_format(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """Q4: Measure number format check."""
    findings = []
    measure_col = resolve_column(column_mappings, "Measure Number")
    if not measure_col or measure_col not in df.columns:
        return findings

    measure_series = df[measure_col].dropna().astype(str).str.strip()
    measure_counts = measure_series.value_counts()
    unrecognized = []

    for val, cnt in measure_counts.items():
        if not any(p.match(val) for p in _MEASURE_PATTERNS):
            unrecognized.append({"value": val, "count": int(cnt)})

    unrecog_total = sum(r["count"] for r in unrecognized)
    unrecog_pct = unrecog_total / max(len(measure_series), 1) * 100

    if unrecog_pct > 10:
        findings.append({
            "check": "Q4",
            "raw_column": measure_col,
            "unrecognized_count": len(unrecognized),
            "unrecognized_pct": round(unrecog_pct, 2),
            "unrecognized_values": unrecognized[:10],
            "severity": "MEDIUM",
            "message": f"Q4: {unrecog_pct:.1f}% of measure numbers in unrecognized format",
        })
    else:
        findings.append({
            "check": "Q4",
            "raw_column": measure_col,
            "distinct_measures": len(measure_counts),
            "severity": "INFO",
            "message": f"Q4: {len(measure_counts)} distinct measure numbers; formats recognized",
        })

    return findings


def q5_measurement_period_logic(
    df: pd.DataFrame, column_mappings: list[dict], test_month: str
) -> list[dict]:
    """Q5: Measurement period date logic."""
    findings = []
    start_col = resolve_column(column_mappings, "Measurement Period Start Date")
    end_col = resolve_column(column_mappings, "Measurement Period End Date")

    if not start_col or start_col not in df.columns:
        return findings
    if not end_col or end_col not in df.columns:
        return findings

    start_dates = pd.to_datetime(df[start_col], errors="coerce")
    end_dates = pd.to_datetime(df[end_col], errors="coerce")
    both_valid = start_dates.notna() & end_dates.notna()

    inverted = int((both_valid & (start_dates > end_dates)).sum())
    if inverted > 0:
        findings.append({
            "check": "Q5",
            "inverted_count": inverted,
            "severity": "HIGH",
            "message": f"Q5: {inverted:,} rows where Measurement Period Start > End",
        })

    # Period length analysis
    lengths = (end_dates[both_valid] - start_dates[both_valid]).dt.days + 1
    if len(lengths) > 0:
        typical = {28, 29, 30, 31, 89, 90, 91, 92, 181, 182, 183, 365, 366}
        unusual = lengths[~lengths.isin(typical)]
        if len(unusual) > len(lengths) * 0.2:
            findings.append({
                "check": "Q5",
                "unusual_period_count": len(unusual),
                "period_length_sample": lengths.value_counts().head(5).to_dict(),
                "severity": "MEDIUM",
                "message": f"Q5: {len(unusual):,} measurement periods have unusual length",
            })

    findings.append({
        "check": "Q5",
        "severity": "INFO",
        "message": f"Q5: Measurement periods present; most common length: {int(lengths.mode()[0]) if len(lengths) > 0 else 'N/A'} days",
    })

    return findings


def run_checks(
    df: pd.DataFrame,
    column_mappings: list[dict],
    test_month: str,
) -> tuple[list[dict], dict]:
    """Run all quality-specific checks. Returns (findings, cross_source_prep)."""
    findings = []
    findings.extend(q1_performance_rate_range(df, column_mappings))
    findings.extend(q2_numerator_denominator(df, column_mappings))
    findings.extend(q3_is_inverse_validation(df, column_mappings))
    findings.extend(q4_measure_number_format(df, column_mappings))
    findings.extend(q5_measurement_period_logic(df, column_mappings, test_month))
    return findings, {}
