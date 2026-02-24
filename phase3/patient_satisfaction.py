"""
phase3/patient_satisfaction.py

Source-specific data quality checks for Patient Satisfaction files.
Checks PS1-PS4.

Patient satisfaction has no staging table; columns resolved via raw names from Phase 1.
"""

from __future__ import annotations

import pandas as pd

from shared.column_utils import resolve_column


def ps1_score_range(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """PS1: Score range validation."""
    findings = []
    score_col = resolve_column(column_mappings, "Score")
    if not score_col or score_col not in df.columns:
        return findings

    scores = pd.to_numeric(df[score_col], errors="coerce")
    null_count = int(scores.isna().sum())
    valid_scores = scores.dropna()

    if len(valid_scores) == 0:
        return findings

    # Detect scale
    max_val = valid_scores.max()
    if max_val <= 5:
        scale_min, scale_max = 1, 5
        scale_name = "1-5 Likert"
    elif max_val <= 10:
        scale_min, scale_max = 1, 10
        scale_name = "1-10"
    else:
        scale_min, scale_max = 0, 100
        scale_name = "0-100 percentage"

    out_of_range = int(((valid_scores < scale_min) | (valid_scores > scale_max)).sum())
    negative_count = int((valid_scores < 0).sum())

    if out_of_range > 0 or negative_count > 0:
        findings.append({
            "check": "PS1",
            "raw_column": score_col,
            "detected_scale": scale_name,
            "out_of_range_count": out_of_range,
            "negative_count": negative_count,
            "severity": "HIGH",
            "message": (
                f"PS1: {out_of_range + negative_count:,} scores outside detected scale "
                f"({scale_name})"
            ),
        })

    if null_count > 0:
        findings.append({
            "check": "PS1",
            "raw_column": score_col,
            "null_count": null_count,
            "severity": "HIGH",
            "message": f"PS1: {null_count:,} null Score values",
        })

    findings.append({
        "check": "PS1",
        "raw_column": score_col,
        "detected_scale": scale_name,
        "min_score": round(float(valid_scores.min()), 2),
        "max_score": round(float(valid_scores.max()), 2),
        "mean_score": round(float(valid_scores.mean()), 2),
        "severity": "INFO",
        "message": f"PS1: Score scale detected: {scale_name}; range {valid_scores.min():.1f}–{valid_scores.max():.1f}",
    })

    return findings


def ps2_survey_date_range(
    df: pd.DataFrame, column_mappings: list[dict], test_month: str
) -> list[dict]:
    """PS2: Survey date range logic."""
    findings = []
    start_col = resolve_column(column_mappings, "Survey Date Range Start")
    end_col = resolve_column(column_mappings, "Survey Date Range End")

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
            "check": "PS2",
            "inverted_count": inverted,
            "severity": "HIGH",
            "message": f"PS2: {inverted:,} rows where Survey Date Start > End",
        })

    # Long survey periods (> 6 months = 180 days)
    lengths = (end_dates[both_valid] - start_dates[both_valid]).dt.days
    long_periods = int((lengths > 180).sum())
    if long_periods > 0:
        findings.append({
            "check": "PS2",
            "long_period_count": long_periods,
            "severity": "MEDIUM",
            "message": f"PS2: {long_periods:,} survey periods > 6 months — unusual for monthly submissions",
        })

    if len(lengths) > 0:
        findings.append({
            "check": "PS2",
            "severity": "INFO",
            "message": f"PS2: Survey period lengths: min {lengths.min()} days, max {lengths.max()} days",
        })

    return findings


def ps3_question_order_validation(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """PS3: Question order validation."""
    findings = []
    order_col = resolve_column(column_mappings, "Question Order")
    npi_col = resolve_column(column_mappings, "Provider NPI")

    if not order_col or order_col not in df.columns:
        return findings

    orders = pd.to_numeric(df[order_col], errors="coerce")
    null_count = int(orders.isna().sum())
    non_positive = int((orders <= 0).dropna().sum())

    if non_positive > 0:
        findings.append({
            "check": "PS3",
            "non_positive_count": non_positive,
            "severity": "MEDIUM",
            "message": f"PS3: {non_positive:,} Question Order values are <= 0 (should be positive integers)",
        })

    # Check sequential per provider
    if npi_col and npi_col in df.columns:
        gap_count = 0
        dup_count = 0

        for npi, grp in df.groupby(npi_col):
            q_orders = pd.to_numeric(grp[order_col], errors="coerce").dropna().sort_values()
            if len(q_orders) == 0:
                continue
            # Check for duplicates
            if q_orders.duplicated().sum() > 0:
                dup_count += 1
            # Check for gaps
            expected = range(int(q_orders.min()), int(q_orders.max()) + 1)
            if len(set(expected)) != len(q_orders):
                gap_count += 1

        if gap_count > 0 or dup_count > 0:
            findings.append({
                "check": "PS3",
                "providers_with_gaps": gap_count,
                "providers_with_duplicates": dup_count,
                "severity": "MEDIUM",
                "message": (
                    f"PS3: {gap_count} providers with question order gaps; "
                    f"{dup_count} providers with duplicate question orders"
                ),
            })

    return findings


def ps4_provider_npi(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """PS4: Provider NPI validation."""
    findings = []
    npi_col = resolve_column(column_mappings, "Provider NPI")
    if not npi_col or npi_col not in df.columns:
        return findings

    npi_series = df[npi_col].dropna().astype(str).str.strip()
    npi_series = npi_series[npi_series != ""]

    import re
    npi_pattern = re.compile(r"^\d{10}$")
    invalid = npi_series[~npi_series.apply(lambda v: bool(npi_pattern.match(v)))]
    distinct = npi_series.nunique()

    if len(invalid) > 0:
        findings.append({
            "check": "PS4",
            "raw_column": npi_col,
            "invalid_npi_count": len(invalid),
            "sample_values": list(invalid.value_counts().index[:5]),
            "severity": "MEDIUM",
            "message": f"PS4: {len(invalid):,} non-10-digit NPI values in Provider NPI column",
        })

    findings.append({
        "check": "PS4",
        "raw_column": npi_col,
        "distinct_npi_count": distinct,
        "severity": "INFO",
        "message": f"PS4: {distinct:,} distinct Provider NPIs in patient satisfaction data",
    })

    return findings


def run_checks(
    df: pd.DataFrame,
    column_mappings: list[dict],
    test_month: str,
) -> tuple[list[dict], dict]:
    """Run all patient satisfaction checks. Returns (findings, cross_source_prep)."""
    findings = []
    findings.extend(ps1_score_range(df, column_mappings))
    findings.extend(ps2_survey_date_range(df, column_mappings, test_month))
    findings.extend(ps3_question_order_validation(df, column_mappings))
    findings.extend(ps4_provider_npi(df, column_mappings))
    return findings, {}
