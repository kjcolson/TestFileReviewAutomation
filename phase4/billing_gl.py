"""
phase4/billing_gl.py

C1: Billing <-> GL Cost Center Alignment.

Does the billing data have a field that connects to GL cost centers?
Reports both record-count match rate AND dollar amount affected by unmatched values,
because a low % match is acceptable when the dollar amount affected is low.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from shared.column_utils import resolve_column
from shared import staging_meta


# ---------------------------------------------------------------------------
# Billing org columns to try (in priority order)
# ---------------------------------------------------------------------------
_BILLING_ORG_STAGING_COLS = [
    "BillDepartmentId",
    "BillDepartmentName",
    "BillLocationId",
    "BillLocationName",
    "BillPracticeId",
    "BillPracticeName",
]

# GL staging column names
_GL_COST_CENTER_NUMBER = "CostCenterNumberOrig"
_GL_COST_CENTER_NAME = "CostCenterNameOrig"

# Billing amount column
_BILLING_AMOUNT_STAGING = "ChargeAmountOriginal"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(s: Any) -> str:
    return str(s).strip().lower()


def _to_float(val: Any) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _build_gl_reference_set(gl_df: pd.DataFrame, gl_maps: list[dict]) -> set[str]:
    """Build a normalized set of all GL cost center numbers and names."""
    ref: set[str] = set()
    for staging in (_GL_COST_CENTER_NUMBER, _GL_COST_CENTER_NAME):
        raw = resolve_column(gl_maps, staging)
        if raw and raw in gl_df.columns:
            vals = (
                gl_df[raw]
                .dropna()
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
            )
            ref.update(vals.str.lower())
    return ref


def _check_billing_org_column(
    billing_df: pd.DataFrame,
    org_col_raw: str,
    billing_maps: list[dict],
    gl_ref_set: set[str],
    staging_col_name: str,
) -> dict:
    """
    For one billing org column, compute match rate and unmatched dollar amounts.
    """
    amt_col = resolve_column(billing_maps, _BILLING_AMOUNT_STAGING)

    # Get distinct billing org values (non-empty)
    org_vals = (
        billing_df[org_col_raw]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
    )

    if len(org_vals) == 0:
        return None  # column is all-null; skip

    distinct_vals = set(org_vals.str.lower())
    matched_vals = {v for v in distinct_vals if v in gl_ref_set}
    unmatched_vals = distinct_vals - matched_vals

    matched_count = len(matched_vals)
    unmatched_count = len(unmatched_vals)
    total_distinct = len(distinct_vals)
    match_pct = matched_count / total_distinct * 100 if total_distinct > 0 else 100.0

    # Dollar amounts for unmatched rows
    total_charge_amount = 0.0
    unmatched_charge_amount = 0.0

    if amt_col and amt_col in billing_df.columns:
        billing_work = billing_df[[org_col_raw, amt_col]].copy()
        billing_work["_org_norm"] = billing_work[org_col_raw].astype(str).str.strip().str.lower()
        billing_work["_amt"] = billing_work[amt_col].apply(_to_float)
        # Only count positive amounts (charges, not payments/adjustments in combined billing)
        billing_work["_amt"] = billing_work["_amt"].clip(lower=0)
        total_charge_amount = billing_work["_amt"].sum()
        unmatched_rows = billing_work[billing_work["_org_norm"].isin(unmatched_vals)]
        unmatched_charge_amount = unmatched_rows["_amt"].sum()
    else:
        amt_col = None

    unmatched_charge_pct = (
        unmatched_charge_amount / total_charge_amount * 100
        if total_charge_amount > 0.01 else 0.0
    )

    # Severity
    if unmatched_count == 0:
        severity = "PASS"
        msg = f"Billing {staging_col_name}: all {total_distinct} distinct values match GL cost centers"
    elif unmatched_count / total_distinct > 0.20 and unmatched_charge_pct > 5.0:
        severity = "HIGH"
        msg = (
            f"Billing {staging_col_name}: {match_pct:.1f}% match ({matched_count}/{total_distinct} distinct values); "
            f"{unmatched_charge_pct:.1f}% of charge dollars are unmatched"
        )
    elif unmatched_count / total_distinct > 0.20:
        severity = "MEDIUM"
        msg = (
            f"Billing {staging_col_name}: {match_pct:.1f}% match ({matched_count}/{total_distinct} distinct values); "
            f"low dollar impact ({unmatched_charge_pct:.1f}% of charges)"
        )
    else:
        severity = "MEDIUM"
        msg = (
            f"Billing {staging_col_name}: {match_pct:.1f}% match ({matched_count}/{total_distinct} distinct values); "
            f"{unmatched_count} value(s) not found in GL"
        )

    # Sample of unmatched values (up to 20), with row counts and dollar amounts
    unmatched_sample = []
    if unmatched_vals and amt_col:
        billing_work = billing_df[[org_col_raw, amt_col]].copy()
        billing_work["_org_norm"] = billing_work[org_col_raw].astype(str).str.strip().str.lower()
        billing_work["_org_orig"] = billing_work[org_col_raw].astype(str).str.strip()
        billing_work["_amt"] = billing_work[amt_col].apply(_to_float).clip(lower=0)
        unmatched_rows = billing_work[billing_work["_org_norm"].isin(unmatched_vals)]
        summary = (
            unmatched_rows.groupby("_org_orig")
            .agg(row_count=("_org_orig", "count"), dollar_amount=("_amt", "sum"))
            .reset_index()
            .sort_values("dollar_amount", ascending=False)
            .head(20)
        )
        unmatched_sample = [
            {"value": row["_org_orig"], "row_count": int(row["row_count"]), "dollar_amount": round(row["dollar_amount"], 2)}
            for _, row in summary.iterrows()
        ]
    elif unmatched_vals:
        unmatched_sample = [{"value": v} for v in list(unmatched_vals)[:20]]

    return {
        "check": "C1",
        "severity": severity,
        "message": msg,
        "billing_column": staging_col_name,
        "billing_raw_column": org_col_raw,
        "matched_count": matched_count,
        "unmatched_count": unmatched_count,
        "match_pct": round(match_pct, 2),
        "total_charge_amount": round(total_charge_amount, 2),
        "unmatched_charge_amount": round(unmatched_charge_amount, 2),
        "unmatched_charge_pct": round(unmatched_charge_pct, 2),
        "unmatched_sample": unmatched_sample,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_checks(
    file_entries: dict[str, dict],
) -> dict:
    """
    Run C1 check: Billing <-> GL cost center alignment.

    Returns a finding dict with:
      "check": "C1"
      "severity": worst severity across all billing org columns
      "files_compared": str
      "skipped": bool
      "findings": list of per-column finding dicts
    """
    # Locate billing and GL DataFrames
    billing_entry = None
    billing_source = None
    gl_entry = None

    for fname, entry in file_entries.items():
        source = entry.get("source", "")
        if source in ("billing_combined", "billing_charges") and billing_entry is None:
            if entry.get("df") is not None:
                billing_entry = entry
                billing_source = source
        elif source == "gl" and entry.get("df") is not None:
            gl_entry = entry

    if billing_entry is None:
        return {
            "check": "C1",
            "severity": "INFO",
            "message": "Skipped — no billing file present",
            "skipped": True,
        }
    if gl_entry is None:
        return {
            "check": "C1",
            "severity": "INFO",
            "message": "Skipped — GL file not present",
            "skipped": True,
        }

    billing_df = billing_entry["df"]
    billing_maps = billing_entry.get("column_mappings", [])
    gl_df = gl_entry["df"]
    gl_maps = gl_entry.get("column_mappings", [])

    # Build GL reference set
    gl_ref_set = _build_gl_reference_set(gl_df, gl_maps)
    if not gl_ref_set:
        return {
            "check": "C1",
            "severity": "INFO",
            "message": "Skipped — GL file has no CostCenterNumber or CostCenterName data",
            "skipped": True,
        }

    # For combined billing, use only charge rows for amount calculations
    if billing_source == "billing_combined":
        from phase4.transactions_charges import _build_charge_mask
        charge_mask = _build_charge_mask(billing_df, billing_maps)
        if charge_mask.any():
            billing_df_for_amounts = billing_df[charge_mask].reset_index(drop=True)
        else:
            billing_df_for_amounts = billing_df
    else:
        billing_df_for_amounts = billing_df

    # Check each billing org column
    column_findings = []
    for staging_col in _BILLING_ORG_STAGING_COLS:
        raw_col = resolve_column(billing_maps, staging_col)
        if not raw_col or raw_col not in billing_df_for_amounts.columns:
            continue
        result = _check_billing_org_column(
            billing_df_for_amounts, raw_col, billing_maps, gl_ref_set, staging_col
        )
        if result is not None:
            column_findings.append(result)

    if not column_findings:
        return {
            "check": "C1",
            "severity": "INFO",
            "message": "C1: No billing org columns (dept/location/practice) found in billing file",
            "skipped": False,
            "files_compared": f"{billing_source} + gl",
            "findings": [],
        }

    # Worst severity across all columns
    sev_order = {"HIGH": 0, "MEDIUM": 1, "PASS": 2, "INFO": 3}
    worst = min(column_findings, key=lambda f: sev_order.get(f.get("severity", "INFO"), 3))
    overall_severity = worst.get("severity", "INFO")
    if overall_severity == "PASS":
        overall_severity = "PASS"

    return {
        "check": "C1",
        "severity": overall_severity,
        "message": f"C1: Billing <-> GL cost center check complete ({len(column_findings)} org column(s) checked)",
        "files_compared": f"{billing_source} + gl",
        "skipped": False,
        "findings": column_findings,
    }
