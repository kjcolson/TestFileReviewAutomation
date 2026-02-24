"""
phase4/transactions_charges.py

C0: Billing Transactions <-> Billing Charges linkage and payment balance.

Runs for both combined and separate billing:
  - Separate billing: billing_charges + billing_transactions DataFrames
  - Combined billing: billing_combined DataFrame filtered into charge/transaction rows

Sub-checks:
  C0a — Charge ID Linkage (threshold: 75%)
  C0b — Payment Balance Reasonableness (threshold: 65% zero-balance)
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from shared.column_utils import resolve_column
from shared import staging_meta


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _normalize(s: Any) -> str:
    return str(s).strip().lower()


def _to_float(val: Any) -> float:
    try:
        return float(str(val).replace(",", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _build_charge_mask(df: pd.DataFrame, column_mappings: list[dict]) -> pd.Series:
    """Return boolean mask of charge rows in a combined billing DataFrame."""
    charge_codes, charge_descs = staging_meta.get_charge_type_sets()
    tt_col = resolve_column(column_mappings, "TransactionType")
    ttd_col = resolve_column(column_mappings, "TransactionTypeDesc")

    mask = pd.Series([False] * len(df), index=df.index)
    if tt_col and tt_col in df.columns and charge_codes:
        mask |= df[tt_col].astype(str).str.strip().isin(charge_codes)
    if ttd_col and ttd_col in df.columns and charge_descs:
        mask |= df[ttd_col].astype(str).str.strip().str.lower().isin(charge_descs)
    return mask


def _build_transaction_mask(df: pd.DataFrame, column_mappings: list[dict]) -> pd.Series:
    """Return boolean mask of transaction rows in a combined billing DataFrame."""
    trans_codes, trans_descs = staging_meta.get_transaction_type_sets()
    tt_col = resolve_column(column_mappings, "TransactionType")
    ttd_col = resolve_column(column_mappings, "TransactionTypeDesc")

    mask = pd.Series([False] * len(df), index=df.index)
    if tt_col and tt_col in df.columns and trans_codes:
        mask |= df[tt_col].astype(str).str.strip().isin(trans_codes)
    if ttd_col and ttd_col in df.columns and trans_descs:
        mask |= df[ttd_col].astype(str).str.strip().str.lower().isin(trans_descs)
    return mask


def _resolve_charge_id_col(df: pd.DataFrame, column_mappings: list[dict]) -> str | None:
    """Return the raw column for ChargeId or InvoiceNumber, whichever is present."""
    for staging in ("ChargeId", "InvoiceNumber"):
        raw = resolve_column(column_mappings, staging)
        if raw and raw in df.columns:
            return raw
    return None


def _resolve_amount_col(df: pd.DataFrame, column_mappings: list[dict], *staging_names) -> str | None:
    """Return the raw column for the first staging column name that resolves and is in df."""
    for staging in staging_names:
        raw = resolve_column(column_mappings, staging)
        if raw and raw in df.columns:
            return raw
    return None


# ---------------------------------------------------------------------------
# Sub-checks
# ---------------------------------------------------------------------------

def _c0a_charge_id_linkage(
    charges_df: pd.DataFrame,
    charges_maps: list[dict],
    trans_df: pd.DataFrame,
    trans_maps: list[dict],
) -> dict:
    """C0a: What % of transaction records link back to a charge?"""
    charge_id_col = _resolve_charge_id_col(charges_df, charges_maps)
    trans_id_col = _resolve_charge_id_col(trans_df, trans_maps)

    if not charge_id_col:
        return {
            "check": "C0a",
            "severity": "INFO",
            "message": "C0a skipped — no ChargeId or InvoiceNumber column in charges",
        }
    if not trans_id_col:
        return {
            "check": "C0a",
            "severity": "INFO",
            "message": "C0a skipped — no ChargeId or InvoiceNumber column in transactions",
        }

    charge_ids = set(
        charges_df[charge_id_col]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
    )
    trans_ids = (
        trans_df[trans_id_col]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
    )

    total_trans = len(trans_ids)
    if total_trans == 0:
        return {
            "check": "C0a",
            "severity": "INFO",
            "message": "C0a skipped — no non-null transaction IDs found",
        }

    matched = trans_ids.isin(charge_ids).sum()
    match_pct = matched / total_trans * 100

    unmatched_sample = (
        trans_ids[~trans_ids.isin(charge_ids)]
        .drop_duplicates()
        .head(10)
        .tolist()
    )

    if match_pct < 75:
        severity = "HIGH"
    elif match_pct < 90:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    return {
        "check": "C0a",
        "severity": severity,
        "message": (
            f"Transactions-to-charges match: {match_pct:.1f}% "
            f"({matched:,}/{total_trans:,} transaction records link to a charge)"
        ),
        "charge_id_column": charge_id_col,
        "transaction_id_column": trans_id_col,
        "charge_distinct": len(charge_ids),
        "transaction_distinct": int(trans_ids.nunique()),
        "match_count": int(matched),
        "match_pct": round(match_pct, 2),
        "unmatched_transaction_sample": unmatched_sample,
    }


def _c0b_payment_balance(
    charges_df: pd.DataFrame,
    charges_maps: list[dict],
    trans_df: pd.DataFrame,
    trans_maps: list[dict],
) -> dict:
    """C0b: Payment balance reasonableness — what % of charges are fully paid off?"""
    charge_id_col = _resolve_charge_id_col(charges_df, charges_maps)
    trans_id_col = _resolve_charge_id_col(trans_df, trans_maps)

    charge_amt_col = _resolve_amount_col(charges_df, charges_maps, "ChargeAmountOriginal")
    payment_col = _resolve_amount_col(trans_df, trans_maps, "PaymentOriginal")
    adj_col = _resolve_amount_col(trans_df, trans_maps, "AdjustmentOriginal")
    refund_col = _resolve_amount_col(trans_df, trans_maps, "RefundOriginal")

    if not charge_id_col or not trans_id_col or not charge_amt_col:
        return {
            "check": "C0b",
            "severity": "INFO",
            "message": (
                "C0b skipped — required columns not found "
                f"(charge_id={charge_id_col}, trans_id={trans_id_col}, charge_amt={charge_amt_col})"
            ),
        }

    # Build charges summary: ChargeId → ChargeAmount
    charges_work = charges_df[[charge_id_col, charge_amt_col]].copy()
    charges_work["_charge_id"] = charges_work[charge_id_col].astype(str).str.strip()
    charges_work["_charge_amt"] = charges_work[charge_amt_col].apply(_to_float)
    charges_summary = charges_work.groupby("_charge_id")["_charge_amt"].sum().reset_index()
    charges_summary.columns = ["_charge_id", "_charge_total"]

    # Build transactions summary: ChargeId → total payments/adjustments/refunds
    trans_work = trans_df[[trans_id_col]].copy()
    trans_work["_charge_id"] = trans_work[trans_id_col].astype(str).str.strip()

    for col_name, col_key in [("_payment", payment_col), ("_adj", adj_col), ("_refund", refund_col)]:
        if col_key and col_key in trans_df.columns:
            trans_work[col_name] = trans_df[col_key].apply(_to_float)
        else:
            trans_work[col_name] = 0.0

    trans_summary = (
        trans_work.groupby("_charge_id")[["_payment", "_adj", "_refund"]]
        .sum()
        .reset_index()
    )
    trans_summary["_trans_total"] = (
        trans_summary["_payment"] + trans_summary["_adj"] + trans_summary["_refund"]
    )

    # Join
    merged = charges_summary.merge(trans_summary[["_charge_id", "_trans_total"]], on="_charge_id", how="left")
    merged["_trans_total"] = merged["_trans_total"].fillna(0.0)
    merged["_outstanding"] = merged["_charge_total"] + merged["_trans_total"]

    total_charges = len(merged)
    if total_charges == 0:
        return {
            "check": "C0b",
            "severity": "INFO",
            "message": "C0b skipped — no charges found after joining",
        }

    zero_balance_count = int((merged["_outstanding"].abs() < 0.01).sum())
    zero_balance_pct = zero_balance_count / total_charges * 100

    total_charge_amount = merged["_charge_total"].sum()
    outstanding_balance = merged["_outstanding"].sum()
    avg_outstanding = merged["_outstanding"].mean()

    outstanding_balance_rate = (
        abs(outstanding_balance) / abs(total_charge_amount) * 100
        if abs(total_charge_amount) > 0.01 else 0.0
    )

    if zero_balance_pct < 65 and outstanding_balance_rate > 15:
        severity = "HIGH"
    elif zero_balance_pct < 65 or outstanding_balance_rate > 15:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    return {
        "check": "C0b",
        "severity": severity,
        "message": (
            f"Payment balance: {zero_balance_pct:.1f}% of charges are zero-balance "
            f"(threshold: 65%); outstanding balance rate: {outstanding_balance_rate:.1f}% "
            f"(threshold: ≤15%)"
        ),
        "total_charges": total_charges,
        "zero_balance_count": zero_balance_count,
        "zero_balance_pct": round(zero_balance_pct, 2),
        "outstanding_balance": round(float(outstanding_balance), 2),
        "total_charge_amount": round(float(total_charge_amount), 2),
        "outstanding_balance_rate": round(outstanding_balance_rate, 2),
        "avg_outstanding_balance": round(float(avg_outstanding), 2),
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_checks(
    file_entries: dict[str, dict],
    billing_format: str,
) -> dict:
    """
    Run C0 checks.

    Parameters
    ----------
    file_entries : dict[filename → {df, source, column_mappings, ...}]
        All loaded file entries from loader.load_files().
    billing_format : str
        "combined", "separate", "none", or "unknown" from phase1_findings.json["billing_format"]["format"].

    Returns
    -------
    dict with keys "C0a" and "C0b", each a finding dict, plus
    top-level "check", "files_compared", "skipped" flag.
    """
    # Locate relevant DataFrames
    combined_entry = None
    charges_entry = None
    trans_entry = None

    for fname, entry in file_entries.items():
        source = entry.get("source", "")
        if source == "billing_combined" and entry.get("df") is not None:
            combined_entry = entry
        elif source == "billing_charges" and entry.get("df") is not None:
            charges_entry = entry
        elif source == "billing_transactions" and entry.get("df") is not None:
            trans_entry = entry

    # Determine what we have
    has_combined = combined_entry is not None
    has_charges = charges_entry is not None
    has_transactions = trans_entry is not None

    if not has_combined and not (has_charges and has_transactions):
        # Not enough billing data — skip
        skip_msg = "Skipped — no billing file(s) available for cross-source check"
        if has_charges and not has_transactions:
            skip_msg = "Skipped — billing_transactions file not present"
        elif has_transactions and not has_charges:
            skip_msg = "Skipped — billing_charges file not present"
        elif not has_combined and not has_charges and not has_transactions:
            skip_msg = "Skipped — no billing files present"
        return {
            "check": "C0",
            "severity": "INFO",
            "message": skip_msg,
            "skipped": True,
        }

    # Split combined billing if needed
    if has_combined:
        combined_df = combined_entry["df"]
        combined_maps = combined_entry.get("column_mappings", [])

        charge_mask = _build_charge_mask(combined_df, combined_maps)
        trans_mask = _build_transaction_mask(combined_df, combined_maps)

        charges_df = combined_df[charge_mask].reset_index(drop=True)
        trans_df = combined_df[trans_mask].reset_index(drop=True)

        charges_maps = combined_maps
        trans_maps = combined_maps
        files_compared = f"billing_combined (charge rows: {len(charges_df):,}, transaction rows: {len(trans_df):,})"
    else:
        charges_df = charges_entry["df"]
        charges_maps = charges_entry.get("column_mappings", [])
        trans_df = trans_entry["df"]
        trans_maps = trans_entry.get("column_mappings", [])
        files_compared = "billing_charges + billing_transactions"

    if len(charges_df) == 0 or len(trans_df) == 0:
        return {
            "check": "C0",
            "severity": "INFO",
            "message": f"Skipped — insufficient rows after filtering (charges: {len(charges_df)}, transactions: {len(trans_df)})",
            "skipped": True,
            "files_compared": files_compared,
        }

    c0a = _c0a_charge_id_linkage(charges_df, charges_maps, trans_df, trans_maps)
    c0b = _c0b_payment_balance(charges_df, charges_maps, trans_df, trans_maps)

    # Overall severity for C0 = worst of sub-checks
    sev_order = {"HIGH": 0, "MEDIUM": 1, "PASS": 2, "INFO": 3}
    worst = min(
        [c0a.get("severity", "INFO"), c0b.get("severity", "INFO")],
        key=lambda s: sev_order.get(s, 3),
    )

    return {
        "check": "C0",
        "severity": worst,
        "files_compared": files_compared,
        "skipped": False,
        "sub_checks": {
            "C0a": c0a,
            "C0b": c0b,
        },
    }
