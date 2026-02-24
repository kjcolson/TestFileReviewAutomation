"""
phase4/billing_payroll.py

C2: Billing <-> Payroll Provider Name/NPI Matching.

NPI may not be present in Payroll. If absent, falls back to name-only matching.
Low name match rates are expected (hospital-employed vs contracted providers).
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from shared.column_utils import resolve_column

try:
    from rapidfuzz import fuzz as _fuzz
    _HAS_RAPIDFUZZ = True
except ImportError:
    _HAS_RAPIDFUZZ = False


# ---------------------------------------------------------------------------
# Staging column names
# ---------------------------------------------------------------------------
_BILLING_PROVIDER_NPI = "RenderingProviderNpi"
_BILLING_PROVIDER_NAME = "RenderingProviderFullName"
_BILLING_AMOUNT = "ChargeAmountOriginal"

_PAYROLL_EMPLOYEE_NPI = "EmployeeNpi"
_PAYROLL_EMPLOYEE_NAME = "EmployeeFullName"

# Threshold for NPI "present" — > 10% non-null
_NPI_PRESENCE_THRESHOLD = 0.10
# Fuzzy score thresholds
_NPI_NAME_FUZZY_THRESHOLD = 85
_NAME_ONLY_FUZZY_THRESHOLD = 80
# Top N providers to report by charge volume
_TOP_PROVIDERS_N = 20


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


def _fuzzy_match_candidates(
    unmatched: list[str],
    reference: list[str],
    threshold: int,
    max_candidates: int = 50,
) -> list[dict]:
    """Find fuzzy name match candidates for unmatched values against reference set."""
    if not _HAS_RAPIDFUZZ or not unmatched or not reference:
        return []
    candidates = []
    for val in unmatched[:max_candidates]:
        best_score = 0
        best_ref = None
        for ref_val in reference:
            score = _fuzz.token_sort_ratio(val, ref_val)
            if score > best_score:
                best_score = score
                best_ref = ref_val
        if best_score >= threshold and best_ref:
            candidates.append({
                "billing_name": val,
                "payroll_name_candidate": best_ref,
                "score": best_score,
            })
    return candidates


def _check_npi_presence(
    payroll_df: pd.DataFrame,
    payroll_maps: list[dict],
    cross_source_prep: dict | None,
) -> tuple[bool, str | None]:
    """
    Returns (has_npi, raw_npi_col).
    has_npi is True if the column exists and > 10% non-null.
    """
    # Check Phase 3 cross_source_prep first
    if cross_source_prep:
        npi_pop = cross_source_prep.get("provider_npi_population_pct")
        npi_col_raw = cross_source_prep.get("employee_npi_column")
        if npi_pop is not None:
            return (float(npi_pop) > 10.0), npi_col_raw

    # Fall back to resolving from column_mappings
    raw_npi = resolve_column(payroll_maps, _PAYROLL_EMPLOYEE_NPI)
    if not raw_npi or raw_npi not in payroll_df.columns:
        return False, None

    non_null = payroll_df[raw_npi].replace("", pd.NA).notna().sum()
    total = len(payroll_df)
    if total == 0:
        return False, None

    pct_non_null = non_null / total
    return pct_non_null > _NPI_PRESENCE_THRESHOLD, raw_npi


def _get_top_providers_by_charge(
    billing_df: pd.DataFrame,
    billing_maps: list[dict],
    npi_col: str | None,
    name_col: str | None,
    matched_npis: set[str],
    matched_names: set[str],
    match_method: str,
) -> list[dict]:
    """Build top N providers by charge volume with match status."""
    amt_col = resolve_column(billing_maps, _BILLING_AMOUNT)
    if not amt_col or amt_col not in billing_df.columns:
        return []

    work = billing_df.copy()
    work["_amt"] = work[amt_col].apply(_to_float).clip(lower=0)

    if npi_col and npi_col in billing_df.columns:
        work["_npi"] = billing_df[npi_col].astype(str).str.strip()
    else:
        work["_npi"] = ""

    if name_col and name_col in billing_df.columns:
        work["_name"] = billing_df[name_col].astype(str).str.strip()
    else:
        work["_name"] = ""

    if match_method == "npi" and npi_col:
        group_col = "_npi"
        agg = work.groupby(group_col).agg(
            charge_amount=("_amt", "sum"),
            name=("_name", lambda x: x.mode()[0] if len(x) > 0 else ""),
        ).reset_index()
        agg.columns = ["npi", "charge_amount", "name"]
        agg["matched"] = agg["npi"].isin(matched_npis)
    else:
        group_col = "_name"
        agg = work.groupby(group_col).agg(
            charge_amount=("_amt", "sum"),
            npi=("_npi", lambda x: x.mode()[0] if len(x) > 0 else ""),
        ).reset_index()
        agg.columns = ["name", "charge_amount", "npi"]
        agg["matched"] = agg["name"].str.lower().isin(matched_names)

    top = agg.sort_values("charge_amount", ascending=False).head(_TOP_PROVIDERS_N)
    return [
        {
            "npi": row.get("npi", ""),
            "name": row.get("name", ""),
            "charge_amount": round(float(row["charge_amount"]), 2),
            "matched": bool(row["matched"]),
        }
        for _, row in top.iterrows()
    ]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_checks(
    file_entries: dict[str, dict],
    cross_source_prep_by_file: dict[str, dict],
) -> dict:
    """
    Run C2 check: Billing <-> Payroll provider name/NPI matching.

    Parameters
    ----------
    file_entries : dict[filename → {df, source, column_mappings, ...}]
    cross_source_prep_by_file : dict[filename → cross_source_prep dict from Phase 3]

    Returns
    -------
    Finding dict with check="C2".
    """
    # Locate billing and payroll DataFrames
    billing_entry = None
    billing_source = None
    payroll_entry = None
    payroll_fname = None

    for fname, entry in file_entries.items():
        source = entry.get("source", "")
        if source in ("billing_combined", "billing_charges") and billing_entry is None:
            if entry.get("df") is not None:
                billing_entry = entry
                billing_source = source
        elif source == "payroll" and entry.get("df") is not None:
            payroll_entry = entry
            payroll_fname = fname

    if billing_entry is None:
        return {
            "check": "C2",
            "severity": "INFO",
            "message": "Skipped — no billing file present",
            "skipped": True,
        }
    if payroll_entry is None:
        return {
            "check": "C2",
            "severity": "INFO",
            "message": "Skipped — payroll file not present",
            "skipped": True,
        }

    billing_df = billing_entry["df"]
    billing_maps = billing_entry.get("column_mappings", [])
    payroll_df = payroll_entry["df"]
    payroll_maps = payroll_entry.get("column_mappings", [])
    payroll_prep = cross_source_prep_by_file.get(payroll_fname, {})

    # For combined billing, use only charge rows
    if billing_source == "billing_combined":
        from phase4.transactions_charges import _build_charge_mask
        charge_mask = _build_charge_mask(billing_df, billing_maps)
        if charge_mask.any():
            billing_df = billing_df[charge_mask].reset_index(drop=True)

    # Resolve column names
    bill_npi_col = resolve_column(billing_maps, _BILLING_PROVIDER_NPI)
    bill_name_col = resolve_column(billing_maps, _BILLING_PROVIDER_NAME)
    pay_name_col = resolve_column(payroll_maps, _PAYROLL_EMPLOYEE_NAME)

    # Determine if payroll has NPI
    has_npi, pay_npi_col = _check_npi_presence(payroll_df, payroll_maps, payroll_prep)

    # Get distinct billing NPIs / names
    billing_npis: list[str] = []
    billing_names: list[str] = []

    if bill_npi_col and bill_npi_col in billing_df.columns:
        billing_npis = (
            billing_df[bill_npi_col]
            .dropna().astype(str).str.strip()
            .replace("", pd.NA).dropna()
            .unique().tolist()
        )
    if bill_name_col and bill_name_col in billing_df.columns:
        billing_names = (
            billing_df[bill_name_col]
            .dropna().astype(str).str.strip()
            .replace("", pd.NA).dropna()
            .str.lower().unique().tolist()
        )

    # Get distinct payroll NPIs / names
    payroll_npis: set[str] = set()
    payroll_names: list[str] = []

    if has_npi and pay_npi_col and pay_npi_col in payroll_df.columns:
        payroll_npis = set(
            payroll_df[pay_npi_col]
            .dropna().astype(str).str.strip()
            .replace("", pd.NA).dropna()
            .unique().tolist()
        )
    if pay_name_col and pay_name_col in payroll_df.columns:
        payroll_names = (
            payroll_df[pay_name_col]
            .dropna().astype(str).str.strip()
            .replace("", pd.NA).dropna()
            .str.lower().unique().tolist()
        )

    billing_provider_distinct = len(billing_npis) if has_npi else len(billing_names)
    payroll_provider_distinct = len(payroll_npis) if has_npi else len(payroll_names)

    matched_npis: set[str] = set()
    matched_names: set[str] = set()

    if has_npi and billing_npis and payroll_npis:
        # NPI matching mode
        match_method = "npi"

        billing_npi_set = set(billing_npis)
        matched_npis = billing_npi_set & payroll_npis
        unmatched_npis = billing_npi_set - payroll_npis
        exact_match_count = len(matched_npis)
        exact_match_pct = exact_match_count / len(billing_npi_set) * 100 if billing_npi_set else 0.0

        # Name-match candidates for unmatched NPIs
        billing_names_norm = {
            npi: _normalize(name)
            for npi, name in zip(
                billing_df[bill_npi_col].astype(str).str.strip() if bill_npi_col else [],
                billing_df[bill_name_col].astype(str).str.strip() if bill_name_col else [],
            )
            if npi in unmatched_npis
        }
        unmatched_billing_names = list(set(billing_names_norm.values()))
        name_match_candidates = _fuzzy_match_candidates(
            unmatched_billing_names, payroll_names, _NPI_NAME_FUZZY_THRESHOLD
        )

        unmatched_no_candidate = len(unmatched_npis) - len(
            {c["billing_name"] for c in name_match_candidates}
        )
        unmatched_pct = len(unmatched_npis) / len(billing_npi_set) * 100 if billing_npi_set else 0.0

        if unmatched_pct > 30 and not name_match_candidates:
            severity = "HIGH"
        elif unmatched_pct > 10:
            severity = "MEDIUM"
        elif name_match_candidates:
            severity = "INFO"
        else:
            severity = "PASS"

        msg = (
            f"Billing <-> Payroll NPI match: {exact_match_pct:.1f}% "
            f"({exact_match_count}/{len(billing_npi_set)} distinct billing NPIs found in payroll)"
        )
        unmatched_sample = list(unmatched_npis)[:20]

    else:
        # Name-only matching mode
        match_method = "name_only"
        payroll_name_set = set(payroll_names)

        exact_match_count = sum(1 for n in billing_names if n in payroll_name_set)
        matched_names = {n for n in billing_names if n in payroll_name_set}
        unmatched_billing_names = [n for n in billing_names if n not in payroll_name_set]
        exact_match_pct = exact_match_count / len(billing_names) * 100 if billing_names else 0.0

        name_match_candidates = _fuzzy_match_candidates(
            unmatched_billing_names, payroll_names, _NAME_ONLY_FUZZY_THRESHOLD
        )
        unmatched_pct = len(unmatched_billing_names) / len(billing_names) * 100 if billing_names else 0.0

        if unmatched_pct > 50 and not name_match_candidates:
            severity = "HIGH"
        elif unmatched_billing_names:
            severity = "MEDIUM"
        else:
            severity = "PASS"

        msg = (
            f"Billing <-> Payroll name-only match: {exact_match_pct:.1f}% "
            f"({exact_match_count}/{len(billing_names)} distinct billing provider names found in payroll)"
        )
        unmatched_sample = unmatched_billing_names[:20]

    # Top providers by charge volume
    top_providers = _get_top_providers_by_charge(
        billing_df, billing_maps,
        bill_npi_col if has_npi else None,
        bill_name_col,
        matched_npis,
        matched_names,
        match_method,
    )

    return {
        "check": "C2",
        "severity": severity,
        "message": msg,
        "files_compared": f"{billing_source} + payroll",
        "skipped": False,
        "match_method": match_method,
        "billing_provider_distinct": billing_provider_distinct,
        "payroll_provider_distinct": payroll_provider_distinct,
        "exact_match_count": exact_match_count,
        "exact_match_pct": round(exact_match_pct, 2),
        "name_match_candidates": name_match_candidates,
        "top_providers": top_providers,
        "unmatched_sample": unmatched_sample,
    }
