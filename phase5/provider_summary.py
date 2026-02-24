"""
phase5/provider_summary.py

Builds a cross-source Provider Summary grid for the Phase 5 Excel report.

One row per unique Provider NPI found across Billing, Scheduling, Payroll,
and Quality.  Only valid 10-digit NPIs are included.

Output columns:
  ProviderNPI, ProviderName,
  WorkRvu, Charges, Payment, Adjustments,
  CompletedAppointments,
  PayrollHours, PayrollAmount,
  QualityRecordCount
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from shared.column_utils import resolve_column
from phase3.payroll import _classify_job

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PROVIDER_JOB_CLASSES = {"Physician", "APP"}

_NPI_RE = __import__("re").compile(r"^\d{10}$")

_OUTPUT_COLUMNS = [
    "ProviderNPI",
    "ProviderName",
    "WorkRvu",
    "Charges",
    "Payment",
    "Adjustments",
    "CompletedAppointments",
    "PayrollHours",
    "PayrollAmount",
    "QualityRecordCount",
]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build(file_entries: dict) -> list[dict]:
    """
    Build a provider summary grid from loaded file entries.

    Parameters
    ----------
    file_entries : dict
        Keyed by filename.  Each value must have at minimum:
        ``{"df": pd.DataFrame | None, "source": str, "column_mappings": list}``

    Returns
    -------
    list[dict]
        One dict per provider (NPI) row, keyed by _OUTPUT_COLUMNS.
    """
    billing_entries    = _entries_by_group(file_entries, {"billing_combined", "billing_charges", "billing_transactions"})
    scheduling_entries = _entries_by_group(file_entries, {"scheduling"})
    payroll_entries    = _entries_by_group(file_entries, {"payroll"})
    quality_entries    = _entries_by_group(file_entries, {"quality"})

    # ---- Per-source aggregations ----------------------------------------
    billing_prov  = _process_billing(billing_entries)
    # {npi: {name, WorkRvu, Charges, Payment, Adjustments}}

    sched_prov    = _process_scheduling(scheduling_entries)
    # {npi: {name, CompletedAppointments}}

    payroll_prov  = _process_payroll(payroll_entries)
    # {npi: {name, PayrollHours, PayrollAmount}}

    quality_prov  = _process_quality(quality_entries)
    # {npi: {name, QualityRecordCount}}

    # ---- Master NPI set -------------------------------------------------
    all_npis: dict[str, str] = {}  # npi -> provider_name

    for source_dict in (billing_prov, sched_prov, payroll_prov, quality_prov):
        for npi, data in source_dict.items():
            if npi not in all_npis:
                all_npis[npi] = data.get("name", "")
            elif not all_npis[npi] and data.get("name"):
                all_npis[npi] = data["name"]

    # ---- Assemble rows --------------------------------------------------
    rows: list[dict] = []

    for npi in sorted(all_npis.keys()):
        name = all_npis[npi]
        b    = billing_prov.get(npi, {})
        s    = sched_prov.get(npi, {})
        p    = payroll_prov.get(npi, {})
        q    = quality_prov.get(npi, {})
        rows.append({
            "ProviderNPI":           npi,
            "ProviderName":          name,
            "WorkRvu":               b.get("WorkRvu", 0) or 0,
            "Charges":               b.get("Charges", 0) or 0,
            "Payment":               b.get("Payment", 0) or 0,
            "Adjustments":           b.get("Adjustments", 0) or 0,
            "CompletedAppointments": s.get("CompletedAppointments", 0) or 0,
            "PayrollHours":          p.get("PayrollHours", 0) or 0,
            "PayrollAmount":         p.get("PayrollAmount", 0) or 0,
            "QualityRecordCount":    q.get("QualityRecordCount", 0) or 0,
        })

    return rows


# ---------------------------------------------------------------------------
# Source processors
# ---------------------------------------------------------------------------

def _process_billing(entries: list[dict]) -> dict[str, dict]:
    """
    Returns {npi: {name, WorkRvu, Charges, Payment, Adjustments}}.
    Uses staging_meta charge mask for billing_combined; all rows for charges/transactions.
    """
    from shared import staging_meta

    result: dict[str, dict] = {}
    charge_codes, charge_descs = staging_meta.get_charge_type_sets()

    for entry in entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])
        source   = entry.get("source", "")

        npi_col  = resolve_column(col_maps, "RenderingProviderNpi")
        name_col = resolve_column(col_maps, "RenderingProviderFullName")
        wrvu_col = resolve_column(col_maps, "WorkRvuOriginal")
        chg_col  = resolve_column(col_maps, "ChargeAmountOriginal")
        pay_col  = resolve_column(col_maps, "PaymentOriginal")
        adj_col  = resolve_column(col_maps, "AdjustmentOriginal")
        tt_col   = resolve_column(col_maps, "TransactionType")
        ttd_col  = resolve_column(col_maps, "TransactionTypeDesc")

        if not npi_col or npi_col not in df.columns:
            continue

        # Charge / transaction split
        if source == "billing_combined":
            from phase5.cost_center_summary import _build_charge_mask
            charge_mask = _build_charge_mask(df, tt_col, ttd_col, charge_codes, charge_descs)
            txn_mask    = ~charge_mask
        elif source == "billing_charges":
            charge_mask = pd.Series(True,  index=df.index)
            txn_mask    = pd.Series(False, index=df.index)
        else:  # billing_transactions
            charge_mask = pd.Series(False, index=df.index)
            txn_mask    = pd.Series(True,  index=df.index)

        npi_series = df[npi_col].fillna("").astype(str).str.strip()

        for npi, grp in df.groupby(npi_series):
            if not _NPI_RE.match(npi):
                continue

            grp_charge = grp[charge_mask.reindex(grp.index, fill_value=False)]
            grp_txn    = grp[txn_mask.reindex(grp.index, fill_value=False)]

            if npi not in result:
                result[npi] = {
                    "name":        "",
                    "WorkRvu":     0.0,
                    "Charges":     0.0,
                    "Payment":     0.0,
                    "Adjustments": 0.0,
                }
            r = result[npi]

            if not r["name"] and name_col and name_col in df.columns:
                names = grp[name_col].dropna().astype(str).str.strip()
                first = names[names != ""].head(1)
                if not first.empty:
                    r["name"] = first.iloc[0]

            if wrvu_col and wrvu_col in df.columns:
                r["WorkRvu"]  += _safe_sum(grp_charge, wrvu_col)
            if chg_col and chg_col in df.columns:
                r["Charges"]  += _safe_sum(grp_charge, chg_col)
            if pay_col and pay_col in df.columns:
                r["Payment"]  += _safe_sum(grp_txn, pay_col)
            if adj_col and adj_col in df.columns:
                r["Adjustments"] += _safe_sum(grp_txn, adj_col)

    return result


def _process_scheduling(entries: list[dict]) -> dict[str, dict]:
    """
    Returns {npi: {name, CompletedAppointments}}.
    Only rows where ApptStatus == 'Completed' are counted.
    """
    result: dict[str, dict] = {}

    for entry in entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])

        npi_col    = resolve_column(col_maps, "ApptProvNPI")
        name_col   = resolve_column(col_maps, "ApptProvFullName")
        status_col = resolve_column(col_maps, "ApptStatus")

        if not npi_col or npi_col not in df.columns:
            continue

        if status_col and status_col in df.columns:
            mask = df[status_col].astype(str).str.strip().str.lower() == "completed"
            grp_df = df[mask]
        else:
            grp_df = df

        npi_series = grp_df[npi_col].fillna("").astype(str).str.strip()

        for npi, grp in grp_df.groupby(npi_series):
            if not _NPI_RE.match(npi):
                continue

            if npi not in result:
                result[npi] = {"name": "", "CompletedAppointments": 0}
            r = result[npi]

            if not r["name"] and name_col and name_col in grp_df.columns:
                names = grp[name_col].dropna().astype(str).str.strip()
                first = names[names != ""].head(1)
                if not first.empty:
                    r["name"] = first.iloc[0]

            r["CompletedAppointments"] += len(grp)

    return result


def _process_payroll(entries: list[dict]) -> dict[str, dict]:
    """
    Returns {npi: {name, PayrollHours, PayrollAmount}}.
    Only includes rows where JobCodeDesc classifies as a provider job.
    """
    result: dict[str, dict] = {}

    for entry in entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])

        npi_col  = resolve_column(col_maps, "EmployeeNpi")
        name_col = resolve_column(col_maps, "EmployeeFullName")
        job_col  = resolve_column(col_maps, "JobCodeDesc")
        hrs_col  = resolve_column(col_maps, "Hours")
        amt_col  = resolve_column(col_maps, "AmountOrig")

        if not npi_col or npi_col not in df.columns:
            continue

        npi_series = df[npi_col].fillna("").astype(str).str.strip()

        for npi, grp in df.groupby(npi_series):
            if not _NPI_RE.match(npi):
                continue

            # Filter to provider rows only
            if job_col and job_col in df.columns:
                provider_mask = grp[job_col].fillna("").astype(str).apply(
                    lambda d: _classify_job(d) in _PROVIDER_JOB_CLASSES
                )
                grp = grp[provider_mask]
                if grp.empty:
                    continue
            # If no job column, include all rows for the NPI

            if npi not in result:
                result[npi] = {"name": "", "PayrollHours": 0.0, "PayrollAmount": 0.0}
            r = result[npi]

            if not r["name"] and name_col and name_col in df.columns:
                names = grp[name_col].dropna().astype(str).str.strip()
                first = names[names != ""].head(1)
                if not first.empty:
                    r["name"] = first.iloc[0]

            if hrs_col and hrs_col in df.columns:
                r["PayrollHours"]  += _safe_sum(grp, hrs_col)
            if amt_col and amt_col in df.columns:
                r["PayrollAmount"] += _safe_sum(grp, amt_col)

    return result


def _process_quality(entries: list[dict]) -> dict[str, dict]:
    """
    Returns {npi: {name, QualityRecordCount}}.
    """
    result: dict[str, dict] = {}

    for entry in entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])

        npi_col  = _find_quality_npi_col(df, col_maps)
        name_col = _find_quality_name_col(df, col_maps)

        if not npi_col:
            continue

        npi_series = df[npi_col].fillna("").astype(str).str.strip()

        for npi, grp in df.groupby(npi_series):
            if not _NPI_RE.match(npi):
                continue

            if npi not in result:
                result[npi] = {"name": "", "QualityRecordCount": 0}
            r = result[npi]

            if not r["name"] and name_col and name_col in df.columns:
                names = grp[name_col].dropna().astype(str).str.strip()
                first = names[names != ""].head(1)
                if not first.empty:
                    r["name"] = first.iloc[0]

            r["QualityRecordCount"] += len(grp)

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _entries_by_group(file_entries: dict, source_names: set) -> list[dict]:
    return [e for e in file_entries.values() if e.get("source") in source_names]


def _find_quality_npi_col(df: pd.DataFrame, col_maps: list) -> str | None:
    """Find the provider NPI column in a quality file via column mappings."""
    for mapping in col_maps:
        staging = str(mapping.get("staging_column", "") or "").lower()
        if "npi" in staging and "prov" in staging:
            raw = mapping.get("raw_column", "")
            if raw and raw in df.columns:
                return raw
    # Fallback: scan raw column names
    for col in df.columns:
        if "npi" in col.lower() and "prov" in col.lower():
            return col
    for col in df.columns:
        if "npi" in col.lower():
            return col
    return None


def _find_quality_name_col(df: pd.DataFrame, col_maps: list) -> str | None:
    """Find the provider name column in a quality file."""
    for mapping in col_maps:
        staging = str(mapping.get("staging_column", "") or "").lower()
        if ("name" in staging or "fullname" in staging) and "prov" in staging:
            raw = mapping.get("raw_column", "")
            if raw and raw in df.columns:
                return raw
    for col in df.columns:
        if "prov" in col.lower() and "name" in col.lower():
            return col
    return None


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns or df.empty:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
