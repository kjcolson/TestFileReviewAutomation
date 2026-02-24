"""
phase5/cost_center_summary.py

Builds a cross-source Cost Center Summary grid for the Phase 5 Excel report.

One row per unique cost center / department ID found across Billing, Scheduling,
Payroll, and GL.  Quality record counts are linked via NPI -> billing dept ID.

Output columns:
  CostCenterNumber, CostCenterName,
  WorkRvu, Charges, Payment, Adjustments,
  CompletedAppointments,
  ProviderHours, ProviderAmount, SupportStaffHours, SupportStaffAmount,
  GL_ProviderPayAmount, GL_SupportStaffAmount,
  GL_MedicalPracticeCharges, GL_MedicalPracticeAdjustments,
  QualityRecordCount
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from shared.column_utils import resolve_column
from shared import staging_meta

# Import GL classification from phase3 (read-only — no side effects)
from phase3.gl import _CATEGORY_PATTERNS, _classify_account
from phase3.payroll import _classify_job

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_PROVIDER_JOB_CLASSES = {"Physician", "APP"}
_SUPPORT_JOB_CLASSES  = {"RN", "LPN", "MA/CNA", "Other Clinical"}

_OUTPUT_COLUMNS = [
    "CostCenterNumber",
    "CostCenterName",
    "WorkRvu",
    "Charges",
    "Payment",
    "Adjustments",
    "CompletedAppointments",
    "ProviderHours",
    "ProviderAmount",
    "SupportStaffHours",
    "SupportStaffAmount",
    "GL_ProviderPayAmount",
    "GL_SupportStaffAmount",
    "GL_MedicalPracticeCharges",
    "GL_MedicalPracticeAdjustments",
    "QualityRecordCount",
]

_FUZZY_MATCH_THRESHOLD = 80  # rapidfuzz score cutoff


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def build(file_entries: dict) -> list[dict]:
    """
    Build a cost center summary grid from loaded file entries.

    Parameters
    ----------
    file_entries : dict
        Keyed by filename.  Each value must have at minimum:
        ``{"df": pd.DataFrame | None, "source": str, "column_mappings": list}``

    Returns
    -------
    list[dict]
        One dict per cost center row, keyed by _OUTPUT_COLUMNS.
    """
    # Separate entries by source group
    billing_entries  = _entries_by_group(file_entries, {"billing_combined", "billing_charges", "billing_transactions"})
    scheduling_entries = _entries_by_group(file_entries, {"scheduling"})
    payroll_entries  = _entries_by_group(file_entries, {"payroll"})
    gl_entries       = _entries_by_group(file_entries, {"gl"})
    quality_entries  = _entries_by_group(file_entries, {"quality"})

    # ---- Build per-source aggregations ---------------------------------
    billing_cc  = _process_billing(billing_entries)     # {cc_id: {cc_name, WorkRvu, Charges, Payment, Adjustments, npis: set}}
    payroll_cc  = _process_payroll(payroll_entries)     # {cc_id: {cc_name, ProviderHours, ProviderAmount, SupportStaffHours, SupportStaffAmount}}
    gl_cc       = _process_gl(gl_entries)               # {cc_id: {cc_name, GL_ProviderPayAmount, GL_SupportStaffAmount, GL_MedicalPracticeCharges, GL_MedicalPracticeAdjustments}}

    # ---- Build master CC ID set ----------------------------------------
    # Union of billing dept IDs, payroll dept IDs, and GL cost center numbers
    all_cc_ids: dict[str, str] = {}  # cc_id -> display_name

    for cc_id, data in gl_cc.items():
        if cc_id not in all_cc_ids:
            all_cc_ids[cc_id] = data.get("cc_name", "")

    for cc_id, data in billing_cc.items():
        if cc_id not in all_cc_ids:
            all_cc_ids[cc_id] = data.get("cc_name", "")
        elif not all_cc_ids[cc_id] and data.get("cc_name"):
            all_cc_ids[cc_id] = data["cc_name"]

    for cc_id, data in payroll_cc.items():
        if cc_id not in all_cc_ids:
            all_cc_ids[cc_id] = data.get("cc_name", "")
        elif not all_cc_ids[cc_id] and data.get("cc_name"):
            all_cc_ids[cc_id] = data["cc_name"]

    # ---- Scheduling: fuzzy-match location names to GL CC names ---------
    sch_matched, sch_unmatched = _process_scheduling(scheduling_entries, gl_cc)
    # sch_matched:  {gl_cc_id: completed_appt_count}
    # sch_unmatched: {loc_name: completed_appt_count}

    # ---- Quality → CC linkage via NPI -> billing dept ID ---------------
    quality_cc = _process_quality(quality_entries, billing_cc)  # {cc_id: count}

    # ---- Assemble rows -------------------------------------------------
    rows: list[dict] = []

    for cc_id, cc_name in sorted(all_cc_ids.items(), key=lambda x: str(x[0])):
        b = billing_cc.get(cc_id, {})
        p = payroll_cc.get(cc_id, {})
        g = gl_cc.get(cc_id, {})
        rows.append({
            "CostCenterNumber":              cc_id,
            "CostCenterName":                cc_name or b.get("cc_name", "") or p.get("cc_name", "") or "",
            "WorkRvu":                       b.get("WorkRvu", 0) or 0,
            "Charges":                       b.get("Charges", 0) or 0,
            "Payment":                       b.get("Payment", 0) or 0,
            "Adjustments":                   b.get("Adjustments", 0) or 0,
            "CompletedAppointments":         sch_matched.get(cc_id, 0) or 0,
            "ProviderHours":                 p.get("ProviderHours", 0) or 0,
            "ProviderAmount":                p.get("ProviderAmount", 0) or 0,
            "SupportStaffHours":             p.get("SupportStaffHours", 0) or 0,
            "SupportStaffAmount":            p.get("SupportStaffAmount", 0) or 0,
            "GL_ProviderPayAmount":          g.get("GL_ProviderPayAmount", 0) or 0,
            "GL_SupportStaffAmount":         g.get("GL_SupportStaffAmount", 0) or 0,
            "GL_MedicalPracticeCharges":     g.get("GL_MedicalPracticeCharges", 0) or 0,
            "GL_MedicalPracticeAdjustments": g.get("GL_MedicalPracticeAdjustments", 0) or 0,
            "QualityRecordCount":            quality_cc.get(cc_id, 0) or 0,
        })

    # Append unmatched scheduling locations as separate rows
    for loc_name, appt_count in sorted(sch_unmatched.items()):
        rows.append({
            "CostCenterNumber":              f"SCH:{loc_name}",
            "CostCenterName":                loc_name,
            "WorkRvu":                       0,
            "Charges":                       0,
            "Payment":                       0,
            "Adjustments":                   0,
            "CompletedAppointments":         appt_count,
            "ProviderHours":                 0,
            "ProviderAmount":                0,
            "SupportStaffHours":             0,
            "SupportStaffAmount":            0,
            "GL_ProviderPayAmount":          0,
            "GL_SupportStaffAmount":         0,
            "GL_MedicalPracticeCharges":     0,
            "GL_MedicalPracticeAdjustments": 0,
            "QualityRecordCount":            0,
        })

    return rows


# ---------------------------------------------------------------------------
# Source processors
# ---------------------------------------------------------------------------

def _process_billing(entries: list[dict]) -> dict[str, dict]:
    """
    Returns {dept_id: {cc_name, WorkRvu, Charges, Payment, Adjustments, npi_set}}.
    billing_combined  → split by charge mask
    billing_charges   → all rows are charges
    billing_transactions → all rows are transactions
    """
    result: dict[str, dict] = {}
    charge_codes, charge_descs = staging_meta.get_charge_type_sets()

    for entry in entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])
        source   = entry.get("source", "")

        dept_col  = resolve_column(col_maps, "BillDepartmentId")
        dname_col = resolve_column(col_maps, "BillDepartmentName")
        wrvu_col  = resolve_column(col_maps, "WorkRvuOriginal")
        chg_col   = resolve_column(col_maps, "ChargeAmountOriginal")
        pay_col   = resolve_column(col_maps, "PaymentOriginal")
        adj_col   = resolve_column(col_maps, "AdjustmentOriginal")
        npi_col   = resolve_column(col_maps, "RenderingProviderNpi")
        tt_col    = resolve_column(col_maps, "TransactionType")
        ttd_col   = resolve_column(col_maps, "TransactionTypeDesc")

        if not dept_col or dept_col not in df.columns:
            continue

        # Determine charge/transaction masks
        if source == "billing_combined":
            charge_mask = _build_charge_mask(df, tt_col, ttd_col, charge_codes, charge_descs)
            txn_mask    = ~charge_mask
        elif source == "billing_charges":
            charge_mask = pd.Series(True,  index=df.index)
            txn_mask    = pd.Series(False, index=df.index)
        else:  # billing_transactions
            charge_mask = pd.Series(False, index=df.index)
            txn_mask    = pd.Series(True,  index=df.index)

        dept_series = df[dept_col].fillna("").astype(str).str.strip()

        # Per-dept aggregation
        for dept_id, grp in df.groupby(dept_series):
            if not dept_id:
                continue
            grp_charge = grp[charge_mask.reindex(grp.index, fill_value=False)]
            grp_txn    = grp[txn_mask.reindex(grp.index, fill_value=False)]

            if dept_id not in result:
                result[dept_id] = {
                    "cc_name":     "",
                    "WorkRvu":     0.0,
                    "Charges":     0.0,
                    "Payment":     0.0,
                    "Adjustments": 0.0,
                    "npi_set":     set(),
                }
            r = result[dept_id]

            # Name from first non-empty value
            if not r["cc_name"] and dname_col and dname_col in df.columns:
                names = grp[dname_col].dropna().astype(str).str.strip()
                first_name = names[names != ""].head(1)
                if not first_name.empty:
                    r["cc_name"] = first_name.iloc[0]

            # Charge rows
            if wrvu_col and wrvu_col in df.columns:
                r["WorkRvu"] += _safe_sum(grp_charge, wrvu_col)
            if chg_col and chg_col in df.columns:
                r["Charges"] += _safe_sum(grp_charge, chg_col)

            # Transaction rows
            if pay_col and pay_col in df.columns:
                r["Payment"] += _safe_sum(grp_txn, pay_col)
            if adj_col and adj_col in df.columns:
                r["Adjustments"] += _safe_sum(grp_txn, adj_col)

            # NPIs for quality linkage
            if npi_col and npi_col in grp_charge.columns:
                valid_npis = grp_charge[npi_col].dropna().astype(str).str.strip()
                valid_npis = valid_npis[valid_npis.str.match(r"^\d{10}$")]
                r["npi_set"].update(valid_npis.tolist())

    return result


def _process_scheduling(entries: list[dict], gl_cc: dict) -> tuple[dict, dict]:
    """
    Fuzzy-match scheduling location names to GL cost center names.

    Returns
    -------
    matched   : {gl_cc_id: completed_appt_count}
    unmatched : {loc_name: completed_appt_count}
    """
    # Build count of completed appointments per location name
    loc_counts: dict[str, int] = {}

    for entry in entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])

        loc_col    = resolve_column(col_maps, "BillLocNameOrig")
        status_col = resolve_column(col_maps, "ApptStatus")

        if not loc_col or loc_col not in df.columns:
            continue

        if status_col and status_col in df.columns:
            mask = df[status_col].astype(str).str.strip().str.lower() == "completed"
            grp_df = df[mask]
        else:
            grp_df = df  # no status col — count all

        loc_series = grp_df[loc_col].fillna("").astype(str).str.strip()
        for loc_name, count in loc_series.value_counts().items():
            if loc_name:
                loc_counts[loc_name] = loc_counts.get(loc_name, 0) + int(count)

    if not loc_counts:
        return {}, {}

    # Build GL name → CC ID mapping
    gl_name_to_id: dict[str, str] = {}
    for cc_id, data in gl_cc.items():
        name = data.get("cc_name", "")
        if name:
            gl_name_to_id[name] = cc_id

    if not gl_name_to_id:
        # No GL names available — return all scheduling as unmatched
        return {}, dict(loc_counts)

    # Fuzzy match
    try:
        from rapidfuzz import process as rfprocess, utils as rfutils
        gl_names = list(gl_name_to_id.keys())
        matched:   dict[str, int] = {}
        unmatched: dict[str, int] = {}

        for loc_name, count in loc_counts.items():
            result = rfprocess.extractOne(
                loc_name,
                gl_names,
                score_cutoff=_FUZZY_MATCH_THRESHOLD,
                processor=rfutils.default_process,
            )
            if result:
                best_name = result[0]
                cc_id = gl_name_to_id[best_name]
                matched[cc_id] = matched.get(cc_id, 0) + count
            else:
                unmatched[loc_name] = unmatched.get(loc_name, 0) + count

        return matched, unmatched

    except ImportError:
        # rapidfuzz not available — treat all as unmatched
        return {}, dict(loc_counts)


def _process_payroll(entries: list[dict]) -> dict[str, dict]:
    """
    Returns {dept_id: {cc_name, ProviderHours, ProviderAmount, SupportStaffHours, SupportStaffAmount}}.
    """
    result: dict[str, dict] = {}

    for entry in entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])

        dept_col  = resolve_column(col_maps, "DepartmentId")
        dname_col = resolve_column(col_maps, "DepartmentName")
        job_col   = resolve_column(col_maps, "JobCodeDesc")
        hrs_col   = resolve_column(col_maps, "Hours")
        amt_col   = resolve_column(col_maps, "AmountOrig")

        if not dept_col or dept_col not in df.columns:
            continue

        dept_series = df[dept_col].fillna("").astype(str).str.strip()

        for dept_id, grp in df.groupby(dept_series):
            if not dept_id:
                continue

            if dept_id not in result:
                result[dept_id] = {
                    "cc_name":          "",
                    "ProviderHours":    0.0,
                    "ProviderAmount":   0.0,
                    "SupportStaffHours":  0.0,
                    "SupportStaffAmount": 0.0,
                }
            r = result[dept_id]

            if not r["cc_name"] and dname_col and dname_col in df.columns:
                names = grp[dname_col].dropna().astype(str).str.strip()
                first = names[names != ""].head(1)
                if not first.empty:
                    r["cc_name"] = first.iloc[0]

            if job_col and job_col in df.columns:
                for _, row in grp.iterrows():
                    job_class = _classify_job(str(row.get(job_col, "") or ""))
                    hrs = _safe_float(row.get(hrs_col) if hrs_col else None)
                    amt = _safe_float(row.get(amt_col) if amt_col else None)
                    if job_class in _PROVIDER_JOB_CLASSES:
                        r["ProviderHours"]  += hrs
                        r["ProviderAmount"] += amt
                    elif job_class in _SUPPORT_JOB_CLASSES:
                        r["SupportStaffHours"]  += hrs
                        r["SupportStaffAmount"] += amt
            else:
                # No job code column — aggregate all into unclassified (skip split)
                pass

    return result


def _process_gl(entries: list[dict]) -> dict[str, dict]:
    """
    Returns {cc_id: {cc_name, GL_ProviderPayAmount, GL_SupportStaffAmount,
                     GL_MedicalPracticeCharges, GL_MedicalPracticeAdjustments}}.
    """
    result: dict[str, dict] = {}

    for entry in entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])

        cc_col    = resolve_column(col_maps, "CostCenterNumberOrig")
        ccname_col = resolve_column(col_maps, "CostCenterNameOrig")
        desc_col  = resolve_column(col_maps, "AcctDesc")
        amt_col   = resolve_column(col_maps, "AmountOrig")
        acct_col  = resolve_column(col_maps, "AcctNumber")

        if not cc_col or cc_col not in df.columns:
            continue

        cc_series = df[cc_col].fillna("").astype(str).str.strip()

        for cc_id, grp in df.groupby(cc_series):
            if not cc_id:
                continue

            if cc_id not in result:
                result[cc_id] = {
                    "cc_name":                   "",
                    "GL_ProviderPayAmount":       0.0,
                    "GL_SupportStaffAmount":      0.0,
                    "GL_MedicalPracticeCharges":  0.0,
                    "GL_MedicalPracticeAdjustments": 0.0,
                }
            r = result[cc_id]

            if not r["cc_name"] and ccname_col and ccname_col in df.columns:
                names = grp[ccname_col].dropna().astype(str).str.strip()
                first = names[names != ""].head(1)
                if not first.empty:
                    r["cc_name"] = first.iloc[0]

            if not desc_col or desc_col not in df.columns:
                continue
            if not amt_col or amt_col not in df.columns:
                continue

            for _, row in grp.iterrows():
                desc     = str(row.get(desc_col, "") or "")
                acct_type = str(row.get(acct_col, "") or "") if acct_col else ""
                category, _ = _classify_account(desc, acct_type)
                amt = _safe_float(row.get(amt_col))
                if category == "Provider Compensation":
                    r["GL_ProviderPayAmount"] += amt
                elif category == "Support Staff Compensation":
                    r["GL_SupportStaffAmount"] += amt
                elif category == "Charges":
                    r["GL_MedicalPracticeCharges"] += amt
                elif category == "Adjustments":
                    r["GL_MedicalPracticeAdjustments"] += amt

    return result


def _process_quality(quality_entries: list[dict], billing_cc: dict) -> dict[str, int]:
    """
    Link quality records to cost centers via NPI -> billing dept ID.
    Returns {cc_id: record_count}.
    """
    # Build reverse map: npi -> cc_id  (from billing)
    npi_to_cc: dict[str, str] = {}
    for cc_id, data in billing_cc.items():
        for npi in data.get("npi_set", set()):
            if npi not in npi_to_cc:
                npi_to_cc[npi] = cc_id

    result: dict[str, int] = {}

    for entry in quality_entries:
        df = entry.get("df")
        if df is None or df.empty:
            continue
        col_maps = entry.get("column_mappings", [])

        npi_col = _find_quality_npi_col(df, col_maps)
        if not npi_col:
            continue

        npi_series = df[npi_col].fillna("").astype(str).str.strip()
        valid_mask = npi_series.str.match(r"^\d{10}$")
        for npi in npi_series[valid_mask]:
            cc_id = npi_to_cc.get(npi)
            if cc_id:
                result[cc_id] = result.get(cc_id, 0) + 1

    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _entries_by_group(file_entries: dict, source_names: set) -> list[dict]:
    """Return list of file_entry dicts whose source is in source_names."""
    return [e for e in file_entries.values() if e.get("source") in source_names]


def _build_charge_mask(
    df: pd.DataFrame,
    tt_col: str | None,
    ttd_col: str | None,
    charge_codes: set,
    charge_descs: set,
) -> pd.Series:
    """Build a boolean mask for charge rows in billing_combined."""
    mask = pd.Series(False, index=df.index)

    if tt_col and tt_col in df.columns and charge_codes:
        mask = mask | df[tt_col].astype(str).str.strip().isin(charge_codes)

    if ttd_col and ttd_col in df.columns and charge_descs:
        mask = mask | df[ttd_col].astype(str).str.strip().str.lower().isin(
            {d.lower() for d in charge_descs}
        )

    # Fallback: keyword detection if staging_meta not loaded
    if not mask.any():
        if tt_col and tt_col in df.columns:
            mask = mask | df[tt_col].astype(str).str.contains(r"chg|charge", case=False, na=False)
        if ttd_col and ttd_col in df.columns:
            mask = mask | df[ttd_col].astype(str).str.contains(r"chg|charge", case=False, na=False)

    return mask


def _find_quality_npi_col(df: pd.DataFrame, col_maps: list) -> str | None:
    """Find the NPI column in a quality file."""
    # Try staging column names that contain both 'npi' and 'prov'
    for mapping in col_maps:
        staging = str(mapping.get("staging_column", "") or "").lower()
        if "npi" in staging and "prov" in staging:
            raw = mapping.get("raw_column", "")
            if raw and raw in df.columns:
                return raw

    # Fallback: look for any column with 'npi' in the raw column name
    for col in df.columns:
        if "npi" in col.lower() and "prov" in col.lower():
            return col
    for col in df.columns:
        if "npi" in col.lower():
            return col

    return None


def _safe_sum(df: pd.DataFrame, col: str) -> float:
    """Sum a column, coercing to numeric first."""
    if col not in df.columns or df.empty:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())


def _safe_float(val: Any) -> float:
    """Convert a scalar value to float, returning 0.0 on failure."""
    try:
        return float(val) if val is not None and str(val).strip() not in ("", "nan", "None") else 0.0
    except (ValueError, TypeError):
        return 0.0
