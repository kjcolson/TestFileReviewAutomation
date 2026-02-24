"""
phase3/billing.py

Source-specific data quality checks for billing files.
Checks B1-B14.
"""

from __future__ import annotations

import re
from calendar import monthrange
from datetime import date

import pandas as pd
import numpy as np

from shared.column_utils import resolve_column
from shared import staging_meta

# Charge-conditional columns (same as universal.py)
_CHARGE_CONDITIONAL_COLS = {
    "CptCode", "Units", "WorkRvuOriginal", "PrimaryIcdCode", "SecondaryIcdCodes",
}

# CPT ranges by type
_EM_CPT_START, _EM_CPT_END = 99202, 99499
_PROC_CPT_START, _PROC_CPT_END = 10000, 69999
_LAB_CPT_START, _LAB_CPT_END = 80000, 89999
_ANC_CPT_START, _ANC_CPT_END = 90000, 99199

# Void/reversal keywords
_VOID_KEYWORDS = {"void", "reversal", "vd", "rev", "reverse", "reversed", "cancel", "cancelled"}

# Payer financial class buckets
_PAYER_BUCKETS = {
    "Commercial/Managed Care": re.compile(
        r"commercial|managed care|bcbs|blue cross|blue shield|aetna|uhc|united|cigna|"
        r"humana|anthem|hmo|ppo|epo|pos\b|kaiser|molina|centene|wellpoint|health net|"
        r"coventry|magellan|beacon", re.I
    ),
    "Medicare": re.compile(r"medicare|mcr\b|medicare advantage|ma\b", re.I),
    "Medicaid": re.compile(r"medicaid|mcd\b|chip\b|state program|medical assistance", re.I),
    "Self-Pay": re.compile(r"self.?pay|self\b|cash|uninsured|self pay|private pay", re.I),
    "Workers Comp": re.compile(r"work.?comp|workers.?comp|wc\b|occupational", re.I),
    "Other Government": re.compile(r"tricare|va\b|champva|champus|veteran|indian health", re.I),
    "Other": re.compile(r"charity|contractual|other", re.I),
}


def _build_charge_mask(df: pd.DataFrame, column_mappings: list[dict]) -> pd.Series | None:
    charge_codes, charge_descs = staging_meta.get_charge_type_sets()
    tt_col = resolve_column(column_mappings, "TransactionType")
    ttd_col = resolve_column(column_mappings, "TransactionTypeDesc")

    mask = pd.Series([False] * len(df), index=df.index)
    if tt_col and tt_col in df.columns and charge_codes:
        mask |= df[tt_col].astype(str).str.strip().isin(charge_codes)
    if ttd_col and ttd_col in df.columns and charge_descs:
        mask |= df[ttd_col].astype(str).str.strip().str.lower().isin(charge_descs)
    return mask if mask.any() else None


def _is_void_row(val: str) -> bool:
    return val.strip().lower() in _VOID_KEYWORDS


def b1_transaction_type_validation(
    df: pd.DataFrame, column_mappings: list[dict], source: str
) -> list[dict]:
    """B1: Transaction Type validation (Combined Billing)."""
    findings = []
    tt_col = resolve_column(column_mappings, "TransactionType")
    ttd_col = resolve_column(column_mappings, "TransactionTypeDesc")

    col = tt_col if (tt_col and tt_col in df.columns) else (
        ttd_col if (ttd_col and ttd_col in df.columns) else None
    )
    if not col:
        return findings

    charge_codes, charge_descs = staging_meta.get_charge_type_sets()
    trans_codes, trans_descs = staging_meta.get_transaction_type_sets()
    values = df[col].dropna().astype(str).str.strip()
    value_counts = values.value_counts()

    charge_kw = re.compile(r"charge|chg\b|^c$", re.I)
    payment_kw = re.compile(r"payment|pmt\b|pay\b|^p$", re.I)
    adj_kw = re.compile(r"adjust|adj\b|^a$", re.I)
    void_kw = re.compile(r"void|reversal|vd\b|rev\b", re.I)
    refund_kw = re.compile(r"refund|ref\b|^r$", re.I)

    has_charges = False
    has_transactions = False
    unrecognized = []

    for val, cnt in value_counts.items():
        v = str(val)
        if charge_kw.search(v) or v.lower() in charge_descs or v in charge_codes:
            has_charges = True
        elif (
            (trans_codes and v in trans_codes)
            or (trans_descs and v.lower() in trans_descs)
            or payment_kw.search(v) or adj_kw.search(v)
            or void_kw.search(v) or refund_kw.search(v)
        ):
            has_transactions = True
        else:
            unrecognized.append({"value": v, "count": int(cnt)})

    if source == "billing_combined":
        if not has_charges or not has_transactions:
            if not has_charges and not has_transactions:
                msg = "Combined billing file: cannot classify any Transaction Type values"
                sev = "CRITICAL"
            elif not has_charges:
                msg = "Combined billing: only transaction rows found — no charge rows (may be billing_transactions file)"
                sev = "CRITICAL"
            else:
                msg = "Combined billing: only charge rows found — no transaction rows (may be billing_charges file)"
                sev = "CRITICAL"
            findings.append({
                "check": "B1",
                "severity": sev,
                "message": msg,
                "distinct_values": value_counts.to_dict(),
            })

    if unrecognized:
        unrecog_count = sum(r["count"] for r in unrecognized)
        sev = "HIGH" if unrecog_count / len(df) > 0.05 else "MEDIUM"
        findings.append({
            "check": "B1",
            "severity": sev,
            "message": f"'{col}': {len(unrecognized)} unrecognizable Transaction Type value(s) "
                       f"({unrecog_count:,} rows)",
            "unrecognized_values": unrecognized[:20],
        })

    return findings


def b2_charge_transaction_linkage(
    billing_dfs: dict[str, dict],
) -> list[dict]:
    """B2: Charge-Transaction linkage for separate billing files."""
    findings = []

    charges_entry = next(
        (v for v in billing_dfs.values() if v.get("source") == "billing_charges"), None
    )
    txn_entry = next(
        (v for v in billing_dfs.values() if v.get("source") == "billing_transactions"), None
    )

    if not charges_entry or not txn_entry:
        return findings

    chg_df = charges_entry["df"]
    txn_df = txn_entry["df"]
    chg_mappings = charges_entry["column_mappings"]
    txn_mappings = txn_entry["column_mappings"]

    chg_id_col = resolve_column(chg_mappings, "ChargeId")
    txn_id_col = resolve_column(txn_mappings, "ChargeId")

    if not chg_id_col or chg_id_col not in chg_df.columns:
        chg_id_col = resolve_column(chg_mappings, "InvoiceNumber")
    if not txn_id_col or txn_id_col not in txn_df.columns:
        txn_id_col = resolve_column(txn_mappings, "InvoiceNumber")

    if not chg_id_col or not txn_id_col:
        findings.append({
            "check": "B2",
            "severity": "INFO",
            "message": "B2: Linkage check skipped — ChargeId/InvoiceNumber not mapped in one or both billing files",
        })
        return findings

    chg_ids = set(chg_df[chg_id_col].dropna().astype(str).str.strip())
    txn_ids = set(txn_df[txn_id_col].dropna().astype(str).str.strip())

    orphaned = txn_ids - chg_ids
    unmatched_charges = chg_ids - txn_ids
    orphan_pct = len(orphaned) / max(len(txn_ids), 1) * 100
    unmatched_chg_pct = len(unmatched_charges) / max(len(chg_ids), 1) * 100

    if orphan_pct > 20:
        sev = "HIGH"
    elif orphan_pct > 0:
        sev = "INFO"
    else:
        sev = "INFO"

    findings.append({
        "check": "B2",
        "severity": sev,
        "charge_id_count": len(chg_ids),
        "transaction_id_count": len(txn_ids),
        "orphaned_transaction_ids": len(orphaned),
        "orphaned_pct": round(orphan_pct, 2),
        "unmatched_charge_ids": len(unmatched_charges),
        "unmatched_charge_pct": round(unmatched_chg_pct, 2),
        "message": (
            f"B2 linkage: {len(orphaned):,} transaction IDs ({orphan_pct:.1f}%) not found in charges; "
            f"{len(unmatched_charges):,} charge IDs ({unmatched_chg_pct:.1f}%) have no transaction"
        ),
    })

    if unmatched_chg_pct > 50:
        findings.append({
            "check": "B2",
            "severity": "MEDIUM",
            "message": f"B2: {unmatched_chg_pct:.1f}% of charges have no matching transaction (unbilled or pending)",
        })

    return findings


def b3_wrvu_validation(df: pd.DataFrame, column_mappings: list[dict], source: str) -> list[dict]:
    """B3: Work RVU validation."""
    findings = []
    wrvu_col = resolve_column(column_mappings, "WorkRvuOriginal")
    cpt_col = resolve_column(column_mappings, "CptCode")

    if not wrvu_col or wrvu_col not in df.columns:
        return findings

    # Apply charge mask for billing_combined
    work_df = df
    if source == "billing_combined":
        mask = _build_charge_mask(df, column_mappings)
        if mask is not None:
            work_df = df[mask]

    if cpt_col and cpt_col in work_df.columns:
        cpt_numeric = pd.to_numeric(work_df[cpt_col].astype(str).str.replace(r"\D", "", regex=True), errors="coerce")
        em_proc_mask = (
            ((cpt_numeric >= _EM_CPT_START) & (cpt_numeric <= _EM_CPT_END)) |
            ((cpt_numeric >= _PROC_CPT_START) & (cpt_numeric <= _PROC_CPT_END))
        )
        em_proc_df = work_df[em_proc_mask]
    else:
        em_proc_df = work_df

    if len(em_proc_df) == 0:
        return findings

    wrvus = pd.to_numeric(em_proc_df[wrvu_col], errors="coerce")
    zero_null_count = int((wrvus.isna() | (wrvus == 0)).sum())
    neg_count = int((wrvus < 0).sum())
    extreme_count = int((wrvus > 100).sum())
    pct_zero_null = zero_null_count / len(em_proc_df) * 100

    if pct_zero_null > 10:
        sev = "HIGH"
    elif pct_zero_null > 0:
        sev = "MEDIUM"
    else:
        sev = None

    if sev:
        findings.append({
            "check": "B3",
            "raw_column": wrvu_col,
            "em_proc_rows": len(em_proc_df),
            "zero_null_count": zero_null_count,
            "zero_null_pct": round(pct_zero_null, 2),
            "negative_count": neg_count,
            "extreme_count": extreme_count,
            "severity": sev,
            "message": (
                f"B3: {pct_zero_null:.1f}% of E&M/procedural rows have zero or null wRVUs "
                f"({zero_null_count:,} of {len(em_proc_df):,} rows)"
            ),
        })

    if neg_count > 0:
        findings.append({
            "check": "B3",
            "raw_column": wrvu_col,
            "negative_count": neg_count,
            "severity": "MEDIUM",
            "message": f"B3: {neg_count:,} negative wRVU values found",
        })

    # Summary INFO
    total_non_zero = int((wrvus.notna() & (wrvus != 0)).sum())
    findings.append({
        "check": "B3",
        "raw_column": wrvu_col,
        "em_proc_rows": len(em_proc_df),
        "populated_count": total_non_zero,
        "population_pct": round(total_non_zero / max(len(em_proc_df), 1) * 100, 2),
        "severity": "INFO",
        "message": f"B3: wRVU population rate {total_non_zero / max(len(em_proc_df), 1) * 100:.1f}% on E&M/procedural rows",
    })

    return findings


def b4_charge_amount(df: pd.DataFrame, column_mappings: list[dict], source: str) -> list[dict]:
    """B4: Charge amount reasonableness."""
    findings = []
    amt_col = resolve_column(column_mappings, "ChargeAmountOriginal")
    if not amt_col or amt_col not in df.columns:
        return findings

    work_df = df
    if source == "billing_combined":
        mask = _build_charge_mask(df, column_mappings)
        if mask is not None:
            work_df = df[mask]

    if len(work_df) == 0:
        return findings

    amounts = pd.to_numeric(work_df[amt_col], errors="coerce")
    null_zero = int((amounts.isna() | (amounts == 0)).sum())
    pct = null_zero / len(work_df) * 100

    tt_col = resolve_column(column_mappings, "TransactionType")
    ttd_col = resolve_column(column_mappings, "TransactionTypeDesc")

    def is_void_series(s: pd.Series) -> pd.Series:
        return s.astype(str).str.strip().str.lower().apply(_is_void_row)

    neg_mask = amounts < 0
    if tt_col and tt_col in work_df.columns:
        void_mask = is_void_series(work_df[tt_col])
    elif ttd_col and ttd_col in work_df.columns:
        void_mask = is_void_series(work_df[ttd_col])
    else:
        void_mask = pd.Series([False] * len(work_df), index=work_df.index)

    non_void_neg = int((neg_mask & ~void_mask).sum())
    extreme = int((amounts.dropna() > 100000).sum())

    if pct > 5:
        findings.append({
            "check": "B4",
            "raw_column": amt_col,
            "null_zero_count": null_zero,
            "null_zero_pct": round(pct, 2),
            "severity": "HIGH",
            "message": f"B4: {pct:.1f}% of charge rows have null/zero charge amounts ({null_zero:,} rows)",
        })

    if non_void_neg > 0:
        findings.append({
            "check": "B4",
            "raw_column": amt_col,
            "neg_non_void_count": non_void_neg,
            "severity": "MEDIUM",
            "message": f"B4: {non_void_neg:,} negative charge amounts on non-void rows",
        })

    if extreme > 0:
        findings.append({
            "check": "B4",
            "raw_column": amt_col,
            "extreme_count": extreme,
            "severity": "INFO",
            "message": f"B4: {extreme:,} charge amounts exceed $100,000",
        })

    return findings


def b5_rendering_npi(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """B5: Rendering Provider NPI consistency."""
    findings = []
    npi_col = resolve_column(column_mappings, "RenderingProviderNpi")
    if not npi_col or npi_col not in df.columns:
        return findings

    npi_series = df[npi_col].dropna().astype(str).str.strip()
    npi_counts = npi_series.value_counts()
    distinct = len(npi_counts)

    if distinct == 0:
        return findings

    top_npi, top_count = npi_counts.index[0], int(npi_counts.iloc[0])
    top_pct = top_count / len(npi_series) * 100

    if top_pct > 50:
        findings.append({
            "check": "B5",
            "raw_column": npi_col,
            "top_npi": top_npi,
            "top_npi_pct": round(top_pct, 2),
            "severity": "MEDIUM",
            "message": f"B5: Single NPI '{top_npi}' appears on {top_pct:.1f}% of rows (unusual concentration)",
        })

    # Check NPI → name mapping consistency
    name_col = resolve_column(column_mappings, "RenderingProviderFullName")
    if name_col and name_col in df.columns:
        npi_name_df = df[[npi_col, name_col]].dropna(subset=[npi_col])
        npi_name_df = npi_name_df[npi_name_df[npi_col].astype(str).str.strip() != ""]
        npi_to_names = npi_name_df.groupby(npi_col)[name_col].nunique()
        multi_name = npi_to_names[npi_to_names > 1]
        if len(multi_name) > 0:
            findings.append({
                "check": "B5",
                "severity": "MEDIUM",
                "multi_name_npi_count": len(multi_name),
                "message": f"B5: {len(multi_name):,} NPIs map to multiple provider names (data quality issue)",
                "sample_npis": list(multi_name.index[:5]),
            })

    findings.append({
        "check": "B5",
        "raw_column": npi_col,
        "distinct_npi_count": distinct,
        "severity": "INFO",
        "message": f"B5: {distinct:,} distinct Rendering Provider NPIs",
    })

    return findings


def b6_org_hierarchy(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """B6: Cost Center / Org Hierarchy coverage."""
    findings = []
    org_cols = {
        "BillDepartmentId": resolve_column(column_mappings, "BillDepartmentId"),
        "BillDepartmentName": resolve_column(column_mappings, "BillDepartmentName"),
        "BillLocationName": resolve_column(column_mappings, "BillLocationName"),
        "BillPracticeName": resolve_column(column_mappings, "BillPracticeName"),
    }

    pop_rates = {}
    for stg, raw in org_cols.items():
        if raw and raw in df.columns:
            series = df[raw]
            non_null = series.notna() & (series.astype(str).str.strip() != "")
            pop_rates[stg] = non_null.sum() / len(df) * 100

    primary_pop = max(
        pop_rates.get("BillDepartmentId", 0),
        pop_rates.get("BillDepartmentName", 0),
    )
    any_good = any(v >= 90 for v in pop_rates.values())

    if primary_pop < 50 and not any_good:
        sev = "CRITICAL"
        msg = "B6: All org hierarchy fields < 50% populated — no GL crosswalk possible"
    elif primary_pop < 90:
        sev = "HIGH"
        msg = f"B6: Cost Center < 90% populated ({primary_pop:.1f}%) — GL crosswalk may be unreliable"
    else:
        sev = "INFO"
        msg = f"B6: Cost Center {primary_pop:.1f}% populated"

    findings.append({
        "check": "B6",
        "population_rates": {k: round(v, 2) for k, v in pop_rates.items()},
        "severity": sev,
        "message": msg,
    })

    return findings


def b7_cpt_modifier_separation(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """B7: CPT modifier separation check."""
    findings = []
    cpt_col = resolve_column(column_mappings, "CptCode")
    if not cpt_col or cpt_col not in df.columns:
        return findings

    embedded_pattern = re.compile(r"[-\s][A-Z0-9]{2}$")
    cpt_series = df[cpt_col].dropna().astype(str).str.strip()
    long_values = cpt_series[cpt_series.str.len() > 5]
    embedded = cpt_series[cpt_series.apply(lambda v: bool(embedded_pattern.search(v)))]

    if len(embedded) > 0:
        findings.append({
            "check": "B7",
            "raw_column": cpt_col,
            "embedded_count": len(embedded),
            "sample_values": list(embedded.value_counts().index[:5]),
            "severity": "MEDIUM",
            "message": f"B7: {len(embedded):,} CPT codes appear to have embedded modifiers (e.g., '99213-25')",
        })

    # Check modifier columns
    for i in range(1, 5):
        mod_col = resolve_column(column_mappings, f"Modifier{i}")
        if not mod_col or mod_col not in df.columns:
            continue
        mod_series = df[mod_col].dropna().astype(str).str.strip()
        multi_mod = mod_series[(mod_series.str.len() > 2) & (mod_series != "")]
        if len(multi_mod) > 0:
            findings.append({
                "check": "B7",
                "raw_column": mod_col,
                "multi_modifier_count": len(multi_mod),
                "severity": "MEDIUM",
                "message": f"B7: '{mod_col}': {len(multi_mod):,} values > 2 chars (may contain concatenated modifiers)",
            })

    return findings


def b8_icd10_separation(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """B8: ICD-10 code separation check."""
    findings = []
    icd_cols = {
        "PrimaryIcdCode": resolve_column(column_mappings, "PrimaryIcdCode"),
        "SecondaryIcdCodes": resolve_column(column_mappings, "SecondaryIcdCodes"),
    }
    concat_pattern = re.compile(r"[,;|]")
    icd9_pattern = re.compile(r"^\d{3}(\.\d{0,2})?$")

    for stg, raw in icd_cols.items():
        if not raw or raw not in df.columns:
            continue
        icd_series = df[raw].dropna().astype(str).str.strip()
        icd_series = icd_series[icd_series != ""]

        concat_count = int(icd_series.str.contains(concat_pattern).sum())
        icd9_count = int(icd_series.apply(lambda v: bool(icd9_pattern.match(v))).sum())

        if concat_count > 0:
            findings.append({
                "check": "B8",
                "raw_column": raw,
                "staging_column": stg,
                "concatenated_count": concat_count,
                "severity": "HIGH",
                "message": f"B8: '{raw}': {concat_count:,} values appear to contain multiple ICD codes (concatenated)",
            })

        if icd9_count > 0:
            findings.append({
                "check": "B8",
                "raw_column": raw,
                "staging_column": stg,
                "icd9_count": icd9_count,
                "severity": "MEDIUM",
                "message": f"B8: '{raw}': {icd9_count:,} values appear to be ICD-9 format (legacy data or mapping issue)",
            })

    return findings


def b9_payer_financial_class(df: pd.DataFrame, column_mappings: list[dict], source: str) -> list[dict]:
    """B9: Payer Financial Class categorization."""
    findings = []
    payer_cols = []
    for stg in ("ChargePayerFinancialClass", "TransactionPayerFinancialClass"):
        raw = resolve_column(column_mappings, stg)
        if raw and raw in df.columns:
            payer_cols.append(raw)

    if not payer_cols:
        return findings

    combined = df[payer_cols[0]].dropna().astype(str).str.strip()
    if len(payer_cols) > 1:
        combined = pd.concat([df[c].dropna().astype(str).str.strip() for c in payer_cols])

    distinct_vals = combined[combined != ""].unique()
    unclassified = []
    classified = {}

    for val in distinct_vals:
        found = False
        for bucket, pattern in _PAYER_BUCKETS.items():
            if pattern.search(val):
                classified[val] = bucket
                found = True
                break
        if not found:
            unclassified.append(val)

    unclass_pct = len(unclassified) / max(len(distinct_vals), 1) * 100

    if unclass_pct > 10:
        sev = "MEDIUM"
    else:
        sev = "INFO"

    findings.append({
        "check": "B9",
        "raw_columns": payer_cols,
        "distinct_value_count": len(distinct_vals),
        "unclassified_count": len(unclassified),
        "unclassified_pct": round(unclass_pct, 2),
        "unclassified_values": list(unclassified[:20]),
        "severity": sev,
        "message": (
            f"B9: {len(distinct_vals)} distinct Financial Class values; "
            f"{len(unclassified)} ({unclass_pct:.1f}%) unclassifiable"
        ),
    })

    return findings


def b10_void_charge_validation(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """B10: Void charge validation."""
    findings = []
    units_col = resolve_column(column_mappings, "Units")
    tt_col = resolve_column(column_mappings, "TransactionType")
    ttd_col = resolve_column(column_mappings, "TransactionTypeDesc")

    if not units_col or units_col not in df.columns:
        return findings

    col = tt_col if (tt_col and tt_col in df.columns) else (
        ttd_col if (ttd_col and ttd_col in df.columns) else None
    )
    if not col:
        return findings

    tt_series = df[col].astype(str).str.strip().str.lower()
    void_mask = tt_series.isin(_VOID_KEYWORDS)
    units = pd.to_numeric(df[units_col], errors="coerce")

    void_pos_units = int((void_mask & (units > 0)).sum())
    non_void_neg_units = int((~void_mask & (units < 0)).sum())

    if void_pos_units > 0:
        findings.append({
            "check": "B10",
            "positive_units_on_void_count": void_pos_units,
            "severity": "MEDIUM",
            "message": f"B10: {void_pos_units:,} void rows have positive Units (expected negative for voids)",
        })

    if non_void_neg_units > 0:
        findings.append({
            "check": "B10",
            "negative_units_non_void_count": non_void_neg_units,
            "severity": "MEDIUM",
            "message": f"B10: {non_void_neg_units:,} non-void rows have negative Units",
        })

    return findings


def b11_post_date_window(
    df: pd.DataFrame,
    column_mappings: list[dict],
    test_month: str,
    date_range: tuple[date, date] | None = None,
) -> list[dict]:
    """B11: Post Date window validation.

    Only runs when *date_range* is explicitly provided (set via --date-start /
    --date-end in run_phase1.py). Returns empty list if date_range is None.
    """
    findings = []
    if date_range is None:
        return findings

    post_col = resolve_column(column_mappings, "PostDate")
    if not post_col or post_col not in df.columns:
        return findings

    window_start, window_end = date_range

    parsed = pd.to_datetime(df[post_col], errors="coerce")
    valid = parsed.dropna()
    if len(valid) == 0:
        return findings

    outside = ((valid.dt.date < window_start) | (valid.dt.date > window_end)).sum()
    pct = outside / len(df) * 100

    sev = "HIGH" if pct > 10 else ("MEDIUM" if pct > 0 else "INFO")

    findings.append({
        "check": "B11",
        "raw_column": post_col,
        "expected_window": f"{window_start} to {window_end}",
        "outside_count": int(outside),
        "outside_pct": round(pct, 2),
        "min_date": str(valid.min().date()),
        "max_date": str(valid.max().date()),
        "severity": sev,
        "message": (
            f"B11: Post Date range {valid.min().date()} to {valid.max().date()}; "
            f"{outside:,} rows ({pct:.1f}%) outside window {window_start} to {window_end}"
        ),
    })

    return findings


def b12_patient_id_format(
    df: pd.DataFrame, column_mappings: list[dict]
) -> tuple[list[dict], dict]:
    """B12: Patient ID format consistency. Returns (findings, cross_source_prep)."""
    findings = []
    prep = {}

    for stg in ("PatientId", "PatientMrn"):
        raw = resolve_column(column_mappings, stg)
        if raw and raw in df.columns:
            pat_col = raw
            pat_stg = stg
            break
    else:
        return findings, prep

    series = df[pat_col].dropna().astype(str).str.strip()
    series = series[series != ""]
    if len(series) == 0:
        return findings, prep

    all_numeric = series.str.match(r"^\d+$").all()
    all_alpha = series.str.match(r"^[A-Za-z]+$").all()
    lengths = series.str.len()
    consistent_len = (lengths.max() == lengths.min())
    has_leading_zeros = series.str.match(r"^0\d+").any()

    if all_numeric:
        fmt = f"numeric_{int(lengths.median())}digit"
    elif all_alpha:
        fmt = "alpha"
    else:
        fmt = "alphanumeric"

    inconsistent = not (all_numeric or all_alpha) and series.str.match(r"^\d+$").mean() < 0.9
    if inconsistent:
        findings.append({
            "check": "B12",
            "raw_column": pat_col,
            "detected_format": fmt,
            "severity": "MEDIUM",
            "message": f"B12: '{pat_col}' has inconsistent Patient ID format (mix of numeric and alphanumeric)",
        })

    prep = {
        "patient_id_column": pat_col,
        "patient_id_staging": pat_stg,
        "patient_id_format": fmt,
        "patient_id_leading_zeros": bool(has_leading_zeros),
        "patient_id_length_consistent": bool(consistent_len),
        "patient_id_median_length": int(lengths.median()),
        "patient_id_sample_count": len(series),
    }

    findings.append({
        "check": "B12",
        "raw_column": pat_col,
        "detected_format": fmt,
        "leading_zeros": bool(has_leading_zeros),
        "severity": "INFO",
        "message": f"B12: Patient ID format detected as '{fmt}' (leading zeros: {has_leading_zeros})",
    })

    return findings, prep


def b13_cpt_validation(df: pd.DataFrame, column_mappings: list[dict], source: str) -> list[dict]:
    """B13: CPT code validation against stdCmsCpt."""
    findings = []
    cpt_df = staging_meta.get_cms_cpt()
    if cpt_df is None:
        findings.append({
            "check": "B13",
            "severity": "INFO",
            "message": "B13: Skipped — stdCmsCpt.csv not found in KnowledgeSources/",
        })
        return findings

    cpt_col = resolve_column(column_mappings, "CptCode")
    wrvu_col = resolve_column(column_mappings, "WorkRvuOriginal")
    if not cpt_col or cpt_col not in df.columns:
        return findings

    work_df = df
    if source == "billing_combined":
        mask = _build_charge_mask(df, column_mappings)
        if mask is not None:
            work_df = df[mask]

    cpt_series = work_df[cpt_col].dropna().astype(str).str.strip()
    cpt_series = cpt_series[cpt_series != ""]
    cpt_counts = cpt_series.value_counts()
    distinct_codes = set(cpt_counts.index)

    cms_codes = set(cpt_df.index.astype(str))
    unmatched = distinct_codes - cms_codes
    unmatched_pct = len(unmatched) / max(len(distinct_codes), 1) * 100

    if unmatched_pct > 5:
        sev = "HIGH"
    elif unmatched_pct > 0:
        sev = "MEDIUM"
    else:
        sev = "INFO"

    unmatched_with_counts = [
        {"code": c, "row_count": int(cpt_counts.get(c, 0))}
        for c in sorted(unmatched, key=lambda x: -cpt_counts.get(x, 0))[:20]
    ]

    findings.append({
        "check": "B13",
        "raw_column": cpt_col,
        "distinct_code_count": len(distinct_codes),
        "unmatched_count": len(unmatched),
        "unmatched_pct": round(unmatched_pct, 2),
        "unmatched_codes": unmatched_with_counts,
        "severity": sev,
        "message": (
            f"B13: {len(unmatched):,} of {len(distinct_codes):,} distinct CPT codes "
            f"({unmatched_pct:.1f}%) not found in stdCmsCpt"
        ),
    })

    # Check for deleted/bundled codes
    status_col = "StatusIndicator" if "StatusIndicator" in cpt_df.columns else None
    if status_col:
        inactive_codes = []
        for code in distinct_codes & cms_codes:
            status = str(cpt_df.loc[code, status_col]) if code in cpt_df.index else ""
            if status.upper() in ("D", "B", "I", "DELETED", "BUNDLED"):
                inactive_codes.append({"code": code, "status": status, "row_count": int(cpt_counts.get(code, 0))})
        if inactive_codes:
            findings.append({
                "check": "B13",
                "inactive_code_count": len(inactive_codes),
                "inactive_codes": inactive_codes[:20],
                "severity": "MEDIUM",
                "message": f"B13: {len(inactive_codes):,} deleted/bundled CPT codes still being billed",
            })

    # wRVU variance check
    if wrvu_col and wrvu_col in work_df.columns:
        wrvu_col_cms = "WorkRvu" if "WorkRvu" in cpt_df.columns else None
        if wrvu_col_cms:
            variance_flags = []
            code_wrvus = work_df.groupby(cpt_col)[wrvu_col].median()
            for code, client_wrvu in code_wrvus.items():
                code_str = str(code).strip()
                if code_str not in cpt_df.index:
                    continue
                cms_wrvu = pd.to_numeric(cpt_df.loc[code_str, wrvu_col_cms], errors="coerce")
                if pd.isna(cms_wrvu) or cms_wrvu == 0:
                    continue
                client_val = pd.to_numeric(client_wrvu, errors="coerce")
                if pd.isna(client_val):
                    continue
                if client_val == 0 and cms_wrvu > 0:
                    variance_flags.append({"code": code_str, "client_wrvu": 0, "cms_wrvu": float(cms_wrvu), "variance_pct": 100})
                elif cms_wrvu > 0:
                    var_pct = abs(client_val - cms_wrvu) / cms_wrvu * 100
                    if var_pct > 20:
                        variance_flags.append({
                            "code": code_str, "client_wrvu": float(client_val),
                            "cms_wrvu": float(cms_wrvu), "variance_pct": round(var_pct, 1),
                        })

            if variance_flags:
                var_pct_of_total = len(variance_flags) / max(len(distinct_codes & cms_codes), 1) * 100
                sev2 = "MEDIUM" if var_pct_of_total > 10 else "INFO"
                findings.append({
                    "check": "B13",
                    "wrvu_variance_count": len(variance_flags),
                    "wrvu_variance_pct_of_matched_codes": round(var_pct_of_total, 2),
                    "top_variances": variance_flags[:20],
                    "severity": sev2,
                    "message": (
                        f"B13: {len(variance_flags):,} CPT codes have >20% wRVU variance vs CMS published values"
                    ),
                })

    return findings


def b14_pos_validation(df: pd.DataFrame, column_mappings: list[dict], source: str) -> list[dict]:
    """B14: Place of Service validation against stdCmsPos."""
    findings = []
    pos_df = staging_meta.get_cms_pos()
    if pos_df is None:
        findings.append({
            "check": "B14",
            "severity": "INFO",
            "message": "B14: Skipped — stdCmsPos.csv not found in KnowledgeSources/",
        })
        return findings

    pos_col = resolve_column(column_mappings, "PlaceOfServiceCode")
    if not pos_col or pos_col not in df.columns:
        return findings

    pos_series = df[pos_col].dropna().astype(str).str.strip().str.zfill(2)
    pos_series = pos_series[pos_series != "00"]
    pos_counts = pos_series.value_counts()
    distinct_codes = set(pos_counts.index)

    cms_codes = set(pos_df.index.astype(str).str.zfill(2))
    invalid = distinct_codes - cms_codes
    invalid_row_count = sum(pos_counts.get(c, 0) for c in invalid)
    invalid_row_pct = invalid_row_count / max(len(df), 1) * 100

    if invalid_row_pct > 5:
        sev = "HIGH"
    elif invalid_row_pct > 0:
        sev = "MEDIUM"
    else:
        sev = "INFO"

    findings.append({
        "check": "B14",
        "raw_column": pos_col,
        "distinct_code_count": len(distinct_codes),
        "invalid_code_count": len(invalid),
        "invalid_row_count": int(invalid_row_count),
        "invalid_row_pct": round(invalid_row_pct, 2),
        "invalid_codes": sorted(invalid)[:20],
        "severity": sev,
        "message": (
            f"B14: {len(invalid):,} POS codes not in stdCmsPos "
            f"({invalid_row_pct:.1f}% of rows)"
        ),
    })

    # Distribution: POS 21 (inpatient) > 30%
    inpatient_pct = pos_counts.get("21", 0) / max(len(pos_series), 1) * 100
    if inpatient_pct > 30:
        findings.append({
            "check": "B14",
            "pos_21_pct": round(inpatient_pct, 2),
            "severity": "MEDIUM",
            "message": f"B14: POS 21 (Inpatient Hospital) = {inpatient_pct:.1f}% — unusual for physician practice billing",
        })

    # No POS 11 (Office)
    if "11" not in distinct_codes:
        findings.append({
            "check": "B14",
            "severity": "MEDIUM",
            "message": "B14: No POS 11 (Office) found — may indicate non-standard POS values or non-physician-practice data",
        })

    # POS code distribution summary
    name_col = "PosName" if "PosName" in pos_df.columns else None
    dist = []
    for code, cnt in pos_counts.head(15).items():
        row = {"code": code, "row_count": int(cnt), "row_pct": round(int(cnt) / len(pos_series) * 100, 2)}
        if name_col and code in pos_df.index:
            row["cms_name"] = str(pos_df.loc[code, name_col])
        else:
            row["cms_name"] = "(not in CMS)" if code in invalid else "OK"
        dist.append(row)

    findings.append({
        "check": "B14",
        "raw_column": pos_col,
        "pos_distribution": dist,
        "severity": "INFO",
        "message": f"B14: POS distribution — {len(distinct_codes)} distinct codes; top code: {pos_counts.index[0]}",
    })

    return findings


def run_checks(
    billing_dfs: dict[str, dict],
    test_month: str,
    date_range: tuple[date, date] | None = None,
) -> dict[str, list[dict]]:
    """
    Run all billing-specific checks.
    billing_dfs: {filename: {df, source, column_mappings, staging_table, ...}}
    date_range:  (start, end) from phase1_findings.json; None skips window checks.
    Returns: {filename: [findings]}
    """
    results: dict[str, list[dict]] = {}
    cross_source_prep: dict[str, dict] = {}

    # B2 requires both files simultaneously
    b2_findings = b2_charge_transaction_linkage(billing_dfs)

    for fname, entry in billing_dfs.items():
        df = entry["df"]
        source = entry["source"]
        col_maps = entry["column_mappings"]
        findings = []

        findings.extend(b1_transaction_type_validation(df, col_maps, source))
        findings.extend(b2_findings)  # same findings appended to each billing file
        findings.extend(b3_wrvu_validation(df, col_maps, source))
        findings.extend(b4_charge_amount(df, col_maps, source))
        findings.extend(b5_rendering_npi(df, col_maps))
        findings.extend(b6_org_hierarchy(df, col_maps))
        findings.extend(b7_cpt_modifier_separation(df, col_maps))
        findings.extend(b8_icd10_separation(df, col_maps))
        findings.extend(b9_payer_financial_class(df, col_maps, source))
        findings.extend(b10_void_charge_validation(df, col_maps))
        findings.extend(b11_post_date_window(df, col_maps, test_month, date_range))
        b12_f, prep = b12_patient_id_format(df, col_maps)
        findings.extend(b12_f)
        findings.extend(b13_cpt_validation(df, col_maps, source))
        findings.extend(b14_pos_validation(df, col_maps, source))

        results[fname] = findings
        if prep:
            cross_source_prep[fname] = prep

    return results, cross_source_prep
