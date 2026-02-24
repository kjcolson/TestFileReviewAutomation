"""
phase4/billing_scheduling.py

C3: Billing <-> Scheduling (Location / Provider NPI / Patient ID).

Three sub-checks:
  C3a — Location/Department cross-reference
  C3b — Provider NPI cross-reference
  C3c — Patient ID cross-reference (with leading-zero normalization from Phase 3)
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

# Billing location/dept/practice org columns
_BILLING_LOC_COLS = ["BillLocationName", "BillDepartmentName", "BillPracticeName"]
_BILLING_PATIENT_ID = "PatientId"
_BILLING_PROVIDER_NPI = "RenderingProviderNpi"
_BILLING_DEPT_ID = "BillDepartmentId"

# Scheduling staging column names (from constants.py)
_SCHED_LOC_COLS = ["BillLocNameOrig", "DeptNameOrig", "PracNameOrig"]
_SCHED_PATIENT_ID = "PatIdOrig"
_SCHED_PROVIDER_NPI = "ApptProvNPI"
_SCHED_DEPT_ID = "DeptId"

_FUZZY_THRESHOLD = 80


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(s: Any) -> str:
    return str(s).strip().lower()


def _get_distinct_vals(df: pd.DataFrame, raw_col: str) -> set[str]:
    """Return set of normalized non-empty distinct values from a column."""
    if raw_col not in df.columns:
        return set()
    return set(
        df[raw_col]
        .dropna()
        .astype(str)
        .str.strip()
        .replace("", pd.NA)
        .dropna()
        .str.lower()
        .unique()
        .tolist()
    )


def _fuzzy_candidates(
    unmatched: set[str],
    reference: set[str],
    threshold: int,
    max_pairs: int = 50,
) -> list[dict]:
    """Find fuzzy match candidates."""
    if not _HAS_RAPIDFUZZ or not unmatched or not reference:
        return []
    ref_list = list(reference)
    candidates = []
    for val in list(unmatched)[:max_pairs]:
        best_score = 0
        best_ref = None
        for ref_val in ref_list:
            score = _fuzz.token_sort_ratio(val, ref_val)
            if score > best_score:
                best_score = score
                best_ref = ref_val
        if best_score >= threshold and best_ref:
            candidates.append({
                "source_value": val,
                "match_candidate": best_ref,
                "score": best_score,
            })
    return candidates


def _strip_leading_zeros(val: str) -> str:
    """Strip leading zeros from a numeric-looking string."""
    stripped = val.lstrip("0")
    return stripped if stripped else "0"


# ---------------------------------------------------------------------------
# C3a — Location/Department cross-reference
# ---------------------------------------------------------------------------

def _c3a_location(
    billing_df: pd.DataFrame,
    billing_maps: list[dict],
    sched_df: pd.DataFrame,
    sched_maps: list[dict],
) -> list[dict]:
    """C3a: Billing location/dept/practice vs scheduling location/dept/practice."""
    findings = []

    # Build scheduling reference set
    sched_ref: set[str] = set()
    for staging in _SCHED_LOC_COLS:
        raw = resolve_column(sched_maps, staging)
        if raw and raw in sched_df.columns:
            sched_ref |= _get_distinct_vals(sched_df, raw)

    if not sched_ref:
        findings.append({
            "check": "C3a",
            "severity": "INFO",
            "message": "C3a: No location/dept/practice columns found in scheduling file",
        })
        return findings

    # Check each billing location column
    for staging in _BILLING_LOC_COLS:
        raw = resolve_column(billing_maps, staging)
        if not raw or raw not in billing_df.columns:
            continue

        billing_vals = _get_distinct_vals(billing_df, raw)
        if not billing_vals:
            continue

        exact_matched = billing_vals & sched_ref
        unmatched = billing_vals - sched_ref
        match_pct = len(exact_matched) / len(billing_vals) * 100

        fuzzy_cands = _fuzzy_candidates(unmatched, sched_ref, _FUZZY_THRESHOLD)
        has_fuzzy = len(fuzzy_cands) > 0
        unmatched_no_fuzzy = len(unmatched) - len({c["source_value"] for c in fuzzy_cands})

        if unmatched_no_fuzzy / max(len(billing_vals), 1) > 0.50:
            severity = "HIGH"
        elif unmatched:
            severity = "MEDIUM"
        else:
            severity = "PASS"

        msg = (
            f"C3a {staging}: {match_pct:.1f}% of {len(billing_vals)} billing values "
            f"found in scheduling ({len(unmatched)} unmatched"
            + (f", {len(fuzzy_cands)} fuzzy candidates" if fuzzy_cands else "")
            + ")"
        )

        findings.append({
            "check": "C3a",
            "severity": severity,
            "message": msg,
            "billing_column": staging,
            "billing_raw_column": raw,
            "billing_distinct": len(billing_vals),
            "scheduling_distinct": len(sched_ref),
            "overlap_count": len(exact_matched),
            "billing_coverage_pct": round(match_pct, 2),
            "fuzzy_candidates": fuzzy_cands[:20],
            "unmatched_sample": list(unmatched)[:20],
        })

    # Direct dept ID cross-reference (no fuzzy — IDs must match exactly)
    bill_dept_id_raw = resolve_column(billing_maps, _BILLING_DEPT_ID)
    sched_dept_id_raw = resolve_column(sched_maps, _SCHED_DEPT_ID)
    if (bill_dept_id_raw and bill_dept_id_raw in billing_df.columns
            and sched_dept_id_raw and sched_dept_id_raw in sched_df.columns):
        billing_dept_ids = _get_distinct_vals(billing_df, bill_dept_id_raw)
        sched_dept_ids = _get_distinct_vals(sched_df, sched_dept_id_raw)
        if billing_dept_ids and sched_dept_ids:
            matched = billing_dept_ids & sched_dept_ids
            unmatched = billing_dept_ids - sched_dept_ids
            match_pct = len(matched) / len(billing_dept_ids) * 100
            unmatched_pct = len(unmatched) / max(len(billing_dept_ids), 1)
            if unmatched_pct > 0.30:
                severity = "HIGH"
            elif unmatched:
                severity = "MEDIUM"
            else:
                severity = "PASS"
            findings.append({
                "check": "C3a",
                "severity": severity,
                "message": (
                    f"C3a BillDepartmentId vs DeptId (direct ID match): {match_pct:.1f}% "
                    f"of {len(billing_dept_ids)} billing dept IDs found in scheduling "
                    f"({len(unmatched)} unmatched)"
                ),
                "billing_column": "BillDepartmentId",
                "billing_raw_column": bill_dept_id_raw,
                "billing_distinct": len(billing_dept_ids),
                "scheduling_distinct": len(sched_dept_ids),
                "overlap_count": len(matched),
                "billing_coverage_pct": round(match_pct, 2),
                "fuzzy_candidates": [],
                "unmatched_sample": list(unmatched)[:20],
            })

    if not findings:
        findings.append({
            "check": "C3a",
            "severity": "INFO",
            "message": "C3a: No billing location/dept/practice columns resolved",
        })

    return findings


# ---------------------------------------------------------------------------
# C3b — Provider NPI cross-reference
# ---------------------------------------------------------------------------

def _c3b_provider_npi(
    billing_df: pd.DataFrame,
    billing_maps: list[dict],
    sched_df: pd.DataFrame,
    sched_maps: list[dict],
) -> dict:
    """C3b: Rendering Provider NPI (billing) vs Appt Provider NPI (scheduling)."""
    bill_npi_raw = resolve_column(billing_maps, _BILLING_PROVIDER_NPI)
    sched_npi_raw = resolve_column(sched_maps, _SCHED_PROVIDER_NPI)

    if not bill_npi_raw or bill_npi_raw not in billing_df.columns:
        return {
            "check": "C3b",
            "severity": "INFO",
            "message": "C3b skipped — RenderingProviderNpi not found in billing",
        }
    if not sched_npi_raw or sched_npi_raw not in sched_df.columns:
        return {
            "check": "C3b",
            "severity": "INFO",
            "message": "C3b skipped — ApptProvNPI not found in scheduling",
        }

    billing_npis = _get_distinct_vals(billing_df, bill_npi_raw)
    sched_npis = _get_distinct_vals(sched_df, sched_npi_raw)

    if not billing_npis or not sched_npis:
        return {
            "check": "C3b",
            "severity": "INFO",
            "message": f"C3b skipped — insufficient NPI data (billing: {len(billing_npis)}, scheduling: {len(sched_npis)})",
        }

    billing_in_sched = billing_npis & sched_npis
    sched_in_billing = sched_npis & billing_npis

    billing_coverage = len(billing_in_sched) / len(billing_npis) * 100
    sched_coverage = len(sched_in_billing) / len(sched_npis) * 100
    min_coverage = min(billing_coverage, sched_coverage)

    if min_coverage < 50:
        severity = "HIGH"
    elif min_coverage < 80:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    msg = (
        f"C3b Provider NPI: {billing_coverage:.1f}% of billing NPIs found in scheduling; "
        f"{sched_coverage:.1f}% of scheduling NPIs found in billing"
    )

    return {
        "check": "C3b",
        "severity": severity,
        "message": msg,
        "billing_distinct": len(billing_npis),
        "scheduling_distinct": len(sched_npis),
        "overlap_count": len(billing_in_sched),
        "billing_coverage_pct": round(billing_coverage, 2),
        "scheduling_coverage_pct": round(sched_coverage, 2),
        "unmatched_billing_sample": list(billing_npis - sched_npis)[:10],
        "unmatched_scheduling_sample": list(sched_npis - billing_npis)[:10],
    }


# ---------------------------------------------------------------------------
# C3c — Patient ID cross-reference
# ---------------------------------------------------------------------------

def _c3c_patient_id(
    billing_df: pd.DataFrame,
    billing_maps: list[dict],
    sched_df: pd.DataFrame,
    sched_maps: list[dict],
    billing_prep: dict,
    sched_prep: dict,
) -> dict:
    """C3c: Patient ID cross-reference with leading-zero normalization."""
    bill_pat_raw = resolve_column(billing_maps, _BILLING_PATIENT_ID)
    sched_pat_raw = resolve_column(sched_maps, _SCHED_PATIENT_ID)

    if not bill_pat_raw or bill_pat_raw not in billing_df.columns:
        return {
            "check": "C3c",
            "severity": "INFO",
            "message": "C3c skipped — PatientId not found in billing",
        }
    if not sched_pat_raw or sched_pat_raw not in sched_df.columns:
        return {
            "check": "C3c",
            "severity": "INFO",
            "message": "C3c skipped — PatIdOrig not found in scheduling",
        }

    # Build ID sets (normalized)
    billing_ids_raw = (
        billing_df[bill_pat_raw]
        .dropna().astype(str).str.strip()
        .replace("", pd.NA).dropna()
        .unique().tolist()
    )
    sched_ids_raw = (
        sched_df[sched_pat_raw]
        .dropna().astype(str).str.strip()
        .replace("", pd.NA).dropna()
        .unique().tolist()
    )

    if not billing_ids_raw or not sched_ids_raw:
        return {
            "check": "C3c",
            "severity": "INFO",
            "message": f"C3c skipped — insufficient patient ID data (billing: {len(billing_ids_raw)}, scheduling: {len(sched_ids_raw)})",
        }

    # Leading zero normalization
    bill_leading_zeros = billing_prep.get("patient_id_leading_zeros", False)
    sched_leading_zeros = sched_prep.get("patient_id_leading_zeros", False)
    format_note = None

    billing_ids = set(str(v).lower() for v in billing_ids_raw)
    sched_ids = set(str(v).lower() for v in sched_ids_raw)

    # If one side has leading zeros and the other doesn't, strip them from the side that does
    if bill_leading_zeros and not sched_leading_zeros:
        billing_ids = {_strip_leading_zeros(v) for v in billing_ids}
        format_note = "Billing patient IDs normalized (leading zeros stripped) to match scheduling format"
    elif sched_leading_zeros and not bill_leading_zeros:
        sched_ids = {_strip_leading_zeros(v) for v in sched_ids}
        format_note = "Scheduling patient IDs normalized (leading zeros stripped) to match billing format"

    billing_in_sched = billing_ids & sched_ids
    sched_in_billing = sched_ids & billing_ids

    billing_coverage = len(billing_in_sched) / len(billing_ids) * 100 if billing_ids else 0.0
    sched_coverage = len(sched_in_billing) / len(sched_ids) * 100 if sched_ids else 0.0
    min_coverage = min(billing_coverage, sched_coverage)

    if min_coverage < 30:
        severity = "HIGH"
    elif min_coverage < 65:
        severity = "MEDIUM"
    elif format_note:
        severity = "INFO"
    else:
        severity = "PASS"

    msg = (
        f"C3c Patient IDs: {billing_coverage:.1f}% of billing IDs found in scheduling; "
        f"{sched_coverage:.1f}% of scheduling IDs found in billing"
    )
    if format_note:
        msg += f" (note: {format_note})"

    return {
        "check": "C3c",
        "severity": severity,
        "message": msg,
        "billing_distinct": len(billing_ids),
        "scheduling_distinct": len(sched_ids),
        "overlap_count": len(billing_in_sched),
        "billing_coverage_pct": round(billing_coverage, 2),
        "scheduling_coverage_pct": round(sched_coverage, 2),
        "format_note": format_note,
        "unmatched_billing_sample": list(billing_ids - sched_ids)[:10],
        "unmatched_scheduling_sample": list(sched_ids - billing_ids)[:10],
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_checks(
    file_entries: dict[str, dict],
    cross_source_prep_by_file: dict[str, dict],
) -> dict:
    """
    Run C3 checks: Billing <-> Scheduling.

    Returns a finding dict with sub_checks for C3a, C3b, C3c.
    """
    billing_entry = None
    billing_source = None
    billing_fname = None
    sched_entry = None
    sched_fname = None

    for fname, entry in file_entries.items():
        source = entry.get("source", "")
        if source in ("billing_combined", "billing_charges") and billing_entry is None:
            if entry.get("df") is not None:
                billing_entry = entry
                billing_source = source
                billing_fname = fname
        elif source == "scheduling" and entry.get("df") is not None:
            sched_entry = entry
            sched_fname = fname

    if billing_entry is None:
        return {
            "check": "C3",
            "severity": "INFO",
            "message": "Skipped — no billing file present",
            "skipped": True,
        }
    if sched_entry is None:
        return {
            "check": "C3",
            "severity": "INFO",
            "message": "Skipped — scheduling file not present",
            "skipped": True,
        }

    billing_df = billing_entry["df"]
    billing_maps = billing_entry.get("column_mappings", [])
    sched_df = sched_entry["df"]
    sched_maps = sched_entry.get("column_mappings", [])

    # For combined billing, use only charge rows
    if billing_source == "billing_combined":
        from phase4.transactions_charges import _build_charge_mask
        charge_mask = _build_charge_mask(billing_df, billing_maps)
        if charge_mask.any():
            billing_df = billing_df[charge_mask].reset_index(drop=True)

    billing_prep = cross_source_prep_by_file.get(billing_fname, {})
    sched_prep = cross_source_prep_by_file.get(sched_fname, {})

    # Run sub-checks
    c3a_findings = _c3a_location(billing_df, billing_maps, sched_df, sched_maps)
    c3b = _c3b_provider_npi(billing_df, billing_maps, sched_df, sched_maps)
    c3c = _c3c_patient_id(billing_df, billing_maps, sched_df, sched_maps, billing_prep, sched_prep)

    # Overall severity
    sev_order = {"HIGH": 0, "MEDIUM": 1, "PASS": 2, "INFO": 3}
    all_sevs = [f.get("severity", "INFO") for f in c3a_findings] + [
        c3b.get("severity", "INFO"),
        c3c.get("severity", "INFO"),
    ]
    worst = min(all_sevs, key=lambda s: sev_order.get(s, 3))

    return {
        "check": "C3",
        "severity": worst,
        "message": "C3: Billing <-> Scheduling cross-source check complete",
        "files_compared": f"{billing_source} + scheduling",
        "skipped": False,
        "sub_checks": {
            "C3a": c3a_findings,
            "C3b": c3b,
            "C3c": c3c,
        },
    }
