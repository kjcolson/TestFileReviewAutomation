"""
phase4/scheduling_gl.py

C5: Scheduling <-> GL Location to Cost Center.
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
_SCHED_LOC_COLS = ["BillLocNameOrig", "PracNameOrig", "DeptNameOrig", "DeptId"]
_GL_CC_NAME = "CostCenterNameOrig"
_GL_CC_NUMBER = "CostCenterNumberOrig"

_FUZZY_THRESHOLD = 80


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(s: Any) -> str:
    return str(s).strip().lower()


def _get_distinct_vals(df: pd.DataFrame, raw_col: str) -> list[str]:
    """Return list of normalized non-empty distinct values."""
    if raw_col not in df.columns:
        return []
    return (
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
    unmatched: list[str],
    reference: list[str],
    threshold: int,
    max_pairs: int = 50,
) -> list[dict]:
    """Find fuzzy match candidates for unmatched values against reference."""
    if not _HAS_RAPIDFUZZ or not unmatched or not reference:
        return []
    candidates = []
    for val in unmatched[:max_pairs]:
        best_score = 0
        best_ref = None
        for ref_val in reference:
            score = _fuzz.token_sort_ratio(val, ref_val)
            if score > best_score:
                best_score = score
                best_ref = ref_val
        if best_score >= threshold and best_ref:
            candidates.append({
                "scheduling_value": val,
                "gl_candidate": best_ref,
                "score": best_score,
            })
    return candidates


def _check_sched_col(
    sched_df: pd.DataFrame,
    sched_maps: list[dict],
    staging_col: str,
    gl_ref: list[str],
) -> dict | None:
    """Check one scheduling location column against the GL reference set."""
    raw_col = resolve_column(sched_maps, staging_col)
    if not raw_col or raw_col not in sched_df.columns:
        return None

    sched_vals = _get_distinct_vals(sched_df, raw_col)
    if not sched_vals:
        return None

    gl_ref_set = set(gl_ref)
    exact_matched = [v for v in sched_vals if v in gl_ref_set]
    unmatched = [v for v in sched_vals if v not in gl_ref_set]

    match_pct = len(exact_matched) / len(sched_vals) * 100
    fuzzy_cands = _fuzzy_candidates(unmatched, gl_ref, _FUZZY_THRESHOLD)

    # Severity
    unmatched_no_fuzzy = len(unmatched) - len({c["scheduling_value"] for c in fuzzy_cands})
    unmatched_no_fuzzy_pct = unmatched_no_fuzzy / max(len(sched_vals), 1)

    if unmatched_no_fuzzy_pct > 0.30:
        severity = "HIGH"
    elif unmatched:
        severity = "MEDIUM"
    else:
        severity = "PASS"

    msg = (
        f"C5 {staging_col}: {match_pct:.1f}% of {len(sched_vals)} scheduling values "
        f"found in GL cost centers ({len(unmatched)} unmatched"
        + (f", {len(fuzzy_cands)} fuzzy candidates" if fuzzy_cands else "")
        + ")"
    )

    return {
        "check": "C5",
        "severity": severity,
        "message": msg,
        "scheduling_column": staging_col,
        "scheduling_raw_column": raw_col,
        "location_distinct": len(sched_vals),
        "exact_match_count": len(exact_matched),
        "fuzzy_candidate_count": len(fuzzy_cands),
        "unmatched_count": len(unmatched),
        "match_pct": round(match_pct, 2),
        "fuzzy_candidates": fuzzy_cands[:20],
        "unmatched_sample": unmatched[:20],
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_checks(
    file_entries: dict[str, dict],
) -> dict:
    """
    Run C5 check: Scheduling <-> GL location to cost center.

    Returns a finding dict.
    """
    sched_entry = None
    gl_entry = None

    for fname, entry in file_entries.items():
        source = entry.get("source", "")
        if source == "scheduling" and entry.get("df") is not None:
            sched_entry = entry
        elif source == "gl" and entry.get("df") is not None:
            gl_entry = entry

    if sched_entry is None:
        return {
            "check": "C5",
            "severity": "INFO",
            "message": "Skipped — scheduling file not present",
            "skipped": True,
        }
    if gl_entry is None:
        return {
            "check": "C5",
            "severity": "INFO",
            "message": "Skipped — GL file not present",
            "skipped": True,
        }

    sched_df = sched_entry["df"]
    sched_maps = sched_entry.get("column_mappings", [])
    gl_df = gl_entry["df"]
    gl_maps = gl_entry.get("column_mappings", [])

    # Build GL reference list (names + numbers combined)
    gl_ref: list[str] = []
    for staging in (_GL_CC_NAME, _GL_CC_NUMBER):
        raw = resolve_column(gl_maps, staging)
        if raw and raw in gl_df.columns:
            vals = _get_distinct_vals(gl_df, raw)
            gl_ref.extend(v for v in vals if v not in gl_ref)

    if not gl_ref:
        return {
            "check": "C5",
            "severity": "INFO",
            "message": "C5: GL file has no CostCenterName or CostCenterNumber data",
            "skipped": False,
            "files_compared": "scheduling + gl",
            "findings": [],
        }

    # Check each scheduling location column
    column_findings = []
    for staging in _SCHED_LOC_COLS:
        result = _check_sched_col(sched_df, sched_maps, staging, gl_ref)
        if result is not None:
            column_findings.append(result)

    if not column_findings:
        return {
            "check": "C5",
            "severity": "INFO",
            "message": "C5: No scheduling location/dept/practice columns found",
            "skipped": False,
            "files_compared": "scheduling + gl",
            "findings": [],
        }

    # Worst severity
    sev_order = {"HIGH": 0, "MEDIUM": 1, "PASS": 2, "INFO": 3}
    worst = min(column_findings, key=lambda f: sev_order.get(f.get("severity", "INFO"), 3))
    overall_severity = worst.get("severity", "INFO")

    return {
        "check": "C5",
        "severity": overall_severity,
        "message": f"C5: Scheduling <-> GL location check complete ({len(column_findings)} column(s) checked)",
        "files_compared": "scheduling + gl",
        "skipped": False,
        "findings": column_findings,
    }
