"""
phase4/payroll_gl.py

C4: Payroll <-> GL Department to Cost Center.

100% match expected. Handles the known pattern where Payroll Department IDs
embed the GL Cost Center Number as a substring at a consistent offset
(e.g., 8818748000 -> middle chars [2:7] -> 18748).
"""

from __future__ import annotations

from collections import Counter
from typing import Any

import pandas as pd

from shared.column_utils import resolve_column


# ---------------------------------------------------------------------------
# Staging column names
# ---------------------------------------------------------------------------
_PAYROLL_DEPT_ID = "DepartmentId"
_PAYROLL_DEPT_NAME = "DepartmentName"
_GL_CC_NUMBER = "CostCenterNumberOrig"
_GL_CC_NAME = "CostCenterNameOrig"

# Threshold: if > 80% of payroll dept IDs match at a consistent offset, apply auto-extraction
_AUTO_EXTRACT_THRESHOLD = 0.80
# Minimum number of payroll dept IDs needed to attempt auto-extraction
_AUTO_EXTRACT_MIN_SAMPLES = 5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _normalize(s: Any) -> str:
    return str(s).strip().lower()


def _strip_zeros(s: str) -> str:
    stripped = s.lstrip("0")
    return stripped if stripped else "0"


def _find_extraction_offset(
    payroll_ids: list[str],
    gl_cc_numbers: set[str],
) -> tuple[int, int] | None:
    """
    Try all (start, end) substring offsets on payroll dept IDs.
    If > AUTO_EXTRACT_THRESHOLD of payroll IDs have a matching GL cost center
    at the same (start, end) offset, return that offset.

    Returns (start, end) or None.
    """
    if len(payroll_ids) < _AUTO_EXTRACT_MIN_SAMPLES:
        return None

    # Get GL cost center lengths to know what substring length to try
    gl_lengths = {len(n) for n in gl_cc_numbers if n}
    if not gl_lengths:
        return None

    max_pay_len = max(len(p) for p in payroll_ids) if payroll_ids else 0

    # Count how many payroll IDs match at each (start, end) offset
    offset_counts: Counter = Counter()
    total = len(payroll_ids)

    for gl_len in sorted(gl_lengths):
        for start in range(max_pay_len - gl_len + 1):
            end = start + gl_len
            match_count = 0
            for pay_id in payroll_ids:
                if end > len(pay_id):
                    continue
                substr = pay_id[start:end]
                if substr in gl_cc_numbers or _strip_zeros(substr) in {_strip_zeros(g) for g in gl_cc_numbers}:
                    match_count += 1
            if match_count / total >= _AUTO_EXTRACT_THRESHOLD:
                offset_counts[(start, end)] = match_count

    if not offset_counts:
        return None

    # Return the offset with the highest match count
    best_offset = max(offset_counts, key=offset_counts.get)
    return best_offset


def _match_dept_to_gl(
    dept_id: str,
    gl_numbers: set[str],
    gl_names: set[str],
    offset: tuple[int, int] | None,
) -> bool:
    """
    Try to match a payroll department ID to GL cost center.
    Tries: exact → strip zeros → substring extraction (if offset provided).
    """
    norm = _normalize(dept_id)

    # Exact match
    if norm in gl_numbers or norm in gl_names:
        return True

    # Strip leading zeros
    stripped = _strip_zeros(norm)
    gl_stripped = {_strip_zeros(n) for n in gl_numbers}
    if stripped in gl_stripped:
        return True

    # Substring extraction
    if offset is not None:
        start, end = offset
        if end <= len(dept_id):
            substr = dept_id[start:end]
            norm_substr = substr.lower()
            if norm_substr in gl_numbers or _strip_zeros(norm_substr) in gl_stripped:
                return True

    return False


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_checks(
    file_entries: dict[str, dict],
) -> dict:
    """
    Run C4 check: Payroll <-> GL department to cost center.

    Returns a finding dict.
    """
    payroll_entry = None
    gl_entry = None

    for fname, entry in file_entries.items():
        source = entry.get("source", "")
        if source == "payroll" and entry.get("df") is not None:
            payroll_entry = entry
        elif source == "gl" and entry.get("df") is not None:
            gl_entry = entry

    if payroll_entry is None:
        return {
            "check": "C4",
            "severity": "INFO",
            "message": "Skipped — payroll file not present",
            "skipped": True,
        }
    if gl_entry is None:
        return {
            "check": "C4",
            "severity": "INFO",
            "message": "Skipped — GL file not present",
            "skipped": True,
        }

    payroll_df = payroll_entry["df"]
    payroll_maps = payroll_entry.get("column_mappings", [])
    gl_df = gl_entry["df"]
    gl_maps = gl_entry.get("column_mappings", [])

    # Resolve payroll dept ID and name columns
    pay_dept_id_raw = resolve_column(payroll_maps, _PAYROLL_DEPT_ID)
    pay_dept_name_raw = resolve_column(payroll_maps, _PAYROLL_DEPT_NAME)
    gl_cc_num_raw = resolve_column(gl_maps, _GL_CC_NUMBER)
    gl_cc_name_raw = resolve_column(gl_maps, _GL_CC_NAME)

    if not pay_dept_id_raw or pay_dept_id_raw not in payroll_df.columns:
        return {
            "check": "C4",
            "severity": "INFO",
            "message": f"C4: Payroll DepartmentId column not found (resolved: {pay_dept_id_raw})",
            "skipped": False,
            "files_compared": "payroll + gl",
        }

    # Build GL reference sets
    gl_numbers: set[str] = set()
    gl_names: set[str] = set()

    if gl_cc_num_raw and gl_cc_num_raw in gl_df.columns:
        gl_numbers = set(
            gl_df[gl_cc_num_raw]
            .dropna().astype(str).str.strip()
            .replace("", pd.NA).dropna()
            .str.lower().unique()
        )
    if gl_cc_name_raw and gl_cc_name_raw in gl_df.columns:
        gl_names = set(
            gl_df[gl_cc_name_raw]
            .dropna().astype(str).str.strip()
            .replace("", pd.NA).dropna()
            .str.lower().unique()
        )

    if not gl_numbers and not gl_names:
        return {
            "check": "C4",
            "severity": "INFO",
            "message": "C4: GL file has no CostCenterNumber or CostCenterName data",
            "skipped": False,
            "files_compared": "payroll + gl",
        }

    # Get distinct payroll dept IDs with row counts
    payroll_dept_work = payroll_df[[pay_dept_id_raw]].copy()
    if pay_dept_name_raw and pay_dept_name_raw in payroll_df.columns:
        payroll_dept_work["_dept_name"] = payroll_df[pay_dept_name_raw].astype(str).str.strip()
    else:
        payroll_dept_work["_dept_name"] = ""

    payroll_dept_work["_dept_id"] = payroll_dept_work[pay_dept_id_raw].astype(str).str.strip()
    payroll_dept_work = payroll_dept_work[payroll_dept_work["_dept_id"].replace("nan", "").str.len() > 0]

    dept_counts = (
        payroll_dept_work.groupby("_dept_id")["_dept_name"]
        .agg(row_count="count", dept_name=lambda x: x.mode()[0] if len(x) > 0 else "")
        .reset_index()
    )
    dept_counts.columns = ["dept_id", "row_count", "dept_name"]

    distinct_dept_ids = dept_counts["dept_id"].tolist()
    dept_distinct = len(distinct_dept_ids)

    # Attempt auto-extraction if IDs are longer than GL cost center numbers
    avg_pay_len = sum(len(d) for d in distinct_dept_ids) / max(len(distinct_dept_ids), 1)
    avg_gl_len = sum(len(n) for n in gl_numbers) / max(len(gl_numbers), 1) if gl_numbers else 0

    auto_extracted_offset = None
    extraction_log = None

    if avg_pay_len > avg_gl_len + 1 and gl_numbers:
        # Try first with exact match — if already good, skip extraction
        quick_match = sum(1 for d in distinct_dept_ids if _match_dept_to_gl(d, gl_numbers, gl_names, None))
        if quick_match / max(dept_distinct, 1) < _AUTO_EXTRACT_THRESHOLD:
            offset = _find_extraction_offset(distinct_dept_ids, gl_numbers)
            if offset is not None:
                auto_extracted_offset = list(offset)
                extraction_log = f"AUTO-EXTRACTED: middle chars [{offset[0]}:{offset[1]}] match GL cost center format"
                print(f"  C4: {extraction_log}")

    # Match each dept ID
    matched_depts = []
    unmatched_depts = []

    for _, row in dept_counts.iterrows():
        dept_id = row["dept_id"]
        row_count = int(row["row_count"])
        dept_name = str(row["dept_name"])

        if _match_dept_to_gl(dept_id, gl_numbers, gl_names, tuple(auto_extracted_offset) if auto_extracted_offset else None):
            matched_depts.append({"dept_id": dept_id, "dept_name": dept_name, "row_count": row_count})
        else:
            unmatched_depts.append({"dept_id": dept_id, "dept_name": dept_name, "row_count": row_count})

    matched_dept_count = len(matched_depts)
    unmatched_dept_count = len(unmatched_depts)
    match_pct = matched_dept_count / dept_distinct * 100 if dept_distinct > 0 else 100.0

    # Severity
    high_unmatched = [d for d in unmatched_depts if d["row_count"] > 100]
    medium_unmatched = [d for d in unmatched_depts if d["row_count"] <= 100]

    if high_unmatched:
        severity = "HIGH"
        msg = (
            f"C4 Payroll <-> GL: {match_pct:.1f}% of {dept_distinct} payroll departments match GL cost centers; "
            f"{len(high_unmatched)} department(s) with >100 rows are unmatched"
        )
    elif medium_unmatched:
        severity = "MEDIUM"
        msg = (
            f"C4 Payroll <-> GL: {match_pct:.1f}% of {dept_distinct} payroll departments match GL cost centers; "
            f"{unmatched_dept_count} small department(s) unmatched (≤100 rows each)"
        )
    else:
        severity = "PASS"
        msg = f"C4 Payroll <-> GL: all {dept_distinct} payroll departments matched to GL cost centers"

    if extraction_log:
        msg += f" [{extraction_log}]"

    return {
        "check": "C4",
        "severity": severity,
        "message": msg,
        "files_compared": "payroll + gl",
        "skipped": False,
        "dept_distinct": dept_distinct,
        "matched_dept_count": matched_dept_count,
        "unmatched_dept_count": unmatched_dept_count,
        "match_pct": round(match_pct, 2),
        "auto_extracted_offset": auto_extracted_offset,
        "extraction_log": extraction_log,
        "unmatched_sample": sorted(unmatched_depts, key=lambda d: d["row_count"], reverse=True)[:20],
    }
