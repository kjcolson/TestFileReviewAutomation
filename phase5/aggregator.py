"""
phase5/aggregator.py

Loads all 4 phase JSON manifests and normalizes them into a unified
source-centric model for reporting.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Source group mapping
# ---------------------------------------------------------------------------

_SOURCE_GROUP = {
    "billing_combined": "billing",
    "billing_charges": "billing",
    "billing_transactions": "billing",
    "scheduling": "scheduling",
    "payroll": "payroll",
    "gl": "gl",
    "quality": "quality",
    "patient_satisfaction": "patient_satisfaction",
}

_SOURCE_DISPLAY = {
    "billing": "Billing",
    "scheduling": "Scheduling",
    "payroll": "Payroll",
    "gl": "GL",
    "quality": "Quality",
    "patient_satisfaction": "Patient Satisfaction",
}

_PHASE4_CHECK_SOURCES = {
    "C0": ["billing"],
    "C1": ["billing", "gl"],
    "C2": ["billing", "payroll"],
    "C3": ["billing", "scheduling"],
    "C4": ["payroll", "gl"],
    "C5": ["scheduling", "gl"],
}


# ---------------------------------------------------------------------------
# JSON loading
# ---------------------------------------------------------------------------

def load_all_phases(output_dir: Path) -> dict[str, dict]:
    """Read all four phase JSON files. Returns dict keyed by phase name."""
    result = {}
    for n in (1, 2, 3, 4):
        path = output_dir / f"phase{n}_findings.json"
        with open(path, encoding="utf-8") as fh:
            result[f"phase{n}"] = json.load(fh)
    return result


# ---------------------------------------------------------------------------
# Issue ID generation
# ---------------------------------------------------------------------------

_ID_PREFIXES = {
    "billing": "B",
    "scheduling": "S",
    "payroll": "P",
    "gl": "G",
    "quality": "Q",
    "patient_satisfaction": "PS",
    "cross_source": "X",
}

_id_counters: dict[str, int] = {}


def _next_id(source_group: str) -> str:
    prefix = _ID_PREFIXES.get(source_group, "U")
    _id_counters[prefix] = _id_counters.get(prefix, 0) + 1
    return f"{prefix}-{_id_counters[prefix]:03d}"


# ---------------------------------------------------------------------------
# Unified model builder
# ---------------------------------------------------------------------------

def build_unified_model(phase_data: dict[str, dict]) -> dict:
    """Transform four phase dicts into a source-centric unified model."""
    # Reset counters
    _id_counters.clear()

    p1 = phase_data["phase1"]
    p2 = phase_data["phase2"]
    p3 = phase_data["phase3"]
    p4 = phase_data["phase4"]

    # Extract metadata
    billing_format_raw = p1.get("billing_format", "unknown")
    if isinstance(billing_format_raw, dict):
        billing_format = billing_format_raw.get("format", "unknown")
    else:
        billing_format = str(billing_format_raw) if billing_format_raw else "unknown"

    unified: dict[str, Any] = {
        "client": p1.get("client", ""),
        "round": p1.get("round", ""),
        "test_month": p1.get("test_month", ""),
        "month_aligned": p1.get("month_aligned", True),
        "billing_format": billing_format,
        "billing_notes": p1.get("billing_notes", ""),
        "phase4_skipped": bool(p4.get("skipped", False)),
        "sources": {},
        "cross_source_issues": [],
        "phase_metadata": {
            "phase1_date": p1.get("date_run", ""),
            "phase2_date": p2.get("date_run", ""),
            "phase3_date": p3.get("date_run", ""),
            "phase4_date": p4.get("date_run", ""),
        },
    }

    # Build source entries from Phase 1 file inventory
    for fname, fdata in p1.get("files", {}).items():
        source = fdata.get("source", "unknown")
        group = _SOURCE_GROUP.get(source, source)
        if group not in unified["sources"]:
            unified["sources"][group] = {
                "display_name": _SOURCE_DISPLAY.get(group, group.title()),
                "files": [],
                "source_type": source,
                "row_count": 0,
                "phase2_compatible": "N/A",
                "date_range": {"min": None, "max": None, "date_column": None, "note": ""},
                "issues": [],
            }
        entry = unified["sources"][group]
        entry["files"].append(fname)
        entry["row_count"] += fdata.get("row_count", 0)

        # Merge date range from Phase 1
        dr = fdata.get("date_range", {})
        if dr.get("min_date") or dr.get("max_date"):
            existing = entry["date_range"]
            if not existing["date_column"]:
                existing["date_column"] = dr.get("filter_field")
            if not existing["note"] and dr.get("note"):
                existing["note"] = dr["note"]
            file_min = dr.get("min_date")
            file_max = dr.get("max_date")
            if file_min and (existing["min"] is None or file_min < existing["min"]):
                existing["min"] = file_min
            if file_max and (existing["max"] is None or file_max > existing["max"]):
                existing["max"] = file_max

    # Phase 1: month misalignment issue
    if not p1.get("month_aligned", True):
        for group in unified["sources"]:
            unified["sources"][group]["issues"].append({
                "id": _next_id(group),
                "phase": 1,
                "check": "month_alignment",
                "severity": "HIGH",
                "staging_column": None,
                "raw_column": None,
                "template_field": None,
                "message": "Test month misalignment — not all files cover the same month",
                "affected_rows": "All",
                "requirement_level": None,
                "dedupe_key": f"{group}|month_alignment|all",
            })
            break  # Only add once (to first source)

    # Phase 2: schema + datatype findings
    _aggregate_phase2(p2, unified)

    # Phase 3: universal + source-specific findings
    _aggregate_phase3(p3, unified)

    # Phase 4: cross-source findings
    _aggregate_phase4(p4, unified)

    # Compute severity counts per source
    for group, sdata in unified["sources"].items():
        sdata["severity_counts"] = _count_severities(sdata["issues"])

    return unified


# ---------------------------------------------------------------------------
# Phase 2 aggregation
# ---------------------------------------------------------------------------

def _aggregate_phase2(p2: dict, unified: dict) -> None:
    for fname, fdata in p2.get("files", {}).items():
        source = fdata.get("source", "unknown")
        group = _SOURCE_GROUP.get(source, source)
        if group not in unified["sources"]:
            continue

        entry = unified["sources"][group]
        entry["phase2_compatible"] = fdata.get("compatible", "N/A")

        # Schema findings — only MISSING fields (PRESENT are not issues)
        for sf in fdata.get("schema_findings", []):
            if sf.get("status") != "MISSING":
                continue
            sev = sf.get("severity")
            if not sev:
                continue
            entry["issues"].append({
                "id": _next_id(group),
                "phase": 2,
                "check": "schema_missing",
                "severity": sev,
                "staging_column": sf.get("staging_column"),
                "raw_column": None,
                "template_field": sf.get("template_field"),
                "message": f"Missing {sf.get('requirement_level', '')} field '{sf.get('template_field', '')}'",
                "affected_rows": "All",
                "requirement_level": sf.get("requirement_level"),
                "dedupe_key": f"{group}|schema_missing|{sf.get('staging_column', '')}",
            })

        # Datatype findings with severity
        for dt in fdata.get("datatype_findings", []):
            sev = dt.get("severity")
            if not sev:
                continue
            raw_col = dt.get("raw_column", "")
            staging_col = dt.get("staging_column", "")
            notes = dt.get("notes", "")
            invalid_count = dt.get("invalid_count", 0)
            entry["issues"].append({
                "id": _next_id(group),
                "phase": 2,
                "check": "datatype",
                "severity": sev,
                "staging_column": staging_col,
                "raw_column": raw_col,
                "template_field": None,
                "message": f"Data type issue in '{raw_col}': {notes}" + (f" ({invalid_count} invalid)" if invalid_count else ""),
                "affected_rows": str(invalid_count) if invalid_count else "N/A",
                "requirement_level": dt.get("requirement_level"),
                "example_values": dt.get("domain_invalid_sample", []),
                "dedupe_key": f"{group}|datatype|{staging_col}",
            })


# ---------------------------------------------------------------------------
# Phase 3 aggregation
# ---------------------------------------------------------------------------

def _aggregate_phase3(p3: dict, unified: dict) -> None:
    for fname, fdata in p3.get("files", {}).items():
        source = fdata.get("source", "unknown")
        group = _SOURCE_GROUP.get(source, source)
        if group not in unified["sources"]:
            continue

        entry = unified["sources"][group]
        record_count = fdata.get("record_count", 0)

        # Universal findings
        for uf in fdata.get("universal_findings", []):
            sev = uf.get("severity")
            if not sev:
                continue
            check = uf.get("check", "")
            raw_col = uf.get("raw_column", "")
            staging_col = uf.get("staging_column", "")
            entry["issues"].append({
                "id": _next_id(group),
                "phase": 3,
                "check": check,
                "severity": sev,
                "staging_column": staging_col,
                "raw_column": raw_col,
                "template_field": None,
                "message": uf.get("message", ""),
                "affected_rows": _format_affected(uf, record_count),
                "requirement_level": uf.get("requirement_level"),
                "example_values": _format_sample_values(uf.get("sample_values")),
                "sample_rows": uf.get("sample_rows", []),
                "missing_pct": uf.get("missing_pct"),
                "total_missing": uf.get("total_missing"),
                "dedupe_key": f"{group}|{check}|{staging_col or raw_col}",
            })

        # Source-specific findings
        for sf in fdata.get("source_specific_findings", []):
            sev = sf.get("severity")
            if not sev:
                continue
            check = sf.get("check", "")
            raw_col = sf.get("raw_column", "")
            staging_col = sf.get("staging_column", "")
            entry["issues"].append({
                "id": _next_id(group),
                "phase": 3,
                "check": check,
                "severity": sev,
                "staging_column": staging_col,
                "raw_column": raw_col,
                "template_field": None,
                "message": sf.get("message", ""),
                "affected_rows": _format_affected(sf, record_count),
                "requirement_level": sf.get("requirement_level"),
                "example_values": _format_sample_values(sf.get("sample_values")),
                "sample_rows": sf.get("sample_rows", []),
                "dedupe_key": f"{group}|{check}|{staging_col or raw_col or check}",
            })


def _format_affected(finding: dict, record_count: int) -> str:
    """Build an affected-rows string from a Phase 3 finding."""
    pct = finding.get("missing_pct")
    total = finding.get("total_missing")
    if pct is not None and pct >= 99.0:
        return "All"
    if total is not None:
        return f"{total:,}"
    affected = finding.get("affected_count") or finding.get("duplicate_row_count")
    if affected:
        return f"{affected:,}"
    return "N/A"


# ---------------------------------------------------------------------------
# Phase 4 aggregation
# ---------------------------------------------------------------------------

def _aggregate_phase4(p4: dict, unified: dict) -> None:
    findings = p4.get("findings", {})
    for check_id, finding in findings.items():
        if finding.get("skipped"):
            continue

        sources = _PHASE4_CHECK_SOURCES.get(check_id, [])
        leaf_findings = _flatten_phase4_finding(finding, check_id)

        for lf in leaf_findings:
            sev = lf.get("severity", "INFO")
            if sev == "PASS":
                sev = "INFO"

            issue = {
                "id": _next_id("cross_source"),
                "phase": 4,
                "check": lf.get("check", check_id),
                "severity": sev,
                "staging_column": None,
                "raw_column": None,
                "template_field": None,
                "message": lf.get("message", ""),
                "affected_rows": "N/A",
                "requirement_level": None,
                "sources_involved": sources,
                "files_compared": finding.get("files_compared", ""),
                "dedupe_key": f"cross|{lf.get('check', check_id)}|{lf.get('billing_column', lf.get('scheduling_column', 'top'))}",
            }
            unified["cross_source_issues"].append(issue)


def _flatten_phase4_finding(finding: dict, check_id: str) -> list[dict]:
    """Extract leaf-level findings from a Phase 4 finding structure."""
    leaves: list[dict] = []

    # sub_checks: C0 (C0a/C0b), C3 (C3a/C3b/C3c)
    sub_checks = finding.get("sub_checks", {})
    if isinstance(sub_checks, dict) and sub_checks:
        for sub_key, sub_val in sub_checks.items():
            if isinstance(sub_val, list):
                leaves.extend(sub_val)
            elif isinstance(sub_val, dict):
                leaves.append(sub_val)
        return leaves

    # findings list: C1, C5
    findings_list = finding.get("findings", [])
    if findings_list:
        return findings_list

    # Flat finding: C2, C4
    if finding.get("severity") and not finding.get("skipped"):
        return [finding]

    return []


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_sample_values(sample) -> list[str]:
    """Normalize sample_values from any Phase 3 format into a flat string list."""
    if not sample:
        return []
    if isinstance(sample, dict):
        return [f"{v} ({c:,}\u00d7)" for v, c in list(sample.items())[:5]]
    if isinstance(sample, list):
        return [str(v) for v in sample[:5]]
    return []


def _count_severities(issues: list[dict]) -> dict[str, int]:
    counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    for issue in issues:
        if issue.get("deduplicated"):
            continue
        sev = issue.get("severity", "INFO")
        if sev in counts:
            counts[sev] += 1
    return counts
