"""
phase5/issue_formatter.py

Transforms findings into client-ready issue lines sorted by severity.
"""

from __future__ import annotations

_SEV_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFO": 4}

_SOURCE_DISPLAY = {
    "billing": "Billing",
    "scheduling": "Scheduling",
    "payroll": "Payroll",
    "gl": "GL",
    "quality": "Quality",
    "patient_satisfaction": "Patient Satisfaction",
}


def format_all_issues(unified: dict) -> list[dict]:
    """
    Build a sorted list of client-ready issue dicts from the unified model.

    Each returned dict has:
        id, severity, source, source_display, phase, check, field,
        description, affected_rows, priority, line
    """
    issues: list[dict] = []

    # Source-level issues
    for group, sdata in unified.get("sources", {}).items():
        display = sdata.get("display_name", _SOURCE_DISPLAY.get(group, group.title()))
        for issue in sdata.get("issues", []):
            if issue.get("deduplicated"):
                continue
            sev = issue.get("severity", "INFO")
            if sev == "PASS":
                continue
            field = issue.get("template_field") or issue.get("raw_column") or issue.get("staging_column") or ""
            desc = _build_description(issue, sdata.get("row_count", 0))
            affected = issue.get("affected_rows", "N/A")
            priority = "MUST FIX" if sev == "CRITICAL" else ("SHOULD FIX" if sev == "HIGH" else "REVIEW")

            line = f"[{sev}] {display} — {desc}"
            if affected and affected != "N/A":
                line += f" — {affected} rows" if affected != "All" else " — Affects all rows"

            issues.append({
                "id": issue.get("id", ""),
                "severity": sev,
                "source": group,
                "source_display": display,
                "phase": issue.get("phase", 0),
                "check": issue.get("check", ""),
                "field": field,
                "description": desc,
                "affected_rows": affected,
                "priority": priority,
                "line": line,
            })

    # Cross-source issues (Phase 4)
    for issue in unified.get("cross_source_issues", []):
        if issue.get("deduplicated"):
            continue
        sev = issue.get("severity", "INFO")
        if sev == "PASS" or sev == "INFO":
            continue
        sources_inv = issue.get("sources_involved", [])
        display = " <-> ".join(
            _SOURCE_DISPLAY.get(s, s.title()) for s in sources_inv
        ) if sources_inv else "Cross-Source"
        desc = issue.get("message", "")
        priority = "MUST FIX" if sev == "CRITICAL" else ("SHOULD FIX" if sev == "HIGH" else "REVIEW")

        line = f"[{sev}] {display} — {desc}"

        issues.append({
            "id": issue.get("id", ""),
            "severity": sev,
            "source": "cross_source",
            "source_display": display,
            "phase": 4,
            "check": issue.get("check", ""),
            "field": "",
            "description": desc,
            "affected_rows": "N/A",
            "priority": priority,
            "line": line,
        })

    # Sort: severity first, then source
    issues.sort(key=lambda i: (
        _SEV_ORDER.get(i["severity"], 9),
        i["source"],
        i["phase"],
    ))

    return issues


def _build_description(issue: dict, row_count: int) -> str:
    """Build a client-friendly description from an issue dict."""
    phase = issue.get("phase", 0)
    check = issue.get("check", "")
    msg = issue.get("message", "")

    if phase == 2 and check == "schema_missing":
        field = issue.get("template_field", "")
        req = issue.get("requirement_level", "")
        if req == "required":
            return f"Missing required field '{field}'"
        elif req == "recommended":
            return f"Missing recommended field '{field}'"
        return f"Missing field '{field}'"

    if phase == 2 and check == "datatype":
        return msg

    # Phase 3 — use message directly (already well-formatted)
    if phase == 3:
        return msg

    return msg
