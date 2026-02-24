"""
phase5/checklist.py

Auto-generates a resubmission checklist from CRITICAL and HIGH findings.
"""

from __future__ import annotations

_SOURCE_DISPLAY = {
    "billing": "Billing",
    "scheduling": "Scheduling",
    "payroll": "Payroll",
    "gl": "GL",
    "quality": "Quality",
    "patient_satisfaction": "Patient Satisfaction",
    "cross_source": "Cross-Source",
}

# Static reminders always appended
_STATIC_REMINDERS = [
    "All test files must be pipe-delimited (.txt) with headers and no footers",
    "All core test files must cover the same single month of data",
    "Test file data should be reconciled against an internal report before resubmission",
    "See the PIVOT Data Extract Template for required field names and formats",
]


def generate(
    unified: dict,
    client_issues: list[dict],
    missing_sources: list[str],
) -> list[dict]:
    """
    Generate a prioritized resubmission checklist.

    Returns list of dicts with: priority, source, source_display, item, finding_ids
    """
    items: list[dict] = []

    # MUST FIX — from CRITICAL issues
    for issue in client_issues:
        if issue["severity"] != "CRITICAL":
            continue
        item_text = _checklist_item(issue)
        items.append({
            "priority": "MUST FIX",
            "source": issue["source"],
            "source_display": issue["source_display"],
            "item": item_text,
            "finding_ids": [issue["id"]],
        })

    # SHOULD FIX — from HIGH issues
    for issue in client_issues:
        if issue["severity"] != "HIGH":
            continue
        item_text = _checklist_item(issue)
        items.append({
            "priority": "SHOULD FIX",
            "source": issue["source"],
            "source_display": issue["source_display"],
            "item": item_text,
            "finding_ids": [issue["id"]],
        })

    # Missing sources
    for src in missing_sources:
        display = _SOURCE_DISPLAY.get(src, src.title())
        items.append({
            "priority": "MUST FIX",
            "source": src,
            "source_display": display,
            "item": f"Submit {display} test file for the same month as other test files",
            "finding_ids": [],
        })

    # Static reminders
    for reminder in _STATIC_REMINDERS:
        items.append({
            "priority": "REMINDER",
            "source": "general",
            "source_display": "General",
            "item": reminder,
            "finding_ids": [],
        })

    return items


def _checklist_item(issue: dict) -> str:
    """Convert a client issue into a checklist action item."""
    phase = issue.get("phase", 0)
    check = issue.get("check", "")
    field = issue.get("field", "")
    source_display = issue.get("source_display", "")
    desc = issue.get("description", "")

    if phase == 2 and check == "schema_missing":
        req = "required" if issue["severity"] == "CRITICAL" else "recommended"
        if field:
            return f"Add the '{field}' column to the {source_display} file ({req})"
        return f"Add missing {req} field to the {source_display} file"

    if phase == 3 and check == "null_blank":
        if field:
            return f"Populate the '{field}' column in the {source_display} file (currently has missing values)"
        return f"Fix missing values in {source_display} file"

    if phase == 4:
        return f"Verify: {desc}"

    # Generic fallback
    if field:
        return f"Fix '{field}' in {source_display}: {desc}"
    return f"Fix in {source_display}: {desc}"
