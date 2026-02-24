"""
phase5/missing_sources.py

Identifies expected but absent data sources.
"""

from __future__ import annotations

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

# Core sources that should be present (patient_satisfaction is optional)
_EXPECTED_CORE = ["billing", "scheduling", "payroll", "gl", "quality"]


def detect(phase1: dict) -> list[str]:
    """Return list of expected core source names not present in Phase 1 files."""
    present = set()
    for fname, fdata in phase1.get("files", {}).items():
        source = fdata.get("source", "")
        group = _SOURCE_GROUP.get(source, source)
        present.add(group)

    return [s for s in _EXPECTED_CORE if s not in present]
