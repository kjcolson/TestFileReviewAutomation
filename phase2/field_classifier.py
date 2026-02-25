"""
phase2/field_classifier.py

Classifies each Phase 1 mapped column by its requirement level
(Required / Recommended / Optional / Unclassified) using the
TEMPLATE_TO_STAGING and FIELD_REQUIREMENTS constants.

This enriched list is used downstream by datatype_checker.py to
escalate severity when type issues affect Required fields.
"""

from __future__ import annotations

from typing import Any


def classify(
    column_mappings: list[dict[str, Any]],
    source: str,
) -> list[dict[str, Any]]:
    """
    Return a copy of column_mappings with a "RequirementLevel" key added
    to every entry.

    Values: "Required" | "Recommended" | "ConditionalRequired" | "Optional" | "Unclassified" | "UNMAPPED"
    """
    staging_to_level = _build_staging_to_level(source)

    result: list[dict[str, Any]] = []
    for rec in column_mappings:
        r = dict(rec)
        if r.get("confidence", "") == "UNMAPPED":
            r["RequirementLevel"] = "UNMAPPED"
        else:
            stg_cols = r.get("staging_cols") or (
                [r["staging_col"]] if r.get("staging_col") else []
            )
            level = _resolve_level(stg_cols, staging_to_level)
            r["RequirementLevel"] = level
        result.append(r)
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_staging_to_level(source: str) -> dict[str, str]:
    """
    Build a reverse lookup: staging_column → requirement level.

    If a staging column appears for multiple template fields at different
    levels, the highest level wins (Required > Recommended > Optional).
    """
    from shared.constants import FIELD_REQUIREMENTS, TEMPLATE_TO_STAGING

    _PRIORITY = {"Required": 3, "Recommended": 2, "ConditionalRequired": 2, "Optional": 1}

    requirements = FIELD_REQUIREMENTS.get(source, {})
    staging_to_level: dict[str, str] = {}

    level_map = {
        "required":             "Required",
        "recommended":          "Recommended",
        "optional":             "Optional",
        "conditional_required": "ConditionalRequired",
    }

    for key_level, display_level in level_map.items():
        for field_name in requirements.get(key_level, []):
            staging_target = TEMPLATE_TO_STAGING.get((source, field_name))
            if staging_target is None or staging_target == "_raw_check":
                continue
            targets = staging_target if isinstance(staging_target, list) else [staging_target]
            for stg_col in targets:
                existing = staging_to_level.get(stg_col)
                if existing is None:
                    staging_to_level[stg_col] = display_level
                elif _PRIORITY[display_level] > _PRIORITY[existing]:
                    staging_to_level[stg_col] = display_level

    return staging_to_level


def _resolve_level(
    stg_cols: list[str],
    staging_to_level: dict[str, str],
) -> str:
    """Return the highest requirement level across all staging columns."""
    _PRIORITY = {"Required": 3, "Recommended": 2, "ConditionalRequired": 2, "Optional": 1, "Unclassified": 0}
    best = "Unclassified"
    for col in stg_cols:
        level = staging_to_level.get(col, "Unclassified")
        if _PRIORITY[level] > _PRIORITY[best]:
            best = level
    return best
