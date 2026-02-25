"""
phase2/schema_validator.py

Checks whether each file contains the Required, Recommended, and Optional
template fields for its data source.  Uses TEMPLATE_TO_STAGING to translate
template field names into staging column names, then checks those against
the set of columns Phase 1 successfully mapped.

For sources without staging tables (quality, patient_satisfaction), column
headers are compared directly against template field names using normalization.
"""

from __future__ import annotations

import re
from typing import Any


# Sources that have no staging table — schema check uses raw column names
_NO_STAGING_SOURCES = {"quality", "patient_satisfaction"}


def validate(
    file_data: dict[str, Any],
    source: str,
) -> dict[str, Any]:
    """
    Run schema validation for one file.

    Parameters
    ----------
    file_data : dict
        Entry from shared.loader.load_files() — must contain
        "column_mappings" and optionally "df".
    source : str
        Source name (e.g. "billing_charges").

    Returns
    -------
    dict with keys "schema_findings" (list) and "summary" (dict).
    """
    from shared.constants import FIELD_REQUIREMENTS, TEMPLATE_TO_STAGING

    requirements = FIELD_REQUIREMENTS.get(source)
    if requirements is None:
        return {"schema_findings": [], "summary": _empty_summary()}

    column_mappings: list[dict] = file_data.get("column_mappings", [])
    df = file_data.get("df")

    # Build covered set
    if source in _NO_STAGING_SOURCES:
        covered = _build_raw_covered(column_mappings, df)
    else:
        covered = _build_staging_covered(column_mappings)

    findings: list[dict[str, Any]] = []
    summary = {
        "required_total": 0, "required_present": 0, "required_missing": 0,
        "recommended_total": 0, "recommended_present": 0, "recommended_missing": 0,
        "optional_total": 0, "optional_present": 0, "optional_missing": 0,
    }

    for level in ("required", "recommended", "optional", "conditional_required"):
        fields = requirements.get(level, [])
        # conditional_required fields count toward recommended totals in the summary
        summary_key = "recommended" if level == "conditional_required" else level
        for field_name in fields:
            summary[f"{summary_key}_total"] += 1
            finding = _check_field(field_name, level, source, covered, column_mappings)
            if finding["status"] == "PRESENT":
                summary[f"{summary_key}_present"] += 1
            else:
                summary[f"{summary_key}_missing"] += 1
            findings.append(finding)

    # Special handling for conditional / compound fields
    _apply_special_handling(findings, source, covered)

    return {"schema_findings": findings, "summary": summary}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_staging_covered(column_mappings: list[dict]) -> set[str]:
    """Return staging columns covered by non-UNMAPPED Phase 1 mappings."""
    covered: set[str] = set()
    for r in column_mappings:
        if r.get("confidence", "") == "UNMAPPED":
            continue
        for sc in r.get("staging_cols", []):
            covered.add(sc)
        if r.get("staging_col"):
            covered.add(r["staging_col"])
    return covered


def _build_raw_covered(
    column_mappings: list[dict],
    df: Any,
) -> set[str]:
    """
    For no-staging sources, build a normalized set of raw column names
    found in the file so we can compare against normalized template field names.
    """
    raw_cols: set[str] = set()
    # From Phase 1 mappings
    for r in column_mappings:
        raw_cols.add(_normalize(r.get("raw_col", "")))
    # From DataFrame headers (if available)
    if df is not None:
        for col in df.columns:
            raw_cols.add(_normalize(col))
    return raw_cols


def _check_field(
    field_name: str,
    level: str,
    source: str,
    covered: set[str],
    column_mappings: list[dict],
) -> dict[str, Any]:
    from shared.constants import TEMPLATE_TO_STAGING

    staging_target = TEMPLATE_TO_STAGING.get((source, field_name))

    # Sentinel: no staging column exists → skip check, treat as N/A
    if staging_target is None:
        return {
            "template_field":    field_name,
            "staging_column":    None,
            "requirement_level": level,
            "status":            "N/A",
            "severity":          None,
            "raw_column_matched": None,
            "confidence":        None,
            "notes":             "No staging column — field not checked",
        }

    # Raw-column check (quality / patient_satisfaction)
    if staging_target == "_raw_check":
        norm_field = _normalize(field_name)
        present = norm_field in covered
        matched_raw, confidence = _find_raw_match(field_name, column_mappings)
        return _make_finding(
            field_name, "_raw_check", level, present, matched_raw, confidence
        )

    # Standard staging column check
    targets = staging_target if isinstance(staging_target, list) else [staging_target]
    present = any(t in covered for t in targets)
    matched_raw, confidence = _find_staging_match(targets, column_mappings)
    primary = targets[0]
    return _make_finding(field_name, primary, level, present, matched_raw, confidence)


def _make_finding(
    field_name: str,
    staging_col: str | None,
    level: str,
    present: bool,
    matched_raw: str | None,
    confidence: str | None,
) -> dict[str, Any]:
    severity_map = {
        "required":             "CRITICAL",
        "recommended":          "HIGH",
        "optional":             "INFO",
        "conditional_required": "HIGH",   # column must exist; missing is HIGH not CRITICAL
    }
    return {
        "template_field":    field_name,
        "staging_column":    staging_col,
        "requirement_level": level,
        "status":            "PRESENT" if present else "MISSING",
        "severity":          None if present else severity_map.get(level),
        "raw_column_matched": matched_raw,
        "confidence":        confidence,
        "notes":             "",
    }


def _find_staging_match(
    staging_cols: list[str],
    column_mappings: list[dict],
) -> tuple[str | None, str | None]:
    """Return (raw_col, confidence) of the first mapping that covers any target."""
    targets = set(staging_cols)
    for r in column_mappings:
        if r.get("confidence", "") == "UNMAPPED":
            continue
        if any(sc in targets for sc in r.get("staging_cols", [])):
            return r.get("raw_col"), r.get("confidence")
        if r.get("staging_col") in targets:
            return r.get("raw_col"), r.get("confidence")
    return None, None


def _find_raw_match(
    field_name: str,
    column_mappings: list[dict],
) -> tuple[str | None, str | None]:
    """Return (raw_col, confidence) whose normalized name matches the field."""
    norm_field = _normalize(field_name)
    for r in column_mappings:
        if _normalize(r.get("raw_col", "")) == norm_field:
            return r.get("raw_col"), "EXACT"
    return None, None


def _apply_special_handling(
    findings: list[dict[str, Any]],
    source: str,
    covered: set[str],
) -> None:
    """
    Apply conditional / compound field rules by mutating findings in-place:

    1. Cost Center (billing): CRITICAL only if Cost Center AND all three
       org fields (Practice Name, Billing Location Name, Department Name)
       are all missing.

    2. Charge ID / Invoice Number (billing): CRITICAL only if NEITHER is mapped.

    3. ICD-10 5th–25th (billing): Flag CRITICAL only if zero ICD codes
       beyond PrimaryIcdCode are covered; INFO if covered via SecondaryIcdCodes.
    """
    if source in ("billing_combined", "billing_charges"):
        _handle_cost_center_billing(findings, covered)
        _handle_charge_id_invoice(findings, covered)
        _handle_icd_overflow(findings, covered)

    if source == "billing_transactions":
        _handle_charge_id_invoice(findings, covered)


def _handle_cost_center_billing(
    findings: list[dict[str, Any]], covered: set[str]
) -> None:
    cc_finding = _find_finding(findings, "Cost Center")
    if cc_finding is None or cc_finding["status"] == "PRESENT":
        return

    # Cost Center is missing — check if any org field is present
    org_staging = {
        "Practice Name":          ["BillPracticeName", "BillPracticeId"],
        "Billing Location Name":  ["BillLocationName", "BillLocationId"],
        "Department Name":        ["BillDepartmentName", "BillDepartmentId"],
    }
    org_present = any(
        any(sc in covered for sc in cols)
        for cols in org_staging.values()
    )

    if org_present:
        cc_finding["severity"] = "HIGH"
        cc_finding["notes"] = (
            "Cost Center missing — at least one org field is present; "
            "GL crosswalk will be required"
        )
    else:
        cc_finding["severity"] = "CRITICAL"
        cc_finding["notes"] = (
            "Cost Center missing AND no org fields (Practice, Location, Department) "
            "found — cannot link to GL"
        )


def _handle_charge_id_invoice(
    findings: list[dict[str, Any]], covered: set[str]
) -> None:
    ci_finding  = _find_finding(findings, "Charge ID")
    inv_finding = _find_finding(findings, "Invoice Number / Encounter ID")

    if ci_finding is None or inv_finding is None:
        return

    # If at least one is present, both are satisfied
    if ci_finding["status"] == "PRESENT" or inv_finding["status"] == "PRESENT":
        if ci_finding["status"] == "MISSING":
            ci_finding["severity"] = "HIGH"
            ci_finding["notes"] = "Charge ID missing but Invoice Number is present"
        if inv_finding["status"] == "MISSING":
            inv_finding["severity"] = "HIGH"
            inv_finding["notes"] = "Invoice Number missing but Charge ID is present"
    # If both missing → both stay CRITICAL (already set by default)


def _handle_icd_overflow(
    findings: list[dict[str, Any]], covered: set[str]
) -> None:
    """3rd, 4th, 5th–25th ICD findings: downgrade to INFO if SecondaryIcdCodes covered."""
    secondary_covered = "SecondaryIcdCodes" in covered
    icd_overflow_fields = [
        "Third ICD-10 CM Code", "Fourth ICD-10 CM Code",
        "5th through 25th ICD-10 CM Code",
    ]
    for fname in icd_overflow_fields:
        f = _find_finding(findings, fname)
        if f and f["status"] == "MISSING":
            if secondary_covered:
                # SecondaryIcdCodes is a catch-all; downgrade from CRITICAL to INFO
                f["severity"] = "INFO"
                f["notes"] = "SecondaryIcdCodes column present — additional ICD codes stored there"
            else:
                f["notes"] = "No secondary ICD code column found in file"


def _find_finding(
    findings: list[dict[str, Any]],
    template_field: str,
) -> dict[str, Any] | None:
    for f in findings:
        if f["template_field"] == template_field:
            return f
    return None


def _normalize(s: str) -> str:
    """Lowercase, strip spaces/underscores/hyphens/slashes/dots/parens."""
    return re.sub(r"[\s_\-/\.\(\)#]", "", s).lower()


def _empty_summary() -> dict[str, int]:
    return {
        "required_total": 0, "required_present": 0, "required_missing": 0,
        "recommended_total": 0, "recommended_present": 0, "recommended_missing": 0,
        "optional_total": 0, "optional_present": 0, "optional_missing": 0,
    }
