"""
phase2/datatype_checker.py

Validates that actual data values are compatible with target staging column
SQL types, varchar length constraints, and domain-specific format rules.

For sources without staging tables (quality, patient_satisfaction), only
domain-pattern checks are applied (NPI, date, numeric).

Severity escalation: if an issue affects a Required column (per
field_classifier), the severity is escalated one level:
    MEDIUM → HIGH,  HIGH → CRITICAL
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd


# Pre-compiled domain regex patterns
_COMPILED: dict[str, re.Pattern] = {}

_SAMPLE_SIZE = 5    # bad-value sample shown in report
_ROW_SAMPLE  = 20   # row indices shown in report

# Columns where blanks/nulls are expected by design (don't flag as null issues)
_NULLABLE_BY_DESIGN = {"Modifier1", "Modifier2", "Modifier3", "Modifier4"}

# Columns that are only meaningful for Charge records in a combined billing file;
# null checks are restricted to charge-type rows (identified via TransactionTypes.xlsx)
_CHARGE_CONDITIONAL_COLS = {
    "CptCode", "Units", "WorkRvuOriginal",
    "PrimaryIcdCode", "SecondaryIcdCodes",
}


def _build_charge_mask(
    df: pd.DataFrame,
    column_mappings: list[dict[str, Any]],
) -> "pd.Series | None":
    """
    Return a boolean Series identifying charge rows in a combined billing file.

    Matches TransactionType codes and TransactionTypeDesc values against the
    charge type sets loaded from TransactionTypes.xlsx via staging_meta.
    Returns None if no charge rows can be identified.
    """
    from shared import staging_meta
    charge_codes, charge_descs = staging_meta.get_charge_type_sets()

    tt_col  = None
    ttd_col = None
    for m in column_mappings:
        sc = m.get("staging_col", "")
        if sc == "TransactionType":
            tt_col = m.get("raw_col")
        elif sc == "TransactionTypeDesc":
            ttd_col = m.get("raw_col")

    mask = pd.Series([False] * len(df), index=df.index)
    if tt_col and tt_col in df.columns and charge_codes:
        mask = mask | df[tt_col].astype(str).str.strip().isin(charge_codes)
    if ttd_col and ttd_col in df.columns and charge_descs:
        mask = mask | df[ttd_col].astype(str).str.strip().str.lower().isin(charge_descs)

    return mask if mask.any() else None


def check(
    file_data: dict[str, Any],
    classified_mappings: list[dict[str, Any]],
    source: str,
    staging_table: str | None,
) -> list[dict[str, Any]]:
    """
    Run all data type checks for one file.

    Parameters
    ----------
    file_data : dict
        Entry from shared.loader — must contain "df".
    classified_mappings : list[dict]
        Output of field_classifier.classify() — includes RequirementLevel.
    source : str
        Source name.
    staging_table : str | None
        Staging table name (None for quality / patient_satisfaction).

    Returns
    -------
    list of datatype finding dicts.
    """
    from shared.constants import DOMAIN_FIELD_PATTERNS, DATA_FORMAT_PATTERNS
    from shared import staging_meta

    _compile_patterns(DATA_FORMAT_PATTERNS)

    df: pd.DataFrame | None = file_data.get("df")
    column_mappings: list[dict] = file_data.get("column_mappings", [])
    findings: list[dict[str, Any]] = []

    # Build charge-row mask for billing_combined (used to scope null checks on
    # charge-specific columns to only Charge-type rows)
    charge_mask: "pd.Series | None" = None
    if source == "billing_combined" and df is not None:
        charge_mask = _build_charge_mask(df, column_mappings)

    for rec in classified_mappings:
        confidence = rec.get("confidence", "")
        if confidence == "UNMAPPED":
            continue

        raw_col     = rec.get("raw_col", "")
        stg_col     = rec.get("staging_col") or ""
        stg_table   = rec.get("staging_table") or staging_table or ""
        req_level   = rec.get("RequirementLevel", "Unclassified")

        # SQL type info from Phase 1 (already resolved against StagingTableStructure)
        sql_type   = rec.get("sql_type") or ""
        max_length = rec.get("max_length")
        precision  = rec.get("precision")
        scale      = rec.get("scale")

        # Fall back to staging_meta if Phase 1 didn't populate type info
        if not sql_type and stg_col and stg_table:
            ti = staging_meta.get_column_type(stg_table, stg_col)
            sql_type   = ti.get("sql_type", "")
            max_length = ti.get("max_length")
            precision  = ti.get("precision")
            scale      = ti.get("scale")

        finding: dict[str, Any] = {
            "raw_column":           raw_col,
            "staging_column":       stg_col,
            "requirement_level":    req_level,
            "staging_type":         sql_type,
            "max_length":           max_length,
            "type_compatible":      True,
            "domain_check":         None,
            "domain_valid_pct":     None,
            "domain_invalid_count": 0,
            "domain_invalid_sample": [],
            "domain_invalid_rows":   [],
            "length_exceeded_count": 0,
            "max_observed_length":   None,
            "null_count":            0,
            "null_pct":              0.0,
            "severity":              None,
            "notes":                 "",
        }

        if df is None or raw_col not in df.columns:
            findings.append(finding)
            continue

        series = df[raw_col]

        # 1. Null / blank check
        # For charge-conditional columns in billing_combined, restrict to charge rows only
        if stg_col in _CHARGE_CONDITIONAL_COLS and source == "billing_combined":
            if charge_mask is not None:
                _check_nulls(series[charge_mask], finding)
            # else: no charge rows identified — skip null check for this column
        else:
            _check_nulls(series, finding)

        # 2. SQL type compatibility (only if staging table exists)
        if sql_type and stg_table:
            _check_sql_type(series, sql_type, max_length, precision, scale, finding)

        # 3. Domain-specific pattern check
        domain_key = DOMAIN_FIELD_PATTERNS.get(stg_col)
        if domain_key and domain_key in _COMPILED:
            _check_domain(series, domain_key, DATA_FORMAT_PATTERNS[domain_key], finding)

        # 4. Severity escalation for Required fields
        _escalate_severity(finding, req_level)

        if finding["severity"]:
            findings.append(finding)
        # Always append if there are notes worth reporting
        elif finding["notes"]:
            findings.append(finding)

    return findings


# ---------------------------------------------------------------------------
# Check functions
# ---------------------------------------------------------------------------

def _check_nulls(series: pd.Series, finding: dict[str, Any]) -> None:
    """Count null/blank values and set null_count, null_pct."""
    total = len(series)
    if total == 0:
        return
    null_mask = series.isna() | (series.astype(str).str.strip() == "")
    null_count = int(null_mask.sum())
    null_pct   = round(null_count / total * 100, 1)
    finding["null_count"] = null_count
    finding["null_pct"]   = null_pct

    stg_col = finding.get("staging_column", "")
    req = finding.get("requirement_level", "")

    # Modifier columns are nullable by design — blanks are expected
    if stg_col in _NULLABLE_BY_DESIGN:
        if stg_col == "Modifier1" and null_pct == 100.0:
            finding["severity"] = "INFO"
            finding["notes"]    = "Modifier1 is 100% blank - unusual, confirm with client"
        return

    if req == "Required" and null_count > 0:
        pct = null_pct
        if pct > 50:
            finding["severity"] = "CRITICAL"
            finding["notes"]    = f"Required field is {pct}% null/blank"
        else:
            finding["severity"] = "HIGH"
            finding["notes"]    = f"Required field has {null_count:,} null/blank values ({pct}%)"


def _check_sql_type(
    series: pd.Series,
    sql_type: str,
    max_length: int | None,
    precision: int | None,
    scale: int | None,
    finding: dict[str, Any],
) -> None:
    sql_type_lower = sql_type.lower()
    if "date" in sql_type_lower:
        _check_date(series, finding)
    elif "time" == sql_type_lower:
        _check_time(series, finding)
    elif sql_type_lower in ("int", "integer", "bigint", "smallint"):
        _check_int(series, finding)
    elif sql_type_lower in ("decimal", "numeric", "float", "money"):
        _check_decimal(series, precision, scale, finding)
    elif sql_type_lower in ("varchar", "nvarchar", "char"):
        if max_length and max_length > 0:
            _check_varchar_length(series, max_length, finding)


def _check_date(series: pd.Series, finding: dict[str, Any]) -> None:
    """Try to parse as date; flag unparseable and inconsistent formats."""
    non_blank = series[series.astype(str).str.strip() != ""]
    if len(non_blank) == 0:
        return

    formats_seen: dict[str, int] = {}
    bad_values: list[str] = []
    bad_rows:   list[int]  = []

    for idx, val in non_blank.items():
        val_str = str(val).strip()
        fmt = _detect_date_format(val_str)
        if fmt:
            formats_seen[fmt] = formats_seen.get(fmt, 0) + 1
        else:
            bad_values.append(val_str)
            bad_rows.append(int(idx))

    invalid_count = len(bad_values)
    if invalid_count > 0:
        finding["type_compatible"] = False
        _add_invalids(finding, bad_values, bad_rows, f"{invalid_count:,} values not parseable as date")

    if len(formats_seen) > 1:
        fmt_list = ", ".join(formats_seen.keys())
        note = f"Mixed date formats detected: {fmt_list}"
        finding["notes"] = (finding["notes"] + "; " + note).lstrip("; ")

    if formats_seen:
        finding["date_format_detected"] = max(formats_seen, key=lambda k: formats_seen[k])

    if invalid_count > 0:
        _set_severity(finding, "MEDIUM")


def _check_time(series: pd.Series, finding: dict[str, Any]) -> None:
    """Flag values that don't look like valid time strings."""
    _TIME_PAT = re.compile(
        r"^\d{1,2}:\d{2}(:\d{2})?(\s*[AaPp][Mm])?$"
    )
    non_blank = series[series.astype(str).str.strip() != ""]
    if len(non_blank) == 0:
        return
    bad_mask = ~non_blank.astype(str).str.strip().str.match(_TIME_PAT.pattern)
    bad_vals = non_blank[bad_mask].astype(str).tolist()
    bad_rows = list(non_blank[bad_mask].index)
    if bad_vals:
        finding["type_compatible"] = False
        _add_invalids(finding, bad_vals, bad_rows, f"{len(bad_vals):,} values not valid time format")
        _set_severity(finding, "MEDIUM")


def _check_int(series: pd.Series, finding: dict[str, Any]) -> None:
    """Flag non-integer values."""
    non_blank = series[series.astype(str).str.strip() != ""]
    if len(non_blank) == 0:
        return
    _INT_PAT = re.compile(r"^-?\d+$")
    bad_mask = ~non_blank.astype(str).str.strip().str.match(_INT_PAT.pattern)
    bad_vals = non_blank[bad_mask].astype(str).tolist()
    bad_rows = list(non_blank[bad_mask].index)
    if bad_vals:
        finding["type_compatible"] = False
        _add_invalids(finding, bad_vals, bad_rows, f"{len(bad_vals):,} values are not whole integers")
        _set_severity(finding, "MEDIUM")


def _check_decimal(
    series: pd.Series,
    precision: int | None,
    scale: int | None,
    finding: dict[str, Any],
) -> None:
    """Flag non-numeric values and precision/scale overflows."""
    non_blank = series[series.astype(str).str.strip() != ""]
    if len(non_blank) == 0:
        return
    _DEC_PAT = re.compile(r"^-?\d+(\.\d+)?$")
    bad_mask = ~non_blank.astype(str).str.strip().str.replace(",", "", regex=False).str.match(
        _DEC_PAT.pattern
    )
    bad_vals = non_blank[bad_mask].astype(str).tolist()
    bad_rows = list(non_blank[bad_mask].index)
    if bad_vals:
        finding["type_compatible"] = False
        _add_invalids(finding, bad_vals, bad_rows, f"{len(bad_vals):,} values are not numeric")
        _set_severity(finding, "MEDIUM")
        return

    # Precision / scale overflow check
    if precision and scale is not None:
        int_digits_allowed = precision - scale
        exceeded_count = 0
        for val in non_blank.astype(str).str.strip().str.replace(",", "", regex=False):
            try:
                parts = val.lstrip("-").split(".")
                int_part = len(parts[0])
                if int_part > int_digits_allowed:
                    exceeded_count += 1
            except Exception:
                pass
        if exceeded_count > 0:
            note = (
                f"{exceeded_count:,} values exceed decimal({precision},{scale}) "
                f"integer-digit limit of {int_digits_allowed}"
            )
            finding["notes"] = (finding["notes"] + "; " + note).lstrip("; ")
            _set_severity(finding, "MEDIUM")


def _check_varchar_length(
    series: pd.Series,
    max_length: int,
    finding: dict[str, Any],
) -> None:
    """Flag values that exceed the staging varchar max_length."""
    lengths = series.astype(str).str.len()
    max_obs = int(lengths.max()) if len(lengths) > 0 else 0
    finding["max_observed_length"] = max_obs
    if max_obs <= max_length:
        return

    exceeded_mask = lengths > max_length
    exceeded_count = int(exceeded_mask.sum())
    pct = exceeded_count / len(series) * 100 if len(series) > 0 else 0
    finding["length_exceeded_count"] = exceeded_count

    sev = "HIGH" if pct >= 5.0 else "MEDIUM"
    note = (
        f"{exceeded_count:,} values exceed varchar({max_length}) "
        f"— max observed length: {max_obs} ({pct:.1f}% of rows)"
    )
    finding["notes"] = (finding["notes"] + "; " + note).lstrip("; ")
    _set_severity(finding, sev)


def _check_domain(
    series: pd.Series,
    domain_key: str,
    pattern_info: dict,
    finding: dict[str, Any],
) -> None:
    """Apply pre-compiled regex domain check."""
    compiled = _COMPILED.get(domain_key)
    if compiled is None:
        return

    non_blank = series[series.astype(str).str.strip() != ""]
    if len(non_blank) == 0:
        return

    clean = non_blank.astype(str).str.strip()
    valid_mask = clean.str.match(compiled.pattern)
    invalid_count = int((~valid_mask).sum())
    total_non_blank = len(clean)
    valid_pct = round((total_non_blank - invalid_count) / total_non_blank * 100, 1)

    finding["domain_check"]         = domain_key
    finding["domain_valid_pct"]     = valid_pct
    finding["domain_invalid_count"] = invalid_count

    if invalid_count > 0:
        bad_vals = clean[~valid_mask].tolist()
        bad_rows = list(non_blank[~valid_mask].index)
        finding["domain_invalid_sample"] = bad_vals[:_SAMPLE_SIZE]
        finding["domain_invalid_rows"]   = [int(r) for r in bad_rows[:_ROW_SAMPLE]]

        inv_pct = round(invalid_count / total_non_blank * 100, 1)
        note = (
            f"{invalid_count:,} {domain_key} values fail pattern "
            f"'{pattern_info['description']}' ({inv_pct}%)"
        )
        finding["notes"] = (finding["notes"] + "; " + note).lstrip("; ")
        sev = "HIGH" if inv_pct >= 5.0 else "MEDIUM"
        _set_severity(finding, sev)


# ---------------------------------------------------------------------------
# Severity helpers
# ---------------------------------------------------------------------------

_SEV_ORDER = {None: 0, "INFO": 1, "LOW": 2, "MEDIUM": 3, "HIGH": 4, "CRITICAL": 5}


def _set_severity(finding: dict[str, Any], sev: str) -> None:
    """Set severity if it's higher than the current value."""
    if _SEV_ORDER.get(sev, 0) > _SEV_ORDER.get(finding.get("severity"), 0):
        finding["severity"] = sev


def _escalate_severity(finding: dict[str, Any], req_level: str) -> None:
    """
    If the column is Required, escalate current severity by one level:
    MEDIUM→HIGH, HIGH→CRITICAL.
    """
    if req_level != "Required":
        return
    current = finding.get("severity")
    escalation = {"MEDIUM": "HIGH", "HIGH": "CRITICAL"}
    if current in escalation:
        finding["severity"] = escalation[current]


# ---------------------------------------------------------------------------
# Date format detection
# ---------------------------------------------------------------------------

_DATE_FORMATS = [
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"),         "YYYY-MM-DD"),
    (re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$"),     "M/D/YYYY"),
    (re.compile(r"^\d{1,2}/\d{1,2}/\d{2}$"),     "M/D/YY"),
    (re.compile(r"^\d{8}$"),                       "YYYYMMDD"),
    (re.compile(r"^\d{4}/\d{2}/\d{2}$"),          "YYYY/MM/DD"),
    (re.compile(r"^\d{1,2}-\d{1,2}-\d{4}$"),     "M-D-YYYY"),
    (re.compile(
        r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}"),   "ISO datetime"),
]


def _detect_date_format(val: str) -> str | None:
    for pat, label in _DATE_FORMATS:
        if pat.match(val):
            return label
    return None


# ---------------------------------------------------------------------------
# Misc helpers
# ---------------------------------------------------------------------------

def _add_invalids(
    finding: dict[str, Any],
    bad_vals: list[str],
    bad_rows: list[int],
    note: str,
) -> None:
    finding["domain_invalid_count"] = max(
        finding.get("domain_invalid_count", 0), len(bad_vals)
    )
    finding["domain_invalid_sample"] = bad_vals[:_SAMPLE_SIZE]
    finding["domain_invalid_rows"]   = [int(r) for r in bad_rows[:_ROW_SAMPLE]]
    finding["notes"] = (finding.get("notes", "") + "; " + note).lstrip("; ")


def _compile_patterns(data_format_patterns: dict) -> None:
    for key, info in data_format_patterns.items():
        if key not in _COMPILED:
            _COMPILED[key] = re.compile(info["pattern"])
