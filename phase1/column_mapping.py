"""
phase1/column_mapping.py

Maps raw client column headers to PIVOT staging columns using a 4-step
algorithm: EXACT → NORMALIZED → FUZZY → UNMAPPED.

Reference files (loaded once at module import):
    RawToStagingColumnMapping.xlsx  — known raw→staging aliases
        Columns: RawColumn, StagingColumn, Staging_Table

    StagingTableStructure.xlsx  — staging table SQL type/length constraints
        Columns: Unbr, Staging_Table, Staging_Table_Order, Source_Column,
                 Destination, Schema, Table, Column, Type, Max_Length,
                 Precision, Scale, Required, DataSourceFileTypeFnbr
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import pandas as pd
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FUZZY_THRESHOLD = 85  # minimum token_sort_ratio score for a FUZZY match

# Dual-mapping columns: raw columns that intentionally map to >1 staging col.
# These are identified directly from the mapping file (multiple rows for the
# same RawColumn+Staging_Table).  The list below is for documentation only;
# the code derives them dynamically.

# Required staging columns by source (based on spec §1.11).
# Used to report UNCOVERED required columns.
REQUIRED_STAGING_COLS: dict[str, list[str]] = {
    "#staging_charges": [
        "DateOfService", "PostDate", "CptCode", "Modifier1", "Modifier2",
        "Modifier3", "Modifier4", "Units", "ChargeAmountOriginal", "WorkRvuOriginal",
        "PlaceOfServiceCode", "PrimaryIcdCode", "SecondaryIcdCodes",
        "PatientId", "PatientGender", "PatientZip",
        "RenderingProviderFullName", "RenderingProviderNpi",
        "RenderingProviderSpecialty", "RenderingProviderCredentials",
        "BillingProviderFullName", "BillingProviderNpi",
        "BillingProviderSpecialty", "BillingProviderCredentials",
        "BillPracticeName", "BillLocationName", "BillDepartmentName",
        "ChargePayerName", "ChargePayerPlan", "ChargePayerFinancialClass",
        "ChargeId", "InvoiceNumber",
    ],
    "#staging_transactions": [
        "TransactionType", "TransactionTypeDesc", "PostDate",
        "PaymentOriginal", "AdjustmentOriginal", "RefundOriginal",
        "TransactionPayerName", "TransactionPayerPlan",
        "TransactionPayerFinancialClass", "ChargeId", "InvoiceNumber",
    ],
    "#staging_billing": [
        "DateOfService", "PostDate", "CptCode", "Modifier1", "Modifier2",
        "Modifier3", "Modifier4", "Units", "TransactionType", "TransactionTypeDesc",
        "ChargeAmountOriginal", "WorkRvuOriginal", "PlaceOfServiceCode",
        "PrimaryIcdCode", "SecondaryIcdCodes", "PatientId", "PatientGender",
        "PatientZip", "RenderingProviderFullName", "RenderingProviderNpi",
        "RenderingProviderSpecialty", "RenderingProviderCredentials",
        "BillingProviderFullName", "BillingProviderNpi",
        "BillingProviderSpecialty", "BillingProviderCredentials",
        "BillPracticeName", "BillLocationName", "BillDepartmentName",
        "ChargePayerName", "ChargePayerPlan", "ChargePayerFinancialClass",
        "ChargeId", "InvoiceNumber",
    ],
    "#staging_scheduling": [
        "ApptId", "BillLocNameOrig", "ApptProvFullNameOrig",
        "PatIdOrig", "ApptType", "CreateDate", "ApptDate",
        "CancellationDate", "CancelReason", "ApptTime",
        "ApptSchdLength", "ApptStatus",
    ],
    "#staging_payroll": [
        "EmployeeId", "EmployeeFullName", "JobCode", "JobCodeDesc",
        "DepartmentId", "DepartmentName",
        "PayPeriodStartDate", "PayPeriodEndDate",
        "EarningsCode", "EarningsCodeDesc", "Hours", "AmountOrig",
    ],
    "#staging_gl": [
        "CostCenterNumberOrig", "CostCenterNameOrig", "YearMonth",
        "AcctNumber", "AcctDesc", "AmountOrig",
    ],
}

# Recommended staging columns for Phase 4 join keys (org hierarchy IDs).
# These surface as UNCOVERED (Recommended) in the Mapping Gaps sheet when
# no raw column was routed to them, signalling that Phase 4 joins may fail.
RECOMMENDED_STAGING_COLS: dict[str, list[str]] = {
    "#staging_charges":    ["BillDepartmentId", "BillLocationId", "BillPracticeId"],
    "#staging_scheduling": ["BillLocId", "DeptId", "PracId"],
    "#staging_billing":    ["BillDepartmentId", "BillLocationId", "BillPracticeId"],
}

# Org hierarchy column pairs for value-based rerouting.
# After the EXACT/NORMALIZED/FUZZY lookup resolves a raw column to a *Name
# org staging column, actual data values are inspected.  If values look like
# numeric codes/IDs the record is rerouted to the *Id counterpart (and vice-
# versa).  The decision is logged in the Notes column of the Excel report.
_ORG_NAME_TO_ID: dict[str, str] = {
    "BillDepartmentName": "BillDepartmentId",
    "BillLocationName":   "BillLocationId",
    "BillPracticeName":   "BillPracticeId",
    "DeptNameOrig":       "DeptId",
    "PracNameOrig":       "PracId",
    "BillLocNameOrig":    "BillLocId",
}
_ORG_ID_TO_NAME: dict[str, str] = {v: k for k, v in _ORG_NAME_TO_ID.items()}


# ---------------------------------------------------------------------------
# Reference-file loading  (module-level cache)
# ---------------------------------------------------------------------------

_mapping_loaded = False
# exact_map[(staging_table, raw_col)]        -> [StagingColumn, ...]
_exact_map:  dict[tuple[str, str], list[str]] = {}
# norm_map[(staging_table, norm_raw_col)]    -> [StagingColumn, ...]
_norm_map:   dict[tuple[str, str], list[str]] = {}
# all_aliases[staging_table]                 -> [(raw_col, [StagingColumn, ...])]
_all_aliases: dict[str, list[tuple[str, list[str]]]] = {}
# type_info[(staging_table, staging_col)]    -> {type, max_length, precision, scale}
_type_info:  dict[tuple[str, str], dict[str, Any]] = {}


def _normalize(s: str) -> str:
    """Lowercase, strip, remove spaces/underscores/hyphens/slashes/dots/parens."""
    return re.sub(r"[\s_\-/\.\(\)#]", "", s).lower()


def load_reference_files(ref_dir: str | Path) -> None:
    """
    Load both reference Excel files.  Safe to call multiple times; only
    reads files on the first call.
    """
    global _mapping_loaded
    if _mapping_loaded:
        return

    ref_dir = Path(ref_dir)
    # Reference files live in KnowledgeSources/ subfolder; fall back to project root
    ks_dir = ref_dir / "KnowledgeSources"

    mapping_path = ks_dir / "RawToStagingColumnMapping.xlsx"
    if not mapping_path.exists():
        mapping_path = ref_dir / "RawToStagingColumnMapping.xlsx"

    structure_path = ks_dir / "StagingTableStructure.xlsx"
    if not structure_path.exists():
        structure_path = ref_dir / "StagingTableStructure.xlsx"

    _load_mapping(mapping_path)
    _load_structure(structure_path)
    _mapping_loaded = True


def _load_mapping(path: Path) -> None:
    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    df.columns = [c.strip() for c in df.columns]
    df.dropna(subset=["RawColumn", "StagingColumn", "Staging_Table"], inplace=True)

    from collections import defaultdict
    exact: dict[tuple, list] = defaultdict(list)
    norm:  dict[tuple, list] = defaultdict(list)
    aliases: dict[str, list] = defaultdict(list)

    for _, row in df.iterrows():
        raw  = str(row["RawColumn"]).strip()
        stg  = str(row["StagingColumn"]).strip()
        tbl  = str(row["Staging_Table"]).strip()
        key_exact = (tbl, raw)
        key_norm  = (tbl, _normalize(raw))

        if stg not in exact[key_exact]:
            exact[key_exact].append(stg)
        if stg not in norm[key_norm]:
            norm[key_norm].append(stg)

        # Track (raw_col, stg_cols) per table for fuzzy search
        # Find or create entry
        table_aliases = aliases[tbl]
        existing = next((e for e in table_aliases if e[0] == raw), None)
        if existing is None:
            aliases[tbl].append((raw, [stg]))
        else:
            if stg not in existing[1]:
                existing[1].append(stg)

    _exact_map.update(exact)
    _norm_map.update(norm)
    _all_aliases.update(aliases)


def _load_structure(path: Path) -> None:
    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    for _, row in df.iterrows():
        tbl  = str(row.get("Staging_Table", "")).strip()
        col  = str(row.get("Source_Column", "")).strip()
        if not tbl or not col:
            continue
        _type_info[(tbl, col)] = {
            "sql_type":   str(row.get("Type", "")).strip(),
            "max_length": _to_int(row.get("Max_Length")),
            "precision":  _to_int(row.get("Precision")),
            "scale":      _to_int(row.get("Scale")),
        }


def _to_int(val: Any) -> int | None:
    try:
        v = int(float(str(val)))
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def map_all_files(
    file_dict: dict[str, dict[str, Any]],
    source_assignments: dict[str, str],
    ref_dir: str | Path,
) -> dict[str, list[dict[str, Any]]]:
    """
    Run the 4-step mapping algorithm for every file.

    Returns
    -------
    dict[filename, list_of_mapping_records]

    Each record::

        {
            "raw_col":      str,
            "staging_col":  str | None,      # None = UNMAPPED
            "staging_cols": list[str],        # all mapped cols (len>1 = dual)
            "staging_table":str | None,
            "confidence":   str,             # EXACT / NORMALIZED / FUZZY / UNMAPPED
            "fuzzy_score":  int | None,
            "sql_type":     str | None,
            "max_length":   int | None,
            "precision":    int | None,
            "scale":        int | None,
            "notes":        str,
        }
    """
    load_reference_files(ref_dir)
    results: dict[str, list[dict]] = {}
    for filename, meta in file_dict.items():
        df: pd.DataFrame | None = meta.get("df")
        if df is None:
            results[filename] = []
            continue
        source = source_assignments.get(filename, "unknown")
        from .source_detection import SOURCE_TO_STAGING
        staging_table = SOURCE_TO_STAGING.get(source)
        if not staging_table or staging_table.startswith("("):
            staging_table = None
        results[filename] = _map_file(df.columns.tolist(), staging_table, df=df)
    return results


def get_uncovered_staging_cols(
    filename: str,
    mapping_records: list[dict[str, Any]],
    source_assignments: dict[str, str],
) -> dict[str, list[str]]:
    """
    Return required staging columns that have no raw-column mapping.

    Returns {"required": [...], "recommended": [...]}
    """
    from .source_detection import SOURCE_TO_STAGING
    source = source_assignments.get(filename, "unknown")
    tbl = SOURCE_TO_STAGING.get(source, "")

    req_cols = REQUIRED_STAGING_COLS.get(tbl, [])
    covered = set()
    for rec in mapping_records:
        covered.update(rec.get("staging_cols", []))

    uncovered_required = [c for c in req_cols if c not in covered]
    rec_cols = RECOMMENDED_STAGING_COLS.get(tbl, [])
    uncovered_recommended = [c for c in rec_cols if c not in covered]
    return {"required": uncovered_required, "recommended": uncovered_recommended}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _map_file(
    columns: list[str],
    staging_table: str | None,
    df: pd.DataFrame | None = None,
) -> list[dict[str, Any]]:
    records = []
    for raw_col in columns:
        rec = _map_single(raw_col, staging_table)
        # Value-based org column rerouting
        if df is not None and rec["confidence"] != "UNMAPPED" and staging_table:
            stg = rec.get("staging_col")
            col_data = df.get(raw_col)
            if col_data is not None:
                if stg in _ORG_NAME_TO_ID and _is_id_like(col_data):
                    _reroute(
                        rec, _ORG_NAME_TO_ID[stg], staging_table,
                        "AUTO-ROUTED to *Id: values appear to be codes/IDs",
                    )
                elif stg in _ORG_ID_TO_NAME and not _is_id_like(col_data):
                    _reroute(
                        rec, _ORG_ID_TO_NAME[stg], staging_table,
                        "AUTO-ROUTED to *Name: values appear to be descriptive names",
                    )
        records.append(rec)
    return records


def _is_id_like(series: pd.Series, threshold: float = 0.70) -> bool:
    """Return True if >threshold of non-empty values look like codes/IDs."""
    clean = series.dropna().astype(str).str.strip()
    clean = clean[clean != ""]
    if len(clean) == 0:
        return False
    n = len(clean)
    # All-digit or digit+separator (e.g. "1234", "CC-001", "001.02")
    code_like = clean.str.match(r"^[\d][\d\-\.]*$").sum()
    if code_like / n >= threshold:
        return True
    # Short tokens with no spaces AND at least one digit or separator
    # (e.g. "CC001", "DEPT_A", "LOC42") — excludes pure-alpha words like "Cardiology"
    short_code_like = (
        (clean.str.len() <= 12)
        & (~clean.str.contains(r" "))
        & (clean.str.contains(r"[\d_\-\.]"))
    ).sum()
    if short_code_like / n >= threshold:
        return True
    # Compound codes: no spaces, underscore-delimited, contains at least one digit
    # (e.g. "C0015_C0015_CC00000_121200_16_1184617870_01209001")
    compound_code_like = (
        (~clean.str.contains(r" "))
        & (clean.str.contains(r"_"))
        & (clean.str.contains(r"\d"))
    ).sum()
    if compound_code_like / n >= threshold:
        return True
    return False


def _reroute(
    rec: dict[str, Any],
    new_col: str,
    staging_table: str,
    note: str,
) -> None:
    """Reroute an org mapping record to a different staging column in-place."""
    rec["staging_col"]  = new_col
    rec["staging_cols"] = [new_col]
    ti = _type_info.get((staging_table, new_col), {})
    rec["sql_type"]   = ti.get("sql_type")
    rec["max_length"] = ti.get("max_length")
    rec["precision"]  = ti.get("precision")
    rec["scale"]      = ti.get("scale")
    existing = rec.get("notes", "")
    rec["notes"] = note + (f"; {existing}" if existing else "")


def _map_single(
    raw_col: str,
    staging_table: str | None,
) -> dict[str, Any]:
    base = {
        "raw_col":      raw_col,
        "staging_col":  None,
        "staging_cols": [],
        "staging_table": staging_table,
        "confidence":   "UNMAPPED",
        "fuzzy_score":  None,
        "sql_type":     None,
        "max_length":   None,
        "precision":    None,
        "scale":        None,
        "notes":        "",
    }

    if not staging_table:
        return base

    # --- Step 1: EXACT ---
    key_exact = (staging_table, raw_col)
    if key_exact in _exact_map:
        stg_cols = _exact_map[key_exact]
        return _build_record(base, stg_cols, "EXACT", staging_table)

    # --- Step 2: NORMALIZED ---
    key_norm = (staging_table, _normalize(raw_col))
    if key_norm in _norm_map:
        stg_cols = _norm_map[key_norm]
        return _build_record(base, stg_cols, "NORMALIZED", staging_table)

    # --- Step 3: FUZZY ---
    aliases = _all_aliases.get(staging_table, [])
    best_score = 0
    best_stg_cols: list[str] = []
    best_alias = ""
    norm_raw = _normalize(raw_col)
    for alias_raw, alias_stg_cols in aliases:
        score = fuzz.token_sort_ratio(norm_raw, _normalize(alias_raw))
        if score > best_score:
            best_score = score
            best_stg_cols = alias_stg_cols
            best_alias = alias_raw

    if best_score >= FUZZY_THRESHOLD:
        rec = _build_record(base, best_stg_cols, f"FUZZY ({best_score}%)", staging_table)
        rec["fuzzy_score"] = best_score
        existing_note = rec.get("notes", "")
        rec["notes"] = (
            f"Best alias match: '{best_alias}'"
            + (f"; {existing_note}" if existing_note else "")
        )
        return rec

    # --- Step 4: UNMAPPED ---
    return base


def _build_record(
    base: dict[str, Any],
    stg_cols: list[str],
    confidence: str,
    staging_table: str,
) -> dict[str, Any]:
    rec = dict(base)
    rec["staging_cols"] = stg_cols
    rec["staging_col"]  = stg_cols[0] if stg_cols else None
    rec["confidence"]   = confidence

    # Type info (from first/primary staging col)
    primary = stg_cols[0] if stg_cols else None
    if primary:
        ti = _type_info.get((staging_table, primary), {})
        rec["sql_type"]   = ti.get("sql_type")
        rec["max_length"] = ti.get("max_length")
        rec["precision"]  = ti.get("precision")
        rec["scale"]      = ti.get("scale")

    # Notes for dual-mapped columns
    if len(stg_cols) > 1:
        rec["notes"] = f"DUAL → {' + '.join(stg_cols)}"

    return rec
