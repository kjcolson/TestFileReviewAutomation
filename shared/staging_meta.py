"""
shared/staging_meta.py

Loads StagingTableStructure.xlsx once and exposes type/length lookup
functions keyed on (staging_table, Source_Column).

StagingTableStructure.xlsx columns used:
    Staging_Table, Source_Column, Type, Max_Length, Precision, Scale
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


# Module-level cache
_loaded = False
# (staging_table, Source_Column) → {sql_type, max_length, precision, scale}
_type_info: dict[tuple[str, str], dict[str, Any]] = {}
# staging_table → list of Source_Column values (preserves order)
_table_columns: dict[str, list[str]] = {}
# TransactionType code values (str) that identify Charge records
_charge_trans_type_codes: set[str] = set()
# TransactionTypeDesc values (lowercase) that identify Charge records
_charge_trans_type_descs: set[str] = set()
# TransactionType code values (str) that identify non-charge (Transaction) records
_transaction_type_codes: set[str] = set()
# TransactionTypeDesc values (lowercase) that identify non-charge records
_transaction_type_descs: set[str] = set()
# CMS CPT/HCPCS reference table (indexed by CptCode)
_cms_cpt_df: pd.DataFrame | None = None
# CMS Place of Service reference table (indexed by PosCode)
_cms_pos_df: pd.DataFrame | None = None


def load(ref_dir: str | Path) -> None:
    """Load StagingTableStructure.xlsx.  Safe to call multiple times."""
    global _loaded
    if _loaded:
        return

    ref = Path(ref_dir)
    # Reference files live in KnowledgeSources/ subfolder; fall back to project root
    ks_dir = ref / "KnowledgeSources"
    path = ks_dir / "StagingTableStructure.xlsx"
    if not path.exists():
        path = ref / "StagingTableStructure.xlsx"
    df = pd.read_excel(path, engine="openpyxl", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    from collections import defaultdict
    cols: dict[str, list[str]] = defaultdict(list)

    for _, row in df.iterrows():
        tbl = str(row.get("Staging_Table", "")).strip()
        src_col = str(row.get("Source_Column", "")).strip()
        if not tbl or not src_col:
            continue

        key = (tbl, src_col)
        if key not in _type_info:
            _type_info[key] = {
                "sql_type":   str(row.get("Type", "")).strip(),
                "max_length": _to_int(row.get("Max_Length")),
                "precision":  _to_int(row.get("Precision")),
                "scale":      _to_int(row.get("Scale")),
            }
        if src_col not in cols[tbl]:
            cols[tbl].append(src_col)

    _table_columns.update(cols)

    # Load TransactionTypes.xlsx (charge vs. transaction classification)
    tt_path = ks_dir / "TransactionTypes.xlsx"
    if tt_path.exists():
        tt_df = pd.read_excel(tt_path, engine="openpyxl", dtype=str)
        tt_df.columns = [c.strip() for c in tt_df.columns]
        for _, row in tt_df.iterrows():
            type_cat = str(row.get("Type?", "")).strip().lower()
            code = str(row.get("TransactionType", "")).strip()
            desc = str(row.get("TransactionTypeDesc", "")).strip().lower()
            if type_cat == "charge":
                if code and code.lower() != "nan":
                    _charge_trans_type_codes.add(code)
                if desc and desc != "nan":
                    _charge_trans_type_descs.add(desc)
            elif type_cat == "transactions":
                if code and code.lower() != "nan":
                    _transaction_type_codes.add(code)
                if desc and desc != "nan":
                    _transaction_type_descs.add(desc)

    # Load CMS reference tables — accept both .csv and .xlsx
    def _find_cms_file(stem: str):
        """Return (path, fmt) for stem.csv or stem.xlsx; (None, None) if absent."""
        csv_p = ks_dir / f"{stem}.csv"
        if csv_p.exists():
            return csv_p, "csv"
        xlsx_p = ks_dir / f"{stem}.xlsx"
        if xlsx_p.exists():
            return xlsx_p, "xlsx"
        return None, None

    global _cms_cpt_df, _cms_pos_df
    cpt_file, cpt_fmt = _find_cms_file("stdCmsCpt")
    if cpt_file:
        try:
            _cms_cpt_df = (
                pd.read_csv(cpt_file, dtype=str)
                if cpt_fmt == "csv"
                else pd.read_excel(cpt_file, dtype=str)
            )
            _cms_cpt_df.columns = [c.strip() for c in _cms_cpt_df.columns]
            if "CptCode" in _cms_cpt_df.columns:
                _cms_cpt_df = _cms_cpt_df.set_index("CptCode")
        except Exception:
            _cms_cpt_df = None

    pos_file, pos_fmt = _find_cms_file("stdCmsPos")
    if pos_file:
        try:
            _cms_pos_df = (
                pd.read_csv(pos_file, dtype=str)
                if pos_fmt == "csv"
                else pd.read_excel(pos_file, dtype=str)
            )
            _cms_pos_df.columns = [c.strip() for c in _cms_pos_df.columns]
            if "PosCode" in _cms_pos_df.columns:
                _cms_pos_df = _cms_pos_df.set_index("PosCode")
        except Exception:
            _cms_pos_df = None

    _loaded = True


def get_cms_cpt() -> "pd.DataFrame | None":
    """Return the CMS CPT/HCPCS reference table indexed by CptCode, or None if not loaded."""
    return _cms_cpt_df


def get_cms_pos() -> "pd.DataFrame | None":
    """Return the CMS Place of Service reference table indexed by PosCode, or None if not loaded."""
    return _cms_pos_df


def get_column_type(staging_table: str, source_column: str) -> dict[str, Any]:
    """
    Return type metadata for a (staging_table, source_column) pair.

    Returns {"sql_type": str, "max_length": int|None, "precision": int|None,
             "scale": int|None} or an empty dict if not found.
    """
    return _type_info.get((staging_table, source_column), {})


def get_charge_type_sets() -> tuple[set[str], set[str]]:
    """
    Return (charge_type_codes, charge_type_descs) loaded from TransactionTypes.xlsx.

    charge_type_codes — TransactionType code values (str) that identify Charge records.
    charge_type_descs — TransactionTypeDesc values (lowercase) that identify Charge records.
    """
    return _charge_trans_type_codes, _charge_trans_type_descs


def get_transaction_type_sets() -> tuple[set[str], set[str]]:
    """
    Return (transaction_type_codes, transaction_type_descs) for non-charge records.

    transaction_type_codes — TransactionType values (str) that identify Transaction records.
    transaction_type_descs — TransactionTypeDesc values (lowercase) that identify Transaction records.
    """
    return _transaction_type_codes, _transaction_type_descs


def get_all_source_columns(staging_table: str) -> list[str]:
    """Return all Source_Column values for a staging table, in Excel order."""
    return _table_columns.get(staging_table, [])


def _to_int(val: Any) -> int | None:
    try:
        v = int(float(str(val)))
        return v if v >= 0 else None
    except (ValueError, TypeError):
        return None
