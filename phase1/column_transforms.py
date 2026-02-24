"""
phase1/column_transforms.py

Loads optional per-client column transform rules from
``input/{client}/column_transforms.csv``.

CSV format (header row required, case-insensitive):
    StagingColumn,Formula
    BillDepartmentId,SPLIT_PART(x,'_',3)
    CostCenterNumberOrig,TRIM(UPPER(x))

Each rule maps a staging column name to a SQL-style formula applied to the
corresponding raw column value whenever that file is loaded.

Supported SQL functions (positions are 1-indexed):
    LEFT(x, n)                        — first n characters
    RIGHT(x, n)                       — last n characters
    SUBSTRING(x, start_expr, len_expr)— substring; start/len may be CHARINDEX or LEN expressions
    SPLIT_PART(x, 'delim', n)         — nth segment when split by delimiter
    CHARINDEX('needle', x)            — 1-indexed position of needle (standalone or inside SUBSTRING)
    LEN(x)                            — string length (standalone or inside SUBSTRING)
    TRIM(x)                           — strip leading/trailing whitespace
    UPPER(x)                          — uppercase
    LOWER(x)                          — lowercase
    REPLACE(x, 'old', 'new')          — replace substring

Segments may be concatenated with ||.  Whitespace around operators is ignored.
Note: || cannot itself be used as a SPLIT_PART delimiter.

CHARINDEX/LEN inside SUBSTRING:
    SUBSTRING(x, 1, CHARINDEX('_', x) - 1)       — everything before first '_'
    SUBSTRING(x, CHARINDEX('_', x) + 1, LEN(x))  — everything after first '_'
"""

from __future__ import annotations

import csv
import re
from pathlib import Path


# Per-function validation patterns (each matches one SQL function call).
# SUBSTRING uses a relaxed pattern because its position arguments may contain
# CHARINDEX or LEN sub-expressions; execution-time parsing handles correctness.
_SQL_VALIDATION_PATTERNS = [
    re.compile(r"^\s*(LEFT|RIGHT)\s*\(\s*x\s*,\s*\d+\s*\)\s*$", re.I),
    re.compile(r"^\s*SUBSTRING\s*\(\s*x\s*,\s*.+?\s*,\s*.+?\s*\)\s*$", re.I | re.DOTALL),
    re.compile(r"^\s*SPLIT_PART\s*\(\s*x\s*,\s*'[^']*'\s*,\s*\d+\s*\)\s*$", re.I),
    re.compile(r"^\s*CHARINDEX\s*\(\s*'[^']*'\s*,\s*x\s*\)\s*$", re.I),
    re.compile(r"^\s*LEN\s*\(\s*x\s*\)\s*$", re.I),
    re.compile(r"^\s*(TRIM|UPPER|LOWER)\s*\(\s*x\s*\)\s*$", re.I),
    re.compile(r"^\s*REPLACE\s*\(\s*x\s*,\s*'[^']*'\s*,\s*'[^']*'\s*\)\s*$", re.I),
]


def load_column_transforms(input_dir: str | Path) -> list[dict]:
    """
    Load ``column_transforms.csv`` from *input_dir*.

    Returns a list of ``{"staging_column": str, "formula": str}`` dicts,
    one per valid row.  Returns ``[]`` if the file does not exist.
    """
    path = Path(input_dir) / "column_transforms.csv"
    if not path.exists():
        return []

    rules: list[dict] = []
    try:
        with open(path, newline="", encoding="utf-8-sig") as fh:
            reader = csv.DictReader(fh)
            # Normalise header names to lowercase for case-insensitive lookup
            if reader.fieldnames is None:
                print(f"  WARNING: column_transforms.csv has no headers — skipped.")
                return []

            fieldnames_lower = [f.strip().lower() for f in reader.fieldnames]
            if "stagingcolumn" not in fieldnames_lower and "staging_column" not in fieldnames_lower:
                print(
                    f"  WARNING: column_transforms.csv missing 'StagingColumn' header — skipped."
                )
                return []
            if "formula" not in fieldnames_lower:
                print(
                    f"  WARNING: column_transforms.csv missing 'Formula' header — skipped."
                )
                return []

            for row in reader:
                # Normalise keys; guard against None values from short rows
                norm = {
                    k.strip().lower().replace("_", ""): (v or "").strip()
                    for k, v in row.items()
                    if k is not None
                }
                staging_col = norm.get("stagingcolumn", "").strip()
                formula     = norm.get("formula", "").strip()

                if not staging_col or not formula:
                    continue

                if not _validate_formula(formula):
                    print(
                        f"  WARNING: column_transforms.csv — unsupported formula for "
                        f"'{staging_col}': '{formula}' — skipped. "
                        f"Only SQL functions (LEFT, RIGHT, SUBSTRING, SPLIT_PART, "
                        f"CHARINDEX, LEN, TRIM, UPPER, LOWER, REPLACE) with || concatenation are supported."
                    )
                    continue

                rules.append({"staging_column": staging_col, "formula": formula})

    except Exception as exc:
        print(f"  WARNING: Could not read column_transforms.csv: {exc}")
        return []

    if rules:
        print(
            f"  column_transforms.csv found — {len(rules)} rule(s): "
            f"{[r['staging_column'] for r in rules]}"
        )
    return rules


def _validate_formula(formula: str) -> bool:
    """
    Return True if every ``||``-separated segment of *formula* matches
    a recognised SQL function call.
    """
    for segment in formula.split("||"):
        if not any(p.match(segment) for p in _SQL_VALIDATION_PATTERNS):
            return False
    return True
