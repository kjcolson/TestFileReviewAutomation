"""
shared/loader.py

Re-loads source file DataFrames using Phase 1 metadata stored in
phase1_findings.json.  Keeps Phase 2 decoupled from Phase 1's in-memory
state so the phases can run independently.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from shared.column_utils import resolve_column


def load_files(
    phase1_json_path: str | Path,
    input_base_dir: str | Path,
) -> dict[str, dict[str, Any]]:
    """
    Load every file described in phase1_findings.json.

    Returns
    -------
    dict[filename, {
        "df":              pd.DataFrame | None,
        "source":          str,
        "staging_table":   str,
        "column_mappings": list[dict],
        "file_path":       str,
        "delimiter":       str,
        "encoding":        str,
        "row_count":       int,
        "col_count":       int,
        "unmapped_raw":    list[str],
        "uncovered_staging": dict,
        "parse_issues":    list[str],
    }]
    """
    phase1_json_path = Path(phase1_json_path)
    if not phase1_json_path.exists():
        raise FileNotFoundError(
            f"phase1_findings.json not found at: {phase1_json_path}\n"
            "Run Phase 1 first:  py run_phase1.py --client \"ClientName\" --round v1"
        )

    with open(phase1_json_path, encoding="utf-8") as fh:
        manifest = json.load(fh)

    input_base_dir = Path(input_base_dir)
    transforms = manifest.get("column_transforms", [])
    results: dict[str, dict[str, Any]] = {}

    for filename, fmeta in manifest.get("files", {}).items():
        file_path_str = fmeta.get("file_path", "")
        delimiter     = fmeta.get("delimiter") or "|"
        encoding      = fmeta.get("encoding")  or "utf-8"

        # Resolve file path
        resolved_path = _find_file(filename, file_path_str, input_base_dir)

        df: pd.DataFrame | None = None
        col_mappings = fmeta.get("column_mappings", [])
        if resolved_path and resolved_path.exists():
            df = _load_df(resolved_path, delimiter, encoding)
            if df is not None and transforms:
                _apply_transforms(df, col_mappings, transforms)
        else:
            print(f"  WARNING: Could not locate '{filename}' — datatype checks will be skipped.")

        results[filename] = {
            "df":               df,
            "source":           fmeta.get("source", "unknown"),
            "staging_table":    fmeta.get("staging_table", "(unknown)"),
            "column_mappings":  fmeta.get("column_mappings", []),
            "file_path":        file_path_str,
            "delimiter":        delimiter,
            "encoding":         encoding,
            "row_count":        fmeta.get("row_count", 0),
            "col_count":        fmeta.get("col_count", 0),
            "unmapped_raw":     fmeta.get("unmapped_raw", []),
            "uncovered_staging": fmeta.get("uncovered_staging", {}),
            "parse_issues":     fmeta.get("parse_issues", []),
        }

    return results


def load_manifest(phase1_json_path: str | Path) -> dict[str, Any]:
    """Return the full phase1_findings.json dict."""
    with open(phase1_json_path, encoding="utf-8") as fh:
        return json.load(fh)


# ---------------------------------------------------------------------------
# Column transform helpers
# ---------------------------------------------------------------------------

# Compiled regexes for position-expression evaluation inside SUBSTRING
_CHARINDEX_POS_RE = re.compile(
    r"^\s*CHARINDEX\s*\(\s*'([^']*)'\s*,\s*x\s*\)\s*(?:([+-])\s*(\d+))?\s*$", re.I
)
_LEN_POS_RE = re.compile(
    r"^\s*LEN\s*\(\s*x\s*\)\s*(?:([+-])\s*(\d+))?\s*$", re.I
)
_SUBSTRING_RE = re.compile(
    r"^\s*SUBSTRING\s*\(\s*x\s*,\s*(.*)\s*\)\s*$", re.I | re.DOTALL
)


def _eval_pos(expr: str, s: str) -> int:
    """
    Evaluate a numeric position/length expression for use as a SUBSTRING argument.

    Accepts:
        n                          — literal integer
        CHARINDEX('needle', x)     — 1-indexed position of needle in s (0 if absent)
        CHARINDEX('needle', x) ± n — position with offset
        LEN(x)                     — len(s)
        LEN(x) ± n                 — length with offset
    """
    expr = expr.strip()
    if expr.lstrip("-").isdigit():
        return int(expr)
    m = _CHARINDEX_POS_RE.match(expr)
    if m:
        needle, op, offset = m.group(1), m.group(2), m.group(3)
        pos = s.find(needle) + 1  # 1-indexed; 0 if not found
        if op and offset:
            pos = pos + int(offset) if op == "+" else pos - int(offset)
        return max(0, pos)
    m = _LEN_POS_RE.match(expr)
    if m:
        op, offset = m.group(1), m.group(2)
        length = len(s)
        if op and offset:
            length = length + int(offset) if op == "+" else length - int(offset)
        return max(0, length)
    return 0  # unrecognised — safe fallback


def _split_args(inner: str) -> tuple[str, str]:
    """
    Split ``start_expr, len_expr`` on the first comma that is not inside
    parentheses or single-quoted strings.
    """
    depth = 0
    in_quote = False
    for i, ch in enumerate(inner):
        if ch == "'" and not in_quote:
            in_quote = True
        elif ch == "'" and in_quote:
            in_quote = False
        elif not in_quote:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            elif ch == "," and depth == 0:
                return inner[:i].strip(), inner[i + 1:].strip()
    return inner.strip(), ""  # fallback


def _apply_formula(formula: str, value: str) -> str:
    """
    Apply a SQL-style formula string to *value* (treated as a string).

    Supported functions (positions are 1-indexed):
        LEFT(x, n)                        — first n characters
        RIGHT(x, n)                       — last n characters
        SUBSTRING(x, start_expr, len_expr)— substring; start/len may use CHARINDEX or LEN
        SPLIT_PART(x, 'delim', n)         — nth segment when split by delimiter
        CHARINDEX('needle', x)            — 1-indexed position of needle (standalone)
        LEN(x)                            — string length (standalone)
        TRIM(x)                           — strip whitespace
        UPPER(x)                          — uppercase
        LOWER(x)                          — lowercase
        REPLACE(x, 'old', 'new')          — replace substring

    Segments may be concatenated with ``||``.
    """
    s = str(value)
    parts: list[str] = []
    for seg in formula.split("||"):
        seg = seg.strip()

        m = re.match(r"^\s*LEFT\s*\(\s*x\s*,\s*(\d+)\s*\)\s*$", seg, re.I)
        if m:
            parts.append(s[:int(m.group(1))])
            continue

        m = re.match(r"^\s*RIGHT\s*\(\s*x\s*,\s*(\d+)\s*\)\s*$", seg, re.I)
        if m:
            n = int(m.group(1))
            parts.append(s[-n:] if n else "")
            continue

        m = _SUBSTRING_RE.match(seg)
        if m:
            start_expr, len_expr = _split_args(m.group(1))
            start  = _eval_pos(start_expr, s)
            length = _eval_pos(len_expr, s)
            parts.append(s[start - 1 : start - 1 + length] if start > 0 else "")
            continue

        m = re.match(r"^\s*SPLIT_PART\s*\(\s*x\s*,\s*'([^']*)'\s*,\s*(\d+)\s*\)\s*$", seg, re.I)
        if m:
            delim, n = m.group(1), int(m.group(2))
            sp = s.split(delim)
            parts.append(sp[n - 1] if 1 <= n <= len(sp) else "")
            continue

        m = re.match(r"^\s*CHARINDEX\s*\(\s*'([^']*)'\s*,\s*x\s*\)\s*$", seg, re.I)
        if m:
            pos = s.find(m.group(1)) + 1
            parts.append(str(max(0, pos)))
            continue

        m = re.match(r"^\s*LEN\s*\(\s*x\s*\)\s*$", seg, re.I)
        if m:
            parts.append(str(len(s)))
            continue

        m = re.match(r"^\s*TRIM\s*\(\s*x\s*\)\s*$", seg, re.I)
        if m:
            parts.append(s.strip())
            continue

        m = re.match(r"^\s*(UPPER|LOWER)\s*\(\s*x\s*\)\s*$", seg, re.I)
        if m:
            parts.append(s.upper() if m.group(1).upper() == "UPPER" else s.lower())
            continue

        m = re.match(r"^\s*REPLACE\s*\(\s*x\s*,\s*'([^']*)'\s*,\s*'([^']*)'\s*\)\s*$", seg, re.I)
        if m:
            parts.append(s.replace(m.group(1), m.group(2)))
            continue

        parts.append(seg)  # unrecognised — pass through as literal
    return "".join(parts)


def _apply_transforms(
    df: "pd.DataFrame",
    column_mappings: list[dict],
    transforms: list[dict],
) -> None:
    """
    Apply column transforms in-place to *df*.

    For each transform rule, resolve the staging column name to its raw
    column name via *column_mappings*, then overwrite that column with
    the formula-transformed values.
    """
    for t in transforms:
        staging_col = t.get("staging_column", "")
        formula     = t.get("formula", "")
        if not staging_col or not formula:
            continue
        raw_col = resolve_column(column_mappings, staging_col)
        if raw_col and raw_col in df.columns:
            df[raw_col] = (
                df[raw_col]
                .fillna("")
                .astype(str)
                .apply(lambda v, f=formula: _apply_formula(f, v))
            )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _find_file(
    filename: str,
    stored_path: str,
    input_base_dir: Path,
) -> Path | None:
    """
    Locate the source file using stored path first, then by searching
    input_base_dir and its immediate subdirectories.
    """
    # Try stored absolute path
    if stored_path:
        p = Path(stored_path)
        if p.exists():
            return p

    # Search input directory for the filename
    for candidate in input_base_dir.rglob(filename):
        return candidate

    return None


def _load_df(
    file_path: Path,
    delimiter: str,
    encoding: str,
) -> pd.DataFrame | None:
    """Re-parse a file using the delimiter and encoding recorded by Phase 1."""
    try:
        df = pd.read_csv(
            file_path,
            sep=delimiter,
            dtype=str,
            encoding=encoding,
            engine="c",
            on_bad_lines="warn",
            keep_default_na=False,
        )
        if df.shape[1] < 2:
            return None
        df.columns = [str(c).strip() for c in df.columns]
        df = df.replace("", pd.NA).dropna(how="all").dropna(axis=1, how="all").fillna("")
        df.reset_index(drop=True, inplace=True)
        return df
    except Exception as exc:
        print(f"  WARNING: Could not reload '{file_path.name}': {exc}")
        return None
