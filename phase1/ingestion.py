"""
phase1/ingestion.py

Scans the input directory, parses every .txt/.csv file into a DataFrame,
and returns per-file metadata.  Handles pipe-delimited files with fallback
delimiter detection.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import chardet
import pandas as pd

SAMPLE_ROW_LIMIT = 10_000   # max rows loaded into memory per file
FOOTER_TAIL_ROWS = 20       # rows read from file tail for footer detection


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def ingest_directory(input_dir: str | Path) -> dict[str, dict[str, Any]]:
    """
    Scan *input_dir* and parse every supported file.

    Also scans first-level subdirectories. Files found inside a subdirectory
    have ``source_folder`` set to the subdirectory name (e.g. ``"billing_charges"``),
    which source_detection uses as an implicit source override when the name
    matches a known source type.

    Returns
    -------
    dict keyed by filename (basename only).  Each value::

        {
            "df":                   pd.DataFrame | None,
            "ext":                  str,          # ".txt" or ".csv"
            "raw_col_count":        int,
            "row_count":            int,
            "delimiter":            str | None,
            "encoding":             str,
            "parse_issues":         list[str],
            "footer_rows_stripped": int,
            "source_folder":        str | None,   # subfolder name, or None for top-level
        }
    """
    input_dir = Path(input_dir)
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    results: dict[str, dict[str, Any]] = {}

    # Config files that live in the input directory but are NOT data files
    _CONFIG_FILES = {"sources.csv", "column_transforms.csv"}

    # Top-level files (no source subfolder — fall back to auto-detection)
    for file_path in sorted(input_dir.iterdir()):
        if not file_path.is_file():
            continue
        if file_path.name.lower() in _CONFIG_FILES:
            continue
        if file_path.suffix.lower() not in (".txt", ".csv"):
            continue
        meta = _parse_file(file_path)
        meta["source_folder"] = None
        meta["file_path"] = str(file_path.resolve())
        results[file_path.name] = meta

    # First-level subdirectories (source-named folders)
    for subdir in sorted(input_dir.iterdir()):
        if not subdir.is_dir():
            continue
        for file_path in sorted(subdir.iterdir()):
            if not file_path.is_file():
                continue
            if file_path.suffix.lower() not in (".txt", ".csv"):
                continue
            meta = _parse_file(file_path)
            meta["source_folder"] = subdir.name
            meta["file_path"] = str(file_path.resolve())
            results[file_path.name] = meta

    return results


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_FOOTER_PAT = re.compile(
    r"^\s*(total|grand\s*total|record\s*count|count|eof|end\s*of\s*file|summary|page)\s*$",
    re.IGNORECASE,
)


def _detect_encoding(file_path: Path) -> str:
    """Return chardet-detected encoding; fall back to utf-8."""
    with open(file_path, "rb") as fh:
        raw = fh.read(min(1_000_000, file_path.stat().st_size))
    result = chardet.detect(raw)
    enc = result.get("encoding") or "utf-8"
    # chardet sometimes returns 'ascii' for valid utf-8 supersets
    if enc.lower() in ("ascii", "windows-1252"):
        enc = "utf-8-sig"
    return enc


def _try_parse(file_path: Path, encoding: str, delimiter: str, nrows: int | None = None) -> pd.DataFrame | None:
    """Try to parse with a specific delimiter; return None on failure."""
    try:
        df = pd.read_csv(
            file_path,
            sep=delimiter,
            dtype=str,
            encoding=encoding,
            engine="c",
            on_bad_lines="warn",
            keep_default_na=False,
            nrows=nrows,
        )
        if df.shape[1] < 2:
            return None
        return df
    except Exception:
        # Fall back to Python engine if C engine can't handle this file
        try:
            df = pd.read_csv(
                file_path,
                sep=delimiter,
                dtype=str,
                encoding=encoding,
                engine="python",
                on_bad_lines="warn",
                keep_default_na=False,
                nrows=nrows,
            )
            if df.shape[1] < 2:
                return None
            return df
        except Exception:
            return None


def _count_lines(file_path: Path, encoding: str) -> int:
    """Count data rows (excluding header) using fast binary chunk reads."""
    count = 0
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):  # 1 MB chunks
            count += chunk.count(b"\n")
    return max(0, count - 1)  # subtract the header newline


def _read_tail(file_path: Path, encoding: str, delimiter: str, total_data_rows: int, n: int = FOOTER_TAIL_ROWS) -> pd.DataFrame | None:
    """Read the last n data rows using a binary seek — no full-file scan."""
    import io
    CHUNK = 65_536  # 64 KB — enough for any realistic footer section
    try:
        file_size = file_path.stat().st_size
        read_start = max(0, file_size - CHUNK)
        with open(file_path, "rb") as fh:
            if read_start > 0:
                fh.seek(read_start)
            tail_bytes = fh.read()

        tail_text = tail_bytes.decode(encoding, errors="replace")
        lines = [l for l in tail_text.splitlines() if l.strip()]

        # Re-read only the header line (cheap: one readline)
        with open(file_path, "r", encoding=encoding, errors="replace") as fh:
            header_line = fh.readline().rstrip("\n")

        data_lines = lines[-n:]
        csv_text = header_line + "\n" + "\n".join(data_lines) + "\n"

        return pd.read_csv(
            io.StringIO(csv_text),
            sep=delimiter,
            dtype=str,
            engine="python",
            on_bad_lines="warn",
            keep_default_na=False,
        )
    except Exception:
        return None


def _strip_footers(df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
    """Remove trailing footer rows identified by the first-column value."""
    count = 0
    while len(df) > 0:
        first_val = str(df.iloc[-1, 0]).strip()
        if _FOOTER_PAT.match(first_val):
            df = df.iloc[:-1]
            count += 1
        else:
            break
    return df.reset_index(drop=True), count


def _check_embedded_pipes(df: pd.DataFrame) -> list[str]:
    issues: list[str] = []
    for col in df.columns:
        if df[col].str.contains(r"\|", regex=True, na=False).any():
            issues.append(f"Column '{col}' contains embedded pipe characters")
    return issues


def _has_encoding_artifacts(df: pd.DataFrame) -> bool:
    sample = df.iloc[:, 0].head(50).str.cat(sep=" ")
    return any(marker in sample for marker in ("â€", "Ã", "\ufffd", "Â"))


def _parse_file(file_path: Path) -> dict[str, Any]:
    encoding = _detect_encoding(file_path)
    parse_issues: list[str] = []
    df: pd.DataFrame | None = None
    used_delimiter: str | None = None

    for delimiter, label in (("|", "pipe"), (",", "comma"), ("\t", "tab")):
        df = _try_parse(file_path, encoding, delimiter, nrows=SAMPLE_ROW_LIMIT)
        if df is not None:
            used_delimiter = delimiter
            if label != "pipe":
                parse_issues.append(
                    f"Pipe-delimited parse failed; used {label} delimiter as fallback"
                )
            break

    if df is None:
        return {
            "df": None,
            "ext": file_path.suffix.lower(),
            "raw_col_count": 0,
            "row_count": 0,
            "delimiter": None,
            "encoding": encoding,
            "parse_issues": ["Could not parse with pipe, comma, or tab delimiter"],
            "footer_rows_stripped": 0,
            "source_folder": None,
        }

    # Clean column names
    df.columns = [str(c).strip() for c in df.columns]

    # Drop empty rows/columns
    df = df.replace("", pd.NA)
    df.dropna(how="all", inplace=True)
    df.dropna(axis=1, how="all", inplace=True)
    df = df.fillna("")
    df.reset_index(drop=True, inplace=True)

    raw_col_count = df.shape[1]

    # Accurate row count from a fast line scan (not len(df), which is only the sample)
    total_data_rows = _count_lines(file_path, encoding)
    is_large_file = total_data_rows > SAMPLE_ROW_LIMIT

    # Footer detection
    if is_large_file:
        # Read only the file tail to check for footer rows
        tail_df = _read_tail(file_path, encoding, used_delimiter, total_data_rows)
        if tail_df is not None:
            tail_df.columns = [str(c).strip() for c in tail_df.columns]
            _, footer_count = _strip_footers(tail_df)
        else:
            footer_count = 0
        row_count = total_data_rows - footer_count
    else:
        df, footer_count = _strip_footers(df)
        row_count = len(df)

    if footer_count:
        parse_issues.append(f"{footer_count} footer row(s) stripped")

    # Encoding check
    if _has_encoding_artifacts(df):
        parse_issues.append("Possible encoding issues detected (garbled characters)")

    # Embedded pipe check
    parse_issues.extend(_check_embedded_pipes(df))

    if is_large_file:
        parse_issues.append(
            f"Large file: sampled first {SAMPLE_ROW_LIMIT:,} of {row_count:,} rows for column analysis"
        )

    return {
        "df": df,
        "ext": file_path.suffix.lower(),
        "raw_col_count": raw_col_count,
        "row_count": row_count,
        "delimiter": used_delimiter,
        "encoding": encoding,
        "parse_issues": parse_issues,
        "footer_rows_stripped": footer_count,
    }
