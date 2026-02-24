"""
phase1/report.py

Renders the Phase 1 results to:
  1. Console  — per-file summary boxes + test-month alignment block
  2. Excel    — 5-sheet workbook in output/
  3. JSON     — phase1_findings.json in output/
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd


# ---------------------------------------------------------------------------
# Console rendering
# ---------------------------------------------------------------------------

_BOX_WIDTH = 67


def _line(left: str = "", right: str = "", sep: str = "│") -> str:
    """Render one row of the summary box.

    Layout: │ {left:<20} {sep} {right:<right_width} │
    Widths: 1+1+20+1+1+1+right_width+1+1 = 27+right_width = _BOX_WIDTH
    """
    right_width = _BOX_WIDTH - 27
    right_padded = right[:right_width].ljust(right_width)
    return f"│ {left:<20s} {sep} {right_padded} │"


def _divider(char: str = "─") -> str:
    # 1(├) + 22(─) + 1(┬) + (_BOX_WIDTH-25)(─) + 1(┤) = _BOX_WIDTH
    return "├" + "─" * 22 + "┬" + "─" * (_BOX_WIDTH - 25) + "┤"


def _full_divider() -> str:
    return "├" + "─" * (_BOX_WIDTH - 2) + "┤"


def _top() -> str:
    return "┌" + "─" * (_BOX_WIDTH - 2) + "┐"


def _bottom() -> str:
    return "└" + "─" * (_BOX_WIDTH - 2) + "┘"


def _header(title: str) -> str:
    pad = _BOX_WIDTH - 4
    return f"│ {title:<{pad}} │"


def render_file_summary(
    filename: str,
    meta: dict[str, Any],
    source: str,
    staging_table: str,
    mapping_records: list[dict[str, Any]],
    df: "pd.DataFrame | None",
) -> None:
    from .source_detection import SOURCE_TO_STAGING

    exact   = sum(1 for r in mapping_records if r["confidence"] == "EXACT")
    norm    = sum(1 for r in mapping_records if r["confidence"] == "NORMALIZED")
    fuzzy   = sum(1 for r in mapping_records if r["confidence"].startswith("FUZZY"))
    unmapped = sum(1 for r in mapping_records if r["confidence"] == "UNMAPPED")
    unmapped_names = [r["raw_col"] for r in mapping_records if r["confidence"] == "UNMAPPED"]
    dual    = sum(1 for r in mapping_records if len(r.get("staging_cols", [])) > 1)
    dual_names = [r["raw_col"] for r in mapping_records if len(r.get("staging_cols", [])) > 1]

    # Staging coverage — how many REQUIRED staging cols are mapped
    covered_stg = set()
    for r in mapping_records:
        covered_stg.update(r.get("staging_cols", []))
    from .column_mapping import REQUIRED_STAGING_COLS
    req_cols = REQUIRED_STAGING_COLS.get(staging_table, [])
    all_stg_cols_count = len(req_cols)
    covered_required_count = sum(1 for c in req_cols if c in covered_stg)

    delimiter_label = {"|": "Pipe-delimited", ",": "Comma-delimited", "\t": "Tab-delimited"}.get(
        meta.get("delimiter", ""), "Unknown"
    )
    ext = meta.get("ext", "").lstrip(".")
    format_str = f"{delimiter_label} .{ext}"

    parse_issues = meta.get("parse_issues", [])
    issues_str = "; ".join(parse_issues) if parse_issues else "None"

    source_label = source.replace("_", " ").title()
    unmapped_str = f"{unmapped} columns"
    if unmapped_names:
        sample = ", ".join(unmapped_names[:3])
        if len(unmapped_names) > 3:
            sample += f" + {len(unmapped_names)-3} more"
        unmapped_str += f" — [{sample}]"

    dual_str = f"{dual}"
    if dual_names:
        sample = ", ".join(dual_names[:3])
        dual_str += f" — [{sample}]"

    print(_top())
    print(_header("FILE SUMMARY"))
    print(_divider())
    print(_line("File Name",            filename))
    print(_line("Detected Source",      source_label))
    print(_line("Target Staging Table", staging_table))
    print(_line("Format",               format_str))
    print(_line("Record Count",         f"{meta.get('row_count', 0):,}"))
    print(_line("Column Count",         str(meta.get("raw_col_count", 0))))
    print(_line("Headers Present",      "Yes"))
    print(_line("Footer Stripped",      str(meta.get("footer_rows_stripped", 0)) + " row(s)"))
    print(_full_divider())
    print(_header("COLUMN MAPPING RESULTS"))
    print(_divider())
    print(_line("EXACT match",          f"{exact} columns"))
    print(_line("NORMALIZED match",     f"{norm} columns"))
    print(_line("FUZZY match (review)", f"{fuzzy} columns"))
    print(_line("UNMAPPED",             unmapped_str))
    if staging_table and not staging_table.startswith("("):
        print(_line("Staging cols covered", f"{covered_required_count} of {all_stg_cols_count} required"))
    print(_line("Dual-map cols found",  dual_str))
    print(_line("Parse Issues",         issues_str[:_BOX_WIDTH - 25]))
    if df is not None and len(df) > 0:
        print(_full_divider())
        print(_header("SAMPLE DATA (first 3 rows)"))
        sample_cols = list(df.columns[:6])
        header_row = " | ".join(c[:15] for c in sample_cols)
        print(_header(header_row[:_BOX_WIDTH - 4]))
        for _, row in df.head(3).iterrows():
            row_str = " | ".join(str(row[c])[:15] for c in sample_cols)
            print(_header(row_str[:_BOX_WIDTH - 4]))
    print(_bottom())
    print()


def render_test_month(month_results: dict[str, Any]) -> None:
    test_month = month_results.get("test_month")
    aligned    = month_results.get("aligned", False)
    per_file   = month_results.get("per_file", {})

    print()
    print("TEST MONTH ALIGNMENT")
    print("─" * _BOX_WIDTH)
    for filename, info in per_file.items():
        source  = info.get("source", "unknown")
        min_d   = info.get("min_date", "N/A") or "N/A"
        max_d   = info.get("max_date", "N/A") or "N/A"
        implied = info.get("implied_month", "N/A") or "N/A"
        field   = info.get("filter_field", "N/A") or "N/A"
        note    = info.get("note", "")
        label   = source.replace("_", " ").title()
        status  = "✓" if implied == test_month else "⚠ MISALIGNED"
        print(f"  {label:<30s}  {field} {min_d} → {max_d}  →  Test Month: {implied}  {status}")
        if note:
            print(f"    ({note})")
    print("─" * _BOX_WIDTH)
    if test_month:
        if aligned:
            print(f"  ✅ All files aligned to {test_month}")
        else:
            print(f"  ⚠  MISALIGNMENT DETECTED — consensus month: {test_month}")
    else:
        print("  ⚠  Could not determine test month")
    print()


# ---------------------------------------------------------------------------
# Excel output
# ---------------------------------------------------------------------------

def write_excel_report(
    output_dir:         str | Path,
    client:             str,
    round_:             str,
    file_dict:          dict[str, dict[str, Any]],
    source_assignments: dict[str, str],
    billing_format:     dict[str, Any],
    mapping_results:    dict[str, list[dict[str, Any]]],
    month_results:      dict[str, Any],
) -> Path:
    from .source_detection import SOURCE_TO_STAGING
    from .column_mapping import get_uncovered_staging_cols, REQUIRED_STAGING_COLS

    output_dir = Path(output_dir) / client
    output_dir.mkdir(parents=True, exist_ok=True)

    today = date.today().strftime("%Y%m%d")
    out_path = output_dir / f"{client}_{round_}_Phase1_{today}.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        # ---- Sheet 1: File Inventory ----
        inv_rows = []
        for fn, meta in file_dict.items():
            source  = source_assignments.get(fn, "unknown")
            tbl     = SOURCE_TO_STAGING.get(source, "(unknown)")
            records = mapping_results.get(fn, [])
            inv_rows.append({
                "File Name":            fn,
                "Detected Source":      source.replace("_", " ").title(),
                "Target Staging Table": tbl,
                "Format":               meta.get("delimiter", "?") + "-delimited " + meta.get("ext", ""),
                "Record Count":         meta.get("row_count", 0),
                "Column Count":         meta.get("raw_col_count", 0),
                "Parse Issues":         "; ".join(meta.get("parse_issues", [])) or "None",
                "EXACT":                sum(1 for r in records if r["confidence"] == "EXACT"),
                "NORMALIZED":           sum(1 for r in records if r["confidence"] == "NORMALIZED"),
                "FUZZY":                sum(1 for r in records if r["confidence"].startswith("FUZZY")),
                "UNMAPPED":             sum(1 for r in records if r["confidence"] == "UNMAPPED"),
            })
        pd.DataFrame(inv_rows).to_excel(writer, sheet_name="File Inventory", index=False)

        # ---- Sheet 2: Column Mappings ----
        map_rows = []
        for fn, records in mapping_results.items():
            source = source_assignments.get(fn, "unknown")
            tbl    = SOURCE_TO_STAGING.get(source, "(unknown)")
            for r in records:
                map_rows.append({
                    "File":             fn,
                    "Source":           source,
                    "Staging Table":    tbl,
                    "Raw Column":       r["raw_col"],
                    "Staging Column":   r.get("staging_col") or "",
                    "All Staging Cols": ", ".join(r.get("staging_cols", [])),
                    "Confidence":       r["confidence"],
                    "Fuzzy Score":      r.get("fuzzy_score") or "",
                    "SQL Type":         r.get("sql_type") or "",
                    "Max Length":       r.get("max_length") or "",
                    "Precision":        r.get("precision") or "",
                    "Scale":            r.get("scale") or "",
                    "Notes":            r.get("notes") or "",
                })
        pd.DataFrame(map_rows).to_excel(writer, sheet_name="Column Mappings", index=False)

        # ---- Sheet 3: Mapping Gaps ----
        gap_rows = []
        for fn, records in mapping_results.items():
            source = source_assignments.get(fn, "unknown")
            tbl    = SOURCE_TO_STAGING.get(source, "(unknown)")
            # Unmapped raw columns
            for r in records:
                if r["confidence"] == "UNMAPPED":
                    gap_rows.append({
                        "File":          fn,
                        "Gap Type":      "UNMAPPED Raw Column",
                        "Column":        r["raw_col"],
                        "Staging Table": tbl,
                        "Notes":         "Not in staging schema — may be client-specific or optional",
                    })
            # Uncovered required/recommended staging cols
            uncovered = get_uncovered_staging_cols(fn, records, source_assignments)
            for col in uncovered.get("required", []):
                gap_rows.append({
                    "File":          fn,
                    "Gap Type":      "UNCOVERED (Required)",
                    "Column":        col,
                    "Staging Table": tbl,
                    "Notes":         "Required staging column — no raw column mapped to it",
                })
            for col in uncovered.get("recommended", []):
                gap_rows.append({
                    "File":          fn,
                    "Gap Type":      "UNCOVERED (Recommended)",
                    "Column":        col,
                    "Staging Table": tbl,
                    "Notes":         "Recommended staging column — no raw column mapped to it",
                })
            # Dual-mapped columns
            for r in records:
                if len(r.get("staging_cols", [])) > 1:
                    gap_rows.append({
                        "File":          fn,
                        "Gap Type":      "DUAL-MAPPED",
                        "Column":        r["raw_col"],
                        "Staging Table": tbl,
                        "Notes":         r.get("notes", ""),
                    })
        pd.DataFrame(gap_rows).to_excel(writer, sheet_name="Mapping Gaps", index=False)

        # ---- Sheet 4: Test Month ----
        tm_rows = []
        per_file = month_results.get("per_file", {})
        consensus = month_results.get("test_month", "")
        for fn, info in per_file.items():
            implied = info.get("implied_month", "")
            tm_rows.append({
                "File":           fn,
                "Source":         info.get("source", ""),
                "Filter Field":   info.get("filter_field", ""),
                "Min Date":       info.get("min_date", ""),
                "Max Date":       info.get("max_date", ""),
                "Implied Month":  implied,
                "Consensus Month": consensus,
                "Aligned":        "✓" if implied == consensus else "⚠ MISALIGNED",
                "Note":           info.get("note", ""),
            })
        pd.DataFrame(tm_rows).to_excel(writer, sheet_name="Test Month", index=False)

        # ---- Sheet 5: Submission Metadata ----
        from datetime import datetime
        files_present = [fn for fn, src in source_assignments.items() if src != "unknown"]
        unknown_files = [fn for fn, src in source_assignments.items() if src == "unknown"]
        meta_rows = [
            {"Field": "Client Name",        "Value": client},
            {"Field": "Submission Round",   "Value": round_},
            {"Field": "Date Run",           "Value": datetime.now().strftime("%Y-%m-%d %H:%M")},
            {"Field": "Test Month",         "Value": month_results.get("test_month", "Unknown")},
            {"Field": "Billing Format",     "Value": billing_format.get("format", "unknown")},
            {"Field": "Billing Notes",      "Value": billing_format.get("notes", "")},
            {"Field": "Files Present",      "Value": ", ".join(files_present)},
            {"Field": "Files Unknown",      "Value": ", ".join(unknown_files) or "None"},
            {"Field": "Month Aligned",      "Value": str(month_results.get("aligned", False))},
        ]
        pd.DataFrame(meta_rows).to_excel(writer, sheet_name="Submission Metadata", index=False)

    return out_path


# ---------------------------------------------------------------------------
# JSON manifest
# ---------------------------------------------------------------------------

def write_json_manifest(
    output_dir:         str | Path,
    client:             str,
    round_:             str,
    file_dict:          dict[str, dict[str, Any]],
    source_assignments: dict[str, str],
    billing_format:     dict[str, Any],
    mapping_results:    dict[str, list[dict[str, Any]]],
    month_results:      dict[str, Any],
    date_start:         str | None = None,
    date_end:           str | None = None,
    column_transforms:  list[dict] | None = None,
) -> Path:
    from .source_detection import SOURCE_TO_STAGING
    from .column_mapping import get_uncovered_staging_cols
    from datetime import datetime

    output_dir = Path(output_dir) / client
    output_dir.mkdir(parents=True, exist_ok=True)

    per_file_dates = month_results.get("per_file", {})

    files_payload: dict[str, Any] = {}
    for fn, meta in file_dict.items():
        source  = source_assignments.get(fn, "unknown")
        tbl     = SOURCE_TO_STAGING.get(source, "(unknown)")
        records = mapping_results.get(fn, [])
        uncovered = get_uncovered_staging_cols(fn, records, source_assignments)

        # Date range from test_month analysis
        date_info = per_file_dates.get(fn, {})
        date_range = {
            "filter_field": date_info.get("filter_field"),
            "min_date":     date_info.get("min_date"),
            "max_date":     date_info.get("max_date"),
            "note":         date_info.get("note", ""),
        }

        files_payload[fn] = {
            "source":             source,
            "staging_table":      tbl,
            "file_path":          meta.get("file_path", ""),
            "delimiter":          meta.get("delimiter", ""),
            "encoding":           meta.get("encoding", "utf-8"),
            "row_count":          meta.get("row_count", 0),
            "col_count":          meta.get("raw_col_count", 0),
            "parse_issues":       meta.get("parse_issues", []),
            "date_range":         date_range,
            "column_mappings":    [
                {
                    "raw_col":      r["raw_col"],
                    "staging_col":  r.get("staging_col"),
                    "staging_cols": r.get("staging_cols", []),
                    "confidence":   r["confidence"],
                    "sql_type":     r.get("sql_type"),
                    "max_length":   r.get("max_length"),
                    "precision":    r.get("precision"),
                    "scale":        r.get("scale"),
                    "notes":        r.get("notes", ""),
                }
                for r in records
            ],
            "unmapped_raw":       [r["raw_col"] for r in records if r["confidence"] == "UNMAPPED"],
            "uncovered_staging":  uncovered,
        }

    # Build per-source expected date ranges (null if not provided → Phase 3 skips check)
    if date_start and date_end:
        expected_date_ranges = {
            src: {"start": date_start, "end": date_end}
            for src in ("billing", "scheduling", "payroll", "gl", "quality", "patient_satisfaction")
        }
    else:
        expected_date_ranges = None

    manifest = {
        "client":               client,
        "round":                round_,
        "date_run":             datetime.now().strftime("%Y-%m-%d %H:%M"),
        "test_month":           month_results.get("test_month"),
        "month_aligned":        month_results.get("aligned", False),
        "billing_format":       billing_format.get("format"),
        "billing_notes":        billing_format.get("notes", ""),
        "expected_date_ranges": expected_date_ranges,
        "column_transforms":    column_transforms or [],
        "files":                files_payload,
    }

    out_path = output_dir / "phase1_findings.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, default=str)

    return out_path


# ---------------------------------------------------------------------------
# Master render function
# ---------------------------------------------------------------------------

def render(
    file_dict:          dict[str, dict[str, Any]],
    source_assignments: dict[str, str],
    billing_format:     dict[str, Any],
    mapping_results:    dict[str, list[dict[str, Any]]],
    month_results:      dict[str, Any],
    output_dir:         str | Path,
    client:             str,
    round_:             str,
    date_start:         str | None = None,
    date_end:           str | None = None,
    column_transforms:  list[dict] | None = None,
) -> None:
    from .source_detection import SOURCE_TO_STAGING

    print()
    print("=" * _BOX_WIDTH)
    print(f"  PIVOT TEST FILE REVIEW — Phase 1")
    print(f"  Client: {client}   Round: {round_}")
    print("=" * _BOX_WIDTH)

    # Per-file summaries
    for filename, meta in file_dict.items():
        df = meta.get("df")
        source  = source_assignments.get(filename, "unknown")
        tbl     = SOURCE_TO_STAGING.get(source, "(unknown)")
        records = mapping_results.get(filename, [])
        render_file_summary(filename, meta, source, tbl, records, df)

    # Test month alignment
    render_test_month(month_results)

    # Billing format summary
    fmt = billing_format.get("format", "unknown")
    print(f"BILLING FORMAT: {fmt.upper()}")
    print(f"  {billing_format.get('notes', '')}")
    print()

    # Write outputs
    excel_path = write_excel_report(
        output_dir, client, round_,
        file_dict, source_assignments, billing_format,
        mapping_results, month_results,
    )
    json_path = write_json_manifest(
        output_dir, client, round_,
        file_dict, source_assignments, billing_format,
        mapping_results, month_results,
        date_start=date_start, date_end=date_end,
        column_transforms=column_transforms,
    )

    print("─" * _BOX_WIDTH)
    print(f"  Excel report : {excel_path}")
    print(f"  JSON manifest: {json_path}")
    print("─" * _BOX_WIDTH)
