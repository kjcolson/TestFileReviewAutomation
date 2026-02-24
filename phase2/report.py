"""
phase2/report.py

Renders Phase 2 results to:
  1. Console — per-file schema validation boxes + overall compatibility table
  2. Excel   — 5-sheet workbook in output/{client}/
  3. JSON    — phase2_findings.json in output/{client}/
"""

from __future__ import annotations

import json
from datetime import date, datetime
from pathlib import Path
from typing import Any

import pandas as pd


_BOX_WIDTH = 69


# ---------------------------------------------------------------------------
# Console rendering
# ---------------------------------------------------------------------------

def _top() -> str:
    return "+" + "-" * (_BOX_WIDTH - 2) + "+"

def _bottom() -> str:
    return "+" + "-" * (_BOX_WIDTH - 2) + "+"

def _full_div() -> str:
    return "+" + "-" * (_BOX_WIDTH - 2) + "+"

def _hdr(text: str) -> str:
    pad = _BOX_WIDTH - 4
    return f"| {text:<{pad}} |"

def _row(left: str, right: str) -> str:
    right_w = _BOX_WIDTH - 27
    return f"| {left:<22s} | {right[:right_w]:<{right_w}s} |"

def _sev_icon(sev: str | None) -> str:
    return {"CRITICAL": "X", "HIGH": "!", "MEDIUM": "o", "INFO": ".", "LOW": "."}.get(sev or "", ".")


def render_file_box(filename: str, file_results: dict[str, Any]) -> None:
    source        = file_results.get("source", "unknown")
    staging_table = file_results.get("staging_table", "(unknown)")
    schema        = file_results.get("schema_results", {})
    summary       = schema.get("summary", {})
    dtype_findings = file_results.get("datatype_findings", [])
    unrecog       = file_results.get("unrecognized_results", {})
    unrec_list    = unrecog.get("unrecognized_findings", [])
    fuzzy_list    = unrecog.get("fuzzy_review_list", [])

    req_p = summary.get("required_present",     0)
    req_t = summary.get("required_total",        0)
    rec_p = summary.get("recommended_present",   0)
    rec_t = summary.get("recommended_total",     0)
    opt_p = summary.get("optional_present",      0)
    opt_t = summary.get("optional_total",        0)

    print(_top())
    print(_hdr("SCHEMA VALIDATION"))
    print(_full_div())
    print(_row("File Name",       filename))
    print(_row("Source",          source.replace("_", " ").title()))
    print(_row("Staging Table",   staging_table))
    print(_full_div())
    print(_row("Required fields",    f"{req_p} / {req_t} present"))
    print(_row("Recommended fields", f"{rec_p} / {rec_t} present"))
    print(_row("Optional fields",    f"{opt_p} / {opt_t} present"))

    # Schema issues by severity
    schema_findings = schema.get("schema_findings", [])
    critical_schema = [f for f in schema_findings if f.get("severity") == "CRITICAL"]
    high_schema     = [f for f in schema_findings if f.get("severity") == "HIGH"]

    if critical_schema or high_schema:
        print(_full_div())
    if critical_schema:
        print(_hdr(f"CRITICAL ({len(critical_schema)})"))
        for f in critical_schema:
            lvl  = f.get("requirement_level", "").title()
            note = f" - {f['notes']}" if f.get("notes") else ""
            print(_hdr(f"  X {f['template_field']} - {lvl}, MISSING{note}"))
    if high_schema:
        print(_hdr(f"HIGH ({len(high_schema)})"))
        for f in high_schema:
            lvl  = f.get("requirement_level", "").title()
            note = f" - {f['notes']}" if f.get("notes") else ""
            print(_hdr(f"  ! {f['template_field']} - {lvl}, MISSING{note}"))

    # Data type issues
    dtype_with_sev = [f for f in dtype_findings if f.get("severity")]
    if dtype_with_sev:
        print(_full_div())
        print(_hdr(f"DATA TYPE ISSUES ({len(dtype_with_sev)})"))
        for f in dtype_with_sev[:10]:
            icon  = _sev_icon(f.get("severity"))
            col   = f.get("staging_column") or f.get("raw_column") or ""
            notes = f.get("notes", "")[:_BOX_WIDTH - 10]
            print(_hdr(f"  {icon} {col} - {notes}"))
        if len(dtype_with_sev) > 10:
            print(_hdr(f"  ... and {len(dtype_with_sev) - 10} more"))

    # Unrecognized columns
    if unrec_list:
        print(_full_div())
        print(_hdr(f"UNRECOGNIZED COLUMNS ({len(unrec_list)})"))
        for f in unrec_list[:8]:
            icon = _sev_icon(f.get("severity"))
            col  = f.get("raw_column", "")
            note = f.get("notes", "")
            print(_hdr(f"  {icon} {col} - {note[:_BOX_WIDTH - 10]}"))
        if len(unrec_list) > 8:
            print(_hdr(f"  ... and {len(unrec_list) - 8} more"))

    # Fuzzy review
    if fuzzy_list:
        print(_full_div())
        print(_hdr(f"FUZZY MATCHES NEEDING REVIEW ({len(fuzzy_list)})"))
        for f in fuzzy_list[:5]:
            raw  = f.get("raw_column", "")
            stg  = f.get("mapped_to_staging", "")
            conf = f.get("confidence", "")
            print(_hdr(f"  ? {raw} -> {stg} ({conf})"))
        if len(fuzzy_list) > 5:
            print(_hdr(f"  ... and {len(fuzzy_list) - 5} more"))

    print(_bottom())
    print()


def render_compatibility_table(all_results: dict[str, dict[str, Any]]) -> None:
    col_w = 33
    print()
    print("+" + "-" * (_BOX_WIDTH - 2) + "+")
    print(_hdr("DATABASE COMPATIBILITY SUMMARY"))
    hdr_line = (
        f"| {'File':<{col_w}} | {'CRIT':>4} | {'HIGH':>4} | "
        f"{'MED':>4} | {'Compatible?':<12} |"
    )
    print(hdr_line)
    print("+" + "-" * (col_w + 2) + "+" + "-" * 6 + "+" + "-" * 6 +
          "+" + "-" * 6 + "+" + "-" * 14 + "+")

    overall_ready = True
    for filename, res in all_results.items():
        crit   = res.get("critical_count", 0)
        high   = res.get("high_count",     0)
        med    = res.get("medium_count",   0)
        compat = res.get("compatible",     "?")
        if compat == "NO":
            overall_ready = False
        short_name = filename[:col_w]
        row = (
            f"| {short_name:<{col_w}} | {crit:>4} | {high:>4} | "
            f"{med:>4} | {compat:<12} |"
        )
        print(row)

    print("+" + "-" * (_BOX_WIDTH - 2) + "+")
    note = "* = Conditionally compatible (HIGH issues present)"
    if overall_ready:
        overall_msg = "READY - all files pass compatibility check"
    else:
        not_ready = [f for f, r in all_results.items() if r.get("compatible") == "NO"]
        overall_msg = f"NOT READY - {len(not_ready)} file(s) have CRITICAL issues"
    print(_hdr(note))
    print(_hdr(f"Overall: {overall_msg}"))
    print("+" + "-" * (_BOX_WIDTH - 2) + "+")
    print()


# ---------------------------------------------------------------------------
# Compatibility determination
# ---------------------------------------------------------------------------

def determine_compatibility(
    schema_findings: list[dict],
    datatype_findings: list[dict],
) -> tuple[str, int, int, int]:
    """
    Return (compatible_label, critical_count, high_count, medium_count).

    YES   — 0 CRITICAL, 0 HIGH
    YES*  — 0 CRITICAL, ≥1 HIGH
    NO    — ≥1 CRITICAL
    """
    all_findings = schema_findings + datatype_findings
    crit = sum(1 for f in all_findings if f.get("severity") == "CRITICAL")
    high = sum(1 for f in all_findings if f.get("severity") == "HIGH")
    med  = sum(1 for f in all_findings if f.get("severity") == "MEDIUM")

    if crit > 0:
        label = "NO"
    elif high > 0:
        label = "YES*"
    else:
        label = "YES"

    return label, crit, high, med


# ---------------------------------------------------------------------------
# Excel output
# ---------------------------------------------------------------------------

def write_excel_report(
    output_dir: str | Path,
    client: str,
    round_: str,
    all_results: dict[str, dict[str, Any]],
) -> Path:
    output_dir = Path(output_dir) / client
    output_dir.mkdir(parents=True, exist_ok=True)

    today    = date.today().strftime("%Y%m%d")
    out_path = output_dir / f"{client}_{round_}_Phase2_{today}.xlsx"

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        _write_schema_validation(writer, all_results)
        _write_schema_summary(writer, all_results)
        _write_datatype_checks(writer, all_results)
        _write_unrecognized(writer, all_results)
        _write_compat_summary(writer, all_results)

    return out_path


def _write_schema_validation(writer, all_results: dict) -> None:
    rows = []
    for fn, res in all_results.items():
        source  = res.get("source", "")
        staging = res.get("staging_table", "")
        for f in res.get("schema_results", {}).get("schema_findings", []):
            rows.append({
                "File":               fn,
                "Source":             source,
                "Staging Table":      staging,
                "Template Field":     f.get("template_field", ""),
                "Staging Column":     f.get("staging_column") or "",
                "Requirement Level":  f.get("requirement_level", "").title(),
                "Status":             f.get("status", ""),
                "Severity":           f.get("severity") or "",
                "Raw Column Matched": f.get("raw_column_matched") or "",
                "Confidence":         f.get("confidence") or "",
                "Notes":              f.get("notes") or "",
            })
    pd.DataFrame(rows).to_excel(writer, sheet_name="Schema Validation", index=False)


def _write_schema_summary(writer, all_results: dict) -> None:
    rows = []
    for fn, res in all_results.items():
        s = res.get("schema_results", {}).get("summary", {})
        rows.append({
            "File":                   fn,
            "Source":                 res.get("source", ""),
            "Required Present":       s.get("required_present",     0),
            "Required Missing":       s.get("required_missing",      0),
            "Required Total":         s.get("required_total",        0),
            "Recommended Present":    s.get("recommended_present",  0),
            "Recommended Missing":    s.get("recommended_missing",   0),
            "Recommended Total":      s.get("recommended_total",     0),
            "Optional Present":       s.get("optional_present",     0),
            "Optional Missing":       s.get("optional_missing",      0),
            "Optional Total":         s.get("optional_total",        0),
        })
    pd.DataFrame(rows).to_excel(writer, sheet_name="Schema Summary", index=False)


def _write_datatype_checks(writer, all_results: dict) -> None:
    rows = []
    for fn, res in all_results.items():
        for f in res.get("datatype_findings", []):
            rows.append({
                "File":                fn,
                "Raw Column":          f.get("raw_column", ""),
                "Staging Column":      f.get("staging_column", ""),
                "Requirement Level":   f.get("requirement_level", ""),
                "Staging Type":        f.get("staging_type", ""),
                "Max Length":          f.get("max_length") or "",
                "Type Compatible":     str(f.get("type_compatible", True)),
                "Domain Check":        f.get("domain_check") or "",
                "Domain Valid %":      f.get("domain_valid_pct") or "",
                "Invalid Count":       f.get("domain_invalid_count", 0),
                "Invalid Sample":      "; ".join(f.get("domain_invalid_sample", [])),
                "Length Exceeded":     f.get("length_exceeded_count", 0),
                "Null Count":          f.get("null_count", 0),
                "Null %":              f.get("null_pct", 0.0),
                "Severity":            f.get("severity") or "",
                "Notes":               f.get("notes", ""),
            })
    pd.DataFrame(rows).to_excel(writer, sheet_name="Data Type Checks", index=False)


def _write_unrecognized(writer, all_results: dict) -> None:
    rows = []
    for fn, res in all_results.items():
        unrecog = res.get("unrecognized_results", {})
        for f in unrecog.get("unrecognized_findings", []):
            rows.append({
                "File":                   fn,
                "Type":                   "UNMAPPED",
                "Raw Column":             f.get("raw_column", ""),
                "Severity":               f.get("severity", ""),
                "Nearest Staging Match":  f.get("nearest_staging_match") or "",
                "Match Score":            f.get("nearest_score", 0),
                "Notes":                  f.get("notes", ""),
            })
        for f in unrecog.get("fuzzy_review_list", []):
            rows.append({
                "File":                   fn,
                "Type":                   "FUZZY",
                "Raw Column":             f.get("raw_column", ""),
                "Severity":               "REVIEW",
                "Nearest Staging Match":  f.get("mapped_to_staging", ""),
                "Match Score":            f.get("confidence", ""),
                "Notes":                  f.get("notes", ""),
            })
    pd.DataFrame(rows).to_excel(writer, sheet_name="Unrecognized Columns", index=False)


def _write_compat_summary(writer, all_results: dict) -> None:
    rows = []
    any_no = any(r.get("compatible") == "NO" for r in all_results.values())
    for fn, res in all_results.items():
        rows.append({
            "File":            fn,
            "Source":          res.get("source", ""),
            "Staging Table":   res.get("staging_table", ""),
            "CRITICAL Issues": res.get("critical_count", 0),
            "HIGH Issues":     res.get("high_count",     0),
            "MEDIUM Issues":   res.get("medium_count",   0),
            "Compatible":      res.get("compatible",     ""),
        })
    rows.append({})
    rows.append({
        "File": "OVERALL",
        "Compatible": "NOT READY" if any_no else "READY",
    })
    pd.DataFrame(rows).to_excel(writer, sheet_name="Compatibility Summary", index=False)


# ---------------------------------------------------------------------------
# JSON manifest
# ---------------------------------------------------------------------------

def write_json_manifest(
    output_dir: str | Path,
    client: str,
    round_: str,
    all_results: dict[str, dict[str, Any]],
) -> Path:
    output_dir = Path(output_dir) / client
    output_dir.mkdir(parents=True, exist_ok=True)

    files_payload: dict[str, Any] = {}
    overall_compatible = all(
        r.get("compatible") != "NO" for r in all_results.values()
    )

    for fn, res in all_results.items():
        files_payload[fn] = {
            "source":               res.get("source"),
            "staging_table":        res.get("staging_table"),
            "compatible":           res.get("compatible"),
            "critical_count":       res.get("critical_count", 0),
            "high_count":           res.get("high_count",     0),
            "medium_count":         res.get("medium_count",   0),
            "schema_findings":      res.get("schema_results", {}).get("schema_findings", []),
            "schema_summary":       res.get("schema_results", {}).get("summary", {}),
            "datatype_findings":    res.get("datatype_findings", []),
            "unrecognized_findings": res.get("unrecognized_results", {}).get("unrecognized_findings", []),
            "fuzzy_review_list":    res.get("unrecognized_results", {}).get("fuzzy_review_list", []),
        }

    manifest = {
        "client":             client,
        "round":              round_,
        "date_run":           datetime.now().strftime("%Y-%m-%d %H:%M"),
        "overall_compatible": overall_compatible,
        "files":              files_payload,
    }

    out_path = output_dir / "phase2_findings.json"
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, default=str)

    return out_path


# ---------------------------------------------------------------------------
# Master render
# ---------------------------------------------------------------------------

def render(
    all_results: dict[str, dict[str, Any]],
    output_dir: str | Path,
    client: str,
    round_: str,
) -> None:
    print()
    print("=" * _BOX_WIDTH)
    print(f"  PIVOT TEST FILE REVIEW - Phase 2")
    print(f"  Client: {client}   Round: {round_}")
    print("=" * _BOX_WIDTH)

    for filename, res in all_results.items():
        render_file_box(filename, res)

    render_compatibility_table(all_results)

    excel_path = write_excel_report(output_dir, client, round_, all_results)
    json_path  = write_json_manifest(output_dir, client, round_, all_results)

    print("-" * _BOX_WIDTH)
    print(f"  Excel report : {excel_path}")
    print(f"  JSON manifest: {json_path}")
    print("-" * _BOX_WIDTH)
