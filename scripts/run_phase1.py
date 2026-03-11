"""
run_phase1.py

CLI entry point for Phase 1: Initial Setup & Data Ingestion.

Usage
-----
    python run_phase1.py --input ./input --output ./output --client "ClientName" --round v1

Arguments
---------
--input     Base directory for client input files; {client} subfolder is
            appended automatically (e.g. ./input/ClientName)  (default: ./input)
--output    Base directory for report output; {client} subfolder is appended
            automatically (e.g. ./output/ClientName)          (default: ./output)
--client    Client name, used in subfolder paths and output file names (default: Client)
--round     Submission round (v1/v2/v3)                     (default: v1)
--ref       Path to directory containing the two reference  (default: project root)
            Excel files (RawToStagingColumnMapping.xlsx and
            StagingTableStructure.xlsx)
--no-prompt Skip interactive prompts; process all files with auto-detection
"""

from __future__ import annotations

import argparse
import sys
import traceback
from pathlib import Path
from typing import Any

# Ensure UTF-8 output on Windows terminals
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Ordered list of valid source names for the interactive menu
_SOURCE_MENU = [
    "billing_combined",
    "billing_charges",
    "billing_transactions",
    "scheduling",
    "payroll",
    "gl",
    "quality",
    "patient_satisfaction",
]


# ---------------------------------------------------------------------------
# Interactive helpers
# ---------------------------------------------------------------------------

def _prompt_file_selection(file_dict: dict[str, Any]) -> dict[str, Any]:
    """Show a numbered file list and return only the files the user selects."""
    files = list(file_dict.keys())
    print()
    for i, fn in enumerate(files, 1):
        meta = file_dict[fn]
        rows = meta.get("row_count", "?")
        cols = meta.get("raw_col_count", "?")
        print(f"    [{i}] {fn}  ({rows} rows, {cols} cols)")
    print()

    raw = input("  Select files to process (e.g. 1,3 or 'all') [all]: ").strip()

    if not raw or raw.lower() == "all":
        print(f"  → Processing all {len(files)} file(s)")
        return file_dict

    selected_keys: list[str] = []
    for token in raw.replace(" ", "").split(","):
        try:
            idx = int(token) - 1
            if 0 <= idx < len(files):
                selected_keys.append(files[idx])
            else:
                print(f"  WARNING: '{token}' out of range, ignored.")
        except ValueError:
            print(f"  WARNING: '{token}' is not a number, ignored.")

    if not selected_keys:
        print("  No valid selections — processing all files.")
        return file_dict

    print(f"  → Processing {len(selected_keys)} file(s): {', '.join(selected_keys)}")
    return {k: file_dict[k] for k in selected_keys}


def _prompt_source_assignments(
    file_dict: dict[str, Any],
    auto_assignments: dict[str, str],
    overrides: dict[str, str],
) -> dict[str, str]:
    """
    For each file, show the auto-detected source and allow the analyst to
    confirm or change it. Files in *overrides* are shown as MANUAL and
    skip the prompt.
    """
    final: dict[str, str] = {}
    files = list(file_dict.keys())

    # Build source menu lines (two columns)
    menu_lines: list[str] = []
    for i, src in enumerate(_SOURCE_MENU, 1):
        menu_lines.append(f"[{i}] {src}")
    # Print in rows of 3
    col_w = max(len(l) for l in menu_lines) + 4

    print()
    for file_idx, fn in enumerate(files, 1):
        auto_src = auto_assignments.get(fn, "unknown")

        if fn in overrides:
            print(f"  [{file_idx}] {fn}  (source.csv override: {auto_src})")
            final[fn] = auto_src
            continue

        # Priority 2: source came from subfolder name — no confirmation needed
        folder_src = file_dict[fn].get("source_folder")
        if folder_src and folder_src == auto_src:
            print(f"  [{file_idx}] {fn}  (subfolder: {auto_src})")
            final[fn] = auto_src
            continue

        print(f"  [{file_idx}] {fn}  (auto-detected: {auto_src})")

        # Print source menu in rows of 3
        for row_start in range(0, len(menu_lines), 3):
            row = menu_lines[row_start:row_start + 3]
            print("    " + "".join(l.ljust(col_w) for l in row))

        # Default: keep current auto value, or skip if unknown
        if auto_src != "unknown":
            default_label = f"keep: {auto_src}"
            default_idx = str(len(_SOURCE_MENU) + 1)
            print(f"    [{len(_SOURCE_MENU) + 1}] Keep current ({auto_src})")
        else:
            default_label = "skip (leave unknown)"
            default_idx = str(len(_SOURCE_MENU) + 1)
            print(f"    [{len(_SOURCE_MENU) + 1}] Skip (leave as unknown)")

        raw = input(f"  Enter number [{default_idx} = {default_label}]: ").strip()

        if not raw or raw == default_idx:
            final[fn] = auto_src
            if auto_src != "unknown":
                print(f"  → {fn}: {auto_src}  (confirmed)")
            else:
                print(f"  → {fn}: unknown  (skipped)")
        else:
            try:
                choice = int(raw) - 1
                if 0 <= choice < len(_SOURCE_MENU):
                    chosen = _SOURCE_MENU[choice]
                    final[fn] = chosen
                    print(f"  → {fn}: {chosen}")
                else:
                    print(f"  WARNING: invalid choice — keeping '{auto_src}'")
                    final[fn] = auto_src
            except ValueError:
                print(f"  WARNING: invalid input — keeping '{auto_src}'")
                final[fn] = auto_src

        print()

    return final


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(
        description="PIVOT Test File Review — Phase 1: Initial Setup & Data Ingestion"
    )
    parser.add_argument("client_pos", nargs="?", default=None, help=argparse.SUPPRESS)
    parser.add_argument("round_pos",  nargs="?", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--input",     default="./input",  help="Input directory containing client files")
    parser.add_argument("--output",    default="./output", help="Output directory for reports")
    parser.add_argument("--client",    default="Client",   help="Client name")
    parser.add_argument("--round",     default="v1",       help="Submission round (v1/v2/v3)")
    parser.add_argument(
        "--ref",
        default=str(Path(__file__).parent / "KnowledgeSources"),
        help="Directory containing RawToStagingColumnMapping.xlsx and StagingTableStructure.xlsx",
    )
    parser.add_argument(
        "--no-prompt",
        action="store_true",
        help="Skip interactive prompts; process all files with auto-detection",
    )
    parser.add_argument(
        "--date-start",
        default=None,
        metavar="YYYY-MM-DD",
        help="Expected date range start for all sources (e.g. 2025-11-16). "
             "If omitted, Phase 3 date range checks are skipped.",
    )
    parser.add_argument(
        "--date-end",
        default=None,
        metavar="YYYY-MM-DD",
        help="Expected date range end for all sources (e.g. 2025-12-15). "
             "If omitted, Phase 3 date range checks are skipped.",
    )
    args = parser.parse_args()

    client     = args.client_pos or args.client
    round_     = args.round_pos  or args.round
    input_dir  = Path(args.input) / client
    output_dir = Path(args.output)
    ref_dir    = Path(args.ref)
    interactive = not args.no_prompt

    # Validate paths
    if not input_dir.is_dir():
        print(f"ERROR: Input directory not found: {input_dir}", file=sys.stderr)
        return 1

    ref_mapping = ref_dir / "RawToStagingColumnMapping.xlsx"
    ref_structure = ref_dir / "StagingTableStructure.xlsx"
    if not ref_mapping.exists():
        print(f"ERROR: Mapping reference file not found: {ref_mapping}", file=sys.stderr)
        return 1
    if not ref_structure.exists():
        print(f"ERROR: Structure reference file not found: {ref_structure}", file=sys.stderr)
        return 1

    # ---- Step 1: Ingest ----
    print("Step 1/5 — Scanning and parsing input files...")
    from phase1.ingestion import ingest_directory
    try:
        file_dict = ingest_directory(input_dir)
    except Exception as e:
        print(f"ERROR during ingestion: {e}", file=sys.stderr)
        traceback.print_exc()
        return 1

    if not file_dict:
        print(f"No .txt or .csv files found in {input_dir}.", file=sys.stderr)
        return 1

    print(f"  Found {len(file_dict)} file(s):")
    if interactive:
        file_dict = _prompt_file_selection(file_dict)
    else:
        for fn in file_dict:
            meta = file_dict[fn]
            print(f"    {fn}  ({meta.get('row_count','?')} rows, {meta.get('raw_col_count','?')} cols)")

    # ---- Step 2: Detect sources ----
    print("Step 2/5 — Identifying data sources...")
    from phase1.source_detection import detect_sources, load_source_overrides
    from phase1.column_transforms import load_column_transforms
    overrides = load_source_overrides(input_dir)
    if overrides:
        print(f"  sources.csv found — overrides for: {list(overrides.keys())}")
    column_transforms = load_column_transforms(input_dir)
    auto_assignments = detect_sources(file_dict, overrides=overrides)

    if interactive:
        source_assignments = _prompt_source_assignments(file_dict, auto_assignments, overrides)
    else:
        source_assignments = auto_assignments
        for fn, src in source_assignments.items():
            tag = " [MANUAL]" if fn in overrides else ""
            print(f"  {fn}: {src}{tag}")

    # ---- Step 3: Billing format ----
    print("Step 3/5 — Determining billing format...")
    from phase1.billing_format import detect_billing_format
    billing_format = detect_billing_format(source_assignments)
    print(f"  Format: {billing_format['format']}")
    print(f"  {billing_format['notes']}")

    # ---- Step 4: Column mapping ----
    print("Step 4/5 — Running raw-to-staging column mapping...")
    from phase1.column_mapping import map_all_files
    mapping_results = map_all_files(file_dict, source_assignments, ref_dir)
    for fn, records in mapping_results.items():
        exact    = sum(1 for r in records if r["confidence"] == "EXACT")
        norm     = sum(1 for r in records if r["confidence"] == "NORMALIZED")
        fuzzy    = sum(1 for r in records if r["confidence"].startswith("FUZZY"))
        unmapped = sum(1 for r in records if r["confidence"] == "UNMAPPED")
        print(f"  {fn}: EXACT={exact} NORMALIZED={norm} FUZZY={fuzzy} UNMAPPED={unmapped}")

    # ---- Step 5: Test month ----
    print("Step 5/5 — Identifying test month and checking alignment...")
    from phase1.test_month import identify_test_month
    month_results = identify_test_month(file_dict, mapping_results, source_assignments)
    print(f"  Test month: {month_results.get('test_month', 'Unknown')}")
    print(f"  Aligned:    {month_results.get('aligned', False)}")

    # ---- Render output ----
    print("\nGenerating report output...")
    from phase1.report import render
    render(
        file_dict          = file_dict,
        source_assignments = source_assignments,
        billing_format     = billing_format,
        mapping_results    = mapping_results,
        month_results      = month_results,
        output_dir         = output_dir,
        client             = client,
        round_             = round_,
        date_start         = args.date_start,
        date_end           = args.date_end,
        column_transforms  = column_transforms,
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
