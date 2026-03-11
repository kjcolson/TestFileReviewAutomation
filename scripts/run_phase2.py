"""
run_phase2.py

Phase 2 CLI entry point — Database Compatibility Check.

Usage:
    py run_phase2.py --client "ClientName" --round v1

Reads:
    output/{client}/phase1_findings.json
    input/{client}/  (source files, re-loaded for data type checks)
    StagingTableStructure.xlsx  (in project root)

Writes:
    output/{client}/{client}_{round}_Phase2_{YYYYMMDD}.xlsx
    output/{client}/phase2_findings.json
"""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

# Ensure box-drawing characters display correctly on Windows consoles
if hasattr(sys.stdout, "buffer"):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="PIVOT Test File Review — Phase 2: Database Compatibility Check"
    )
    parser.add_argument("client_pos", nargs="?", default=None, help=argparse.SUPPRESS)
    parser.add_argument("round_pos",  nargs="?", default=None, help=argparse.SUPPRESS)
    parser.add_argument("--client",  default="Client",   help="Client name (must match output subfolder)")
    parser.add_argument("--round",   default="v1",       help="Submission round (v1, v2, ...)")
    parser.add_argument("--input",   default="./input",  help="Base input directory")
    parser.add_argument("--output",  default="./output", help="Base output directory")
    args = parser.parse_args()

    client    = args.client_pos or args.client
    round_    = args.round_pos  or args.round
    input_dir = Path(args.input)  / client
    output_dir = Path(args.output)
    ref_dir   = Path(__file__).parent   # project root (where .xlsx files live)

    phase1_json = output_dir / client / "phase1_findings.json"

    # ------------------------------------------------------------------
    # Validate prerequisites
    # ------------------------------------------------------------------
    if not phase1_json.exists():
        print(
            f"\nERROR: phase1_findings.json not found at:\n"
            f"  {phase1_json}\n\n"
            f"Run Phase 1 first:\n"
            f'  py run_phase1.py --client "{client}" --round {round_}\n'
        )
        return 1

    if not input_dir.exists():
        print(
            f"\nWARNING: Input directory not found: {input_dir}\n"
            f"DataFrames cannot be re-loaded — data type checks will be skipped.\n"
        )

    # ------------------------------------------------------------------
    # Load reference data
    # ------------------------------------------------------------------
    print("\nLoading reference data...")
    from shared import staging_meta
    staging_meta.load(ref_dir)

    # ------------------------------------------------------------------
    # Load Phase 1 results and re-open source files
    # ------------------------------------------------------------------
    print("Step 1/5 — Loading Phase 1 findings and source files...")
    from shared.loader import get_file_manifest, load_single_file
    try:
        file_manifest = get_file_manifest(phase1_json)
    except FileNotFoundError as exc:
        print(f"\nERROR: {exc}")
        return 1

    # ------------------------------------------------------------------
    # Run Phase 2 checks per file (one file loaded at a time)
    # ------------------------------------------------------------------
    from phase2 import schema_validator, field_classifier, datatype_checker, unrecognized_columns
    from phase2.report import determine_compatibility

    print("Step 2/5 — Running schema validation...")
    print("Step 3/5 — Classifying fields by requirement level...")
    print("Step 4/5 — Running data type and domain checks...")
    print("Step 5/5 — Flagging unrecognized columns...")

    all_results: dict = {}
    for filename in file_manifest:
        fdata = load_single_file(phase1_json, input_dir, filename)
        source        = fdata.get("source", "unknown")
        staging_table = fdata.get("staging_table")
        # normalise "(no staging table)" → None for downstream modules
        if staging_table and staging_table.startswith("("):
            staging_table = None

        # Schema validation
        schema_res = schema_validator.validate(fdata, source)

        # Field classification
        classified = field_classifier.classify(
            fdata.get("column_mappings", []), source
        )

        # Data type checks
        dtype_findings = datatype_checker.check(
            fdata, classified, source, staging_table
        )

        # Unrecognized columns
        unrec_res = unrecognized_columns.flag(fdata, source, staging_table)

        # Compatibility determination
        compat_label, crit, high, med = determine_compatibility(
            schema_res.get("schema_findings", []),
            dtype_findings,
        )

        all_results[filename] = {
            "source":               source,
            "staging_table":        fdata.get("staging_table", "(unknown)"),
            "schema_results":       schema_res,
            "classified_mappings":  classified,
            "datatype_findings":    dtype_findings,
            "unrecognized_results": unrec_res,
            "compatible":           compat_label,
            "critical_count":       crit,
            "high_count":           high,
            "medium_count":         med,
        }

    # ------------------------------------------------------------------
    # Render output
    # ------------------------------------------------------------------
    from phase2 import report
    report.render(all_results, output_dir, client, round_)

    return 0


if __name__ == "__main__":
    sys.exit(main())
