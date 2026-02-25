"""
run_phase5.py

Phase 5 CLI orchestrator — Results Generation & Reporting.

Aggregates Phase 1–4 findings into a consolidated client-ready deliverable.

Usage:
    py run_phase5.py "ClientName" v1
    py run_phase5.py --client "ClientName" --round v1 [--output ./output]
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 5 — Results Generation & Reporting",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Positional args (optional, for quick invocation)
    parser.add_argument("client_pos", nargs="?", default=None, help=argparse.SUPPRESS)
    parser.add_argument("round_pos", nargs="?", default=None, help=argparse.SUPPRESS)
    # Named args
    parser.add_argument("--client", default=None)
    parser.add_argument("--round", dest="round_id", default=None)
    parser.add_argument("--output", default="./output")
    parser.add_argument("--input", default="./input")

    args = parser.parse_args()

    client = args.client_pos or args.client
    round_id = args.round_pos or args.round_id

    if not client or not round_id:
        parser.error('Client name and round are required. Usage: py run_phase5.py "ClientName" v1')

    output_dir = Path(args.output) / client
    input_dir  = Path(args.input)

    # Validate all four phase JSONs exist
    required_phases = [1, 2, 3, 4]
    for n in required_phases:
        path = output_dir / f"phase{n}_findings.json"
        if not path.exists():
            print(f"ERROR: phase{n}_findings.json not found at {path}")
            print(f'Run Phase {n} first: py run_phase{n}.py "{client}" {round_id}')
            sys.exit(1)

    print(f"Phase 5 — Results Generation & Reporting")
    print(f"  Client: {client}")
    print(f"  Round:  {round_id}")
    print()

    # Step 1: Load all phase findings
    print("Step 1/4 -- Loading phase findings...")
    from phase5 import aggregator, missing_sources, deduplicator, issue_formatter, readiness, checklist, report
    from phase5 import cost_center_summary, provider_summary
    from shared import loader, staging_meta

    # Load staging metadata (needed for billing charge mask in summary modules)
    staging_meta.load(Path(__file__).parent)

    phase_data = aggregator.load_all_phases(output_dir)
    unified = aggregator.build_unified_model(phase_data)
    print(f"  Loaded {sum(len(s.get('files', [])) for s in unified['sources'].values())} file(s) across {len(unified['sources'])} source(s)")

    # Load raw DataFrames only for the sources needed by cost center and provider summaries.
    # Quality and patient_satisfaction files are excluded — they are not used by the summary builders.
    file_entries: dict = {}
    if input_dir.is_dir():
        try:
            phase1_json_path = output_dir / "phase1_findings.json"
            manifest_meta = loader.get_file_manifest(phase1_json_path)
            summary_sources = {
                "billing_combined", "billing_charges", "billing_transactions",
                "scheduling", "payroll", "gl",
            }
            summary_filenames = [fn for fn, m in manifest_meta.items() if m["source"] in summary_sources]
            file_entries = loader.load_pair(phase1_json_path, input_dir / client, summary_filenames)
            print(f"  DataFrames loaded for {len(file_entries)} file(s) (Cost Center & Provider summaries)")
        except Exception as exc:
            print(f"  WARNING: Could not load DataFrames — Cost Center/Provider summaries will be empty. ({exc})")
    else:
        print(f"  WARNING: Input directory not found ({input_dir}) — Cost Center/Provider summaries will be empty.")

    # Step 2: Aggregate and de-duplicate
    print("Step 2/4 -- Aggregating and de-duplicating issues...")
    missing = missing_sources.detect(phase_data["phase1"])
    if missing:
        print(f"  Missing expected sources: {', '.join(missing)}")
    else:
        print("  All expected core sources present")

    unified = deduplicator.deduplicate(unified)
    client_issues = issue_formatter.format_all_issues(unified)
    print(f"  {len(client_issues)} client issues after de-duplication")

    # Step 3: Determine readiness
    print("Step 3/4 -- Determining readiness...")
    readiness_result = readiness.determine(unified, missing, client_issues)
    print(f"  Readiness: {readiness_result['overall']}")

    # Step 4: Generate checklist and render
    print("Step 4/4 -- Generating report...")
    checklist_items = checklist.generate(unified, client_issues, missing)

    # Build cross-source summary sheets (cost center + provider)
    cc_rows: list = []
    prov_rows: list = []
    if file_entries:
        try:
            cc_rows = cost_center_summary.build(file_entries)
            print(f"  Cost Center Summary: {len(cc_rows)} row(s)")
        except Exception as exc:
            print(f"  WARNING: Cost Center Summary failed — {exc}")
        try:
            prov_rows = provider_summary.build(file_entries)
            print(f"  Provider Summary: {len(prov_rows)} row(s)")
        except Exception as exc:
            print(f"  WARNING: Provider Summary failed — {exc}")

    report.render(
        unified,
        readiness_result,
        client_issues,
        checklist_items,
        missing,
        output_dir,
        client,
        round_id,
        cc_rows=cc_rows,
        prov_rows=prov_rows,
    )

    print("\nPhase 5 complete.")


if __name__ == "__main__":
    main()
