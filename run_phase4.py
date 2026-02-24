"""
run_phase4.py

Phase 4 CLI orchestrator — Cross-Source Validation.

Usage:
    py run_phase4.py "ClientName" v1
    py run_phase4.py --client "ClientName" --round v1
    py run_phase4.py --client "ClientName" --round v1 --input ./input --output ./output --knowledge-dir ./KnowledgeSources
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 4 — Cross-Source Validation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Positional args (optional, for quick invocation)
    parser.add_argument("client_pos", nargs="?", default=None, help=argparse.SUPPRESS)
    parser.add_argument("round_pos", nargs="?", default=None, help=argparse.SUPPRESS)
    # Named args
    parser.add_argument("--client", default=None)
    parser.add_argument("--round", dest="round_id", default=None)
    parser.add_argument("--input", default="./input")
    parser.add_argument("--output", default="./output")
    parser.add_argument("--knowledge-dir", default="./KnowledgeSources")

    args = parser.parse_args()

    client = args.client_pos or args.client
    round_id = args.round_pos or args.round_id

    if not client or not round_id:
        parser.error("Client name and round are required. Usage: py run_phase4.py \"ClientName\" v1")

    input_dir = Path(args.input)
    output_dir = Path(args.output) / client
    ks_dir = Path(args.knowledge_dir)

    # Locate Phase 1 and Phase 3 findings
    phase1_path = output_dir / "phase1_findings.json"
    phase3_path = output_dir / "phase3_findings.json"

    if not phase1_path.exists():
        print(f"ERROR: phase1_findings.json not found at {phase1_path}")
        print("Run Phase 1 first: py run_phase1.py \"ClientName\" v1")
        sys.exit(1)

    if not phase3_path.exists():
        print(f"ERROR: phase3_findings.json not found at {phase3_path}")
        print("Run Phase 3 first: py run_phase3.py \"ClientName\" v1")
        sys.exit(1)

    print(f"Loading Phase 1 findings: {phase1_path}")
    phase1_json = _load_json(phase1_path)

    print(f"Loading Phase 3 findings: {phase3_path}")
    phase3_json = _load_json(phase3_path)

    # Load knowledge sources
    print(f"Loading knowledge sources from: {ks_dir}")
    from shared import staging_meta
    staging_meta.load(ks_dir.parent if (ks_dir.parent / "KnowledgeSources").exists() else ks_dir)

    # Load DataFrames
    print("Loading data files...")
    from shared import loader
    file_entries = loader.load_files(phase1_path, input_dir)

    if not file_entries:
        print("ERROR: No files loaded from Phase 1 metadata.")
        sys.exit(1)

    print(f"Loaded {len(file_entries)} file(s).\n")

    # Extract billing format from Phase 1 JSON
    # billing_format is a dict: {"format": "combined"|"separate"|"none"|"unknown", ...}
    billing_format_obj = phase1_json.get("billing_format", {})
    if isinstance(billing_format_obj, dict):
        billing_format = billing_format_obj.get("format", "unknown")
    else:
        # Fallback: older format where it was a plain string
        billing_format = str(billing_format_obj) if billing_format_obj else "unknown"

    print(f"  Billing format: {billing_format}")

    # Extract cross_source_prep per file from Phase 3 JSON
    cross_source_prep_by_file: dict[str, dict] = {}
    for fname, fdata in phase3_json.get("files", {}).items():
        prep = fdata.get("cross_source_prep", {})
        if prep:
            cross_source_prep_by_file[fname] = prep

    # Import phase4 modules
    from phase4 import transactions_charges, billing_gl, billing_payroll, billing_scheduling, payroll_gl, scheduling_gl, report

    all_findings: dict[str, dict] = {}

    # ── C0: Transactions <-> Charges ─────────────────────────────────────────
    print("  Running C0: Billing Transactions <-> Charges...")
    try:
        all_findings["C0"] = transactions_charges.run_checks(file_entries, billing_format)
    except Exception as exc:
        print(f"    ERROR in C0: {exc}")
        all_findings["C0"] = {"check": "C0", "severity": "INFO", "message": f"C0 failed: {exc}", "skipped": True}

    # ── C1: Billing <-> GL ────────────────────────────────────────────────────
    print("  Running C1: Billing <-> GL...")
    try:
        all_findings["C1"] = billing_gl.run_checks(file_entries)
    except Exception as exc:
        print(f"    ERROR in C1: {exc}")
        all_findings["C1"] = {"check": "C1", "severity": "INFO", "message": f"C1 failed: {exc}", "skipped": True}

    # ── C2: Billing <-> Payroll ───────────────────────────────────────────────
    print("  Running C2: Billing <-> Payroll...")
    try:
        all_findings["C2"] = billing_payroll.run_checks(file_entries, cross_source_prep_by_file)
    except Exception as exc:
        print(f"    ERROR in C2: {exc}")
        all_findings["C2"] = {"check": "C2", "severity": "INFO", "message": f"C2 failed: {exc}", "skipped": True}

    # ── C3: Billing <-> Scheduling ────────────────────────────────────────────
    print("  Running C3: Billing <-> Scheduling...")
    try:
        all_findings["C3"] = billing_scheduling.run_checks(file_entries, cross_source_prep_by_file)
    except Exception as exc:
        print(f"    ERROR in C3: {exc}")
        all_findings["C3"] = {"check": "C3", "severity": "INFO", "message": f"C3 failed: {exc}", "skipped": True}

    # ── C4: Payroll <-> GL ────────────────────────────────────────────────────
    print("  Running C4: Payroll <-> GL...")
    try:
        all_findings["C4"] = payroll_gl.run_checks(file_entries)
    except Exception as exc:
        print(f"    ERROR in C4: {exc}")
        all_findings["C4"] = {"check": "C4", "severity": "INFO", "message": f"C4 failed: {exc}", "skipped": True}

    # ── C5: Scheduling <-> GL ─────────────────────────────────────────────────
    print("  Running C5: Scheduling <-> GL...")
    try:
        all_findings["C5"] = scheduling_gl.run_checks(file_entries)
    except Exception as exc:
        print(f"    ERROR in C5: {exc}")
        all_findings["C5"] = {"check": "C5", "severity": "INFO", "message": f"C5 failed: {exc}", "skipped": True}

    print()

    # Render output
    report.render(all_findings, output_dir, client, round_id)


if __name__ == "__main__":
    main()
