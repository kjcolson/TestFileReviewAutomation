"""
run_all.py

Runs all 5 phases of the PIVOT Test File Review pipeline sequentially.

Usage:
    py run_all.py "ClientName" v1 --no-prompt
    py run_all.py --client "ClientName" --round v1 [--input ./input] [--output ./output]
                  [--ref ./KnowledgeSources] [--no-prompt]
                  [--date-start YYYY-MM-DD] [--date-end YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="PIVOT Test File Review — Run All Phases (1-5)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    # Positional args (optional, for quick invocation)
    parser.add_argument("client_pos", nargs="?", default=None, help=argparse.SUPPRESS)
    parser.add_argument("round_pos", nargs="?", default=None, help=argparse.SUPPRESS)
    # Named args (superset of all phase scripts)
    parser.add_argument("--client", default=None)
    parser.add_argument("--round", dest="round_id", default=None)
    parser.add_argument("--input", default="./input")
    parser.add_argument("--output", default="./output")
    parser.add_argument("--ref", default=str(Path(__file__).parent / "KnowledgeSources"))
    parser.add_argument("--knowledge-dir", default="./KnowledgeSources")
    parser.add_argument("--no-prompt", action="store_true")
    parser.add_argument("--date-start", default=None, metavar="YYYY-MM-DD")
    parser.add_argument("--date-end", default=None, metavar="YYYY-MM-DD")

    args = parser.parse_args()

    client = args.client_pos or args.client
    round_id = args.round_pos or args.round_id

    if not client or not round_id:
        parser.error('Client name and round are required. Usage: py run_all.py "ClientName" v1')

    project_dir = Path(__file__).parent
    phase1_json_path = Path(args.output) / client / "phase1_findings.json"

    # Build phase-specific argument lists
    phases = [
        (1, _build_phase1_args(args, client, round_id)),
        (2, _build_phase2_args(args, client, round_id)),
        (3, _build_phase3_args(args, client, round_id)),
        (4, _build_phase4_args(args, client, round_id)),
        (5, _build_phase5_args(args, client, round_id)),
    ]

    for phase_num, phase_args in phases:
        # Auto-skip Phase 4 when fewer than 2 compatible source groups are present
        if phase_num == 4 and not _should_run_phase4(phase1_json_path):
            _print_banner(4)
            print("  SKIPPED — fewer than 2 compatible source groups present.")
            print("  Cross-source checks (C0–C5) require pairs such as billing+GL,")
            print("  billing+payroll, billing+scheduling, payroll+GL, or scheduling+GL.")
            print("  Submit additional source files to enable cross-source validation.\n")
            # Write a stub so Phase 5 can find phase4_findings.json
            stub_path = Path(args.output) / client / "phase4_findings.json"
            stub_path.parent.mkdir(parents=True, exist_ok=True)
            with open(stub_path, "w", encoding="utf-8") as fh:
                json.dump(
                    {"skipped": True, "skip_reason": "Fewer than 2 compatible source groups present", "findings": {}},
                    fh,
                )
            continue

        script = project_dir / "scripts" / f"run_phase{phase_num}.py"
        if not script.exists():
            print(f"\nERROR: {script} not found.")
            return 1

        _print_banner(phase_num)
        cmd = [sys.executable, "-u", str(script)] + phase_args
        child_env = {**os.environ}
        existing_pp = child_env.get("PYTHONPATH", "")
        child_env["PYTHONPATH"] = str(project_dir) + (os.pathsep + existing_pp if existing_pp else "")
        flags = subprocess.CREATE_NO_WINDOW if sys.platform == "win32" else 0
        result = subprocess.run(cmd, cwd=str(project_dir), env=child_env, creationflags=flags)

        if result.returncode != 0:
            print(f"\n{'=' * 60}")
            print(f"  Phase {phase_num} FAILED (exit code {result.returncode})")
            print(f"  Pipeline stopped. Fix the issue and re-run.")
            print(f"{'=' * 60}")
            return result.returncode

    print(f"\n{'=' * 60}")
    print(f"  ALL PHASES COMPLETE — {client} {round_id}")
    print(f"{'=' * 60}")
    return 0


def _print_banner(phase_num: int) -> None:
    labels = {
        1: "Initial Setup & Data Ingestion",
        2: "Database Compatibility Check",
        3: "Data Quality Review",
        4: "Cross-Source Validation",
        5: "Results Generation & Reporting",
    }
    label = labels.get(phase_num, "")
    print(f"\n{'=' * 60}")
    print(f"  PHASE {phase_num}: {label}")
    print(f"{'=' * 60}\n")


def _build_phase1_args(args, client: str, round_id: str) -> list[str]:
    cmd = ["--client", client, "--round", round_id,
           "--input", args.input, "--output", args.output,
           "--ref", args.ref]
    if args.no_prompt:
        cmd.append("--no-prompt")
    if args.date_start:
        cmd.extend(["--date-start", args.date_start])
    if args.date_end:
        cmd.extend(["--date-end", args.date_end])
    return cmd


def _build_phase2_args(args, client: str, round_id: str) -> list[str]:
    return ["--client", client, "--round", round_id,
            "--input", args.input, "--output", args.output]


def _build_phase3_args(args, client: str, round_id: str) -> list[str]:
    return ["--client", client, "--round", round_id,
            "--input", args.input, "--output", args.output,
            "--knowledge-dir", args.knowledge_dir]


def _build_phase4_args(args, client: str, round_id: str) -> list[str]:
    return ["--client", client, "--round", round_id,
            "--input", args.input, "--output", args.output,
            "--knowledge-dir", args.knowledge_dir]


def _build_phase5_args(args, client: str, round_id: str) -> list[str]:
    return ["--client", client, "--round", round_id,
            "--output", args.output, "--input", args.input]


def _should_run_phase4(phase1_json_path: Path) -> bool:
    """Return True if ≥2 compatible source groups are present for cross-source checks."""
    _SOURCE_TO_GROUP = {
        "billing_combined":     "billing",
        "billing_charges":      "billing",
        "billing_transactions": "billing",
        "scheduling":           "scheduling",
        "payroll":              "payroll",
        "gl":                   "gl",
    }
    try:
        with open(phase1_json_path, encoding="utf-8") as fh:
            manifest = json.load(fh)
        groups = {
            _SOURCE_TO_GROUP[fdata.get("source", "")]
            for fdata in manifest.get("files", {}).values()
            if fdata.get("source", "") in _SOURCE_TO_GROUP
        }
        return len(groups) >= 2
    except Exception:
        return True  # Default to running Phase 4 if JSON can't be read


if __name__ == "__main__":
    sys.exit(main())
