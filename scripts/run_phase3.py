"""
run_phase3.py

Phase 3 CLI orchestrator — Data Quality Review.

Usage:
    py run_phase3.py "ClientName" v1
    py run_phase3.py --client "ClientName" --round v1
    py run_phase3.py --client "ClientName" --round v1 --input ./input --output ./output --knowledge-dir ./KnowledgeSources
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _load_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _merge_requirement_levels(phase1_files: dict, phase2_files: dict) -> None:
    """
    Merge Phase 2 RequirementLevel into Phase 1 column_mappings in-place.
    phase1_files: phase1_json["files"]
    phase2_files: phase2_json["files"]
    """
    # Build staging_col → requirement_level from Phase 2 (file-level)
    # Phase 2 JSON stores findings under "datatype_findings" and "schema_findings" keys.
    global_req: dict[str, str] = {}
    for fname, fdata in phase2_files.items():
        for finding in (
            fdata.get("findings", [])
            + fdata.get("datatype_findings", [])
            + fdata.get("schema_findings", [])
        ):
            stg = finding.get("staging_column") or finding.get("staging_col", "")
            rl = finding.get("requirement_level", "Optional")
            if stg and rl:
                if stg not in global_req:
                    global_req[stg] = rl

    for fname, fdata in phase1_files.items():
        staging_table = fdata.get("staging_table", "")
        col_maps = fdata.get("column_mappings", [])
        for mapping in col_maps:
            stg = mapping.get("staging_col", "")
            # Set requirement_level from Phase 2 if available
            if stg in global_req:
                mapping["requirement_level"] = global_req[stg]
            elif "requirement_level" not in mapping:
                mapping["requirement_level"] = "Optional"
            # Attach staging_table for column_utils lookups
            if "staging_table" not in mapping:
                mapping["staging_table"] = staging_table


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 3 — Data Quality Review",
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
        parser.error("Client name and round are required. Usage: py run_phase3.py \"ClientName\" v1")

    input_dir = Path(args.input)
    output_dir = Path(args.output) / client
    ks_dir = Path(args.knowledge_dir)

    # Locate Phase 1 and Phase 2 findings
    phase1_path = output_dir / "phase1_findings.json"
    phase2_path = output_dir / "phase2_findings.json"

    if not phase1_path.exists():
        print(f"ERROR: phase1_findings.json not found at {phase1_path}")
        print("Run Phase 1 first: py run_phase1.py \"ClientName\" v1")
        sys.exit(1)

    if not phase2_path.exists():
        print(f"ERROR: phase2_findings.json not found at {phase2_path}")
        print("Run Phase 2 first: py run_phase2.py \"ClientName\" v1")
        sys.exit(1)

    print(f"Loading Phase 1 findings: {phase1_path}")
    phase1_json = _load_json(phase1_path)

    print(f"Loading Phase 2 findings: {phase2_path}")
    phase2_json = _load_json(phase2_path)

    # Load knowledge sources (staging meta + CMS CSVs)
    print(f"Loading knowledge sources from: {ks_dir}")
    from shared import staging_meta
    staging_meta.load(ks_dir.parent if (ks_dir.parent / "KnowledgeSources").exists() else ks_dir)

    # Warn if CMS sources are missing
    if staging_meta.get_cms_cpt() is None:
        print(f"WARNING: stdCmsCpt.csv not found — B13 CPT validation will be skipped")
    if staging_meta.get_cms_pos() is None:
        print(f"WARNING: stdCmsPos.csv not found — B14 POS validation will be skipped")

    # Load DataFrames
    print("Loading data files...")
    from shared import loader
    phase1_files = phase1_json.get("files", {})
    phase2_files = phase2_json.get("files", {})

    # Merge RequirementLevel into column_mappings
    _merge_requirement_levels(phase1_files, phase2_files)

    # Build expected date range lookup from Phase 1 JSON (populated by --date-start/--date-end)
    from datetime import date as _date
    _edr = phase1_json.get("expected_date_ranges") or {}
    _SOURCE_BASE = {
        "billing_combined": "billing", "billing_charges": "billing",
        "billing_transactions": "billing", "scheduling": "scheduling",
        "payroll": "payroll", "gl": "gl", "quality": "quality",
        "patient_satisfaction": "patient_satisfaction",
    }

    def _get_date_range(src: str):
        """Return (start_date, end_date) for source, or None if not configured."""
        entry = _edr.get(_SOURCE_BASE.get(src, src)) or {}
        s, e = entry.get("start"), entry.get("end")
        if s and e:
            try:
                return (_date.fromisoformat(s), _date.fromisoformat(e))
            except (ValueError, AttributeError):
                pass
        return None

    # Build manifest (metadata only, no DataFrames loaded yet)
    manifest_meta = loader.get_file_manifest(phase1_path)

    if not manifest_meta:
        print("ERROR: No files found in Phase 1 metadata.")
        sys.exit(1)

    print(f"Found {len(manifest_meta)} file(s).\n")

    # Import phase3 modules
    from phase3 import universal, billing, scheduling, payroll, gl, quality, patient_satisfaction, report

    billing_sources = {"billing_combined", "billing_charges", "billing_transactions"}
    billing_filenames = [fn for fn, m in manifest_meta.items() if m["source"] in billing_sources]

    # Pre-load all billing files together (billing.run_checks needs them simultaneously)
    billing_dfs: dict[str, dict] = {}
    for fn in billing_filenames:
        fdata = loader.load_single_file(phase1_path, input_dir, fn)
        fdata["column_mappings"] = phase1_files[fn].get("column_mappings", [])
        billing_dfs[fn] = fdata

    all_file_results: dict[str, dict] = {}
    test_month = phase1_json.get("test_month", "")

    # ── Run checks per file ────────────────────────────────────────────────────
    # Billing files use pre-loaded billing_dfs; non-billing files are loaded lazily.
    billing_checked = False
    billing_results: dict = {}
    billing_prep: dict = {}

    for fname, meta in manifest_meta.items():
        source = meta["source"]

        if source in billing_sources:
            entry = billing_dfs[fname]
        else:
            entry = loader.load_single_file(phase1_path, input_dir, fname)
            entry["column_mappings"] = phase1_files[fname].get("column_mappings", [])

        df = entry.get("df")
        col_maps = entry.get("column_mappings", [])
        row_count = meta.get("row_count", 0) or (len(df) if df is not None else 0)

        print(f"  Checking: {Path(fname).name} ({source}, {row_count:,} rows)")

        # Universal checks
        u_findings = []
        if df is not None and len(df) > 0:
            u_findings = universal.run_all_checks(
                df, col_maps, source, test_month, date_range=_get_date_range(source)
            )

        # Source-specific checks
        s_findings: list[dict] = []
        cross_source_prep: dict = {}

        if source in billing_sources:
            if not billing_checked:
                billing_results, billing_prep = billing.run_checks(
                    billing_dfs, test_month, date_range=_get_date_range("billing_combined")
                )
                billing_checked = True
            s_findings = billing_results.get(fname, [])
            cross_source_prep = billing_prep.get(fname, {})

        elif source == "scheduling":
            if df is not None and len(df) > 0:
                s_findings, cross_source_prep = scheduling.run_checks(df, col_maps, test_month)

        elif source == "payroll":
            if df is not None and len(df) > 0:
                s_findings, cross_source_prep = payroll.run_checks(df, col_maps, test_month)

        elif source == "gl":
            if df is not None and len(df) > 0:
                # Find raw Account Type column if present (no staging col)
                acct_type_raw = None
                for m in col_maps:
                    if "account type" in (m.get("raw_col") or "").lower():
                        acct_type_raw = m["raw_col"]
                        break
                s_findings, _ = gl.run_checks(df, col_maps, test_month, acct_type_raw)

        elif source == "quality":
            if df is not None and len(df) > 0:
                s_findings, cross_source_prep = quality.run_checks(df, col_maps, test_month)

        elif source == "patient_satisfaction":
            if df is not None and len(df) > 0:
                s_findings, cross_source_prep = patient_satisfaction.run_checks(df, col_maps, test_month)

        all_file_results[fname] = {
            "source": source,
            "record_count": row_count,
            "universal_findings": u_findings,
            "source_specific_findings": s_findings,
            "cross_source_prep": cross_source_prep,
        }

    # Release billing DataFrames now that all checks are complete
    del billing_dfs

    print()

    # Render output
    report.render(all_file_results, output_dir, client, round_id)


if __name__ == "__main__":
    main()
