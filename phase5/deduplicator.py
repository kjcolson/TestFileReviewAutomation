"""
phase5/deduplicator.py

Cross-phase de-duplication: removes redundant findings where the same
root cause appears in multiple phases.
"""

from __future__ import annotations

from phase5.aggregator import _count_severities


def deduplicate(unified: dict) -> dict:
    """
    Mark redundant findings as deduplicated.

    Rules:
    1. P2 schema MISSING + P3 null_blank (>=99% missing) for same column → keep P2
    2. P2 datatype + P3 domain/format check for same column → keep P2
    3. P4 cross-source findings are never deduplicated against P2/P3
    """
    for group, sdata in unified.get("sources", {}).items():
        issues = sdata["issues"]

        # Build lookup: staging_column → P2 schema_missing issues
        p2_missing_by_col: dict[str, dict] = {}
        for issue in issues:
            if issue.get("phase") == 2 and issue.get("check") == "schema_missing":
                col = issue.get("staging_column")
                if col:
                    p2_missing_by_col[col] = issue

        # Build lookup: staging_column → P2 datatype issues
        p2_datatype_by_col: dict[str, dict] = {}
        for issue in issues:
            if issue.get("phase") == 2 and issue.get("check") == "datatype":
                col = issue.get("staging_column")
                if col:
                    p2_datatype_by_col[col] = issue

        # Apply dedup rules
        for issue in issues:
            if issue.get("phase") != 3:
                continue

            col = issue.get("staging_column")
            if not col:
                continue

            # Rule 1: P2 schema MISSING + P3 null_blank (>=99%)
            if issue.get("check") == "null_blank" and col in p2_missing_by_col:
                missing_pct = issue.get("missing_pct")
                if missing_pct is not None and missing_pct >= 99.0:
                    issue["deduplicated"] = True
                    issue["dedupe_reason"] = f"Subsumed by Phase 2 schema finding for {col}"
                    # Enrich P2 finding with P3 row count
                    total_missing = issue.get("total_missing")
                    if total_missing:
                        p2_missing_by_col[col]["affected_count"] = total_missing

            # Rule 2: P2 datatype + P3 domain/format for same column
            if issue.get("check") in ("format_check", "domain_check") and col in p2_datatype_by_col:
                issue["deduplicated"] = True
                issue["dedupe_reason"] = f"Subsumed by Phase 2 datatype finding for {col}"

        # Recompute severity counts excluding deduplicated
        sdata["severity_counts"] = _count_severities(issues)

    return unified
