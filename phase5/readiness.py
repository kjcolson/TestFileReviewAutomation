"""
phase5/readiness.py

Determines per-source pass/fail and overall readiness for historical extract.
"""

from __future__ import annotations

import re


def determine(unified: dict, missing_sources: list[str], client_issues: list[dict]) -> dict:
    """
    Return readiness determination dict with per-source verdicts and overall status.
    """
    round_id = unified.get("round", "v1")

    # Per-source verdicts
    per_source: dict[str, dict] = {}
    for group, sdata in unified.get("sources", {}).items():
        counts = sdata.get("severity_counts", {})
        per_source[group] = {
            "display_name": sdata.get("display_name", group.title()),
            "status": _source_verdict(counts),
            "critical": counts.get("CRITICAL", 0),
            "high": counts.get("HIGH", 0),
            "medium": counts.get("MEDIUM", 0),
            "low": counts.get("LOW", 0),
            "info": counts.get("INFO", 0),
            "total": sum(v for k, v in counts.items() if k != "INFO"),
        }

    # Cross-source severity counts
    cross_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    for issue in unified.get("cross_source_issues", []):
        if issue.get("deduplicated"):
            continue
        sev = issue.get("severity", "INFO")
        if sev in cross_counts:
            cross_counts[sev] += 1

    per_source["cross_source"] = {
        "display_name": "Cross-Source (C0-C5)",
        "status": _source_verdict(cross_counts),
        "critical": cross_counts.get("CRITICAL", 0),
        "high": cross_counts.get("HIGH", 0),
        "medium": cross_counts.get("MEDIUM", 0),
        "low": cross_counts.get("LOW", 0),
        "info": cross_counts.get("INFO", 0),
        "total": sum(v for k, v in cross_counts.items() if k != "INFO"),
    }

    # Totals across everything
    total_counts = {"CRITICAL": 0, "HIGH": 0, "MEDIUM": 0, "LOW": 0, "INFO": 0}
    for ps in per_source.values():
        for sev in total_counts:
            total_counts[sev] += ps.get(sev.lower(), 0)

    # Overall determination
    total_critical = total_counts["CRITICAL"]
    total_high = total_counts["HIGH"]

    if total_critical > 0 or missing_sources:
        next_round = _increment_round(round_id)
        overall = f"Needs Revision (Round {next_round})"
        reasons = []
        if total_critical > 0:
            reasons.append(f"{total_critical} CRITICAL issue(s)")
        if missing_sources:
            reasons.append(f"missing source(s): {', '.join(missing_sources)}")
        reason = "; ".join(reasons)
    elif total_high > 0:
        overall = "Conditionally Ready"
        reason = f"{total_high} HIGH issue(s) require review before historical extract"
    else:
        overall = "Ready for Historical Extract"
        reason = "All checks passed with no CRITICAL or HIGH issues"

    return {
        "overall": overall,
        "reason": reason,
        "per_source": per_source,
        "missing_sources": missing_sources,
        "total_counts": total_counts,
    }


def _source_verdict(counts: dict[str, int]) -> str:
    if counts.get("CRITICAL", 0) > 0:
        return "FAIL"
    if counts.get("HIGH", 0) > 0:
        return "CONDITIONAL"
    return "PASS"


def _increment_round(round_id: str) -> str:
    """v1 -> v2, v2 -> v3, etc."""
    match = re.match(r"v(\d+)", round_id)
    if match:
        num = int(match.group(1)) + 1
        if num > 3:
            return f"v{num} (additional rounds may incur charges per contract)"
        return f"v{num}"
    return f"{round_id}+1"
