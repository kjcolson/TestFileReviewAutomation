"""
phase3/scheduling.py

Source-specific data quality checks for scheduling files.
Checks S1-S9.
"""

from __future__ import annotations

import re
from calendar import monthrange
from datetime import date

import pandas as pd

from shared.column_utils import resolve_column

# Appointment status keyword maps
_STATUS_MAP = {
    "Completed": re.compile(r"complet|comp\b|arrived|checked.?out|checked out|visit complete", re.I),
    "Cancelled": re.compile(r"cancel|canc\b|cx\b|canceled", re.I),
    "Rescheduled": re.compile(r"rescheduled|rsch\b|reschedule", re.I),
    "No Show": re.compile(r"no.?show|ns\b|noshow", re.I),
    "Scheduled": re.compile(r"scheduled|pending|future|open slot", re.I),
    "Checked In": re.compile(r"check.?in|checkin\b|arrived|roomed", re.I),
    "Bumped": re.compile(r"bump", re.I),
    "Left Without": re.compile(r"left without|lwbs|lwot", re.I),
}

_NEW_PT_KW = re.compile(r"new\s*patient|new\s*pt\b|new\s*visit|initial\b|init\b|new\s*consult|^new$", re.I)
_EST_PT_KW = re.compile(r"establish|est\s*pt\b|return\b|returning|follow.?up|fu\b|f/u\b|existing|re-visit|revisit", re.I)


def _classify_status(val: str) -> str | None:
    for cat, pattern in _STATUS_MAP.items():
        if pattern.search(val):
            return cat
    return None


def s1_appointment_status_mapping(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """S1: Appointment status mapping to standard categories."""
    findings = []
    status_col = resolve_column(column_mappings, "ApptStatus")
    if not status_col or status_col not in df.columns:
        return findings

    status_counts = df[status_col].dropna().astype(str).str.strip().value_counts()
    unclassifiable = []
    classification = {}

    for val, cnt in status_counts.items():
        cat = _classify_status(val)
        if cat:
            classification[val] = {"category": cat, "count": int(cnt)}
        else:
            unclassifiable.append({"value": val, "count": int(cnt)})

    unclass_rows = sum(r["count"] for r in unclassifiable)
    unclass_pct = unclass_rows / max(len(df), 1) * 100

    if unclass_pct > 10:
        sev = "HIGH"
    elif unclass_pct > 0:
        sev = "MEDIUM"
    else:
        sev = "INFO"

    findings.append({
        "check": "S1",
        "raw_column": status_col,
        "unclassifiable_count": len(unclassifiable),
        "unclassifiable_rows": unclass_rows,
        "unclassifiable_pct": round(unclass_pct, 2),
        "classification": classification,
        "unclassifiable_values": unclassifiable[:10],
        "severity": sev,
        "message": (
            f"S1: {len(unclassifiable)} unclassifiable appointment status values "
            f"({unclass_pct:.1f}% of rows)"
        ) if unclassifiable else (
            f"S1: All appointment statuses mapped to standard categories"
        ),
    })

    return findings


def s2_cancelled_appointment_completeness(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """S2: Cancelled appointment completeness."""
    findings = []
    status_col = resolve_column(column_mappings, "ApptStatus")
    cancel_date_col = resolve_column(column_mappings, "CancellationDate")
    cancel_reason_col = resolve_column(column_mappings, "CancelReason")

    if not status_col or status_col not in df.columns:
        return findings

    cancel_mask = df[status_col].astype(str).apply(
        lambda v: _classify_status(v) in ("Cancelled", "Rescheduled")
    )
    cancelled_df = df[cancel_mask]

    if len(cancelled_df) == 0:
        return findings

    if cancel_date_col and cancel_date_col in df.columns:
        no_date = cancelled_df[cancel_date_col].isna() | (
            cancelled_df[cancel_date_col].astype(str).str.strip() == ""
        )
        no_date_count = int(no_date.sum())
        no_date_pct = no_date_count / len(cancelled_df) * 100
        if no_date_pct > 20:
            sev = "HIGH"
        elif no_date_pct > 0:
            sev = "MEDIUM"
        else:
            sev = "INFO"
        if no_date_count > 0:
            findings.append({
                "check": "S2",
                "cancelled_row_count": len(cancelled_df),
                "missing_cancel_date_count": no_date_count,
                "missing_cancel_date_pct": round(no_date_pct, 2),
                "severity": sev,
                "message": f"S2: {no_date_count:,} ({no_date_pct:.1f}%) cancelled appointments have no Cancel Date",
            })

    if cancel_reason_col and cancel_reason_col in df.columns:
        no_reason = cancelled_df[cancel_reason_col].isna() | (
            cancelled_df[cancel_reason_col].astype(str).str.strip() == ""
        )
        no_reason_count = int(no_reason.sum())
        no_reason_pct = no_reason_count / len(cancelled_df) * 100
        if no_reason_pct > 20:
            findings.append({
                "check": "S2",
                "missing_cancel_reason_count": no_reason_count,
                "missing_cancel_reason_pct": round(no_reason_pct, 2),
                "severity": "MEDIUM",
                "message": f"S2: {no_reason_count:,} ({no_reason_pct:.1f}%) cancelled appointments have no Cancel Reason",
            })

    # Non-cancelled rows with cancel date
    if cancel_date_col and cancel_date_col in df.columns:
        non_cancel_with_date = df[~cancel_mask & df[cancel_date_col].notna()]
        nc_count = len(non_cancel_with_date)
        if nc_count > 0:
            findings.append({
                "check": "S2",
                "non_cancelled_with_cancel_date": nc_count,
                "severity": "MEDIUM",
                "message": f"S2: {nc_count:,} non-cancelled appointments have a Cancel Date populated",
            })

    return findings


def s3_appointment_time_duration(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """S3: Appointment time and duration logic."""
    findings = []
    status_col = resolve_column(column_mappings, "ApptStatus")
    duration_col = resolve_column(column_mappings, "ApptSchdLength")

    if not duration_col or duration_col not in df.columns:
        return findings

    completed_mask = pd.Series([True] * len(df), index=df.index)
    if status_col and status_col in df.columns:
        completed_mask = df[status_col].astype(str).apply(
            lambda v: _classify_status(v) == "Completed"
        )

    completed_df = df[completed_mask]
    if len(completed_df) == 0:
        return findings

    durations = pd.to_numeric(completed_df[duration_col], errors="coerce")
    zero_count = int((durations == 0).sum())
    null_count = int(durations.isna().sum())
    extreme_long = int((durations > 480).sum())
    extreme_short = int((durations < 5).sum())

    if zero_count + null_count > 0:
        findings.append({
            "check": "S3",
            "raw_column": duration_col,
            "zero_count": zero_count,
            "null_count": null_count,
            "severity": "MEDIUM",
            "message": f"S3: {zero_count + null_count:,} completed appointments have zero or null duration",
        })

    if extreme_long > 0:
        findings.append({
            "check": "S3",
            "raw_column": duration_col,
            "extreme_long_count": extreme_long,
            "severity": "INFO",
            "message": f"S3: {extreme_long:,} appointments > 480 minutes (> 8 hours)",
        })

    return findings


def s4_checkin_checkout_validation(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """S4: Check In / Check Out validation."""
    findings = []
    ci_date_col = resolve_column(column_mappings, "CheckInDate")
    co_date_col = resolve_column(column_mappings, "CheckOutDate")
    ci_time_col = resolve_column(column_mappings, "CheckInTime")
    co_time_col = resolve_column(column_mappings, "CheckOutTime")

    # Need at least check-in and check-out
    if not (ci_date_col or ci_time_col) or not (co_date_col or co_time_col):
        return findings

    ci_col = ci_date_col if (ci_date_col and ci_date_col in df.columns) else ci_time_col
    co_col = co_date_col if (co_date_col and co_date_col in df.columns) else co_time_col

    if not ci_col or ci_col not in df.columns or not co_col or co_col not in df.columns:
        return findings

    ci = pd.to_datetime(df[ci_col], errors="coerce")
    co = pd.to_datetime(df[co_col], errors="coerce")

    both_valid = ci.notna() & co.notna()
    checkout_before_checkin = int((both_valid & (co < ci)).sum())
    gap_over_12h = int((both_valid & ((co - ci).dt.total_seconds() > 43200)).sum())

    if checkout_before_checkin > 0:
        findings.append({
            "check": "S4",
            "checkout_before_checkin_count": checkout_before_checkin,
            "severity": "MEDIUM",
            "message": f"S4: {checkout_before_checkin:,} rows where Check Out is before Check In",
        })

    if gap_over_12h > 0:
        findings.append({
            "check": "S4",
            "gap_over_12h_count": gap_over_12h,
            "severity": "INFO",
            "message": f"S4: {gap_over_12h:,} appointments with check-in/out gap > 12 hours",
        })

    return findings


def s5_patient_id_format(
    df: pd.DataFrame, column_mappings: list[dict]
) -> tuple[list[dict], dict]:
    """S5: Patient Identifier format for Phase 4 matching. Returns (findings, cross_source_prep)."""
    findings = []
    prep = {}

    pat_col = resolve_column(column_mappings, "PatIdOrig")
    if not pat_col or pat_col not in df.columns:
        return findings, prep

    series = df[pat_col].dropna().astype(str).str.strip()
    series = series[series != ""]
    if len(series) == 0:
        return findings, prep

    all_numeric = series.str.match(r"^\d+$").all()
    lengths = series.str.len()
    has_leading_zeros = series.str.match(r"^0\d+").any()

    if all_numeric:
        fmt = f"numeric_{int(lengths.median())}digit"
    else:
        fmt = "alphanumeric"

    prep = {
        "patient_id_column": pat_col,
        "patient_id_format": fmt,
        "patient_id_leading_zeros": bool(has_leading_zeros),
        "patient_id_sample_count": len(series),
    }

    findings.append({
        "check": "S5",
        "raw_column": pat_col,
        "detected_format": fmt,
        "leading_zeros": bool(has_leading_zeros),
        "severity": "INFO",
        "message": f"S5: Scheduling Patient ID format: '{fmt}' — compare with Billing B12 for Phase 4 matching",
    })

    return findings, prep


def s6_location_npi_validation(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """S6: Location / Provider NPI validation."""
    findings = []
    npi_col = resolve_column(column_mappings, "ApptProvNPI")
    loc_col = resolve_column(column_mappings, "BillLocNameOrig")

    if npi_col and npi_col in df.columns:
        npi_series = df[npi_col].dropna().astype(str).str.strip()
        npi_series = npi_series[npi_series != ""]
        npi_counts = npi_series.value_counts()
        distinct = len(npi_counts)

        if distinct > 0:
            top_pct = int(npi_counts.iloc[0]) / len(npi_series) * 100
            findings.append({
                "check": "S6",
                "raw_column": npi_col,
                "distinct_npi_count": distinct,
                "top_npi_pct": round(top_pct, 2),
                "severity": "INFO",
                "message": f"S6: {distinct:,} distinct Appt Provider NPIs",
            })

    if loc_col and loc_col in df.columns:
        loc_series = df[loc_col].dropna().astype(str).str.strip()
        loc_series = loc_series[loc_series != ""]
        distinct_locs = loc_series.nunique()

        # Flag if location appears to be numeric IDs rather than names
        numeric_locs = int(loc_series.str.match(r"^\d+$").sum())
        if numeric_locs > len(loc_series) * 0.5:
            findings.append({
                "check": "S6",
                "raw_column": loc_col,
                "numeric_location_count": numeric_locs,
                "severity": "MEDIUM",
                "message": f"S6: '{loc_col}' appears to contain numeric IDs rather than location names ({numeric_locs:,} numeric values)",
            })
        else:
            findings.append({
                "check": "S6",
                "raw_column": loc_col,
                "distinct_location_count": distinct_locs,
                "severity": "INFO",
                "message": f"S6: {distinct_locs:,} distinct location names in scheduling data",
            })

    return findings


def s7_appointment_date_range(
    df: pd.DataFrame, column_mappings: list[dict], test_month: str
) -> list[dict]:
    """S7: Appointment date range validation."""
    findings = []
    appt_col = resolve_column(column_mappings, "ApptDate")
    created_col = resolve_column(column_mappings, "CreateDate")

    if not appt_col or appt_col not in df.columns or not test_month:
        return findings

    yr, mo = int(test_month[:4]), int(test_month[5:7])
    window_start = date(yr, mo, 1)
    window_end = date(yr, mo, monthrange(yr, mo)[1])

    appt_dates = pd.to_datetime(df[appt_col], errors="coerce")
    valid = appt_dates.dropna()
    if len(valid) == 0:
        return findings

    outside = ((valid.dt.date < window_start) | (valid.dt.date > window_end)).sum()
    pct = outside / len(df) * 100

    sev = "HIGH" if pct > 5 else ("MEDIUM" if pct > 0 else "INFO")

    findings.append({
        "check": "S7",
        "raw_column": appt_col,
        "expected_window": f"{window_start} to {window_end}",
        "outside_count": int(outside),
        "outside_pct": round(pct, 2),
        "severity": sev,
        "message": (
            f"S7: {outside:,} appointment rows ({pct:.1f}%) outside expected month "
            f"{window_start} to {window_end}"
        ),
    })

    # Created Date after Appt Date
    if created_col and created_col in df.columns:
        created = pd.to_datetime(df[created_col], errors="coerce")
        retroactive = int((created.notna() & appt_dates.notna() & (created > appt_dates)).sum())
        retro_pct = retroactive / max(len(df), 1) * 100
        if retro_pct > 5:
            findings.append({
                "check": "S7",
                "retroactive_scheduling_count": retroactive,
                "retroactive_pct": round(retro_pct, 2),
                "severity": "MEDIUM",
                "message": f"S7: {retroactive:,} rows ({retro_pct:.1f}%) where Created Date > Appointment Date (retroactive scheduling)",
            })

    return findings


def s8_status_distribution(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """S8: Appointment status distribution sanity check."""
    findings = []
    status_col = resolve_column(column_mappings, "ApptStatus")
    if not status_col or status_col not in df.columns:
        return findings

    status_series = df[status_col].dropna().astype(str).str.strip()
    cat_counts: dict[str, int] = {}

    for val in status_series:
        cat = _classify_status(val)
        if cat:
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

    total = sum(cat_counts.values())
    if total == 0:
        return findings

    # Completed should be highest
    completed_count = sum(v for k, v in cat_counts.items() if k in ("Completed", "Checked In"))
    completed_pct = completed_count / total * 100

    # Find highest category
    top_cat = max(cat_counts, key=cat_counts.get)
    top_pct = cat_counts[top_cat] / total * 100

    if top_cat not in ("Completed", "Checked In") and top_pct > completed_pct:
        findings.append({
            "check": "S8",
            "top_category": top_cat,
            "top_category_pct": round(top_pct, 2),
            "completed_pct": round(completed_pct, 2),
            "severity": "HIGH",
            "message": (
                f"S8: '{top_cat}' ({top_pct:.1f}%) is the largest status category, "
                f"not 'Completed' ({completed_pct:.1f}%) — may indicate data filtering issue"
            ),
        })

    cancel_count = sum(v for k, v in cat_counts.items() if k in ("Cancelled", "Rescheduled"))
    if cancel_count == 0:
        findings.append({
            "check": "S8",
            "severity": "HIGH",
            "message": "S8: Zero rows with Cancelled or Rescheduled status — cancellation data appears missing",
        })

    if cat_counts.get("No Show", 0) == 0:
        findings.append({
            "check": "S8",
            "severity": "MEDIUM",
            "message": "S8: Zero No Show appointments — may limit access analysis (some systems fold No Shows into Cancelled)",
        })

    dist = [
        {"category": k, "count": v, "pct": round(v / total * 100, 2)}
        for k, v in sorted(cat_counts.items(), key=lambda x: -x[1])
    ]
    findings.append({
        "check": "S8",
        "status_distribution": dist,
        "severity": "INFO",
        "message": f"S8: Status distribution — {len(cat_counts)} categories, {total:,} classifiable rows",
    })

    return findings


def s9_appointment_type_coverage(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """S9: Appointment type coverage (New vs Established)."""
    findings = []
    type_col = resolve_column(column_mappings, "ApptType")
    if not type_col or type_col not in df.columns:
        return findings

    type_series = df[type_col].dropna().astype(str).str.strip()
    type_counts = type_series.value_counts()

    has_new = any(_NEW_PT_KW.search(v) for v in type_counts.index)
    has_est = any(_EST_PT_KW.search(v) for v in type_counts.index)

    if not has_new and not has_est:
        findings.append({
            "check": "S9",
            "severity": "HIGH",
            "message": "S9: Cannot identify New Patient or Established/Return appointment types — new vs established analysis not possible",
        })
    elif not has_new or not has_est:
        missing = "Established/Return" if has_new else "New Patient"
        findings.append({
            "check": "S9",
            "severity": "MEDIUM",
            "message": f"S9: Only one appointment type category found — '{missing}' not identifiable",
        })
    else:
        new_count = sum(v for k, v in type_counts.items() if _NEW_PT_KW.search(k))
        est_count = sum(v for k, v in type_counts.items() if _EST_PT_KW.search(k))
        total = new_count + est_count
        new_pct = new_count / max(total, 1) * 100

        if new_pct < 5 or new_pct > 50:
            findings.append({
                "check": "S9",
                "new_pct": round(new_pct, 2),
                "severity": "MEDIUM",
                "message": f"S9: New patient % is {new_pct:.1f}% — outside typical range (10-30%)",
            })
        else:
            findings.append({
                "check": "S9",
                "new_pct": round(new_pct, 2),
                "severity": "INFO",
                "message": f"S9: New patient {new_pct:.1f}%, Established {100 - new_pct:.1f}% — distribution within expected range",
            })

    findings.append({
        "check": "S9",
        "distinct_appt_types": len(type_counts),
        "severity": "INFO",
        "message": f"S9: {len(type_counts)} distinct appointment types",
    })

    return findings


def run_checks(
    df: pd.DataFrame,
    column_mappings: list[dict],
    test_month: str,
) -> tuple[list[dict], dict]:
    """Run all scheduling-specific checks. Returns (findings, cross_source_prep)."""
    findings = []
    cross_source_prep = {}

    findings.extend(s1_appointment_status_mapping(df, column_mappings))
    findings.extend(s2_cancelled_appointment_completeness(df, column_mappings))
    findings.extend(s3_appointment_time_duration(df, column_mappings))
    findings.extend(s4_checkin_checkout_validation(df, column_mappings))
    s5_f, prep = s5_patient_id_format(df, column_mappings)
    findings.extend(s5_f)
    if prep:
        cross_source_prep.update(prep)
    findings.extend(s6_location_npi_validation(df, column_mappings))
    findings.extend(s7_appointment_date_range(df, column_mappings, test_month))
    findings.extend(s8_status_distribution(df, column_mappings))
    findings.extend(s9_appointment_type_coverage(df, column_mappings))

    return findings, cross_source_prep
