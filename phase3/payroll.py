"""
phase3/payroll.py

Source-specific data quality checks for payroll files.
Checks P1-P7.
"""

from __future__ import annotations

import re

import pandas as pd

from shared.column_utils import resolve_column

# Job code classification keywords
_PHYSICIAN_KW = re.compile(r"physician|md\b|do\b|doctor|surgeon|hospitalist", re.I)
_APP_KW = re.compile(r"\bapp\b|np\b|nurse\s*pract|pa\b|physician\s*asst|pa-c\b|aprn\b|crna\b", re.I)
_RN_KW = re.compile(r"\brn\b|registered\s*nurse|staff\s*nurse", re.I)
_LPN_KW = re.compile(r"\blpn\b|lvn\b|licensed\s*practical|licensed\s*vocational", re.I)
_MA_KW = re.compile(r"\bma\b|medical\s*asst|medical\s*assistant|cma\b|clinical\s*asst", re.I)
_CNA_KW = re.compile(r"\bcna\b|certified\s*nursing|nursing\s*asst", re.I)
_CLINICAL_KW = re.compile(r"\bnurse\b|clinical|tech\b|technician|therapist|phlebotom", re.I)
_ADMIN_KW = re.compile(
    r"patient\s*access|front\s*desk|receptionist|registration|scheduler|scheduling|"
    r"check.?in|administrative|admin\b|office\b|clerical|secretary|coordinator|"
    r"billing\b|coding\b|collections|revenue\s*cycle", re.I
)
_MGMT_KW = re.compile(r"manager|director|supervisor|administrator|chief|vp\b|vice\s*pres", re.I)

# Earnings code keywords for classification
_OT_KW = re.compile(r"overtime|ot\b|o/t\b", re.I)
_PTO_KW = re.compile(r"pto\b|vacation|sick|holiday|leave|time\s*off|personal\s*day", re.I)
_BONUS_KW = re.compile(r"bonus|incentive|productivity|quality\s*pay", re.I)
_CALL_KW = re.compile(r"call\s*pay|on.?call|callback", re.I)
_REG_KW = re.compile(r"regular|base|salary|hourly|straight|reg\b|biweekly|semi.?monthly", re.I)
_BENEFIT_KW = re.compile(r"benefit|retirement|401|403|pension|fica|tax|insurance\s*contrib", re.I)


def _classify_job(desc: str) -> str:
    if _PHYSICIAN_KW.search(desc):
        return "Physician"
    if _APP_KW.search(desc):
        return "APP"
    if _RN_KW.search(desc):
        return "RN"
    if _LPN_KW.search(desc):
        return "LPN"
    if _MA_KW.search(desc) or _CNA_KW.search(desc):
        return "MA/CNA"
    if _CLINICAL_KW.search(desc):
        return "Other Clinical"
    if _ADMIN_KW.search(desc):
        return "Admin/Patient Access"
    if _MGMT_KW.search(desc):
        return "Management"
    return "Unclassified"


def _classify_earnings(desc: str) -> str:
    if _OT_KW.search(desc):
        return "Overtime"
    if _PTO_KW.search(desc):
        return "PTO/Leave"
    if _BONUS_KW.search(desc):
        return "Bonus/Incentive"
    if _CALL_KW.search(desc):
        return "Call Pay"
    if _REG_KW.search(desc):
        return "Base/Regular"
    if _BENEFIT_KW.search(desc):
        return "Benefits/Retirement"
    return "Unclassified"


def p1_hours_reasonableness(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """P1: Hours reasonableness check."""
    findings = []
    hours_col = resolve_column(column_mappings, "Hours")
    earn_col = resolve_column(column_mappings, "EarningsCodeDesc")
    emp_col = resolve_column(column_mappings, "EmployeeId")
    period_col = resolve_column(column_mappings, "PayPeriodEndDate")

    if not hours_col or hours_col not in df.columns:
        return findings

    hours = pd.to_numeric(df[hours_col], errors="coerce")

    # Detect adjustment earnings codes (negative hours may be valid there)
    adj_mask = pd.Series([False] * len(df), index=df.index)
    if earn_col and earn_col in df.columns:
        earn_series = df[earn_col].astype(str).str.lower()
        adj_mask = earn_series.str.contains(r"adj|correct|reversal|reissue", na=False)

    neg_count = int((hours < 0).sum())
    neg_non_adj = int(((hours < 0) & ~adj_mask).sum())
    extreme_count = int((hours > 200).sum())

    if neg_non_adj > 0:
        findings.append({
            "check": "P1",
            "raw_column": hours_col,
            "negative_non_adjustment_count": neg_non_adj,
            "severity": "HIGH",
            "message": f"P1: {neg_non_adj:,} negative hours on non-adjustment earnings codes",
        })

    if extreme_count > 0:
        findings.append({
            "check": "P1",
            "raw_column": hours_col,
            "extreme_count": extreme_count,
            "severity": "MEDIUM",
            "message": f"P1: {extreme_count:,} rows with hours > 200 per earnings code",
        })

    # Per-employee per-period total hours
    if emp_col and emp_col in df.columns and period_col and period_col in df.columns:
        try:
            grp = df.groupby([emp_col, period_col])[hours_col].apply(
                lambda s: pd.to_numeric(s, errors="coerce").sum()
            )
            extreme_emp = int((grp > 400).sum())
            zero_emp = int((grp == 0).sum())
            if extreme_emp > 0:
                findings.append({
                    "check": "P1",
                    "extreme_employee_periods": extreme_emp,
                    "severity": "MEDIUM",
                    "message": f"P1: {extreme_emp:,} employee-pay-period combinations exceed 400 total hours",
                })
        except Exception:
            pass

    findings.append({
        "check": "P1",
        "raw_column": hours_col,
        "total_negative": neg_count,
        "severity": "INFO",
        "message": f"P1: Hours range: {hours.min():.1f} to {hours.max():.1f}; {neg_count:,} negative rows",
    })

    return findings


def p2_amount_reasonableness(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """P2: Amount reasonableness."""
    findings = []
    amt_col = resolve_column(column_mappings, "AmountOrig")
    earn_col = resolve_column(column_mappings, "EarningsCodeDesc")
    hours_col = resolve_column(column_mappings, "Hours")

    if not amt_col or amt_col not in df.columns:
        return findings

    amounts = pd.to_numeric(df[amt_col], errors="coerce")

    adj_mask = pd.Series([False] * len(df), index=df.index)
    if earn_col and earn_col in df.columns:
        earn_series = df[earn_col].astype(str).str.lower()
        adj_mask = earn_series.str.contains(r"adj|correct|reversal|reissue", na=False)

    neg_non_adj = int(((amounts < 0) & ~adj_mask).sum())
    extreme = int((amounts.abs() > 500000).sum())

    if neg_non_adj > 0:
        findings.append({
            "check": "P2",
            "raw_column": amt_col,
            "negative_non_adjustment_count": neg_non_adj,
            "severity": "MEDIUM",
            "message": f"P2: {neg_non_adj:,} negative amounts on non-adjustment earnings codes",
        })

    if extreme > 0:
        findings.append({
            "check": "P2",
            "raw_column": amt_col,
            "extreme_count": extreme,
            "severity": "MEDIUM",
            "message": f"P2: {extreme:,} rows with |amount| > $500,000",
        })

    # Implied hourly rate check
    if hours_col and hours_col in df.columns and earn_col and earn_col in df.columns:
        hours = pd.to_numeric(df[hours_col], errors="coerce")
        earn_series = df[earn_col].astype(str).str.lower()
        hourly_mask = earn_series.str.contains(r"hourly|regular|hour", na=False) & (hours > 0)
        if hourly_mask.sum() > 0:
            rate = amounts[hourly_mask] / hours[hourly_mask]
            high_rate = int((rate > 1000).sum())
            low_rate = int((rate < 7).sum())
            if high_rate > 0:
                findings.append({
                    "check": "P2",
                    "high_rate_count": high_rate,
                    "severity": "MEDIUM",
                    "message": f"P2: {high_rate:,} hourly-type rows imply rate > $1,000/hr",
                })
            if low_rate > 0:
                findings.append({
                    "check": "P2",
                    "low_rate_count": low_rate,
                    "severity": "INFO",
                    "message": f"P2: {low_rate:,} hourly-type rows imply rate < $7/hr",
                })

    return findings


def p3_pay_period_logic(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """P3: Pay period date logic."""
    findings = []
    start_col = resolve_column(column_mappings, "PayPeriodStartDate")
    end_col = resolve_column(column_mappings, "PayPeriodEndDate")
    check_col = resolve_column(column_mappings, "CheckDate")

    if not end_col or end_col not in df.columns:
        return findings

    end_dates = pd.to_datetime(df[end_col], errors="coerce")

    if start_col and start_col in df.columns:
        start_dates = pd.to_datetime(df[start_col], errors="coerce")
        both_valid = start_dates.notna() & end_dates.notna()
        inverted = int((both_valid & (start_dates > end_dates)).sum())
        if inverted > 0:
            findings.append({
                "check": "P3",
                "inverted_count": inverted,
                "severity": "CRITICAL",
                "message": f"P3: {inverted:,} rows where Pay Period Start > End Date",
            })

        # Pay period length distribution
        lengths = (end_dates[both_valid] - start_dates[both_valid]).dt.days + 1
        valid_lengths = {7, 14, 15, 16, 28, 29, 30, 31}
        unusual = lengths[~lengths.isin(valid_lengths)]
        if len(unusual) > len(lengths) * 0.1:
            findings.append({
                "check": "P3",
                "unusual_length_count": len(unusual),
                "common_lengths": lengths.value_counts().head(5).to_dict(),
                "severity": "MEDIUM",
                "message": f"P3: {len(unusual):,} pay periods have unusual length (not 7/14/15/30 days)",
            })
        else:
            cadence = lengths.value_counts().index[0] if len(lengths) > 0 else "unknown"
            findings.append({
                "check": "P3",
                "dominant_cadence_days": int(cadence) if len(lengths) > 0 else None,
                "severity": "INFO",
                "message": f"P3: Pay period cadence: {cadence}-day periods",
            })

    if check_col and check_col in df.columns:
        check_dates = pd.to_datetime(df[check_col], errors="coerce")
        both_valid = check_dates.notna() & end_dates.notna()
        early_check = int((both_valid & (check_dates < end_dates)).sum())
        if early_check > 0:
            findings.append({
                "check": "P3",
                "check_before_period_end_count": early_check,
                "severity": "MEDIUM",
                "message": f"P3: {early_check:,} rows where Check Date is before Pay Period End Date",
            })

    return findings


def p4_department_gl_linkage(
    df: pd.DataFrame, column_mappings: list[dict]
) -> tuple[list[dict], dict]:
    """P4: Department ID / GL linkage preparation. Returns (findings, cross_source_prep)."""
    findings = []
    prep = {}

    dept_id_col = resolve_column(column_mappings, "DepartmentId")
    dept_name_col = resolve_column(column_mappings, "DepartmentName")

    if dept_id_col and dept_id_col in df.columns:
        dept_series = df[dept_id_col].dropna().astype(str).str.strip()
        dept_series = dept_series[dept_series != ""]
        pop_rate = len(dept_series) / max(len(df), 1) * 100
        distinct_count = dept_series.nunique()

        if pop_rate < 50:
            sev = "HIGH"
        else:
            sev = "INFO"

        all_numeric = dept_series.str.match(r"^\d+$").all()
        lengths = dept_series.str.len()
        fmt = f"numeric_{int(lengths.median())}digit" if all_numeric else "alphanumeric"

        # Flag catch-all departments
        dept_counts = dept_series.value_counts()
        catchall_threshold = 0.30
        catchall = dept_counts[dept_counts / len(dept_series) > catchall_threshold]

        findings.append({
            "check": "P4",
            "raw_column": dept_id_col,
            "population_pct": round(pop_rate, 2),
            "distinct_count": distinct_count,
            "detected_format": fmt,
            "severity": sev,
            "message": f"P4: Department ID {pop_rate:.1f}% populated, {distinct_count} distinct values, format: {fmt}",
        })

        if len(catchall) > 0:
            findings.append({
                "check": "P4",
                "dominant_departments": [{"dept": str(k), "pct": round(v / len(dept_series) * 100, 2)} for k, v in catchall.items()],
                "severity": "INFO",
                "message": f"P4: {len(catchall)} departments represent > 30% of payroll rows (possible catch-all)",
            })

        prep = {
            "department_id_column": dept_id_col,
            "department_id_format": fmt,
            "department_distinct_count": distinct_count,
            "department_population_pct": round(pop_rate, 2),
        }

    return findings, prep


def p5_employee_npi(
    df: pd.DataFrame, column_mappings: list[dict]
) -> tuple[list[dict], dict]:
    """P5: Employee NPI for provider identification. Returns (findings, cross_source_prep)."""
    findings = []
    prep = {}

    npi_col = resolve_column(column_mappings, "EmployeeNpi")
    job_col = resolve_column(column_mappings, "JobCodeDesc")

    if not job_col or job_col not in df.columns:
        return findings, prep

    job_series = df[job_col].astype(str)
    provider_mask = job_series.apply(lambda v: _PHYSICIAN_KW.search(v) is not None or _APP_KW.search(v) is not None)
    provider_df = df[provider_mask]

    if len(provider_df) == 0:
        findings.append({
            "check": "P5",
            "severity": "INFO",
            "message": "P5: No provider-type employees identified in job codes",
        })
        return findings, prep

    if npi_col and npi_col in df.columns:
        npi_series = provider_df[npi_col]
        missing_npi = npi_series.isna() | (npi_series.astype(str).str.strip() == "")
        missing_count = int(missing_npi.sum())
        missing_pct = missing_count / len(provider_df) * 100

        populated_npis = npi_series[~missing_npi].astype(str).str.strip()
        pop_pct = (len(provider_df) - missing_count) / max(len(provider_df), 1) * 100

        if missing_pct > 20:
            sev = "HIGH"
        elif missing_pct > 0:
            sev = "MEDIUM"
        else:
            sev = "INFO"

        findings.append({
            "check": "P5",
            "raw_column": npi_col,
            "provider_employee_count": len(provider_df),
            "missing_npi_count": missing_count,
            "missing_npi_pct": round(missing_pct, 2),
            "severity": sev,
            "message": (
                f"P5: {missing_count:,} of {len(provider_df):,} provider employees ({missing_pct:.1f}%) "
                f"have no NPI"
            ),
        })

        prep = {
            "employee_npi_column": npi_col,
            "provider_npi_population_pct": round(pop_pct, 2),
            "provider_employee_count": len(provider_df),
        }
    else:
        findings.append({
            "check": "P5",
            "severity": "INFO",
            "message": "P5: Employee NPI column not mapped — cannot check provider NPI population",
        })

    return findings, prep


def p6_job_earnings_classification(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """P6: Job Code and Earnings Code classification."""
    findings = []

    job_col = resolve_column(column_mappings, "JobCodeDesc")
    earn_col = resolve_column(column_mappings, "EarningsCodeDesc")

    for col_name, col_key, classify_fn, check_id in [
        (job_col, "JobCode", _classify_job, "P6"),
        (earn_col, "EarningsCode", _classify_earnings, "P6"),
    ]:
        if not col_name or col_name not in df.columns:
            continue

        desc_counts = df[col_name].dropna().astype(str).str.strip().value_counts()
        classifications: dict[str, int] = {}
        unclassified = []

        for val, cnt in desc_counts.items():
            cat = classify_fn(val)
            if cat == "Unclassified":
                unclassified.append({"value": val, "count": int(cnt)})
            else:
                classifications[cat] = classifications.get(cat, 0) + int(cnt)

        total = sum(desc_counts.values)
        unclass_count = sum(r["count"] for r in unclassified)
        unclass_pct = unclass_count / max(total, 1) * 100

        sev = "MEDIUM" if unclass_pct > 15 else "INFO"
        findings.append({
            "check": check_id,
            "field": col_key,
            "raw_column": col_name,
            "unclassified_pct": round(unclass_pct, 2),
            "classification_summary": classifications,
            "unclassified_values": unclassified[:20],
            "severity": sev,
            "message": (
                f"P6: {col_key} — {unclass_pct:.1f}% of values unclassifiable "
                f"({len(unclassified)} distinct unclassified codes)"
            ),
        })

    return findings


def p7_support_staff_presence(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """P7: Support staff presence validation."""
    findings = []
    job_col = resolve_column(column_mappings, "JobCodeDesc")
    emp_col = resolve_column(column_mappings, "EmployeeId")

    if not job_col or job_col not in df.columns:
        return findings

    # Classify employees
    if emp_col and emp_col in df.columns:
        emp_job = df.drop_duplicates(subset=[emp_col])[[emp_col, job_col]]
    else:
        emp_job = df[[job_col]].copy()
        emp_job["_emp"] = range(len(emp_job))
        emp_col = "_emp"

    cat_employees: dict[str, list] = {
        "Physician": [], "APP": [], "RN": [], "LPN": [], "MA/CNA": [],
        "Other Clinical": [], "Admin/Patient Access": [], "Management": [], "Unclassified": [],
    }

    for _, row in emp_job.iterrows():
        desc = str(row.get(job_col, "") or "")
        cat = _classify_job(desc)
        emp_id = row.get(emp_col, "")
        cat_employees[cat].append(emp_id)

    cat_counts = {k: len(v) for k, v in cat_employees.items()}
    total_emp = sum(cat_counts.values())

    # Check required categories
    provider_count = cat_counts.get("Physician", 0) + cat_counts.get("APP", 0)
    clinical_support = cat_counts.get("RN", 0) + cat_counts.get("LPN", 0) + cat_counts.get("MA/CNA", 0) + cat_counts.get("Other Clinical", 0)
    non_clinical = cat_counts.get("Admin/Patient Access", 0)

    if clinical_support == 0:
        findings.append({
            "check": "P7",
            "severity": "CRITICAL",
            "message": "P7: CRITICAL — Zero clinical support staff (RN/LPN/MA) found in payroll — extract appears incomplete",
        })

    if non_clinical == 0:
        findings.append({
            "check": "P7",
            "severity": "HIGH",
            "message": "P7: Zero non-clinical/patient access staff found (front desk, admin, scheduling) — extract may be incomplete",
        })

    if provider_count == 0:
        findings.append({
            "check": "P7",
            "severity": "HIGH",
            "message": "P7: Zero provider employees (Physician/APP) found — may be support-staff-only extract missing provider compensation",
        })

    if total_emp > 0:
        provider_pct = provider_count / total_emp * 100
        if provider_pct > 60 and provider_count > 0:
            findings.append({
                "check": "P7",
                "provider_pct": round(provider_pct, 2),
                "severity": "MEDIUM",
                "message": f"P7: Providers represent {provider_pct:.1f}% of distinct employees — support staff may be excluded",
            })

    dist = [
        {"category": k, "employee_count": v, "pct": round(v / max(total_emp, 1) * 100, 2)}
        for k, v in sorted(cat_counts.items(), key=lambda x: -x[1]) if v > 0
    ]
    dist_str = "; ".join(
        f"{d['category']}: {d['employee_count']}" for d in dist
    )
    findings.append({
        "check": "P7",
        "staff_distribution": dist,
        "total_classified_employees": total_emp,
        "severity": "INFO",
        "message": f"P7: Staff distribution ({total_emp:,} employees) — {dist_str}",
    })

    return findings


def run_checks(
    df: pd.DataFrame,
    column_mappings: list[dict],
    test_month: str,
) -> tuple[list[dict], dict]:
    """Run all payroll-specific checks. Returns (findings, cross_source_prep)."""
    findings = []
    cross_source_prep = {}

    findings.extend(p1_hours_reasonableness(df, column_mappings))
    findings.extend(p2_amount_reasonableness(df, column_mappings))
    findings.extend(p3_pay_period_logic(df, column_mappings))
    p4_f, p4_prep = p4_department_gl_linkage(df, column_mappings)
    findings.extend(p4_f)
    p5_f, p5_prep = p5_employee_npi(df, column_mappings)
    findings.extend(p5_f)
    findings.extend(p6_job_earnings_classification(df, column_mappings))
    findings.extend(p7_support_staff_presence(df, column_mappings))

    cross_source_prep.update(p4_prep)
    cross_source_prep.update(p5_prep)

    return findings, cross_source_prep
