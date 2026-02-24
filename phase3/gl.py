"""
phase3/gl.py

Source-specific data quality checks for General Ledger files.
Checks G1-G7.
"""

from __future__ import annotations

import re

import pandas as pd

from shared.column_utils import resolve_column

# PIVOT P&L category keyword patterns
_CATEGORY_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("Charges", re.compile(
        r"charge|gross\s*revenue|gross\s*patient\s*rev|patient\s*charge|gross\s*charge|"
        r"fee\s*for\s*service|ffs\s*rev", re.I
    )),
    ("Adjustments", re.compile(
        r"adjustment|contractual|allowance|write.?off|bad\s*debt|charity|discount|"
        r"deduction|contra\s*rev", re.I
    )),
    ("Other Revenue", re.compile(
        r"other\s*rev|other\s*income|misc\s*rev|grant|capitation|incentive\s*rev|"
        r"meaningful\s*use|quality\s*bonus|340b|interest\s*income|rental\s*income", re.I
    )),
    ("Provider Compensation", re.compile(
        r"physician\s*comp|provider\s*comp|provider\s*sal|physician\s*sal|app\s*sal|"
        r"app\s*comp|doctor\s*sal|md\s*comp|do\s*comp|np\s*sal|pa\s*sal|crna\s*sal|"
        r"provider\s*bonus|physician\s*bonus|provider\s*benefit|physician\s*benefit|"
        r"provider\s*retire|physician\s*401|provider\s*fica|physician\s*payroll", re.I
    )),
    ("Support Staff Compensation", re.compile(
        r"staff\s*sal|staff\s*wage|support\s*staff|nursing\s*sal|rn\s*sal|lpn\s*sal|"
        r"ma\s*sal|medical\s*asst|clerical\s*sal|admin\s*sal|front\s*desk|"
        r"patient\s*access|staff\s*benefit|staff\s*retire|staff\s*fica|"
        r"staff\s*payroll|temp\s*labor|agency|overtime", re.I
    )),
    ("Facilities / Occupancy", re.compile(
        r"rent\b|lease\b|occupancy|building|facility|depreciation|amortization|"
        r"maintenance|repair|utilities|electric|water|janitorial|property\s*tax|"
        r"property\s*ins", re.I
    )),
    ("Medical Supplies", re.compile(
        r"medical\s*supply|medical\s*suppl|clinical\s*supply|surgical\s*supply|"
        r"pharmaceutical|drug\b|vaccine|implant|lab\s*supply|reagent", re.I
    )),
    ("Other Operating Expenses", re.compile(
        r"office\s*supply|office\s*suppl|it\b|technology|software|hardware|telephone|"
        r"internet|postage|printing|travel|education|cme\b|dues|subscription|license\b|"
        r"insurance\b|legal|consulting|professional\s*fee|outsource|billing\s*service|"
        r"collection|marketing|advertising|recruitment|malpractice", re.I
    )),
]

_REQUIRED_CATEGORIES = {
    "Charges", "Adjustments", "Provider Compensation",
    "Support Staff Compensation", "Other Operating Expenses",
}
_RECOMMENDED_CATEGORIES = {
    "Other Revenue", "Facilities / Occupancy", "Medical Supplies",
}


def _classify_account(desc: str, acct_type: str = "") -> tuple[str, str]:
    """Return (category, confidence). Confidence: HIGH or LOW."""
    text = f"{acct_type} {desc}".strip()
    matches = []
    for cat, pattern in _CATEGORY_PATTERNS:
        if pattern.search(text):
            matches.append(cat)
    if len(matches) == 1:
        return matches[0], "HIGH"
    if len(matches) > 1:
        return matches[0], "LOW"  # ambiguous; take first
    return "Unclassified", "NONE"


def g1_account_number_format(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """G1: Account number format consistency."""
    findings = []
    acct_col = resolve_column(column_mappings, "AcctNumber")
    if not acct_col or acct_col not in df.columns:
        return findings

    acct_series = df[acct_col].dropna().astype(str).str.strip()
    acct_series = acct_series[acct_series != ""]
    lengths = acct_series.str.len()

    if len(acct_series) == 0:
        return findings

    # Check length consistency
    length_cv = lengths.std() / max(lengths.mean(), 1)
    distinct_lengths = lengths.nunique()

    if distinct_lengths > 3:
        findings.append({
            "check": "G1",
            "raw_column": acct_col,
            "distinct_length_count": distinct_lengths,
            "length_distribution": lengths.value_counts().head(5).to_dict(),
            "severity": "MEDIUM",
            "message": f"G1: Account numbers have {distinct_lengths} distinct lengths — inconsistent format",
        })
    else:
        findings.append({
            "check": "G1",
            "raw_column": acct_col,
            "distinct_count": acct_series.nunique(),
            "common_length": int(lengths.mode()[0]),
            "severity": "INFO",
            "message": f"G1: Account number format consistent — {acct_series.nunique():,} distinct accounts",
        })

    return findings


def g2_cost_center_format(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """G2: Cost center format and uniqueness."""
    findings = []
    cc_num_col = resolve_column(column_mappings, "CostCenterNumberOrig")
    cc_name_col = resolve_column(column_mappings, "CostCenterNameOrig")

    if not cc_num_col or cc_num_col not in df.columns:
        return findings

    if cc_name_col and cc_name_col in df.columns:
        cc_df = df[[cc_num_col, cc_name_col]].dropna(subset=[cc_num_col])
        cc_df = cc_df[cc_df[cc_num_col].astype(str).str.strip() != ""]

        # Check num → name is 1:1
        num_to_name_count = cc_df.groupby(cc_num_col)[cc_name_col].nunique()
        many_names = num_to_name_count[num_to_name_count > 1]

        # Check name → num is 1:1
        name_to_num_count = cc_df.groupby(cc_name_col)[cc_num_col].nunique()
        many_nums = name_to_num_count[name_to_num_count > 1]

        if len(many_names) > 0 or len(many_nums) > 0:
            findings.append({
                "check": "G2",
                "num_to_multiple_names": len(many_names),
                "name_to_multiple_nums": len(many_nums),
                "severity": "HIGH",
                "message": (
                    f"G2: Cost center mapping not 1:1 — "
                    f"{len(many_names)} number(s) with multiple names, "
                    f"{len(many_nums)} name(s) with multiple numbers"
                ),
            })
        else:
            distinct_cc = cc_df[cc_num_col].nunique()
            findings.append({
                "check": "G2",
                "distinct_cost_centers": distinct_cc,
                "severity": "INFO",
                "message": f"G2: {distinct_cc:,} distinct cost centers with 1:1 number-to-name mapping",
            })

    return findings


def g3_account_classification(
    df: pd.DataFrame, column_mappings: list[dict], acct_type_raw: str | None
) -> tuple[list[dict], dict]:
    """G3: PIVOT P&L account category classification. Returns (findings, classification_result)."""
    findings = []
    acct_col = resolve_column(column_mappings, "AcctNumber")
    desc_col = resolve_column(column_mappings, "AcctDesc")
    amt_col = resolve_column(column_mappings, "AmountOrig")

    if not desc_col or desc_col not in df.columns:
        return findings, {}

    account_classifications = {}
    category_amounts: dict[str, float] = {}
    category_counts: dict[str, int] = {}

    # Per-account classification
    acct_groups = df.groupby(acct_col)[desc_col].first() if (acct_col and acct_col in df.columns) else None
    acct_amounts = (
        df.groupby(acct_col)[amt_col].sum().apply(lambda x: pd.to_numeric(x, errors="coerce"))
        if (acct_col and acct_col in df.columns and amt_col and amt_col in df.columns)
        else None
    )

    if acct_groups is not None:
        for acct_num, acct_desc in acct_groups.items():
            acct_str = str(acct_num)
            desc_str = str(acct_desc or "")
            acct_type_str = ""
            if acct_type_raw and acct_type_raw in df.columns:
                type_vals = df[df[acct_col].astype(str) == acct_str][acct_type_raw]
                acct_type_str = str(type_vals.dropna().iloc[0]) if len(type_vals.dropna()) > 0 else ""

            cat, conf = _classify_account(desc_str, acct_type_str)
            amount = float(acct_amounts[acct_num]) if acct_amounts is not None and acct_num in acct_amounts else 0.0

            account_classifications[acct_str] = {
                "acct_desc": desc_str,
                "category": cat,
                "confidence": conf,
            }
            category_amounts[cat] = category_amounts.get(cat, 0.0) + amount
            category_counts[cat] = category_counts.get(cat, 0) + 1
    else:
        # No account number — classify each unique description
        for desc_val, cnt in df[desc_col].value_counts().items():
            desc_str = str(desc_val)
            cat, conf = _classify_account(desc_str)
            account_classifications[desc_str] = {
                "acct_desc": desc_str,
                "category": cat,
                "confidence": conf,
            }
            if amt_col and amt_col in df.columns:
                amount = float(pd.to_numeric(
                    df[df[desc_col] == desc_val][amt_col], errors="coerce"
                ).sum())
            else:
                amount = 0.0
            category_amounts[cat] = category_amounts.get(cat, 0.0) + amount
            category_counts[cat] = category_counts.get(cat, 0) + int(cnt)

    total_accounts = len(account_classifications)
    unclass_count = category_counts.get("Unclassified", 0)
    unclass_pct = unclass_count / max(total_accounts, 1) * 100

    if unclass_pct > 30:
        sev = "HIGH"
    elif unclass_pct > 10:
        sev = "MEDIUM"
    else:
        sev = "INFO"

    findings.append({
        "check": "G3",
        "total_accounts": total_accounts,
        "unclassified_count": unclass_count,
        "unclassified_pct": round(unclass_pct, 2),
        "category_summary": {
            k: {"acct_count": category_counts.get(k, 0), "total_amount": round(category_amounts.get(k, 0), 2)}
            for k in [c for c, _ in _CATEGORY_PATTERNS] + ["Unclassified"]
        },
        "severity": sev,
        "message": f"G3: {unclass_count:,} of {total_accounts:,} accounts ({unclass_pct:.1f}%) unclassified",
    })

    result = {
        "account_classifications": account_classifications,
        "category_summary": {
            k: {"acct_count": category_counts.get(k, 0), "total_amount": round(category_amounts.get(k, 0), 2)}
            for k in [c for c, _ in _CATEGORY_PATTERNS] + ["Unclassified"]
        },
    }

    return findings, result


def g4_pl_category_presence(classification_result: dict) -> list[dict]:
    """G4: P&L category presence check."""
    findings = []
    cat_summary = classification_result.get("category_summary", {})

    for cat in _REQUIRED_CATEGORIES:
        info = cat_summary.get(cat, {})
        acct_count = info.get("acct_count", 0)
        total_amount = info.get("total_amount", 0)

        if acct_count == 0 or total_amount == 0:
            if cat == "Charges":
                sev = "CRITICAL"
            elif cat == "Provider Compensation":
                sev = "CRITICAL"
            elif cat in ("Adjustments", "Support Staff Compensation"):
                sev = "HIGH"
            else:
                sev = "MEDIUM"

            findings.append({
                "check": "G4",
                "missing_category": cat,
                "severity": sev,
                "message": f"G4: Required P&L category '{cat}' is missing from GL extract",
            })

    # Check Adjustments sign
    adj_info = cat_summary.get("Adjustments", {})
    if adj_info.get("acct_count", 0) > 0 and adj_info.get("total_amount", 0) > 0:
        findings.append({
            "check": "G4",
            "severity": "MEDIUM",
            "message": "G4: Adjustments category has positive total — contractual adjustments should be negative (sign issue)",
        })

    for cat in _RECOMMENDED_CATEGORIES:
        info = cat_summary.get(cat, {})
        if info.get("acct_count", 0) == 0:
            findings.append({
                "check": "G4",
                "absent_recommended_category": cat,
                "severity": "INFO",
                "message": f"G4: Recommended P&L category '{cat}' not present (may not apply to this practice)",
            })

    return findings


def g5_amount_reasonableness(df: pd.DataFrame, column_mappings: list[dict]) -> list[dict]:
    """G5: GL amount reasonableness."""
    findings = []
    amt_col = resolve_column(column_mappings, "AmountOrig")
    if not amt_col or amt_col not in df.columns:
        return findings

    amounts = pd.to_numeric(df[amt_col], errors="coerce")
    non_null = amounts.dropna()

    if len(non_null) == 0:
        return findings

    zero_count = int((non_null == 0).sum())
    if zero_count == len(non_null):
        findings.append({
            "check": "G5",
            "severity": "HIGH",
            "message": "G5: All GL amounts are zero — extract may be empty or incorrectly formatted",
        })
        return findings

    extreme = int((non_null.abs() > 100_000_000).sum())
    if extreme > 0:
        findings.append({
            "check": "G5",
            "extreme_count": extreme,
            "severity": "MEDIUM",
            "message": f"G5: {extreme:,} GL rows with |amount| > $100M (likely data error or wrong units)",
        })

    all_same = (non_null.nunique() == 1)
    if all_same:
        findings.append({
            "check": "G5",
            "severity": "HIGH",
            "message": "G5: All GL amounts are identical — likely placeholder data",
        })

    findings.append({
        "check": "G5",
        "total_amount": round(float(non_null.sum()), 2),
        "min_amount": round(float(non_null.min()), 2),
        "max_amount": round(float(non_null.max()), 2),
        "severity": "INFO",
        "message": f"G5: GL amount range: ${non_null.min():,.0f} to ${non_null.max():,.0f}, total: ${non_null.sum():,.0f}",
    })

    return findings


def g6_yearmonth_validation(
    df: pd.DataFrame, column_mappings: list[dict], test_month: str
) -> list[dict]:
    """G6: Report Date / YearMonth validation."""
    findings = []
    ym_col = resolve_column(column_mappings, "YearMonth")
    if not ym_col or ym_col not in df.columns:
        return findings

    ym_series = df[ym_col].dropna().astype(str).str.strip()
    ym_numeric = pd.to_numeric(ym_series, errors="coerce")

    # If it's a date column, extract YYYYMM and note the format mismatch
    is_date_format = ym_numeric.isna().mean() > 0.5
    if is_date_format:
        parsed = pd.to_datetime(ym_series, errors="coerce")
        ym_numeric = parsed.dt.year * 100 + parsed.dt.month
        sample_val = ym_series.iloc[0] if len(ym_series) > 0 else ""
        findings.append({
            "check": "G6",
            "severity": "MEDIUM",
            "message": (
                f"G6: GL Report Period is not in the requested YYYYMM format "
                f"(e.g. '{sample_val}') — converted to YYYYMM for analysis"
            ),
        })

    valid_ym = ym_numeric.dropna()
    if len(valid_ym) == 0:
        return findings

    invalid_ym = ((valid_ym < 200001) | (valid_ym > 203012))
    invalid_count = int(invalid_ym.sum())
    if invalid_count > 0:
        findings.append({
            "check": "G6",
            "invalid_ym_count": invalid_count,
            "severity": "MEDIUM",
            "message": f"G6: {invalid_count:,} invalid YearMonth values (outside 200001–203012)",
        })

    distinct_months = sorted(valid_ym[~invalid_ym].unique().astype(int))
    month_counts = valid_ym[~invalid_ym].astype(int).value_counts().sort_index().to_dict()

    if test_month:
        expected_ym = int(test_month.replace("-", ""))
        if expected_ym not in distinct_months:
            findings.append({
                "check": "G6",
                "expected_yearmonth": expected_ym,
                "available_months": distinct_months,
                "severity": "HIGH",
                "message": (
                    f"G6: Test month {test_month} ({expected_ym}) not found in GL YearMonth values "
                    f"— GL covers different period"
                ),
            })

    findings.append({
        "check": "G6",
        "months_present": distinct_months,
        "month_row_counts": {str(k): v for k, v in month_counts.items()},
        "severity": "INFO",
        "message": f"G6: GL covers {len(distinct_months)} month(s): {distinct_months}",
    })

    return findings


def g7_cost_center_pl(
    df: pd.DataFrame, column_mappings: list[dict], classification_result: dict
) -> list[dict]:
    """G7: Cost center P&L completeness."""
    findings = []
    cc_num_col = resolve_column(column_mappings, "CostCenterNumberOrig")
    cc_name_col = resolve_column(column_mappings, "CostCenterNameOrig")
    acct_col = resolve_column(column_mappings, "AcctNumber")
    amt_col = resolve_column(column_mappings, "AmountOrig")

    if not cc_num_col or cc_num_col not in df.columns:
        return findings
    if not amt_col or amt_col not in df.columns:
        return findings

    acct_class = classification_result.get("account_classifications", {})
    if not acct_class:
        return findings

    # Build acct → category map
    def get_cat(acct_val: str) -> str:
        return acct_class.get(str(acct_val), {}).get("category", "Unclassified")

    work_df = df[[cc_num_col]].copy()
    if cc_name_col and cc_name_col in df.columns:
        work_df[cc_name_col] = df[cc_name_col]
    if acct_col and acct_col in df.columns:
        work_df["_category"] = df[acct_col].astype(str).apply(get_cat)
    else:
        work_df["_category"] = "Unclassified"
    work_df["_amount"] = pd.to_numeric(df[amt_col], errors="coerce").fillna(0)

    cc_pl_list = []
    all_cats = [c for c, _ in _CATEGORY_PATTERNS]

    for cc_num, cc_df in work_df.groupby(cc_num_col):
        cc_name = ""
        if cc_name_col and cc_name_col in cc_df.columns:
            names = cc_df[cc_name_col].dropna().astype(str)
            cc_name = names.iloc[0] if len(names) > 0 else ""

        cat_amounts: dict[str, float] = {}
        for cat, grp in cc_df.groupby("_category"):
            cat_amounts[cat] = float(grp["_amount"].sum())

        charges = cat_amounts.get("Charges", 0)
        adj = cat_amounts.get("Adjustments", 0)
        other_rev = cat_amounts.get("Other Revenue", 0)
        net_revenue = charges + adj + other_rev

        prov_comp = cat_amounts.get("Provider Compensation", 0)
        staff_comp = cat_amounts.get("Support Staff Compensation", 0)
        facilities = cat_amounts.get("Facilities / Occupancy", 0)
        supplies = cat_amounts.get("Medical Supplies", 0)
        other_ops = cat_amounts.get("Other Operating Expenses", 0)
        total_expenses = prov_comp + staff_comp + facilities + supplies + other_ops
        net_income = net_revenue + total_expenses

        required_present = [
            c for c in _REQUIRED_CATEGORIES
            if cat_amounts.get(c, 0) != 0
        ]
        required_missing = [c for c in _REQUIRED_CATEGORIES if c not in required_present]

        n_req = len(required_present)
        if n_req == 5:
            tier = "Complete"
        elif n_req >= 3:
            tier = "Mostly Complete"
        elif n_req >= 1:
            tier = "Incomplete"
        else:
            tier = "Empty"

        cc_pl_list.append({
            "cost_center_number": str(cc_num),
            "cost_center_name": cc_name,
            "categories_present": list(cat_amounts.keys()),
            "categories_missing": required_missing,
            "required_present": n_req,
            "required_missing": required_missing,
            "completeness_tier": tier,
            "amounts": {
                "Charges": round(charges, 2),
                "Adjustments": round(adj, 2),
                "Other Revenue": round(other_rev, 2),
                "Net Revenue": round(net_revenue, 2),
                "Provider Compensation": round(prov_comp, 2),
                "Support Staff Compensation": round(staff_comp, 2),
                "Facilities / Occupancy": round(facilities, 2),
                "Medical Supplies": round(supplies, 2),
                "Other Operating Expenses": round(other_ops, 2),
                "Total Expenses": round(total_expenses, 2),
                "Net Income": round(net_income, 2),
            },
        })

    # Tier summary
    tier_counts = {}
    for cc in cc_pl_list:
        t = cc["completeness_tier"]
        tier_counts[t] = tier_counts.get(t, 0) + 1

    incomplete_count = tier_counts.get("Incomplete", 0) + tier_counts.get("Empty", 0)
    mostly_count = tier_counts.get("Mostly Complete", 0)

    if incomplete_count > 0:
        findings.append({
            "check": "G7",
            "incomplete_cost_center_count": incomplete_count,
            "severity": "HIGH",
            "message": f"G7: {incomplete_count:,} cost centers are Incomplete or Empty (missing 3+ required P&L categories)",
        })

    if mostly_count > 0:
        findings.append({
            "check": "G7",
            "mostly_complete_count": mostly_count,
            "severity": "MEDIUM",
            "message": f"G7: {mostly_count:,} cost centers are Mostly Complete (missing 1-2 required P&L categories)",
        })

    # Specific gap patterns
    for cc in cc_pl_list:
        amts = cc["amounts"]
        cc_label = f"CC {cc['cost_center_number']} ({cc['cost_center_name']})"
        if amts["Charges"] != 0 and amts["Adjustments"] == 0:
            findings.append({
                "check": "G7",
                "cost_center": cc["cost_center_number"],
                "severity": "MEDIUM",
                "message": f"G7: {cc_label} has Charges but no Adjustments — net revenue will be overstated",
            })
        if amts["Provider Compensation"] != 0 and amts["Charges"] == 0:
            findings.append({
                "check": "G7",
                "cost_center": cc["cost_center_number"],
                "severity": "MEDIUM",
                "message": f"G7: {cc_label} has Provider Comp but no Charges — production may be booked elsewhere",
            })

    findings.append({
        "check": "G7",
        "cost_center_count": len(cc_pl_list),
        "tier_summary": tier_counts,
        "cost_center_pl": cc_pl_list,
        "severity": "INFO",
        "message": f"G7: {len(cc_pl_list):,} cost centers — {tier_counts}",
    })

    return findings


def run_checks(
    df: pd.DataFrame,
    column_mappings: list[dict],
    test_month: str,
    acct_type_raw: str | None = None,
) -> tuple[list[dict], dict]:
    """Run all GL-specific checks. Returns (findings, cross_source_prep)."""
    findings = []

    findings.extend(g1_account_number_format(df, column_mappings))
    findings.extend(g2_cost_center_format(df, column_mappings))

    g3_findings, classification_result = g3_account_classification(df, column_mappings, acct_type_raw)
    findings.extend(g3_findings)

    findings.extend(g4_pl_category_presence(classification_result))
    findings.extend(g5_amount_reasonableness(df, column_mappings))
    findings.extend(g6_yearmonth_validation(df, column_mappings, test_month))
    findings.extend(g7_cost_center_pl(df, column_mappings, classification_result))

    return findings, {}
