# Phase 3 Implementation Plan — Data Quality Review

## Overview

Phase 3 performs row-level and value-level data quality analysis on every ingested file. Where Phase 2 asked *"Are the right columns present and structurally compatible?"*, Phase 3 asks *"Is the actual data clean, complete, logically consistent, and ready for production use?"*

Phase 3 runs two layers of checks: **universal checks** that apply to every data source (nulls, duplicates, date ranges, encoding, placeholder data) and **source-specific checks** that enforce domain rules unique to each PIVOT data source (billing transaction logic, scheduling appointment integrity, payroll reasonableness, GL account structure, quality measure math, and patient satisfaction scoring). Billing checks additionally cross-reference client data against CMS knowledge sources (`stdCmsCpt` and `stdCmsPos`) to validate CPT codes and Place of Service codes against authoritative federal reference tables.

**Inputs:** `phase1_findings.json` + `phase2_findings.json` + parsed DataFrames (re-loaded via `shared/loader.py`)
**Outputs:** `phase3_findings.json`; Excel report sheets; console summary
**Reference data:** `StagingTableStructure.xlsx`, `RawToStagingColumnMapping.xlsx`, `shared/constants.py` (field classifications from Phase 2), `stdCmsCpt` (CMS Physician Fee Schedule CPT/HCPCS lookup), `stdCmsPos` (CMS Place of Service code set)
**Libraries:** `pandas`, `openpyxl`, `re`, `numpy`

---

## Project Structure (additions to Phase 1–2 tree)

```
TestFileReviewAutomation/
├── phase3/
│   ├── __init__.py
│   ├── universal.py                # Null/blank, duplicate, date range, encoding, placeholder checks
│   ├── billing.py                  # Combined + Separate Charges + Separate Transactions checks
│   ├── scheduling.py               # Appointment-specific quality checks
│   ├── payroll.py                  # Payroll-specific quality checks
│   ├── gl.py                       # General Ledger-specific quality checks
│   ├── quality.py                  # Quality measure-specific checks
│   ├── patient_satisfaction.py     # Patient satisfaction-specific checks
│   └── report.py                   # Console display + Excel/JSON writer for Phase 3
├── KnowledgeSources/
│   ├── stdCmsCpt.csv               # CMS CPT/HCPCS fee schedule reference (code, desc, wRVU, status)
│   └── stdCmsPos.csv               # CMS Place of Service reference (code, name, desc, effective date)
├── run_phase3.py                   # CLI orchestrator
└── ...
```

---

## Shared Infrastructure (from Phase 1–2)

Phase 3 reuses:
- **`shared/loader.py`** — re-loads DataFrames using Phase 1 metadata (delimiter, encoding, file path)
- **`shared/constants.py`** — `FIELD_REQUIREMENTS` for knowing which columns are Required (used to escalate null severity), `DOMAIN_FIELD_PATTERNS` for pattern references
- **`shared/staging_meta.py`** — staging column type info; also loads and caches `stdCmsCpt` / `stdCmsPos` knowledge source DataFrames at startup (alongside existing TransactionTypes loading)
- **`shared/staging_meta.py`** — staging column type info for interpreting numeric ranges
- **Phase 1 column mappings** — `phase1_findings.json` provides the raw→staging column map so Phase 3 can address columns by their staging name regardless of client naming
- **Phase 2 field classifications** — `phase2_findings.json` provides `RequirementLevel` per column, used to scale severity (e.g., nulls in a Required field are CRITICAL; nulls in an Optional field are INFO)

### Knowledge Sources

Phase 3 introduces two CMS knowledge sources used by billing-specific checks B13 and B14. These are loaded once at startup and cached as indexed DataFrames in `shared/staging_meta.py` (alongside `StagingTableStructure.xlsx` and `TransactionTypes.xlsx`):

**`stdCmsCpt`** — CMS Physician Fee Schedule CPT/HCPCS reference table.
- Source: CMS Physician Fee Schedule national release files, updated annually
- Key columns: `CptCode` (5-char), `ShortDesc`, `LongDesc`, `WorkRvu` (decimal), `TotalRvu` (decimal), `StatusIndicator` (A=Active, D=Deleted, B=Bundled, etc.), `EffectiveDate`, `TermDate`
- Used by: B13 (CPT code existence, status, wRVU comparison)
- Location: `KnowledgeSources/stdCmsCpt.csv`

**`stdCmsPos`** — CMS Place of Service code set reference table.
- Source: CMS Place of Service Code Set, updated periodically
- Key columns: `PosCode` (2-digit string), `PosName`, `PosDescription`, `EffectiveDate`, `TermDate`
- Used by: B14 (POS code existence, active status, description consistency, distribution analysis)
- Location: `KnowledgeSources/stdCmsPos.csv`

Both tables should be refreshed annually (or when CMS publishes updates). The file path is configurable via `--knowledge-dir` CLI argument (defaults to `./KnowledgeSources/`).

### Column Resolution Helper

Phase 3 modules frequently need to find the raw column name in a DataFrame given a staging column name. A shared utility function is used throughout:

```python
def resolve_column(column_mappings: list[dict], staging_col: str) -> str | None:
    """
    Given Phase 1's column_mappings list and a staging column name,
    return the raw column name that mapped to it, or None if unmapped.
    """
```

This is critical because Phase 3 checks reference staging column names (e.g., `RenderingProviderNpi`, `PostDate`) but the DataFrame uses the client's raw column names (e.g., `Provider NPI`, `Post Date`). All Phase 3 checks use `resolve_column()` and gracefully skip checks when the target column is unmapped.

---

## Module Specs

### `phase3/universal.py`

**Purpose:** Apply data quality checks that are meaningful for every data source.

All checks in this module operate on the full DataFrame and the column mappings from Phase 1. Each check returns a list of finding dicts. The module orchestrates all universal checks and returns the combined list.

---

#### Check 1: Null / Blank Values in Required Fields

**What:** For every column classified as `Required` by Phase 2, count null, empty string, and whitespace-only values.

**Logic:**
1. Iterate Phase 1 column mappings; filter to columns where `RequirementLevel == "Required"` (from Phase 2)
2. For each required column, compute:
   - `null_count` = `series.isna().sum()`
   - `blank_count` = `(series.astype(str).str.strip() == '').sum()` (for non-null but empty/whitespace)
   - `total_missing` = `null_count + blank_count`
   - `missing_pct` = `total_missing / len(df) * 100`
3. Collect first 20 row indices where the value is missing
4. **For `billing_combined` only:** charge-conditional columns (`CptCode`, `Units`, `WorkRvuOriginal`, `PrimaryIcdCode`, `SecondaryIcdCodes`) are only checked against charge rows (rows where `TransactionType`/`TransactionTypeDesc` identifies as a Charge). Apply the same charge mask logic used by Phase 2. Skip null check entirely for these columns if no charge rows can be identified.

**Severity:**
- `CRITICAL` — Required field is > 50% missing (data is fundamentally incomplete)
- `HIGH` — Required field is > 0% and ≤ 50% missing
- `MEDIUM` — Recommended field is > 10% missing
- `INFO` — Optional field is > 25% missing (informational only)
- Skip fields that are 0% missing

**Output per finding:**
```python
{
    "check": "null_blank",
    "raw_column": "Rendering Provider NPI",
    "staging_column": "RenderingProviderNpi",
    "requirement_level": "Required",
    "null_count": 45,
    "blank_count": 12,
    "total_missing": 57,
    "missing_pct": 0.4,
    "sample_rows": [3, 17, 44, ...],
    "severity": "HIGH",
    "message": "Required field 'Rendering Provider NPI' has 57 missing values (0.4%)"
}
```

---

#### Check 2: Duplicate Records

**What:** Detect duplicate rows based on logical primary keys per source.

**Logical primary keys by source:**

| Source | Primary Key Columns (staging names) |
|---|---|
| `billing_combined` | `ChargeId` + `PostDate` + `TransactionType` (a charge can have multiple transaction types) |
| `billing_charges` | `ChargeId` (each charge should appear once) |
| `billing_transactions` | `ChargeId` + `PostDate` (or `InvoiceNumber` if `ChargeId` unmapped) |
| `scheduling` | `ApptId` (each appointment should appear once) |
| `payroll` | `EmployeeId` + `PayPeriodEndDate` + `EarningsCode` (one row per employee per pay period per earnings code) |
| `gl` | `CostCenterNumberOrig` + `AcctNumber` + `YearMonth` |
| `quality` | raw Provider NPI column + raw Measurement Period Start Date + raw Measurement Period End Date + raw Measure Number (quality has no staging table — use raw column names from Phase 1 mapping) |
| `patient_satisfaction` | raw Provider NPI column + raw Survey Date Range Start + raw Question Order (no staging table — use raw column names from Phase 1 mapping) |

**Logic:**
1. Resolve each key column from staging name to raw column name using `resolve_column()`
2. If any key column is unmapped, skip the duplicate check for that source and log a note: `"Cannot check duplicates — key column '{col}' not mapped"`
3. Drop rows where all key columns are null (these are flagged by the null check, not the duplicate check)
4. Identify duplicate groups using `df.duplicated(subset=key_cols, keep=False)`
5. Count total duplicate rows and number of duplicate groups

**Severity:**
- `HIGH` — any exact duplicates found on the primary key
- Report includes: duplicate count, duplicate group count, first 10 duplicate group samples (showing key column values and row indices)

**Output per finding:**
```python
{
    "check": "duplicate_records",
    "key_columns": ["Charge ID", "Post Date", "Transaction Type"],
    "duplicate_row_count": 284,
    "duplicate_group_count": 142,
    "sample_groups": [
        {"key_values": {"Charge ID": "CHG001", "Post Date": "2026-01-15", "Transaction Type": "Payment"}, "row_indices": [100, 5023]},
        ...
    ],
    "severity": "HIGH",
    "message": "284 duplicate rows found across 142 groups based on key [Charge ID, Post Date, Transaction Type]"
}
```

**Full-row duplicate check (supplementary):** In addition to the primary key duplicate check, perform a full-row exact duplicate check (`df.duplicated(keep=False)`) to catch rows that are completely identical across all columns. These are almost always data extraction errors.

---

#### Check 3: Date Range / Test Month Alignment

**What:** Verify that date values in the filter date column fall within the expected test month identified by Phase 1. Also check for out-of-range dates in all date columns.

**Logic:**
1. Retrieve the `test_month` from `phase1_findings.json` (e.g. `"2026-01"`)
2. For the filter date column (from Phase 1's `test_month.py` — `PostDate` for billing, `ApptDate` for scheduling, etc.):
   - Count rows where the date falls outside the expected month window
   - For billing, the window is the 15th of prior month through the 14th of current month (per the submission cadence spec)
   - For scheduling/payroll/GL, the window is the full calendar month
3. For all other date columns in the file:
   - Flag dates before `1900-01-01` or after `2099-12-31` as obviously invalid
   - Flag dates more than 10 years in the future as suspicious
   - Flag dates that parse to the Unix epoch (`1970-01-01`) as likely parse failures

**Billing-specific date window:**
The spec states billing data is submitted "for the 15th of the previous month through the 14th of the current month." For a test month of `2026-01`:
- Expected Post Date range: `2025-12-15` through `2026-01-14`
- Flag rows outside this window but don't fail them as CRITICAL (some clients submit full calendar months)

**Severity:**
- `HIGH` — > 5% of rows in the filter date column fall outside the expected test month
- `MEDIUM` — > 0% but ≤ 5% of rows outside the expected month (minor date bleed is common)
- `MEDIUM` — any obviously invalid dates (pre-1900, post-2099) in any date column
- `INFO` — minor date anomalies (e.g., a few service dates from the prior month in billing)

---

#### Check 4: Numeric Value Range

**What:** Check all numeric columns (int, decimal, numeric) for outliers, negative values where unexpected, and obviously erroneous magnitudes.

**Logic:**
1. For each mapped column with a numeric staging type:
   - Compute: min, max, mean, median, std, count of zeros, count of negatives
   - Flag negative values in columns where negatives are unexpected (source-specific rules define which columns allow negatives — see source-specific modules)
   - Flag zero values in columns where zero is suspicious (e.g., `ChargeAmountOriginal` for billing charges — a $0 charge is unusual but valid for some CPT codes)
2. Apply IQR-based outlier detection: values beyond Q1 - 3×IQR or Q3 + 3×IQR are flagged as extreme outliers
3. This is a generic sweep; source-specific modules apply tighter domain rules

**Severity:**
- `MEDIUM` — extreme outliers detected (> 3×IQR)
- `INFO` — statistical summary provided for review (not an error, just informational)

---

#### Check 5: Test / Placeholder Data

**What:** Detect obvious test or placeholder values that should not appear in production data.

**Patterns to detect (case-insensitive):**
- Exact matches: `test`, `testing`, `xxx`, `zzz`, `TBD`, `N/A`, `NA`, `NULL`, `NONE`, `DUMMY`, `SAMPLE`, `FAKE`, `PLACEHOLDER`, `DEFAULT`, `TODO`, `temp`, `unknown`
- Repeating characters: `0000000000`, `1111111111`, `9999999999`, `AAAA`, `1234567890`
- Common test names: `John Doe`, `Jane Doe`, `Test Patient`, `Mickey Mouse`, `Donald Duck`
- Common test NPIs: `1234567890`, `0000000000`, `9999999999`
- Common test MRNs: `000000`, `999999`, `TEST001`, `PATIENT1`

**Logic:**
1. For each column, check values against the placeholder pattern list
2. Exclude columns where these values might be legitimate (e.g., `TranTypeDesc` might legitimately contain "Adjustment" which partially matches — use exact match only for short values, not substring)
3. Only flag values in columns mapped to staging columns (ignore unrecognized columns)

**Severity:**
- `HIGH` — placeholder values found in Required fields (e.g., NPI = `1234567890`)
- `MEDIUM` — placeholder values found in Recommended fields
- `LOW` — placeholder values found in Optional fields

---

#### Check 6: Encoding / Character Issues

**What:** Detect garbled or non-standard characters that indicate encoding problems.

**Logic:**
1. For each text/varchar column, scan for:
   - Mojibake patterns: `Ã©`, `Ã¡`, `â€™`, `Ã¶`, `Â`, `Ã` (UTF-8 bytes misread as Latin-1)
   - Replacement characters: `\ufffd` (Unicode replacement character)
   - Control characters: bytes 0x00–0x08, 0x0B, 0x0C, 0x0E–0x1F (excluding tab, newline, carriage return)
   - Non-printable characters that would cause display issues
2. Count affected rows per column

**Severity:**
- `MEDIUM` — encoding issues found in any column (> 0 affected rows)
- Note: Phase 1 already detected encoding at the file level; Phase 3 identifies specific columns and rows

---

### `phase3/billing.py`

**Purpose:** Source-specific data quality checks for billing files (`billing_combined`, `billing_charges`, `billing_transactions`).

All checks use `resolve_column()` to find raw column names and gracefully skip if the target column is unmapped.

---

#### B1: Transaction Type Validation (Combined Billing)

**What:** Verify that Transaction Type values are recognizable and that the file contains both charges and transactions.

**Logic:**
1. Resolve the `TransactionType` staging column to its raw column
2. Extract distinct values
3. Check if values are classifiable into standard categories:
   - **Charge** types: `Charge`, `CHG`, `C`, or similar
   - **Payment** types: `Payment`, `PMT`, `PAY`, `P`, or similar
   - **Adjustment** types: `Adjustment`, `ADJ`, `A`, or similar
   - **Void/Reversal** types: `Void`, `Reversal`, `VD`, `REV`, or similar
   - **Refund** types: `Refund`, `REF`, `R`, or similar
4. Flag any Transaction Type values that don't fit the above categories
5. For Combined Billing: verify that at least one Charge type AND at least one non-Charge type (Payment/Adjustment/Refund) exist — if only charges are present, this is a Separate Charges file mislabeled as Combined

**Severity:**
- `CRITICAL` — Combined billing file contains only charge rows (no transactions) or only transaction rows (no charges). This indicates a format mismatch; should be re-classified.
- `HIGH` — unrecognizable Transaction Type values present (cannot be auto-mapped to standard categories)
- `MEDIUM` — all Transaction Types are recognizable but distribution is unusual (e.g., 99% charges, <1% payments)

**Output includes:** Distinct Transaction Type values with row counts and proposed category mapping.

---

#### B2: Charge-Transaction Linkage (Separate Billing)

**What:** For Separate Billing, verify that Charges and Transactions can be linked.

**Logic:**
1. Resolve `ChargeId` and `InvoiceNumber` in both the charges file and the transactions file
2. Extract the set of linking IDs from each file
3. Compute:
   - Charges with no matching transaction ID
   - Transactions with no matching charge ID
   - Match rate (% of charge IDs that appear in transactions)
4. Note: Not all charges will have transactions (unbilled, pending), but transactions should generally reference an existing charge

**Severity:**
- `HIGH` — > 20% of transactions reference charge IDs not found in the charges file (orphaned transactions)
- `MEDIUM` — > 50% of charges have no matching transaction (may be normal for a single month, but worth noting)
- `INFO` — linkage summary statistics

**Note:** This check requires access to both billing files simultaneously. `billing.py` receives a dict of all billing-source DataFrames.

---

#### B3: Work RVU Validation

**What:** Check that wRVU values are populated and reasonable for the CPT codes billed.

**Logic:**
1. Resolve `WorkRvuOriginal` and `CptCode` staging columns
2. For rows with E&M CPT codes (99202–99499) and procedural codes (10000–69999):
   - Flag rows where wRVU is null, blank, or zero (these codes should have wRVUs)
   - Flag rows where wRVU is negative
   - Flag rows where wRVU > 100 (extreme outlier — very few procedures exceed 50 wRVUs)
3. For lab/ancillary codes (80000–89999, 90000–99199): wRVU of 0 is legitimate — do not flag
4. Compute overall wRVU population rate: `(non-null, non-zero wRVU count) / (total E&M + procedural row count)`

**Severity:**
- `HIGH` — > 10% of E&M/procedural rows have zero or null wRVUs (undermines wRVU reconciliation)
- `MEDIUM` — > 0% but ≤ 10% have zero/null wRVUs
- `MEDIUM` — any negative wRVUs found
- `INFO` — wRVU population summary

---

#### B4: Charge Amount Reasonableness

**What:** Check that charge amounts are populated and not obviously erroneous.

**Logic:**
1. Resolve `ChargeAmountOriginal` (staging column for charge amounts in both separate and combined billing)
2. For charge rows:
   - Flag null/blank/zero charge amounts (charges should have a dollar amount)
   - Flag negative charge amounts that are NOT on void/reversal transaction types
   - Flag extremely high charges (> $100,000 per line — rare but possible for some procedures; flag as INFO)
3. For Combined Billing, only apply to rows where Transaction Type = Charge

**Severity:**
- `HIGH` — > 5% of charge rows have null/zero charge amounts
- `MEDIUM` — negative charge amounts on non-void rows
- `INFO` — extreme charge amount outliers

---

#### B5: Rendering Provider NPI Consistency

**What:** Validate that Rendering Provider NPIs are consistently formatted and populated.

**Logic:**
1. Resolve `RenderingProviderNpi` staging column
2. Phase 2 already checked NPI format (10-digit pattern). Phase 3 adds:
   - Count distinct NPIs in the file
   - Flag if a single NPI accounts for > 50% of all rows (unusual concentration; may indicate a default/fallback NPI)
   - Flag if same NPI maps to multiple different `RenderingProviderFullName` values (name mismatch — suggests NPI data quality issue)
   - Cross-check: if `BillingProviderNpi` is also present, count rows where Rendering NPI == Billing NPI (common in solo practices, but unusual if it's 100%)

**Severity:**
- `MEDIUM` — single NPI accounts for > 50% of rows
- `MEDIUM` — NPI-to-name mapping is not 1:1 (same NPI, different names)
- `INFO` — NPI distribution summary

---

#### B6: Cost Center / Org Hierarchy Coverage

**What:** Verify that organizational identifiers are populated and can support GL crosswalk.

**Logic:**
1. Resolve `BillDepartmentId`, `BillDepartmentName`, `BillLocationName`, `BillPracticeName`
2. For each resolved column, compute:
   - Population rate (% non-null, non-blank)
   - Distinct value count
3. If Cost Center (mapped to `BillDepartmentId` or `BillDepartmentName`) is < 90% populated:
   - Check whether `BillPracticeName` or `BillLocationName` could serve as a crosswalk
   - Flag if none of the org fields is > 90% populated

**Severity:**
- `CRITICAL` — `BillDepartmentId`/`BillDepartmentName` and all other org fields are < 50% populated (no crosswalk to GL possible)
- `HIGH` — Cost Center < 90% populated but at least one other org field is well-populated (crosswalk may work)
- `INFO` — Org hierarchy population summary with distinct values

---

#### B7: CPT Modifier Separation

**What:** Check that CPT modifiers are in separate columns and not concatenated with the CPT code.

**Logic:**
1. Resolve `CptCode` and `Modifier1`–`Modifier4`
2. In the CPT code column:
   - Flag values containing a hyphen or space followed by a 2-character modifier (e.g., `99213-25`, `99213 25`)
   - Flag values longer than 5 characters that appear to have modifiers appended
3. In the modifier columns:
   - Check that values are 2 characters or blank
   - Flag values > 2 characters (may contain multiple concatenated modifiers)

**Severity:**
- `MEDIUM` — modifiers embedded in CPT code column (count and sample rows)
- `MEDIUM` — modifier columns contain concatenated multi-modifier values

---

#### B8: ICD-10 Code Separation

**What:** Verify that ICD-10 codes are in individual columns (especially 5th–25th codes) and not concatenated.

**Logic:**
1. Resolve ICD-10 staging columns: `PrimaryIcdCode` and `SecondaryIcdCodes`
2. For each ICD-10 column:
   - Flag values containing commas, semicolons, or pipe characters (multiple codes concatenated)
   - Flag values that appear to be ICD-9 format (3-digit numeric like `250`, `401`)
3. Check if the file has separate columns for 5th through 25th ICD-10 codes or if they're combined

**Severity:**
- `HIGH` — ICD-10 codes appear concatenated (multiple codes in one column)
- `MEDIUM` — ICD-9 format codes detected (suggests legacy data or mapping issue)
- `INFO` — count of ICD-10 columns present (e.g., "15 of 25 ICD-10 columns populated")

---

#### B9: Payer Financial Class Categorization

**What:** Check that Payer Financial Class values can be mapped to standard categories.

**Logic:**
1. Resolve `ChargePayerFinancialClass` (billing_charges) or `ChargePayerFinancialClass`/`TransactionPayerFinancialClass` (billing_combined)
2. Extract distinct values
3. Attempt to classify each into standard buckets:
   - **Commercial/Managed Care**: Blue Cross, Aetna, UHC, Cigna, HMO, PPO, etc.
   - **Medicare**: Medicare, MCR, MA (Medicare Advantage), etc.
   - **Medicaid**: Medicaid, MCD, state program names, etc.
   - **Self-Pay**: Self-Pay, Self, Cash, Uninsured, etc.
   - **Workers' Compensation**: Work Comp, WC, etc.
   - **Other Government**: Tricare, VA, CHAMPVA, etc.
   - **Other**: Charity, Contractual, etc.
4. Flag values that cannot be classified

**Severity:**
- `MEDIUM` — > 10% of distinct Financial Class values are unclassifiable
- `INFO` — Financial Class distribution summary

---

#### B10: Void Charge Validation

**What:** Verify that void/reversal charges have negative units.

**Logic:**
1. Resolve `TransactionType` (or `TransactionTypeDesc`) and `Units`
2. For rows where Transaction Type indicates a void/reversal:
   - Check if Units is negative (expected for voids)
   - Flag rows where Units is positive on a void (data inconsistency)
3. For non-void rows:
   - Flag rows where Units is negative but Transaction Type is not a void

**Severity:**
- `MEDIUM` — void rows with positive units (count and sample)
- `MEDIUM` — non-void rows with negative units

---

#### B11: Post Date Window Validation

**What:** Verify Post Dates fall within the expected test month submission window.

**Logic:** (Delegated detail from universal Check 3, with billing-specific window logic)
1. Resolve `PostDate`
2. For the identified test month, compute the expected window: 15th of prior month through 14th of test month
3. Count rows outside this window
4. Also flag: Post Date < Date of Service (charge posted before it occurred — unusual but possible for pre-billing)

**Severity:**
- `HIGH` — > 10% of rows have Post Dates outside the expected window
- `MEDIUM` — > 0% but ≤ 10% outside window
- `INFO` — Post Date range summary (min, max, median)

---

#### B12: Patient Identifier Format Consistency

**What:** Check that Patient MRN/Identifier has a consistent format (will need to match Scheduling).

**Logic:**
1. Resolve `PatientId` and/or `PatientMrn` (either may be populated depending on the client's mapping)
2. Analyze value patterns:
   - All numeric? All alphanumeric? Mixed?
   - Consistent length? Variable length?
   - Leading zeros present?
3. Flag format inconsistencies within the column (e.g., mix of numeric and alphanumeric)
4. Store the detected format pattern for Phase 4 cross-source matching

**Severity:**
- `MEDIUM` — inconsistent format within the column (may cause matching failures with Scheduling)
- `INFO` — format summary

---

#### B13: CPT Code Validation Against stdCmsCpt

**What:** Cross-reference CPT codes in the billing data against the CMS CPT knowledge source (`stdCmsCpt`) to confirm that billed codes are valid, current, and that associated fields (wRVUs, descriptions) are consistent with CMS-published values.

**Knowledge source:** `stdCmsCpt` — the standard CMS Physician Fee Schedule CPT/HCPCS lookup. Contains: CPT/HCPCS code, short description, long description, work RVU, total RVU, status indicator (Active, Deleted, Bundled), and effective date range.

**Logic:**
1. Resolve `CptCode` and `WorkRvuOriginal` staging columns
2. Load the `stdCmsCpt` reference table (cached at startup in `shared/staging_meta.py`)
3. For each distinct CPT code in the billing data:
   - **Existence check:** Does the code exist in `stdCmsCpt`? Flag codes not found (may be invalid, retired, or client-specific internal codes)
   - **Status check:** Is the code Active in the current fee schedule? Flag codes with Deleted or Bundled status (client may be billing retired codes)
   - **wRVU reasonableness check:** Compare the client's reported wRVU for this CPT code against the CMS-published work RVU from `stdCmsCpt`. Flag significant discrepancies:
     - Client wRVU is 0 but CMS wRVU > 0 (missing wRVU — reinforces B3 findings with CMS authority)
     - Client wRVU differs from CMS wRVU by > 20% (may indicate the client is using a different fee schedule year, custom RVU assignments, or data extraction errors)
   - **Category confirmation:** Use `stdCmsCpt` status indicators and RVU values to confirm the E&M vs. procedural vs. lab/ancillary classification used by B3 (e.g., a code the client treats as procedural but CMS classifies differently)
4. Compute: % of distinct CPT codes that match `stdCmsCpt`, % that match on wRVU within tolerance

**Severity:**
- `HIGH` — > 5% of distinct CPT codes not found in `stdCmsCpt` (suggests systematic coding issues or outdated code set)
- `MEDIUM` — codes found but with Deleted/Bundled status (retired codes being billed)
- `MEDIUM` — > 10% of CPT-wRVU pairs have > 20% variance from CMS published values
- `INFO` — CPT validation summary: match rate, top unmatched codes, wRVU variance distribution

**Output includes:** Table of unmatched CPT codes with row counts, table of wRVU variances (client vs. CMS) for the top 20 highest-volume codes, and list of deleted/bundled codes still being billed.

---

#### B14: Place of Service Validation Against stdCmsPos

**What:** Cross-reference CMS Place of Service codes in the billing data against the CMS POS knowledge source (`stdCmsPos`) to confirm codes are valid and descriptions are consistent.

**Knowledge source:** `stdCmsPos` — the standard CMS Place of Service code set. Contains: POS code (2-digit), POS name, POS description, and effective/termination dates.

**Logic:**
1. Resolve `PlaceOfServiceCode` staging column (and optionally raw POS name/description columns if mapped)
2. Load the `stdCmsPos` reference table (cached at startup in `shared/staging_meta.py`)
3. For each distinct POS code in the billing data:
   - **Existence check:** Does the code exist in `stdCmsPos`? Flag codes not found (invalid POS code)
   - **Active check:** Is the code currently active (not terminated)? Flag terminated POS codes
   - **Description consistency:** If a POS name/description column is present in the client data (raw column), compare against the CMS-published name/description for that code. Flag significant mismatches (e.g., client says code `11` is "Emergency Room" but CMS says `11` is "Office"). Minor wording differences are acceptable; flag only when the semantic meaning differs.
4. **Distribution analysis:** Report the POS code distribution and flag unusual patterns:
   - A medical group billing > 30% of charges to POS 21 (Inpatient Hospital) is unusual for an outpatient physician practice — may indicate the extract includes hospital-based charges that should be excluded
   - POS 11 (Office) should typically be the dominant code for physician practice billing
   - Flag if no POS 11 rows exist at all (may indicate the POS field contains non-standard values)
5. Compute: % of distinct POS codes that match `stdCmsPos`, % of rows with valid POS codes

**Severity:**
- `HIGH` — > 5% of rows have POS codes not found in `stdCmsPos` (invalid codes in data)
- `MEDIUM` — terminated POS codes still in use
- `MEDIUM` — POS description mismatches between client data and CMS reference
- `MEDIUM` — POS distribution suggests non-physician-practice data mixed in (e.g., > 30% inpatient)
- `INFO` — POS validation summary: match rate, code distribution, description comparison

**Output includes:** Table of all distinct POS codes with CMS name, client-provided name (if different), row count, and validity status.

---

### `phase3/scheduling.py`

**Purpose:** Source-specific data quality checks for scheduling files.

---

#### S1: Appointment Status Mapping

**What:** Verify that appointment statuses can be mapped to standard PIVOT categories.

**Standard categories:** Completed, Cancelled, Rescheduled, No Show, Scheduled (future), Checked In, Arrived, Bumped, Left Without Being Seen

**Logic:**
1. Resolve `ApptStatus`
2. Extract distinct values with row counts
3. Attempt fuzzy classification into standard categories:
   - Exact/substring match against a keyword dictionary (e.g., `Completed`, `Complete`, `COMP`, `Arrived`, `Checked Out`, `No Show`, `NS`, `NOSHOW`, `Cancelled`, `Canceled`, `CANC`, `CX`, `Rescheduled`, `RSCH`)
4. Flag unmappable statuses

**Severity:**
- `HIGH` — > 10% of rows have appointment statuses that cannot be classified
- `MEDIUM` — a small number of unclassifiable statuses (< 10% of rows)
- `INFO` — status distribution summary with proposed category mapping

---

#### S2: Cancelled Appointment Completeness

**What:** Cancelled appointments should have Cancel Date and Cancel Reason populated.

**Logic:**
1. Resolve `ApptStatus`, `CancellationDate`, `CancelReason`
2. Identify cancelled rows (status maps to "Cancelled" or "Rescheduled")
3. For cancelled rows:
   - Count rows where Cancel Date is null
   - Count rows where Cancel Reason is null/blank
4. For non-cancelled rows:
   - Flag rows that have a Cancel Date but status is not Cancelled (data inconsistency)

**Severity:**
- `HIGH` — > 20% of cancelled appointments have no Cancel Date
- `MEDIUM` — > 20% of cancelled appointments have no Cancel Reason
- `MEDIUM` — non-cancelled rows with populated Cancel Date

---

#### S3: Appointment Time and Duration Logic

**What:** Check that appointment times and scheduled lengths are logical.

**Logic:**
1. Resolve `ApptTime` and `ApptSchdLength`
2. For completed appointments:
   - Flag zero-length appointments (Duration = 0 or null for completed visits)
   - Flag extremely long appointments (> 480 minutes / 8 hours)
   - Flag extremely short appointments (< 5 minutes for non-phone/non-virtual visit types)
3. For appointment times:
   - Flag times outside typical clinic hours (before 5:00 AM or after 11:00 PM) as INFO (some practices run late/overnight)
   - Flag null appointment times on completed appointments

**Severity:**
- `MEDIUM` — zero-length completed appointments (count and sample)
- `INFO` — duration outliers and off-hours appointments

---

#### S4: Check In / Check Out Validation

**What:** When check-in/check-out times are available, verify logical consistency.

**Logic:**
1. Resolve `CheckInDate`, `CheckInTime`, `CheckOutDate`, `CheckOutTime`
2. For completed appointments with check-in/check-out data:
   - Flag Check Out before Check In
   - Flag Check In before Appointment Date
   - Flag check-in/check-out time gap > 12 hours (extreme outlier)
   - Flag completed appointments with Check In but no Check Out (patient may have left without completing)

**Severity:**
- `MEDIUM` — Check Out before Check In (data integrity issue)
- `INFO` — other time anomalies

---

#### S5: Patient Identifier Match Preparation

**What:** Analyze the Patient Identifier format for cross-source matching with Billing (Phase 4 prep).

**Logic:**
1. Resolve `PatIdOrig`
2. Same format analysis as B12 (pattern detection, length consistency, leading zeros)
3. Compare detected format pattern against what was found in Billing's B12 check (if both Phase 3 results are available)
4. Flag format mismatches preemptively

**Severity:**
- `HIGH` — Patient ID format appears fundamentally different from Billing (e.g., numeric vs. alphanumeric)
- `INFO` — format summary for Phase 4 use

---

#### S6: Location / Provider NPI Validation

**What:** Verify Appt Provider NPIs and Location Names are populated and valid.

**Logic:**
1. Resolve `ApptProvNPI` and `BillLocNameOrig`
2. NPI validation: 10-digit check (similar to B5), distinct count, concentration analysis
3. Location: distinct value count, population rate
4. Flag if any location values appear to be IDs rather than names (numeric values in a Name column — similar to Phase 1's org hierarchy auto-routing logic)

**Severity:**
- `MEDIUM` — NPI format issues
- `INFO` — location/NPI distribution summary

---

#### S7: Appointment Date Range

**What:** Verify appointment dates fall within the expected test month.

**Logic:**
1. Resolve `ApptDate`
2. Expected window: the full calendar month of the test month (scheduling uses calendar month per spec)
3. Count rows outside this window
4. Also flag: Created Date after Appointment Date (appointment created after it occurred — suspicious but possible for retroactive scheduling)

**Severity:**
- `HIGH` — > 5% of rows outside the expected month
- `MEDIUM` — Created Date > Appt Date on > 5% of rows

---

#### S8: Appointment Status Distribution Sanity

**What:** Verify that the status distribution is consistent with a real-world scheduling extract — Completed appointments should be the largest status category, and at least one cancellation/reschedule status must exist.

**Logic:**
1. Resolve `ApptStatus`
2. Using the category mapping from S1, compute row counts and percentages per standard category (Completed, Cancelled, Rescheduled, No Show, Scheduled, etc.)
3. **Completed dominance check:** Completed (or equivalent: Arrived, Checked Out) should be the highest-percentage status. If another status (e.g., Scheduled, Cancelled) has a higher count than Completed, the data may be filtered incorrectly or the status mapping is wrong.
4. **Cancel/Reschedule existence check:** At least one row must map to a Cancelled or Rescheduled category. A scheduling extract with zero cancellations is almost certainly missing data or has been pre-filtered — PIVOT needs cancellation data for no-show and access metrics.
5. **No Show existence check:** Flag if zero rows map to No Show — this is not always a data error (some systems fold No Shows into Cancelled), but it is noteworthy for PIVOT access analysis.

**Severity:**
- `HIGH` — Completed is not the highest-percentage status (suggests data filtering issue or wrong status mapping)
- `HIGH` — zero rows with a Cancelled or Rescheduled status (cancellation data missing)
- `MEDIUM` — zero rows with a No Show status (may limit access analysis; note for client discussion)
- `INFO` — full status distribution table with counts and percentages

---

#### S9: Appointment Type Coverage (New vs. Established/Return)

**What:** Verify that appointment types include both New Patient and Established/Returning Patient visits, which are essential for PIVOT access and demand analysis.

**Logic:**
1. Resolve `ApptType`
2. Extract distinct appointment type values with row counts
3. **New Patient detection:** Search for values matching keywords: `New`, `New Patient`, `NEW PT`, `NP`, `New Visit`, `Initial`, `INIT`. A match indicates the data captures new patient appointments.
4. **Established/Return Patient detection:** Search for values matching keywords: `Established`, `Est`, `EST PT`, `Return`, `Returning`, `Follow Up`, `Follow-Up`, `FU`, `F/U`, `Existing`. A match indicates the data captures returning patient appointments.
5. **Both must exist:** PIVOT uses new vs. established appointment type breakdowns for access and lag calculations. If only one or neither is present:
   - The Appt Type field may use procedure-level descriptions (e.g., `Office Visit`, `Annual Physical`) rather than new/established designations — note this for client discussion
   - The client may need to provide a crosswalk or add a New/Established flag
6. **Distribution check:** Report the percentage split between New and Established types. Typical ranges: 10–30% New, 70–90% Established. Flag distributions far outside this range as unusual.

**Severity:**
- `HIGH` — neither New Patient nor Established/Return Patient appointment types are identifiable (cannot perform new vs. established analysis)
- `MEDIUM` — only one of the two types is identifiable (partial coverage — client discussion needed)
- `MEDIUM` — distribution is outside typical range (< 5% New or > 50% New — may indicate classification issues)
- `INFO` — appointment type distribution summary with new/established mapping

---

### `phase3/payroll.py`

**Purpose:** Source-specific data quality checks for payroll files.

---

#### P1: Hours Reasonableness

**What:** Check that hours values are reasonable for the earnings type and pay period.

**Logic:**
1. Resolve `Hours`, `EarningsCode`, `EarningsCodeDesc`, `EmployeeId`, `PayPeriodEndDate`
2. Per-row checks:
   - Flag negative hours (should not occur for regular/OT earnings; may be valid for adjustments)
   - Flag single-row hours > 200 (no single earnings code should exceed ~180 hours per pay period for a full-time biweekly employee)
3. Per-employee per-pay-period aggregation:
   - Sum hours by employee + pay period end date
   - Flag employees with total hours > 400 in a single pay period (extremely unlikely unless multiple correction entries)
   - Flag employees with total hours = 0 across all earnings codes (no productive hours — may be leave-only)
4. Per-earnings-code analysis:
   - Identify overtime codes (OT, Overtime, O/T) and check if hours are disproportionately high
   - Identify PTO/Leave codes and verify hours are reasonable (≤ 80 per biweekly period)

**Severity:**
- `HIGH` — negative hours on non-adjustment earnings codes
- `MEDIUM` — hours > 200 on a single row
- `INFO` — hours distribution summary

---

#### P2: Amount Reasonableness

**What:** Check that pay amounts are reasonable relative to earnings type.

**Logic:**
1. Resolve `AmountOrig`, `EarningsCode`, `EarningsCodeDesc`
2. Flag:
   - Negative amounts on non-adjustment/non-reversal earnings codes
   - Amounts > $500,000 on a single row (extreme outlier)
   - Amounts = $0 on regular/salary codes (should have a value)
3. Compute: amount-to-hours ratio for hourly earnings codes → flag implied hourly rates > $1,000/hr or < $7/hr

**Severity:**
- `MEDIUM` — extreme amount outliers
- `INFO` — amount distribution summary

---

#### P3: Pay Period Logic

**What:** Verify pay period start/end dates define logical periods.

**Logic:**
1. Resolve `PayPeriodStartDate`, `PayPeriodEndDate`, `CheckDate`
2. Checks:
   - Start Date must be before End Date
   - Pay period length should be 7 (weekly), 14 (biweekly), ~15 (semi-monthly), or ~30 (monthly) days
   - Flag periods outside these ranges (too short or too long)
   - Check Date should be on or after Pay Period End Date (you get paid after the period ends)
   - Flag Check Date before End Date
3. All pay periods in the file should use the same cadence (don't mix weekly and biweekly)

**Severity:**
- `HIGH` — Pay Period Start > End Date (inverted dates)
- `MEDIUM` — unusual pay period lengths
- `MEDIUM` — Check Date before Pay Period End Date
- `INFO` — pay period cadence summary

---

#### P4: Department ID / GL Linkage Preparation

**What:** Analyze Department ID/Name for GL crosswalk capability (Phase 4 prep).

**Logic:**
1. Resolve `DepartmentId`, `DepartmentName`
2. Distinct value count for each
3. Population rate
4. Format analysis: numeric IDs vs. text codes vs. mixed
5. Flag departments with very high row counts (> 30% of file — possible default/catch-all department)

**Severity:**
- `HIGH` — `DepartmentId` is < 50% populated (cannot link most payroll to GL)
- `INFO` — department distribution summary

---

#### P5: Employee NPI for Provider Identification

**What:** Check if Employee NPI is populated for provider employees (physicians, APPs).

**Logic:**
1. Resolve `EmployeeNpi`, `JobCodeDesc`
2. Identify provider-type job codes using keyword matching on Job Code Description:
   - Physician keywords: `Physician`, `MD`, `DO`, `Doctor`, `Surgeon`
   - APP keywords: `APP`, `NP`, `Nurse Practitioner`, `PA`, `Physician Assistant`, `PA-C`, `APRN`, `CRNA`
3. For provider-type employees: check if NPI is populated
4. Flag providers without NPIs (needed for Billing↔Payroll matching in Phase 4)

**Severity:**
- `HIGH` — > 20% of identified providers have no NPI (breaks Phase 4 cross-source matching)
- `MEDIUM` — > 0% but ≤ 20% missing
- `INFO` — provider NPI population summary

---

#### P6: Job Code / Earnings Code Mappability

**What:** Check if Job Codes and Earnings Codes can be categorized for PIVOT analysis.

**Logic:**
1. Resolve `JobCodeDesc`, `EarningsCodeDesc`
2. Job Code Description — attempt classification into MGMA categories:
   - Physician, APP, RN, LPN, MA, Admin/Clerical, Management, Other Clinical, Other Non-Clinical
   - Use keyword matching against common job titles
3. Earnings Code Description — attempt classification:
   - Base/Regular Pay, Overtime, PTO/Vacation, Sick Leave, Holiday, Bonus, Call Pay, Benefits, Retirement, Other
   - Use keyword matching
4. Flag unclassifiable codes

**Severity:**
- `MEDIUM` — > 15% of job codes unclassifiable
- `MEDIUM` — > 15% of earnings code descriptions unclassifiable
- `INFO` — classification summary with proposed mappings

---

#### P7: Support Staff Presence Validation

**What:** Verify that the payroll extract includes support staff (not just providers). PIVOT cost analysis requires a complete picture of practice staffing — physicians and APPs alone are insufficient. The extract should include clinical support staff (RNs, LPNs, MAs) and non-clinical staff (patient access, front desk, admin, billing).

**Logic:**
1. Resolve `JobCodeDesc` (and optionally `JobCode` for the raw code value)
2. Using the classification from P6, identify employees in each staff category
3. **Required staff categories** — at minimum, the payroll extract must contain:
   - **Providers:** At least one employee classified as Physician or APP (if none, the extract may be missing provider compensation entirely)
   - **Clinical support staff:** At least one employee classified as RN, LPN, MA (Medical Assistant), CNA, or similar clinical support role. Keywords: `RN`, `Registered Nurse`, `LPN`, `Licensed Practical Nurse`, `LVN`, `Licensed Vocational Nurse`, `MA`, `Medical Assistant`, `CMA`, `Certified Medical Assistant`, `CNA`, `Certified Nursing Assistant`, `Clinical`, `Nurse`
   - **Non-clinical / patient access staff:** At least one employee classified as front desk, patient access, registration, scheduling, administrative, or similar. Keywords: `Patient Access`, `Front Desk`, `Receptionist`, `Registration`, `Scheduler`, `Scheduling`, `Check-In`, `Check In`, `Administrative`, `Admin`, `Office`, `Clerical`, `Secretary`, `Coordinator`, `Billing`, `Coding`, `Collections`
4. **Category completeness check:** Flag missing categories
5. **Distribution check:** In a typical physician practice, support staff (non-provider) should represent 50–80% of total employees by headcount. If providers are > 60% of distinct employees, the extract may be missing support staff.
6. Compute: distinct employee count per category, percentage distribution

**Severity:**
- `CRITICAL` — zero clinical support staff found (RN, LPN, MA categories all empty — payroll extract appears incomplete)
- `HIGH` — zero non-clinical / patient access staff found (front desk, scheduling, admin categories all empty)
- `HIGH` — zero providers found (no Physician or APP employees — may be a support-staff-only extract missing provider compensation)
- `MEDIUM` — providers represent > 60% of distinct employees (staffing distribution is unusual — support staff may be excluded from the extract)
- `INFO` — staff category distribution summary with employee counts and percentages

**Output includes:** Table of detected staff categories with distinct employee count, total hours, total amount, and percentage of payroll. List of unclassified job codes for client review.

---

### `phase3/gl.py`

**Purpose:** Source-specific data quality checks for General Ledger files.

---

#### G1: Account Number Format Consistency

**What:** Verify Account Numbers follow a consistent format.

**Logic:**
1. Resolve `AcctNumber`
2. Analyze format patterns:
   - Length distribution
   - Numeric vs. alphanumeric
   - Common prefix/suffix patterns (e.g., `4000-001`, `REV-001`)
3. Flag format inconsistencies (e.g., mix of 4-digit and 8-digit account numbers)

**Severity:**
- `MEDIUM` — inconsistent account number formats
- `INFO` — format summary

---

#### G2: Cost Center Format and Uniqueness

**What:** Verify Cost Center Numbers are consistently formatted and unique per cost center name.

**Logic:**
1. Resolve `CostCenterNumberOrig`, `CostCenterNameOrig`
2. Check 1:1 mapping: each Cost Center Number should map to exactly one Cost Center Name (and vice versa)
3. Flag: same number → different names, or same name → different numbers
4. Population rate for both columns

**Severity:**
- `HIGH` — Cost Center Number/Name mapping is not 1:1 (will cause GL join ambiguity)
- `INFO` — distinct cost center count and format summary

---

#### G3: PIVOT Account Category Classification

**What:** Classify every account into PIVOT's standard P&L categories. These categories are the foundation for G4 (presence check) and G7 (cost center P&L completeness). PIVOT requires the GL to distinguish between specific revenue and expense types to build a meaningful practice-level P&L.

**PIVOT P&L categories (authoritative list):**

| Category | Keywords / Patterns (case-insensitive on AcctDesc or AcctType) |
|---|---|
| **Charges (Gross Revenue)** | `charge`, `gross revenue`, `gross patient revenue`, `patient charges`, `gross charges`, `fee for service`, `FFS revenue` |
| **Adjustments (Contractual / Write-offs)** | `adjustment`, `contractual`, `allowance`, `write-off`, `writeoff`, `write off`, `bad debt`, `charity`, `discount`, `deduction`, `contra revenue` |
| **Other Revenue** | `other revenue`, `other income`, `miscellaneous revenue`, `misc revenue`, `grant`, `capitation`, `incentive revenue`, `meaningful use`, `quality bonus`, `340B`, `interest income`, `rental income` |
| **Provider Compensation** | `physician comp`, `provider comp`, `provider salary`, `physician salary`, `APP salary`, `APP comp`, `doctor salary`, `MD comp`, `DO comp`, `NP salary`, `PA salary`, `CRNA salary`, `provider bonus`, `physician bonus`, `provider benefit`, `physician benefit`, `provider retirement`, `physician 401`, `provider FICA`, `physician payroll tax` |
| **Support Staff Compensation** | `staff salary`, `staff wages`, `support staff`, `nursing salary`, `RN salary`, `LPN salary`, `MA salary`, `medical assistant`, `clerical salary`, `admin salary`, `front desk`, `patient access`, `staff benefit`, `staff retirement`, `staff FICA`, `staff payroll tax`, `temp labor`, `agency`, `overtime` (when not provider-specific) |
| **Facilities / Occupancy** | `rent`, `lease`, `occupancy`, `building`, `facility`, `depreciation`, `amortization`, `maintenance`, `repair`, `utilities`, `electric`, `water`, `janitorial`, `property tax`, `property insurance` |
| **Medical Supplies** | `medical supply`, `medical supplies`, `clinical supply`, `surgical supply`, `pharmaceutical`, `drug`, `vaccine`, `implant`, `lab supply`, `reagent` |
| **Other Operating Expenses** | `office supply`, `office supplies`, `IT`, `technology`, `software`, `hardware`, `telephone`, `internet`, `postage`, `printing`, `travel`, `education`, `CME`, `dues`, `subscription`, `license`, `insurance` (not property), `legal`, `consulting`, `professional fee`, `outsource`, `billing service`, `collection`, `marketing`, `advertising`, `recruitment`, `malpractice` |
| **Unclassified** | No keyword match — requires manual review or Chart of Accounts file |

**Logic:**
1. Resolve `AcctDesc`. Note: Account Type is a Recommended field but has no staging column mapping — if a raw Account Type column was captured in Phase 1, use its raw name directly.
2. If a raw Account Type column is available, use it as the primary classification input (it is typically more structured than descriptions)
3. Apply keyword matching against the category table above — first exact phrase match, then substring match
4. For ambiguous matches (a description matches keywords in multiple categories), prefer the more specific category and log the ambiguity
5. Store the classification result per account number for use by G4 and G7

**Returns:**
```python
{
    "account_classifications": {
        "4000": {"acct_desc": "GROSS PATIENT REVENUE", "category": "Charges", "confidence": "HIGH"},
        "4500": {"acct_desc": "CONTRACTUAL ADJUSTMENTS", "category": "Adjustments", "confidence": "HIGH"},
        "5100": {"acct_desc": "PHYSICIAN SALARIES", "category": "Provider Compensation", "confidence": "HIGH"},
        "5200": {"acct_desc": "OFFICE RENT", "category": "Facilities / Occupancy", "confidence": "HIGH"},
        "5999": {"acct_desc": "MISC EXPENSE", "category": "Unclassified", "confidence": "NONE"},
        ...
    },
    "category_summary": {
        "Charges": {"acct_count": 3, "total_amount": 2500000},
        "Adjustments": {"acct_count": 5, "total_amount": -1800000},
        ...
    }
}
```

**Severity:**
- `HIGH` — > 30% of accounts (by count) are Unclassified (GL may need a Chart of Accounts file to interpret)
- `MEDIUM` — > 10% but ≤ 30% Unclassified
- `INFO` — classification summary with category distribution

---

#### G4: P&L Category Presence

**What:** Verify that the GL extract contains accounts from each critical P&L category. A complete physician practice GL should have Charges, Adjustments, Provider Compensation, Support Staff Compensation, and at least some Other Operating Expenses. Missing an entire category suggests the extract is incomplete or filtered too narrowly.

**Required P&L categories** (at least one account must exist in each):

| Category | Why Required |
|---|---|
| **Charges (Gross Revenue)** | Without gross charges, PIVOT cannot compute net revenue or charge-based productivity. |
| **Adjustments** | Without contractual adjustments, net revenue will be overstated — a fundamental P&L error. |
| **Provider Compensation** | Core to PIVOT's compensation-to-production benchmarking. |
| **Support Staff Compensation** | PIVOT's overhead and staffing analysis requires non-provider labor costs. |
| **Other Operating Expenses** | At minimum, some non-compensation expense should exist (rent, supplies, etc.) for a complete P&L. |

**Recommended P&L categories** (should exist but may not in all practices):

| Category | Why Recommended |
|---|---|
| **Other Revenue** | Not all practices have non-patient revenue, but its absence is worth noting. |
| **Facilities / Occupancy** | Some practices include facility costs in a corporate allocation rather than the practice GL — note if absent. |
| **Medical Supplies** | Some practices don't have significant supply costs — acceptable if absent. |

**Logic:**
1. Use the G3 classification output: `account_classifications` and `category_summary`
2. For each Required category: check if at least one account was classified into it AND has non-zero total amount
3. For each Recommended category: check presence, flag if absent as INFO
4. Special case — **Adjustments may be negative:** Contractual adjustments are typically negative amounts (reducing gross revenue to net). If the Adjustments category exists but all amounts are positive, flag as MEDIUM (may be sign-reversed or misclassified).

**Severity:**
- `CRITICAL` — Charges category missing (no gross revenue accounts — GL extract is fundamentally incomplete)
- `CRITICAL` — Provider Compensation category missing (core to PIVOT benchmarking)
- `HIGH` — Adjustments category missing (net revenue cannot be calculated)
- `HIGH` — Support Staff Compensation category missing (overhead analysis impossible)
- `MEDIUM` — Other Operating Expenses category missing (P&L is incomplete beyond compensation)
- `MEDIUM` — Adjustments exist but all amounts are positive (likely sign issue)
- `INFO` — Recommended categories absent (Other Revenue, Facilities, Medical Supplies)

---

#### G5: Amount Reasonableness

**What:** Check GL amounts for obvious errors.

**Logic:**
1. Resolve `AmountOrig`
2. Negative amounts are valid in GL (revenue credits, expense reversals) — do not flag negatives by default
3. Flag:
   - Any single row with |Amount| > $100,000,000 (likely data error or wrong units)
   - All amounts are zero (empty GL extract)
   - All amounts are the same value (likely placeholder data)
4. By cost center: total amounts should be reasonable for a monthly period

**Severity:**
- `HIGH` — all amounts zero or all identical
- `MEDIUM` — extreme magnitude outliers
- `INFO` — amount distribution summary by account type

---

#### G6: Report Date / YearMonth Validation

**What:** Verify the Report Date or YearMonth field is populated and that the data covers the expected test month. Multiple months may be present in the GL extract — this is normal (clients often provide 2–3 months of test data).

**Logic:**
1. Resolve `YearMonth` (int, YYYYMM format)
2. Extract the set of distinct YearMonth values present
3. Verify the test month (from Phase 1) is included among them. If not, the GL may cover a different period than the other core files.
4. Report all months present and their row counts
5. If Report Date is a date field instead of YearMonth: parse, extract YYYYMM, apply same logic
6. Flag any obviously invalid YearMonth values (e.g., `000000`, `999999`, values before `200001` or after `203012`)

**Severity:**
- `HIGH` — the test month identified by Phase 1 is not present in the GL's YearMonth values (GL covers a different period than Billing/Scheduling/Payroll)
- `MEDIUM` — invalid or unparseable YearMonth values detected
- `INFO` — list of months present with row counts and total amounts per month

---

#### G7: Cost Center P&L Completeness (Rough P&L)

**What:** Build a rough P&L for each cost center and flag cost centers that are missing critical account categories. G4 checks presence at the file level (does the entire GL have Charges? Adjustments?). G7 checks at the cost center level — because a GL file can pass G4 overall while individual cost centers are missing entire categories. A cost center with Provider Compensation but no Charges is a red flag; a cost center with Charges but no Provider Compensation may indicate the cost center is an ancillary department or the data is split across cost centers.

**Logic:**
1. Using the G3 classification output, pivot the data into a matrix: rows = cost centers, columns = P&L categories, values = sum of `AmountOrig`
2. For each cost center, determine which of the 5 Required P&L categories (from G4) are present with non-zero amounts:
   - Charges (Gross Revenue)
   - Adjustments
   - Provider Compensation
   - Support Staff Compensation
   - Other Operating Expenses
3. **Completeness scoring per cost center:**
   - **Complete (5/5):** All 5 required categories present — cost center has a full P&L
   - **Mostly Complete (3–4/5):** Missing 1–2 categories — flag the gaps
   - **Incomplete (1–2/5):** Missing 3+ categories — likely a partial or non-practice cost center
   - **Empty (0/5):** Only Unclassified or Recommended categories — cannot build a P&L
4. **Common gap patterns to call out specifically:**
   - **Charges present, no Adjustments:** Net revenue will be overstated for this cost center. Client may book adjustments at a consolidated level rather than per cost center.
   - **Provider Comp present, no Charges:** Compensation is recorded here but production is booked elsewhere — need to understand cost center structure.
   - **Charges present, no Provider Comp:** May be a location/department that bills under one cost center but pays providers under another — common in multi-site practices. Not necessarily an error, but important for PIVOT configuration.
   - **No Support Staff Comp:** Staff may be pooled into a shared/corporate cost center rather than allocated. Note for client discussion.
   - **Only one category:** Cost center may be a holding/allocation center (e.g., "Corporate Overhead", "Shared Services") — not a true practice P&L.
5. **Rough P&L table per cost center:** Compute and output:

   ```
   Cost Center: 1001 - Internal Medicine
   -------------------------------------
   Charges (Gross Revenue)       $2,450,000
   Adjustments                  ($1,720,000)
   -------------------------------------
   Net Revenue                     $730,000
   -------------------------------------
   Provider Compensation          ($425,000)
   Support Staff Compensation     ($180,000)
   Facilities / Occupancy          ($45,000)
   Medical Supplies                ($12,000)
   Other Operating Expenses        ($38,000)
   -------------------------------------
   Total Expenses                 ($700,000)
   -------------------------------------
   Net Income / (Loss)              $30,000
   -------------------------------------
   Categories Present: 7/8  Missing: [Other Revenue]
   Completeness: COMPLETE (5/5 required)
   ```

6. **Summary statistics:**
   - Count of cost centers by completeness tier (Complete, Mostly Complete, Incomplete, Empty)
   - List of cost centers missing each required category
   - Aggregate rough P&L across all cost centers (sanity check: does total net revenue and total expense look reasonable for a physician practice?)

**Output per cost center:**
```python
{
    "cost_center_number": "1001",
    "cost_center_name": "Internal Medicine",
    "categories_present": ["Charges", "Adjustments", "Provider Compensation", "Support Staff Compensation", "Facilities / Occupancy", "Medical Supplies", "Other Operating Expenses"],
    "categories_missing": ["Other Revenue"],
    "required_present": 5,
    "required_missing": [],
    "completeness_tier": "Complete",
    "amounts": {
        "Charges": 2450000,
        "Adjustments": -1720000,
        "Net Revenue": 730000,
        "Provider Compensation": -425000,
        "Support Staff Compensation": -180000,
        "Facilities / Occupancy": -45000,
        "Medical Supplies": -12000,
        "Other Operating Expenses": -38000,
        "Total Expenses": -700000,
        "Net Income": 30000
    }
}
```

**Severity:**
- `HIGH` — any cost center is Incomplete (1–2 of 5 required categories) or Empty — suggests the GL structure needs client explanation before PIVOT can use it
- `MEDIUM` — any cost center is Mostly Complete (3–4 of 5 required categories) — specific gaps should be discussed with the client
- `MEDIUM` — Charges present with no Adjustments on a cost center (net revenue will be wrong)
- `MEDIUM` — Provider Comp present with no Charges on a cost center (compensation without production)
- `INFO` — all cost centers are Complete; rough P&L summary provided for review

**Excel output:** A dedicated sub-sheet or section in the `Source-Specific Findings` sheet containing the rough P&L table for each cost center, plus a summary row showing aggregate totals and the completeness tier distribution.

---

### `phase3/quality.py`

**Purpose:** Source-specific data quality checks for Quality measure files.

---

#### Q1: Performance Rate Range

**What:** Performance Rates should be between 0 and 100 (percentage).

**Logic:**
1. Resolve the staging column for Performance Rate
2. Check all values are numeric and between 0 and 100 (inclusive)
3. Flag values outside this range
4. Flag null Performance Rates (required field)

**Severity:**
- `HIGH` — Performance Rates outside 0–100 range
- `HIGH` — null Performance Rates on required rows

---

#### Q2: Numerator ≤ Denominator Logic

**What:** The Numerator should not exceed the Denominator (after accounting for Exclusions/Exceptions).

**Logic:**
1. Resolve Numerator, Denominator, Exclusions/Exceptions columns
2. Compute effective denominator: `Denominator - Exclusions/Exceptions`
3. Flag rows where `Numerator > effective denominator`
4. Flag rows where Denominator = 0 (cannot calculate a rate)
5. Verify: `Performance Rate ≈ (Numerator / effective denominator) × 100` (within ±1% tolerance for rounding)

**Severity:**
- `HIGH` — Numerator > Denominator (logically impossible)
- `MEDIUM` — calculated rate doesn't match reported Performance Rate (data inconsistency)
- `INFO` — zero-denominator rows (may be legitimate for measures with no eligible patients)

---

#### Q3: Is_Inverse Validation

**What:** Verify Is_Inverse is correctly populated.

**Logic:**
1. Resolve Is_Inverse column
2. Check values are `Y` or `N` (or `Yes`/`No`, `1`/`0`, `TRUE`/`FALSE`)
3. Flag non-standard values
4. Known inverse measures (lower = better): HbA1c Poor Control (CMS-122), Diabetes: Eye Exam (inverse in some contexts). Flag if Is_Inverse is blank when a known inverse measure number is present.

**Severity:**
- `MEDIUM` — non-standard Is_Inverse values
- `INFO` — Is_Inverse distribution

---

#### Q4: Measure Number Format

**What:** Verify Measure Numbers are in a recognizable format.

**Logic:**
1. Resolve Measure Number column
2. Check against known format patterns:
   - eCQM: `CMS###v##` (e.g., `CMS122v12`)
   - CMS: `CMS-###` or `CMS###`
   - QPP: `QPP-###` or `###` (standalone number)
   - MIPS: `MIPS-###`
3. Flag values that don't match any recognized format

**Severity:**
- `MEDIUM` — > 10% of measure numbers in unrecognized format
- `INFO` — measure number format summary

---

#### Q5: Measurement Period Logic

**What:** Verify measurement period dates are logical.

**Logic:**
1. Resolve Measurement Period Start Date and End Date
2. Start must be before End
3. Period length should be recognizable: 1 month, 1 quarter, 6 months, or 1 year (most common)
4. All rows should have consistent periods (or a small number of distinct periods)
5. Dates should be within or overlap the test month

**Severity:**
- `HIGH` — Start > End (inverted dates)
- `MEDIUM` — period lengths vary widely or are unusually short/long
- `INFO` — measurement period summary

---

### `phase3/patient_satisfaction.py`

**Purpose:** Source-specific data quality checks for Patient Satisfaction files.

---

#### PS1: Score Range Validation

**What:** Scores should be within a valid numeric range.

**Logic:**
1. Resolve Score column
2. Determine the scoring scale:
   - If max value ≤ 5: likely a 1–5 Likert scale
   - If max value ≤ 10: likely a 1–10 scale
   - If max value ≤ 100: likely a 0–100 percentage scale
3. Flag scores outside the detected range
4. Flag negative scores
5. Flag null scores (required field)

**Severity:**
- `HIGH` — scores outside detected valid range or negative
- `INFO` — score distribution summary and detected scale

---

#### PS2: Survey Date Range Logic

**What:** Verify survey date ranges are logical.

**Logic:**
1. Resolve Survey Date Range Start and End
2. Start must be before End
3. Dates should be within or overlap the test month
4. Flag: very long survey periods (> 6 months — unusual for monthly submissions)

**Severity:**
- `HIGH` — Start > End
- `MEDIUM` — survey period > 6 months
- `INFO` — date range summary

---

#### PS3: Question Order Validation

**What:** Verify Question Order is populated and sequential.

**Logic:**
1. Resolve Question Order column
2. Values should be positive integers
3. Per provider: Question Order should be sequential (1, 2, 3, ...) without gaps
4. Flag: non-numeric values, gaps in sequence, duplicate question orders per provider

**Severity:**
- `MEDIUM` — gaps or duplicates in question order sequence
- `INFO` — question order summary

---

#### PS4: Provider NPI Validation

**What:** Verify Provider NPIs are valid (10-digit format).

**Logic:** Same as B5 NPI checks but scoped to the patient satisfaction file.

**Severity:**
- `MEDIUM` — NPI format issues
- `INFO` — NPI summary

---

### `phase3/report.py`

**Purpose:** Render Phase 3 results to console and write to the Excel report and JSON manifest.

**Console output — per file:**

```
+-------------------------------------------------------------------+
| DATA QUALITY REVIEW                                               |
+----------------------+--------------------------------------------+
| File Name            | PIVOT_Billing_Epic_202601.txt             |
| Source               | Billing Charges (Separate)                |
| Total Records        | 14,327                                    |
+----------------------+--------------------------------------------+
| UNIVERSAL CHECKS                                                  |
|  X CRITICAL: 'Work RVUs' is 62% null (Required field)   [8,883] |
|  ! HIGH: 284 duplicate rows on key [Charge ID]                   |
|  ! HIGH: 'Patient City' has 1,204 missing values (8.4%)          |
|  o MEDIUM: 47 placeholder values ('test') in Patient MRN         |
|  o INFO: Date range 2025-12-15 to 2026-01-14 (aligned)           |
+-------------------------------------------------------------------+
| BILLING-SPECIFIC CHECKS                                           |
|  ! HIGH: 12.3% of E&M rows have zero wRVUs                       |
|  o MEDIUM: 83 CPT codes contain embedded modifiers               |
|  o MEDIUM: NPI '1234567890' appears on 52% of rows               |
|  o INFO: 15 of 25 ICD-10 columns populated                       |
|  o INFO: Payer Financial Class - 4 of 47 values unclassifiable   |
+-------------------------------------------------------------------+
| ISSUE SUMMARY                                                     |
|  CRITICAL: 1  |  HIGH: 3  |  MEDIUM: 3  |  INFO: 2              |
+-------------------------------------------------------------------+
```

**Console output — overall Phase 3 summary:**

```
+-------------------------------------------------------------------+
| DATA QUALITY SUMMARY                                              |
+---------------------------+------+------+--------+-----+---------+
| File                      | CRIT | HIGH | MEDIUM | LOW | Total   |
+---------------------------+------+------+--------+-----+---------+
| Billing_Charges.txt       |  1   |  3   |  3     |  0  |   7     |
| Billing_Transactions.txt  |  0   |  1   |  2     |  0  |   3     |
| Scheduling.txt            |  0   |  2   |  1     |  0  |   3     |
| Payroll.txt               |  0   |  1   |  3     |  0  |   4     |
| GL.txt                    |  0   |  0   |  1     |  1  |   2     |
+---------------------------+------+------+--------+-----+---------+
| Total Issues: 19  (CRITICAL: 1, HIGH: 7, MEDIUM: 10, LOW: 1)    |
+-------------------------------------------------------------------+
```

**Excel report** — `{client}_{round}_Phase3_{YYYYMMDD}.xlsx` — sheets:

| Sheet | Contents |
|---|---|
| `Universal Findings` | One row per finding per file: check name, raw column, staging column, severity, message, affected row count, sample rows |
| `Source-Specific Findings` | One row per finding per file: check ID (B1–B14, S1–S9, P1–P7, G1–G7, Q1–Q5, PS1–PS4), severity, message, affected row count, sample rows/values |
| `Null Analysis` | One row per column per file: column name, requirement level, null count, blank count, total missing, missing %, severity |
| `Duplicate Analysis` | One row per file: key columns used, duplicate row count, duplicate group count, sample groups |
| `Cost Center P&L` | One row per cost center: cost center number/name, amount per P&L category, net revenue, total expense, net income, completeness tier, missing required categories |
| `Data Quality Summary` | One row per file: counts by severity level, total issue count |

**JSON manifest** — `phase3_findings.json`:
```json
{
    "client": "ClientName",
    "round": "v1",
    "date_run": "YYYY-MM-DD",
    "overall_issue_count": 19,
    "overall_critical_count": 1,
    "files": {
        "PIVOT_BillingCharges_Epic_202601.txt": {
            "source": "billing_charges",
            "record_count": 14327,
            "universal_findings": [...],
            "source_specific_findings": [...],
            "issue_summary": {
                "critical": 1,
                "high": 3,
                "medium": 3,
                "low": 0
            }
        }
    }
}
```

---

### `run_phase3.py`

**Purpose:** CLI entry point for Phase 3.

**Usage:**
```
py run_phase3.py --input ./input --output ./output --client "ClientName" --round v1 --knowledge-dir ./KnowledgeSources
```

**Execution order:**
1. Load `phase1_findings.json` and `phase2_findings.json` from `output/{client}/`
2. `loader.load_files(phase1_json, input_dir)` → DataFrames
3. Merge Phase 2 `RequirementLevel` classifications into column mappings
4. For each file:
   a. `universal.run_all_checks(df, column_mappings, test_month, source)` → universal findings
   b. Route to source-specific module based on `source`:
      - `billing.run_checks(billing_dfs, column_mappings, test_month)` — receives all billing files for cross-file linkage check (B2)
      - `scheduling.run_checks(df, column_mappings, test_month)`
      - `payroll.run_checks(df, column_mappings, test_month)`
      - `gl.run_checks(df, column_mappings, test_month)`
      - `quality.run_checks(df, column_mappings, test_month)`
      - `patient_satisfaction.run_checks(df, column_mappings, test_month)`
5. Combine all findings
6. `report.render(all_findings, output_dir, client, round)` → console + Excel + JSON

**Cross-file checks within Phase 3:** Only the B2 (Charge-Transaction Linkage) check requires multiple files. All other checks are single-file. Phase 4 handles cross-source validation (Billing↔GL, Billing↔Payroll, etc.).

---

## Key Implementation Notes

- **Phase 1 + 2 dependency** — Phase 3 requires both `phase1_findings.json` and `phase2_findings.json`. If either is missing, print an error with instructions.
- **Graceful column resolution** — Every source-specific check uses `resolve_column()` and skips gracefully if the target column was not mapped in Phase 1. This prevents crashes when clients omit columns. Each skipped check logs a note: `"Skipped check {id}: column '{staging_col}' not mapped in this file."`
- **Severity inheritance from Phase 2** — Phase 3 uses Phase 2's `RequirementLevel` to escalate severity. A null finding in a Required field is CRITICAL; the same finding in an Optional field is INFO. This means Phase 2 must run before Phase 3.
- **Row indices** — All findings that reference specific rows use 0-based DataFrame index values. The report adds 2 to convert to the client's file row number (accounting for the header row and 1-based counting).
- **Sample limits** — To keep findings manageable, all sample lists cap at 20 items. The `affected_row_count` field gives the true total.
- **Performance** — Source-specific checks that iterate per-row (e.g., B3 CPT-based wRVU logic) use vectorized pandas operations where possible. Checks that require per-group aggregation (e.g., P1 hours per employee) use `groupby()`.
- **Billing cross-file access** — `billing.py`'s `run_checks()` function receives a dict of all billing-source DataFrames so that B2 (Charge-Transaction Linkage) can access both the charges and transactions files. For Combined Billing, B2 is skipped (charges and transactions are in the same file — no linkage check needed).
- **Quality and Patient Satisfaction** — These sources have no staging table in `StagingTableStructure.xlsx`. Column resolution uses the Phase 1 mapping output directly (which mapped raw columns to conceptual staging names based on template field names). Type information comes from `DATA_FORMAT_PATTERNS` in `shared/constants.py`.
- **Avoiding double-counting with Phase 2** — Phase 2's `datatype_checker.py` already performed structural type checks (date parseability, NPI format, varchar length). Phase 3 does not repeat these. Instead, Phase 3 focuses on *semantic* quality: are the values logically correct, complete, and consistent? The one exception is the null pre-check in Phase 2 — Phase 3 performs the authoritative null analysis with full severity grading, and Phase 2's null pre-check is superseded.
- **Knowledge source caching** — `stdCmsCpt` and `stdCmsPos` are loaded once at Phase 3 startup via `shared/staging_meta.py`'s `load()` function (alongside the existing `StagingTableStructure.xlsx` and `TransactionTypes.xlsx` loading) and held as indexed DataFrames (indexed by `CptCode` and `PosCode` respectively). Lookups are O(1) dict-style. If the knowledge source files are missing, B13 and B14 are skipped with a warning: `"Knowledge source '{name}' not found at {path} — skipping CMS validation checks."`
- **Knowledge source freshness** — The `stdCmsCpt` file should correspond to the CMS fee schedule year matching the client's test data. If the test month is in CY2026, use the CY2026 fee schedule. A mismatch in fee schedule year can cause false-positive wRVU variance flags (CMS updates wRVUs annually). The report notes which fee schedule version was used.
- **Phase 4 prep notes** — Checks B12, S5, P4, and P5 produce metadata (ID format patterns, NPI population rates, department distributions) that Phase 4 will consume for cross-source matching. These are stored in the `phase3_findings.json` under a `cross_source_prep` key per file.

---

## Verification Checklist

After implementation, validate with a real or synthetic test batch:

**Universal Checks:**
- [ ] Required field with > 50% nulls → `CRITICAL`
- [ ] Required field with 5% nulls → `HIGH`
- [ ] Recommended field with 15% nulls → `MEDIUM`
- [ ] Optional field with 30% nulls → `INFO`
- [ ] File with exact duplicate rows on primary key → `HIGH` with correct count
- [ ] File with dates outside test month → correct severity based on percentage
- [ ] Values like `test`, `TBD`, `1234567890` in NPI field → `HIGH` placeholder finding
- [ ] File with mojibake characters (`Ã©`) → `MEDIUM` encoding finding

**Billing Checks:**
- [ ] Combined billing file with only charge rows → `CRITICAL` (B1)
- [ ] Separate billing: 25% orphaned transactions → `HIGH` (B2)
- [ ] E&M code (99213) with wRVU = 0 → flagged (B3); lab code (80053) with wRVU = 0 → not flagged
- [ ] Charge amount = $0 on non-void charge → flagged (B4)
- [ ] NPI `1234567890` on > 50% of rows → `MEDIUM` concentration warning (B5)
- [ ] CPT value `99213-25` → `MEDIUM` embedded modifier (B7)
- [ ] ICD-10 column with value `250.00,401.9` → `HIGH` concatenation (B8)
- [ ] Void row with positive Units → `MEDIUM` (B10)
- [ ] CPT code `99999` not in stdCmsCpt → flagged as unmatched (B13)
- [ ] Client wRVU = 0 for CPT 99213 but CMS wRVU = 1.92 → variance flagged (B13)
- [ ] Deleted CPT code still being billed → `MEDIUM` (B13)
- [ ] POS code `99` not in stdCmsPos → `HIGH` invalid code (B14)
- [ ] POS description mismatch: client says `11` = "Emergency" but CMS says "Office" → `MEDIUM` (B14)
- [ ] > 30% of charges at POS 21 (Inpatient) → `MEDIUM` distribution flag (B14)

**Scheduling Checks:**
- [ ] Appointment status `COMP` maps to Completed (S1)
- [ ] Cancelled appointment with no Cancel Date → `HIGH` (S2)
- [ ] Completed appointment with Duration = 0 → `MEDIUM` (S3)
- [ ] Check Out time before Check In → `MEDIUM` (S4)
- [ ] Patient ID format (numeric) different from Billing (alphanumeric) → `HIGH` (S5)
- [ ] Scheduled status has highest % instead of Completed → `HIGH` (S8)
- [ ] Zero rows with Cancelled or Rescheduled status → `HIGH` (S8)
- [ ] Neither New Patient nor Established/Return appointment types found → `HIGH` (S9)
- [ ] Only Established types found, no New Patient types → `MEDIUM` (S9)

**Payroll Checks:**
- [ ] Hours = -40 on regular earnings → `HIGH` (P1)
- [ ] Employee with 500 total hours in one pay period → `MEDIUM` (P1)
- [ ] Pay Period Start > End → `HIGH` (P3)
- [ ] Physician job code with no NPI → flagged (P5)
- [ ] Earnings code `REG PAY` classifies as Base/Regular (P6)
- [ ] Zero RN/LPN/MA employees in payroll → `CRITICAL` (P7)
- [ ] Zero patient access/admin employees → `HIGH` (P7)
- [ ] Providers > 60% of distinct employees → `MEDIUM` (P7)

**GL Checks:**
- [ ] Same Cost Center Number maps to two different names → `HIGH` (G2)
- [ ] > 30% of accounts Unclassified → `HIGH` (G3)
- [ ] Zero Charges (Gross Revenue) accounts in entire GL → `CRITICAL` (G4)
- [ ] Zero Provider Compensation accounts → `CRITICAL` (G4)
- [ ] Zero Adjustments accounts → `HIGH` (G4)
- [ ] Zero Support Staff Compensation accounts → `HIGH` (G4)
- [ ] Adjustments exist but all positive amounts → `MEDIUM` sign issue (G4)
- [ ] GL has multiple YearMonth values (e.g., 202601, 202602) → `INFO` normal (G6)
- [ ] Test month not present among GL YearMonth values → `HIGH` (G6)
- [ ] Cost center "Internal Medicine" has Charges + Provider Comp but no Adjustments → `MEDIUM` (G7)
- [ ] Cost center "Shared Services" has only Support Staff Comp → completeness tier = Incomplete, `HIGH` flagged appropriately but noted as likely allocation center (G7)
- [ ] Rough P&L output shows Net Revenue and Total Expenses per cost center in Excel `Cost Center P&L` sheet (G7)

**Quality Checks:**
- [ ] Performance Rate = 105 → `HIGH` (Q1)
- [ ] Numerator (80) > Denominator (50) → `HIGH` (Q2)
- [ ] Is_Inverse = `Maybe` → `MEDIUM` (Q3)
- [ ] Measure Number `ABC123` → flagged as unrecognized format (Q4)
- [ ] Measurement Period Start > End → `HIGH` (Q5)

**Patient Satisfaction Checks:**
- [ ] Score = -5 → `HIGH` (PS1)
- [ ] Survey Date Start > End → `HIGH` (PS2)
- [ ] Question Order gaps (1, 2, 4 — missing 3) → `MEDIUM` (PS3)

**Reports:**
- [ ] Console prints one data quality box per file
- [ ] Console prints overall Phase 3 summary table
- [ ] Excel contains all 6 Phase 3 sheets (including Cost Center P&L)
- [ ] `phase3_findings.json` is valid JSON with all expected keys
- [ ] `cross_source_prep` metadata is populated for B12, S5, P4, P5

**Knowledge Sources:**
- [ ] Missing `stdCmsCpt.csv` → B13 skipped with warning, other checks unaffected
- [ ] Missing `stdCmsPos.csv` → B14 skipped with warning, other checks unaffected
- [ ] `stdCmsCpt` loads and indexes by CPT code correctly; lookups return expected wRVU values
- [ ] `stdCmsPos` loads and indexes by POS code correctly; POS `11` resolves to "Office"
