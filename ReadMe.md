# PIVOT Test File Review — Project Instructions

## Overview

This project helps review client-submitted test data files for **PIVOT** (Performance Insights Value Optimization Tool) database compatibility and data quality. Clients send one-month test files across multiple core data sources that must be validated individually and cross-referenced against each other before full historical data extraction begins.

## Phases
Phase 1: Initial Setup & Data Ingestion

File upload and identification
Detect data source types (Billing, Scheduling, Payroll, GL, Quality, Patient Satisfaction)
Determine billing format (Combined vs. Separate)
Parse files (handle pipe-delimited .txt, .csv, .xlsx formats)
Display basic file metadata (row counts, columns, samples)
Identify the test month across all files

Phase 2: Database Compatibility Check

Schema validation (required fields present?)
Field name recognition against specification
Data type verification (dates, NPIs, amounts, codes)
Classification of fields (Required/Recommended/Optional)
Flag missing fields by severity (CRITICAL/HIGH/Info)
Identify unexpected/unrecognized columns

Phase 3: Data Quality Review

Universal checks (nulls, duplicates, date ranges, encoding)
Source-specific validation rules:

Billing: transaction types, wRVUs, charge amounts, modifiers, NPIs
Scheduling: appointment statuses, times, cancellations
Payroll: hours reasonableness, earnings codes, pay periods
GL: account formats, cost centers, amounts
Quality: performance rates, measure formats, numerator/denominator logic
Patient Satisfaction: score ranges, survey dates



Phase 4: Cross-Source Validation

Inter-file relationship mapping:

Billing ↔ GL (cost center alignment)
Billing ↔ Payroll (provider NPI matching)
Billing ↔ Scheduling (patient identifiers, provider NPIs, locations)
Payroll ↔ GL (department to cost center)
Scheduling ↔ GL (location to cost center)
wRVU Reconciliation ↔ Billing (provider-level wRVU totals)
GL Reconciliation ↔ GL (P&L validation)



Phase 5: Results Generation & Reporting

Executive summary (Pass/Fail, issue counts by severity)
Detailed findings by data source
Client-ready issue list with specific row references
Resubmission checklist
Readiness determination for historical data extract
Date ranges per source (filter column and min/max dates from Phase 1)

**Test files must:**
- Be pipe-delimited (`|`) `.txt` format for all core data sources
- Be Excel format for supporting data files
- Include headers; must **not** include footers
- All core test files must be pulled for the **same month** of data
- Test file data must be reconciled against an internal report before sending to PIVOT

---

## Core Data Sources

### 1. Billing

Billing data can arrive in one of two formats depending on the client's billing system:

- **Combined Billing** — A single file containing both charges and transactions in one extract with fields: Transaction Type, Transaction Description, and a single Amount field.
- **Separate Billing** — Two distinct files:
  - **Billing Charges** — Charge-level detail with Charge Amount and Work RVUs (no transaction/payment data)
  - **Billing Transactions** — Payment, adjustment, and refund detail linked to charges via Charge ID or Invoice Number/Encounter ID. Contains: Transaction ID, Transaction Description, Post Date, Payment Amount, Adjustment Amount, Refund Amount, Payer Name/Plan/Financial Class, Reason Category, Claim Adjudication Reason Code/Description.

**Filter:** Post Date
**Submission cadence:** Monthly on the 15th for data from the 15th of the previous month through the 14th of the current month.

#### Required Billing Fields (Charges)

Date of Service, Post Date, CPT-4 Code, CPT Code Modifier 1–4, Units, Work RVUs, CMS Place of Service Code, Primary ICD-10 CM Code, Secondary ICD-10 CM Code, Third ICD-10 CM Code, Fourth ICD-10 CM Code, 5th–25th ICD-10 CM Code (in separate columns), Patient MRN/Identifier, Patient DOB, Patient Gender, Patient ZIP Code, Rendering Provider Full Name, Rendering Provider NPI, Rendering Provider's Primary Specialty, Billing Provider Full Name, Billing Provider NPI, Billing Provider's Primary Specialty, Practice Name, Billing Location Name, Department Name, Cost Center, Primary Payer Name, Primary Payer Plan, Primary Payer Financial Class, Charge ID, Invoice Number/Encounter ID.

- **Combined Billing also requires:** Transaction Type, Transaction Description, Amount.
- **Separate Charges also require:** Charge Amount.

#### Required Billing Transaction Fields (Separate Billing only)

Transaction ID, Transaction Description, Post Date, Payment Amount, Adjustment Amount, Refund Amount, Payer Name, Payer Plan, Payer Financial Class, Charge ID, Invoice Number/Encounter ID.

#### Recommended (not required)

CPT Code Description, Last Modified Date, Rendering Provider First/Middle/Last Name, Rendering Provider ID, Rendering Provider Credentials, Billing Provider First/Middle/Last Name, Billing Provider ID, Billing Provider Credentials, Patient Race/Ethnicity, Patient Marital Status, Patient City, Cost Center *(if not available, a crosswalk to GL cost centers is required)*.

#### Referring Provider Fields (Optional)

Referring Provider First/Middle/Last Name, Full Name, NPI, ID, Primary Specialty, Credentials.

---

### 2. Scheduling

**Filter:** Appointment Date
**Submission cadence:** Monthly after the 1st for the previous calendar month.

#### Required Fields

Appt ID, Location Name *(must tie back to Billing location or GL cost center)*, Appt Provider Full Name, Appt Provider NPI, Patient Identifier *(must match Patient MRN/Identifier in Billing)*, Appt Type, Created Date, Appt Date, Cancel Date, Cancel Reason, Appt Time, Scheduled Length, Appt Status.

#### Recommended

Practice Name, Department Name, Cost Center, Appt Provider First/Middle/Last Name, Appt Provider Credentials, Appt Provider ID, Appt Provider Primary Specialty, Check In Date/Time, Check Out Date/Time.

#### Optional

Referring Provider fields (Full Name, First/Middle/Last Name, Credentials, NPI, ID, Primary Specialty).

---

### 3. Payroll

**Filter:** Pay Period End Date
**Submission cadence:** Monthly after the 1st for pay periods ending in the prior month.

#### Required Fields

Employee ID, Employee Full Name, Job Code ID, Job Code Description, Department ID *(must tie back to a GL cost center)*, Department Name, Pay Period Start Date, Pay Period End Date, Check/Pay Date, Earnings Code, Earnings Description, Hours, Amount.

#### Recommended

Provider ID, Employee First/Middle/Last Name, Employee NPI.

---

### 4. General Ledger

**Filter:** Report Date by Calendar Year (YearMonth in YYYYMM format preferred; other date formats are auto-detected and converted to YYYYMM for analysis)
**Submission cadence:** Monthly after month-end close (typically 1–3 weeks after month end).

#### Required Fields

Cost Center Number, Cost Center Name, Report Date, Account #, Account Description, Amount.

#### Recommended

Account Type *(the textual description providing clarity on revenue vs. expense categories)*.

#### Optional

Sub-Account Number, Sub-Account Description.

---

### 5. Quality

**Filter:** Measurement Period
**Submission cadence:** Preferred monthly.

#### Required Fields

Provider NPI, Measurement Period Start Date, Measurement Period End Date, Measure Number *(eCQM ID, CMS measure number, or QPP number)*, Is_Inverse *(Y/N — identifies measures where lower score = better)*, Denominator, Exclusions/Exceptions, Numerator, Performance Rate.

#### Recommended/Optional

Provider Name, Measure Description, Initial Population, Benchmark Target.

---

### 6. Patient Satisfaction *(Optional)*

**Filter:** Survey Date Range
**Submission cadence:** Preferred monthly.

#### Required Fields

Provider NPI, Survey Date Range Start, Survey Date Range End, Survey Question Full *(limit 500 chars)*, Question Order, Score *(numeric, up to 2 decimals)*.

#### Optional

Provider Name, Survey Question Abbreviated *(limit 100 chars)*, Number of Respondents, Standard Deviation, Benchmarking Filter, Benchmark 1, Benchmark 2.

---

## Supporting Data Files *(Excel format)*

| File | Cadence | Required Fields |
|---|---|---|
| **wRVU Reconciliation** | Required, Monthly | Rendering Provider NPI, Rendering Provider Name, YearMonth (YYYYMM), wRVUs. *Recommended: Units, CPT-4 Code, Modifiers 1–4.* |
| **GL Reconciliation / P&L** | Required, One-Time | Cost Center Number, Cost Center Name, Account Number, Account Name, YearMonth, Amount (Actuals). |
| **Chart of Accounts** | Required upon request, One-Time | Account Number, Account Name, Descriptor *(e.g., "Revenue - OUTPATIENT - ROUTINE", "Expense - IP CONTRACTUAL ALLOWANCES")*. |
| **Provider Specialties & cFTEs** | Required, Preferred Monthly | Provider NPI, Provider Name, Provider Specialty, YearMonth (YYYYMM), cFTE (clinical FTE). *Optional: Billing Provider ID, Payroll Provider ID, Non-Clinical FTE breakdown (Administrative, Research, Teaching, Service).* |
| **Industry Productivity Benchmarks** | Required, Yearly | Benchmark Specialty, wRVU percentiles 10th–90th. |
| **Industry Compensation Benchmarks** | Required, Yearly | Benchmark Specialty, Compensation percentiles 10th–90th. |

---

## Review Process Workflow

### Phase 1: Initial Setup & Data Loading

When test files are uploaded:

1. Identify which data source(s) are being reviewed and whether billing is Combined or Separate format.
2. Load and parse the files (handle pipe-delimited `.txt`, `.csv`, and `.xlsx`).
3. Display basic file information:
   - File name and detected source type
   - Number of records
   - Columns present vs. expected columns
   - Sample of first few rows
4. Identify the test month — all core files should cover the same single month.

---

### Phase 2: Database Compatibility Check

For each file, verify:

#### Schema Validation
- Do all required fields exist per the field lists above?
- Are field names recognizable (check against the exact field names listed in this document)?
- Are data types correct? (Dates as Short Date, NPIs as 10-digit text, amounts as numbers, codes as text)

#### Required vs. Recommended vs. Optional
- Flag missing **Required** fields as **CRITICAL**
- Flag missing **Recommended** fields as **HIGH**
- Note missing **Optional** fields as informational only
- Flag any unexpected/unrecognized columns in the file

#### Data Format Validation
- Date fields parseable and in a consistent format
- NPI fields are 10 digits (no alpha, no truncation)
- CPT codes are 5 characters
- ICD-10 codes follow standard format (letter + digits with optional decimal)
- CMS Place of Service is a 2-digit code
- Amount/RVU/Units fields are numeric
- No embedded pipe characters within field values that would break delimiters

---

### Phase 3: Data Quality Review

#### Universal Checks (All Sources)
- Null/blank values in required fields
- Duplicate records (by primary key or logical key)
- Date ranges within the expected test month
- Numeric values within expected ranges
- Obvious test/placeholder data (e.g., "test", "xxx", "TBD")
- Consistent encoding (no garbled characters)

#### Billing-Specific
- Do Transaction Types make sense? (Charge, Payment, Adjustment, Void, Refund)
- For Combined Billing: are both charges and transactions present in the same file?
- For Separate Billing: can Charge ID or Invoice Number/Encounter ID link the Charges and Transactions files?
- Are wRVU values populated for E&M and procedural CPT codes? (Lab/ancillary codes may legitimately be 0)
- Do charge amounts appear reasonable for the CPT codes billed?
- Are Rendering Provider NPIs consistently 10 digits?
- Is Cost Center populated? If not, is there a Billing Location or Department that can serve as a crosswalk to GL?
- Are Payer Financial Class values categorizable (Commercial, Medicare, Medicaid, Self-Pay)?
- Do CPT Modifiers appear in separate columns (not concatenated with the CPT code)?
- Are ICD-10 codes in separate columns (5th–25th in individual columns, not combined)?
- Is Patient MRN/Identifier present and consistent (will need to match Scheduling)?
- Are void charges reflected with negative units?
- Do Post Dates fall within the expected test month window?

#### Scheduling-Specific
- Are appointment statuses mappable to standard categories (Completed, Cancelled, Rescheduled, No Show)?
- Do cancelled appointments have Cancel Date and Cancel Reason populated?
- Are Appt Times and Scheduled Lengths logical (no zero-length appointments for completed visits)?
- Do completed appointments have Check In/Out times when available?
- Does Patient Identifier match the format used in Billing?
- Can Location Name or Practice Name/Department Name tie back to a Billing location or GL cost center?
- Do Appt Provider NPIs appear valid (10 digits)?
- Are appointment dates within the expected test month?

#### Payroll-Specific
- Do Employee IDs appear consistent and non-null?
- Are Hours reasonable (not negative, not exceeding ~180 per pay period for a single earnings code)?
- Are Amounts reasonable for the earnings type?
- Do Pay Period Start/End dates define logical periods (start before end, reasonable length)?
- Can Department ID/Name tie back to GL cost centers?
- Are Job Code Descriptions mappable to MGMA employee categories (Physician, APP, RN, MA, etc.)?
- Are Earnings Codes distinguishable as clinical compensation, non-clinical, PTO, benefits, etc.?
- Is Employee NPI populated for providers (physicians, APPs)?

#### General Ledger-Specific
- Do Account Numbers follow a consistent format?
- Are Cost Center Numbers unique and consistently formatted?
- Can Account Descriptions or Account Types be categorized into standard PIVOT categories (revenue types, expense types: provider comp, staff expense, facilities, etc.)?
- Do amounts appear reasonable (no obviously erroneous magnitudes)?
- Is the Report Date within the expected test month?
- Are both revenue and expense accounts present?

#### Quality-Specific
- Are Performance Rates between 0 and 100?
- Is Is_Inverse correctly populated (Y for measures where lower = better, like HbA1c poor control)?
- Does Numerator ≤ Denominator (after accounting for Exclusions/Exceptions)?
- Are Measure Numbers in a recognizable format (CMS-###v##, QPP-###)?
- Do Measurement Period dates make sense?

#### Patient Satisfaction-Specific
- Are Scores within a valid range (typically 0–100)?
- Are Survey Date Ranges logical (start before end)?
- Is Question Order populated and sequential?
- Are Provider NPIs valid 10-digit values?

---

### Phase 4: Cross-Source Validation

After individual file review, check the data mapping relationships that PIVOT depends on:

#### Billing ↔ GL
- Can Billing Location, Department, or Practice Name be mapped to GL Cost Center Numbers? Flag any billing records with locations/departments that don't have a clear GL cost center match.
- If Cost Center is populated in Billing, do those values exist in the GL file?

#### Billing ↔ Payroll
- Do Rendering Provider NPIs in Billing appear as Employee NPIs in Payroll? Flag providers in Billing with no payroll match.
- If NPI is not in Payroll, can provider names be matched (name matching algorithm will be needed)?

#### Billing ↔ Scheduling
- Do Patient MRN/Identifiers in Billing match Patient Identifiers in Scheduling? Flag mismatched ID formats.
- Do Rendering Providers in Billing match Appt Providers in Scheduling (by NPI)?
- Can Scheduling Locations tie to Billing Locations or GL Cost Centers?

#### Payroll ↔ GL
- Do Payroll Department IDs/Names correspond to GL Cost Center Numbers/Names?

#### Scheduling ↔ GL
- Can Scheduling Location/Practice/Department tie to GL Cost Centers (directly or through Billing location mapping)?

#### wRVU Reconciliation ↔ Billing
- Do provider NPIs in the wRVU recon file match Rendering Provider NPIs in Billing?
- Compare wRVU totals by provider between the recon file and the calculated/extracted wRVUs in Billing for the test month. Flag variances and calculate reconciliation percentage (target: >98%).

#### GL Reconciliation ↔ GL
- Do Cost Center Numbers and Account Numbers in the P&L reconciliation file match those in the GL extract?
- Compare totals by cost center/account between the two sources.

---

## Results Generation

### Executive Summary
- Pass/Fail status for each data source
- Count of issues by severity: **CRITICAL**, **HIGH**, **MEDIUM**, **LOW**
- Overall recommendation: *Ready for Historical Extract* / *Needs Revision (Round X)*
- Note which billing format was submitted (Combined or Separate)

### Detailed Findings by Data Source

For each source:
- Missing required fields (CRITICAL)
- Missing recommended fields (HIGH)
- Data type/format issues with specific column names
- Data quality issues with specific row numbers or counts
- Cross-reference failures

### Issue List for Client

Formatted as clear, actionable items:

```
[CRITICAL] Billing - Missing required field 'Rendering Provider NPI' — Affects all rows
[CRITICAL] Billing - Cost Center field is blank and no Billing Location to GL crosswalk provided — Rows: All
[HIGH] Scheduling - Patient Identifier format (numeric) does not match Billing Patient MRN format (alphanumeric) — Cannot link scheduling to billing patients
[HIGH] Payroll - Department ID '99999' has no matching GL Cost Center — Rows: 12, 45, 78
[MEDIUM] Billing - 37 records have blank CPT Code Modifier columns where modifiers appear concatenated in CPT-4 Code field — Rows: 5, 18, 22...
[LOW] GL - Sub-Account Number and Sub-Account Description columns are empty (optional fields)
```

### Resubmission Checklist
- Specific items the client must fix
- Reference to the required field lists and formats in this document
- Reminder: files must be pipe-delimited `.txt` with headers, no footers
- Reminder: all core test files must cover the same month
- Reminder: test file data should be reconciled against an internal report before resubmission

---

## How to Use This Project

### When a Client Submits Test Files

1. Upload all test files for the submission round.
2. State the client name and which submission iteration this is (v1, v2, v3 — up to 3 rounds per contract).
3. Claude will automatically:
   - Detect file types and identify data sources
   - Determine if billing is Combined or Separate format
   - Run all schema, quality, and cross-source validation checks
   - Generate the findings report with the client-ready issue list

### Handling Multiple Iterations

- Label files with version or round numbers (e.g., `ClientName_Billing_v2.txt`)
- Claude will compare against previous submission issues when prior reports are available
- Track which issues were resolved vs. persistent across rounds
- **Note:** per contract, up to 3 rounds of test files per dataset are included; additional rounds may incur charges

### Adding New Validation Rules

To add a new check, say:
> "Add this check to [Source] review: [description]"

Claude will incorporate it into future reviews.

---

## Quick Commands

### CLI Commands

| Command | Description |
|---|---|
| `py run_phase1.py "ClientName" v1` | Phase 1 — Ingestion, source detection, column mapping, test month |
| `py run_phase2.py "ClientName" v1` | Phase 2 — Schema validation, data type checks, compatibility |
| `py run_phase3.py "ClientName" v1` | Phase 3 — Data quality review (universal + source-specific) |
| `py run_phase4.py "ClientName" v1` | Phase 4 — Cross-source validation (C0–C5) |
| `py run_phase5.py "ClientName" v1` | Phase 5 — Results generation, readiness determination |
| `py run_all.py "ClientName" v1 --no-prompt` | Run all 5 phases sequentially |

All scripts accept `--client "Name" --round v1` as an alternative to positional arguments. See the individual How-To guides for full option lists.

### Natural Language Prompts

| Command | Description |
|---|---|
| `"Review these test files"` | Starts full validation process |
| `"Check cross-source mappings"` | Focuses on Billing↔GL, Billing↔Payroll, Scheduling↔Billing linkages |
| `"Generate client report"` | Creates formatted output for client |
| `"Compare to previous submission"` | Analyzes what changed from last round |
| `"What's missing from this submission?"` | Identifies which expected data sources haven't been submitted |
| `"Reconcile wRVUs"` | Compares wRVU recon file against billing wRVU totals by provider |
| `"Check billing format"` | Determines if file is Combined or Separate billing and validates accordingly |
