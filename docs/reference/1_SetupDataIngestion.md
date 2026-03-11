# Phase 1: Initial Setup & Data Ingestion

## Purpose

Phase 1 is the entry point of the PIVOT Test File Review process. When a client submits test files, this phase handles identification, ingestion, preliminary characterization, and intelligent column-to-staging mapping of every file before any validation begins. The goal is to establish a clear inventory of what was received, confirm the data sources represented, determine the billing format, verify that all files cover the same test month, and produce a preliminary raw-to-staging column mapping that feeds directly into the database ingestion pipeline.

**Implementation:** This phase will be implemented in Python, leveraging `pandas` for file parsing and data inspection, `rapidfuzz` (or equivalent) for fuzzy column name matching, and `openpyxl` for reading the staging structure and mapping knowledge base from the project Excel files. Scripts will be built in a separate step following plan approval.

---

## 1.1 File Receipt & Inventory

### Expected File Types

| Category | Format | Delimiter | Extension |
|---|---|---|---|
| Core Data Sources | Pipe-delimited text | `\|` | `.txt` |

### Core Data Sources (up to 8 files depending on billing format)

| # | Source | Filter Field | Submission Cadence |
|---|---|---|---|
| 1 | **Billing** (Combined) — OR — **Billing Charges** + **Billing Transactions** (Separate) | Post Date | Monthly on the 15th (15th of prior month → 14th of current month) |
| 2 | **Scheduling** | Appointment Date | Monthly after the 1st (previous calendar month) |
| 3 | **Payroll** | Pay Period End Date | Monthly after the 1st (pay periods ending in prior month) |
| 4 | **General Ledger (GL)** | Report Date (Calendar Year) | Monthly after month-end close (1–3 weeks post month-end) |
| 5 | **Quality** | Measurement Period | Preferred monthly |
| 6 | **Patient Satisfaction** *(Optional)* | Survey Date Range | Preferred monthly |

### File Naming Convention

Core files should follow: `PIVOT_"Source"_"System"_"YYYYMM of Data"`
Example: `PIVOT_Scheduling_Epic_202501`

### File Pickup

Test files are manually uploaded to a local working directory. On each run, the Python scripts will scan the designated input directory, detect all files present, and process them as a single submission batch. The directory should contain only the files for one client and one submission round — mixing clients or rounds in the same directory is not supported.

```
/input/
  ├── PIVOT_BillingCharges_Epic_202601.txt
  ├── PIVOT_BillingTransactions_Epic_202601.txt
  ├── PIVOT_Scheduling_Epic_202601.txt
  ├── PIVOT_Payroll_Epic_202601.txt
  ├── PIVOT_GL_Epic_202601.txt
  └── PIVOT_Quality_Epic_202601.txt
```

The scripts will iterate over all files in the input directory, identify each by extension and column fingerprinting, and proceed through the Phase 1 workflow.

---

## 1.2 Billing Format Detection

The first critical determination is whether the client uses **Combined** or **Separate** billing. This affects which staging table the data targets and how many billing files to expect.

### Combined Billing → `#staging_billing` (89 staging columns)

A single file containing both charges and transactions. Identified by the presence of these three fields alongside charge-level detail:

- `Transaction Type` — Identifier for charges, void charges, payments, adjustments, and refunds
- `Transaction Description` — Description of the transaction type
- `Amount` — The total charge, payment, adjustment, or refund associated to the transaction type

Combined billing maps to a single staging table (`#staging_billing`) that contains charge-level columns, transaction-level columns, and payment/adjustment/refund amount columns all in one structure.

**Template reference:** "Billing" tab in `PIVOT_Data_Extract_Template_FY26_Combined_Billing.xlsx`

### Separate Billing → `#staging_charges` (80 columns) + `#staging_transactions` (17 columns)

Two distinct files that link via `Charge ID` or `Invoice Number / Encounter ID`:

**File 1 — Billing Charges → `#staging_charges`:** Charge-level detail with `Charge Amount` and `Work RVUs` (no transaction/payment columns).

**File 2 — Billing Transactions → `#staging_transactions`:** Payment, adjustment, and refund detail with fields including Transaction ID, Transaction Description, Post Date, Payment Amount, Adjustment Amount, Refund Amount, Payer Name/Plan/Financial Class, Reason Category, Claim Adjudication Reason Code/Description, Other Reason Detail, Charge ID, and Invoice Number / Encounter ID.

**Template reference:** "Billing Charges" and "Billing Transactions" tabs in `PIVOT_Data_Extract_Template_FY26_Seperate_Billing.xlsx` *(filename preserved as-is; "Seperate" is a known misspelling in the actual file)*

### Detection Logic

```
IF file contains columns [Transaction Type, Transaction Description, Amount]
   AND file contains charge-level fields (CPT-4 Code, Work RVUs, etc.)
   AND file contains transaction-level amounts (Payment Amount, Adjustment Amount, Refund Amount)
   THEN → Combined Billing → target: #staging_billing

ELSE IF one file contains [Charge Amount] without transaction fields
   AND a second file contains [Transaction ID, Payment Amount, Adjustment Amount, Refund Amount]
   THEN → Separate Billing → targets: #staging_charges + #staging_transactions

ELSE → Flag for manual review / ask client to clarify
```

---

## 1.3 File Parsing Rules

### Core Files (Pipe-Delimited .txt)

- **Delimiter:** Pipe character (`|`)
- **Headers:** First row must be column headers — REQUIRED
- **Footers:** Must NOT be present; strip any trailing summary/footer rows
- **Encoding:** UTF-8 expected; flag garbled characters
- **Quoting:** Embedded pipes within field values will break parsing — flag if detected
- **Line endings:** Handle both `\n` and `\r\n`

### Parsing Steps

1. Detect file extension (`.txt`, `.csv`)
2. Attempt pipe-delimited parse first; fall back to comma/tab if pipe fails
3. Strip leading/trailing whitespace from all column names
4. Remove any completely empty rows or columns
5. Count total records (excluding header)

---

## 1.4 Data Source Identification

After parsing, match each file to its data source by comparing column headers against the expected field lists.

### Fingerprint Columns by Source

Use the presence of distinctive columns to identify the source:

| Source | Distinctive Columns |
|---|---|
| Billing (Combined) | `Transaction Type`, `Transaction Description`, `Amount`, `CPT-4 Code`, `Work RVUs` |
| Billing Charges (Separate) | `Charge Amount`, `CPT-4 Code`, `Work RVUs` (no `Transaction Type`) |
| Billing Transactions (Separate) | `Transaction ID`, `Payment Amount`, `Adjustment Amount`, `Refund Amount` |
| Scheduling | `Appt ID`, `Appt Date`, `Appt Status`, `Appt Type`, `Scheduled Length` |
| Payroll | `Employee ID`, `Job Code ID`, `Earnings Code`, `Pay Period Start Date`, `Pay Period End Date` |
| General Ledger | `Cost Center Number`, `Cost Center Name`, `Account #`, `Account Description` |
| Quality | `Measure Number`, `Is_Inverse`, `Denominator`, `Numerator`, `Performance Rate` |
| Patient Satisfaction | `Survey Date Range Start`, `Survey Date Range End`, `Survey Question Full`, `Question Order`, `Score` |

---

## 1.5 Intelligent Raw-to-Staging Column Mapping

### Overview

Once a file is identified, each raw column header from the client file must be mapped to its corresponding staging column in the PIVOT database. Client billing systems use wildly inconsistent naming conventions — the mapping knowledge base contains **2,573 known raw-to-staging mappings** across all sources. This step is critical: it determines whether the data can flow into the database and surfaces any columns that cannot be automatically resolved.

### Staging Table Targets

| Source File | Target Staging Table | Staging Columns | Known Raw Aliases |
|---|---|---|---|
| Billing Charges (Separate) | `#staging_charges` | 80 | 1,237 |
| Billing Transactions (Separate) | `#staging_transactions` | 17 | 277 |
| Billing (Combined) | `#staging_billing` | 89 | 95+ |
| Scheduling | `#staging_scheduling` | 35 | 380 |
| Payroll | `#staging_payroll` | 18 | 381 |
| General Ledger | `#staging_gl` | 9 | 189 |
| Quality | *(no staging table — own ingestion path)* | — | — |
| Patient Satisfaction | *(no staging table — own ingestion path)* | — | — |

### Mapping Algorithm

For each raw column header in the client file:

```
1. EXACT MATCH: Check if the raw column name matches a known RawColumn 
   in the mapping table for the identified staging table.
   → If match: assign the mapped StagingColumn

2. NORMALIZED MATCH: Normalize both the raw column and all known aliases
   (lowercase, strip spaces/underscores/hyphens/special chars) and compare.
   → If match: assign with HIGH confidence

3. FUZZY MATCH: Use string similarity (Levenshtein distance, token overlap)
   against all known aliases for the target staging table.
   → If similarity > 85%: suggest mapping with MEDIUM confidence, flag for review

4. SEMANTIC MATCH: For remaining unmatched columns, compare against staging
   column descriptions and field purpose definitions.
   → If probable match: suggest mapping with LOW confidence, flag for manual review

5. UNMAPPED: Column cannot be matched to any staging column.
   → Flag as UNRECOGNIZED — may be a client-specific custom field, 
     an optional field not in the staging schema, or a naming issue
```

### Mapping Output Format

For each file, produce a mapping table:

| Raw Column (Client) | Staging Column | Staging Table | Confidence | SQL Type | Max Length | Notes |
|---|---|---|---|---|---|---|
| Date of Service | DateOfService | #staging_charges | EXACT | date | 3 (prec=10) | |
| Post Date | PostDate | #staging_charges | EXACT | date | 3 (prec=10) | |
| CPT-4 Code | CptCode | #staging_charges | EXACT | varchar | 30 | |
| CHARGE_AMOUNT | ChargeAmountOriginal | #staging_charges | NORMALIZED | decimal | 9 (prec=14,scale=2) | Also maps to ChargeAmountClean |
| BillingProcedureCode | CptCode | #staging_charges | FUZZY (92%) | varchar | 30 | Known alias |
| CUSTOM_FIELD_1 | — | — | UNMAPPED | — | — | Not in staging schema |

### Dual-Mapping Columns

Some raw columns map to **two** staging columns simultaneously. These are critical to identify:

| Raw Column | Staging Column 1 (Original) | Staging Column 2 (Clean) | Type |
|---|---|---|---|
| Charge Amount | ChargeAmountOriginal | ChargeAmountClean | decimal(14,2) |
| Work RVUs | WorkRvuOriginal | WorkRvuClean | decimal(8,2) |
| Payment Amount | PaymentOriginal | PaymentClean | decimal(13,2) |
| Adjustment Amount | AdjustmentOriginal | AdjustmentClean | decimal(13,2) |
| Refund Amount | RefundOriginal | RefundClean | decimal(13,2) |
| Amount (GL) | AmountOrig | AmountClean | decimal(14,2) |
| Amount (Payroll) | AmountOrig | AmountClean | decimal(10,2) |
| Patient MRN/Identifier | PatientId | PatientMrn | varchar(50) / varchar(25) |

The "Clean" column is typically a calculated/transformed version of the "Original" populated during downstream ETL processing. During test file review, both should be noted in the mapping but only the "Original" needs data present in the raw file.

---

## 1.6 Staging Table Structure — Data Type Reference

The staging tables define the exact SQL types, max lengths, precision, and scale that raw data must conform to. These constraints drive the data type validation in Phase 2.

### `#staging_charges` — Key Data Type Constraints

| Staging Column | SQL Type | Max Length | Precision | Scale | Validation Rule |
|---|---|---|---|---|---|
| ChargeId | varchar | 150 | — | — | Text, max 150 chars |
| InvoiceNumber | varchar | 40 | — | — | Text, max 40 chars |
| DateOfService | date | — | 10 | 0 | Must parse as valid date |
| PostDate | date | — | 10 | 0 | Must parse as valid date |
| CptCode | varchar | 30 | — | — | Text; expect 5-char CPT codes |
| CptCodeDesc | varchar | 1000 | — | — | Text, max 1000 chars |
| Modifier1–4 | varchar | 20 | — | — | Text, max 20 chars each |
| RenderingProviderNpi | varchar | 20 | — | — | Text; expect 10-digit NPI |
| RenderingProviderFullName | varchar | 120 | — | — | Text, max 120 chars |
| PatientId | varchar | 50 | — | — | Text, max 50 chars |
| PatientMrn | varchar | 25 | — | — | Text, max 25 chars |
| PatientZip | varchar | 30 | — | — | Text (not numeric) |
| PatientGender | varchar | 20 | — | — | Text, max 20 chars |
| PlaceOfServiceCode | varchar | 10 | — | — | Text; expect 2-digit POS code |
| PrimaryIcdCode | varchar | 50 | — | — | Text; ICD-10 format |
| SecondaryIcdCodes | varchar | 50 | — | — | Text; ICD-10 format |
| ChargePayerName | varchar | 120 | — | — | Text, max 120 chars |
| ChargePayerFinancialClass | varchar | 120 | — | — | Text, max 120 chars |
| Units | int | — | 10 | 0 | Integer |
| ChargeAmountOriginal | decimal | — | 14 | 2 | Numeric, 2 decimal places |
| WorkRvuOriginal | decimal | — | 8 | 2 | Numeric, 2 decimal places |

### `#staging_transactions` — Key Data Type Constraints

| Staging Column | SQL Type | Max Length | Precision | Scale | Validation Rule |
|---|---|---|---|---|---|
| ChargeId | varchar | 150 | — | — | Links to #staging_charges |
| InvoiceNumber | varchar | 40 | — | — | Links to #staging_charges |
| PostDate | date | — | 10 | 0 | Must parse as valid date |
| TransactionType | varchar | 50 | — | — | Text |
| TransactionTypeDesc | varchar | 100 | — | — | Text |
| TransactionPayerName | varchar | 120 | — | — | Text |
| TransactionPayerFinancialClass | varchar | 120 | — | — | Text |
| ReasonCode | varchar | 50 | — | — | Text |
| ReasonCodeDesc | varchar | 250 | — | — | Text |
| PaymentOriginal | decimal | — | 13 | 2 | Numeric, 2 decimal places |
| AdjustmentOriginal | decimal | — | 13 | 2 | Numeric, 2 decimal places |
| RefundOriginal | decimal | — | 13 | 2 | Numeric, 2 decimal places |

### `#staging_scheduling` — Key Data Type Constraints

| Staging Column | SQL Type | Max Length | Precision | Scale | Validation Rule |
|---|---|---|---|---|---|
| ApptId | varchar | 40 | — | — | Unique appointment identifier |
| ApptStatus | varchar | 100 | — | — | Text |
| ApptType | varchar | 100 | — | — | Text |
| BillLocNameOrig | varchar | 120 | — | — | Must tie to billing/GL |
| DeptNameOrig | varchar | 120 | — | — | Text |
| PatIdOrig | varchar | 40 | — | — | Must match billing Patient ID |
| ApptProvNPI | varchar | 10 | — | — | **10 chars only** (stricter than billing) |
| ApptProvFullNameOrig | varchar | 92 | — | — | Max 92 chars |
| ApptDate | date | — | 10 | 0 | Must parse as valid date |
| ApptTime | time | — | 16 | 7 | Must parse as valid time |
| ApptSchdLength | numeric | — | 12 | 2 | Numeric; appointment duration |
| CancellationDate | date | — | 10 | 0 | Valid date or null |
| CancelReason | varchar | 100 | — | — | Text |
| CheckInDate / CheckOutDate | date | — | 10 | 0 | Valid date or null |
| CheckInTime / CheckOutTime | time | — | 16 | 7 | Valid time or null |

### `#staging_payroll` — Key Data Type Constraints

| Staging Column | SQL Type | Max Length | Precision | Scale | Validation Rule |
|---|---|---|---|---|---|
| EmployeeId | varchar | 40 | — | — | Non-null, consistent ID |
| EmployeeFullName | varchar | 120 | — | — | Text |
| EmployeeNpi | varchar | 20 | — | — | 10-digit NPI for providers |
| JobCode | varchar | 50 | — | — | Text |
| JobCodeDesc | varchar | 120 | — | — | Text |
| DepartmentId | varchar | 50 | — | — | Must tie to GL cost center |
| DepartmentName | varchar | 120 | — | — | Text |
| PayPeriodStartDate | date | — | 10 | 0 | Must parse as valid date |
| PayPeriodEndDate | date | — | 10 | 0 | Must parse as valid date |
| CheckDate | date | — | 10 | 0 | Check maturity date |
| EarningsCode | varchar | 50 | — | — | Text |
| EarningsCodeDesc | varchar | 120 | — | — | Text |
| Hours | decimal | — | 7 | 2 | Numeric, 2 decimal places |
| AmountOrig | decimal | — | 10 | 2 | Numeric, 2 decimal places |

### `#staging_gl` — Key Data Type Constraints

| Staging Column | SQL Type | Max Length | Precision | Scale | Validation Rule |
|---|---|---|---|---|---|
| CostCenterNumberOrig | varchar | 50 | — | — | Text; consistent format |
| CostCenterNameOrig | varchar | 120 | — | — | Text |
| AcctNumber | varchar | 40 | — | — | Text |
| AcctDesc | varchar | 250 | — | — | Text |
| SubAcctNumber | varchar | 30 | — | — | Text (optional) |
| SubAcctDesc | varchar | 250 | — | — | Text (optional) |
| YearMonth | int | — | 10 | 0 | Integer; YYYYMM format |
| AmountOrig | decimal | — | 14 | 2 | Numeric, 2 decimal places |

### `#staging_billing` — Data Type Constraints

`#staging_billing` (89 columns) is the Combined billing target. Its constraints are a superset of `#staging_charges` and `#staging_transactions`. Key distinctions from Separate billing:

- `TransactionType` (varchar 50) and `TransactionTypeDesc` (varchar 100) are present and required (replace the separate transactions file fields)
- `Amount` maps to both `ChargeAmountOriginal` + `ChargeAmountClean` (decimal(14,2)) for charges **and** serves the transaction-level amount context via `TransactionType`
- All provider, patient, payer, and location columns carry the same constraints as `#staging_charges`
- Payment, adjustment, and refund amounts (`PaymentOriginal`, `AdjustmentOriginal`, `RefundOriginal`) are included as separate columns within the same row

Refer to the Combined Billing field reference in Section 1.11 for the full column list and type mappings.

---

## 1.7 Basic File Information Display

For each file loaded, display:

```
┌─────────────────────────────────────────────────────────────────┐
│ FILE SUMMARY                                                     │
├──────────────────────┬──────────────────────────────────────────┤
│ File Name            │ PIVOT_Billing_Epic_202601.txt            │
│ Detected Source      │ Billing Charges (Separate)               │
│ Target Staging Table │ #staging_charges                         │
│ Format               │ Pipe-delimited .txt                      │
│ Record Count         │ 14,327                                   │
│ Column Count         │ 64                                       │
│ Headers Present      │ Yes                                      │
│ Footer Detected      │ No                                       │
├──────────────────────┴──────────────────────────────────────────┤
│ COLUMN MAPPING RESULTS                                           │
├──────────────────────┬──────────────────────────────────────────┤
│ EXACT match          │ 52 columns                               │
│ NORMALIZED match     │ 6 columns                                │
│ FUZZY match (review) │ 3 columns                                │
│ UNMAPPED             │ 3 columns — [CUSTOM_FIELD_1, ...]        │
│ Staging cols covered │ 61 of 80                                 │
│ Dual-map cols found  │ 4 — [ChargeAmount, WorkRVUs, PatientMRN] │
├──────────────────────┴──────────────────────────────────────────┤
│ SAMPLE DATA (first 3 rows)                                       │
│ Date of Service | Post Date | CPT-4 Code | Units | Amount | ...  │
│ 2026-01-05      | 2026-01-08| 99214      | 1     | 300.00 | ...  │
│ 2026-01-05      | 2026-01-08| 99213      | 1     | 200.00 | ...  │
│ 2026-01-06      | 2026-01-09| 99215      | 1     | 450.00 | ...  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1.8 Test Month Identification & Alignment

All core test files must cover the **same single month** of data. Identify the test month from each file using its filter field:

| Source | Date Field to Check | Expected Pattern |
|---|---|---|
| Billing | `Post Date` | 15th of prior month → 14th of current month |
| Scheduling | `Appt Date` | Full prior calendar month (1st → last day) |
| Payroll | `Pay Period End Date` | Pay periods ending in the prior month |
| General Ledger | `Report Date` | Calendar month |
| Quality | `Measurement Period Start/End Date` | Measurement period range |
| Patient Satisfaction | `Survey Date Range Start/End` | Survey period range |

### Alignment Check

1. Extract the min and max dates from each file's filter field
2. Determine the implied month for each file
3. Compare across all core files
4. **Flag if files cover different months** — this is a CRITICAL issue

Example output:
```
TEST MONTH ALIGNMENT
  Billing:              Post Dates 2026-01-15 to 2026-02-14  → Test Month: Jan 2026
  Scheduling:           Appt Dates 2026-01-01 to 2026-01-31  → Test Month: Jan 2026
  Payroll:              Pay Period End 2026-01-04 to 2026-01-31 → Test Month: Jan 2026
  General Ledger:       Report Date 2026-01-01 to 2026-01-31  → Test Month: Jan 2026
  ✅ All files aligned to January 2026
```

---

## 1.9 Submission Tracking

Record the following metadata for each submission:

- **Client Name**
- **Submission Round** (v1, v2, or v3 — max 3 per contract; additional rounds may incur charges)
- **Date Received**
- **Files Included** (list with detected source type and target staging table)
- **Files Missing** (expected but not submitted)
- **Billing Format** (Combined or Separate)
- **Test Month** (identified from file contents)

---

## 1.10 Phase 1 Outputs

At the end of Phase 1, the following should be established:

| Output | Description |
|---|---|
| **File Inventory Table** | Every file listed with its detected source, format, record count, column count, and target staging table |
| **Billing Format Determination** | Combined (`#staging_billing`) or Separate (`#staging_charges` + `#staging_transactions`), with rationale |
| **Raw-to-Staging Column Mapping** | For each file: every raw column mapped to its staging column with confidence level, SQL type, and max length constraints |
| **Mapping Gap Report** | Unmapped raw columns, unmapped required staging columns, and dual-mapping columns identified |
| **Missing Source Alert** | Any expected core sources not submitted |
| **Test Month Confirmation** | The single month all files should cover, with any misalignment flags |
| **Parsing Issues Log** | Any encoding errors, footer rows stripped, delimiter problems, or unreadable files |
| **Data Type Constraint Summary** | Staging table type/length/precision constraints that will drive Phase 2 validation |

These outputs feed directly into **Phase 2: Database Compatibility Check**, where the column mappings and staging constraints are used to validate schema conformance, data types, and value ranges.

---

## 1.11 Complete Field Reference — Core Data Sources

The tables below provide the exact field names, requirement levels, data formats, and staging column mappings for all core data sources as defined in the FY26 PIVOT Data Extract Templates and staging table structures.

### Billing — Combined (Charges and Transactions in One File)

Target staging table: `#staging_billing` (89 columns)

| Field Name | Required | Data Format | Staging Column | SQL Type |
|---|---|---|---|---|
| Date of Service | Required | Short Date | DateOfService | date |
| Post Date | Required | Short Date | PostDate | date |
| Last Modified Date | Optional | Short Date | — | — |
| CPT-4 Code | Required | Text | CptCode | varchar(30) |
| CPT Code Description | Recommended | Text | CptCodeDesc | varchar(1000) |
| CPT Code Modifier 1 | Required | Text | Modifier1 | varchar(20) |
| CPT Code Modifier 2 | Required | Text | Modifier2 | varchar(20) |
| CPT Code Modifier 3 | Required | Text | Modifier3 | varchar(20) |
| CPT Code Modifier 4 | Required | Text | Modifier4 | varchar(20) |
| Units | Required | Integer | Units | int |
| Transaction Type | Required | Number | TransactionType | varchar(50) |
| Transaction Description | Required | Text | TransactionTypeDesc | varchar(100) |
| Amount | Required | Number | ChargeAmountOriginal + ChargeAmountClean | decimal(14,2) |
| Work RVUs | Required | Number | WorkRvuOriginal + WorkRvuClean | decimal(8,2) |
| CMS Place of Service Code | Required | Text | PlaceOfServiceCode | varchar(10) |
| Primary ICD-10 CM Code | Required | Text | PrimaryIcdCode | varchar(50) |
| Primary ICD-10 CM Code Description | Optional | Text | PrimaryIcdDesc | varchar(250) |
| Secondary ICD-10 CM Code | Required | Text | SecondaryIcdCodes | varchar(50) |
| Secondary ICD-10 CM Code Description | Optional | Text | SecondaryIcdDesc | varchar(700) |
| Third ICD-10 CM Code | Required | Text | SecondaryIcdCodes | varchar(50) |
| Third ICD-10 CM Code Description | Optional | Text | SecondaryIcdDesc | varchar(700) |
| Fourth ICD-10 CM Code | Required | Text | SecondaryIcdCodes | varchar(50) |
| Fourth ICD-10 CM Code Description | Optional | Text | SecondaryIcdDesc | varchar(700) |
| 5th–25th ICD-10 CM Code | Required | Text | SecondaryIcdCodes | varchar(50) |
| 5th–25th ICD-10 CM Description | Optional | Text | SecondaryIcdDesc | varchar(700) |
| Patient MRN/Identifier | Required | Text | PatientId + PatientMrn | varchar(50) / varchar(25) |
| Patient DOB | Required | Short Date | PatientAge (calculated) | varchar(20) |
| Patient Gender | Required | Text | PatientGender | varchar(20) |
| Patient Race/Ethnicity | Optional | Text | PatientRace | varchar(50) |
| Patient Marital Status | Optional | Text | PatientMaritalStatus | varchar(50) |
| Patient City | Required | Text | PatientCity | varchar(50) |
| Patient ZIP Code | Required | Text | PatientZip | varchar(30) |
| Rendering Provider First Name | Recommended | Text | RenderingProviderFirstName | varchar(35) |
| Rendering Provider Middle Name/Initial | Recommended | Text | RenderingProviderMiddleName | varchar(35) |
| Rendering Provider Last Name | Recommended | Text | RenderingProviderLastName | varchar(40) |
| Rendering Provider Full Name | Required | Text | RenderingProviderFullName | varchar(120) |
| Rendering Provider NPI | Required | Text | RenderingProviderNpi | varchar(20) |
| Rendering Provider ID | Recommended | Text | RenderingProviderId | varchar(40) |
| Rendering Provider's Primary Specialty | Required | Text | RenderingProviderSpecialty | varchar(100) |
| Rendering Provider Credentials | Required | Text | RenderingProviderCredentials | varchar(70) |
| Billing Provider First Name | Recommended | Text | BillingProviderFirstName | varchar(35) |
| Billing Provider Middle Name/Initial | Recommended | Text | BillingProviderMiddleName | varchar(35) |
| Billing Provider Last Name | Recommended | Text | BillingProviderLastName | varchar(40) |
| Billing Provider Full Name | Required | Text | BillingProviderFullName | varchar(120) |
| Billing Provider NPI | Required | Text | BillingProviderNpi | varchar(20) |
| Billing Provider ID | Recommended | Text | BillingProviderId | varchar(40) |
| Billing Provider's Primary Specialty | Required | Text | BillingProviderSpecialty | varchar(100) |
| Billing Provider Credentials | Required | Text | BillingProviderCredentials | varchar(70) |
| Referring Provider First Name | Optional | Text | ReferringProviderFirstName | varchar(35) |
| Referring Provider Middle Name/Initial | Optional | Text | ReferringProviderMiddleName | varchar(35) |
| Referring Provider Last Name | Optional | Text | ReferringProviderLastName | varchar(40) |
| Referring Provider Full Name | Optional | Text | ReferringProviderFullName | varchar(120) |
| Referring Provider NPI | Optional | Text | ReferringProviderNpi | varchar(20) |
| Referring Provider ID | Recommended | Text | ReferringProviderId | varchar(40) |
| Referring Provider's Primary Specialty | Optional | Text | ReferringProviderSpecialty | varchar(100) |
| Referring Provider Credentials | Optional | Text | ReferringProviderCredentials | varchar(70) |
| Practice Name | Required | Text | BillPracticeName | varchar(120) |
| Billing Location Name | Required | Text | BillLocationName | varchar(120) |
| Department Name | Required | Text | BillDepartmentName | varchar(120) |
| Cost Center | Required* | Text | BillPracticeName (varies) | varchar(120) |
| Primary Payer Name | Required | Text | ChargePayerName | varchar(120) |
| Primary Payer Plan | Required | Text | ChargePayerPlan | varchar(120) |
| Primary Payer Financial Class | Required | Text | ChargePayerFinancialClass | varchar(120) |
| Charge ID | Required | Text | ChargeId | varchar(150) |
| Invoice Number / Encounter ID | Required | Text | InvoiceNumber | varchar(40) |

*\*If Cost Center is not available in Billing data, a crosswalk to GL must be provided.*

### Billing Charges — Separate (Charges Only File)

Target staging table: `#staging_charges` (80 columns)

Same as Combined Billing above, except:
- **Remove:** `Transaction Type`, `Transaction Description`, `Amount`
- **Add:** `Charge Amount` → maps to `ChargeAmountOriginal` + `ChargeAmountClean` (decimal(14,2))

### Billing Transactions — Separate (Transactions Only File)

Target staging table: `#staging_transactions` (17 columns)

| Field Name | Required | Data Format | Staging Column | SQL Type |
|---|---|---|---|---|
| Transaction ID | Required | Text | TransactionType | varchar(50) | ⚠️ Raw field "Transaction ID" is a numeric code (e.g., 1=Payment, 2=Adjustment) identifying the transaction type — not a unique row key. Maps to `TransactionType`, not `TransactionId`. |
| Transaction Description | Required | Text | TransactionTypeDesc | varchar(100) | |
| Post Date | Required | Short Date | PostDate | date |
| Last Modified Date | Optional | Short Date | — | — |
| Payment Amount | Required | Number | PaymentOriginal + PaymentClean | decimal(13,2) |
| Adjustment Amount | Required | Number | AdjustmentOriginal + AdjustmentClean | decimal(13,2) |
| Refund Amount | Required | Number | RefundOriginal + RefundClean | decimal(13,2) |
| Payer Name | Required | Text | TransactionPayerName | varchar(120) |
| Payer Plan | Required | Text | TransactionPayerPlan | varchar(120) |
| Payer Financial Class | Required | Text | TransactionPayerFinancialClass | varchar(120) |
| Reason Category | Optional | Text | ReasonCodeCategory | varchar(50) |
| Claim Adjudication Reason Code | Optional | Text | ReasonCode | varchar(50) |
| Claim Adjudication Reason Description | Optional | Text | ReasonCodeDesc | varchar(250) |
| Other Reason Detail | Optional | Text | ReasonCodeCategory | varchar(50) |
| Charge ID | Required | Text | ChargeId | varchar(150) |
| Invoice Number / Encounter ID | Required | Text | InvoiceNumber | varchar(40) |

### Scheduling

Target staging table: `#staging_scheduling` (35 columns)

| Field Name | Required | Data Format | Staging Column | SQL Type |
|---|---|---|---|---|
| Appt ID | Required | Text | ApptId | varchar(40) |
| Location Name | Required | Text | BillLocNameOrig | varchar(120) |
| Practice Name | Recommended | Text | PracNameOrig | varchar(120) |
| Department Name | Required | Text | DeptNameOrig | varchar(120) |
| Cost Center | Recommended | Text | PracNameOrig | varchar(120) |
| Appt Provider Full Name | Required | Text | ApptProvFullNameOrig | varchar(92) |
| Appt Provider First Name | Recommended | Text | ApptProvFirstName | varchar(35) |
| Appt Provider Middle Name | Recommended | Text | ApptProvMidName | varchar(20) |
| Appt Provider Last Name | Optional | Text | ApptProvLastName | varchar(35) |
| Appt Provider Credentials | Recommended | Text | — | — |
| Appt Provider NPI | Optional | Text | ApptProvNPI | varchar(10) |
| Appt Provider ID | Optional | Text | ApptProvId | varchar(40) |
| Appt Provider Primary Specialty | Optional | Text | ApptProvSpecialty | varchar(75) |
| Referring Provider Full Name | Optional | Text | ReferProvFullNameOrig | varchar(92) |
| Referring Provider First Name | Optional | Text | ReferProvFirstName | varchar(35) |
| Referring Provider Middle Name | Optional | Text | ReferProvMidName | varchar(20) |
| Referring Provider Last Name | Optional | Text | ReferProvLastName | varchar(35) |
| Referring Provider Credentials | Recommended | Text | — | — |
| Referring Provider NPI | Optional | Text | ReferProvNPI | varchar(10) |
| Referring Provider ID | Optional | Text | ReferProvId | varchar(40) |
| Referring Provider Primary Specialty | Optional | Text | ReferProvSpecialty | varchar(75) |
| Patient Identifier | Required | Text | PatIdOrig | varchar(40) |
| Appt Type | Required | Text | ApptType | varchar(100) |
| Created Date | Required | Short Date | CreateDate | date |
| Appt Date | Required | Short Date | ApptDate | date |
| Cancel Date | Required | Short Date | CancellationDate | date |
| Cancel Reason | Required | Text | CancelReason | varchar(100) |
| Appt Time | Required | Text | ApptTime | time |
| Scheduled Length | Required | Number | ApptSchdLength | numeric(12,2) |
| Appt Status | Required | Text | ApptStatus | varchar(100) |
| Check In Date | Recommended | Date | CheckInDate | date |
| Check In Time | Recommended | Time | CheckInTime | time |
| Check Out Date | Recommended | Date | CheckOutDate | date |
| Check Out Time | Recommended | Time | CheckOutTime | time |

### Payroll

Target staging table: `#staging_payroll` (18 columns)

| Field Name | Required | Data Format | Staging Column | SQL Type |
|---|---|---|---|---|
| Employee ID | Required | Text | EmployeeId | varchar(40) |
| Provider ID | Recommended | Text | — | — |
| Employee First Name | Recommended | Text | EmployeeFirstName | varchar(35) |
| Employee Middle Name | Recommended | Text | EmployeeMiddleName | varchar(35) |
| Employee Last Name | Recommended | Text | EmployeeLastName | varchar(40) |
| Employee Full Name | Required | Text | EmployeeFullName | varchar(120) |
| Employee NPI | Recommended | Text | EmployeeNpi | varchar(20) |
| Job Code ID | Required | Text | JobCode | varchar(50) |
| Job Code Description | Required | Text | JobCodeDesc | varchar(120) |
| Department ID | Required | Text | DepartmentId | varchar(50) |
| Department Name | Required | Text | DepartmentName | varchar(120) |
| Pay Period Start Date | Required | Short Date | PayPeriodStartDate | date |
| Pay Period End Date | Required | Short Date | PayPeriodEndDate | date |
| Check/Pay Date | Optional | Short Date | CheckDate | date |
| Earnings Code | Required | Text | EarningsCode | varchar(50) |
| Earnings Description | Required | Text | EarningsCodeDesc | varchar(120) |
| Hours | Required | Number | Hours | decimal(7,2) |
| Amount | Required | Number | AmountOrig + AmountClean | decimal(10,2) |

### General Ledger

Target staging table: `#staging_gl` (9 columns)

| Field Name | Required | Data Format | Staging Column | SQL Type |
|---|---|---|---|---|
| Cost Center Number | Required | Text | CostCenterNumberOrig | varchar(50) |
| Cost Center Name | Required | Text | CostCenterNameOrig | varchar(120) |
| Report Date | Required | Short Date | YearMonth | int (YYYYMM) |
| Account # | Required | Text | AcctNumber | varchar(40) |
| Account Description | Required | Text | AcctDesc | varchar(250) |
| Account Type | Recommended | Text | — | — |
| Sub-Account Number | Optional | Text | SubAcctNumber | varchar(30) |
| Sub-Account Desc | Optional | Text | SubAcctDesc | varchar(250) |
| Amount | Required | Number | AmountOrig + AmountClean | decimal(14,2) |

### Quality

*No staging table defined in current staging structure. Quality data follows its own ingestion path.*

| Field Name | Required | Data Format |
|---|---|---|
| Provider NPI | Required | Text |
| Provider Name | Optional | Text |
| Measurement Period Start Date | Required | Short Date |
| Measurement Period End Date | Required | Short Date |
| Measure Number | Required | Text |
| Measure Description | Optional | Text |
| Is_Inverse | Required | Text |
| Initial Population | Optional | Number |
| Denominator | Required | Number |
| Exclusions/Exceptions | Required | Number |
| Numerator | Required | Number |
| Performance Rate | Required | Number |
| Benchmark Target | Optional | Number |

### Patient Satisfaction (Optional Source)

*No staging table defined in current staging structure. Patient Satisfaction data follows its own ingestion path.*

| Field Name | Required | Data Format |
|---|---|---|
| Provider NPI | Required | Text |
| Provider Name | Optional | Text |
| Survey Date Range Start | Required | Short Date |
| Survey Date Range End | Required | Short Date |
| Survey Question Full (limit 500 chars) | Required | Text |
| Survey Question Abbreviated (limit 100 chars) | Optional | Text |
| Question Order | Required | Number |
| Number of Respondents | Optional | Number |
| Score (Numeric, up to 2 decimals) | Required | Number |
| Standard Deviation (Up to 6 decimals) | Optional | Number |
| Benchmarking Filter | Optional | Text |
| Benchmark 1 | Optional | Number |
| Benchmark 2 | Optional | Number |

---

## 1.12 High-Volume Alias Examples

The mapping knowledge base contains extensive alias coverage for client systems that use non-standard column names. Below are representative examples showing the breadth of known aliases for key staging columns:

| Staging Column | # Known Aliases | Example Raw Names |
|---|---|---|
| RenderingProviderNpi | 26 | `Rendering Provider NPI`, `RDNPI`, `rndrng prvdr npi no`, `Rendering_Provider_NPI`, `Rendering_Prov_NPI`, `NPI`, `PERFORMING_PROVIDER_NPI`, `RenderingProviderNPI`, `RenderingPhysNPI`, `ServicingProvNPI`, `Rendering NPI Number`, `RENDERING_PR_NPI`, `RendProvNpi`, `RVU_PROVIDER_NPI` |
| CptCode | 19 | `CPT-4 Code`, `CPT_4_CODE`, `CPT4Code`, `CPT4`, `cpt4_cd`, `proccode`, `BillingProcedureCode`, `Procedure CPT - 5`, `PPCHCPCSID`, `CptCode`, `CPT Code`, `CPT_CODE`, `CPT-4Code` |
| PatientId | 28 | `Patient MRN/Identifier`, `PATIENT_MRN`, `Patient_Identifier`, `PrimaryMrn`, `enterpriseid`, `ACTPAT`, `Patient`, `INT_PAT_ID`, `Patient Account No`, `MedicalRecord`, `Patient Enterprise ID`, `PATIENT_ENTERPRISE_ID` |
| ApptProvFullNameOrig | 18 | `Appt Provider Full Name`, `Appt_Provider_Full_Name`, `AppointmentProviderFullName`, `appt schdlng prvdrfullnme`, `Sched Provider Name`, `Scheduling Provider`, `ProviderName`, `Doctor - SB` |
| EmployeeId | ~25 | `Employee ID`, `EMP ID`, `EMPLOYEE`, `EmployeeID`, `EMPLID`, `Employee_ID`, `EE ID`, `EEID`, `empl_id`, `File#`, `Employee #`, `EmployeeNo`, `EmployeeCode`, `WD ID`, `Person`, `EmpID`, `PERSON_ID` |

---

## 1.13 Quick Reference: Phase 1 Checklist

- [ ] All uploaded files inventoried with name, extension, and size
- [ ] Each file parsed successfully (no delimiter or encoding failures)
- [ ] Each file matched to a PIVOT data source and target staging table identified
- [ ] Billing format determined: Combined (`#staging_billing`) or Separate (`#staging_charges` + `#staging_transactions`)
- [ ] Record counts and column counts captured
- [ ] Raw-to-staging column mapping executed for each file (exact → normalized → fuzzy → semantic)
- [ ] Mapping confidence levels assigned (EXACT / NORMALIZED / FUZZY / UNMAPPED)
- [ ] Dual-mapping columns identified (Original + Clean pairs)
- [ ] Staging data type constraints documented for Phase 2 validation
- [ ] Unrecognized columns flagged with context for client discussion
- [ ] Test month identified from each file's filter date field
- [ ] Test month alignment confirmed across all core files
- [ ] Missing expected sources identified and noted
- [ ] Sample rows displayed for visual spot-check
- [ ] Submission metadata recorded (client name, round, date)
- [ ] All findings passed to Phase 2 for schema and compatibility validation