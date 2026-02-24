# Phase 1 Implementation Plan — Initial Setup & Data Ingestion

## Overview

Phase 1 is the entry point for PIVOT Test File Review automation. It scans a local input directory, parses every client-submitted file, identifies each data source, determines billing format, maps raw columns to staging columns, and confirms all files cover the same test month. This plan describes the Python code to be built — no execution happens until this plan is approved.

**Reference spec:** `1_SetupDataIngestion.md`
**Reference data:** `RawToStagingColumnMapping.xlsx` (3,520 mappings), `StagingTableStructure.xlsx`
**Libraries:** `pandas`, `openpyxl`, `rapidfuzz`, `chardet`

---

## Project Structure

```
TestFileReviewAutomation/
├── input/
│   └── {ClientName}/               # One subfolder per client (created by user)
│       ├── billing_charges/        # Optional source subfolders — name sets source automatically
│       ├── billing_transactions/
│       ├── billing_combined/
│       ├── scheduling/
│       ├── payroll/
│       ├── gl/
│       └── sources.csv             # Optional manual overrides (per-client)
├── output/
│   └── {ClientName}/               # Created automatically on first run
│       ├── {Client}_{round}_Phase1_{YYYYMMDD}.xlsx
│       └── phase1_findings.json
├── phase1/
│   ├── __init__.py
│   ├── ingestion.py                # File scanning, parsing, metadata
│   ├── source_detection.py         # Column fingerprinting → source ID
│   ├── billing_format.py           # Combined vs. Separate detection
│   ├── column_mapping.py           # 4-step raw-to-staging mapping
│   ├── test_month.py               # Date extraction, alignment check
│   └── report.py                   # Console display + Excel/JSON writer
├── run_phase1.py                   # CLI orchestrator
└── requirements.txt
```

---

## Module Specs

### `phase1/ingestion.py`

**Purpose:** Parse every file in the input directory into a DataFrame.

**Steps:**
1. Scan `input/` for all `.txt` and `.csv` files
2. Detect file encoding with `chardet`
3. Attempt pipe-delimited parse first; fall back to comma, then tab if pipe yields only 1 column
4. Strip leading/trailing whitespace from all column names
5. Drop fully empty rows and columns
6. Detect and strip footer rows (trailing rows matching patterns like "Total", "Record Count", "EOF")
7. Flag embedded pipe characters within field values (would break re-parsing)
8. Flag encoding issues (garbled characters, non-UTF-8)

**Returns:** `dict[filename → {df, ext, raw_col_count, row_count, delimiter, encoding, parse_issues, footer_rows_stripped, source_folder}]`

`source_folder` is the name of the subdirectory the file was found in (e.g. `"billing_charges"`), or `None` for files at the top level of the client directory.

---

### `phase1/source_detection.py`

**Purpose:** Identify which PIVOT data source each file represents.

**Steps:**
1. Define fingerprint column sets per source (exact column names from spec §1.4):

   | Source | Distinctive Columns |
   |---|---|
   | Billing (Combined) | `Transaction Type`, `Transaction Description`, `Amount`, `CPT-4 Code`, `Work RVUs` |
   | Billing Charges (Separate) | `Charge Amount`, `CPT-4 Code`, `Work RVUs` *(no `Transaction Type`)* |
   | Billing Transactions (Separate) | `Transaction ID`, `Payment Amount`, `Adjustment Amount`, `Refund Amount` |
   | Scheduling | `Appt ID`, `Appt Date`, `Appt Status`, `Appt Type`, `Scheduled Length` |
   | Payroll | `Employee ID`, `Job Code ID`, `Earnings Code`, `Pay Period Start Date`, `Pay Period End Date` |
   | General Ledger | `Cost Center Number`, `Cost Center Name`, `Account #`, `Account Description` |
   | Quality | `Measure Number`, `Is_Inverse`, `Denominator`, `Numerator`, `Performance Rate` |
   | Patient Satisfaction | `Survey Date Range Start`, `Survey Date Range End`, `Survey Question Full`, `Question Order`, `Score` |

2. Check source assignment in priority order:
   - **Priority 1:** explicit `sources.csv` override for that filename
   - **Priority 2:** `source_folder` metadata — if the file was found in a subdirectory whose name matches a valid source type (e.g. `billing_charges/`), use that name directly. Files placed in named source subfolders are confirmed automatically without an interactive prompt; only files auto-detected via column fingerprinting (Priority 3) require confirmation.
   - **Priority 3:** column-fingerprint auto-detection (compare headers, minimum 2 hits required)
3. If two sources tie or no source clears the minimum threshold → flag as `unknown` for manual review

**Returns:** `dict[filename → source_name]`

Source name values: `billing_combined`, `billing_charges`, `billing_transactions`, `scheduling`, `payroll`, `gl`, `quality`, `patient_satisfaction`, `unknown`

---

### `phase1/billing_format.py`

**Purpose:** Determine whether the submission uses Combined or Separate billing.

**Detection logic (from spec §1.2):**

```
IF any file contains [Transaction Type, Transaction Description, Amount]
   AND contains charge-level fields [CPT-4 Code, Work RVUs]
   AND contains transaction-level amounts [Payment Amount, Adjustment Amount, Refund Amount]
   THEN → Combined Billing → #staging_billing

ELSE IF one file has [Charge Amount] and no transaction fields
   AND a second file has [Transaction ID, Payment Amount, Adjustment Amount, Refund Amount]
   THEN → Separate Billing → #staging_charges + #staging_transactions

ELSE → unknown → flag for manual review
```

**Returns:** `{"format": "combined"|"separate"|"unknown", "billing_files": [list of billing filenames]}`

---

### `phase1/column_mapping.py`

**Purpose:** Map every raw column header to its staging column using the mapping knowledge base.

**Setup:**
- Load `RawToStagingColumnMapping.xlsx` once at startup; cache as a lookup dict
- Load `StagingTableStructure.xlsx` for SQL type/length/precision constraints per staging column
- Define a shared `normalize(s)` function: lowercase → strip spaces, underscores, hyphens, slashes, dots

**Mapping algorithm (4 steps, applied in order per raw column):**

1. **EXACT** — raw column name matches a `RawColumn` entry in the mapping table for this source → assign `StagingColumn`, confidence = `EXACT`

2. **NORMALIZED** — `normalize(raw_col)` matches `normalize(known_alias)` for any alias in this source → confidence = `NORMALIZED`

3. **FUZZY** — `rapidfuzz.fuzz.token_sort_ratio(raw_col, known_alias) ≥ 85` for any alias → confidence = `FUZZY (score%)`; flag for review

4. **UNMAPPED** — no match found → `UNRECOGNIZED`; flag for client discussion

**Dual-mapping:** Detect columns that intentionally map to two staging columns simultaneously. Legitimate dual-maps are limited to:
- Amount → `*Clean` + `*Orig` pairs (e.g. `Charge Amount` → `ChargeAmountOriginal` + `ChargeAmountClean`)
- Patient identifiers that serve as both PatientId and PatientMrn in a system
- Datetime fields split into Date + Time staging columns
- In `billing_combined`: Payer/Insurance fields that populate both Charge- and Transaction-payer staging columns

Cross-concept multi-maps (e.g. `Hours` → `AmountClean`, `EarningsCode` → `EarningsCodeDesc`) are not permitted and must be removed from `RawToStagingColumnMapping.xlsx` during mapping table maintenance. Record both staging columns in `Notes`.

**Org hierarchy auto-routing:** After the EXACT/NORMALIZED/FUZZY step resolves a raw column to an org `*Name` staging column (e.g. `BillDepartmentName`), the actual data values are inspected. If >70% of values look like numeric codes or short alphanumeric tokens (e.g. `1001`, `CC-001`, `LOC42`), the mapping is auto-rerouted to the `*Id` counterpart (e.g. `BillDepartmentId`) and a note is written: `AUTO-ROUTED to *Id: values appear to be codes/IDs`. The reverse applies if an `*Id` column contains descriptive text. Routing pairs:

| `*Name` column | `*Id` counterpart |
|---|---|
| `BillDepartmentName` | `BillDepartmentId` |
| `BillLocationName` | `BillLocationId` |
| `BillPracticeName` | `BillPracticeId` |
| `DeptNameOrig` | `DeptId` |
| `PracNameOrig` | `PracId` |
| `BillLocNameOrig` | `BillLocId` |

**Cost-center / dept-ID routing rule:** Raw column names containing `ID`, `Id`, `Number`, `#`, `Nbr`, `Num`, `Code`, or `CD` map to the `*Id` staging column. Names containing `Name`, `Desc`, or `Description` map to the `*Name` staging column. Ambiguous short names (`Cost Center`, `DEPT`, `Bill Area`, `Billing Area`) default to the `*Id` column; the runtime `_is_id_like()` check will auto-reroute to `*Name` if actual values are descriptive text.

In billing (`#staging_charges`, `#staging_billing`), cost-center and billing-area columns route to `BillDepartmentId` / `BillDepartmentName` — **never** to `BillPracticeName` or `BillLocationName`.
In scheduling (`#staging_scheduling`), cost-center columns route to `DeptId` / `DeptNameOrig` — **never** to `PracId` or `PracNameOrig`.

**Gap detection:** After mapping, identify staging columns that have no raw column mapping:
- `UNCOVERED (Required)` — required columns per `REQUIRED_STAGING_COLS`
- `UNCOVERED (Recommended)` — Phase 4 join key ID columns per `RECOMMENDED_STAGING_COLS`: `BillDepartmentId`, `BillLocationId`, `BillPracticeId` (charges/billing) and `BillLocId`, `DeptId`, `PracId` (scheduling). A missing recommended ID column signals that Phase 4 cross-source joins may fail.

**Returns per file:** DataFrame with columns:
`[RawColumn, StagingColumn, StagingTable, Confidence, SQLType, MaxLength, Precision, Scale, Notes]`

---

### `phase1/test_month.py`

**Purpose:** Identify the test month from each file and confirm all core files align.

**Filter date field per source (from spec §1.8):**

| Source | Filter Field (Staging Column Name) |
|---|---|
| Billing (Combined or Charges) | `PostDate` |
| Billing Transactions | `PostDate` |
| Scheduling | `ApptDate` |
| Payroll | `PayPeriodEndDate` |
| General Ledger | `YearMonth` |
| Quality | `MeasurementPeriodStartDate` / `MeasurementPeriodEndDate` |
| Patient Satisfaction | `SurveyDateRangeStart` / `SurveyDateRangeEnd` |

**Steps:**
1. Use the column mapping output to locate the filter date column in each DataFrame (map from staging col name back to the matched raw col name)
2. Parse the date column; extract min and max dates
3. Infer the implied calendar month from the date range
4. Compare implied months across all core files
5. Flag `MISALIGNED` if any core file's implied month differs from the consensus month

**Returns:**
```json
{
  "test_month": "YYYY-MM",
  "aligned": true,
  "per_file": {
    "filename.txt": {"min_date": "...", "max_date": "...", "implied_month": "YYYY-MM"}
  }
}
```

---

### `phase1/report.py`

**Purpose:** Render console output and write the Excel report and JSON manifest.

**Console output — per file (spec §1.7 format):**
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
│ EXACT match          │ 52 columns                               │
│ NORMALIZED match     │ 6 columns                                │
│ FUZZY match (review) │ 3 columns                                │
│ UNMAPPED             │ 3 columns — [CUSTOM_FIELD_1, ...]        │
│ Staging cols covered │ 61 of 80                                 │
│ Dual-map cols found  │ 4                                        │
│ Parse Issues         │ None                                     │
├──────────────────────┴──────────────────────────────────────────┤
│ SAMPLE DATA (first 3 rows)                                       │
│ ...                                                              │
└─────────────────────────────────────────────────────────────────┘
```

**Console output — test month alignment block (spec §1.8 format)**

**Excel report** — `output/{client}/{client}_{round}_Phase1_{YYYYMMDD}.xlsx` — 5 sheets:

| Sheet | Contents |
|---|---|
| `File Inventory` | One row per file: name, detected source, staging table, format, record count, column count, parse issues |
| `Column Mappings` | All raw→staging mappings across all files: confidence, SQL type, max length, precision, scale, notes |
| `Mapping Gaps` | UNMAPPED raw cols + UNCOVERED required/recommended staging cols + dual-map col list |
| `Test Month` | Per-file date ranges, implied months, alignment status, overall test month |
| `Submission Metadata` | Client name, submission round, date run, billing format, files present, files missing |

**JSON manifest** — `output/{client}/phase1_findings.json` — consumed by Phase 2 without re-parsing:
```json
{
  "client": "ClientName",
  "round": "v1",
  "date_run": "YYYY-MM-DD",
  "test_month": "YYYY-MM",
  "billing_format": "separate",
  "files": {
    "PIVOT_BillingCharges_Epic_202601.txt": {
      "source": "billing_charges",
      "staging_table": "#staging_charges",
      "row_count": 14327,
      "col_count": 64,
      "parse_issues": [],
      "column_mappings": [...],
      "unmapped_raw": [...],
      "uncovered_staging": [...]
    }
  }
}
```

---

### `run_phase1.py`

**Purpose:** CLI entry point; orchestrates all modules in sequence.

**Usage:**
```
py run_phase1.py --input ./input --output ./output --client "ClientName" --round v1
```

`--input` and `--output` are **base** directories. The `{client}` subfolder is appended automatically: files are read from `./input/ClientName/` and reports are written to `./output/ClientName/`.

**Execution order:**
1. `ingestion.ingest_directory(input_dir)` → parsed file dict
2. `source_detection.detect_sources(file_dict)` → source assignments
3. `billing_format.detect_billing_format(file_dict, source_assignments)` → billing format
4. `column_mapping.map_all_files(file_dict, source_assignments, ref_dir)` → mapping results
5. `test_month.identify_test_month(file_dict, mapping_results, source_assignments)` → alignment
6. `report.render(all_results, output_dir, client, round)` → console + Excel + JSON

---

## Key Implementation Notes

- **`normalize()` shared helper** — defined once in `column_mapping.py`, imported by `source_detection.py` for case/whitespace-insensitive fingerprint matching
- **Reference file loading** — load both `.xlsx` files once in `column_mapping.py` at import time; do not re-read per file
- **Phase 2 handoff** — all downstream phases consume `phase1_findings.json`; nothing in `input/` is re-read after Phase 1 completes
- **`argparse` is in the standard library** — no need to add it to `requirements.txt`
- **Org hierarchy single-mapping rule** — `RawToStagingColumnMapping.xlsx` has been cleaned so that every org-related raw column maps to exactly one staging column. Billing cost-center/dept columns route to `BillDepartmentId` or `BillDepartmentName` only (never `BillPracticeName` or `BillLocationName`). Scheduling cost-center/dept columns route to `DeptId` or `DeptNameOrig` only (never `PracId` or `PracNameOrig`). Ambiguous/numeric-ID raw columns default to the `*Id` staging column; name/description columns map to `*Name`. Runtime value inspection (`_is_id_like`) handles edge cases where actual data values disagree with the column name.
- **`#staging_billing` alias coverage** — `billing_combined` files use the `#staging_billing` staging table, which carries the full set of charge-level aliases (sourced from `#staging_charges`) and transaction-level aliases (sourced from `#staging_transactions`). No fallback logic is needed in code; the mapping table is the single source of truth.
- **Multi-map policy** — Only intentional dual-maps are permitted in `RawToStagingColumnMapping.xlsx` (Amount→Clean+Orig pairs, patient ID+MRN, datetime splits, combined-billing payer fields). Cross-concept multi-maps (Hours→Amount, Code→Desc, etc.) must be removed from the Excel during mapping table maintenance; they are not handled in code.

---

## Verification Checklist

After implementation, validate with a real or synthetic test batch:

- [ ] Run `pip install -r requirements.txt` successfully
- [ ] Create `input/Test/billing_charges/` and `input/Test/gl/`; place sample files inside; run `py run_phase1.py --client "Test" --round v1`
- [ ] Console prints one file summary box per file with correct source and record count
- [ ] Console prints test month alignment block
- [ ] `output/Test/` contains both the Excel report and `phase1_findings.json`
- [ ] Excel `File Inventory` sheet has one row per file
- [ ] Excel `Column Mappings` sheet: `CPT-4 Code` → `CptCode` with `EXACT` confidence
- [ ] Excel `Column Mappings` sheet: a fuzzy alias (e.g., `BillingProcedureCode`) shows `FUZZY (≥85%)`
- [ ] Excel `Mapping Gaps` sheet: an unrecognized column appears as `UNMAPPED`
- [ ] Excel `Mapping Gaps` sheet: `BillDepartmentId` / `BillLocationId` / `BillPracticeId` appear as `UNCOVERED (Recommended)` when no org ID columns are present in the file
- [ ] Billing file where a cost center column contains numeric codes: `Notes` column says `AUTO-ROUTED to *Id` and `StagingColumn` = `BillDepartmentId`
- [ ] Billing file where a cost center column contains text names: `StagingColumn` = `BillDepartmentName`, no auto-route note
- [ ] If a file covers a different month, `MISALIGNED` appears in console and `Test Month` sheet
- [ ] `phase1_findings.json` is valid JSON with all expected keys (`client`, `round`, `test_month`, `billing_format`, `files`)
- [ ] `billing_combined` file: `CPT-4 CODE` maps to `CptCode` with `NORMALIZED` confidence (not UNMAPPED)
- [ ] Files in named source subfolders auto-confirm without an interactive prompt; only fingerprint-detected files prompt
- [ ] `Hours` column in payroll maps to `Hours` staging column (not `AmountClean` or `AmountOrig`)
- [ ] `CREDIT COST CENTER` maps to `BillDepartmentId` (not `BillPracticeName`)
