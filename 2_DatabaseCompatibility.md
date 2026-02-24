Phase 2: Database Compatibility Check

Schema validation (required fields present?)
Field name recognition against specification
Data type verification (dates, NPIs, amounts, codes)
Classification of fields (Required/Recommended/Optional)
Flag missing fields by severity (CRITICAL/HIGH/Info)
Identify unexpected/unrecognized columns

# Phase 2 Implementation Plan — Database Compatibility Check

## Overview

Phase 2 consumes the `phase1_findings.json` manifest produced by Phase 1 and performs deep schema validation, field classification, and data type verification for every ingested file. It answers the core question: *Can this data be loaded into the PIVOT staging tables without structural failures?*

**Inputs:** `phase1_findings.json` + parsed DataFrames (re-loaded from source files using Phase 1 metadata)
**Outputs:** Per-file compatibility findings appended to `phase2_findings.json`; Excel report sheets; console summary
**Reference data:** `StagingTableStructure.xlsx`, `RawToStagingColumnMapping.xlsx`, PIVOT Data Extract Templates (FY26)
**Libraries:** `pandas`, `openpyxl`, `re`, `datetime`

---

## Project Structure (additions to Phase 1 tree)

```
TestFileReviewAutomation/
├── phase2/
│   ├── __init__.py
│   ├── schema_validator.py         # Required/Recommended/Optional field checks
│   ├── field_classifier.py         # Classifies each mapped column by requirement level
│   ├── datatype_checker.py         # SQL type, length, format, pattern validation
│   ├── unrecognized_columns.py     # Flags unexpected columns not in mapping table
│   └── report.py                   # Console display + Excel/JSON writer for Phase 2
├── shared/
│   ├── __init__.py
│   ├── constants.py                # REQUIRED/RECOMMENDED/OPTIONAL field definitions per source
│   ├── staging_meta.py             # Loads StagingTableStructure.xlsx once; exposes type/length info
│   └── loader.py                   # Re-loads DataFrames from source files using Phase 1 metadata
├── run_phase2.py                   # CLI orchestrator (or extend run_phase1.py with --phase flag)
└── ...
```

---

## Authoritative Field Requirement Definitions

The PIVOT Data Extract Templates (FY26) are the single source of truth for which fields are Required, Recommended, or Optional. The definitions below are extracted directly from those templates. Phase 2 code will embed these as frozen dictionaries in `shared/constants.py`.

### Billing — Combined (`billing_combined` → `#staging_billing`)

| Level | Field Name (as in template) |
|---|---|
| **Required** | Date of Service, Post Date, CPT-4 Code, CPT Code Modifier 1, CPT Code Modifier 2, CPT Code Modifier 3, CPT Code Modifier 4, Units, Transaction Type, Transaction Description, Amount, Work RVUs, CMS Place of Service Code, Primary ICD-10 CM Code, Secondary ICD-10 CM Code, Third ICD-10 CM Code, Fourth ICD-10 CM Code, 5th through 25th ICD-10 CM Code, Patient MRN/Identifier, Patient DOB, Patient Gender, Patient City, Patient ZIP Code, Rendering Provider Full Name, Rendering Provider NPI, Rendering Provider's Primary Specialty, Rendering Provider Credentials, Billing Provider Full Name, Billing Provider NPI, Billing Provider's Primary Specialty, Billing Provider Credentials, Practice Name, Billing Location Name, Department Name, Cost Center*, Primary Payer Name, Primary Payer Plan, Primary Payer Financial Class, Charge ID, Invoice Number / Encounter ID |
| **Recommended** | CPT Code Description, Rendering Provider First Name, Rendering Provider Middle Name/Initial, Rendering Provider Last Name, Rendering Provider ID, Billing Provider First Name, Billing Provider Middle Name/Initial, Billing Provider Last Name, Billing Provider ID, Referring Provider ID |
| **Optional** | Last Modified Date, Primary ICD-10 CM Code Description, Secondary ICD-10 CM Code Description, Third ICD-10 CM Code Description, Fourth ICD-10 CM Code Description, 5th through 25th ICD-10 CM Description, Patient Race/Ethnicity, Patient Marital Status, Referring Provider First Name, Referring Provider Middle Name/Initial, Referring Provider Last Name, Referring Provider Full Name, Referring Provider NPI, Referring Provider's Primary Specialty, Referring Provider Credentials |

*\*Cost Center is "Required — if not available in Billing data, a crosswalk to GL will need to be provided." Phase 2 flags this as CRITICAL only if Cost Center AND all three org fields (Practice Name, Billing Location Name, Department Name) are missing.*

### Billing — Separate Charges (`billing_charges` → `#staging_charges`)

Same as Combined above, **except**:
- **Remove from Required:** Transaction Type, Transaction Description, Amount
- **Add to Required:** Charge Amount

### Billing — Separate Transactions (`billing_transactions` → `#staging_transactions`)

| Level | Field Name |
|---|---|
| **Required** | Transaction ID, Transaction Description, Post Date, Payment Amount, Adjustment Amount, Refund Amount, Payer Name, Payer Plan, Payer Financial Class, Charge ID, Invoice Number / Encounter ID |
| **Optional** | Last Modified Date, Reason Category, Claim Adjudication Reason Code, Claim Adjudication Reason Description, Other Reason Detail |

### Scheduling (`scheduling` → `#staging_scheduling`)

| Level | Field Name |
|---|---|
| **Required** | Appt ID, Location Name*, Appt Provider Full Name, Appt Provider NPI, Patient Identifier, Appt Type, Created Date, Appt Date, Cancel Date, Cancel Reason, Appt Time, Scheduled Length, Appt Status |
| **Recommended** | Practice Name*, Department Name*, Cost Center, Appt Provider First Name, Appt Provider Middle Name, Appt Provider Last Name, Appt Provider ID, Referring Provider ID, Check In Date, Check In Time, Check Out Date, Check Out Time |
| **Optional** | Appt Provider Credentials, Appt Provider Primary Specialty, Referring Provider Full Name, Referring Provider First Name, Referring Provider Middle Name, Referring Provider Last Name, Referring Provider Credentials, Referring Provider NPI, Referring Provider Primary Specialty |

*\*Location Name is Required. Practice Name and Department Name are Recommended — one or two of these three must tie back to Billing or GL cost centers.*

### Payroll (`payroll` → `#staging_payroll`)

| Level | Field Name |
|---|---|
| **Required** | Employee ID, Employee Full Name, Job Code ID, Job Code Description, Department ID*, Pay Period Start Date, Pay Period End Date, Earnings Code, Earnings Description, Hours, Amount |
| **Recommended** | Provider ID, Employee First Name, Employee Middle Name, Employee Last Name, Employee NPI, Department Name* |
| **Optional** | Check/Pay Date |

*\*Department ID is "Required — needs to tie back to a GL cost center." Department Name is Recommended.*

### General Ledger (`gl` → `#staging_gl`)

| Level | Field Name |
|---|---|
| **Required** | Cost Center Number, Cost Center Name, Report Date, Account #, Account Description, Amount |
| **Recommended** | Account Type |
| **Optional** | Sub-Account Number, Sub-Account Desc |

### Quality (`quality` — no staging table; loaded directly)

| Level | Field Name |
|---|---|
| **Required** | Provider NPI, Measurement Period Start Date, Measurement Period End Date, Measure Number, Is_Inverse, Denominator, Exclusions/Exceptions, Numerator, Performance Rate |
| **Optional** | Provider Name, Measure Description, Initial Population, Benchmark Target |

### Patient Satisfaction (`patient_satisfaction` — no staging table; loaded directly)

| Level | Field Name |
|---|---|
| **Required** | Provider NPI, Survey Date Range Start, Survey Date Range End, Survey Question Full, Question Order, Score |
| **Optional** | Provider Name, Survey Question Abbreviated, Number of Respondents, Standard Deviation, Benchmarking Filter, Benchmark 1, Benchmark 2 |

---

## Module Specs

### `shared/constants.py`

**Purpose:** Single source of truth for field requirement classifications and data type expectations.

**Contents:**

1. **`FIELD_REQUIREMENTS`** — nested dict keyed by source name → requirement level → list of template field names:
   ```python
   FIELD_REQUIREMENTS = {
       "billing_combined": {
           "required": ["Date of Service", "Post Date", "CPT-4 Code", ...],
           "recommended": ["CPT Code Description", ...],
           "optional": ["Last Modified Date", ...]
       },
       "billing_charges": { ... },
       "billing_transactions": { ... },
       "scheduling": { ... },
       "payroll": { ... },
       "gl": { ... },
       "quality": { ... },
       "patient_satisfaction": { ... }
   }
   ```

2. **`TEMPLATE_TO_STAGING`** — maps each template field name to its staging column name(s). Built by joining template field names against `RawToStagingColumnMapping.xlsx` (template field names are a subset of raw column aliases). This allows Phase 2 to reason about requirement levels in terms of staging columns:
   ```python
   TEMPLATE_TO_STAGING = {
       ("billing_charges", "Date of Service"): "DateOfService",
       ("billing_charges", "Post Date"): "PostDate",
       ("billing_charges", "Rendering Provider NPI"): "ProvNpi",  # Rendering context
       ...
   }
   ```

3. **`STAGING_COL_TYPES`** — loaded from `StagingTableStructure.xlsx`, keyed by `(staging_table, column_name)`:
   ```python
   STAGING_COL_TYPES = {
       ("#staging_charges", "ChrgId"): {"type": "varchar", "max_length": 150, "precision": 0, "scale": 0},
       ("#staging_charges", "DateOfService"): {"type": "date", "max_length": 3, "precision": 10, "scale": 0},
       ("#staging_charges", "Units"): {"type": "int", "max_length": 4, "precision": 10, "scale": 0},
       ("#staging_charges", "ChargeAmtOrig"): {"type": "decimal", "max_length": 9, "precision": 14, "scale": 2},
       ...
   }
   ```

4. **`DATA_FORMAT_PATTERNS`** — regex patterns and validation rules for domain-specific fields:
   ```python
   DATA_FORMAT_PATTERNS = {
       "npi": {"pattern": r"^\d{10}$", "description": "10-digit numeric"},
       "cpt_code": {"pattern": r"^\d{5}$|^[A-Z]\d{4}$", "description": "5-char alphanumeric (99213 or T1015)"},
       "icd10": {"pattern": r"^[A-TV-Z]\d{2,4}\.?\d{0,4}$", "description": "Letter + digits, optional decimal"},
       "pos_code": {"pattern": r"^\d{1,2}$", "description": "1-2 digit numeric"},
       "zip_code": {"pattern": r"^\d{5}(-\d{4})?$", "description": "5 or 9-digit ZIP"},
       "yearmonth": {"pattern": r"^\d{6}$", "description": "YYYYMM integer"},
   }
   ```

5. **`DOMAIN_FIELD_PATTERNS`** — maps staging column names to their applicable `DATA_FORMAT_PATTERNS` key:
   ```python
   DOMAIN_FIELD_PATTERNS = {
       "ProvNpi": "npi",
       "PayEmpNpi": "npi",
       "CptCodeOrig": "cpt_code",
       "IcdCodeOrig": "icd10",
       "PosCode": "pos_code",
       "ZipOrig": "zip_code",
       "PatZipOrig": "zip_code",
       "BillLocZipOrig": "zip_code",
       "YearMonth": "yearmonth",
   }
   ```

---

### `shared/staging_meta.py`

**Purpose:** Load `StagingTableStructure.xlsx` once and provide lookup functions.

**Functions:**
- `get_column_type(staging_table, column_name) → dict` — returns `{"type", "max_length", "precision", "scale"}`
- `get_all_columns(staging_table) → list[dict]` — all columns for a staging table with their metadata
- `get_source_column_name(staging_table, column_name) → str` — returns the `Source_Column` (human-readable name) from StagingTableStructure for reporting purposes

---

### `shared/loader.py`

**Purpose:** Re-load source file DataFrames using Phase 1 metadata.

Phase 1 already parsed all files. To avoid re-parsing, `loader.py` reads `phase1_findings.json` for file paths, delimiters, encodings, and re-loads each file into a DataFrame. This keeps Phase 2 decoupled from Phase 1's in-memory state so the phases can run independently.

**Function:**
```python
def load_files(phase1_json_path: str, input_base_dir: str) → dict[str, dict]:
    """
    Returns {filename: {"df": DataFrame, "source": str, "staging_table": str, "column_mappings": list}}
    """
```

---

### `phase2/schema_validator.py`

**Purpose:** Check whether each file contains the required, recommended, and optional fields for its data source. This is the core "are the right columns present?" check.

**Algorithm:**

1. For each file, retrieve:
   - The detected `source` from Phase 1 (e.g. `billing_charges`)
   - The `column_mappings` from Phase 1 (list of `{RawColumn, StagingColumn, Confidence, ...}`)

2. Build the set of **covered staging columns** = all `StagingColumn` values from the mappings where `Confidence != "UNRECOGNIZED"`.

3. For each template field in `FIELD_REQUIREMENTS[source]`:
   - Look up the expected staging column(s) via `TEMPLATE_TO_STAGING[(source, field_name)]`
   - Check if the staging column is in the covered set
   - Classify the result:
     - **PRESENT** — staging column is covered
     - **MISSING (CRITICAL)** — field is Required and staging column is not covered
     - **MISSING (HIGH)** — field is Recommended and staging column is not covered
     - **MISSING (INFO)** — field is Optional and staging column is not covered

4. **Special handling — ICD-10 codes (5th–25th):** The template lists "5th through 25th ICD-10 CM Code" as a single required field, but clients may provide 0–21 individual columns (e.g. `5th ICD-10 CM Code`, `6th ICD-10 CM Code`, ... `25th ICD-10 CM Code`). Phase 2 checks if *at least one* column beyond the 4th ICD-10 is present. If zero are present, flag as CRITICAL. If some but not all 21 are present, flag as INFO noting the count found.

5. **Special handling — Cost Center (Billing):** Cost Center is conditionally required. If `Cost Center` is missing, check whether Practice Name, Billing Location Name, and Department Name are all present. If at least one org field is present → flag Cost Center as HIGH with note: "Crosswalk to GL required." If all org fields AND Cost Center are missing → CRITICAL.

6. **Special handling — Charge ID / Invoice Number (Billing):** Both are listed as Required, but in practice at least one must be present (the other may be blank). Flag as CRITICAL only if *neither* is mapped.

**Returns per file:**
```python
{
    "schema_findings": [
        {
            "template_field": "Rendering Provider NPI",
            "staging_column": "ProvNpi",
            "requirement_level": "required",
            "status": "PRESENT",  # or "MISSING"
            "severity": None,     # or "CRITICAL" / "HIGH" / "INFO"
            "raw_column_matched": "Provider NPI",  # from Phase 1 mapping
            "confidence": "EXACT",
            "notes": ""
        },
        ...
    ],
    "summary": {
        "required_total": 40,
        "required_present": 38,
        "required_missing": 2,
        "recommended_total": 12,
        "recommended_present": 8,
        "recommended_missing": 4,
        "optional_total": 15,
        "optional_present": 5,
        "optional_missing": 10
    }
}
```

---

### `phase2/field_classifier.py`

**Purpose:** For every mapped column in the file (not just template-defined fields), classify it into one of four buckets:

1. **Required** — column maps to a staging column that corresponds to a Required template field
2. **Recommended** — maps to a Recommended template field
3. **Optional** — maps to an Optional template field
4. **Unclassified** — column is mapped to a staging column but does not appear in the template field lists (e.g. `WorkRvuCustom`, `ContractAllowableOrig`). These are valid staging columns that accept data but aren't explicitly listed in the client-facing template.

This module enriches Phase 1's column mapping output with a `RequirementLevel` column. This is used downstream in reports so that every column in the file has a clear classification.

**Returns per file:** The Phase 1 `column_mappings` list, each entry augmented with:
```python
{"RequirementLevel": "Required" | "Recommended" | "Optional" | "Unclassified"}
```

---

### `phase2/datatype_checker.py`

**Purpose:** Validate that the actual data values in each column are compatible with the target staging column's SQL type, length, and format constraints.

**Checks performed per mapped column:**

#### 1. SQL Type Compatibility

For each mapped column, look up the target type from `STAGING_COL_TYPES`:

| Staging Type | Validation Rule |
|---|---|
| `varchar` | Values must be castable to string. Check `max_length` — flag values exceeding it. |
| `date` | Values must be parseable as dates. Detect format (YYYY-MM-DD, MM/DD/YYYY, M/D/YY, etc.). Flag unparseable values. Flag inconsistent formats within same column. |
| `int` | Values must be whole numbers (no decimals). Flag non-numeric values. Flag values outside ±2^31. |
| `decimal(p,s)` | Values must be numeric. Check precision (total digits) and scale (decimal places). Flag values exceeding precision. |
| `time` | Values must be parseable as time (HH:MM, HH:MM:SS, H:MM AM/PM). Flag invalid time values. |
| `numeric(p,s)` | Same as `decimal(p,s)`. |

**Implementation:**
```python
def check_column_type(series: pd.Series, staging_type: str, max_length: int, 
                      precision: int, scale: int) → dict:
    """
    Returns {
        "type_compatible": bool,
        "invalid_count": int,
        "invalid_sample": list[str],     # up to 5 sample bad values
        "invalid_rows": list[int],       # row indices of first 20 bad values
        "max_observed_length": int,       # for varchar
        "length_exceeded_count": int,     # for varchar
        "date_format_detected": str,      # for date columns
        "date_format_inconsistent": bool,
        "precision_exceeded_count": int,  # for decimal
        "notes": str
    }
    ```

#### 2. Domain-Specific Format Validation

After generic type checking, apply domain-specific pattern checks using `DOMAIN_FIELD_PATTERNS`:

| Staging Column | Check |
|---|---|
| `ProvNpi`, `PayEmpNpi` | Must be exactly 10 digits. Flag: alpha characters, truncated (9 digits), padded (11+), leading zeros stripped. |
| `CptCodeOrig` | Must be 5 characters: 5 digits (e.g. `99213`) or 1 letter + 4 digits (e.g. `T1015`). Flag: embedded modifiers (e.g. `99213-25`), truncated codes, descriptions in code field. |
| `IcdCodeOrig` | Must start with a letter followed by digits, optional decimal. Flag: missing leading letter, old ICD-9 format (3-digit numeric), descriptions in code field. |
| `PosCode` | Must be 1–2 digit numeric code. Flag: text descriptions instead of codes, values > 99. |
| `Modifier1`–`Modifier4` | Should be 2-character codes or blank. Flag: modifiers concatenated with CPT code, multiple modifiers in one field. |
| `ZipOrig`, `PatZipOrig`, `BillLocZipOrig` | 5-digit or 9-digit (ZIP+4) format. Flag: partial ZIPs, non-numeric, international postal codes. |
| `YearMonth` | Must be YYYYMM integer format (e.g. `202601`). Flag: date strings, MM/YYYY, other formats. |
| `GenderOrig` | Should be M/F/U/Male/Female/Unknown or similar. Flag: unexpected coded values, numeric codes without reference. |
| `PatAge` | Should be numeric or parseable age. Flag: dates of birth in age field, negative values, ages > 120. |

#### 3. Varchar Length Truncation Check

For every `varchar` staging column, compare actual max string length in the data against `max_length` from `StagingTableStructure.xlsx`. If any values exceed the limit:
- Count of rows that would be truncated
- Sample values that exceed the limit
- Severity: **MEDIUM** if < 5% of rows affected, **HIGH** if ≥ 5%

#### 4. Null/Blank in Required Fields (Pre-check)

While iterating columns for type checks, also count null/blank values in columns classified as Required by `field_classifier.py`. This overlaps with Phase 3's full data quality review but is flagged here as a structural concern:
- Severity: **CRITICAL** if a Required column is > 50% null
- Severity: **HIGH** if a Required column is > 0% but ≤ 50% null
- Note: This is a lightweight pre-check. Phase 3 performs row-level null analysis.

**Returns per file:**
```python
{
    "datatype_findings": [
        {
            "raw_column": "Provider NPI",
            "staging_column": "ProvNpi",
            "staging_type": "varchar",
            "max_length": 20,
            "type_compatible": True,
            "domain_check": "npi",
            "domain_valid_pct": 94.2,
            "domain_invalid_count": 832,
            "domain_invalid_sample": ["123456789", "NPI00012345", "1234567890A"],
            "domain_invalid_rows": [4, 18, 22, ...],
            "length_exceeded_count": 0,
            "null_count": 0,
            "null_pct": 0.0,
            "severity": "HIGH",    # because domain pattern failed on >5% of rows
            "notes": "832 NPI values are not 10-digit numeric"
        },
        ...
    ]
}
```

---

### `phase2/unrecognized_columns.py`

**Purpose:** Flag columns in the source file that Phase 1 could not map to any staging column.

This module consumes Phase 1's column mapping output and identifies:

1. **UNRECOGNIZED columns** — raw columns with `Confidence == "UNRECOGNIZED"` (from Phase 1's `UNMAPPED` step).
   - Severity: **LOW** if column name appears to be a system/internal field (e.g. `ROW_ID`, `LAST_UPDATED_BY`, `EXTRACT_DATE`)
   - Severity: **MEDIUM** if column name resembles a PIVOT-relevant concept but didn't match any alias (potential mapping table gap)
   - Severity: **HIGH** if column name closely resembles a required staging column (near-miss — possible typo or naming variant not in mapping table)

2. **FUZZY-matched columns** — raw columns with `Confidence == "FUZZY"` should be surfaced as needing human review. They mapped successfully, but the match was probabilistic.

**Near-miss detection:** For UNRECOGNIZED columns, compute `rapidfuzz.fuzz.token_sort_ratio` against all Source_Column names in `StagingTableStructure.xlsx` for the relevant staging table. If any score is between 60–84 (below the Phase 1 fuzzy threshold of 85 but above noise):
- Flag as **MEDIUM** with note: `Possible match to '{SourceColumn}' (similarity: {score}%) — confirm with client`

**Returns per file:**
```python
{
    "unrecognized_findings": [
        {
            "raw_column": "CUSTOM_FIELD_1",
            "severity": "LOW",
            "nearest_staging_match": None,
            "nearest_score": 0,
            "notes": "No close match found — likely system/internal field"
        },
        {
            "raw_column": "Rending Provider NPI",
            "severity": "HIGH",
            "nearest_staging_match": "RenderingProviderNpi",
            "nearest_score": 82,
            "notes": "Possible typo — very close to required field 'Rendering Provider NPI'"
        }
    ],
    "fuzzy_review_list": [
        {
            "raw_column": "BillingProcedureCode",
            "mapped_to_staging": "CptCodeOrig",
            "confidence": "FUZZY (87%)",
            "notes": "Confirm this column contains CPT-4 codes"
        }
    ]
}
```

---

### `phase2/report.py`

**Purpose:** Render Phase 2 results to console and write to the Excel report and JSON manifest.

**Console output — per file:**

```
┌─────────────────────────────────────────────────────────────────┐
│ SCHEMA VALIDATION                                                │
├──────────────────────┬──────────────────────────────────────────┤
│ File Name            │ PIVOT_Billing_Epic_202601.txt            │
│ Source               │ Billing Charges (Separate)               │
│ Staging Table        │ #staging_charges                         │
├──────────────────────┼──────────────────────────────────────────┤
│ Required fields      │ 38 / 40 present                         │
│ Recommended fields   │ 8 / 12 present                          │
│ Optional fields      │ 5 / 15 present                          │
├──────────────────────┴──────────────────────────────────────────┤
│ CRITICAL (2)                                                     │
│  ✗ Rendering Provider NPI — Required, MISSING                   │
│  ✗ Cost Center — Required, MISSING (no org crosswalk available) │
│ HIGH (4)                                                         │
│  ⚠ Rendering Provider ID — Recommended, MISSING                │
│  ⚠ Billing Provider ID — Recommended, MISSING                  │
│  ⚠ Billing Provider First Name — Recommended, MISSING          │
│  ⚠ Billing Provider Middle Name/Initial — Recommended, MISSING │
├──────────────────────────────────────────────────────────────────┤
│ DATA TYPE ISSUES (3)                                             │
│  ✗ ProvNpi — 832 values not 10-digit numeric (5.8%)             │
│  ⚠ DateOfService — Inconsistent date formats detected           │
│  ⚠ CptCodeOrig — 47 values contain embedded modifiers           │
├──────────────────────────────────────────────────────────────────┤
│ UNRECOGNIZED COLUMNS (3)                                         │
│  ⚠ Rending Provider NPI — Near-miss to 'Rendering Provider NPI' │
│  ○ CUSTOM_FIELD_1 — No close match                              │
│  ○ EXTRACT_DATE — No close match (likely system field)          │
├──────────────────────────────────────────────────────────────────┤
│ FUZZY MATCHES NEEDING REVIEW (1)                                 │
│  ? BillingProcedureCode → CptCodeOrig (87%) — confirm mapping   │
└─────────────────────────────────────────────────────────────────┘
```

**Console output — overall compatibility summary:**

```
┌─────────────────────────────────────────────────────────────────┐
│ DATABASE COMPATIBILITY SUMMARY                                   │
├───────────────────────────┬──────┬──────┬────────┬─────────────┤
│ File                      │ CRIT │ HIGH │ MEDIUM │ Compatible? │
├───────────────────────────┼──────┼──────┼────────┼─────────────┤
│ Billing_Charges.txt       │  2   │  4   │  1     │  NO         │
│ Billing_Transactions.txt  │  0   │  1   │  0     │  YES*       │
│ Scheduling.txt            │  0   │  0   │  2     │  YES        │
│ Payroll.txt               │  1   │  2   │  0     │  NO         │
│ GL.txt                    │  0   │  0   │  0     │  YES        │
├───────────────────────────┴──────┴──────┴────────┴─────────────┤
│ * = Conditionally compatible (HIGH issues present)              │
│ Overall: NOT READY — 2 files have CRITICAL issues               │
└─────────────────────────────────────────────────────────────────┘
```

**Compatibility determination logic:**
- **YES** — 0 CRITICAL issues, 0 HIGH issues
- **YES*** — 0 CRITICAL issues, ≥1 HIGH issue (conditionally compatible; may need client clarification)
- **NO** — ≥1 CRITICAL issue

**Excel report** — appended to the Phase 1 workbook or new file `{client}_{round}_Phase2_{YYYYMMDD}.xlsx` — additional sheets:

| Sheet | Contents |
|---|---|
| `Schema Validation` | One row per template field per file: field name, staging column, requirement level, status (PRESENT/MISSING), severity, matched raw column, confidence, notes |
| `Schema Summary` | One row per file: required present/missing counts, recommended present/missing counts, optional present/missing counts |
| `Data Type Checks` | One row per mapped column per file: raw column, staging column, staging type, max length, type compatible, domain check, invalid count, invalid %, sample bad values, severity, notes |
| `Unrecognized Columns` | One row per unrecognized/fuzzy column per file: raw column, severity, nearest match, score, notes |
| `Compatibility Summary` | One row per file: CRITICAL count, HIGH count, MEDIUM count, compatible status, overall assessment |

**JSON manifest** — `phase2_findings.json` — consumed by Phase 3:
```json
{
    "client": "ClientName",
    "round": "v1",
    "date_run": "YYYY-MM-DD",
    "overall_compatible": false,
    "files": {
        "PIVOT_BillingCharges_Epic_202601.txt": {
            "source": "billing_charges",
            "staging_table": "#staging_charges",
            "compatible": false,
            "critical_count": 2,
            "high_count": 4,
            "medium_count": 1,
            "schema_findings": [...],
            "datatype_findings": [...],
            "unrecognized_findings": [...],
            "fuzzy_review_list": [...]
        }
    }
}
```

---

### `run_phase2.py`

**Purpose:** CLI entry point for Phase 2.

**Usage:**
```
py run_phase2.py --input ./input --output ./output --client "ClientName" --round v1
```

**Execution order:**
1. Load `phase1_findings.json` from `output/{client}/`
2. `loader.load_files(phase1_json, input_dir)` → re-load DataFrames
3. Load reference data into `shared/constants.py` and `shared/staging_meta.py`
4. For each file:
   a. `schema_validator.validate(file_data, source)` → schema findings
   b. `field_classifier.classify(file_data, source)` → enriched column mappings
   c. `datatype_checker.check(file_data, source, staging_table)` → type findings
   d. `unrecognized_columns.flag(file_data, source, staging_table)` → unrecognized findings
5. `report.render(all_findings, output_dir, client, round)` → console + Excel + JSON

---

## Key Implementation Notes

- **Phase 1 dependency** — Phase 2 requires `phase1_findings.json` to exist. If not found, print an error and exit with instructions to run Phase 1 first.
- **Template field names vs. staging column names** — The `TEMPLATE_TO_STAGING` mapping bridges the gap. Template field names are what the client sees; staging column names are what the database expects. Phase 2 reasons about both: it validates against the client-facing template (schema_validator) and against the database schema (datatype_checker).
- **Duplicate staging column names** — `StagingTableStructure.xlsx` has repeated column names within a table (e.g. `ProvSpecialty` appears 3 times in `#staging_charges` for Rendering/Billing/Referring providers, `ProvNpi` appears 3 times, etc.). The `Source_Column` field (e.g. `RenderingProviderNpi`, `BillingProviderNpi`) disambiguates. Phase 2 uses `Source_Column` as the canonical key for type lookups when column names repeat.
- **Quality and Patient Satisfaction** — These sources do not have staging tables in `StagingTableStructure.xlsx`. For data type checks, `datatype_checker.py` applies domain-specific patterns (NPI, date, numeric) directly from `DATA_FORMAT_PATTERNS` without a staging table lookup. Schema validation still uses `FIELD_REQUIREMENTS`.
- **Performance** — Domain-specific pattern checks use vectorized `Series.str.match()` with pre-compiled regex for speed on large files.
- **Severity escalation** — If a datatype issue affects a Required field, the severity is escalated by one level (MEDIUM→HIGH, HIGH→CRITICAL). This ensures that type problems in critical fields are surfaced prominently.
- **Combined billing payer disambiguation** — In `#staging_billing`, payer fields appear twice (charge-level: `ChargePayerName` → `PayerName` and transaction-level: `TransactionPayerName` → `PayerName`). Phase 2's type checking runs once per unique raw column; the dual-mapping is noted but not double-counted as an error.

---

## Verification Checklist

After implementation, validate with a real or synthetic test batch:

- [ ] Run `py run_phase2.py --client "Test" --round v1` after a successful Phase 1 run
- [ ] Console prints one schema validation box per file
- [ ] Console prints overall compatibility summary table
- [ ] A file missing `Rendering Provider NPI` shows as `CRITICAL` in schema findings
- [ ] A file missing `Rendering Provider ID` (Recommended) shows as `HIGH`
- [ ] A file missing `Patient Race/Ethnicity` (Optional) shows as `INFO`
- [ ] Cost Center missing + all org fields missing → `CRITICAL`; Cost Center missing + Department Name present → `HIGH` with crosswalk note
- [ ] NPI column with 9-digit values flagged by domain check with count and sample
- [ ] CPT code column with embedded modifiers (e.g. `99213-25`) flagged
- [ ] ICD-10 column with old ICD-9 codes (e.g. `250.00`) flagged
- [ ] Date column with mixed formats (some MM/DD/YYYY, some YYYY-MM-DD) flagged as inconsistent
- [ ] Varchar column with values exceeding max_length → truncation warning with count
- [ ] Required field that is >50% null → `CRITICAL` null pre-check
- [ ] Unrecognized column `Rending Provider NPI` → near-miss to `RenderingProviderNpi` at severity HIGH
- [ ] Fuzzy-matched column surfaces in review list with confidence score
- [ ] `phase2_findings.json` is valid JSON with all expected keys
- [ ] Excel report contains all 5 Phase 2 sheets with correct data
- [ ] Quality file validates NPI and Performance Rate patterns correctly despite no staging table
- [ ] File with 0 CRITICAL issues and 0 HIGH issues → `YES` compatible
- [ ] File with 0 CRITICAL issues and ≥1 HIGH issue → `YES*` conditionally compatible
- [ ] File with ≥1 CRITICAL issue → `NO` not compatible