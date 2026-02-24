# Phase 2 Implementation Plan — Database Compatibility Check

## Overview

Phase 2 consumes the `phase1_findings.json` manifest produced by Phase 1 and performs deep schema validation, field classification, and data type verification for every ingested file. It answers the core question: *Can this data be loaded into the PIVOT staging tables without structural failures?*

**Reference spec:** `2_DatabaseCompatibility.md`
**Reference data:** `StagingTableStructure.xlsx`, `RawToStagingColumnMapping.xlsx`, PIVOT Data Extract Templates (FY26)
**Libraries:** `pandas`, `openpyxl`, `re`, `datetime`, `rapidfuzz` (all already in `requirements.txt`)

**Inputs:** `output/{client}/phase1_findings.json` + source files in `input/{client}/`
**Outputs:** `output/{client}/{client}_{round}_Phase2_{YYYYMMDD}.xlsx`, `output/{client}/phase2_findings.json`

---

## Project Structure (additions to Phase 1 tree)

```
TestFileReviewAutomation/
├── phase2/
│   ├── __init__.py
│   ├── schema_validator.py         # Required/Recommended/Optional field presence checks
│   ├── field_classifier.py         # Classifies each mapped column by requirement level
│   ├── datatype_checker.py         # SQL type, length, format, pattern validation
│   ├── unrecognized_columns.py     # Near-miss detection for UNMAPPED columns
│   └── report.py                   # Console display + Excel/JSON writer for Phase 2
├── shared/
│   ├── __init__.py
│   ├── constants.py                # FIELD_REQUIREMENTS, TEMPLATE_TO_STAGING, DATA_FORMAT_PATTERNS
│   ├── staging_meta.py             # Loads StagingTableStructure.xlsx; exposes type/length lookups
│   └── loader.py                   # Re-loads DataFrames from source files using Phase 1 metadata
├── run_phase2.py                   # CLI orchestrator
└── ...
```

---

## Authoritative Field Requirement Definitions

Embedded as frozen dicts in `shared/constants.py`. Source: PIVOT Data Extract Templates (FY26).

### Billing — Combined (`billing_combined` → `#staging_billing`)

| Level | Fields |
|---|---|
| **Required** | Date of Service, Post Date, CPT-4 Code, CPT Code Modifier 1, CPT Code Modifier 2, CPT Code Modifier 3, CPT Code Modifier 4, Units, Transaction Type, Transaction Description, Amount, Work RVUs, CMS Place of Service Code, Primary ICD-10 CM Code, Secondary ICD-10 CM Code, Third ICD-10 CM Code, Fourth ICD-10 CM Code, 5th through 25th ICD-10 CM Code, Patient MRN/Identifier, Patient DOB, Patient Gender, Patient City, Patient ZIP Code, Rendering Provider Full Name, Rendering Provider NPI, Rendering Provider's Primary Specialty, Rendering Provider Credentials, Billing Provider Full Name, Billing Provider NPI, Billing Provider's Primary Specialty, Billing Provider Credentials, Practice Name, Billing Location Name, Department Name, Cost Center*, Primary Payer Name, Primary Payer Plan, Primary Payer Financial Class, Charge ID, Invoice Number / Encounter ID |
| **Recommended** | CPT Code Description, Rendering Provider First Name, Rendering Provider Middle Name/Initial, Rendering Provider Last Name, Rendering Provider ID, Billing Provider First Name, Billing Provider Middle Name/Initial, Billing Provider Last Name, Billing Provider ID, Referring Provider ID |
| **Optional** | Last Modified Date, Primary ICD-10 CM Code Description, Secondary ICD-10 CM Code Description, Third ICD-10 CM Code Description, Fourth ICD-10 CM Code Description, 5th through 25th ICD-10 CM Description, Patient Race/Ethnicity, Patient Marital Status, Referring Provider First Name, Referring Provider Middle Name/Initial, Referring Provider Last Name, Referring Provider Full Name, Referring Provider NPI, Referring Provider's Primary Specialty, Referring Provider Credentials |

*\*Cost Center conditionally required — see schema_validator special handling.*

### Billing — Separate Charges (`billing_charges` → `#staging_charges`)

Same as Combined, **except**:
- **Remove from Required:** Transaction Type, Transaction Description, Amount
- **Add to Required:** Charge Amount

### Billing — Separate Transactions (`billing_transactions` → `#staging_transactions`)

| Level | Fields |
|---|---|
| **Required** | Transaction ID, Transaction Description, Post Date, Payment Amount, Adjustment Amount, Refund Amount, Payer Name, Payer Plan, Payer Financial Class, Charge ID, Invoice Number / Encounter ID |
| **Optional** | Last Modified Date, Reason Category, Claim Adjudication Reason Code, Claim Adjudication Reason Description, Other Reason Detail |

### Scheduling (`scheduling` → `#staging_scheduling`)

| Level | Fields |
|---|---|
| **Required** | Appt ID, Location Name*, Appt Provider Full Name, Appt Provider NPI, Patient Identifier, Appt Type, Created Date, Appt Date, Cancel Date, Cancel Reason, Appt Time, Scheduled Length, Appt Status |
| **Recommended** | Practice Name*, Department Name*, Cost Center, Appt Provider First Name, Appt Provider Middle Name, Appt Provider Last Name, Appt Provider ID, Referring Provider ID, Check In Date, Check In Time, Check Out Date, Check Out Time |
| **Optional** | Appt Provider Credentials, Appt Provider Primary Specialty, Referring Provider Full Name, Referring Provider First Name, Referring Provider Middle Name, Referring Provider Last Name, Referring Provider Credentials, Referring Provider NPI, Referring Provider Primary Specialty |

*\*Location Name is Required. Practice Name and Department Name are Recommended — one or two of these three must tie back to Billing or GL cost centers.*

### Payroll (`payroll` → `#staging_payroll`)

| Level | Fields |
|---|---|
| **Required** | Employee ID, Employee Full Name, Job Code ID, Job Code Description, Department ID*, Pay Period Start Date, Pay Period End Date, Earnings Code, Earnings Description, Hours, Amount |
| **Recommended** | Provider ID, Employee First Name, Employee Middle Name, Employee Last Name, Employee NPI, Department Name* |
| **Optional** | Check/Pay Date |

*\*Department ID is "Required — needs to tie back to a GL cost center." Department Name is Recommended.*

### General Ledger (`gl` → `#staging_gl`)

| Level | Fields |
|---|---|
| **Required** | Cost Center Number, Cost Center Name, Report Date, Account #, Account Description, Amount |
| **Recommended** | Account Type |
| **Optional** | Sub-Account Number, Sub-Account Desc |

### Quality (`quality` — no staging table; loaded directly)

| Level | Fields |
|---|---|
| **Required** | Provider NPI, Measurement Period Start Date, Measurement Period End Date, Measure Number, Is_Inverse, Denominator, Exclusions/Exceptions, Numerator, Performance Rate |
| **Optional** | Provider Name, Measure Description, Initial Population, Benchmark Target |

### Patient Satisfaction (`patient_satisfaction` — no staging table; loaded directly)

| Level | Fields |
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

2. **`TEMPLATE_TO_STAGING`** — static dict mapping `(source, template_field_name)` → staging column name(s). Hand-curated by cross-referencing the PIVOT Data Extract Templates (FY26) against staging column names in `StagingTableStructure.xlsx`. NOT loaded dynamically from Excel. One-to-many mappings (e.g. "5th through 25th ICD-10 CM Code" → multiple `Icd*Code` columns) are represented as lists:
   ```python
   TEMPLATE_TO_STAGING = {
       ("billing_charges", "Date of Service"): "DateOfService",
       ("billing_charges", "Post Date"): "PostDate",
       ("billing_charges", "Rendering Provider NPI"): "RenderingProviderNpi",
       ("billing_charges", "5th through 25th ICD-10 CM Code"): ["Icd5Code", "Icd6Code", ..., "Icd25Code"],
       ...
   }
   ```

3. **`DATA_FORMAT_PATTERNS`** — regex patterns for domain-specific fields:
   ```python
   DATA_FORMAT_PATTERNS = {
       "npi":       {"pattern": r"^\d{10}$",                     "description": "10-digit numeric"},
       "cpt_code":  {"pattern": r"^\d{5}$|^[A-Z]\d{4}$",        "description": "5-char: 99213 or T1015"},
       "icd10":     {"pattern": r"^[A-TV-Z]\d{2,4}\.?\d{0,4}$", "description": "Letter + digits, optional decimal"},
       "pos_code":  {"pattern": r"^\d{1,2}$",                    "description": "1-2 digit numeric"},
       "zip_code":  {"pattern": r"^\d{5}(-\d{4})?$",             "description": "5 or 9-digit ZIP"},
       "yearmonth": {"pattern": r"^\d{6}$",                      "description": "YYYYMM integer"},
   }
   ```

4. **`DOMAIN_FIELD_PATTERNS`** — maps staging column names to their `DATA_FORMAT_PATTERNS` key:
   ```python
   DOMAIN_FIELD_PATTERNS = {
       "RenderingProviderNpi": "npi",
       "BillingProviderNpi":   "npi",
       "PayEmpNpi":            "npi",
       "CptCodeOrig":          "cpt_code",
       "IcdCodeOrig":          "icd10",
       "PosCode":              "pos_code",
       "ZipOrig":              "zip_code",
       "PatZipOrig":           "zip_code",
       "BillLocZipOrig":       "zip_code",
       "YearMonth":            "yearmonth",
   }
   ```

---

### `shared/staging_meta.py`

**Purpose:** Load `StagingTableStructure.xlsx` once and provide type/length lookup functions.

**Duplicate column name handling:** `StagingTableStructure.xlsx` has repeated display column names within a table (e.g. `ProvNpi` appears 3× in `#staging_charges` for Rendering, Billing, and Referring providers). The `Source_Column` field (e.g. `RenderingProviderNpi`, `BillingProviderNpi`, `ReferringProviderNpi`) is the canonical unique key. All internal lookups index by `(staging_table, Source_Column)`.

**Functions:**
- `get_column_type(staging_table, source_column) → dict` — returns `{"type", "max_length", "precision", "scale"}`
- `get_all_columns(staging_table) → list[dict]` — all columns for a staging table with their metadata
- `get_source_column_name(staging_table, display_col_name) → list[str]` — returns all `Source_Column` values that match a display name (handles duplicates)

---

### `shared/loader.py`

**Purpose:** Re-load source file DataFrames using Phase 1 metadata so Phase 2 can run independently without Phase 1 in memory.

**Phase 1 dependency:** `phase1_findings.json` must contain `file_path`, `delimiter`, and `encoding` per file entry. These values are already captured by `phase1/ingestion.py` but must be written into the JSON by `phase1/report.py` (see Key Implementation Notes — this is a minor update required before Phase 2 can run).

**Function:**
```python
def load_files(phase1_json_path: str, input_base_dir: str) -> dict[str, dict]:
    """
    Returns {
        filename: {
            "df": DataFrame,
            "source": str,
            "staging_table": str,
            "column_mappings": list,
            "file_path": str,
            "delimiter": str,
            "encoding": str
        }
    }
    """
```

---

### `phase2/schema_validator.py`

**Purpose:** Check whether each file contains the required, recommended, and optional fields for its data source.

**Algorithm:**

1. For each file, retrieve `source` and `column_mappings` from Phase 1 (via `loader.py`).
2. Build the set of **covered staging columns** = all `StagingColumn` values from mappings where `Confidence != "UNRECOGNIZED"`.
3. For each template field in `FIELD_REQUIREMENTS[source]`:
   - Look up expected staging column(s) via `TEMPLATE_TO_STAGING[(source, field_name)]`
   - Check if the staging column is in the covered set
   - Classify:
     - **PRESENT** — staging column is covered
     - **MISSING (CRITICAL)** — Required field, staging column not covered
     - **MISSING (HIGH)** — Recommended field, staging column not covered
     - **MISSING (INFO)** — Optional field, staging column not covered

**Special handling:**

- **ICD-10 5th–25th:** Template lists this as one Required field but clients may supply 0–21 individual columns. Flag as CRITICAL only if zero columns beyond the 4th ICD-10 are present. If some but not all 21 are present, flag as INFO noting the count found (e.g. "6 of 21 additional ICD-10 columns present").
- **Cost Center (billing):** Conditionally required. If Cost Center is missing, check whether Practice Name, Billing Location Name, and Department Name are all also missing. If at least one org field is present → flag Cost Center as HIGH with note: "Crosswalk to GL required." If Cost Center AND all three org fields are missing → CRITICAL.
- **Charge ID / Invoice Number (billing):** Both are Required, but in practice at least one must be present. Flag as CRITICAL only if neither is mapped.

**Returns per file:**
```python
{
    "schema_findings": [
        {
            "template_field": "Rendering Provider NPI",
            "staging_column": "RenderingProviderNpi",
            "requirement_level": "required",
            "status": "PRESENT",   # or "MISSING"
            "severity": None,      # or "CRITICAL" / "HIGH" / "INFO"
            "raw_column_matched": "Provider NPI",
            "confidence": "EXACT",
            "notes": ""
        },
        ...
    ],
    "summary": {
        "required_total": 40, "required_present": 38, "required_missing": 2,
        "recommended_total": 12, "recommended_present": 8, "recommended_missing": 4,
        "optional_total": 15, "optional_present": 5, "optional_missing": 10
    }
}
```

---

### `phase2/field_classifier.py`

**Purpose:** Classify every mapped column into one of four requirement buckets.

**Buckets:**
1. **Required** — column maps to a staging column corresponding to a Required template field
2. **Recommended** — maps to a Recommended template field
3. **Optional** — maps to an Optional template field
4. **Unclassified** — maps to a valid staging column not explicitly listed in the template (e.g. `WorkRvuCustom`, `ContractAllowableOrig`); valid and accepted but not client-facing

**Returns per file:** Phase 1 `column_mappings` list, each entry augmented with:
```python
{"RequirementLevel": "Required" | "Recommended" | "Optional" | "Unclassified"}
```

---

### `phase2/datatype_checker.py`

**Purpose:** Validate that actual data values are compatible with target staging column SQL types, length constraints, and domain-specific format rules.

**Checks per mapped column:**

#### 1. SQL Type Compatibility

Look up target type from `staging_meta.get_column_type()`:

| Staging Type | Validation Rule |
|---|---|
| `varchar` | Check `max_length` — flag values exceeding it (count + sample) |
| `date` | Must be parseable as dates; detect format; flag unparseable values; flag inconsistent formats within same column |
| `int` | Must be whole numbers; flag non-numeric; flag values outside ±2,147,483,647 |
| `decimal(p,s)` / `numeric(p,s)` | Must be numeric; check precision (total digits) and scale (decimal places); flag exceeding values |
| `time` | Must be parseable as time (HH:MM, HH:MM:SS, H:MM AM/PM); flag invalid values |

**Implementation:**
```python
def check_column_type(series: pd.Series, staging_type: str, max_length: int,
                      precision: int, scale: int) -> dict:
    """
    Returns {
        "type_compatible": bool,
        "invalid_count": int,
        "invalid_sample": list[str],      # up to 5 bad values
        "invalid_rows": list[int],         # row indices of first 20 bad values
        "max_observed_length": int,        # varchar
        "length_exceeded_count": int,      # varchar
        "date_format_detected": str,       # date columns
        "date_format_inconsistent": bool,
        "precision_exceeded_count": int,   # decimal
        "notes": str
    }
    ```

#### 2. Domain-Specific Format Validation

Using `DOMAIN_FIELD_PATTERNS`, apply pre-compiled regex checks via vectorized `Series.str.match()`:

| Staging Column | Check |
|---|---|
| `RenderingProviderNpi`, `BillingProviderNpi`, `PayEmpNpi` | Must be exactly 10 digits; flag alpha chars, truncated (9 digits), padded (11+), leading-zero stripped |
| `CptCodeOrig` | Must be 5 chars: 5 digits (99213) or letter+4 digits (T1015); flag embedded modifiers (99213-25), truncated codes, descriptions in code field |
| `IcdCodeOrig` | Must start with letter + digits, optional decimal; flag ICD-9 format (3-digit numeric), descriptions in code field |
| `PosCode` | Must be 1–2 digit numeric; flag text descriptions, values > 99 |
| `Modifier1`–`Modifier4` | Should be 2-char codes or blank; flag concatenated modifiers, multiple modifiers in one field |
| `ZipOrig`, `PatZipOrig`, `BillLocZipOrig` | 5-digit or ZIP+4 format; flag partial, non-numeric, international postal codes |
| `YearMonth` | Must be YYYYMM integer (e.g. 202601); flag date strings, MM/YYYY |
| `GenderOrig` | M/F/U/Male/Female/Unknown; flag unexpected coded values, numeric codes without reference |
| `PatAge` | Numeric or parseable age; flag dates of birth in age field, negatives, ages > 120 |

#### 3. Varchar Length Truncation Check

For every `varchar` column, compare actual max string length against `max_length` from `StagingTableStructure.xlsx`:
- **MEDIUM** — < 5% of rows would be truncated
- **HIGH** — ≥ 5% of rows would be truncated

#### 4. Null/Blank Pre-check in Required Fields

While iterating columns for type checks, count null/blank values in Required-classified columns:
- **CRITICAL** — Required column is > 50% null
- **HIGH** — Required column is > 0% and ≤ 50% null
- Note: This is a lightweight pre-check; Phase 3 performs full row-level null analysis.

#### 5. Severity Escalation

If a data type issue affects a column classified as **Required** by `field_classifier.py`, escalate severity by one level: MEDIUM→HIGH, HIGH→CRITICAL.

**Returns per file:**
```python
{
    "datatype_findings": [
        {
            "raw_column": "Provider NPI",
            "staging_column": "RenderingProviderNpi",
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
            "severity": "HIGH",
            "notes": "832 NPI values are not 10-digit numeric"
        },
        ...
    ]
}
```

---

### `phase2/unrecognized_columns.py`

**Purpose:** Flag source columns that Phase 1 could not map to any staging column, and surface fuzzy-matched columns needing review.

**Identifies:**

1. **UNRECOGNIZED columns** — raw columns with `Confidence == "UNRECOGNIZED"` (UNMAPPED):
   - **LOW** — name appears to be a system/internal field (e.g. `ROW_ID`, `LAST_UPDATED_BY`, `EXTRACT_DATE`)
   - **MEDIUM** — name resembles a PIVOT-relevant concept but didn't match any alias (potential mapping table gap)
   - **HIGH** — name closely resembles a required staging column (near-miss — possible typo or naming variant not in mapping table)

2. **FUZZY-matched columns** — raw columns with `Confidence == "FUZZY"` surfaced for human confirmation.

**Near-miss detection:** For UNRECOGNIZED columns, compute `rapidfuzz.fuzz.token_sort_ratio` against all `Source_Column` names in `StagingTableStructure.xlsx` for the relevant staging table. Scores between 60–84 (below Phase 1's fuzzy threshold of 85 but above noise):
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
│ Required fields      │ 38 / 40 present                          │
│ Recommended fields   │ 8 / 12 present                           │
│ Optional fields      │ 5 / 15 present                           │
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

**Compatibility determination:**
- **YES** — 0 CRITICAL, 0 HIGH
- **YES*** — 0 CRITICAL, ≥1 HIGH (conditionally compatible)
- **NO** — ≥1 CRITICAL

**Excel report** — new file `{client}_{round}_Phase2_{YYYYMMDD}.xlsx` — 5 sheets:

| Sheet | Contents |
|---|---|
| `Schema Validation` | One row per template field per file: field name, staging column, requirement level, status, severity, matched raw column, confidence, notes |
| `Schema Summary` | One row per file: required/recommended/optional present + missing counts |
| `Data Type Checks` | One row per mapped column per file: raw column, staging column, staging type, max length, type compatible, domain check, invalid count, invalid %, sample bad values, severity, notes |
| `Unrecognized Columns` | One row per unrecognized/fuzzy column per file: raw column, severity, nearest match, score, notes |
| `Compatibility Summary` | One row per file: CRITICAL/HIGH/MEDIUM counts, compatible status, overall assessment |

**JSON manifest** — `output/{client}/phase2_findings.json` — consumed by Phase 3:
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
py run_phase2.py --client "ClientName" --round v1
```

**Full options:**

| Option | What it does | Default |
|---|---|---|
| `--client` | Client name — must match `output/{client}/` subfolder | `Client` |
| `--round` | Submission round number | `v1` |
| `--input` | Base folder where client input folders live | `./input` |
| `--output` | Base folder where Phase 1/2 output is written | `./output` |

**Execution order:**
1. Load `phase1_findings.json` from `output/{client}/` — exit with error if not found
2. `loader.load_files(phase1_json, input_dir)` → re-load DataFrames
3. Load `StagingTableStructure.xlsx` into `staging_meta`
4. For each file:
   a. `schema_validator.validate(file_data, source)` → schema findings
   b. `field_classifier.classify(file_data, source)` → enriched column mappings
   c. `datatype_checker.check(file_data, source, staging_table)` → type findings
   d. `unrecognized_columns.flag(file_data, source, staging_table)` → unrecognized findings
5. `report.render(all_findings, output_dir, client, round)` → console + Excel + JSON

---

## Key Implementation Notes

- **Phase 1 JSON prerequisite** — Phase 2 exits with a clear error and instructions if `phase1_findings.json` is not found in `output/{client}/`.

- **Phase 1 minor update required** — `phase1/report.py` must be updated to write `file_path`, `delimiter`, and `encoding` into each file's JSON block. These values are already captured by `phase1/ingestion.py` (in the ingestion metadata dict) but are not currently written to the JSON. This is a small change to `phase1/report.py` that must be completed before `shared/loader.py` can function.

- **`TEMPLATE_TO_STAGING` is static** — this dict is hand-curated in `shared/constants.py` by cross-referencing template field names against staging column names. It is not loaded dynamically from Excel. Update when PIVOT templates change.

- **Duplicate staging column names** — `StagingTableStructure.xlsx` repeats display column names within tables (e.g. `ProvNpi` ×3 in `#staging_charges`). Use `Source_Column` (e.g. `RenderingProviderNpi`) as the canonical key in all lookups. `staging_meta.py` indexes by `(staging_table, Source_Column)`.

- **Quality and Patient Satisfaction** — These sources have no staging tables. For `datatype_checker.py`, apply `DATA_FORMAT_PATTERNS` domain checks directly (NPI, date, numeric); skip SQL type/length checks. Schema validation with `FIELD_REQUIREMENTS` runs normally.

- **Performance** — Domain-specific pattern checks use pre-compiled regex + vectorized `Series.str.match()` for speed on 2M+ row files.

- **Severity escalation** — If a type issue affects a Required field, escalate one level: MEDIUM→HIGH, HIGH→CRITICAL. Apply this after `field_classifier` runs.

- **Combined billing payer disambiguation** — In `#staging_billing`, payer fields appear twice (charge-level and transaction-level). Run type checking once per unique raw column; note the dual-mapping but do not double-count as an error.

- **`normalize()` reuse** — Do not duplicate this helper. Import from `phase1/column_mapping.py`, or move to `shared/` if both phases need it.

- **`argparse` CLI** — Same pattern as `run_phase1.py`; `argparse` is in the standard library.

---

## Verification Checklist

After implementation, validate with a real or synthetic test batch:

- [ ] Run `py run_phase1.py --client "Test" --round v1` and confirm `phase1_findings.json` now includes `file_path`, `delimiter`, `encoding` per file
- [ ] Run `py run_phase2.py --client "Test" --round v1` — completes without errors
- [ ] Console prints one schema validation box per file
- [ ] Console prints overall compatibility summary table
- [ ] A file missing `Rendering Provider NPI` → CRITICAL in schema findings
- [ ] A file missing `Rendering Provider ID` (Recommended) → HIGH
- [ ] A file missing `Patient Race/Ethnicity` (Optional) → INFO
- [ ] Cost Center missing + all org fields missing → CRITICAL; Cost Center missing + Department Name present → HIGH with crosswalk note
- [ ] NPI column with 9-digit values flagged by domain check with count and sample
- [ ] CPT code column with embedded modifiers (e.g. `99213-25`) flagged
- [ ] ICD-10 column with old ICD-9 codes (e.g. `250.00`) flagged
- [ ] Date column with mixed formats (MM/DD/YYYY and YYYY-MM-DD in same column) flagged as inconsistent
- [ ] Varchar column with values exceeding max_length → truncation warning with count
- [ ] Required field that is >50% null → CRITICAL null pre-check
- [ ] Unrecognized column `Rending Provider NPI` → near-miss to `RenderingProviderNpi` at severity HIGH
- [ ] Fuzzy-matched column surfaces in review list with confidence score
- [ ] `phase2_findings.json` is valid JSON with all expected keys
- [ ] Excel report contains all 5 Phase 2 sheets with correct data
- [ ] Quality file validates NPI and Performance Rate patterns correctly despite no staging table
- [ ] File with 0 CRITICAL and 0 HIGH → `YES` compatible
- [ ] File with 0 CRITICAL and ≥1 HIGH → `YES*` conditionally compatible
- [ ] File with ≥1 CRITICAL → `NO` not compatible
- [ ] `loader.py` successfully re-reads a 2M+ row billing file using `file_path`/`delimiter`/`encoding` from Phase 1 JSON
