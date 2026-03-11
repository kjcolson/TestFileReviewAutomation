# Phase 3 Technical Plan — Data Quality Review

## Overview

Phase 3 performs semantic, row-level data quality analysis. It consumes outputs from Phase 1 (column mappings, source classification, test month) and Phase 2 (RequirementLevel per column) and produces `phase3_findings.json`, an Excel report, and console output.

---

## Dependencies

| Artifact | Source | Used For |
|---|---|---|
| `phase1_findings.json` | Phase 1 output | Column mappings (raw->staging), source type, test month, file paths |
| `phase2_findings.json` | Phase 2 output | `RequirementLevel` per column (Required/Recommended/Optional) |
| `StagingTableStructure.xlsx` | `KnowledgeSources/` | Staging column types (for numeric range checks) |
| `TransactionTypes.xlsx` | `KnowledgeSources/` | Charge row identification (charge mask for billing_combined) |
| `stdCmsCpt.xlsx` | `KnowledgeSources/` | CPT code validation (B13) |
| `stdCmsPos.xlsx` | `KnowledgeSources/` | Place of Service validation (B14) |

---

## New Files to Create

```
phase3/
    __init__.py
    universal.py          # Checks 1-6 (null, duplicate, date, numeric, placeholder, encoding)
    billing.py            # B1-B14
    scheduling.py         # S1-S9
    payroll.py            # P1-P7
    gl.py                 # G1-G7
    quality.py            # Q1-Q5
    patient_satisfaction.py  # PS1-PS4
    report.py             # Console + Excel + JSON output
run_phase3.py
```

---

## Shared Infrastructure Changes

### `shared/staging_meta.py` - Add CMS knowledge source loading

Add to the existing `load()` function (after `TransactionTypes.xlsx` loading):

```python
# Load CMS reference tables
cpt_path = ks_dir / "stdCmsCpt.xlsx"
pos_path = ks_dir / "stdCmsPos.xlsx"

if cpt_path.exists():
    _cms_cpt_df = pd.read_excel(cpt_path, dtype=str).set_index("CptCode")
if pos_path.exists():
    _cms_pos_df = pd.read_excel(pos_path, dtype=str).set_index("PosCode")

def get_cms_cpt() -> pd.DataFrame | None: ...
def get_cms_pos() -> pd.DataFrame | None: ...
```

### `shared/column_utils.py` - New file for `resolve_column()`

Place here so both Phase 3 and Phase 4 can import it:

```python
def resolve_column(column_mappings: list[dict], staging_col: str) -> str | None:
    """Return the raw column name for a given staging column, or None if unmapped."""
    for m in column_mappings:
        if m.get("staging_col") == staging_col:
            return m.get("raw_col")
    return None
```

---

## Module Implementation Order

Build in this order (each builds on the previous):

1. `shared/column_utils.py` - `resolve_column()` utility
2. `shared/staging_meta.py` - Add CMS CSV loading + getters
3. `phase3/universal.py` - Generic checks (no source knowledge required)
4. `phase3/billing.py` - Most complex; depends on charge mask logic already in `staging_meta`
5. `phase3/scheduling.py`, `payroll.py`, `gl.py` - Independent of each other
6. `phase3/quality.py`, `patient_satisfaction.py` - Simple; no staging table
7. `phase3/report.py` - Renders all findings
8. `run_phase3.py` - CLI orchestrator

---

## Key Implementation Details

### Charge Mask (billing_combined)

Reuse the existing `staging_meta.get_charge_type_sets()` and `_build_charge_mask()` pattern from `phase2/datatype_checker.py`. Apply the mask for null checks on charge-conditional columns: `CptCode`, `Units`, `WorkRvuOriginal`, `PrimaryIcdCode`, `SecondaryIcdCodes`.

### Column Resolution (quality / patient_satisfaction)

Quality and patient satisfaction have no staging table. All columns map to `_raw_check` in `TEMPLATE_TO_STAGING`. For these sources, `resolve_column()` returns the raw column directly from Phase 1's `column_mappings` (where `staging_col` will equal the template field name).

### Severity Escalation

Pull `RequirementLevel` from `phase2_findings.json` per file per column. Merge into the column_mappings list at startup so every check has it available.

```python
# In run_phase3.py startup:
for file_entry in phase1["files"].values():
    for mapping in file_entry["column_mappings"]:
        stg = mapping["staging_col"]
        mapping["requirement_level"] = phase2_req_levels.get(stg, "Optional")
```

### Phase 4 Prep Metadata

Checks B12, S5, P4, P5 store metadata in `phase3_findings.json` under a `cross_source_prep` key per file:

```json
"cross_source_prep": {
    "patient_id_format": "numeric_10digit",
    "patient_id_leading_zeros": true,
    "provider_npi_population_pct": 94.2,
    "department_id_format": "numeric_4digit",
    "department_distinct_count": 12
}
```

Phase 4 will use these to configure matching for:
- Billing <-> Scheduling (patient ID, provider NPI, location)
- Billing <-> Payroll (provider NPI)
- Payroll <-> GL (department ID to cost center)
- Scheduling <-> GL (location name to cost center)

---

## `run_phase3.py` CLI

```
py run_phase3.py "ClientName" v1
py run_phase3.py --client "ClientName" --round v1
py run_phase3.py --client "ClientName" --round v1 --input ./input --output ./output --knowledge-dir ./KnowledgeSources
```

Positional arguments follow the same pattern as `run_phase1.py` and `run_phase2.py`.

**Execution order:**
1. Load `phase1_findings.json` + `phase2_findings.json`
2. `staging_meta.load(ref_dir)` - ensures CMS tables are cached
3. `loader.load_files(phase1_json, input_dir)` -> DataFrames
4. Merge RequirementLevel into column mappings
5. For each file: run universal checks, then route to source-specific module
6. `billing.run_checks()` receives dict of all billing DataFrames (for B2 linkage check)
7. `report.render()` -> console + Excel + JSON

---

## Output Files

| File | Location |
|---|---|
| `{client}_{round}_Phase3_{YYYYMMDD}.xlsx` | `output/{client}/` |
| `phase3_findings.json` | `output/{client}/` |

### Excel Sheets
- `Universal Findings`
- `Source-Specific Findings`
- `Null Analysis`
- `Duplicate Analysis`
- `Cost Center P&L`
- `Data Quality Summary`

---

## Console Output Format (ASCII-only)

All output uses ASCII box characters — no Unicode box-drawing. Severity icons: `X` = CRITICAL, `!` = HIGH, `o` = MEDIUM, `.` = INFO.

```
+-------------------------------------------------------------------+
| DATA QUALITY REVIEW                                               |
+----------------------+--------------------------------------------+
| File Name            | PIVOT_Billing_Epic_202601.txt             |
| Source               | billing_combined                          |
| Total Records        | 14,327                                    |
+-------------------------------------------------------------------+
| UNIVERSAL CHECKS                                                  |
|  X CRITICAL: 'Work RVUs' is 62% null on charge rows     [8,883] |
|  ! HIGH: 284 duplicate rows on key [ChargeId, PostDate]          |
|  o MEDIUM: 47 placeholder values ('test') in Patient MRN         |
+-------------------------------------------------------------------+
| SOURCE-SPECIFIC CHECKS (BILLING)                                  |
|  ! HIGH: 12.3% of E&M rows have zero wRVUs                       |
|  o MEDIUM: 83 CPT codes contain embedded modifiers               |
+-------------------------------------------------------------------+
| ISSUE SUMMARY                                                     |
|  CRITICAL: 1  |  HIGH: 2  |  MEDIUM: 2  |  INFO: 0              |
+-------------------------------------------------------------------+
```

---

## Verification

After implementation, run against a Franciscan test batch and confirm:

- [ ] `phase3_findings.json` is valid JSON with `files`, `universal_findings`, `source_specific_findings`, `cross_source_prep`
- [ ] Excel has all 6 sheets
- [ ] Combined billing: charge-conditional columns (`CptCode`, `Units`, `WorkRvuOriginal`, `PrimaryIcdCode`, `SecondaryIcdCodes`) only checked against charge rows
- [ ] Missing `stdCmsCpt.xlsx` -> B13 skipped with warning, no crash
- [ ] Missing `stdCmsPos.xlsx` -> B14 skipped with warning, no crash
- [ ] Console output is ASCII-only (no encoding errors on redirected stdout)
- [ ] `py run_phase3.py "Franciscan" v1` (positional args) works
- [ ] B1: Combined file with only charges -> CRITICAL
- [ ] B2: Orphaned transactions -> HIGH
- [ ] G4: Missing Charges or Provider Comp account -> CRITICAL
- [ ] G6: YearMonth validation — if GL Report Period is not in YYYYMM integer format (e.g. uses date strings like `2025-06-30`), auto-converts to YYYYMM and emits a MEDIUM finding noting the format mismatch
- [ ] P7: Zero clinical support staff -> CRITICAL
