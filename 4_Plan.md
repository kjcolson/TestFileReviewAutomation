# Phase 4 Implementation Plan — Cross-Source Validation

## Overview

Phase 4 is the fourth phase of the PIVOT Test File Review automation pipeline. It consumes Phase 1 (column mappings, source types, billing format) and Phase 3 (`cross_source_prep` metadata per file) outputs and performs inter-file relationship validation — determining whether the submitted files can be joined as expected in the PIVOT database.

**Libraries:** `pandas`, `rapidfuzz`, `openpyxl`

---

## Project Structure (additions)

```
TestFileReviewAutomation/
├── phase4/
│   ├── __init__.py
│   ├── transactions_charges.py  # C0: Transactions <-> Charges linkage + payment balance
│   ├── billing_gl.py            # C1: Billing <-> GL cost center alignment
│   ├── billing_payroll.py       # C2: Billing <-> Payroll provider name/NPI matching
│   ├── billing_scheduling.py    # C3: Billing <-> Scheduling location, provider NPI, patient ID
│   ├── payroll_gl.py            # C4: Payroll <-> GL department to cost center
│   ├── scheduling_gl.py         # C5: Scheduling <-> GL location to cost center
│   └── report.py                # Console + Excel + JSON output
└── run_phase4.py
```

No changes to `shared/` are required.

---

## Check Specifications

### C0 — Billing Transactions ↔ Billing Charges

**Files needed:** billing data in any form — skip C0 only if no billing file of any type is present.

**Source resolution:**
- Separate billing (`billing_charges` + `billing_transactions`): use the two separate DataFrames directly.
- Combined billing (`billing_combined`): filter charge rows using `_build_charge_mask()` (matches `TransactionType` codes/`TransactionTypeDesc` descs from `staging_meta.get_charge_type_sets()`); filter transaction rows using `staging_meta.get_transaction_type_sets()`.

**C0a — Charge ID Linkage (acceptable threshold: 75%)**

1. Resolve charge link column from charges: `ChargeId` or `InvoiceNumber`.
2. Resolve charge link column from transactions: `ChargeId` or `InvoiceNumber`.
3. Build set of charge IDs from the charges DataFrame.
4. Compute: % of transaction records whose charge link ID appears in the charge ID set.

Severity:
- Transactions-to-charges match < 75% → HIGH
- Transactions-to-charges match ≥ 75% but < 90% → MEDIUM
- ≥ 90% → passes

Extra finding keys: `charge_id_column`, `transaction_id_column`, `charge_distinct`, `transaction_distinct`, `match_count`, `match_pct`, `unmatched_transaction_sample`

**C0b — Payment Balance Reasonableness (acceptable threshold: 65% zero-balance)**

1. For each `ChargeId` in the charges DataFrame, compute `Outstanding Balance = ChargeAmount + sum(Payments) + sum(Adjustments) + sum(Refunds)` by joining charges to transactions on the shared charge link column.
2. Count charges with `|Outstanding Balance| < 0.01` as "zero-balance charges."
3. `% Zero Balance = zero_balance_count / total_charge_count`.
4. `Outstanding Balance Rate = |total outstanding| / |total charge amount|`.

Severity:
- % Zero Balance < 65% AND outstanding balance rate > 15% → HIGH
- % Zero Balance < 65% OR outstanding balance rate > 15% → MEDIUM
- Both within acceptable range → passes

Extra finding keys: `total_charges`, `zero_balance_count`, `zero_balance_pct`, `outstanding_balance`, `total_charge_amount`, `outstanding_balance_rate`, `avg_outstanding_balance`

---

### C1 — Billing ↔ GL (Cost Center Alignment)

**Files needed:** billing (combined or charges) + GL

From the Example Connections workbook: does the billing data have a field to connect to the GL (ideally cost center, or a combination of dept/prac/loc that matches a cost center)? Note: a low % match is acceptable when the dollar amount affected is low.

1. Collect distinct billing org values from (in priority order): `BillDepartmentId`, `BillDepartmentName`, `BillLocationId`, `BillLocationName`, `BillPracticeId`, `BillPracticeName` — use `resolve_column()`.
2. Collect GL cost center values: `CostCenterNumberOrig` and `CostCenterNameOrig`.
3. Build a combined GL reference set: union of all cost center numbers and names (normalized: lowercase + strip).
4. For each billing org column that is present and has data:
   - Compute % of distinct values that match the GL reference set.
   - Compute sum of `ChargeAmountOriginal` for rows whose org value is unmatched.
5. Report both the record-count match rate and the dollar amount affected by unmatched values.

Severity per billing org column:
- > 20% distinct values unmatched AND > 5% of total charge dollars unmatched → HIGH
- > 20% distinct values unmatched BUT dollar impact ≤ 5% → MEDIUM (low impact)
- > 0% unmatched → MEDIUM
- 0% unmatched → passes

Extra finding keys: `billing_column`, `matched_count`, `unmatched_count`, `match_pct`, `total_charge_amount`, `unmatched_charge_amount`, `unmatched_charge_pct`, `unmatched_sample` (up to 20 values)

---

### C2 — Billing ↔ Payroll (Provider Name/NPI Matching)

**Files needed:** billing (combined or charges) + payroll

From the Example Connections workbook: NPI may not be present in Payroll. In the example engagement, names were used as the primary match method, and even name matching showed "very low % match" requiring manual review.

1. Get distinct Rendering Provider NPIs from billing (`RenderingProviderNpi`).
2. Get distinct Employee NPIs from payroll (`EmployeeNpi`) — check if column is present and non-null. Also check `cross_source_prep["provider_npi_population_pct"]` from Phase 3 if available.
3. **If payroll has NPI (> 10% non-null):**
   - Compute NPI exact match rate.
   - For unmatched billing NPIs: attempt name match using `RenderingProviderFullName` vs `EmployeeFullName`; use `rapidfuzz.fuzz.token_sort_ratio ≥ 85`; record as `name_match_candidate`.
4. **If payroll has no NPI (≤ 10% non-null) — name-only matching:**
   - Get distinct `RenderingProviderFullName` from billing (normalized).
   - Get distinct `EmployeeFullName` from payroll (normalized).
   - Compute exact normalized name match rate.
   - For unmatched: fuzzy name match (≥ 80).
5. Report top 20 billing providers by charge volume with their payroll match status.

Severity:
- NPI mode + > 30% billing NPIs unmatched (no name-match candidate) → HIGH
- NPI mode + > 10% unmatched → MEDIUM
- Name-only mode + > 50% unmatched (no fuzzy candidate) → HIGH
- Name-only mode + any unmatched → MEDIUM (expected; flag for manual review)
- All unmatched have name-match candidates → INFO

Extra finding keys: `match_method` (`"npi"` or `"name_only"`), `billing_provider_distinct`, `payroll_provider_distinct`, `exact_match_count`, `exact_match_pct`, `name_match_candidates` (list), `top_providers` (top 20 by charge volume), `unmatched_sample`

---

### C3 — Billing ↔ Scheduling (Location / Provider NPI / Patient ID)

**Files needed:** billing (combined or charges) + scheduling

Three sub-checks:

#### C3a — Location/Department Cross-Reference
1. Get distinct billing location values from: `BillLocationName`, `BillDepartmentName`, `BillPracticeName`.
2. Get distinct scheduling location values from: `BillLocNameOrig`, `DeptNameOrig`, `PracNameOrig`.
3. Exact normalized match; then fuzzy (`token_sort_ratio ≥ 80`) as candidates.
4. Report match rates and fuzzy candidates.

Severity: > 50% unmatched (no fuzzy candidate) → HIGH; > 20% unmatched → MEDIUM.

#### C3b — Provider NPI Cross-Reference
1. Get distinct Rendering Provider NPIs from billing (`RenderingProviderNpi`).
2. Get distinct Appt Provider NPIs from scheduling (`ApptProvNPI`).
3. Compute: % of billing NPIs found in scheduling, and % of scheduling NPIs found in billing.

Severity: < 50% overlap in either direction → HIGH; < 80% → MEDIUM.

#### C3c — Patient ID Cross-Reference
1. Get patient IDs from billing (`PatientId`) and scheduling (`PatIdOrig`).
2. Use `cross_source_prep` from Phase 3:
   - `billing.patient_id_leading_zeros` and `scheduling.patient_id_leading_zeros` → normalize by stripping leading zeros if one side has them and the other doesn't.
3. Compute: % of scheduling patient IDs found in billing, and vice versa.
4. Sample mismatched IDs to show format difference.

Severity: < 30% overlap → HIGH; < 65% overlap → MEDIUM; ≥ 65% with format diff → INFO.

Extra finding keys (per sub-check): `billing_distinct`, `scheduling_distinct`, `overlap_count`, `billing_coverage_pct`, `scheduling_coverage_pct`, `fuzzy_candidates` (C3a only), `format_note` (C3c only)

---

### C4 — Payroll ↔ GL (Department to Cost Center)

**Files needed:** payroll + GL

From the Example Connections workbook: "Do the depts here match the cost centers in the GL?" The example showed Department ID in Payroll = 10-digit code where the **middle 5 characters are the GL Cost Center Number** (e.g. `8818748000` → middle chars `[2:7]` → `18748`). The plan auto-detects and applies this extraction pattern.

1. Get distinct Department IDs from payroll (`DepartmentId`) + Department Names (`DepartmentName`).
2. Get GL cost centers: `CostCenterNumberOrig` and `CostCenterNameOrig`.
3. Normalization / extraction logic:
   - a. Exact match after `str.strip().lower()`.
   - b. If no match: try stripping leading zeros.
   - c. If still no match AND payroll dept IDs are consistently longer than GL cost center numbers: attempt substring extraction — `_find_extraction_offset()` tries all contiguous substrings of length = `len(gl_cost_center)` and checks for a consistent offset. If > 80% of payroll dept IDs have a matching substring at the **same offset**, record `auto_extracted_offset = [start, end]` and re-run the match using that offset. Log: `AUTO-EXTRACTED: middle chars [start:end] match GL cost center format`.
4. Report unmatched payroll departments with their row counts.
5. Acceptable threshold: 100% match expected. Any unmatched department is at least MEDIUM.

Severity:
- Any unmatched payroll department with > 100 payroll rows → HIGH
- Unmatched departments with ≤ 100 rows → MEDIUM
- 0 unmatched → passes

Extra finding keys: `dept_distinct`, `matched_dept_count`, `unmatched_dept_count`, `match_pct`, `auto_extracted_offset`, `unmatched_sample` (list of `{dept_id, dept_name, row_count}`)

---

### C5 — Scheduling ↔ GL (Location to Cost Center)

**Files needed:** scheduling + GL

1. Get distinct location values from scheduling: `BillLocNameOrig`, `PracNameOrig`, `DeptNameOrig`.
2. Get GL cost center values: `CostCenterNameOrig` (and `CostCenterNumberOrig`).
3. Exact normalized match; then fuzzy `token_sort_ratio ≥ 80` as candidates.
4. Report match rates and fuzzy candidates for human review.

Severity:
- > 30% scheduling locations unmatched (no fuzzy candidate) → HIGH
- > 0% unmatched but fuzzy candidates present → MEDIUM (review needed)
- > 0% unmatched with no candidate → MEDIUM

Extra finding keys: `scheduling_column`, `location_distinct`, `exact_match_count`, `fuzzy_candidate_count`, `unmatched_count`, `match_pct`, `fuzzy_candidates`, `unmatched_sample`

---

## `phase4/report.py`

**Console output** (ASCII-only, matching Phase 3 convention):
- One check block per check: files compared, findings list
- Overall summary table: CRIT/HIGH/MEDIUM counts per check, PASS/CONDITIONAL/FAIL/SKIPPED

Pass determination: PASS = 0 CRIT + 0 HIGH; CONDITIONAL = 0 CRIT + ≥ 1 HIGH; FAIL = ≥ 1 CRIT; SKIPPED = required file missing.

**Excel report** — `{client}_{round}_Phase4_{YYYYMMDD}.xlsx` — 7 sheets:

| Sheet | Contents |
|---|---|
| `Trans-Charges` | C0: charge ID linkage match table + payment balance summary |
| `Billing-GL` | C1: one row per billing org value; matched flag, row count, dollar amount |
| `Billing-Payroll` | C2: provider match table (NPI or name), top 20 by charge volume |
| `Billing-Scheduling` | C3: location match, provider NPI overlap, patient ID overlap (3 sections) |
| `Payroll-GL` | C4: dept ID match table, auto-extraction note if applicable |
| `Scheduling-GL` | C5: location match table with fuzzy candidates |
| `Cross-Source Summary` | One row per check: CRIT/HIGH/MEDIUM counts, pass status, skipped flag |

**JSON manifest** — `phase4_findings.json`:
```json
{
  "client": "...",
  "round": "v1",
  "date_run": "YYYY-MM-DD",
  "checks_run": ["C0", "C1", "C2"],
  "checks_skipped": ["C3", "C4", "C5"],
  "overall_pass": false,
  "findings": {
    "C0": { "check": "C0", "sub_checks": { "C0a": {...}, "C0b": {...} } },
    "C1": { "check": "C1", "severity": "MEDIUM", "findings": [...] }
  }
}
```

---

## `run_phase4.py` CLI

```
py run_phase4.py "ClientName" v1
py run_phase4.py --client "ClientName" --round v1
py run_phase4.py --client "ClientName" --round v1 --input ./input --output ./output --knowledge-dir ./KnowledgeSources
```

**Execution order:**
1. Load `phase1_findings.json` — exit with error if not found
2. Load `phase3_findings.json` — exit with error if not found
3. `staging_meta.load(ref_dir)` — knowledge source caching
4. `loader.load_files(phase1_json_path, input_dir)` — re-load DataFrames
5. Extract `cross_source_prep` per file from Phase 3 JSON; extract `billing_format["format"]` from Phase 1 JSON
6. Route to each check module based on which sources are present; skip if required file(s) missing
7. `report.render(all_findings, output_dir, client, round)` — console + Excel + JSON

---

## Key Implementation Notes

- **`resolve_column()` usage** — import from `shared/column_utils.py`; use to get raw column names from staging column names before accessing DataFrame columns.
- **`_normalize(s)`** — `str(s).strip().lower()` defined in each module; additionally `lstrip("0")` for numeric IDs when leading zeros need to be stripped.
- **Fuzzy matching** — use `rapidfuzz.fuzz.token_sort_ratio`; ≥ 85 for provider name matching (C2), ≥ 80 for location name matching (C3a/C5).
- **C0 combined billing** — filter using `_build_charge_mask()` and `_build_transaction_mask()` patterns from `phase3/billing.py`. Same logic: match `TransactionType` against `staging_meta.get_charge_type_sets()` / `get_transaction_type_sets()`.
- **`billing_format` access** — `phase1_json["billing_format"]["format"]` (dict, not string). Values: `"combined"`, `"separate"`, `"none"`, `"unknown"`.
- **C1 dollar amounts** — use `ChargeAmountOriginal` raw column for charge rows.
- **C2 NPI presence** — if `EmployeeNpi` raw column absent OR > 90% null/blank → `match_method = "name_only"`. Also check Phase 3 `cross_source_prep["provider_npi_population_pct"]` if available.
- **C3c patient ID normalization** — use Phase 3 `cross_source_prep["patient_id_leading_zeros"]` per file.
- **C4 `_find_extraction_offset()`** — tries all (start, end) pairs; applies if > 80% of payroll dept IDs match at a consistent offset. Handles the known pattern from the example workbook (`8818748000` → `[2:7]` → `18748`).
- **Skipped checks** — if required source file absent, emit one INFO finding: `{"check": "C0", "severity": "INFO", "message": "Skipped — billing_transactions file not present"}`.
- **Console output** — ASCII-only (no Unicode box-drawing), matching Phase 3 convention.
- **argparse pattern** — same as `run_phase1.py` / `run_phase3.py` (positional fallback + named args).

---

## Verification Checklist

- [ ] `py run_phase4.py "Test" v1` completes without error
- [ ] Console prints one check block per run check + overall summary table
- [ ] Excel has all 7 sheets; `Cross-Source Summary` has one row per check
- [ ] `phase4_findings.json` is valid JSON with `checks_run`, `checks_skipped`, `findings`
- [ ] C0 runs for `billing_combined` (row-type filtering) and separate billing; skipped only if no billing present
- [ ] C0a: transactions-to-charges match < 75% → HIGH
- [ ] C0b: % zero balance < 65% AND outstanding rate > 15% → HIGH
- [ ] C1: Billing dept not in GL → MEDIUM; low record mismatch but high dollar impact → HIGH
- [ ] C2: Payroll has no NPI column → `match_method = "name_only"`, name match rates reported, top 20 providers listed
- [ ] C2: Payroll has NPI → NPI match attempted first, name-match candidates listed for NPI misses
- [ ] C3a: Scheduling location with fuzzy billing match → MEDIUM (not HIGH)
- [ ] C3b: Provider NPI overlap < 80% → MEDIUM
- [ ] C3c: Patient IDs have leading-zero format difference → normalized match attempted; ≥ 65% overlap → INFO note
- [ ] C4: Payroll dept ID `8818748000` vs GL `18748` → auto-extracted `[2:7]`, match succeeds, note written
- [ ] C4: Unmatched dept with > 100 rows → HIGH
- [ ] C5: Scheduling location with fuzzy GL candidate → MEDIUM (not HIGH)
- [ ] If GL file missing → C1, C4, C5 all show SKIPPED
- [ ] If scheduling file missing → C3, C5 all show SKIPPED
