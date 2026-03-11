# Phase 4 Specification — Cross-Source Validation

Phase 4 checks whether the submitted files can be joined as expected in the PIVOT database. Rather than validating individual files in isolation, it validates the *relationships between* files — confirming that shared identifiers (cost centers, provider NPIs, patient IDs, department IDs) are consistent and that linking keys actually match across sources.

**Reference plan:** `4_Plan.md` (technical implementation detail)
**How-to guide:** `4_How_To_Run_Phase4.md`

---

## When Phase 4 Runs vs. Is Skipped

Phase 4 is **automatically skipped** when fewer than 2 compatible source groups (billing, GL, payroll, scheduling) are present. A skip message is shown in the terminal and pipeline continues directly to Phase 5. No Phase 4 Excel or JSON files are created when skipped.

Phase 4 also skips individual checks when the required source files for that check are absent.

---

## The Six Cross-Source Checks (C0–C5)

### C0 — Billing Transactions ↔ Billing Charges

**Files needed:** Any billing file (combined or separate charges + transactions)

Verifies that billing transactions can be linked back to their originating charges, and that the payment amounts balance reasonably against charges.

**C0a — Charge ID Linkage**
- Tests whether transaction records reference a valid Charge ID (or Invoice/Encounter ID) that exists in the charges
- Target: ≥ 90% of transactions linked → passes; 75–89% → MEDIUM; < 75% → HIGH

**C0b — Payment Balance Reasonableness**
- Calculates outstanding balance per charge (ChargeAmount + Payments + Adjustments + Refunds)
- Target: ≥ 65% of charges should have a near-zero balance; if significantly below AND outstanding rate > 15% → HIGH

**Applies to:** Both Combined billing (filters row types internally) and Separate billing.

---

### C1 — Billing ↔ GL (Cost Center Alignment)

**Files needed:** Billing (combined or charges) + GL

Confirms that billing location/department/practice values can be mapped to GL cost centers. PIVOT requires this link to reconcile billing revenue against GL financials.

- Checks distinct billing org values (Cost Center, Department Name, Location Name, Practice Name) against GL cost center numbers and names
- Reports both record-count match rate and **dollar amount** affected by unmatched values (a 30% record mismatch affecting only 2% of charge dollars may be acceptable; a 5% mismatch affecting 40% of charge dollars is HIGH)

**Severity:** > 20% distinct values unmatched AND > 5% of charge dollars unmatched → HIGH; > 20% unmatched with low dollar impact → MEDIUM; any unmatched → MEDIUM

---

### C2 — Billing ↔ Payroll (Provider Matching)

**Files needed:** Billing (combined or charges) + Payroll

Confirms that rendering providers in billing can be identified in payroll. PIVOT uses this to allocate provider compensation against billing wRVUs and charges.

- **If Employee NPI is present in payroll (> 10% non-null):** matches on Rendering Provider NPI = Employee NPI; falls back to name-matching for unmatched NPIs
- **If Employee NPI is absent:** matches on normalized provider full name (fuzzy threshold ≥ 80%)
- Reports top 20 billing providers by charge volume with their payroll match status

**Severity:** NPI mode with > 30% unmatched (no name candidate) → HIGH; name-only mode with > 50% unmatched → HIGH; any unmatched → MEDIUM (name mismatches are common and expected — flag for manual review)

---

### C3 — Billing ↔ Scheduling (Location / Provider / Patient)

**Files needed:** Billing (combined or charges) + Scheduling

Three sub-checks confirm the billing-scheduling join that PIVOT uses for care management and visit-level analytics.

**C3a — Location/Department Cross-Reference**
- Matches billing location/department/practice values against scheduling location/department/practice names
- > 50% unmatched (no fuzzy candidate) → HIGH; > 20% unmatched → MEDIUM

**C3b — Provider NPI Cross-Reference**
- % of billing rendering NPIs found in scheduling, and vice versa
- < 50% overlap in either direction → HIGH; < 80% → MEDIUM

**C3c — Patient ID Cross-Reference**
- % of scheduling patient IDs found in billing, and vice versa
- Handles leading-zero normalization (detected from Phase 3 metadata)
- < 30% overlap → HIGH; 30–64% → MEDIUM; ≥ 65% with format difference → INFO

---

### C4 — Payroll ↔ GL (Department to Cost Center)

**Files needed:** Payroll + GL

Confirms that payroll departments can be mapped to GL cost centers. PIVOT requires this to allocate payroll expense to the correct cost center in financial analysis.

- Matches payroll Department ID/Name against GL Cost Center Number/Name
- Auto-detects substring extraction patterns (e.g., payroll dept ID `8818748000` where characters 2–7 = GL cost center `18748`)
- 100% match expected; any unmatched department is at minimum MEDIUM

**Severity:** Unmatched department with > 100 payroll rows → HIGH; ≤ 100 rows → MEDIUM

---

### C5 — Scheduling ↔ GL (Location to Cost Center)

**Files needed:** Scheduling + GL

Confirms that scheduling locations can be mapped to GL cost centers. PIVOT uses this to link appointment volume to financial performance by location.

- Matches scheduling location/department/practice names against GL cost center names (with fuzzy matching for near-misses)
- Reports exact matches and fuzzy candidates requiring human confirmation

**Severity:** > 30% scheduling locations unmatched (no fuzzy candidate) → HIGH; any unmatched but fuzzy candidates present → MEDIUM (review needed)

---

## Output

| File | Description |
|---|---|
| `{Client}_{Round}_Phase4_{date}.xlsx` | 7-sheet Excel report (Trans-Charges, Billing-GL, Billing-Payroll, Billing-Scheduling, Payroll-GL, Scheduling-GL, Cross-Source Summary) |
| `phase4_findings.json` | Machine-readable findings consumed by Phase 5 |

---

## Relationship to the PIVOT Data Model

These checks directly mirror the join conditions used in PIVOT's staging tables:

| Check | PIVOT Join |
|---|---|
| C0 | `#staging_charges` ↔ `#staging_transactions` via `ChargeId` / `InvoiceNumber` |
| C1 | `#staging_billing/charges` ↔ `#staging_gl` via `CostCenter` / `BillDepartmentId` |
| C2 | `#staging_billing/charges` ↔ `#staging_payroll` via `RenderingProviderNpi` / `EmployeeNpi` |
| C3 | `#staging_billing/charges` ↔ `#staging_scheduling` via `PatientId`, `RenderingProviderNpi`, location |
| C4 | `#staging_payroll` ↔ `#staging_gl` via `DepartmentId` ↔ `CostCenterNumber` |
| C5 | `#staging_scheduling` ↔ `#staging_gl` via location / practice / department ↔ `CostCenterName` |
