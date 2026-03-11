# How to Run Phase 4 — Cross-Source Validation

## What Does Phase 4 Do?

Phase 4 checks whether the submitted files can be joined as expected in the PIVOT database. Where Phase 3 asked *"Are the values inside each file correct?"*, Phase 4 asks *"Can the files be connected to each other?"*

It runs up to 6 cross-source checks:
- **C0** — Billing Transactions ↔ Billing Charges (charge ID linkage and payment balance)
- **C1** — Billing ↔ GL (cost center alignment)
- **C2** — Billing ↔ Payroll (provider NPI/name matching)
- **C3** — Billing ↔ Scheduling (location, provider NPI, patient ID)
- **C4** — Payroll ↔ GL (department to cost center)
- **C5** — Scheduling ↔ GL (location to cost center)

Any check whose required source files aren't present is automatically skipped.

At the end, it produces:
- A **console summary** showing pass/conditional/fail/skipped status per check
- A **7-sheet Excel report** with detailed match tables for each check
- A **JSON file** (`phase4_findings.json`) used by Phase 5

---

## Before You Start

You must have completed Phase 1 and Phase 3 for this client and round. Phase 4 reads their output files.

Check that these files exist in `output/{ClientName}/`:
- `phase1_findings.json`
- `phase3_findings.json`

> **Note:** Phase 4 is automatically skipped when fewer than 2 compatible source groups are present. If you only submitted billing + quality (no GL, payroll, or scheduling), Phase 4 will be skipped entirely and the pipeline proceeds to Phase 5.

---

## Quick Start (Most Common Usage)

```
py scripts/run_phase4.py "ClientName" v1
```

**Examples:**
```
py scripts/run_phase4.py "Franciscan" v1
py scripts/run_phase4.py "Memorial Health" v2
```

---

## Full Command with All Options

```
py scripts/run_phase4.py --client "ClientName" --round v1 --input ./input --output ./output --knowledge-dir ./KnowledgeSources
```

| Option | Default | What It Does |
|---|---|---|
| `--client` | (required) | Client name — must match Phase 1/3 |
| `--round` | (required) | Round identifier (e.g., `v1`, `v2`) |
| `--input` | `./input` | Folder containing the raw data files |
| `--output` | `./output` | Folder where reports are saved |
| `--knowledge-dir` | `./KnowledgeSources` | Folder with reference files (TransactionTypes.xlsx) |

---

## What You'll See

```
+-------------------------------------------------------------------+
| CROSS-SOURCE VALIDATION                                           |
+-------------------------------------------------------------------+
| C0 — Trans ↔ Charges: PASS                                       |
|   Charge ID linkage: 97.3% (high-side)                           |
|   Payment balance: 71.2% zero-balance                            |
+-------------------------------------------------------------------+
| C1 — Billing ↔ GL: CONDITIONAL                                   |
|   BillDepartmentId: 78.4% match (21.6% unmatched — $24,831)     |
+-------------------------------------------------------------------+
| C2 — Billing ↔ Payroll: CONDITIONAL                              |
|   Match method: NPI                                               |
|   NPI match: 82.1% (12 providers unmatched)                      |
+-------------------------------------------------------------------+
| C3 — Billing ↔ Scheduling: CONDITIONAL                           |
|   Location: 90.0% match  Provider NPI: 88.3%  Patient ID: 74.1% |
+-------------------------------------------------------------------+
| C4 — Payroll ↔ GL: PASS                                          |
|   Dept match: 100% (auto-extracted offset [2:7])                 |
+-------------------------------------------------------------------+
| C5 — Scheduling ↔ GL: CONDITIONAL                                |
|   Location match: 85.7% (2 locations need manual review)         |
+-------------------------------------------------------------------+
| SUMMARY                                                           |
|  C0: PASS  C1: COND  C2: COND  C3: COND  C4: PASS  C5: COND    |
+-------------------------------------------------------------------+
```

**Status meanings:**
| Status | Meaning |
|---|---|
| `PASS` | 0 CRITICAL and 0 HIGH findings |
| `CONDITIONAL` | 0 CRITICAL, but 1+ HIGH findings |
| `FAIL` | 1+ CRITICAL findings |
| `SKIPPED` | Required source file(s) not present |

---

## Output Files

| File | Description |
|---|---|
| `{Client}_{Round}_Phase4_YYYYMMDD.xlsx` | Full Excel report with 7 sheets |
| `phase4_findings.json` | Machine-readable findings (used by Phase 5) |

**Excel Sheets:**
1. **Trans-Charges** — C0: charge ID linkage match table + payment balance summary
2. **Billing-GL** — C1: one row per billing org value with matched flag, row count, and dollar amount
3. **Billing-Payroll** — C2: provider match table (NPI or name), top 20 providers by charge volume
4. **Billing-Scheduling** — C3: location match, provider NPI overlap, and patient ID overlap (3 sections)
5. **Payroll-GL** — C4: department ID match table; auto-extraction note if applicable
6. **Scheduling-GL** — C5: location match table with fuzzy candidates listed for review
7. **Cross-Source Summary** — one row per check with CRITICAL/HIGH/MEDIUM counts and pass status

---

## Common Issues

### "phase1_findings.json not found"
Run Phase 1 first: `py scripts/run_phase1.py "ClientName" v1 --no-prompt`

### "phase3_findings.json not found"
Run Phase 3 first: `py scripts/run_phase3.py "ClientName" v1`

### All checks show SKIPPED
Phase 4 was auto-skipped because fewer than 2 compatible source groups are present. Submit additional source files (GL, payroll, or scheduling) alongside billing to enable cross-source validation.

### C2 shows "match method: name_only"
The payroll file doesn't include an Employee NPI column (or it's mostly blank). Phase 4 falls back to matching on provider full name. Name matching produces lower match rates than NPI matching — flag unmatched providers for manual review.

### C4 shows "AUTO-EXTRACTED offset [x:y]"
Payroll department IDs are longer than GL cost center numbers. Phase 4 found a consistent substring offset that yields matches (e.g., dept ID `8818748000` → characters 2–7 = `18748` = GL cost center). This is normal for some EHR/payroll systems — the auto-detection is correct.

### Excel file is open / "Permission denied"
Close the existing Phase 4 Excel report before re-running.

---

## Tips

- Run all four phases at once: `py run_all.py "ClientName" v1 --no-prompt`
- CRITICAL and HIGH findings from Phase 3 may explain why C2, C3, or C4 match rates are low (e.g., missing NPIs in payroll). Resolve Phase 3 issues first if cross-source match rates are unexpectedly low.
- The **Cross-Source Summary** sheet gives a quick view of which checks passed and which need attention.
- Fuzzy match candidates in C3a (location) and C5 (scheduling↔GL) should be reviewed manually — the tool flags likely matches but human confirmation is needed before concluding the data is compatible.
- See `reference/4_CrossSourceValidation.md` for the full specification and severity thresholds for each check.
