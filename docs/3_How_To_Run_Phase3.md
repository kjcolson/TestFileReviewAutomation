# How to Run Phase 3 — Data Quality Review

## What Does Phase 3 Do?

Phase 3 checks whether the data inside each file is actually clean and ready for analysis. Where Phase 1 and 2 checked *"Do you have the right files and columns?"*, Phase 3 checks *"Are the values in those columns correct and complete?"*

It looks for things like:
- Missing required data (null or blank values)
- Duplicate records
- Dates that fall outside the expected month
- Placeholder or test values (like "TEST", "N/A", "1234567890")
- CPT codes that don't exist in the CMS fee schedule
- Invalid Place of Service codes
- Appointments with no cancellation reason
- Payroll missing clinical or administrative staff
- GL accounts missing entire P&L categories (no revenue, no provider compensation)

At the end, it produces:
- A **console summary** you can read right in the terminal
- An **Excel report** with 6 worksheets covering every finding in detail
- A **JSON file** (`phase3_findings.json`) used by Phase 4

---

## Before You Start

You must have already completed Phase 1 and Phase 2 for this client and round. Phase 3 reads their output files.

Check that these files exist in your `output/{ClientName}/` folder:
- `phase1_findings.json`
- `phase2_findings.json`

---

## Quick Start (Most Common Usage)

Open a terminal in the `TestFileReviewAutomation` folder and run:

```
py scripts/run_phase3.py "ClientName" v1
```

Replace `ClientName` with your client's name (same spelling you used in Phase 1 and 2) and `v1` with the round number.

**Examples:**
```
py scripts/run_phase3.py "Franciscan" v1
py scripts/run_phase3.py "Memorial Health" v2
```

---

## Full Command with All Options

If your files are in non-default locations, use the full form:

```
py scripts/run_phase3.py --client "ClientName" --round v1 --input ./input --output ./output --knowledge-dir ./KnowledgeSources
```

| Option | Default | What It Does |
|---|---|---|
| `--client` | (required) | Client name — must match Phase 1/2 |
| `--round` | (required) | Round identifier (e.g., `v1`, `v2`) |
| `--input` | `./input` | Folder containing the raw data files |
| `--output` | `./output` | Folder where reports are saved |
| `--knowledge-dir` | `./KnowledgeSources` | Folder with CMS reference files |

---

## What You'll See

When the script runs, it prints a box for each file:

```
+-------------------------------------------------------------------+
| DATA QUALITY REVIEW                                               |
+----------------------+--------------------------------------------+
| File Name            | PIVOT_Billing_Epic_202601.txt             |
| Source               | billing_combined                          |
| Total Records        | 14,327                                    |
+-------------------------------------------------------------------+
| UNIVERSAL CHECKS                                                  |
|  X CRITICAL: Required field 'Work RVUs' has 8,883 missing       |
|  ! HIGH: 284 duplicate rows on key [Charge ID, Post Date]        |
|  o MEDIUM: 47 placeholder values ('test') in Patient MRN         |
+-------------------------------------------------------------------+
| SOURCE-SPECIFIC CHECKS (BILLING)                                  |
|  ! HIGH: 12.3% of E&M rows have zero wRVUs                       |
|  o MEDIUM: 83 CPT codes contain embedded modifiers               |
+-------------------------------------------------------------------+
| ISSUE SUMMARY                                                     |
|  CRITICAL: 1  |  HIGH: 2  |  MEDIUM: 2  |  LOW/INFO: 0          |
+-------------------------------------------------------------------+
```

Then an overall summary table for all files.

**Severity levels:**
| Symbol | Level | What It Means |
|---|---|---|
| `X` | CRITICAL | Data is fundamentally incomplete or broken — must be fixed before analysis |
| `!` | HIGH | Significant data quality issue requiring client discussion |
| `o` | MEDIUM | Moderate issue worth noting; may affect specific analyses |
| `.` | LOW/INFO | Informational — not an error, just something to be aware of |

---

## Output Files

After running, find these files in `output/{ClientName}/`:

| File | Description |
|---|---|
| `{Client}_{Round}_Phase3_YYYYMMDD.xlsx` | Full Excel report with 6 sheets |
| `phase3_findings.json` | Machine-readable findings (used by Phase 4) |

**Excel Sheets:**
1. **Universal Findings** — null/blank, duplicate, date, encoding issues across all files
2. **Source-Specific Findings** — billing, scheduling, payroll, GL, quality, satisfaction checks
3. **Null Analysis** — detailed null counts per column per file
4. **Duplicate Analysis** — duplicate records with key column samples
5. **Cost Center P&L** — rough P&L per cost center from the GL (shows charges, adjustments, expenses, net income)
6. **Data Quality Summary** — issue counts by severity per file

---

## Common Issues and What to Do

### "phase1_findings.json not found"
Run Phase 1 first: `py scripts/run_phase1.py "ClientName" v1`

### "phase2_findings.json not found"
Run Phase 2 first: `py scripts/run_phase2.py "ClientName" v1`

### "WARNING: stdCmsCpt.xlsx not found"
The CMS CPT reference file is missing from `KnowledgeSources/`. CPT code validation (check B13) will be skipped. Add the file to re-enable that check.

### "WARNING: stdCmsPos.xlsx not found"
The CMS Place of Service reference file is missing. POS validation (check B14) will be skipped.

### CRITICAL: Combined billing file contains only charge rows
The file is classified as `billing_combined` but appears to contain only charges. This usually means it should be re-classified as `billing_charges`. Check Phase 1 output and re-run if needed.

### CRITICAL: Zero clinical support staff found in payroll
The payroll extract does not contain RN, LPN, or MA employees. This usually means the extract was filtered to providers only. Ask the client to provide a complete payroll export.

### CRITICAL: GL missing Charges or Provider Compensation accounts
The GL extract does not include gross revenue or provider compensation accounts. It may be filtered to a subset of cost centers or account types. The full chart of accounts is needed for Phase 4.

---

## Tips

- Run all three phases (1, 2, 3) before reviewing findings. Phase 3 builds on both.
- The Excel `Cost Center P&L` sheet is a quick way to see which departments have a complete P&L and which are missing categories.
- CRITICAL and HIGH findings should be resolved before moving to Phase 4 (cross-source validation). Otherwise Phase 4 matching will produce unreliable results.
- Phase 4 uses the `phase3_findings.json` to configure patient ID matching between Billing and Scheduling, provider NPI matching between Billing and Payroll, and department/cost center linking between Payroll and GL.
