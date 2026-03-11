# How to Run Phase 5 — Results Generation & Reporting

## What Does Phase 5 Do?

Phase 5 reads the output from Phases 1–4 and generates a single consolidated report covering every finding. No new validation checks are run — Phase 5 only aggregates and presents.

It produces:
- An **executive summary** with readiness verdict (Ready / Conditionally Ready / Needs Revision)
- A **source summary** showing pass/fail status and severity counts per data source
- **Date ranges** per source showing the filter column and min/max dates found in the data
- A **client issue list** sorted by severity, ready to send to the client
- A **cross-source validation summary** of all C0–C5 checks
- A **resubmission checklist** with MUST FIX and SHOULD FIX items
- De-duplication of overlapping Phase 2 (schema) and Phase 3 (data quality) findings

---

> **Before running:** Complete Phases 1–4 first. Phase 5 reads their JSON output from `output/{ClientName}/`.

---

## Quick Start (Most Common Usage)

```
py run_phase5.py "ClientName" v1
```

**Examples:**
```
py run_phase5.py "Franciscan" v1
py run_phase5.py "Memorial Health" v2
```

---

## Full Command with All Options

```
py run_phase5.py --client "ClientName" --round v1 --output ./output --input ./input
```

| Option | Default | What It Does |
|---|---|---|
| `--client` | (required) | Client name — must match earlier phases |
| `--round` | (required) | Round identifier (e.g., `v1`, `v2`) |
| `--output` | `./output` | Folder where reports are saved |
| `--input` | `./input` | Base folder for client input files — used to load Billing, Scheduling, Payroll, and GL source DataFrames for the Cost Center Summary and Provider Summary sheets (Quality and Patient Satisfaction files are not re-read in Phase 5) |

---

## What You'll See

```
Phase 5 — Results Generation & Reporting
  Client: Franciscan
  Round:  v1

Step 1/4 -- Loading phase findings...
  Loaded 7 file(s) across 7 source(s)
Step 2/4 -- Aggregating and de-duplicating issues...
  All expected core sources present
  206 client issues after de-duplication
Step 3/4 -- Determining readiness...
  Readiness: NEEDS REVISION (Round v2)
Step 4/4 -- Generating report...
```

<details>
<summary>Readiness verdicts and per-source status codes</summary>

**Readiness verdicts:**
| Verdict | What It Means |
|---|---|
| **Ready for Historical Extract** | No CRITICAL or HIGH issues; all core sources present. Client can proceed. |
| **Conditionally Ready** | No CRITICAL issues but has HIGH issues. Client may proceed with caveats. |
| **Needs Revision (Round vX+1)** | Has CRITICAL issues or missing core sources. Client must fix and resubmit. |

**Per-source status:**
| Status | Meaning |
|---|---|
| `PASS` | No CRITICAL or HIGH findings |
| `COND` | Has HIGH findings, no CRITICAL |
| `FAIL` | Has CRITICAL findings |

</details>

---

## Output Files

| File | Description |
|---|---|
| `{Client}_{Round}_Phase5_YYYYMMDD.xlsx` | Full Excel report with 9 sheets |
| `phase5_findings.json` | Machine-readable findings |

<details>
<summary>Excel sheet descriptions (9 sheets)</summary>

1. **Executive Summary** — metadata, readiness verdict, billing format, test month, phase run dates
2. **Source Summary** — per-source status, severity counts, date column, date range
3. **Client Issue List** — all non-deduplicated issues formatted for client delivery
4. **Detailed Findings** — all findings from all phases (deduplicated ones marked)
5. **Cross-Source Validation** — C0–C5 check results
6. **Resubmission Checklist** — prioritized action items
7. **Phase Run Metadata** — dates each phase was run and key summary stats
8. **Cost Center Summary** — one row per unique cost center / department ID found across all sources (Billing, Scheduling, Payroll, GL), with aggregated wRVUs, charges, payments, appointments, payroll hours/amounts, and GL category totals per cost center
9. **Provider Summary** — one row per unique Provider NPI found across all sources (Billing, Scheduling, Payroll, Quality), with aggregated wRVUs, charges, payments, appointments, payroll hours/amounts, and quality record counts per provider

</details>

---

## Common Issues

### "phase1_findings.json not found" (or phase2/3/4)
Run the missing phase first. All four phases must complete before Phase 5 can run.

### Excel file is open / "Permission denied"
Close the existing Phase 5 Excel report before re-running — Excel locks the file while it's open.

### Issue counts differ from individual phase reports
Phase 5 de-duplicates across phases. If Phase 2 flagged a missing field and Phase 3 flagged the same column as 100% null, Phase 5 keeps only the Phase 2 finding. The total will be lower than the sum of individual phase findings.

### Date ranges show "NOTE: GL Report Period is not in the requested YYYYMM format"
The GL file uses a date format other than YYYYMM integers. Phase 1 auto-converted for analysis — the note is informational. Ask the client to use YYYYMM format in future submissions.

---

## Tips

- Run all five phases at once: `py run_all.py "ClientName" v1 --no-prompt`
- The **Client Issue List** sheet is designed to be shared directly with the client.
- The **Resubmission Checklist** sheet provides a prioritized action list to send alongside the issue list.
- If the readiness verdict is "Needs Revision", the next round number is automatically calculated (v1 → v2, v2 → v3).
