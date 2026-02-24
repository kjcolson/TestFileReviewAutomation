# Phase 5: Results Generation & Reporting — Specification

## Purpose

Phase 5 aggregates all findings from Phases 1–4 into a single consolidated deliverable. It produces three output formats: console summary, Excel workbook, and JSON manifest.

---

## Outputs

### Console Summary
An ASCII box-drawn report printed to the terminal containing:
- **Executive Summary** — client name, round, test month, billing format
- **Readiness** — overall verdict (Ready for Historical Extract / Conditionally Ready / Needs Revision)
- **Source Summary** — per-source status (PASS / COND / FAIL) with severity counts (CRITICAL, HIGH, MEDIUM, total)
- **Date Ranges** — per-source date filter column, min date, and max date. If GL data was not in YYYYMM format, a NOTE line explains the auto-conversion.
- **Client Issue List** — sorted by severity (CRITICAL first), showing `[SEVERITY] Source — Description — Affected Rows`
- **Cross-Source Validation** — C0–C5 check summaries
- **Resubmission Checklist** — MUST FIX, SHOULD FIX, and static reminders

### Excel Workbook (`{client}_{round}_Phase5_{YYYYMMDD}.xlsx`)

| Sheet | Contents |
|---|---|
| **Executive Summary** | Client metadata, readiness verdict, billing format, test month, phase run dates |
| **Source Summary** | One row per source: status, CRITICAL/HIGH/MEDIUM/LOW/INFO counts, total issues, date column, date range |
| **Client Issue List** | All non-deduplicated issues formatted for client delivery: severity, source, description, affected rows |
| **Detailed Findings** | All findings from all phases including deduplicated ones (marked with `deduplicated = TRUE`) |
| **Cross-Source Validation** | C0–C5 checks: check ID, label, severity, message, files compared |
| **Resubmission Checklist** | Priority-ordered action items: MUST FIX (CRITICAL), SHOULD FIX (HIGH), static reminders |
| **Phase Run Metadata** | Dates each phase was run, key summary stats |

### JSON Manifest (`phase5_findings.json`)
Machine-readable output containing:
- `readiness` — overall and per-source status
- `client_issues` — formatted issue list
- `checklist` — resubmission items
- `source_summary` — per-source severity counts and `date_range` (min, max, date_column, note)
- `all_findings` — complete finding set with deduplication markers

---

## Readiness Determination

| Verdict | Criteria |
|---|---|
| **Ready for Historical Extract** | 0 CRITICAL, 0 HIGH, all core sources present |
| **Conditionally Ready** | 0 CRITICAL, ≥1 HIGH, all core sources present |
| **Needs Revision (Round vX+1)** | Any CRITICAL, or any core source missing |

Per-source status:
- **FAIL** — any CRITICAL finding
- **COND** — ≥1 HIGH, 0 CRITICAL
- **PASS** — no CRITICAL or HIGH

Core sources: billing, scheduling, payroll, gl, quality. Patient satisfaction is optional.

---

## De-duplication Rules

Phase 2 (schema) and Phase 3 (data quality) can both flag the same column. De-duplication prevents double-counting:

1. **Missing field + 100% null**: If P2 says a staging column is MISSING and P3 flags the same column as null/blank with `missing_pct >= 99.0`, keep the P2 finding and mark P3 as deduplicated.
2. **Datatype + domain format**: If P2 flags a datatype issue and P3 flags a format issue on the same column, keep P2.
3. **Cross-source (P4)**: Never deduplicated against P2/P3 findings.

---

## Date Ranges

Phase 1 records per-file date range information (filter column, min date, max date) in `phase1_findings.json`. Phase 5 merges these into per-source-group ranges:

| Source | Date Filter Column |
|---|---|
| Billing | PostDate |
| Scheduling | ApptDate |
| Payroll | PayPeriodEndDate |
| GL | YearMonth (YYYYMM preferred; other formats auto-converted) |
| Quality | MeasurementPeriodEndDate |
| Patient Satisfaction | SurveyDateRangeStart |

When GL data is not in YYYYMM integer format, Phase 1 auto-converts and adds a note. This note is carried through to Phase 5 and displayed in the console DATE RANGES section and stored in the JSON output.

---

## Checklist Rules

| Priority | Source | Description |
|---|---|---|
| **MUST FIX** | Any CRITICAL finding | One checklist item per CRITICAL issue |
| **SHOULD FIX** | Any HIGH finding | One checklist item per HIGH issue |
| **REMINDER** | Static | Files must be pipe-delimited `.txt` with headers, no footers |
| **REMINDER** | Static | All core test files must cover the same month |
| **REMINDER** | Static | Test file data should be reconciled against an internal report before resubmission |
