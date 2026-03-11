# Phase 5 Technical Plan — Results Generation & Reporting

## Overview

Phase 5 is the final phase of the PIVOT Test File Review automation pipeline. It reads all four prior phase JSON manifests (`phase1_findings.json` through `phase4_findings.json`) and generates a single consolidated, client-ready deliverable with executive summary, actionable issue list, resubmission checklist, and readiness determination.

Phase 5 does NOT add new validation checks — it only aggregates and presents existing findings.

---

## Dependencies

| Artifact | Source | Used For |
|---|---|---|
| `phase1_findings.json` | Phase 1 output | Metadata: test month, billing format, file inventory, column mappings, per-file date ranges |
| `phase2_findings.json` | Phase 2 output | Schema validation: missing fields, data type issues, compatibility status |
| `phase3_findings.json` | Phase 3 output | Data quality: universal checks + source-specific findings |
| `phase4_findings.json` | Phase 4 output | Cross-source validation: C0–C5 check results |

---

## File Structure

```
phase5/
    __init__.py
    aggregator.py        # Loads all 4 phase JSONs, normalizes into source-centric model
    deduplicator.py      # Cross-phase de-duplication (P2 schema + P3 null overlap)
    issue_formatter.py   # Transforms findings into client-ready issue lines
    readiness.py         # Per-source pass/fail + overall readiness determination
    checklist.py         # Auto-generates resubmission checklist from CRITICAL/HIGH
    missing_sources.py   # Identifies expected but absent data sources
    report.py            # Console + Excel + JSON output
run_phase5.py
```

No changes to `shared/` were required.

---

## Module Responsibilities

### `aggregator.py` — Load & Normalize
- Reads all four phase JSON files from `output/{client}/`
- Builds a unified source-centric model: all findings grouped by source (billing, scheduling, payroll, gl, quality, patient_satisfaction)
- Phase 4 cross-source findings stored separately (they involve 2+ sources)
- Source group mapping: `billing_combined`/`billing_charges`/`billing_transactions` → `"billing"`
- Extracts per-file `date_range` from Phase 1 JSON and merges into per-source-group min/max date ranges
- Generates sequential issue IDs per source group (e.g. B-001, S-001, G-001, X-001 for cross-source)

### `deduplicator.py` — Cross-Phase De-duplication
- If Phase 2 has `status == "MISSING"` for a staging column AND Phase 3 has `null_blank` for the same column with `missing_pct >= 99.0` → keeps Phase 2 finding, marks Phase 3 as deduplicated
- Datatype (P2) + domain format (P3) for same column → keeps P2
- Cross-source (P4) findings are never deduplicated against P2/P3

### `issue_formatter.py` — Client-Ready Issue Lines
- Formats each finding as: `[SEVERITY] Source — Description — Row/Impact Reference`
- Sorted by severity (CRITICAL first), then by source

### `readiness.py` — Readiness Determination
- **Ready for Historical Extract**: 0 CRITICAL, 0 HIGH, all core sources present
- **Conditionally Ready**: 0 CRITICAL, ≥1 HIGH
- **Needs Revision (Round vX+1)**: Any CRITICAL or missing core source
- Per-source pass/fail: FAIL = any CRITICAL, CONDITIONAL = ≥1 HIGH, PASS = neither

### `checklist.py` — Resubmission Checklist
- CRITICAL → "MUST FIX" items
- HIGH → "SHOULD FIX" items
- Static reminders always appended (pipe-delimited format, same month, reconcile internally)

### `missing_sources.py` — Expected Source Detection
- Expected core: billing, scheduling, payroll, gl, quality
- Patient satisfaction is optional (not flagged as missing)

### `report.py` — Console + Excel + JSON Output
- Console: ASCII-only boxes matching Phase 3/4 convention
- Excel: 7 sheets (Executive Summary, Source Summary, Client Issue List, Detailed Findings, Cross-Source Validation, Resubmission Checklist, Phase Run Metadata)
- JSON: `phase5_findings.json` with readiness, client_issues, checklist, all findings
- Date ranges per source displayed in console (DATE RANGES section), Excel (Date Column and Date Range columns on Source Summary sheet), and JSON (`date_range` per source in `source_summary`)

---

## `run_phase5.py` CLI

```
py run_phase5.py "ClientName" v1
py run_phase5.py --client "ClientName" --round v1 [--output ./output]
```

No `--input` or `--knowledge-dir` needed (reads only JSON from output dir).

---

## Output Files

| File | Location |
|---|---|
| `{client}_{round}_Phase5_{YYYYMMDD}.xlsx` | `output/{client}/` |
| `phase5_findings.json` | `output/{client}/` |

### Excel Sheets
- `Executive Summary` — metadata, readiness verdict, phase run dates
- `Source Summary` — per-source status with severity counts, date column, and date range
- `Client Issue List` — actionable issues sorted by severity
- `Detailed Findings` — all findings from all phases (including deduplicated)
- `Cross-Source Validation` — C0–C5 check summary
- `Resubmission Checklist` — prioritized action items
- `Phase Run Metadata` — phase dates and key results

---

## Console Output Format (ASCII-only)

```
+-------------------------------------------------------------------+
| EXECUTIVE SUMMARY                                                 |
+-------------------------------------------------------------------+
| Client               | Franciscan                                 |
| Round                | v1                                         |
| Test Month           | 2025-06                                    |
| Billing Format       | combined                                   |
+-------------------------------------------------------------------+
| READINESS: NEEDS REVISION (Round v2)                              |
+-------------------------------------------------------------------+
| Source               | Status | CRIT | HIGH |  MED | Total        |
+-------------------------------------------------------------------+
| Billing              | FAIL   |   18 |   24 |    7 |           51 |
| Scheduling           | COND   |    0 |    4 |    4 |           10 |
| Payroll              | FAIL   |    8 |   14 |    6 |           30 |
| GL                   | FAIL   |   11 |   12 |    4 |           29 |
| Quality              | FAIL   |    6 |   13 |    6 |           27 |
| Patient Satisfaction | FAIL   |    2 |    9 |    4 |           16 |
| Cross-Source         |        |    0 |    0 |    0 |           43 |
+-------------------------------------------------------------------+
| TOTALS               |        |   45 |   76 |   31 |          206 |
+-------------------------------------------------------------------+
| DATE RANGES                                                       |
+-------------------------------------------------------------------+
|   Billing          PostDate             2025-06-01 to 2025-06-30  |
|   Scheduling       ApptDate             2025-06-01 to 2025-06-30  |
|   Payroll          PayPeriodEndDate     2025-06-01 to 2025-06-30  |
|   GL               YearMonth            2025-06 to 2025-06        |
|     NOTE: GL Report Period is not in the requested YYYYMM fo...   |
|   Quality          MeasurementPeri...   2025-06-30 to 2025-06-30  |
+-------------------------------------------------------------------+
```

---

## `run_all.py` — Run All Phases

A convenience script runs all 5 phases sequentially using subprocess isolation:

```
py run_all.py "ClientName" v1 --no-prompt
py run_all.py --client "ClientName" --round v1 [--input ./input] [--output ./output]
              [--ref ./KnowledgeSources] [--no-prompt]
              [--date-start YYYY-MM-DD] [--date-end YYYY-MM-DD]
```

Stops on first failure with an error message. Prints banners between phases.

---

## Verification (Franciscan v1)

- `py run_phase5.py Franciscan v1` completes without error
- Console shows executive summary with severity counts per source
- Readiness = "NEEDS REVISION (Round v2)" (has CRITICAL issues)
- Client issue list sorted CRITICAL first, then HIGH, then MEDIUM
- Resubmission checklist has MUST FIX for every CRITICAL finding
- De-duplication: P2 schema MISSING + P3 100% null → single issue (206 issues after dedup from ~155 pre-dedup)
- Excel has 7 sheets with correct names
- `phase5_findings.json` is valid JSON with readiness, client_issues, checklist
- Date ranges displayed in console, Excel Source Summary, and JSON
- GL date format mismatch noted with "NOTE:" line in console date ranges
