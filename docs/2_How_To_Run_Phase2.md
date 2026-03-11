# How to Run Phase 2 — Step-by-Step Guide

Phase 2 reads the results of Phase 1 and performs a database compatibility check: it verifies that the client's data matches the PIVOT staging schema, checks that field values meet SQL type and domain-format requirements, and flags unmapped columns that may be near-misses to known staging fields.

---

## Prerequisites

### Phase 1 must be complete

Phase 2 requires the `phase1_findings.json` file that Phase 1 produces. If you haven't run Phase 1 yet, do that first:

```
py scripts/run_phase1.py --client "AcmeMedical" --round v1
```

The JSON file will be at:
```
output/AcmeMedical/phase1_findings.json
```

If this file is missing, Phase 2 will stop immediately with an error message telling you to run Phase 1 first.

### Source files must still be in `input/`

Phase 2 re-opens the original source files to inspect actual data values. The files must remain in the same `input/{ClientName}/` location they were in when Phase 1 ran.

---

## Running Phase 2

### 1. Open a terminal in the project folder

Navigate to the `TestFileReviewAutomation` project folder in your terminal.

### 2. Run the script

The basic command is:

```
py scripts/run_phase2.py --client "AcmeMedical" --round v1
```

Replace `AcmeMedical` with your client name (must match the folder name used in Phase 1) and `v1` with the correct round number.

**Full list of options:**

| Option | What it does | Default |
|---|---|---|
| `--client` | Client name — must match the input and output subfolder names | `Client` |
| `--round` | Submission round number | `v1` |
| `--input` | Base folder where client input folders live | `./input` |
| `--output` | Base folder where Phase 1 JSON and Phase 2 reports will be written | `./output` |

**Examples:**

First round for a client:
```
py scripts/run_phase2.py --client "AcmeMedical" --round v1
```

Re-run after a client submits corrected files:
```
py scripts/run_phase2.py --client "AcmeMedical" --round v2
```

---

## What Happens When It Runs

The script works through 5 steps and prints progress to the terminal:

```
Step 1/5 — Loading Phase 1 findings and source files...
Step 2/5 — Running schema validation...
Step 3/5 — Classifying fields by requirement level...
Step 4/5 — Running data type and domain checks...
Step 5/5 — Flagging unrecognized columns...
```

After that, it prints a schema validation summary box for each file, then an overall compatibility table.

### Schema validation box (per file)

For each file, the terminal shows:

- **File name, source, and staging table**
- **Field coverage counts** — how many Required / Recommended / Optional template fields are present
- **CRITICAL and HIGH schema issues** — template fields that are missing or cannot be found in the client's mapping
- **Data type issues** — columns where values don't match the expected SQL type, domain format (NPI, ICD-10, ZIP, date, etc.), or null rules
- **Unrecognized columns** — unmapped columns that closely resemble known staging columns (possible aliases or typos)
- **Fuzzy matches needing review** — columns that Phase 1 matched via fuzzy logic, which should be confirmed

### Compatibility summary table

At the end, a table shows the CRITICAL / HIGH / MEDIUM issue counts per file and an overall compatibility verdict:

| Label | Meaning |
|---|---|
| **YES** | No critical or high issues — file is ready to load |
| **YES\*** | No critical issues, but high issues exist — conditionally compatible, review recommended |
| **NO** | One or more critical issues — file cannot be loaded as-is |

---

## Output Files

All output is written to `output/{ClientName}/` alongside the Phase 1 files.

| File | Description |
|---|---|
| `AcmeMedical_v1_Phase2_YYYYMMDD.xlsx` | Full Excel report — 5 sheets (see below) |
| `phase2_findings.json` | Machine-readable summary used by later phases |

### Excel report sheets

| Sheet | What's in it |
|---|---|
| **Schema Validation** | One row per template field per file: requirement level, whether it was found in the client's mapping, severity of any gap, and notes |
| **Schema Summary** | One row per file: Required / Recommended / Optional counts present and missing, plus overall schema coverage percentage |
| **Data Type Checks** | One row per column per file: SQL type, null counts, domain pattern results, severity, and sample invalid values with row indices |
| **Unrecognized Columns** | Unmapped columns from Phase 1, scored for similarity to known staging columns, with severity and nearest match |
| **Compatibility Summary** | One row per file: CRITICAL / HIGH / MEDIUM counts and overall YES / YES* / NO verdict |

---

## Folder Layout Reference

```
TestFileReviewAutomation/
├── input/
│   └── AcmeMedical/
│       ├── billing_charges/
│       │   └── ACME_BillingCharges_202601.txt   <- must still be here for Phase 2
│       ├── gl/
│       │   └── ACME_GL_January2026.csv
│       └── ...
├── output/
│   └── AcmeMedical/
│       ├── AcmeMedical_v1_Phase1_20260219.xlsx
│       ├── phase1_findings.json                  <- required by Phase 2
│       ├── AcmeMedical_v1_Phase2_20260219.xlsx   <- created by Phase 2
│       └── phase2_findings.json                  <- created by Phase 2
├── scripts/
│   ├── run_phase1.py
│   ├── run_phase2.py
│   └── ...
└── ...
```

---

## Common Issues

**"ERROR: phase1_findings.json not found"**
Phase 1 has not been run for this client, or the `--client` name doesn't exactly match the folder used when Phase 1 ran (check capitalization). Run Phase 1 first.

**"WARNING: Input directory not found"**
The source files have been moved or the `--input` path is wrong. Phase 2 will still run schema validation, but data type checks will be skipped because the files can't be re-read.

**Excel file is open / "Permission denied"**
Close the existing Phase 2 Excel report before running — Excel locks the file while it's open.

**Compatibility shows NO for all files**
This is expected when reviewing a first submission — clients rarely submit perfectly formatted files on the first round. Use the CRITICAL findings in the schema and data type sheets to prepare feedback for the client.

**A file that was processed in Phase 1 is missing from Phase 2**
If a source file was moved or renamed after Phase 1 ran, Phase 2 may not be able to re-open it. Keep source files in their original `input/` location until both phases are complete.
