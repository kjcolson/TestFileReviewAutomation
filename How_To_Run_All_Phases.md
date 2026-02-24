# How to Run All Phases — Full Pipeline Guide

## What Does `run_all.py` Do?

`run_all.py` runs all 5 phases of the PIVOT Test File Review pipeline sequentially in a single command. Each phase runs as a separate subprocess, so if any phase fails, the pipeline stops immediately with an error message.

The phases run in order:
1. **Phase 1** — Initial Setup & Data Ingestion
2. **Phase 2** — Database Compatibility Check
3. **Phase 3** — Data Quality Review
4. **Phase 4** — Cross-Source Validation
5. **Phase 5** — Results Generation & Reporting

---

## Before You Start

Complete the same setup steps as Phase 1:
1. Install Python dependencies: `pip install -r requirements.txt`
2. Create the client's input folder: `input/{ClientName}/`
3. Place files in source subfolders (e.g., `input/{ClientName}/billing_combined/`, `input/{ClientName}/gl/`, etc.)

See `1_How_To_Run_Phase1.md` for detailed setup instructions.

---

## Quick Start

```
py run_all.py "ClientName" v1 --no-prompt
```

The `--no-prompt` flag skips Phase 1's interactive file selection and source confirmation prompts, running everything automatically. This is the recommended way to run the full pipeline.

**Examples:**
```
py run_all.py "Franciscan" v1 --no-prompt
py run_all.py "Memorial Health" v2 --no-prompt
```

---

## Full Command with All Options

```
py run_all.py --client "ClientName" --round v1 --input ./input --output ./output
              --ref ./KnowledgeSources --knowledge-dir ./KnowledgeSources
              --no-prompt --date-start 2025-11-16 --date-end 2025-12-15
```

| Option | Default | What It Does | Used By |
|---|---|---|---|
| `--client` | (required) | Client name — must match the input subfolder name | All phases |
| `--round` | (required) | Round identifier (e.g., `v1`, `v2`) | All phases |
| `--input` | `./input` | Base folder where client input folders live | Phases 1–5 |
| `--output` | `./output` | Base folder where reports are saved | All phases |
| `--ref` | `./KnowledgeSources` | Directory containing reference Excel files | Phase 1 |
| `--knowledge-dir` | `./KnowledgeSources` | Directory containing CMS reference CSVs | Phases 3–4 |
| `--no-prompt` | *(off)* | Skip Phase 1 interactive prompts | Phase 1 |
| `--date-start` | *(none)* | Expected date range start (e.g., `2025-11-16`) | Phase 1 (stored in JSON for Phase 3) |
| `--date-end` | *(none)* | Expected date range end (e.g., `2025-12-15`) | Phase 1 (stored in JSON for Phase 3) |

---

## What You'll See

The script prints a banner before each phase and passes through all phase output:

```
============================================================
  PHASE 1: Initial Setup & Data Ingestion
============================================================

Step 1/5 — Scanning and parsing input files...
  Found 7 file(s):
    ...
Step 5/5 — Identifying test month and checking alignment...
  Test month: 2025-06
  Aligned:    True

============================================================
  PHASE 2: Database Compatibility Check
============================================================

...

============================================================
  PHASE 3: Data Quality Review
============================================================

...

============================================================
  PHASE 4: Cross-Source Validation
============================================================

...

============================================================
  PHASE 5: Results Generation & Reporting
============================================================

...

============================================================
  ALL PHASES COMPLETE — Franciscan v1
============================================================
```

If a phase fails, you'll see:

```
============================================================
  Phase 3 FAILED (exit code 1)
  Pipeline stopped. Fix the issue and re-run.
============================================================
```

---

## Output Files

After a successful run, find all output in `output/{ClientName}/`:

| File | Phase | Description |
|---|---|---|
| `{Client}_{Round}_Phase1_{date}.xlsx` | 1 | File inventory, column mappings, test month |
| `phase1_findings.json` | 1 | Machine-readable Phase 1 summary |
| `{Client}_{Round}_Phase2_{date}.xlsx` | 2 | Schema validation, data type checks |
| `phase2_findings.json` | 2 | Machine-readable Phase 2 summary |
| `{Client}_{Round}_Phase3_{date}.xlsx` | 3 | Data quality findings |
| `phase3_findings.json` | 3 | Machine-readable Phase 3 summary |
| `{Client}_{Round}_Phase4_{date}.xlsx` | 4 | Cross-source validation |
| `phase4_findings.json` | 4 | Machine-readable Phase 4 summary |
| `{Client}_{Round}_Phase5_{date}.xlsx` | 5 | Consolidated report (the main deliverable) |
| `phase5_findings.json` | 5 | Machine-readable final summary |

---

## Common Issues

### Phase fails with "Input directory not found"
The client subfolder doesn't exist inside `input/` or the `--client` name doesn't match. Check capitalization and spaces.

### Phase fails with "phaseN_findings.json not found"
This shouldn't happen when using `run_all.py` since phases run in order. If it does, a prior phase failed silently. Re-run the individual phase to see the error.

### "Permission denied" on Excel files
Close any open Excel reports from previous runs before re-running the pipeline.

### Pipeline stops at Phase 1 waiting for input
You forgot `--no-prompt`. Add it to skip interactive prompts: `py run_all.py "ClientName" v1 --no-prompt`

---

## Tips

- Always use `--no-prompt` for unattended runs. Without it, Phase 1 will pause for interactive file selection.
- Use `--date-start` and `--date-end` if you know the expected date window. Phase 3 will flag records outside this range.
- If only one phase needs re-running (e.g., after fixing code), you can run that phase individually and then re-run Phase 5 to regenerate the consolidated report.
- The Phase 5 Excel report (`*_Phase5_*.xlsx`) is the primary client-facing deliverable. Earlier phase reports are useful for internal debugging.
