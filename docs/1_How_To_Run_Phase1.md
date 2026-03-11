# How to Run Phase 1 — Step-by-Step Guide

Phase 1 scans a client's test files, identifies each data source, maps their columns to the PIVOT staging schema, and produces an Excel report and a JSON summary in `output/`.

**Quick start:** `py run_phase1.py "ClientName" v1 --no-prompt`

---

## One-Time Setup

These steps only need to be done once on a new machine.

### 1. Install Python dependencies

Open a terminal in the project folder and run:

```
pip install -r requirements.txt
```

You should see a success message for each package (`pandas`, `openpyxl`, `rapidfuzz`, `chardet`).

---

## Every-Client Setup

### 2. Create the client's input folder

Inside the `input/` folder, create a subfolder named after the client. The name must match exactly (including capitalization) what you use when running the script.

**Example:**
```
input/
  AcmeMedical/
```

### 3. Create source subfolders (optional but recommended)

Inside the client folder, create one subfolder per data source. Folder names must be spelled exactly as shown.

| Folder name | Use for |
|---|---|
| `billing_combined` | Combined billing file (charges + transactions in one file) |
| `billing_charges` | Separate billing — charge-level file only |
| `billing_transactions` | Separate billing — payment/adjustment/refund file only |
| `scheduling` | Scheduling / appointments file |
| `payroll` | Payroll file |
| `gl` | General ledger file |
| `quality` | Quality measures file |
| `patient_satisfaction` | Patient satisfaction file |

> Alternatively, drop files directly into `input/AcmeMedical/` and the tool will auto-detect the source from column headers.

### 4. Copy the client's files into the correct folders

Drop each file into its matching source subfolder. File names don't matter — only the folder determines the source type.

**Example:**
```
input/
  AcmeMedical/
    billing_charges/
      ACME_BillingCharges_202601.txt
    billing_transactions/
      ACME_BillingTransactions_202601.txt
    gl/
      ACME_GL_January2026.csv
    payroll/
      ACME_Payroll_Jan2026.txt
    scheduling/
      ACME_Scheduling_202601.txt
```

### 5. (Optional) Create a column transform configuration

Some clients encode multiple values in a single field — for example, a composite cost center string like `C0015_C0015_CC20815_403600_16` where the actual cost center ID is the 3rd `_`-delimited segment. A `column_transforms.csv` file lets you define a formula that cleans the raw value before any analysis runs, so every phase sees the corrected value automatically.

**File location:** `input/{ClientName}/column_transforms.csv`

**File format:**

```csv
StagingColumn,Formula
BillDepartmentId,"SPLIT_PART(x,'_',3)"
```

- `StagingColumn` — the staging column name whose raw source column should be transformed
- `Formula` — a SQL-style formula applied to each value; `x` is the placeholder for the current value
- **Important:** any formula containing commas (e.g. `SPLIT_PART`, `SUBSTRING`, `REPLACE`) must be wrapped in double quotes in the CSV

**Supported SQL functions:**

| Function | Syntax | What it does |
|---|---|---|
| `LEFT` | `LEFT(x, n)` | First *n* characters |
| `RIGHT` | `RIGHT(x, n)` | Last *n* characters |
| `SUBSTRING` | `SUBSTRING(x, start, len)` | Substring; 1-indexed. *start* and *len* may be integers, `CHARINDEX`, or `LEN` expressions. |
| `SPLIT_PART` | `SPLIT_PART(x, 'delim', n)` | The *n*th segment when *x* is split by *delim* (1-indexed) |
| `CHARINDEX` | `CHARINDEX('needle', x)` | 1-indexed position of *needle* in *x* (0 if not found) |
| `LEN` | `LEN(x)` | Length of *x* as a string |
| `TRIM` | `TRIM(x)` | Strip leading/trailing whitespace |
| `UPPER` | `UPPER(x)` | Convert to uppercase |
| `LOWER` | `LOWER(x)` | Convert to lowercase |
| `REPLACE` | `REPLACE(x, 'old', 'new')` | Replace all occurrences of *old* with *new* |

Segments may be **concatenated with `||`** — for example: `LEFT(x,5)||SPLIT_PART(x,'_',3)`

<details>
<summary>Advanced: CHARINDEX and LEN inside SUBSTRING</summary>

When the start position or length of a `SUBSTRING` depends on where a character appears, embed `CHARINDEX` or `LEN` directly:

| Pattern | Formula | Example input → output |
|---|---|---|
| Everything before the first `_` | `SUBSTRING(x, 1, CHARINDEX('_', x) - 1)` | `CC20815_403600` → `CC20815` |
| Everything after the first `_` | `SUBSTRING(x, CHARINDEX('_', x) + 1, LEN(x))` | `CC20815_403600` → `403600` |
| Fixed-position extraction | `SUBSTRING(x, 1, 7)` | `CC20815_403600` → `CC20815` |

Arithmetic offsets (`+ n` or `- n`) are supported on both `CHARINDEX` and `LEN` inside `SUBSTRING`.

</details>

When Phase 1 detects a `column_transforms.csv` it prints:
```
  column_transforms.csv found — 1 rule(s): ['BillDepartmentId']
```

The rules are stored in `phase1_findings.json` and applied automatically every time any phase loads the source files.

---

## Running Phase 1

### 6. Open a terminal in the project folder

Navigate to the `TestFileReviewAutomation` project folder in your terminal.

### 7. Run the script

```
py run_phase1.py "AcmeMedical" v1 --no-prompt
```

**Full list of options:**

| Option | What it does | Default |
|---|---|---|
| `--client` | Client name — must match the input subfolder name | `Client` |
| `--round` | Submission round number | `v1` |
| `--input` | Base folder where client input folders live | `./input` |
| `--output` | Base folder where reports will be written | `./output` |
| `--ref` | Directory containing reference Excel files | `./KnowledgeSources` |
| `--no-prompt` | Skip all interactive prompts | *(off)* |
| `--date-start` | Expected date range start (e.g. `2025-11-16`) | *(none)* |
| `--date-end` | Expected date range end (e.g. `2025-12-15`) | *(none)* |

**Examples:**

```
py run_phase1.py "AcmeMedical" v1 --no-prompt
py run_phase1.py "AcmeMedical" v2 --no-prompt --date-start 2025-11-16 --date-end 2025-12-15
```

<details>
<summary>What happens when it runs (terminal output & interactive prompts)</summary>

The script works through 5 steps and prints progress:

```
Step 1/5 — Scanning and parsing input files...
Step 2/5 — Identifying data sources...
Step 3/5 — Determining billing format...
Step 4/5 — Running raw-to-staging column mapping...
Step 5/5 — Identifying test month and checking alignment...
```

After that, it prints a summary box for each file, a test month alignment table, and the paths to the output files.

**Interactive prompts (if `--no-prompt` is not used):**

**File selection:** Lists every file found and asks which ones to process. Type `all` or press Enter to process everything, or enter comma-separated numbers (e.g. `1,3`) to select specific files.

**Source confirmation:** For files placed directly in the client folder (no source subfolder), shows the auto-detected source and asks you to confirm or change it. Press Enter to accept, or enter a number to choose a different source.

</details>

---

## Output Files

All output is written to `output/{ClientName}/` and is created automatically.

| File | Description |
|---|---|
| `AcmeMedical_v1_Phase1_YYYYMMDD.xlsx` | Full Excel report — 5 sheets |
| `phase1_findings.json` | Machine-readable summary used by later phases |

<details>
<summary>Excel sheet descriptions</summary>

| Sheet | What's in it |
|---|---|
| **File Inventory** | One row per file: source detected, staging table, record count, column count, mapping summary, parse issues. For large files (>10,000 rows) a note appears in parse issues: "Large file: sampled first 10,000 of N rows for column analysis" — the record count is still exact. |
| **Column Mappings** | Every raw column mapped to its PIVOT staging column, with confidence level (EXACT / NORMALIZED / FUZZY / UNMAPPED) and SQL type info |
| **Mapping Gaps** | Columns that couldn't be mapped (UNMAPPED), required staging columns with no match (UNCOVERED), and columns that map to two staging targets (DUAL-MAPPED) |
| **Test Month** | Date ranges per file, implied test month, and whether all files align to the same month |
| **Submission Metadata** | Client name, round, date run, test month, billing format, files found/unrecognized |

</details>

---

## Common Issues

**"Input directory not found"**
The client subfolder inside `input/` doesn't exist or the name in `--client` doesn't match exactly (check capitalization and spaces).

**"No .txt or .csv files found"**
No supported files are in the client's input folder or subfolders. Make sure files end in `.txt` or `.csv`.

**File shows source "unknown"**
The file was placed directly in the client folder and the tool couldn't identify it from its columns. Move it into the correct source subfolder, or select the correct source from the interactive menu.

**Excel file is open / "Permission denied"**
Close the existing Excel report before running — Excel locks the file while it's open.

**GL date format note in output**
The GL Report Period column is expected in YYYYMM integer format (e.g. `202506`). If the GL file uses a different format (e.g. `2025-06-30`), Phase 1 auto-converts and adds a note to the JSON. Phase 3 will flag this as a MEDIUM finding (G6) recommending the client switch to YYYYMM format.

---

## Tips

- Phase 1 samples up to 10,000 rows per file for column analysis, so it stays fast on very large files. The row count shown in the output is always exact (full line count). Test month detection reads the complete date column regardless of file size.
- Always use `--no-prompt` for unattended runs.
- Use `--date-start` and `--date-end` when you know the expected date window — Phase 3 will flag records outside this range.
- If only Phase 1 needs re-running, run it individually, then re-run Phase 5 to regenerate the consolidated report.
