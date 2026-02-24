# How to Run Phase 1 — Step-by-Step Guide

Phase 1 scans a client's test files, identifies each data source, maps their columns to the PIVOT staging schema, and produces an Excel report and a JSON summary in `output/`.

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

Inside the `input/` folder, create a subfolder named after the client. Use the exact name you plan to use when running the script — it must match exactly (including capitalization).

**Example:**
```
input/
  AcmeMedical/
```

### 3. Create source subfolders (optional but recommended)

Inside the client folder, create one subfolder per data source you received. The folder names must be spelled exactly as shown below — these names tell the tool what source each file belongs to without needing to auto-detect.

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

**Example after adding source folders:**
```
input/
  AcmeMedical/
    billing_charges/
    billing_transactions/
    gl/
    payroll/
    scheduling/
```

> **If you skip source subfolders:** You can also drop all files directly into `input/AcmeMedical/` and the tool will try to identify the source of each file automatically from its column headers.

### 4. Copy the client's files into the correct folders

Drop each file into its matching source subfolder. The file names do not matter — only the folder they are in determines the source type.

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

Some clients encode multiple values in a single field — for example, a composite cost center string like `C0015_C0015_CC20815_403600_16` where the actual cost center ID is the 3rd `_`-delimited segment. A `column_transforms.csv` file lets you define a formula that transforms the raw value before any analysis runs, so every phase sees the cleaned value automatically.

**File location:** `input/{ClientName}/column_transforms.csv`

**File format:**

```csv
StagingColumn,Formula
BillDepartmentId,"SPLIT_PART(x,'_',3)"
```

- `StagingColumn` — the staging column name whose raw source column should be transformed
- `Formula` — a SQL-style formula applied to each value in that column; `x` is the placeholder for the current value
- **Important:** any formula that contains commas (e.g. `SPLIT_PART`, `SUBSTRING`, `REPLACE`) must be wrapped in double quotes in the CSV file

**Supported SQL functions:**

| Function | Syntax | What it does |
|---|---|---|
| `LEFT` | `LEFT(x, n)` | First *n* characters |
| `RIGHT` | `RIGHT(x, n)` | Last *n* characters |
| `SUBSTRING` | `SUBSTRING(x, start, len)` | Substring; positions are 1-indexed. *start* and *len* may be integers, `CHARINDEX`, or `LEN` expressions (see below). |
| `SPLIT_PART` | `SPLIT_PART(x, 'delim', n)` | The *n*th segment when *x* is split by *delim* (1-indexed) |
| `CHARINDEX` | `CHARINDEX('needle', x)` | 1-indexed position of *needle* within *x* (0 if not found). Can be used standalone or as a position argument inside `SUBSTRING`. |
| `LEN` | `LEN(x)` | Length of *x* as a string. Can be used standalone or as a length argument inside `SUBSTRING`. |
| `TRIM` | `TRIM(x)` | Strip leading/trailing whitespace |
| `UPPER` | `UPPER(x)` | Convert to uppercase |
| `LOWER` | `LOWER(x)` | Convert to lowercase |
| `REPLACE` | `REPLACE(x, 'old', 'new')` | Replace all occurrences of *old* with *new* |

Segments may be **concatenated with `||`** — for example: `LEFT(x,5)||SPLIT_PART(x,'_',3)`

**CHARINDEX and LEN inside SUBSTRING:**

When the start position or length of a `SUBSTRING` depends on where a character appears, you can embed `CHARINDEX` or `LEN` expressions directly:

| Pattern | Formula | Example input → output |
|---|---|---|
| Everything before the first `_` | `SUBSTRING(x, 1, CHARINDEX('_', x) - 1)` | `CC20815_403600` → `CC20815` |
| Everything after the first `_` | `SUBSTRING(x, CHARINDEX('_', x) + 1, LEN(x))` | `CC20815_403600` → `403600` |
| Fixed-position extraction | `SUBSTRING(x, 1, 7)` | `CC20815_403600` → `CC20815` |

Arithmetic offsets (`+ n` or `- n`) are supported on both `CHARINDEX` and `LEN` inside `SUBSTRING`.

**Examples:**

```csv
StagingColumn,Formula
BillDepartmentId,"SPLIT_PART(x,'_',3)"
CostCenterNumberOrig,TRIM(UPPER(x))
BillDepartmentId,"SUBSTRING(x, 1, CHARINDEX('_', x) - 1)"
```

When Phase 1 detects a `column_transforms.csv` it prints a confirmation line:
```
  column_transforms.csv found — 1 rule(s): ['BillDepartmentId']
```

The rules are stored in `phase1_findings.json` and applied automatically every time any phase loads the source files — no further action is needed.

---

## Running Phase 1

### 6. Open a terminal in the project folder

Navigate to the `TestFileReviewAutomation` project folder in your terminal.

### 7. Run the script

The quickest way to run Phase 1 is with positional arguments:

```
py run_phase1.py "AcmeMedical" v1
```

Or equivalently with named arguments:

```
py run_phase1.py --client "AcmeMedical" --round v1
```

Replace `AcmeMedical` with your client name (must match the folder you created in Step 2) and `v1` with the correct round number (`v1`, `v2`, or `v3`).

**Full list of options:**

| Option | What it does | Default |
|---|---|---|
| `--client` | Client name — must match the input subfolder name | `Client` |
| `--round` | Submission round number | `v1` |
| `--input` | Base folder where client input folders live | `./input` |
| `--output` | Base folder where reports will be written | `./output` |
| `--ref` | Directory containing reference Excel files (`RawToStagingColumnMapping.xlsx`, `StagingTableStructure.xlsx`) | `./KnowledgeSources` |
| `--no-prompt` | Skip all interactive prompts; process everything automatically | *(off)* |
| `--date-start` | Expected date range start for all sources (e.g. `2025-11-16`). Stored in JSON for Phase 3 date range checks. | *(none)* |
| `--date-end` | Expected date range end for all sources (e.g. `2025-12-15`). Stored in JSON for Phase 3 date range checks. | *(none)* |

**Examples:**

Run interactively (prompts you to confirm each file and its source):
```
py run_phase1.py --client "AcmeMedical" --round v1
```

Run without any prompts (fully automatic):
```
py run_phase1.py --client "AcmeMedical" --round v1 --no-prompt
```

Run a second round for the same client:
```
python run_phase1.py --client "AcmeMedical" --round v2
```

Specify expected date ranges (used by Phase 3 to flag out-of-range records):
```
py run_phase1.py "AcmeMedical" v1 --no-prompt --date-start 2025-11-16 --date-end 2025-12-15
```

---

## What Happens When It Runs

The script works through 5 steps and prints progress to the terminal:

```
Step 1/5 — Scanning and parsing input files...
Step 2/5 — Identifying data sources...
Step 3/5 — Determining billing format...
Step 4/5 — Running raw-to-staging column mapping...
Step 5/5 — Identifying test month and checking alignment...
```

After that, it prints a summary box for each file, a test month alignment table, and the paths to the output files.

### Interactive prompts (if `--no-prompt` is not used)

**File selection:** The tool lists every file it found and asks which ones you want to process. Type `all` or press Enter to process everything, or enter comma-separated numbers (e.g. `1,3`) to select specific files.

**Source confirmation:** For any file that was placed directly in the client folder (no source subfolder), the tool shows its auto-detected source and asks you to confirm or change it. Press Enter to accept the suggestion, or enter a number from the menu to choose a different source.

---

## Output Files

All output is written to `output/{ClientName}/` and is created automatically.

| File | Description |
|---|---|
| `AcmeMedical_v1_Phase1_YYYYMMDD.xlsx` | Full Excel report — 5 sheets (see below) |
| `phase1_findings.json` | Machine-readable summary used by later phases (includes per-file `date_range` with filter field, min/max dates) |

### Excel report sheets

| Sheet | What's in it |
|---|---|
| **File Inventory** | One row per file: source detected, staging table, record count, column count, mapping summary, parse issues |
| **Column Mappings** | Every raw column mapped to its PIVOT staging column, with confidence level (EXACT / NORMALIZED / FUZZY / UNMAPPED) and SQL type info |
| **Mapping Gaps** | Columns that couldn't be mapped (UNMAPPED), required staging columns with no match (UNCOVERED), and columns that map to two staging targets (DUAL-MAPPED) |
| **Test Month** | Date ranges per file, implied test month, and whether all files align to the same month |
| **Submission Metadata** | Client name, round, date run, test month, billing format, files found/unrecognized |

---

## Folder Layout Reference

```
TestFileReviewAutomation/
├── input/
│   └── AcmeMedical/          ← you create this
│       ├── billing_charges/  ← you create these and drop files in
│       ├── billing_transactions/
│       ├── gl/
│       ├── payroll/
│       └── scheduling/
├── output/
│   └── AcmeMedical/          ← created automatically
│       ├── AcmeMedical_v1_Phase1_20260219.xlsx
│       └── phase1_findings.json
├── run_phase1.py
└── ...
```

---

## Common Issues

**"Input directory not found"**
The client subfolder inside `input/` doesn't exist or the name in `--client` doesn't match the folder name exactly (check capitalization and spaces).

**"No .txt or .csv files found"**
No supported files are in the client's input folder or its subfolders. Make sure files end in `.txt` or `.csv`.

**File shows source "unknown"**
The file was placed directly in the client folder (not in a source subfolder) and the tool couldn't identify it from its columns. Either move it into the correct source subfolder, or when prompted, select the correct source from the menu.

**Excel file is open / "Permission denied"**
Close the existing Excel report before running — Excel locks the file while it's open.

---

## Tips

**GL Date Format Tolerance:** The GL Report Period column is expected in YYYYMM integer format (e.g. `202506`). If the GL file uses a different date format (e.g. `2025-06-30`), Phase 1 will auto-detect the format and convert to YYYYMM for analysis. A note is added to the JSON output, and Phase 3 will flag this as a MEDIUM finding (G6) recommending the client switch to YYYYMM format.

**Date Ranges in JSON:** The `phase1_findings.json` output includes a `date_range` object per file containing the filter column name, min date, and max date. Phase 5 aggregates these into per-source date ranges displayed in the results summary.
