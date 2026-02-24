# Getting Started — PIVOT Test File Review

## Prerequisites

- **Python 3.10 or later** — download from [python.org](https://www.python.org/downloads/)
  - During installation, check **"Add Python to PATH"**
- **Git** (optional) — needed only to clone; team members without Git can download the repo as a ZIP from GitHub

---

## One-Time Setup

1. **Clone the repo** (or download and unzip it):
   ```
   git clone https://github.com/your-org/TestFileReviewAutomation.git
   cd TestFileReviewAutomation
   ```

2. **Run setup** — double-click `setup.bat`, or from a terminal:
   ```
   setup.bat
   ```
   This installs the required Python packages and creates the `input\` and `output\` folders.

---

## Running a Client

### Step 1 — Create the client's input folder

```
input\
  AcmeMedical\
    billing_combined\
    gl\
    payroll\
    scheduling\
```

Folder names for source subfolders must match exactly:
`billing_combined`, `billing_charges`, `billing_transactions`, `scheduling`, `payroll`, `gl`, `quality`, `patient_satisfaction`

### Step 2 — Drop the client's files in

Copy each file into its matching source subfolder. File names don't matter — only the folder determines the source type.

### Step 3 — Run all phases

Open a terminal in the project folder and run:

```
py run_all.py "AcmeMedical" v1 --no-prompt
```

Replace `AcmeMedical` with the client name (must match the folder you created) and `v1` with the round number.

---

## Output

All reports are written to `output\{ClientName}\`:

| File | Description |
|---|---|
| `*_Phase5_*.xlsx` | **The main deliverable** — 9-sheet consolidated report |
| `*_Phase1_*.xlsx` through `*_Phase4_*.xlsx` | Per-phase detail (for internal review) |

---

## More Details

| Guide | What it covers |
|---|---|
| [1_How_To_Run_Phase1.md](1_How_To_Run_Phase1.md) | Full Phase 1 options, source subfolders, column transforms config |
| [How_To_Run_All_Phases.md](How_To_Run_All_Phases.md) | All command-line options for the full pipeline |
| [5_How_To_Run_Phase5.md](5_How_To_Run_Phase5.md) | Phase 5 report sheets and options |
| [ReadMe.md](ReadMe.md) | Project overview and field reference |
