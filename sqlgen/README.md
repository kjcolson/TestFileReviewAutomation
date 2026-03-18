# SQL Generation Module (`sqlgen/`)

## Overview

The `sqlgen` module automates the final step of the PIVOT client onboarding workflow. After test files pass validation (Phases 1–5), this module reads the Phase 1 findings and user-provided parameters to generate three ready-to-run artifacts:

| Artifact | Destination | Purpose |
|----------|-------------|---------|
| `{client}_config.sql` | Run in client SQL Server database | Populates `ds.DataSource`, `ds.DataSourceFile`, `dl.RawColumn`, `dl.ColumnMapping` |
| `cst.dl{NN}_{Entity}.sql` | Add to `inflow-db-cst` repo | Client-specific transformation stored procedure(s) |
| `{client}_liquibase.xml` | Append to `inflow-db-cst/changelog/release_scripts/` | Liquibase changeset entries for the transformation sprocs |

---

## Background: The Manual Process This Replaces

Before this module, after a client's test files passed review, a developer had to manually:

1. Write `INSERT` statements to populate `ds.DataSource` and `ds.DataSourceFile` in the client database
2. Run `MapColumnsNewClientFileSimplified.sql` to populate `dl.RawColumn` and `dl.ColumnMapping`
3. Hand-craft each `cst.dl{NN}_{Entity}.sql` transformation sproc, writing the client-specific `UPDATE #Raw SET` statements for each calculated column
4. Add calculated field entries via `AddNewCalculatedField.sql` for any column transforms
5. Write Liquibase XML changeset entries and add them to the client's release file

This process was tedious, error-prone, and duplicated information Phase 1 already captured (column names, data types, delimiter, encoding, source type, transforms).

---

## How It Works

```
Phase 1 findings JSON
   └─ column_mappings      ──┐
   └─ source type            ├─► sqlgen/generator.py ──► 3 output files
   └─ delimiter / encoding   │
   └─ col_count              │
   └─ column_transforms   ───┘
User-provided parameters
   └─ Client ID (4-digit)
   └─ Data Source Number per file
   └─ SFTP folder paths
   └─ File name patterns
   └─ Raw database name
```

### Data Flow

1. `generator.py` loads `output/{client}/phase1_findings.json` and user parameters
2. `config_sql.py` generates the client database configuration SQL
3. `load_sproc.py` generates each transformation stored procedure
4. `liquibase_xml.py` generates the Liquibase changeset XML snippet
5. All files are written to `output/{client}/sqlgen/`

---

## Module Files

| File | Purpose |
|------|---------|
| `constants.py` | Source type → `StdExtractTypeFnbr` mappings, default data source numbers, staging table names |
| `config_sql.py` | Generates `{client}_config.sql` — populates all four inflow-db-client config tables |
| `load_sproc.py` | Generates each `cst.dl{NN}_{Entity}.sql` transformation sproc |
| `liquibase_xml.py` | Generates Liquibase `<changeSet>` XML entries |
| `generator.py` | Main orchestrator — called by the API route |

---

## Output: Transformation Sproc (`cst.dl{NN}_{Entity}.sql`)

These are the **client-specific transformation layer** sprocs that live in `inflow-db-cst`. They receive a pre-populated `#Raw` temp table from the Inflow framework parent sproc and apply `UPDATE` statements to populate the `_Calculated` columns.

The framework parent handles: BULK INSERT, `#Raw` creation, dimension/fact INSERTs, and `ds.LoadLog`. The generated sproc contains only the logic that differs per client.

### Sproc Signature

```sql
CREATE OR ALTER PROC cst.dl{NN}_{Entity}
    @LoadId      VARCHAR(36),
    @ParentSproc VARCHAR(50),
    @UserId      VARCHAR(50)
AS
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN
    ...
    BEGIN TRY /*Client Specific Transformations*/
        UPDATE #Raw
        SET
            [DateOfService_Calculated] = CONVERT(DATE, ...),
            [BillDepartmentId_Calculated] = REPLACE(...);
    END TRY
    BEGIN CATCH
        THROW 500007, @ErrorText, 0;
    END CATCH;
END;
```

If there are no transforms, the body logs: `'No client specific transformations.'`

### Naming Convention

| DS# | Source type | Generated sproc |
|-----|-------------|-----------------|
| 01 | `billing_charges` | `cst.dl01_Charges` |
| 01 | `billing_transactions` | `cst.dl01_Transactions` |
| 02 | `payroll` | `cst.dl02_Payroll` |
| 03 | `gl` | `cst.dl03_GeneralLedger` |
| 04 | `scheduling` | `cst.dl04_Scheduling` |

### Calculated Field Handling

**Column transforms** (`column_transforms` in `phase1_findings.json`) — SQL formulas identified during Phase 1:

```sql
UPDATE #Raw
SET
    [InvoiceNumber_Calculated] = REPLACE(REPLACE([Invoice_Number], CHAR(13), ''), CHAR(10), ''),
    [DateOfService_Calculated] = CONVERT(DATE, CONCAT(RIGHT([Date_of_Service], 4), ...));
```

**Uncovered required columns** (`uncovered_staging.required`) — required staging columns with no raw mapping get TODO placeholders:

```sql
-- TODO: UPDATE #Raw SET [PatientAge_Calculated] = ???;
```

Both follow `AddNewCalculatedField.sql`: `_Calculated` suffix, `InClientFile = 0`, `FlagForCleanup = 1`.

---

## Output: Config SQL (`{client}_config.sql`)

Structured in labeled sections, each wrapped in its own transaction for safe SSMS review.

### Script Structure

```sql
/*===== PRE-FLIGHT CHECKS =====*/
-- Database context warning
-- StagingTableStructure validation

/*===== SECTION 1: ds.DataSource =====*/
BEGIN TRAN;
  -- INSERT with NOT EXISTS guard
  SELECT * FROM ds.DataSource WHERE SourceName = 'Payroll';  -- verify
ROLLBACK; -- ← replace with COMMIT after review

/*===== SECTION 2: ds.DataSourceFile =====*/
BEGIN TRAN;
  -- INSERT with NOT EXISTS guard (SFTP path, load sproc = cst.dl{NN}_{Entity}, delimiter, etc.)
  SELECT * FROM ds.DataSourceFile WHERE SourceFnbr = @SourceUnbr;
ROLLBACK;

/*===== SECTION 3: dl.RawColumn =====*/
BEGIN TRAN;
  -- INSERT one row per column from Phase 1 column_mappings
  -- INSERT one row per calculated field (InClientFile = 0)
  SELECT * FROM dl.RawColumn WHERE DataSourceFileFnbr = @DataSourceFileUnbr;
ROLLBACK;

/*===== SECTION 4: dl.ColumnMapping =====*/
BEGIN TRAN;
  DROP TABLE IF EXISTS #PreMapping;
  CREATE TABLE #PreMapping (...);
  INSERT INTO #PreMapping VALUES (...);  -- generated from Phase 1 mappings
  -- Validation: throws if raw column or staging column not found
  INSERT INTO dl.ColumnMapping ...
  SELECT * FROM dl.ColumnMapping ...;   -- verify
ROLLBACK;

/*===== POST-FLIGHT VERIFICATION =====*/
-- Full JOIN query: DataSource → DataSourceFile → RawColumn → ColumnMapping → StagingTableStructure
```

---

## Output: Liquibase XML (`{client}_liquibase.xml`)

A snippet ready to append to `inflow-db-cst/changelog/release_scripts/{ClientId}_{ClientName}.xml`:

```xml
<!-- Generated by TestFileReviewAutomation sqlgen — append to release_scripts/{ClientId}_{ClientName}.xml -->
<changeSet id="0073_Ardent.cst.dl02_Payroll"
           author="Liquibase CST" context="0073_Ardent" runOnChange="true">
  <sqlFile encoding="UTF-8"
           path="../database_objects/0073_Ardent/Stored Procedures/cst.dl02_Payroll.sql"
           relativeToChangelogFile="true" splitStatements="true" stripComments="false" endDelimiter="GO" />
</changeSet>
```

One `<changeSet>` block is generated per transformation sproc file.

---

## Source Type Mappings

| Phase 1 `source` | Entity name | `StdExtractTypeFnbr` | Default DS# | Staging Table |
|-----------------|-------------|---------------------|-------------|---------------|
| `billing_charges` | Charges | 1 | 01 | `#staging_charges` |
| `billing_transactions` | Transactions | 1 | 01 | `#staging_transactions` |
| `billing_combined` | ChargeAndTransactionData | 1 | 01 | `#staging_billing` |
| `payroll` | Payroll | 3 | 02 | `#staging_payroll` |
| `gl` | GeneralLedger | 2 | 03 | `#staging_gl` |
| `scheduling` | Scheduling | 4 | 04 | `#staging_scheduling` |

---

## Output Location

All generated files are written to:
```
output/{ClientName}/sqlgen/
├── {ClientName}_config.sql              ← run in client database
├── cst.dl01_Charges.sql                 ← add to inflow-db-cst
├── cst.dl01_Transactions.sql
├── cst.dl02_Payroll.sql
├── {ClientName}_liquibase.xml           ← append to release_scripts/
└── generation_summary.json             ← metadata about what was generated
```

---

## Integration with the Dashboard

After Phase 5 runs, navigate to the client in the dashboard and click **"Generate SQL"**. The form pre-fills defaults from Phase 1 findings. After reviewing and confirming parameters, generation runs instantly and output files are available for download directly from the dashboard.

The API endpoint driving this is `POST /api/sqlgen/generate`.

---

## Integration with inflow-db-cst

After generation:

1. Copy the `cst.dl{NN}_{Entity}.sql` file(s) to:
   ```
   inflow-db-cst/changelog/database_objects/{ClientId}_{ClientName}/Stored Procedures/
   ```
2. Append the XML snippet from `{client}_liquibase.xml` to:
   ```
   inflow-db-cst/changelog/release_scripts/{ClientId}_{ClientName}.xml
   ```
3. Commit and push — the CI/CD pipeline will validate and deploy via Liquibase.

If the client folder does not yet exist in `inflow-db-cst`, run `GenerateFolders.ps1` first (see `inflow-db-cst/changelog/database_objects/Meta/`).

---

## Reference Scripts

These existing scripts informed the design of this module:

| Script | Pattern Used |
|--------|-------------|
| `MapColumnsNewClientFileSimplified.sql` | `#PreMapping` temp table with validation guards for `dl.ColumnMapping` inserts |
| `AddNewCalculatedField.sql` | `_Calculated` suffix, `InClientFile = 0`, `FlagForCleanup = 1`, `UPDATE #Raw` pattern |
| `inflow-db-cst/.../cst.dl02_Payroll.sql` | Modern transformation sproc structure |
| `inflow-db-cst/changelog/release_scripts/0005_PresbyterianHealthSystem.xml` | Liquibase changeset XML format |
