"""
sqlgen/config_sql.py

Generates the client database configuration SQL script that populates:
  - ds.DataSource
  - ds.DataSourceFile
  - dl.RawColumn  (one row per raw file column + calculated fields)
  - dl.ColumnMapping  (via #PreMapping pattern from MapColumnsNewClientFileSimplified.sql)

The script uses BEGIN TRAN / ROLLBACK guards so users review before committing.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlgen.constants import (
    SOURCE_TO_EXTRACT_TYPE,
    SOURCE_TO_FILE_TYPE_FNBR,
    sql_col_type,
)


def generate(
    client: str,
    client_id: str,
    round_: str,
    files: dict[str, dict[str, Any]],   # filename → FileParams (see generator.py)
    phase1: dict[str, Any],
) -> str:
    """Return the full config SQL script as a string."""
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines: list[str] = []

    # ── Header ───────────────────────────────────────────────────────────────
    lines += [
        "-- ================================================================",
        f"-- PIVOT SQL CONFIGURATION SCRIPT",
        f"-- Client : {client} (ID: {client_id})",
        f"-- Generated: {now}",
        f"-- Source  : TestFileReviewAutomation Phase 1 findings ({round_})",
        "-- ================================================================",
        "-- INSTRUCTIONS:",
        "--   1. Open in SSMS connected to the CLIENT database",
        f"--      (e.g., {client_id}_{client.replace(' ', '')})",
        "--   2. Run the pre-flight checks first and confirm they pass.",
        "--   3. Execute each section, review the verification SELECT, then",
        "--      replace ROLLBACK with COMMIT and re-run to persist.",
        "--   4. Run the post-flight verification at the end to confirm.",
        "-- ================================================================",
        "",
    ]

    # ── Pre-flight checks ────────────────────────────────────────────────────
    lines += [
        "/*===== PRE-FLIGHT CHECKS =====*/",
        "",
        "-- 1. Verify you are connected to the correct database",
        f"IF DB_NAME() NOT LIKE '%{client.replace(' ', '')}%' AND DB_NAME() NOT LIKE '%{client_id}%'",
        "    PRINT 'WARNING: Database name does not appear to match the expected client. Verify your connection.';",
        "ELSE",
        "    PRINT 'Database context: ' + DB_NAME();",
        "",
    ]

    # Helper: look up per-file Phase 1 metadata by filename
    _phase1_files_raw = phase1.get("files", {})

    def _get_file_meta(fname: str) -> dict:
        if isinstance(_phase1_files_raw, dict):
            return _phase1_files_raw.get(fname, {})
        if isinstance(_phase1_files_raw, list):
            return next((f for f in _phase1_files_raw if f.get("filename") == fname), {})
        return {}

    # Collect unique staging tables across all files
    all_staging_tables: set[str] = set()
    for fname, fparams in files.items():
        file_meta = _get_file_meta(fname)
        stbl = file_meta.get("staging_table", fparams.get("staging_table", ""))
        if stbl:
            all_staging_tables.add(stbl)

    for stbl in sorted(all_staging_tables):
        lines += [
            f"-- 2. Verify std.StagingTableStructure has rows for {stbl}",
            "DECLARE @StsCount INT;",
            f"SELECT @StsCount = COUNT(*) FROM std.StagingTableStructure WHERE Staging_Table = '{stbl}';",
            f"PRINT 'StagingTableStructure rows for {stbl}: ' + CAST(@StsCount AS VARCHAR(10));",
            f"IF @StsCount = 0",
            f"    RAISERROR('ERROR: No StagingTableStructure rows found for {stbl}. Cannot generate column mappings.', 16, 1);",
            "",
        ]

    lines.append("")

    # ── Per-file sections ─────────────────────────────────────────────────────
    for fname, fparams in files.items():
        file_meta = _get_file_meta(fname)
        if not file_meta:
            continue

        source = file_meta.get("source", "unknown")
        staging_table = file_meta.get("staging_table", "")
        delimiter = file_meta.get("delimiter", "|")
        col_count = file_meta.get("col_count", 0)
        column_mappings: list[dict] = file_meta.get("column_mappings", [])
        column_transforms: list[dict] = phase1.get("column_transforms", [])
        uncovered_required: list[str] = file_meta.get("uncovered_staging", {}).get("required", [])

        ds_number = fparams.get("ds_number", 1)
        source_name = fparams.get("source_name", source.replace("_", " ").title())
        entity = fparams.get("entity", source_name.replace(" ", ""))
        sftp_folder = fparams.get("sftp_folder", "")
        loaded_folder = fparams.get("loaded_folder", "")
        file_name_like = fparams.get("file_name_like", f"%{fname.split('.')[0]}%")
        load_sproc = fparams.get("load_sproc", f"cst.dl{ds_number:02d}_{entity}")
        file_extension = fparams.get("file_extension", "." + fname.rsplit(".", 1)[-1] if "." in fname else ".txt")
        row_terminator = fparams.get("row_terminator", "0x0a")
        automated_load = 1 if fparams.get("automated_load", False) else 0
        daily_load = 1 if fparams.get("daily_load", False) else 0
        raw_db = fparams.get("raw_db_name", f"{client_id}_{client.replace(' ', '')}_Raw")
        std_extract_type = SOURCE_TO_EXTRACT_TYPE.get(source, 1)
        file_type_fnbr = SOURCE_TO_FILE_TYPE_FNBR.get(source, 1)

        header_rows = file_meta.get("header_rows", 1)
        first_row = header_rows + 1

        section = fname
        lines += [
            f"-- ================================================================",
            f"-- FILE: {fname}",
            f"-- Source type : {source}",
            f"-- Staging table: {staging_table}",
            f"-- Delimiter    : {delimiter}",
            f"-- Columns      : {col_count}",
            f"-- ================================================================",
            "",
        ]

        # ── SECTION 1: ds.DataSource ──────────────────────────────────────────
        lines += [
            f"/*===== SECTION 1: ds.DataSource — {source_name} =====*/",
            "BEGIN TRAN;",
            "",
            "DECLARE @SourceUnbr INT;",
            "",
            f"IF NOT EXISTS (SELECT 1 FROM ds.DataSource WHERE SourceName = '{source_name}')",
            "BEGIN",
            "    INSERT INTO ds.DataSource",
            "        (SourceName, SourceDesc, FileName, StdExtractTypeFnbr, IncludeInAnalysis, Notes)",
            "    VALUES",
            "    (",
            f"        '{source_name}',",
            f"        '{source_name} data for {client}',",
            f"        '{file_name_like}',",
            f"        {std_extract_type},  -- StdExtractTypeFnbr: 1=Billing, 2=GL, 3=Payroll, 4=Scheduling",
            "        1,",
            "        NULL",
            "    );",
            "    PRINT 'INSERTED ds.DataSource: ' + CAST(SCOPE_IDENTITY() AS VARCHAR);",
            "END",
            "ELSE",
            "    PRINT 'SKIPPED ds.DataSource (already exists): ' + '{source_name}';",
            "",
            f"SELECT @SourceUnbr = SourceUnbr FROM ds.DataSource WHERE SourceName = '{source_name}';",
            "",
            "-- Verify",
            "SELECT 'ds.DataSource' AS [Table], SourceUnbr, SourceName, StdExtractTypeFnbr, FileName",
            f"FROM ds.DataSource WHERE SourceName = '{source_name}';",
            "",
            "ROLLBACK; -- ← Review the SELECT above, then replace ROLLBACK with COMMIT",
            "",
        ]

        # ── SECTION 2: ds.DataSourceFile ─────────────────────────────────────
        lines += [
            f"/*===== SECTION 2: ds.DataSourceFile — {source_name} =====*/",
            "BEGIN TRAN;",
            "",
            "DECLARE @DataSourceFileUnbr INT;",
            f"SELECT @SourceUnbr = SourceUnbr FROM ds.DataSource WHERE SourceName = '{source_name}';",
            "",
            "IF NOT EXISTS (",
            "    SELECT 1 FROM ds.DataSourceFile",
            "    WHERE SourceFnbr = @SourceUnbr",
            f"    AND FileNameLike = '{file_name_like}'",
            ")",
            "BEGIN",
            "    INSERT INTO ds.DataSourceFile",
            "    (",
            "         SourceFnbr",
            "        ,AutomatedLoad",
            "        ,DailyLoad",
            "        ,FileSftpFolder",
            "        ,LoadedFolder",
            "        ,FileNameLike",
            "        ,LoadSproc",
            "        ,FileExtension",
            "        ,FileDelimiter",
            "        ,FileHeaderRow",
            "        ,FileFirstRow",
            "        ,FileColumnCount",
            "        ,DataSourceFileTypeFnbr",
            "        ,FileRowTerminator",
            "        ,ValidateVsPriorLoad",
            "    )",
            "    VALUES",
            "    (",
            "         @SourceUnbr",
            f"        ,{automated_load}",
            f"        ,{daily_load}",
            f"        ,'{sftp_folder}'",
            f"        ,'{loaded_folder}'",
            f"        ,'{file_name_like}'",
            f"        ,'{load_sproc}'",
            f"        ,'{file_extension}'",
            f"        ,'|'",
            f"        ,{header_rows}",
            f"        ,{first_row}",
            f"        ,{col_count}",
            f"        ,{file_type_fnbr}",
            f"        ,'{row_terminator}'",
            "        ,1",
            "    );",
            "    PRINT 'INSERTED ds.DataSourceFile: ' + CAST(SCOPE_IDENTITY() AS VARCHAR);",
            "END",
            "ELSE",
            "    PRINT 'SKIPPED ds.DataSourceFile (already exists)';",
            "",
            "SELECT @DataSourceFileUnbr = Unbr FROM ds.DataSourceFile",
            f"WHERE SourceFnbr = @SourceUnbr AND FileNameLike = '{file_name_like}';",
            "",
            "-- Verify",
            "SELECT 'ds.DataSourceFile' AS [Table], Unbr, SourceFnbr, FileNameLike, LoadSproc,",
            "       FileDelimiter, FileColumnCount, FileHeaderRow, FileSftpFolder",
            "FROM ds.DataSourceFile WHERE SourceFnbr = @SourceUnbr;",
            "",
            "ROLLBACK; -- ← Review, then replace with COMMIT",
            "",
        ]

        # ── SECTION 3: dl.RawColumn ───────────────────────────────────────────
        lines += [
            f"/*===== SECTION 3: dl.RawColumn — {source_name} =====*/",
            "BEGIN TRAN;",
            "",
            f"SELECT @SourceUnbr = SourceUnbr FROM ds.DataSource WHERE SourceName = '{source_name}';",
            "SELECT @DataSourceFileUnbr = Unbr FROM ds.DataSourceFile",
            f"WHERE SourceFnbr = @SourceUnbr AND FileNameLike = '{file_name_like}';",
            "",
            "-- Client file columns (InClientFile = 1)",
        ]

        # Build raw column inserts — one per mapped column in order
        for i, m in enumerate(column_mappings):
            raw_col = m["raw_col"].replace("'", "''")
            lines += [
                f"IF NOT EXISTS (SELECT 1 FROM dl.RawColumn WHERE ColumnName = '{raw_col}' AND DataSourceFileFnbr = @DataSourceFileUnbr)",
                f"    INSERT INTO dl.RawColumn (ColumnName, ColumnOrder, DataSourceFileFnbr, InClientFile)",
                f"    VALUES ('{raw_col}', {i + 1}, @DataSourceFileUnbr, 1);",
            ]

        # Calculated fields from column_transforms
        transforms_for_file = [t for t in column_transforms if t.get("file") == fname or not t.get("file")]
        if transforms_for_file:
            lines.append("")
            lines.append("-- Calculated columns from column_transforms (InClientFile = 0)")
            for t in transforms_for_file:
                sc = t.get("staging_col") or t.get("staging_column", "")
                if not sc:
                    continue
                calc_col = f"{sc}_Calculated".replace("'", "''")
                lines += [
                    f"IF NOT EXISTS (SELECT 1 FROM dl.RawColumn WHERE ColumnName = '{calc_col}' AND DataSourceFileFnbr = @DataSourceFileUnbr)",
                    f"    INSERT INTO dl.RawColumn (ColumnName, ColumnOrder, DataSourceFileFnbr, InClientFile)",
                    f"    VALUES ('{calc_col}', 0, @DataSourceFileUnbr, 0);",
                ]

        # Placeholder calculated fields for uncovered required staging columns
        if uncovered_required:
            lines.append("")
            lines.append("-- ⚠ Placeholder calculated columns for required staging columns with no raw mapping")
            lines.append("-- These MUST be implemented in the load sproc before going to production.")
            for staging_col in uncovered_required:
                calc_col = f"{staging_col}_Calculated".replace("'", "''")
                lines += [
                    f"IF NOT EXISTS (SELECT 1 FROM dl.RawColumn WHERE ColumnName = '{calc_col}' AND DataSourceFileFnbr = @DataSourceFileUnbr)",
                    f"    INSERT INTO dl.RawColumn (ColumnName, ColumnOrder, DataSourceFileFnbr, InClientFile)",
                    f"    VALUES ('{calc_col}', 0, @DataSourceFileUnbr, 0);",
                ]

        rc_count_expected = len(column_mappings) + len(transforms_for_file) + len(uncovered_required)
        lines += [
            "",
            f"PRINT 'dl.RawColumn rows inserted (expected ~{rc_count_expected}): ' + CAST(@@ROWCOUNT AS VARCHAR);",
            "",
            "-- Verify",
            "SELECT 'dl.RawColumn' AS [Table], Unbr, ColumnName, ColumnOrder, InClientFile",
            "FROM dl.RawColumn WHERE DataSourceFileFnbr = @DataSourceFileUnbr",
            "ORDER BY InClientFile DESC, ColumnOrder;",
            "",
            "ROLLBACK; -- ← Review, then replace with COMMIT",
            "",
        ]

        # ── SECTION 4: dl.ColumnMapping ──────────────────────────────────────
        lines += [
            f"/*===== SECTION 4: dl.ColumnMapping — {source_name} =====*/",
            "-- Uses the #PreMapping pattern with validation guards.",
            "-- Pattern from MapColumnsNewClientFileSimplified.sql.",
            "BEGIN TRAN;",
            "",
            f"SELECT @SourceUnbr = SourceUnbr FROM ds.DataSource WHERE SourceName = '{source_name}';",
            "SELECT @DataSourceFileUnbr = Unbr FROM ds.DataSourceFile",
            f"WHERE SourceFnbr = @SourceUnbr AND FileNameLike = '{file_name_like}';",
            "",
            "DROP TABLE IF EXISTS #PreMapping;",
            "CREATE TABLE #PreMapping",
            "(",
            "     RawColumnName    VARCHAR(100)",
            "    ,StagingColumnName VARCHAR(100)",
            "    ,StagingTableName  VARCHAR(40)",
            "    ,CleanupRequest    VARCHAR(1000)",
            ");",
            "",
            "INSERT INTO #PreMapping (RawColumnName, StagingColumnName, StagingTableName, CleanupRequest)",
            "VALUES",
        ]

        # Build VALUES rows from column_mappings
        value_rows: list[str] = []

        # Normal mapped columns — handle dual-mapping (staging_cols)
        for m in column_mappings:
            raw_col = m["raw_col"].replace("'", "''")
            _fallback = [m["staging_col"]] if m.get("staging_col") else []
            for stg_col in m.get("staging_cols", _fallback):
                cleanup = m.get("notes", "") or ""
                cleanup_sql = f"'{cleanup.replace(chr(39), chr(39)+chr(39))}'" if cleanup else "NULL"
                value_rows.append(f"    ('{raw_col}', '{stg_col}', '{staging_table}', {cleanup_sql})")

        # Calculated columns from transforms
        for t in transforms_for_file:
            sc = t.get("staging_col") or t.get("staging_column", "")
            if not sc:
                continue
            calc_col = f"{sc}_Calculated".replace("'", "''")
            formula = t.get("formula", t.get("expression", "")).replace("'", "''")
            value_rows.append(f"    ('{calc_col}', '{sc}', '{staging_table}', '{formula}')")

        # Placeholder calculated columns for uncovered required staging columns
        for staging_col in uncovered_required:
            calc_col = f"{staging_col}_Calculated".replace("'", "''")
            value_rows.append(
                f"    ('{calc_col}', '{staging_col}', '{staging_table}', 'TODO: Calculate {staging_col}')"
            )

        if value_rows:
            lines.append(",\n".join(value_rows) + ";")
        else:
            lines.append("-- (no mappings to insert)")

        lines += [
            "",
            "-- Validation: throw if any raw column in #PreMapping doesn't exist in dl.RawColumn",
            "DECLARE @MissingRaw VARCHAR(1000);",
            "SELECT @MissingRaw =",
            "    'Raw columns not found in dl.RawColumn (insert them first): '",
            "    + STRING_AGG(pm.RawColumnName, ', ')",
            "FROM #PreMapping pm",
            "    LEFT JOIN dl.RawColumn rc",
            "        ON rc.ColumnName = pm.RawColumnName",
            "        AND rc.DataSourceFileFnbr = @DataSourceFileUnbr",
            "WHERE rc.Unbr IS NULL;",
            "IF @MissingRaw IS NOT NULL",
            "    THROW 50000, @MissingRaw, 1;",
            "",
            "-- Validation: throw if any staging column in #PreMapping doesn't exist in std.StagingTableStructure",
            "DECLARE @MissingStg VARCHAR(1000);",
            "SELECT @MissingStg =",
            "    'Staging columns not found in std.StagingTableStructure: '",
            "    + STRING_AGG(CONCAT(pm.StagingColumnName, ' (', pm.StagingTableName, ')'), ', ')",
            "FROM #PreMapping pm",
            "    LEFT JOIN std.StagingTableStructure sts",
            "        ON sts.Source_Column = pm.StagingColumnName",
            "        AND sts.Staging_Table = pm.StagingTableName",
            "WHERE sts.Unbr IS NULL;",
            "IF @MissingStg IS NOT NULL",
            "    THROW 50000, @MissingStg, 1;",
            "",
            "-- Insert column mappings (skip any that already exist)",
            "INSERT INTO dl.ColumnMapping (StagingTableStructureFnbr, RawColumnFnbr, FlagForCleanup, CleanupRequest)",
            "SELECT",
            "     sts.Unbr",
            "    ,rc.Unbr",
            "    ,CASE WHEN pm.CleanupRequest IS NOT NULL THEN 1 ELSE 0 END",
            "    ,pm.CleanupRequest",
            "FROM #PreMapping pm",
            "    INNER JOIN std.StagingTableStructure sts",
            "        ON sts.Source_Column = pm.StagingColumnName",
            "        AND sts.Staging_Table = pm.StagingTableName",
            "    INNER JOIN dl.RawColumn rc",
            "        ON rc.ColumnName = pm.RawColumnName",
            "        AND rc.DataSourceFileFnbr = @DataSourceFileUnbr",
            "    LEFT JOIN dl.ColumnMapping existing",
            "        ON existing.RawColumnFnbr = rc.Unbr",
            "        AND existing.StagingTableStructureFnbr = sts.Unbr",
            "WHERE existing.Unbr IS NULL;",
            "",
            f"PRINT 'dl.ColumnMapping rows inserted: ' + CAST(@@ROWCOUNT AS VARCHAR(10));",
        ]

        if uncovered_required:
            lines += [
                "",
                "-- ⚠ WARNING: The following required staging columns have no raw column mapping.",
                "-- Placeholder _Calculated entries have been inserted, but you MUST add",
                "-- calculation logic to the load sproc before this is production-ready:",
            ]
            for col in uncovered_required:
                lines.append(f"--   {col}")

        lines += [
            "",
            "-- Verify",
            "SELECT",
            "     rc.ColumnName      AS RawColumn",
            "    ,sts.Source_Column  AS StagingColumn",
            "    ,sts.Staging_Table  AS StagingTable",
            "    ,cm.FlagForCleanup",
            "    ,cm.CleanupRequest",
            "FROM dl.RawColumn rc",
            "    LEFT JOIN dl.ColumnMapping cm ON cm.RawColumnFnbr = rc.Unbr",
            "    LEFT JOIN std.StagingTableStructure sts ON sts.Unbr = cm.StagingTableStructureFnbr",
            "WHERE rc.DataSourceFileFnbr = @DataSourceFileUnbr",
            "ORDER BY rc.InClientFile DESC, rc.ColumnOrder;",
            "",
            "ROLLBACK; -- ← Review, then replace with COMMIT",
            "",
        ]

    # ── Post-flight verification ───────────────────────────────────────────────
    lines += [
        "/*===== POST-FLIGHT VERIFICATION =====*/",
        "-- Run this after all sections are committed to confirm everything is wired correctly.",
        "",
        "SELECT",
        "     ds.SourceUnbr",
        "    ,ds.SourceName",
        "    ,ds.StdExtractTypeFnbr",
        "    ,dsf.Unbr            AS DataSourceFileUnbr",
        "    ,dsf.LoadSproc",
        "    ,dsf.FileDelimiter",
        "    ,dsf.FileColumnCount",
        "    ,dsf.FileSftpFolder",
        "    ,COUNT(rc.Unbr)      AS RawColumnCount",
        "    ,COUNT(cm.Unbr)      AS MappedColumnCount",
        "FROM ds.DataSource ds",
        "    LEFT JOIN ds.DataSourceFile dsf ON dsf.SourceFnbr = ds.SourceUnbr",
        "    LEFT JOIN dl.RawColumn rc      ON rc.DataSourceFileFnbr = dsf.Unbr",
        "    LEFT JOIN dl.ColumnMapping cm  ON cm.RawColumnFnbr = rc.Unbr",
    ]
    source_names = [
        fparams.get("source_name", "")
        for fparams in files.values()
        if fparams.get("source_name")
    ]
    if source_names:
        quoted = ", ".join(f"'{n}'" for n in source_names)
        lines.append(f"WHERE ds.SourceName IN ({quoted})")
    lines += [
        "GROUP BY ds.SourceUnbr, ds.SourceName, ds.StdExtractTypeFnbr,",
        "         dsf.Unbr, dsf.LoadSproc, dsf.FileDelimiter, dsf.FileColumnCount, dsf.FileSftpFolder;",
        "",
    ]

    return "\n".join(lines)
