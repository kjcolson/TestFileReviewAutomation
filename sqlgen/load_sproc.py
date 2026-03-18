"""
sqlgen/load_sproc.py

Generates cst.dl{NN}_{Source} stored procedure SQL files.

These are the CLIENT-SPECIFIC TRANSFORMATION LAYER sprocs only.
They receive a pre-populated #Raw temp table from the framework parent
sproc and apply UPDATE statements to populate the _Calculated columns.

The framework parent handles: BULK INSERT, #Raw creation, dimension/fact
INSERTs, and ds.LoadLog.

Signature:
    CREATE OR ALTER PROC cst.dl{NN}_{Source}
        @LoadId      VARCHAR(36),
        @ParentSproc VARCHAR(50),
        @UserId      VARCHAR(50)
"""

from __future__ import annotations

from .constants import SOURCE_TO_ENTITY_NAME


def generate(
    *,
    source: str,
    ds_number: int,
    column_transforms: list[dict] | None = None,
    uncovered_required: list[str] | None = None,
) -> str:
    """
    Generate the client-specific transformation stored procedure.

    Parameters
    ----------
    source             : Phase 1 source type key (e.g. 'payroll', 'billing_charges')
    ds_number          : Data source number (e.g. 2 → '02')
    column_transforms  : Phase 1 column_transforms list of dicts (optional)
    uncovered_required : Phase 1 uncovered_staging.required column names (optional)

    Returns
    -------
    Full SQL text for the stored procedure file.
    """
    column_transforms = column_transforms or []
    uncovered_required = uncovered_required or []

    entity = SOURCE_TO_ENTITY_NAME[source]
    nn = f"{ds_number:02d}"
    proc_name = f"cst.dl{nn}_{entity}"

    body = _build_body(proc_name, column_transforms, uncovered_required)

    return f"""\
CREATE OR ALTER PROC {proc_name}
    @LoadId      VARCHAR(36),
    @ParentSproc VARCHAR(50),
    @UserId      VARCHAR(50)
AS
SET NOCOUNT ON;
SET XACT_ABORT ON;
BEGIN
    DECLARE
        @ErrorText VARCHAR(500),
        @PrintText VARCHAR(500),
        @SprocName VARCHAR(200);

    SET @SprocName = OBJECT_SCHEMA_NAME(@@PROCID) + '.' + OBJECT_NAME(@@PROCID);

    IF OBJECT_ID('tempdb..#LoadInfo') IS NOT NULL
    BEGIN
        INSERT INTO #LoadInfo (Sproc, StartTime) VALUES (@SprocName, GETDATE());
    END

    SET @PrintText = 'Start of ' + @SprocName;
    EXEC dl.LogPrintText
        @PrintText = @PrintText,
        @LoadId = @LoadId,
        @ParentSproc = @ParentSproc,
        @UserId = @UserId;

    BEGIN TRY /*Client Specific Transformations*/

{body}
    END TRY
    BEGIN CATCH
        SET @ErrorText = N'Error while executing client specific transformations: ' + ERROR_MESSAGE();
        THROW 500007, @ErrorText, 0;
    END CATCH;
END;
"""


def _build_body(
    proc_name: str,
    column_transforms: list[dict],
    uncovered_required: list[str],
) -> str:
    """Build the body content inside BEGIN TRY."""
    lines: list[str] = []

    # Collect transform assignments
    assignments: list[str] = []
    for t in column_transforms:
        sc = t.get("staging_col") or t.get("staging_column", "")
        if not sc:
            continue
        formula = t.get("formula", t.get("expression", "NULL /* TODO: add formula */"))
        assignments.append(f"            [{sc}_Calculated] = {formula}")

    if assignments:
        lines.append("        UPDATE #Raw")
        lines.append("        SET")
        lines.append(",\n".join(assignments) + ";")

    # TODO comments for uncovered required columns
    if uncovered_required:
        if assignments:
            lines.append("")
        lines.append(
            "        -- TODO: the following required staging columns have no raw mapping.\n"
            "        -- Add calculation logic before this sproc is production-ready:"
        )
        for sc in uncovered_required:
            lines.append(f"        -- TODO: UPDATE #Raw SET [{sc}_Calculated] = ???;")

    # If nothing at all, emit the standard "no transforms" message
    if not assignments and not uncovered_required:
        lines.append(f"        SET @PrintText = @SprocName + ': No client specific transformations.';")
        lines.append(
            "        EXEC dl.LogPrintText\n"
            "            @PrintText = @PrintText,\n"
            "            @LoadId = @LoadId,\n"
            "            @ParentSproc = @ParentSproc,\n"
            "            @UserId = @UserId;"
        )

    return "\n".join(lines) + "\n"
