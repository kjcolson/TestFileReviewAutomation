"""
sqlgen/constants.py

Static mappings used throughout the SQL generation module.
"""

# Phase 1 source type → ds.DataSource.StdExtractTypeFnbr
# (1=Billing, 2=GL/Financial, 3=Payroll, 4=Scheduling)
SOURCE_TO_EXTRACT_TYPE: dict[str, int] = {
    "billing_combined": 1,
    "billing_charges": 1,
    "billing_transactions": 1,
    "gl": 2,
    "payroll": 3,
    "scheduling": 4,
}

# Default data source number when not overridden by the user
SOURCE_TO_DEFAULT_DS_NUMBER: dict[str, int] = {
    "billing_combined": 1,
    "billing_charges": 1,
    "billing_transactions": 1,
    "payroll": 2,
    "gl": 3,
    "scheduling": 4,
}

# Human-readable source name used in sproc names and ds.DataSource.SourceName
SOURCE_TO_ENTITY_NAME: dict[str, str] = {
    "billing_combined": "ChargeAndTransactionData",
    "billing_charges": "Charges",
    "billing_transactions": "Transactions",
    "payroll": "Payroll",
    "gl": "GeneralLedger",
    "scheduling": "Scheduling",
}

# Staging table name for each source type
SOURCE_TO_STAGING_TABLE: dict[str, str] = {
    "billing_combined": "#staging_billing",
    "billing_charges": "#staging_charges",
    "billing_transactions": "#staging_transactions",
    "payroll": "#staging_payroll",
    "gl": "#staging_gl",
    "scheduling": "#staging_scheduling",
}

# ds.DataSourceFileTypeFnbr for each source type (lookup in std.DataSourceFileTypes)
SOURCE_TO_FILE_TYPE_FNBR: dict[str, int] = {
    "billing_combined": 1,
    "billing_charges": 1,
    "billing_transactions": 2,
    "payroll": 3,
    "gl": 4,
    "scheduling": 5,
}

# Default folder structure template for SFTP paths
# {client_id} and {entity} are substituted at generation time
SFTP_FOLDER_TEMPLATE = "\\\\devhi\\inflowhealth\\{client_id}_{client_name}\\Client_Data\\{ds_number:02d} - {entity}\\To Load\\"

# Default loaded folder
LOADED_FOLDER_TEMPLATE = "\\\\devhi\\inflowhealth\\{client_id}_{client_name}\\Client_Data\\{ds_number:02d} - {entity}\\Loaded\\"

# SQL type → how to format it in CREATE TABLE
def sql_col_type(sql_type: str, max_length: int | None, precision: int | None, scale: int | None) -> str:
    t = (sql_type or "varchar").lower()
    if t in ("varchar", "nvarchar", "char", "nchar"):
        ml = max_length if max_length and max_length > 0 else 255
        return f"{t.upper()}({ml})"
    if t in ("decimal", "numeric"):
        p = precision if precision else 10
        s = scale if scale else 2
        return f"{t.upper()}({p},{s})"
    if t == "date":
        return "DATE"
    if t in ("int", "bigint", "smallint", "tinyint", "bit"):
        return t.upper()
    if t in ("datetime", "datetime2"):
        return t.upper()
    return t.upper()
