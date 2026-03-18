"""
sqlgen/load_sproc_templates.py

Per-source-type dimension and fact INSERT templates for load stored procedures.

Each template function receives a `col_map`: dict[staging_col → raw_col]
built from Phase 1 column_mappings.  If a staging column has no raw mapping,
the convention `{staging_col}_Calculated` is used.

Pattern matches existing sprocs exactly:
  - inflow-db-cst/.../cst.DataSource02_Payroll_Load.sql
  - inflow-db-cst/.../cst.DataSource03_GL_Load.sql
  - inflow-db-cst/.../cst.DataSource01_Charges_Load.sql
"""

from __future__ import annotations


def _raw(col_map: dict[str, str], staging_col: str, fallback: str = "zzNull") -> str:
    """Return the #Raw reference for a staging column, using ISNULL(...,'zzNull') pattern."""
    raw_col = col_map.get(staging_col) or col_map.get(f"{staging_col}_Calculated")
    if raw_col:
        return f"ISNULL([{raw_col}],'zzNull')"
    return f"'{fallback}'"


def _raw_direct(col_map: dict[str, str], staging_col: str) -> str:
    """Return a raw column reference without ISNULL wrapping (for numeric/date fields)."""
    raw_col = col_map.get(staging_col) or col_map.get(f"{staging_col}_Calculated")
    if raw_col:
        return f"a.[{raw_col}]"
    return "NULL"


# ─────────────────────────────────────────────────────────────────────────────
# PAYROLL
# ─────────────────────────────────────────────────────────────────────────────

def payroll_dimensions_and_fact(col_map: dict[str, str], ds_number: int) -> str:
    """Generate payroll dimension + fact INSERT sections."""
    emp_id = _raw(col_map, "EmployeeId")
    emp_fn = _raw(col_map, "EmployeeFirstName")
    emp_mn = _raw(col_map, "EmployeeMiddleName")
    emp_ln = _raw(col_map, "EmployeeLastName")
    emp_full = _raw(col_map, "EmployeeFullName")
    emp_npi = _raw(col_map, "EmployeeNpi")
    dept_id = _raw(col_map, "DepartmentId")
    dept_name = _raw(col_map, "DepartmentName")
    job_code = _raw(col_map, "JobCode")
    job_desc = _raw(col_map, "JobCodeDesc")
    earnings_code = _raw(col_map, "EarningsCode")
    earnings_desc = _raw(col_map, "EarningsCodeDesc")

    pay_period_start_raw = col_map.get("PayPeriodStartDate") or col_map.get("PayPeriodStartDate_Calculated")
    pay_period_end_raw = col_map.get("PayPeriodEndDate") or col_map.get("PayPeriodEndDate_Calculated")
    check_date_raw = col_map.get("CheckDate") or col_map.get("CheckDate_Calculated")

    # Build date column references (may be NULL if not mapped)
    pp_start = f"[{pay_period_start_raw}]" if pay_period_start_raw else "NULL"
    pp_end = f"[{pay_period_end_raw}]" if pay_period_end_raw else "NULL"
    chk_dt = f"[{check_date_raw}]" if check_date_raw else "NULL"

    amount_raw = col_map.get("AmountOrig") or col_map.get("AmountClean") or col_map.get("AmountOrig_Calculated")
    hours_raw = col_map.get("Hours") or col_map.get("Hours_Calculated")

    return f"""

/* pr.PayEmployee */
BEGIN
\tWITH EmployeeList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Id\t\t\t= {emp_id}
\t\t\t,FirstName\t= {emp_fn}
\t\t\t,MidName\t= {emp_mn}
\t\t\t,LastName\t= {emp_ln}
\t\t\t,FullName\t= {emp_full}
\t\t\t,Npi\t\t= {emp_npi}
\t\tFROM #Raw
\t\t)
\tINSERT INTO pr.PayEmployee WITH(TABLOCK)
\t\t(
\t\t PayEmpId
\t\t,PayEmpFirstName
\t\t,PayEmpMidName
\t\t,PayEmpLastName
\t\t,PayEmpFullNameOrig
\t\t,PayEmpNpi
\t\t)
\tSELECT
\t\t a.Id
\t\t,a.FirstName
\t\t,a.MidName
\t\t,a.LastName
\t\t,a.FullName
\t\t,a.Npi
\tFROM EmployeeList AS a
\t\tLEFT JOIN pr.PayEmployee AS b
\t\t\tON b.PayEmpId = a.Id
\t\t\tAND b.PayEmpFirstName = a.FirstName
\t\t\tAND b.PayEmpMidName = a.MidName
\t\t\tAND b.PayEmpLastName = a.LastName
\t\t\tAND b.PayEmpFullNameOrig = a.FullName
\t\t\tAND b.PayEmpNpi = a.Npi
\tWHERE b.PayEmpId IS NULL
\t\tOR b.PayEmpFirstName IS NULL
\t\tOR b.PayEmpMidName IS NULL
\t\tOR b.PayEmpLastName IS NULL
\t\tOR b.PayEmpFullNameOrig IS NULL
\t\tOR b.PayEmpNpi IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pr.PayEmployee with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pr.PayEmployee with new values.' END;


/* pr.PayDepartment */
BEGIN
\tWITH DepartmentList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Id\t\t= {dept_id}
\t\t\t,Name\t= {dept_name}
\t\tFROM #Raw
\t\t)
\tINSERT INTO pr.PayDepartment WITH(TABLOCK)
\t\t(
\t\t PayDeptId
\t\t,PayDeptName
\t\t)
\tSELECT
\t\t a.Id
\t\t,a.Name
\tFROM DepartmentList AS a
\t\tLEFT JOIN pr.PayDepartment AS b
\t\t\tON b.PayDeptId = a.Id
\t\t\tAND b.PayDeptName = a.Name
\tWHERE b.PayDeptId IS NULL
\t\tOR b.PayDeptName IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pr.PayDepartment with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pr.PayDepartment with new values.' END;


/* pr.JobCode */
BEGIN
\tWITH JobList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Code\t= {job_code}
\t\t\t,Descr\t= {job_desc}
\t\tFROM #Raw
\t\t)
\tINSERT INTO pr.JobCode WITH(TABLOCK)
\t\t(
\t\t JobCodeOrig
\t\t,JobCodeDesc
\t\t)
\tSELECT
\t\t a.Code
\t\t,a.Descr
\tFROM JobList AS a
\t\tLEFT JOIN pr.JobCode AS b
\t\t\tON b.JobCodeOrig = a.Code
\t\t\tAND b.JobCodeDesc = a.Descr
\tWHERE b.JobCodeOrig IS NULL
\t\tOR b.JobCodeDesc IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pr.JobCode with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pr.JobCode with new values.' END;


/* pr.PayCode */
BEGIN
\tWITH PayList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Code\t= {job_code} +'_'+ {earnings_code}
\t\t\t,Descr\t= {job_desc} +': '+ {earnings_desc}
\t\tFROM #Raw
\t\t)
\tINSERT INTO pr.PayCode WITH(TABLOCK)
\t\t(
\t\t PayCodeOrig
\t\t,PayCodeDesc
\t\t)
\tSELECT
\t\t a.Code
\t\t,a.Descr
\tFROM PayList AS a
\t\tLEFT JOIN pr.PayCode AS b
\t\t\tON b.PayCodeOrig = a.Code
\t\t\tAND b.PayCodeDesc = a.Descr
\tWHERE b.PayCodeOrig IS NULL
\t\tOR b.PayCodeDesc IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pr.PayCode with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pr.PayCode with new values.' END;


/* pr.PayrollDate */
BEGIN
\tWITH DateList AS
\t\t(
\t\tSELECT DISTINCT FullDate = {pp_start} FROM #Raw WHERE {pp_start} IS NOT NULL
\t\tUNION
\t\tSELECT DISTINCT FullDate = {pp_end}   FROM #Raw WHERE {pp_end}   IS NOT NULL
\t\tUNION
\t\tSELECT DISTINCT FullDate = {chk_dt}   FROM #Raw WHERE {chk_dt}   IS NOT NULL
\t\t)
\tINSERT INTO pr.PayrollDate WITH(TABLOCK)
\t\t(
\t\t PayrollDate
\t\t,[Year]
\t\t,[Month]
\t\t,YearMonth
\t\t)
\tSELECT
\t\t a.FullDate
\t\t,YEAR(a.FullDate)
\t\t,MONTH(a.FullDate)
\t\t,LEFT(CONVERT(VARCHAR(8),a.FullDate,112),6)
\tFROM DateList AS a
\t\tLEFT JOIN pr.PayrollDate AS b ON b.PayrollDate = a.FullDate
\tWHERE b.PayrollDate IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pr.PayrollDate with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pr.PayrollDate with new values.' END;


/* Populate Load Log */
BEGIN
\tINSERT INTO ds.LoadLog (SourceFnbr,[FileName],EventDesc)
\tVALUES (@DefaultParamaterDataSource,@FileName,'Load')
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate ds.LoadLog.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate ds.LoadLog.'; END;


/* pr.PayFact */
BEGIN
\tWITH NextFileFnbr AS
\t\t(
\t\tSELECT FileUNbr AS FileFnbr
\t\tFROM ds.LoadLog
\t\tWHERE SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND [FileName] = @FileName
\t\t)
\tINSERT INTO pr.PayFact WITH(TABLOCK)
\t\t(
\t\t-- Dimension Foreign Keys
\t\t [SourceFnbr]
\t\t,[FileFnbr]
\t\t,PayEmpFnbr
\t\t,PayDeptFnbr
\t\t,JobCodeFnbr
\t\t,PayCodeFnbr
\t\t,PayPeriodStartDateFnbr
\t\t,PayPeriodEndDateFnbr
\t\t,PayChkDateFnbr
\t\t-- Measures
\t\t,AmountOrig
\t\t,AmountClean
\t\t,PayHoursOrig
\t\t,PayPeriodStartDate
\t\t,PayPeriodEndDate
\t\t,PayChkDate
\t\t)
\tSELECT
\t\t [SourceFnbr]\t\t\t= @DefaultParamaterDataSource
\t\t,[FileFnbr]\t\t\t\t= nf.[FileFnbr]
\t\t,PayEmpFnbr\t\t\t\t= b.PayEmpUnbr
\t\t,PayDeptFnbr\t\t\t= c.PayDeptUnbr
\t\t,JobCodeFnbr\t\t\t= d.JobCodeUnbr
\t\t,PayCodeFnbr\t\t\t= e.PayCodeUnbr
\t\t,PayPeriodStartDateFnbr\t= f.PayrollDateUnbr
\t\t,PayPeriodEndDateFnbr\t= g.PayrollDateUnbr
\t\t,PayChkDateFnbr\t\t\t= h.PayrollDateUnbr
\t\t-- Measures
\t\t,AmountOrig\t\t\t\t= a.[{amount_raw or "Amount"}]
\t\t,AmountClean\t\t\t= a.[{amount_raw or "Amount"}]
\t\t,PayHoursOrig\t\t\t= a.[{hours_raw or "Hours"}]
\t\t,PayPeriodStartDate\t\t= a.{pp_start}
\t\t,PayPeriodEndDate\t\t= a.{pp_end}
\t\t,PayChkDate\t\t\t\t= a.{chk_dt}
\tFROM NextFileFnbr AS nf
\t\t,#Raw AS a
\t\t\tLEFT JOIN pr.PayEmployee AS b
\t\t\t\tON b.PayEmpId = {emp_id}
\t\t\t\tAND PayEmpFirstName = {emp_fn}
\t\t\t\tAND PayEmpMidName = {emp_mn}
\t\t\t\tAND PayEmpLastName = {emp_ln}
\t\t\t\tAND PayEmpFullNameOrig = {emp_full}
\t\t\t\tAND PayEmpNpi = {emp_npi}
\t\t\tLEFT JOIN pr.PayDepartment AS c
\t\t\t\tON c.PayDeptId = {dept_id}
\t\t\t\tAND c.PayDeptName = {dept_name}
\t\t\tLEFT JOIN pr.JobCode AS d
\t\t\t\tON d.JobCodeOrig = {job_code}
\t\t\t\tAND d.JobCodeDesc = {job_desc}
\t\t\tLEFT JOIN pr.PayCode AS e
\t\t\t\tON e.PayCodeOrig = {job_code} +'_'+ {earnings_code}
\t\t\t\tAND e.PayCodeDesc = {job_desc} +': '+ {earnings_desc}
\t\t\tLEFT JOIN pr.PayrollDate AS f -- Pay Period Start Date
\t\t\t\tON f.PayrollDate = a.{pp_start}
\t\t\tLEFT JOIN pr.PayrollDate AS g -- Pay Period End Date
\t\t\t\tON g.PayrollDate = a.{pp_end}
\t\t\tLEFT JOIN pr.PayrollDate AS h -- Check/Pay Date
\t\t\t\tON h.PayrollDate = a.{chk_dt}
END;

BEGIN
\tDECLARE @RowCountVar INT
\tSET @RowCountVar = @@ROWCOUNT;
\tUPDATE ds.LoadLog
\tSET RowsAffected = @RowCountVar
\tWHERE SourceFNbr = @DefaultParamaterDataSource
\t\tAND [FileName] = @FileName
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Insert payroll records and update ds.LoadLog table with row count.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Insert payroll records and update ds.LoadLog table with row count.' END;
"""


# ─────────────────────────────────────────────────────────────────────────────
# GENERAL LEDGER
# ─────────────────────────────────────────────────────────────────────────────

def gl_dimensions_and_fact(col_map: dict[str, str], ds_number: int) -> str:
    """Generate GL dimension + fact INSERT sections."""
    cc_num = _raw(col_map, "CostCenterNumberOrig")
    cc_name = _raw(col_map, "CostCenterNameOrig")
    acct_num = _raw(col_map, "AcctNumber")
    acct_desc = _raw(col_map, "AcctDesc")
    sub_num = _raw(col_map, "SubAcctNumber")
    sub_desc = _raw(col_map, "SubAcctDesc")
    year_month_raw = col_map.get("YearMonth") or col_map.get("YearMonth_Calculated")
    ym_ref = f"[{year_month_raw}]" if year_month_raw else "NULL"
    amount_raw = col_map.get("AmountOrig") or col_map.get("AmountClean") or col_map.get("AmountOrig_Calculated")
    amount_ref = f"[{amount_raw}]" if amount_raw else "NULL"

    return f"""

/* gl.CostCenter */
BEGIN
\tWITH CostCenterList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Number\t= {cc_num}
\t\t\t,Name\t= {cc_name}
\t\tFROM #Raw
\t\t)
\tINSERT INTO gl.CostCenter WITH(TABLOCK)
\t\t(
\t\t SourceFnbr
\t\t,CostCenterNumberOrig
\t\t,CostCenterNameOrig
\t\t)
\tSELECT
\t\t @DefaultParamaterDataSource
\t\t,a.Number
\t\t,a.Name
\tFROM CostCenterList AS a
\t\tLEFT JOIN gl.CostCenter AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND b.CostCenterNumberOrig = a.Number
\t\t\tAND b.CostCenterNameOrig = a.Name
\tWHERE b.CostCenterNumberOrig IS NULL
\t\tOR b.CostCenterNameOrig IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate gl.CostCenter with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate gl.CostCenter with new values.' END;


/* gl.Account */
BEGIN
\tWITH AccountList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Number\t= {acct_num}
\t\t\t,Descr\t= {acct_desc}
\t\tFROM #Raw
\t\t)
\tINSERT INTO gl.Account WITH(TABLOCK)
\t\t(
\t\t SourceFnbr
\t\t,AcctNumber
\t\t,AcctDesc
\t\t)
\tSELECT
\t\t @DefaultParamaterDataSource
\t\t,a.Number
\t\t,a.Descr
\tFROM AccountList AS a
\t\tLEFT JOIN gl.Account AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND b.AcctNumber = a.Number
\t\t\tAND b.AcctDesc = a.Descr
\tWHERE b.AcctNumber IS NULL
\t\tOR b.AcctDesc IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate gl.Account with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate gl.Account with new values.' END;


/* gl.SubAccount */
BEGIN
\tWITH SubAccountList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Number\t= {sub_num}
\t\t\t,Descr\t= {sub_desc}
\t\tFROM #Raw
\t\t)
\tINSERT INTO gl.SubAccount WITH(TABLOCK)
\t\t(
\t\t SourceFnbr
\t\t,SubAcctNumber
\t\t,SubAcctDesc
\t\t)
\tSELECT
\t\t @DefaultParamaterDataSource
\t\t,a.Number
\t\t,a.Descr
\tFROM SubAccountList AS a
\t\tLEFT JOIN gl.SubAccount AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND b.SubAcctNumber = a.Number
\t\t\tAND b.SubAcctDesc = a.Descr
\tWHERE b.SubAcctNumber IS NULL
\t\tOR b.SubAcctDesc IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate gl.SubAccount with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate gl.SubAccount with new values.' END;


/* gl.ReportPeriod */
BEGIN
\tWITH PeriodList AS
\t\t(
\t\tSELECT DISTINCT YearMonth = {ym_ref}
\t\tFROM #Raw
\t\tWHERE {ym_ref} IS NOT NULL
\t\t)
\tINSERT INTO gl.ReportPeriod WITH(TABLOCK)
\t\t(
\t\t [Year]
\t\t,[Month]
\t\t,YearMonth
\t\t)
\tSELECT
\t\t LEFT(a.YearMonth,4)
\t\t,RIGHT(a.YearMonth,2)
\t\t,a.YearMonth
\tFROM PeriodList AS a
\t\tLEFT JOIN gl.ReportPeriod AS b ON b.YearMonth = a.YearMonth
\tWHERE b.YearMonth IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate gl.ReportPeriod with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate gl.ReportPeriod with new values.' END;


/* Populate Load Log */
BEGIN
\tINSERT INTO ds.LoadLog (SourceFnbr,[FileName],EventDesc)
\tVALUES (@DefaultParamaterDataSource,@FileName,'Load')
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate ds.LoadLog.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate ds.LoadLog.'; END;


/* gl.GlFact */
BEGIN
\tWITH NextFileFnbr AS
\t\t(
\t\tSELECT FileUNbr AS FileFnbr
\t\tFROM ds.LoadLog
\t\tWHERE SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND [FileName] = @FileName
\t\t)
\tINSERT INTO gl.GlFact WITH(TABLOCK)
\t\t(
\t\t-- Dimension Foreign Keys
\t\t [SourceFnbr]
\t\t,[FileFnbr]
\t\t,CostCenterFnbr
\t\t,AcctFnbr
\t\t,SubAcctFnbr
\t\t,ReportPeriodFnbr
\t\t-- Measures
\t\t,AmountOrig
\t\t,AmountClean
\t\t,ReportPeriod
\t\t)
\tSELECT
\t\t [SourceFnbr]\t\t= @DefaultParamaterDataSource
\t\t,[FileFnbr]\t\t\t= nf.[FileFnbr]
\t\t,CostCenterFnbr\t\t= b.CostCenterUnbr
\t\t,AcctFnbr\t\t\t= c.AcctUnbr
\t\t,SubAcctFnbr\t\t= d.SubAcctUnbr
\t\t,ReportPeriodFnbr\t= e.ReportPeriodUnbr
\t\t-- Measures
\t\t,AmountOrig\t\t\t= a.{amount_ref}
\t\t,AmountClean\t\t= a.{amount_ref}
\t\t,ReportPeriod\t\t= a.{ym_ref}
\tFROM NextFileFnbr AS nf
\t\t,#Raw AS a
\t\tLEFT JOIN gl.CostCenter AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND b.CostCenterNumberOrig = {cc_num}
\t\t\tAND b.CostCenterNameOrig = {cc_name}
\t\tLEFT JOIN gl.Account AS c
\t\t\tON c.SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND c.AcctNumber = {acct_num}
\t\t\tAND c.AcctDesc = {acct_desc}
\t\tLEFT JOIN gl.SubAccount AS d
\t\t\tON d.SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND d.SubAcctNumber = {sub_num}
\t\t\tAND d.SubAcctDesc = {sub_desc}
\t\tLEFT JOIN gl.ReportPeriod AS e
\t\t\tON e.YearMonth = a.{ym_ref}
END;

BEGIN
\tDECLARE @RowCountVar INT
\tSET @RowCountVar = @@ROWCOUNT;
\tUPDATE ds.LoadLog
\tSET RowsAffected = @RowCountVar
\tWHERE SourceFNbr = @DefaultParamaterDataSource
\t\tAND [FileName] = @FileName
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Insert GL records and update ds.LoadLog table with row count.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Insert GL records and update ds.LoadLog table with row count.' END;
"""


# ─────────────────────────────────────────────────────────────────────────────
# SCHEDULING
# ─────────────────────────────────────────────────────────────────────────────

def scheduling_dimensions_and_fact(col_map: dict[str, str], ds_number: int) -> str:
    """Generate scheduling dimension + fact INSERT sections."""
    appt_date_raw = col_map.get("ApptDate") or col_map.get("ApptDate_Calculated")
    appt_date = f"[{appt_date_raw}]" if appt_date_raw else "NULL"
    loc_id = _raw(col_map, "LocationId")
    loc_name = _raw(col_map, "LocationName")
    dept_id = _raw(col_map, "DepartmentId")
    dept_name = _raw(col_map, "DepartmentName")
    prac_id = _raw(col_map, "PracticeId")
    prac_name = _raw(col_map, "PracticeName")
    prov_npi = _raw(col_map, "RenderingProviderNpi")
    prov_fn = _raw(col_map, "RenderingProviderFirstName")
    prov_ln = _raw(col_map, "RenderingProviderLastName")
    prov_full = _raw(col_map, "RenderingProviderFullName")
    appt_id = _raw(col_map, "ApptId")
    appt_status = _raw(col_map, "ApptStatus")
    pat_id = _raw(col_map, "PatientId")

    return f"""

/* pa.SchdLocation */
BEGIN
\tWITH LocList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Id\t\t= {loc_id}
\t\t\t,Name\t= {loc_name}
\t\tFROM #Raw
\t\t)
\tINSERT INTO pa.SchdLocation WITH(TABLOCK)
\t\t(
\t\t SourceFnbr
\t\t,SchdLocId
\t\t,SchdLocName
\t\t)
\tSELECT
\t\t @DefaultParamaterDataSource
\t\t,a.Id
\t\t,a.Name
\tFROM LocList AS a
\t\tLEFT JOIN pa.SchdLocation AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND b.SchdLocId = a.Id
\t\t\tAND b.SchdLocName = a.Name
\tWHERE b.SchdLocId IS NULL
\t\tOR b.SchdLocName IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pa.SchdLocation with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pa.SchdLocation with new values.' END;


/* pa.SchdDepartment */
BEGIN
\tWITH DeptList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Id\t\t= {dept_id}
\t\t\t,Name\t= {dept_name}
\t\tFROM #Raw
\t\t)
\tINSERT INTO pa.SchdDepartment WITH(TABLOCK)
\t\t(
\t\t SourceFnbr
\t\t,SchdDeptId
\t\t,SchdDeptName
\t\t)
\tSELECT
\t\t @DefaultParamaterDataSource
\t\t,a.Id
\t\t,a.Name
\tFROM DeptList AS a
\t\tLEFT JOIN pa.SchdDepartment AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND b.SchdDeptId = a.Id
\t\t\tAND b.SchdDeptName = a.Name
\tWHERE b.SchdDeptId IS NULL
\t\tOR b.SchdDeptName IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pa.SchdDepartment with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pa.SchdDepartment with new values.' END;


/* pa.SchdProvider */
BEGIN
\tWITH ProvList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t Npi\t\t= {prov_npi}
\t\t\t,FirstName\t= {prov_fn}
\t\t\t,LastName\t= {prov_ln}
\t\t\t,FullName\t= {prov_full}
\t\tFROM #Raw
\t\t)
\tINSERT INTO pa.SchdProvider WITH(TABLOCK)
\t\t(
\t\t SchdProvNpi
\t\t,SchdProvFirstName
\t\t,SchdProvLastName
\t\t,SchdProvFullName
\t\t)
\tSELECT
\t\t a.Npi
\t\t,a.FirstName
\t\t,a.LastName
\t\t,a.FullName
\tFROM ProvList AS a
\t\tLEFT JOIN pa.SchdProvider AS b
\t\t\tON b.SchdProvNpi = a.Npi
\t\t\tAND b.SchdProvFirstName = a.FirstName
\t\t\tAND b.SchdProvLastName = a.LastName
\t\t\tAND b.SchdProvFullName = a.FullName
\tWHERE b.SchdProvNpi IS NULL
\t\tOR b.SchdProvFirstName IS NULL
\t\tOR b.SchdProvLastName IS NULL
\t\tOR b.SchdProvFullName IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pa.SchdProvider with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pa.SchdProvider with new values.' END;


/* pa.SchdDate */
BEGIN
\tWITH DateList AS
\t\t(
\t\tSELECT DISTINCT FullDate = {appt_date}
\t\tFROM #Raw
\t\tWHERE {appt_date} IS NOT NULL
\t\t)
\tINSERT INTO pa.SchdDate WITH(TABLOCK)
\t\t(
\t\t [Date]
\t\t,[Year]
\t\t,[Month]
\t\t,YearMonth
\t\t)
\tSELECT
\t\t a.FullDate
\t\t,YEAR(a.FullDate)
\t\t,MONTH(a.FullDate)
\t\t,LEFT(CONVERT(VARCHAR(8),a.FullDate,112),6)
\tFROM DateList AS a
\t\tLEFT JOIN pa.SchdDate AS b ON b.[Date] = a.FullDate
\tWHERE b.[Date] IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate pa.SchdDate with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate pa.SchdDate with new values.' END;


/* Populate Load Log */
BEGIN
\tINSERT INTO ds.LoadLog (SourceFnbr,[FileName],EventDesc)
\tVALUES (@DefaultParamaterDataSource,@FileName,'Load')
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate ds.LoadLog.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate ds.LoadLog.'; END;


/* pa.SchdApptFact */
BEGIN
\tWITH NextFileFnbr AS
\t\t(
\t\tSELECT FileUNbr AS FileFnbr
\t\tFROM ds.LoadLog
\t\tWHERE SourceFnbr = @DefaultParamaterDataSource
\t\t\tAND [FileName] = @FileName
\t\t)
\tINSERT INTO pa.SchdApptFact WITH(TABLOCK)
\t\t(
\t\t-- Dimension Foreign Keys
\t\t [SourceFnbr]
\t\t,[FileFnbr]
\t\t,SchdLocFnbr
\t\t,SchdDeptFnbr
\t\t,SchdProvFnbr
\t\t,ApptDateFnbr
\t\t-- Key fields
\t\t,ApptId
\t\t,ApptStatus
\t\t,PatientId
\t\t,ApptDate
\t\t)
\tSELECT
\t\t [SourceFnbr]\t= @DefaultParamaterDataSource
\t\t,[FileFnbr]\t\t= nf.[FileFnbr]
\t\t,SchdLocFnbr\t= b.SchdLocUnbr
\t\t,SchdDeptFnbr\t= c.SchdDeptUnbr
\t\t,SchdProvFnbr\t= d.SchdProvUnbr
\t\t,ApptDateFnbr\t= e.DateUnbr
\t\t-- Key fields
\t\t,ApptId\t\t\t= {appt_id}
\t\t,ApptStatus\t\t= {appt_status}
\t\t,PatientId\t\t= {pat_id}
\t\t,ApptDate\t\t= a.{appt_date}
\tFROM NextFileFnbr AS nf
\t\t,#Raw AS a
\t\t\tLEFT JOIN pa.SchdLocation AS b
\t\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource
\t\t\t\tAND b.SchdLocId = {loc_id}
\t\t\t\tAND b.SchdLocName = {loc_name}
\t\t\tLEFT JOIN pa.SchdDepartment AS c
\t\t\t\tON c.SourceFnbr = @DefaultParamaterDataSource
\t\t\t\tAND c.SchdDeptId = {dept_id}
\t\t\t\tAND c.SchdDeptName = {dept_name}
\t\t\tLEFT JOIN pa.SchdProvider AS d
\t\t\t\tON d.SchdProvNpi = {prov_npi}
\t\t\t\tAND d.SchdProvFirstName = {prov_fn}
\t\t\t\tAND d.SchdProvLastName = {prov_ln}
\t\t\t\tAND d.SchdProvFullName = {prov_full}
\t\t\tLEFT JOIN pa.SchdDate AS e
\t\t\t\tON e.[Date] = a.{appt_date}
END;

BEGIN
\tDECLARE @RowCountVar INT
\tSET @RowCountVar = @@ROWCOUNT;
\tUPDATE ds.LoadLog
\tSET RowsAffected = @RowCountVar
\tWHERE SourceFNbr = @DefaultParamaterDataSource
\t\tAND [FileName] = @FileName
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Insert scheduling records and update ds.LoadLog table with row count.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Insert scheduling records and update ds.LoadLog table with row count.' END;
"""


# ─────────────────────────────────────────────────────────────────────────────
# BILLING — CHARGES
# ─────────────────────────────────────────────────────────────────────────────

def billing_charges_dimensions_and_fact(col_map: dict[str, str], ds_number: int) -> str:
    """Generate billing charges dimension + fact INSERT sections."""

    def r(stg: str) -> str:
        return _raw(col_map, stg)

    chrg_id = r("ChargeId")
    invoice = r("InvoiceNumber")
    dos_raw = col_map.get("DateOfService") or col_map.get("DateOfService_Calculated")
    post_date_raw = col_map.get("PostDate") or col_map.get("PostDate_Calculated")
    dos = f"[{dos_raw}]" if dos_raw else "NULL"
    post_dt = f"[{post_date_raw}]" if post_date_raw else "NULL"
    pat_gender = r("PatientGender")
    pat_race = r("PatientRaceEthnicity")
    pat_marital = r("PatientMaritalStatus")
    pat_age = r("PatientAge")
    pat_id = r("PatientId")
    pat_city = r("PatientCity")
    pat_state = r("PatientState")
    pat_zip = r("PatientZip")
    pat_mrn = r("PatientMrn")
    pos_code = r("PlaceOfServiceCode")
    cpt_code = r("CptCode")
    cpt_desc = r("CptCodeDesc")
    mod1 = r("Modifier1")
    mod2 = r("Modifier2")
    mod3 = r("Modifier3")
    mod4 = r("Modifier4")
    dept_id = r("BillDepartmentId")
    dept_name = r("BillDepartmentName")
    loc_id = r("BillLocationId")
    loc_name = r("BillLocationName")
    prac_id = r("BillPracticeId")
    prac_name = r("BillPracticeName")
    payer_name = r("ChargePayerName")
    payer_plan = r("ChargePayerPlan")
    payer_class = r("ChargePayerFinancialClass")
    rend_fn = r("RenderingProviderFirstName")
    rend_mn = r("RenderingProviderMiddleName")
    rend_ln = r("RenderingProviderLastName")
    rend_full = r("RenderingProviderFullName")
    rend_npi = r("RenderingProviderNpi")
    rend_spec = r("RenderingProviderSpecialty")
    rend_cred = r("RenderingProviderCredentials")
    bill_fn = r("BillingProviderFirstName")
    bill_mn = r("BillingProviderMiddleName")
    bill_ln = r("BillingProviderLastName")
    bill_full = r("BillingProviderFullName")
    bill_npi = r("BillingProviderNpi")
    bill_spec = r("BillingProviderSpecialty")
    bill_cred = r("BillingProviderCredentials")
    bill_tin = r("BillingProviderTaxId")
    ref_fn = r("ReferringProviderFirstName")
    ref_mn = r("ReferringProviderMiddleName")
    ref_ln = r("ReferringProviderLastName")
    ref_full = r("ReferringProviderFullName")
    ref_npi = r("ReferringProviderNpi")
    ref_spec = r("ReferringProviderSpecialty")
    ref_cred = r("ReferringProviderCredentials")
    icd1 = r("PrimaryIcdCode")
    icd1_desc = r("PrimaryIcdCodeDesc")
    units_raw = col_map.get("UnitsOrig") or col_map.get("Units") or col_map.get("UnitsOrig_Calculated")
    charge_raw = col_map.get("ChargeAmountOriginal") or col_map.get("ChargeAmountOrig") or col_map.get("ChargeAmountOriginal_Calculated")
    rvu_raw = col_map.get("WorkRvuOriginal") or col_map.get("WorkRvuOrig") or col_map.get("WorkRvuOriginal_Calculated")
    rvu_clean_raw = col_map.get("WorkRvuClean") or col_map.get("WorkRvuClean_Calculated")

    units_ref = f"a.[{units_raw}]" if units_raw else "NULL"
    charge_ref = f"a.[{charge_raw}]" if charge_raw else "NULL"
    rvu_ref = f"a.[{rvu_raw}]" if rvu_raw else "NULL"
    rvu_clean_ref = f"a.[{rvu_clean_raw}]" if rvu_clean_raw else f"a.[{rvu_raw}]" if rvu_raw else "NULL"

    return f"""

/* bl.ChargeId */
BEGIN
\tWITH ChargeIdList AS
\t\t(SELECT DISTINCT Id = {chrg_id} FROM #Raw)
\tINSERT INTO bl.ChargeId WITH(TABLOCK) (SourceFnbr, ChrgId)
\tSELECT @DefaultParamaterDataSource, a.Id
\tFROM ChargeIdList AS a
\t\tLEFT JOIN bl.ChargeId AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource AND a.Id = b.ChrgId
\tWHERE b.ChrgId IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.ChargeId with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.ChargeId with new values.' END;


/* bl.Invoice */
BEGIN
\tWITH InvoiceList AS
\t\t(SELECT DISTINCT Invoice = {invoice} FROM #Raw)
\tINSERT INTO bl.Invoice WITH(TABLOCK) (SourceFnbr, InvoiceNumber)
\tSELECT @DefaultParamaterDataSource, a.Invoice
\tFROM InvoiceList AS a
\t\tLEFT JOIN bl.Invoice AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource AND a.Invoice = b.InvoiceNumber
\tWHERE b.InvoiceNumber IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.Invoice with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.Invoice with new values.' END;


/* bl.Date */
BEGIN
\tWITH DateList AS
\t\t(
\t\tSELECT DISTINCT FullDate = {dos} FROM #Raw WHERE {dos} IS NOT NULL
\t\tUNION
\t\tSELECT DISTINCT FullDate = {post_dt} FROM #Raw WHERE {post_dt} IS NOT NULL
\t\t)
\tINSERT INTO bl.[Date] WITH(TABLOCK) ([Date],[Year],[Month],YearMonth)
\tSELECT
\t\t FullDate
\t\t,YEAR(FullDate)
\t\t,MONTH(FullDate)
\t\t,LEFT(CONVERT(VARCHAR(8),CONVERT(DATE,[FullDate]),112),6)
\tFROM DateList AS a
\t\tLEFT JOIN bl.[Date] AS b ON a.FullDate = b.[Date]
\tWHERE b.[Date] IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.Date with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.Date with new values.' END;


/* bl.TransactionType */
BEGIN
\tWITH TranTypeList AS
\t\t(SELECT DISTINCT TOP 1 [Type] = 'zzNull', TranDesc = 'zzNull' FROM #Raw)
\tINSERT INTO bl.[TransactionType] WITH(TABLOCK) (TranType, TranTypeDesc)
\tSELECT [Type], TranDesc
\tFROM TranTypeList AS a
\t\tLEFT JOIN bl.TransactionType AS b ON a.[Type] = b.TranType AND a.TranDesc = b.TranTypeDesc
\tWHERE b.TranType IS NULL OR b.TranTypeDesc IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.TransactionType with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.TransactionType with new values.' END;


/* bl.PatientAge */
BEGIN
\tWITH AgeList AS (SELECT DISTINCT Age = {pat_age} FROM #Raw)
\tINSERT INTO bl.PatientAge WITH(TABLOCK) (PatAge)
\tSELECT Age FROM AgeList AS a
\t\tLEFT JOIN bl.PatientAge AS b ON a.Age = b.PatAge
\tWHERE b.PatAge IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.PatientAge with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.PatientAge with new values.' END;


/* bl.PatientGender */
BEGIN
\tWITH GenderList AS (SELECT DISTINCT PatGender = {pat_gender} FROM #Raw)
\tINSERT INTO bl.PatientGender WITH(TABLOCK) (GenderOrig)
\tSELECT PatGender FROM GenderList AS a
\t\tLEFT JOIN bl.PatientGender AS b ON a.PatGender = b.GenderOrig
\tWHERE b.GenderOrig IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.PatientGender with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.PatientGender with new values.' END;


/* bl.BillDepartment */
BEGIN
\tWITH DeptList AS
\t\t(SELECT DISTINCT Id = {dept_id}, Name = {dept_name} FROM #Raw)
\tINSERT INTO bl.BillDepartment WITH(TABLOCK) (SourceFnbr, BillDeptId, BillDeptNameOrig)
\tSELECT @DefaultParamaterDataSource, Id, Name
\tFROM DeptList AS a
\t\tLEFT JOIN bl.BillDepartment AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource AND a.Id = b.BillDeptId AND a.Name = b.BillDeptNameOrig
\tWHERE b.BillDeptId IS NULL OR b.BillDeptNameOrig IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.BillDepartment with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.BillDepartment with new values.' END;


/* bl.BillLocation */
BEGIN
\tWITH LocList AS
\t\t(SELECT DISTINCT Id = 'zzNull', Name = {loc_name}, [Address] = 'zzNull', City = 'zzNull', [State] = 'zzNull', Zip = 'zzNull' FROM #Raw)
\tINSERT INTO bl.BillLocation WITH(TABLOCK) (SourceFnbr, BillLocId, BillLocNameOrig, BillLocAddress, BillLocCity, BillLocState, BillLocZipOrig)
\tSELECT @DefaultParamaterDataSource, Id, Name, [Address], City, [State], Zip
\tFROM LocList AS a
\t\tLEFT JOIN bl.BillLocation AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource AND a.Id = b.BillLocId AND a.Name = b.BillLocNameOrig
\t\t\tAND a.[Address] = b.BillLocAddress AND a.City = b.BillLocCity AND a.[State] = b.BillLocState AND a.Zip = b.BillLocZipOrig
\tWHERE b.BillLocId IS NULL OR b.BillLocNameOrig IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.BillLocation with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.BillLocation with new values.' END;


/* bl.BillPractice */
BEGIN
\tWITH PracList AS
\t\t(SELECT DISTINCT Id = 'zzNull', Name = {prac_name} FROM #Raw)
\tINSERT INTO bl.BillPractice WITH(TABLOCK) (SourceFnbr, BillPracId, BillPracNameOrig)
\tSELECT @DefaultParamaterDataSource, Id, Name
\tFROM PracList AS a
\t\tLEFT JOIN bl.BillPractice AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource AND a.Id = b.BillPracId AND a.Name = b.BillPracNameOrig
\tWHERE b.BillPracId IS NULL OR b.BillPracNameOrig IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.BillPractice with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.BillPractice with new values.' END;


/* bl.Payer */
BEGIN
\tWITH PayerList AS
\t\t(
\t\tSELECT DISTINCT Name = {payer_name}, [Plan] = {payer_plan}, Class = {payer_class} FROM #Raw
\t\t)
\tINSERT INTO bl.Payer WITH(TABLOCK) (PayerName, PayerPlan, PayerFinClass)
\tSELECT Name, [Plan], Class
\tFROM PayerList AS a
\t\tLEFT JOIN bl.Payer AS b ON a.Name = b.PayerName AND a.[Plan] = b.PayerPlan AND a.Class = b.PayerFinClass
\tWHERE b.PayerName IS NULL OR b.PayerPlan IS NULL OR b.PayerFinClass IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.Payer with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.Payer with new values.' END;


/* bl.BillPatient */
BEGIN
\tWITH PatientList AS
\t\t(SELECT DISTINCT Id = {pat_id}, City = {pat_city}, State = LEFT({pat_state},30), Zip = {pat_zip}, Mrn = {pat_mrn} FROM #Raw)
\tINSERT INTO bl.BillPatient WITH(TABLOCK) (SourceFnbr, PatIdOrig, PatIdClean, PatCity, PatState, PatZipOrig, PatZipClean, PatMrn)
\tSELECT @DefaultParamaterDataSource, Id, Id, City, State, Zip, LEFT(Zip,5), Mrn
\tFROM PatientList AS a
\t\tLEFT JOIN bl.BillPatient AS b
\t\t\tON b.SourceFnbr = @DefaultParamaterDataSource AND a.Id = b.PatIdOrig AND a.City = b.PatCity
\t\t\tAND a.State = b.PatState AND a.Zip = b.PatZipOrig AND a.Mrn = b.PatMrn
\tWHERE b.PatIdOrig IS NULL OR b.PatCity IS NULL OR b.PatState IS NULL OR b.PatZipOrig IS NULL OR b.PatMrn IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.BillPatient with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.BillPatient with new values.' END;


/* bl.PatientRace */
BEGIN
\tWITH RaceList AS (SELECT DISTINCT Race = {pat_race} FROM #Raw)
\tINSERT INTO bl.PatientRace WITH(TABLOCK) (PatRace)
\tSELECT Race FROM RaceList AS a LEFT JOIN bl.PatientRace AS b ON a.Race = b.PatRace WHERE b.PatRace IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.PatientRace with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.PatientRace with new values.' END;


/* bl.PatientMaritalStatus */
BEGIN
\tWITH MaritalList AS (SELECT DISTINCT MaritalStatus = {pat_marital} FROM #Raw)
\tINSERT INTO bl.PatientMaritalStatus WITH(TABLOCK) (PatMaritalStatus)
\tSELECT MaritalStatus FROM MaritalList AS a
\t\tLEFT JOIN bl.PatientMaritalStatus AS b ON a.MaritalStatus = b.PatMaritalStatus
\tWHERE b.PatMaritalStatus IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.PatientMaritalStatus with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.PatientMaritalStatus with new values.' END;


/* bl.POS */
BEGIN
\tWITH POSList AS (SELECT DISTINCT Code = {pos_code}, Name = 'zzNull', Descr = 'zzNull' FROM #Raw)
\tINSERT INTO bl.POS WITH(TABLOCK) (PosCode, PosName, PosDesc)
\tSELECT Code, Name, Descr FROM POSList AS a
\t\tLEFT JOIN bl.POS AS b ON a.Code = b.PosCode AND a.Name = b.PosName AND a.Descr = b.PosDesc
\tWHERE b.PosCode IS NULL OR b.PosName IS NULL OR b.PosDesc IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.POS with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.POS with new values.' END;


/* bl.CptCodes */
BEGIN
\tWITH CptList AS
\t\t(
\t\tSELECT DISTINCT
\t\t\t CptOrig\t= {cpt_code}
\t\t\t,DescOrig\t= LEFT({cpt_desc},250)
\t\t\t,[YEAR]\t\t= ISNULL(YEAR({dos}),YEAR(GETDATE()))
\t\t\t,Mod1\t\t= {mod1}
\t\t\t,Mod2\t\t= {mod2}
\t\t\t,Mod3\t\t= {mod3}
\t\t\t,Mod4\t\t= {mod4}
\t\tFROM #Raw
\t\t)
\tINSERT INTO bl.CptCodes WITH(TABLOCK)
\t\t(CptCodeOrig, CptCodeDescOrig, CptServiceYear, Modifier1, Modifier2, Modifier3, Modifier4)
\tSELECT CptOrig, DescOrig, [YEAR], Mod1, Mod2, Mod3, Mod4
\tFROM CptList AS a
\t\tLEFT JOIN bl.CptCodes AS b
\t\t\tON a.CptOrig = b.CptCodeOrig AND a.DescOrig = b.CptCodeDescOrig AND a.[YEAR] = b.CptServiceYear
\t\t\tAND a.Mod1 = b.Modifier1 AND a.Mod2 = b.Modifier2 AND a.Mod3 = b.Modifier3 AND a.Mod4 = b.Modifier4
\tWHERE b.CptCodeOrig IS NULL OR b.CptCodeDescOrig IS NULL OR b.CptServiceYear IS NULL
\t\tOR b.Modifier1 IS NULL OR b.Modifier2 IS NULL OR b.Modifier3 IS NULL OR b.Modifier4 IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.CptCodes with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.CptCodes with new values.' END;


/* bl.ProviderSpecialty */
BEGIN
\tWITH SpecialtyList AS
\t\t(
\t\tSELECT DISTINCT Specialty = {rend_spec} FROM #Raw
\t\tUNION
\t\tSELECT DISTINCT Specialty = {bill_spec} FROM #Raw
\t\tUNION
\t\tSELECT DISTINCT Specialty = {ref_spec}  FROM #Raw
\t\t)
\tINSERT INTO bl.ProviderSpecialty WITH(TABLOCK) (ProvSpecialty)
\tSELECT Specialty FROM SpecialtyList AS a
\t\tLEFT JOIN bl.ProviderSpecialty AS b ON a.Specialty = b.ProvSpecialty
\tWHERE b.ProvSpecialty IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.ProviderSpecialty with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.ProviderSpecialty with new values.' END;


/* bl.Provider */
BEGIN
\tWITH ProviderList AS
\t\t(
\t\tSELECT DISTINCT FirstName = {rend_fn}, MidName = {rend_mn}, LastName = {rend_ln}, FullName = {rend_full}, Id = 'zzNull', NPI = {rend_npi}, Cred = {rend_cred} FROM #Raw
\t\tUNION
\t\tSELECT DISTINCT FirstName = {bill_fn}, MidName = {bill_mn}, LastName = {bill_ln}, FullName = {bill_full}, Id = 'zzNull', NPI = {bill_npi}, Cred = {bill_cred} FROM #Raw
\t\tUNION
\t\tSELECT DISTINCT FirstName = {ref_fn},  MidName = {ref_mn},  LastName = {ref_ln},  FullName = {ref_full},  Id = 'zzNull', NPI = {ref_npi},  Cred = {ref_cred}  FROM #Raw
\t\t)
\tINSERT INTO bl.Provider WITH(TABLOCK)
\t\t(ProvFirstName, ProvMidName, ProvLastName, ProvFullNameOrig, ProvId, ProvNpi, ProvCredentials)
\tSELECT FirstName, MidName, LastName, FullName, Id, NPI, Cred
\tFROM ProviderList AS a
\t\tLEFT JOIN bl.Provider AS b
\t\t\tON a.FirstName = b.ProvFirstName AND a.MidName = b.ProvMidName AND a.LastName = b.ProvLastName
\t\t\tAND a.FullName = b.ProvFullNameOrig AND a.Id = b.ProvId AND a.NPI = b.ProvNpi AND a.Cred = b.ProvCredentials
\tWHERE b.ProvFirstName IS NULL OR b.ProvMidName IS NULL OR b.ProvLastName IS NULL
\t\tOR b.ProvFullNameOrig IS NULL OR b.ProvId IS NULL OR b.ProvNpi IS NULL OR b.ProvCredentials IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.Provider with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.Provider with new values.' END;


/* bl.IcdCodes */
BEGIN
\tWITH IcdList AS
\t\t(
\t\tSELECT DISTINCT Code = {icd1}, IcdDesc = {icd1_desc} FROM #Raw
\t\tUNION
\t\tSELECT DISTINCT TOP 1 Code = 'zzNull', IcdDesc = 'zzNull' FROM #Raw
\t\t)
\tINSERT INTO bl.IcdCodes WITH(TABLOCK) (IcdCodeOrig, IcdCodeDesc)
\tSELECT Code, IcdDesc FROM IcdList AS a
\t\tLEFT JOIN bl.IcdCodes AS b ON a.Code = b.IcdCodeOrig AND a.IcdDesc = b.IcdCodeDesc
\tWHERE b.IcdCodeOrig IS NULL OR b.IcdCodeDesc IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.IcdCodes with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.IcdCodes with new values.' END;


/* bl.ReasonCodes */
BEGIN
\tWITH ReasonList AS
\t\t(SELECT DISTINCT TOP 1 Code = 'zzNull', CodeDesc = 'zzNull', CodeCat = 'zzNull' FROM #Raw)
\tINSERT INTO bl.ReasonCodes WITH(TABLOCK) (ReasonCodeOrig, ReasonCodeDescOrig, ReasonCodeCatOrig)
\tSELECT Code, CodeDesc, CodeCat FROM ReasonList AS a
\t\tLEFT JOIN bl.ReasonCodes AS b ON a.Code = b.ReasonCodeOrig AND a.CodeDesc = b.ReasonCodeDescOrig AND a.CodeCat = b.ReasonCodeCatOrig
\tWHERE b.ReasonCodeOrig IS NULL OR b.ReasonCodeDescOrig IS NULL OR b.ReasonCodeCatOrig IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.ReasonCodes with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.ReasonCodes with new values.' END;


/* bl.TaxIdNumber */
BEGIN
\tWITH TinList AS (SELECT DISTINCT Id = {bill_tin} FROM #Raw)
\tINSERT INTO bl.TaxIdNumber WITH(TABLOCK) (SourceFnbr, Tin)
\tSELECT @DefaultParamaterDataSource, a.Id FROM TinList AS a
\t\tLEFT JOIN bl.TaxIdNumber AS b ON b.SourceFnbr = @DefaultParamaterDataSource AND a.Id = b.Tin
\tWHERE b.Tin IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.TaxIdNumber with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.TaxIdNumber with new values.' END;


/* Populate Load Log */
BEGIN
\tINSERT INTO ds.LoadLog (SourceFnbr,[FileName],EventDesc)
\tVALUES (@DefaultParamaterDataSource,@FileName,'Load')
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate ds.LoadLog for Charge records.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate ds.LoadLog for Charge records.'; END;


/* bl.ChargeFact */
BEGIN
\tWITH NextFileFnbr AS
\t\t(
\t\tSELECT FileUNbr AS FileFnbr FROM ds.LoadLog
\t\tWHERE SourceFnbr = @DefaultParamaterDataSource AND [FileName] = @FileName
\t\t)
\tINSERT INTO bl.[ChargeFact] WITH(TABLOCK)
\t\t(
\t\t [SourceFnbr],[FileFnbr]
\t\t,[ChrgIdFnbr],[TransactionPostDateFnbr],[ChargePostDateFnbr],[DateOfServiceFnbr]
\t\t,[TranTypeFnbr],[PatAgeFnbr],[GenderFnbr]
\t\t,[BillDeptFnbr],[BillLocFnbr],[BillPracFnbr]
\t\t,[ChargePayerFnbr],[TransactionPayerFnbr],[PatFnbr]
\t\t,[PatRaceFnbr],[PatMarStatusFnbr],[PosFnbr],[CptFnbr]
\t\t,[BillProvSpecialtyFnbr],[RenderProvSpecialtyFnbr],[ReferProvSpecialtyFnbr]
\t\t,[BillProvFnbr],[RenderProvFnbr],[ReferProvFnbr]
\t\t,[PrimaryIcdCodeFnbr],[SecondaryIcdCodeFnbr]
\t\t,[InvoiceFnbr],[ReasonCodeFnbr],[TinFnbr]
\t\t,[UnitsOrig],[ChargeAmtOrig],[ChargeAmtClean]
\t\t,[WorkRvuOrig],[WorkRvuInflowAdjusted]
\t\t,[TransactionPostDate],[ChargePostDate],[DateOfService]
\t\t)
\tSELECT
\t\t @DefaultParamaterDataSource, nf.[FileFnbr]
\t\t,b.ChrgIdUnbr
\t\t,c.DateUnbr    -- TransactionPostDate
\t\t,d.DateUnbr    -- ChargePostDate
\t\t,e.DateUnbr    -- DateOfService
\t\t,f.TranTypeUnbr
\t\t,g.PatAgeUnbr
\t\t,h.GenderUnbr
\t\t,i.BillDeptUnbr
\t\t,j.BillLocUnbr
\t\t,k.BillPracUnbr
\t\t,l.PayerUnbr   -- ChargePayerFnbr
\t\t,m.PayerUnbr   -- TransactionPayerFnbr
\t\t,n.PatUnbr
\t\t,o.PatRaceUnbr
\t\t,p.PatMarStatusUnbr
\t\t,q.PosUnbr
\t\t,r.CptUnbr
\t\t,s.ProvSpecialtyUnbr  -- Billing
\t\t,t.ProvSpecialtyUnbr  -- Rendering
\t\t,u.ProvSpecialtyUnbr  -- Referring
\t\t,v.ProvUnbr   -- Billing
\t\t,w.ProvUnbr   -- Rendering
\t\t,x.ProvUnbr   -- Referring
\t\t,y.IcdCodeUnbr -- Primary
\t\t,z.IcdCodeUnbr -- Secondary
\t\t,aa.InvoiceUnbr
\t\t,bb.ReasonCodeUnbr
\t\t,cc.TinUnbr
\t\t,CONVERT(DECIMAL(8,0),{units_ref})
\t\t,CONVERT(DECIMAL(14,2),{charge_ref})
\t\t,CONVERT(DECIMAL(14,2),{charge_ref})
\t\t,CONVERT(DECIMAL(8,2),{rvu_ref})
\t\t,CONVERT(DECIMAL(8,2),{rvu_clean_ref})
\t\t,a.{post_dt}   -- TransactionPostDate
\t\t,a.{post_dt}   -- ChargePostDate
\t\t,a.{dos}       -- DateOfService
\tFROM NextFileFnbr AS nf
\t\t,#Raw AS a
\t\t\tLEFT JOIN bl.ChargeId AS b ON b.SourceFnbr = @DefaultParamaterDataSource AND b.ChrgId = {chrg_id}
\t\t\tLEFT JOIN bl.[DATE] AS c ON c.[DATE] = a.{post_dt}
\t\t\tLEFT JOIN bl.[DATE] AS d ON d.[DATE] = a.{post_dt}
\t\t\tLEFT JOIN bl.[DATE] AS e ON e.[DATE] = a.{dos}
\t\t\tLEFT JOIN bl.TransactionType AS f ON f.TranType = 'zzNull' AND f.TranTypeDesc = 'zzNull'
\t\t\tLEFT JOIN bl.PatientAge AS g ON g.PatAge = {pat_age}
\t\t\tLEFT JOIN bl.PatientGender AS h ON h.GenderOrig = {pat_gender}
\t\t\tLEFT JOIN bl.BillDepartment AS i ON i.SourceFnbr = @DefaultParamaterDataSource AND i.BillDeptId = {dept_id} AND i.BillDeptNameOrig = {dept_name}
\t\t\tLEFT JOIN bl.BillLocation AS j ON j.SourceFnbr = @DefaultParamaterDataSource AND j.BillLocId = 'zzNull' AND j.BillLocNameOrig = {loc_name} AND j.BillLocAddress = 'zzNull' AND j.BillLocCity = 'zzNull' AND j.BillLocState = 'zzNull' AND j.BillLocZipOrig = 'zzNull'
\t\t\tLEFT JOIN bl.BillPractice AS k ON k.SourceFnbr = @DefaultParamaterDataSource AND k.BillPracId = 'zzNull' AND k.BillPracNameOrig = {prac_name}
\t\t\tLEFT JOIN bl.Payer AS l ON l.PayerName = {payer_name} AND l.PayerPlan = {payer_plan} AND l.PayerFinClass = {payer_class}
\t\t\tLEFT JOIN bl.Payer AS m ON m.PayerName = {payer_name} AND m.PayerPlan = {payer_plan} AND m.PayerFinClass = {payer_class}
\t\t\tLEFT JOIN bl.BillPatient AS n ON n.SourceFnbr = @DefaultParamaterDataSource AND n.PatIdOrig = {pat_id} AND n.PatCity = {pat_city} AND n.PatState = LEFT({pat_state},30) AND n.PatZipOrig = {pat_zip} AND n.PatMrn = {pat_mrn}
\t\t\tLEFT JOIN bl.PatientRace AS o ON o.PatRace = {pat_race}
\t\t\tLEFT JOIN bl.PatientMaritalStatus AS p ON p.PatMaritalStatus = {pat_marital}
\t\t\tLEFT JOIN bl.Pos AS q ON q.PosCode = {pos_code} AND q.PosName = 'zzNull' AND q.PosDesc = 'zzNull'
\t\t\tLEFT JOIN bl.CptCodes AS r ON r.CptCodeOrig = {cpt_code} AND r.CptCodeDescOrig = LEFT({cpt_desc},250) AND r.CptServiceYear = ISNULL(YEAR(a.{dos}),YEAR(GETDATE())) AND r.Modifier1 = {mod1} AND r.Modifier2 = {mod2} AND r.Modifier3 = {mod3} AND r.Modifier4 = {mod4}
\t\t\tLEFT JOIN bl.ProviderSpecialty AS s ON s.ProvSpecialty = {bill_spec}
\t\t\tLEFT JOIN bl.ProviderSpecialty AS t ON t.ProvSpecialty = {rend_spec}
\t\t\tLEFT JOIN bl.ProviderSpecialty AS u ON u.ProvSpecialty = {ref_spec}
\t\t\tLEFT JOIN bl.Provider AS v ON v.ProvFirstName = {bill_fn} AND v.ProvMidName = {bill_mn} AND v.ProvLastName = {bill_ln} AND v.ProvFullNameOrig = {bill_full} AND v.ProvId = 'zzNull' AND v.ProvNpi = {bill_npi} AND v.ProvCredentials = {bill_cred}
\t\t\tLEFT JOIN bl.Provider AS w ON w.ProvFirstName = {rend_fn} AND w.ProvMidName = {rend_mn} AND w.ProvLastName = {rend_ln} AND w.ProvFullNameOrig = {rend_full} AND w.ProvId = 'zzNull' AND w.ProvNpi = {rend_npi} AND w.ProvCredentials = {rend_cred}
\t\t\tLEFT JOIN bl.Provider AS x ON x.ProvFirstName = {ref_fn}  AND x.ProvMidName = {ref_mn}  AND x.ProvLastName = {ref_ln}  AND x.ProvFullNameOrig = {ref_full}  AND x.ProvId = 'zzNull' AND x.ProvNpi = {ref_npi}  AND x.ProvCredentials = {ref_cred}
\t\t\tLEFT JOIN bl.IcdCodes AS y ON y.IcdCodeOrig = {icd1} AND y.IcdCodeDesc = {icd1_desc}
\t\t\tLEFT JOIN bl.IcdCodes AS z ON z.IcdCodeOrig = 'zzNull' AND z.IcdCodeDesc = 'zzNull'
\t\t\tLEFT JOIN bl.Invoice AS aa ON aa.SourceFnbr = @DefaultParamaterDataSource AND aa.InvoiceNumber = {invoice}
\t\t\tLEFT JOIN bl.ReasonCodes AS bb ON bb.ReasonCodeOrig = 'zzNull' AND bb.ReasonCodeDescOrig = 'zzNull' AND bb.ReasonCodeCatOrig = 'zzNull'
\t\t\tLEFT JOIN bl.TaxIdNumber AS cc ON cc.SourceFnbr = @DefaultParamaterDataSource AND cc.Tin = {bill_tin}
END;

BEGIN
\tDECLARE @RowCountVar INT
\tSET @RowCountVar = @@ROWCOUNT;
\tUPDATE ds.LoadLog
\tSET RowsAffected = @RowCountVar
\tWHERE SourceFNbr = @DefaultParamaterDataSource AND [FileName] = @FileName
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Insert charge records and update ds.LoadLog table with row count.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Insert charge records and update ds.LoadLog table with row count.' END;
"""


# ─────────────────────────────────────────────────────────────────────────────
# BILLING — TRANSACTIONS
# ─────────────────────────────────────────────────────────────────────────────

def billing_transactions_dimensions_and_fact(col_map: dict[str, str], ds_number: int) -> str:
    """Generate billing transactions dimension + fact INSERT sections."""

    def r(stg: str) -> str:
        return _raw(col_map, stg)

    dos_raw = col_map.get("DateOfService") or col_map.get("DateOfService_Calculated")
    post_date_raw = col_map.get("PostDate") or col_map.get("PostDate_Calculated")
    dos = f"[{dos_raw}]" if dos_raw else "NULL"
    post_dt = f"[{post_date_raw}]" if post_date_raw else "NULL"
    tran_type = r("TransactionType")
    tran_desc = r("TransactionTypeDesc")
    payer_name = r("ChargePayerName")
    payer_plan = r("ChargePayerPlan")
    payer_class = r("ChargePayerFinancialClass")
    reason_code = r("ReasonCode")
    reason_desc = r("ReasonCodeDesc")
    reason_cat = r("ReasonCodeCat")
    invoice = r("InvoiceNumber")
    payment_raw = col_map.get("PaymentOriginal") or col_map.get("PaymentOrig") or col_map.get("PaymentOriginal_Calculated")
    adj_raw = col_map.get("AdjustmentOriginal") or col_map.get("AdjustmentOrig") or col_map.get("AdjustmentOriginal_Calculated")
    payment_ref = f"a.[{payment_raw}]" if payment_raw else "NULL"
    adj_ref = f"a.[{adj_raw}]" if adj_raw else "NULL"

    return f"""

/* bl.Date */
BEGIN
\tWITH DateList AS
\t\t(
\t\tSELECT DISTINCT FullDate = {dos}     FROM #Raw WHERE {dos}     IS NOT NULL
\t\tUNION
\t\tSELECT DISTINCT FullDate = {post_dt} FROM #Raw WHERE {post_dt} IS NOT NULL
\t\t)
\tINSERT INTO bl.[Date] WITH(TABLOCK) ([Date],[Year],[Month],YearMonth)
\tSELECT FullDate, YEAR(FullDate), MONTH(FullDate), LEFT(CONVERT(VARCHAR(8),CONVERT(DATE,FullDate),112),6)
\tFROM DateList AS a LEFT JOIN bl.[Date] AS b ON a.FullDate = b.[Date] WHERE b.[Date] IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.Date with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.Date with new values.' END;


/* bl.TransactionType */
BEGIN
\tWITH TranTypeList AS
\t\t(SELECT DISTINCT [Type] = {tran_type}, TranDesc = {tran_desc} FROM #Raw)
\tINSERT INTO bl.[TransactionType] WITH(TABLOCK) (TranType, TranTypeDesc)
\tSELECT [Type], TranDesc FROM TranTypeList AS a
\t\tLEFT JOIN bl.TransactionType AS b ON a.[Type] = b.TranType AND a.TranDesc = b.TranTypeDesc
\tWHERE b.TranType IS NULL OR b.TranTypeDesc IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.TransactionType with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.TransactionType with new values.' END;


/* bl.Payer */
BEGIN
\tWITH PayerList AS
\t\t(SELECT DISTINCT Name = {payer_name}, [Plan] = {payer_plan}, Class = {payer_class} FROM #Raw)
\tINSERT INTO bl.Payer WITH(TABLOCK) (PayerName, PayerPlan, PayerFinClass)
\tSELECT Name, [Plan], Class FROM PayerList AS a
\t\tLEFT JOIN bl.Payer AS b ON a.Name = b.PayerName AND a.[Plan] = b.PayerPlan AND a.Class = b.PayerFinClass
\tWHERE b.PayerName IS NULL OR b.PayerPlan IS NULL OR b.PayerFinClass IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.Payer with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.Payer with new values.' END;


/* bl.ReasonCodes */
BEGIN
\tWITH ReasonList AS
\t\t(SELECT DISTINCT Code = {reason_code}, CodeDesc = {reason_desc}, CodeCat = {reason_cat} FROM #Raw)
\tINSERT INTO bl.ReasonCodes WITH(TABLOCK) (ReasonCodeOrig, ReasonCodeDescOrig, ReasonCodeCatOrig)
\tSELECT Code, CodeDesc, CodeCat FROM ReasonList AS a
\t\tLEFT JOIN bl.ReasonCodes AS b ON a.Code = b.ReasonCodeOrig AND a.CodeDesc = b.ReasonCodeDescOrig AND a.CodeCat = b.ReasonCodeCatOrig
\tWHERE b.ReasonCodeOrig IS NULL OR b.ReasonCodeDescOrig IS NULL OR b.ReasonCodeCatOrig IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.ReasonCodes with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.ReasonCodes with new values.' END;


/* bl.Invoice */
BEGIN
\tWITH InvoiceList AS (SELECT DISTINCT Invoice = {invoice} FROM #Raw)
\tINSERT INTO bl.Invoice WITH(TABLOCK) (SourceFnbr, InvoiceNumber)
\tSELECT @DefaultParamaterDataSource, a.Invoice FROM InvoiceList AS a
\t\tLEFT JOIN bl.Invoice AS b ON b.SourceFnbr = @DefaultParamaterDataSource AND a.Invoice = b.InvoiceNumber
\tWHERE b.InvoiceNumber IS NULL
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate bl.Invoice with new values.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate bl.Invoice with new values.' END;


/* Populate Load Log */
BEGIN
\tINSERT INTO ds.LoadLog (SourceFnbr,[FileName],EventDesc)
\tVALUES (@DefaultParamaterDataSource,@FileName,'Load')
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Populate ds.LoadLog.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Populate ds.LoadLog.'; END;


/* bl.TransactionFact */
BEGIN
\tWITH NextFileFnbr AS
\t\t(SELECT FileUNbr AS FileFnbr FROM ds.LoadLog WHERE SourceFnbr = @DefaultParamaterDataSource AND [FileName] = @FileName)
\tINSERT INTO bl.TransactionFact WITH(TABLOCK)
\t\t(
\t\t [SourceFnbr],[FileFnbr]
\t\t,[TransactionPostDateFnbr],[DateOfServiceFnbr]
\t\t,[TranTypeFnbr],[ChargePayerFnbr],[TransactionPayerFnbr]
\t\t,[ReasonCodeFnbr],[InvoiceFnbr]
\t\t,[PaymentOrig],[PaymentClean]
\t\t,[AdjustmentOrig],[AdjustmentClean]
\t\t,[TransactionPostDate],[DateOfService]
\t\t)
\tSELECT
\t\t @DefaultParamaterDataSource, nf.[FileFnbr]
\t\t,c.DateUnbr    -- TransactionPostDate
\t\t,e.DateUnbr    -- DateOfService
\t\t,f.TranTypeUnbr
\t\t,l.PayerUnbr   -- ChargePayerFnbr
\t\t,m.PayerUnbr   -- TransactionPayerFnbr
\t\t,bb.ReasonCodeUnbr
\t\t,aa.InvoiceUnbr
\t\t,CONVERT(DECIMAL(14,2),{payment_ref})
\t\t,CONVERT(DECIMAL(14,2),{payment_ref})
\t\t,CONVERT(DECIMAL(14,2),{adj_ref})
\t\t,CONVERT(DECIMAL(14,2),{adj_ref})
\t\t,a.{post_dt}
\t\t,a.{dos}
\tFROM NextFileFnbr AS nf
\t\t,#Raw AS a
\t\t\tLEFT JOIN bl.[DATE] AS c ON c.[DATE] = a.{post_dt}
\t\t\tLEFT JOIN bl.[DATE] AS e ON e.[DATE] = a.{dos}
\t\t\tLEFT JOIN bl.TransactionType AS f ON f.TranType = {tran_type} AND f.TranTypeDesc = {tran_desc}
\t\t\tLEFT JOIN bl.Payer AS l ON l.PayerName = {payer_name} AND l.PayerPlan = {payer_plan} AND l.PayerFinClass = {payer_class}
\t\t\tLEFT JOIN bl.Payer AS m ON m.PayerName = {payer_name} AND m.PayerPlan = {payer_plan} AND m.PayerFinClass = {payer_class}
\t\t\tLEFT JOIN bl.ReasonCodes AS bb ON bb.ReasonCodeOrig = {reason_code} AND bb.ReasonCodeDescOrig = {reason_desc} AND bb.ReasonCodeCatOrig = {reason_cat}
\t\t\tLEFT JOIN bl.Invoice AS aa ON aa.SourceFnbr = @DefaultParamaterDataSource AND aa.InvoiceNumber = {invoice}
END;

BEGIN
\tDECLARE @RowCountVar INT
\tSET @RowCountVar = @@ROWCOUNT;
\tUPDATE ds.LoadLog
\tSET RowsAffected = @RowCountVar
\tWHERE SourceFNbr = @DefaultParamaterDataSource AND [FileName] = @FileName
END;
IF @@ERROR <> 0 BEGIN PRINT CHAR(13) + N'ERROR: Insert transaction records and update ds.LoadLog table with row count.'; RETURN 99 END
ELSE BEGIN PRINT CHAR(13) + N'SUCCESS: Insert transaction records and update ds.LoadLog table with row count.' END;
"""


# ─────────────────────────────────────────────────────────────────────────────
# DISPATCHER
# ─────────────────────────────────────────────────────────────────────────────

def get_dimensions_and_fact(source: str, col_map: dict[str, str], ds_number: int) -> str:
    """Return the dimension + fact SQL for the given source type."""
    if source in ("payroll",):
        return payroll_dimensions_and_fact(col_map, ds_number)
    if source in ("gl",):
        return gl_dimensions_and_fact(col_map, ds_number)
    if source in ("scheduling",):
        return scheduling_dimensions_and_fact(col_map, ds_number)
    if source in ("billing_charges", "billing_combined"):
        return billing_charges_dimensions_and_fact(col_map, ds_number)
    if source in ("billing_transactions",):
        return billing_transactions_dimensions_and_fact(col_map, ds_number)
    # Unknown source — return a TODO placeholder
    return f"""
/* TODO: Dimension and fact inserts for source type '{source}' are not yet templated. */
/* Add custom dimension and fact INSERT logic here. */
"""
