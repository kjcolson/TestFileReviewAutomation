"""
shared/constants.py

Single source of truth for field requirement classifications, template-to-staging
column mappings, and data format validation rules.

All definitions are frozen at import time.  Update when PIVOT templates change.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Field requirement levels per source
# Source: PIVOT Data Extract Templates (FY26)
# ---------------------------------------------------------------------------

FIELD_REQUIREMENTS: dict[str, dict[str, list[str]]] = {

    "billing_combined": {
        "required": [
            "Date of Service", "Post Date", "CPT-4 Code",
            "CPT Code Modifier 1", "CPT Code Modifier 2",
            "CPT Code Modifier 3", "CPT Code Modifier 4",
            "Units", "Transaction Type", "Transaction Description", "Amount",
            "Work RVUs", "CMS Place of Service Code",
            "Primary ICD-10 CM Code", "Secondary ICD-10 CM Code",
            "Third ICD-10 CM Code", "Fourth ICD-10 CM Code",
            "5th through 25th ICD-10 CM Code",
            "Patient MRN/Identifier", "Patient DOB", "Patient Gender",
            "Patient City", "Patient ZIP Code",
            "Rendering Provider Full Name", "Rendering Provider NPI",
            "Rendering Provider's Primary Specialty",
            "Rendering Provider Credentials",
            "Billing Provider Full Name", "Billing Provider NPI",
            "Billing Provider's Primary Specialty",
            "Billing Provider Credentials",
            "Practice Name", "Billing Location Name", "Department Name",
            "Cost Center",
            "Primary Payer Name", "Primary Payer Plan",
            "Primary Payer Financial Class",
            "Charge ID", "Invoice Number / Encounter ID",
        ],
        "recommended": [
            "CPT Code Description",
            "Rendering Provider First Name", "Rendering Provider Middle Name/Initial",
            "Rendering Provider Last Name", "Rendering Provider ID",
            "Billing Provider First Name", "Billing Provider Middle Name/Initial",
            "Billing Provider Last Name", "Billing Provider ID",
            "Referring Provider ID",
        ],
        "optional": [
            "Last Modified Date",
            "Primary ICD-10 CM Code Description",
            "Secondary ICD-10 CM Code Description",
            "Third ICD-10 CM Code Description",
            "Fourth ICD-10 CM Code Description",
            "5th through 25th ICD-10 CM Description",
            "Patient Race/Ethnicity", "Patient Marital Status",
            "Referring Provider First Name", "Referring Provider Middle Name/Initial",
            "Referring Provider Last Name", "Referring Provider Full Name",
            "Referring Provider NPI", "Referring Provider's Primary Specialty",
            "Referring Provider Credentials",
        ],
    },

    "billing_charges": {
        "required": [
            "Date of Service", "Post Date", "CPT-4 Code",
            "CPT Code Modifier 1", "CPT Code Modifier 2",
            "CPT Code Modifier 3", "CPT Code Modifier 4",
            "Units", "Charge Amount",
            "Work RVUs", "CMS Place of Service Code",
            "Primary ICD-10 CM Code", "Secondary ICD-10 CM Code",
            "Third ICD-10 CM Code", "Fourth ICD-10 CM Code",
            "5th through 25th ICD-10 CM Code",
            "Patient MRN/Identifier", "Patient DOB", "Patient Gender",
            "Patient City", "Patient ZIP Code",
            "Rendering Provider Full Name", "Rendering Provider NPI",
            "Rendering Provider's Primary Specialty",
            "Rendering Provider Credentials",
            "Billing Provider Full Name", "Billing Provider NPI",
            "Billing Provider's Primary Specialty",
            "Billing Provider Credentials",
            "Practice Name", "Billing Location Name", "Department Name",
            "Cost Center",
            "Primary Payer Name", "Primary Payer Plan",
            "Primary Payer Financial Class",
            "Charge ID", "Invoice Number / Encounter ID",
        ],
        "recommended": [
            "CPT Code Description",
            "Rendering Provider First Name", "Rendering Provider Middle Name/Initial",
            "Rendering Provider Last Name", "Rendering Provider ID",
            "Billing Provider First Name", "Billing Provider Middle Name/Initial",
            "Billing Provider Last Name", "Billing Provider ID",
            "Referring Provider ID",
        ],
        "optional": [
            "Last Modified Date",
            "Primary ICD-10 CM Code Description",
            "Secondary ICD-10 CM Code Description",
            "Third ICD-10 CM Code Description",
            "Fourth ICD-10 CM Code Description",
            "5th through 25th ICD-10 CM Description",
            "Patient Race/Ethnicity", "Patient Marital Status",
            "Referring Provider First Name", "Referring Provider Middle Name/Initial",
            "Referring Provider Last Name", "Referring Provider Full Name",
            "Referring Provider NPI", "Referring Provider's Primary Specialty",
            "Referring Provider Credentials",
        ],
    },

    "billing_transactions": {
        "required": [
            "Transaction ID", "Transaction Description", "Post Date",
            "Payment Amount", "Adjustment Amount", "Refund Amount",
            "Payer Name", "Payer Plan", "Payer Financial Class",
            "Charge ID", "Invoice Number / Encounter ID",
        ],
        "recommended": [],
        "optional": [
            "Last Modified Date", "Reason Category",
            "Claim Adjudication Reason Code",
            "Claim Adjudication Reason Description",
            "Other Reason Detail",
        ],
    },

    "scheduling": {
        "required": [
            "Appt ID", "Location Name", "Appt Provider Full Name",
            "Appt Provider NPI", "Patient Identifier", "Appt Type",
            "Created Date", "Appt Date", "Cancel Date", "Cancel Reason",
            "Appt Time", "Scheduled Length", "Appt Status",
        ],
        "recommended": [
            "Practice Name", "Department Name", "Cost Center",
            "Appt Provider First Name", "Appt Provider Middle Name",
            "Appt Provider Last Name", "Appt Provider ID",
            "Referring Provider ID",
            "Check In Date", "Check In Time",
            "Check Out Date", "Check Out Time",
        ],
        "optional": [
            "Appt Provider Credentials", "Appt Provider Primary Specialty",
            "Referring Provider Full Name",
            "Referring Provider First Name", "Referring Provider Middle Name",
            "Referring Provider Last Name",
            "Referring Provider Credentials", "Referring Provider NPI",
            "Referring Provider Primary Specialty",
        ],
    },

    "payroll": {
        "required": [
            "Employee ID", "Employee Full Name",
            "Job Code ID", "Job Code Description",
            "Department ID",
            "Pay Period Start Date", "Pay Period End Date",
            "Earnings Code", "Earnings Description",
            "Hours", "Amount",
        ],
        "recommended": [
            "Provider ID",
            "Employee First Name", "Employee Middle Name", "Employee Last Name",
            "Employee NPI", "Department Name",
        ],
        "optional": [
            "Check/Pay Date",
        ],
    },

    "gl": {
        "required": [
            "Cost Center Number", "Cost Center Name",
            "Report Date", "Account #", "Account Description", "Amount",
        ],
        "recommended": [
            "Account Type",
        ],
        "optional": [
            "Sub-Account Number", "Sub-Account Desc",
        ],
    },

    "quality": {
        "required": [
            "Provider NPI",
            "Measurement Period Start Date", "Measurement Period End Date",
            "Measure Number", "Is_Inverse",
            "Denominator", "Exclusions/Exceptions", "Numerator",
            "Performance Rate",
        ],
        "recommended": [],
        "optional": [
            "Provider Name", "Measure Description",
            "Initial Population", "Benchmark Target",
        ],
    },

    "patient_satisfaction": {
        "required": [
            "Provider NPI",
            "Survey Date Range Start", "Survey Date Range End",
            "Survey Question Full", "Question Order", "Score",
        ],
        "recommended": [],
        "optional": [
            "Provider Name", "Survey Question Abbreviated",
            "Number of Respondents", "Standard Deviation",
            "Benchmarking Filter", "Benchmark 1", "Benchmark 2",
        ],
    },
}


# ---------------------------------------------------------------------------
# Template field → staging column mapping
#
# Maps (source, template_field_name) → staging Source_Column name(s).
# Values are either a single string or a list[str] (any covered = PRESENT).
#
# Keyed on Source_Column from StagingTableStructure.xlsx, which matches
# StagingColumn in RawToStagingColumnMapping.xlsx and r["staging_col"] in
# phase1_findings.json column_mappings entries.
#
# Special sentinel values:
#   None  — no staging column exists; field is skipped in schema validation
#   "_raw_check" — no staging table; compare against raw column headers instead
# ---------------------------------------------------------------------------

TEMPLATE_TO_STAGING: dict[tuple[str, str], str | list[str] | None] = {

    # ── billing_combined → #staging_billing ─────────────────────────────────
    ("billing_combined", "Date of Service"):                    "DateOfService",
    ("billing_combined", "Post Date"):                          "PostDate",
    ("billing_combined", "CPT-4 Code"):                         "CptCode",
    ("billing_combined", "CPT Code Modifier 1"):                "Modifier1",
    ("billing_combined", "CPT Code Modifier 2"):                "Modifier2",
    ("billing_combined", "CPT Code Modifier 3"):                "Modifier3",
    ("billing_combined", "CPT Code Modifier 4"):                "Modifier4",
    ("billing_combined", "Units"):                              "Units",
    ("billing_combined", "Transaction Type"):                   "TransactionType",
    ("billing_combined", "Transaction Description"):            "TransactionTypeDesc",
    ("billing_combined", "Amount"):         ["ChargeAmountOriginal", "PaymentOriginal"],
    ("billing_combined", "Work RVUs"):                          "WorkRvuOriginal",
    ("billing_combined", "CMS Place of Service Code"):          "PlaceOfServiceCode",
    ("billing_combined", "Primary ICD-10 CM Code"):             "PrimaryIcdCode",
    ("billing_combined", "Secondary ICD-10 CM Code"):           "SecondaryIcdCodes",
    ("billing_combined", "Third ICD-10 CM Code"):               "SecondaryIcdCodes",
    ("billing_combined", "Fourth ICD-10 CM Code"):              "SecondaryIcdCodes",
    ("billing_combined", "5th through 25th ICD-10 CM Code"):    "SecondaryIcdCodes",
    ("billing_combined", "Patient MRN/Identifier"):     ["PatientId", "PatientMrn"],
    ("billing_combined", "Patient DOB"):                        None,
    ("billing_combined", "Patient Gender"):                     "PatientGender",
    ("billing_combined", "Patient City"):                       "PatientCity",
    ("billing_combined", "Patient ZIP Code"):                   "PatientZip",
    ("billing_combined", "Rendering Provider Full Name"):       "RenderingProviderFullName",
    ("billing_combined", "Rendering Provider NPI"):             "RenderingProviderNpi",
    ("billing_combined", "Rendering Provider's Primary Specialty"): "RenderingProviderSpecialty",
    ("billing_combined", "Rendering Provider Credentials"):     "RenderingProviderCredentials",
    ("billing_combined", "Billing Provider Full Name"):         "BillingProviderFullName",
    ("billing_combined", "Billing Provider NPI"):               "BillingProviderNpi",
    ("billing_combined", "Billing Provider's Primary Specialty"): "BillingProviderSpecialty",
    ("billing_combined", "Billing Provider Credentials"):       "BillingProviderCredentials",
    ("billing_combined", "Practice Name"):                      "BillPracticeName",
    ("billing_combined", "Billing Location Name"):              "BillLocationName",
    ("billing_combined", "Department Name"):                    "BillDepartmentName",
    ("billing_combined", "Cost Center"):        ["BillDepartmentId", "BillDepartmentName"],
    ("billing_combined", "Primary Payer Name"): ["ChargePayerName", "TransactionPayerName"],
    ("billing_combined", "Primary Payer Plan"): ["ChargePayerPlan", "TransactionPayerPlan"],
    ("billing_combined", "Primary Payer Financial Class"): [
        "ChargePayerFinancialClass", "TransactionPayerFinancialClass"
    ],
    ("billing_combined", "Charge ID"):                          "ChargeId",
    ("billing_combined", "Invoice Number / Encounter ID"):      "InvoiceNumber",
    # recommended
    ("billing_combined", "CPT Code Description"):               "CptCodeDesc",
    ("billing_combined", "Rendering Provider First Name"):      "RenderingProviderFirstName",
    ("billing_combined", "Rendering Provider Middle Name/Initial"): "RenderingProviderMiddleName",
    ("billing_combined", "Rendering Provider Last Name"):       "RenderingProviderLastName",
    ("billing_combined", "Rendering Provider ID"):              "RenderingProviderId",
    ("billing_combined", "Billing Provider First Name"):        "BillingProviderFirstName",
    ("billing_combined", "Billing Provider Middle Name/Initial"): "BillingProviderMiddleName",
    ("billing_combined", "Billing Provider Last Name"):         "BillingProviderLastName",
    ("billing_combined", "Billing Provider ID"):                "BillingProviderId",
    ("billing_combined", "Referring Provider ID"):              "ReferringProviderId",
    # optional
    ("billing_combined", "Last Modified Date"):                 None,
    ("billing_combined", "Primary ICD-10 CM Code Description"): "PrimaryIcdDesc",
    ("billing_combined", "Secondary ICD-10 CM Code Description"): "SecondaryIcdDesc",
    ("billing_combined", "Third ICD-10 CM Code Description"):   "SecondaryIcdDesc",
    ("billing_combined", "Fourth ICD-10 CM Code Description"):  "SecondaryIcdDesc",
    ("billing_combined", "5th through 25th ICD-10 CM Description"): "SecondaryIcdDesc",
    ("billing_combined", "Patient Race/Ethnicity"):             "PatientRace",
    ("billing_combined", "Patient Marital Status"):             "PatientMaritalStatus",
    ("billing_combined", "Referring Provider First Name"):      "ReferringProviderFirstName",
    ("billing_combined", "Referring Provider Middle Name/Initial"): "ReferringProviderMiddleName",
    ("billing_combined", "Referring Provider Last Name"):       "ReferringProviderLastName",
    ("billing_combined", "Referring Provider Full Name"):       "ReferringProviderFullName",
    ("billing_combined", "Referring Provider NPI"):             "ReferringProviderNpi",
    ("billing_combined", "Referring Provider's Primary Specialty"): "ReferringProviderSpecialty",
    ("billing_combined", "Referring Provider Credentials"):     "ReferringProviderCredentials",

    # ── billing_charges → #staging_charges ──────────────────────────────────
    ("billing_charges", "Date of Service"):                    "DateOfService",
    ("billing_charges", "Post Date"):                          "PostDate",
    ("billing_charges", "CPT-4 Code"):                         "CptCode",
    ("billing_charges", "CPT Code Modifier 1"):                "Modifier1",
    ("billing_charges", "CPT Code Modifier 2"):                "Modifier2",
    ("billing_charges", "CPT Code Modifier 3"):                "Modifier3",
    ("billing_charges", "CPT Code Modifier 4"):                "Modifier4",
    ("billing_charges", "Units"):                              "Units",
    ("billing_charges", "Charge Amount"):                      "ChargeAmountOriginal",
    ("billing_charges", "Work RVUs"):                          "WorkRvuOriginal",
    ("billing_charges", "CMS Place of Service Code"):          "PlaceOfServiceCode",
    ("billing_charges", "Primary ICD-10 CM Code"):             "PrimaryIcdCode",
    ("billing_charges", "Secondary ICD-10 CM Code"):           "SecondaryIcdCodes",
    ("billing_charges", "Third ICD-10 CM Code"):               "SecondaryIcdCodes",
    ("billing_charges", "Fourth ICD-10 CM Code"):              "SecondaryIcdCodes",
    ("billing_charges", "5th through 25th ICD-10 CM Code"):    "SecondaryIcdCodes",
    ("billing_charges", "Patient MRN/Identifier"):     ["PatientId", "PatientMrn"],
    ("billing_charges", "Patient DOB"):                        None,
    ("billing_charges", "Patient Gender"):                     "PatientGender",
    ("billing_charges", "Patient City"):                       "PatientCity",
    ("billing_charges", "Patient ZIP Code"):                   "PatientZip",
    ("billing_charges", "Rendering Provider Full Name"):       "RenderingProviderFullName",
    ("billing_charges", "Rendering Provider NPI"):             "RenderingProviderNpi",
    ("billing_charges", "Rendering Provider's Primary Specialty"): "RenderingProviderSpecialty",
    ("billing_charges", "Rendering Provider Credentials"):     "RenderingProviderCredentials",
    ("billing_charges", "Billing Provider Full Name"):         "BillingProviderFullName",
    ("billing_charges", "Billing Provider NPI"):               "BillingProviderNpi",
    ("billing_charges", "Billing Provider's Primary Specialty"): "BillingProviderSpecialty",
    ("billing_charges", "Billing Provider Credentials"):       "BillingProviderCredentials",
    ("billing_charges", "Practice Name"):                      "BillPracticeName",
    ("billing_charges", "Billing Location Name"):              "BillLocationName",
    ("billing_charges", "Department Name"):                    "BillDepartmentName",
    ("billing_charges", "Cost Center"):        ["BillDepartmentId", "BillDepartmentName"],
    ("billing_charges", "Primary Payer Name"):                 "ChargePayerName",
    ("billing_charges", "Primary Payer Plan"):                 "ChargePayerPlan",
    ("billing_charges", "Primary Payer Financial Class"):      "ChargePayerFinancialClass",
    ("billing_charges", "Charge ID"):                          "ChargeId",
    ("billing_charges", "Invoice Number / Encounter ID"):      "InvoiceNumber",
    # recommended
    ("billing_charges", "CPT Code Description"):               "CptCodeDesc",
    ("billing_charges", "Rendering Provider First Name"):      "RenderingProviderFirstName",
    ("billing_charges", "Rendering Provider Middle Name/Initial"): "RenderingProviderMiddleName",
    ("billing_charges", "Rendering Provider Last Name"):       "RenderingProviderLastName",
    ("billing_charges", "Rendering Provider ID"):              "RenderingProviderId",
    ("billing_charges", "Billing Provider First Name"):        "BillingProviderFirstName",
    ("billing_charges", "Billing Provider Middle Name/Initial"): "BillingProviderMiddleName",
    ("billing_charges", "Billing Provider Last Name"):         "BillingProviderLastName",
    ("billing_charges", "Billing Provider ID"):                "BillingProviderId",
    ("billing_charges", "Referring Provider ID"):              "ReferringProviderId",
    # optional
    ("billing_charges", "Last Modified Date"):                 None,
    ("billing_charges", "Primary ICD-10 CM Code Description"): "PrimaryIcdDesc",
    ("billing_charges", "Secondary ICD-10 CM Code Description"): "SecondaryIcdDesc",
    ("billing_charges", "Third ICD-10 CM Code Description"):   "SecondaryIcdDesc",
    ("billing_charges", "Fourth ICD-10 CM Code Description"):  "SecondaryIcdDesc",
    ("billing_charges", "5th through 25th ICD-10 CM Description"): "SecondaryIcdDesc",
    ("billing_charges", "Patient Race/Ethnicity"):             "PatientRace",
    ("billing_charges", "Patient Marital Status"):             "PatientMaritalStatus",
    ("billing_charges", "Referring Provider First Name"):      "ReferringProviderFirstName",
    ("billing_charges", "Referring Provider Middle Name/Initial"): "ReferringProviderMiddleName",
    ("billing_charges", "Referring Provider Last Name"):       "ReferringProviderLastName",
    ("billing_charges", "Referring Provider Full Name"):       "ReferringProviderFullName",
    ("billing_charges", "Referring Provider NPI"):             "ReferringProviderNpi",
    ("billing_charges", "Referring Provider's Primary Specialty"): "ReferringProviderSpecialty",
    ("billing_charges", "Referring Provider Credentials"):     "ReferringProviderCredentials",

    # ── billing_transactions → #staging_transactions ─────────────────────────
    ("billing_transactions", "Transaction ID"):             ["ChargeId", "InvoiceNumber"],
    ("billing_transactions", "Transaction Description"):    "TransactionTypeDesc",
    ("billing_transactions", "Post Date"):                  "PostDate",
    ("billing_transactions", "Payment Amount"):             "PaymentOriginal",
    ("billing_transactions", "Adjustment Amount"):          "AdjustmentOriginal",
    ("billing_transactions", "Refund Amount"):              "RefundOriginal",
    ("billing_transactions", "Payer Name"):                 "TransactionPayerName",
    ("billing_transactions", "Payer Plan"):                 "TransactionPayerPlan",
    ("billing_transactions", "Payer Financial Class"):      "TransactionPayerFinancialClass",
    ("billing_transactions", "Charge ID"):                  "ChargeId",
    ("billing_transactions", "Invoice Number / Encounter ID"): "InvoiceNumber",
    # optional
    ("billing_transactions", "Last Modified Date"):         None,
    ("billing_transactions", "Reason Category"):            "ReasonCodeCategory",
    ("billing_transactions", "Claim Adjudication Reason Code"): "ReasonCode",
    ("billing_transactions", "Claim Adjudication Reason Description"): "ReasonCodeDesc",
    ("billing_transactions", "Other Reason Detail"):        None,

    # ── scheduling → #staging_scheduling ────────────────────────────────────
    ("scheduling", "Appt ID"):                  "ApptId",
    ("scheduling", "Location Name"):            "BillLocNameOrig",
    ("scheduling", "Appt Provider Full Name"):  "ApptProvFullNameOrig",
    ("scheduling", "Appt Provider NPI"):        "ApptProvNPI",
    ("scheduling", "Patient Identifier"):       "PatIdOrig",
    ("scheduling", "Appt Type"):                "ApptType",
    ("scheduling", "Created Date"):             "CreateDate",
    ("scheduling", "Appt Date"):                "ApptDate",
    ("scheduling", "Cancel Date"):              "CancellationDate",
    ("scheduling", "Cancel Reason"):            "CancelReason",
    ("scheduling", "Appt Time"):                "ApptTime",
    ("scheduling", "Scheduled Length"):         "ApptSchdLength",
    ("scheduling", "Appt Status"):              "ApptStatus",
    # recommended
    ("scheduling", "Practice Name"):            "PracNameOrig",
    ("scheduling", "Department Name"):          "DeptNameOrig",
    ("scheduling", "Cost Center"):              ["DeptId", "DeptNameOrig"],
    ("scheduling", "Appt Provider First Name"): "ApptProvFirstName",
    ("scheduling", "Appt Provider Middle Name"): "ApptProvMidName",
    ("scheduling", "Appt Provider Last Name"):  "ApptProvLastName",
    ("scheduling", "Appt Provider ID"):         "ApptProvId",
    ("scheduling", "Referring Provider ID"):    "ReferProvId",
    ("scheduling", "Check In Date"):            "CheckInDate",
    ("scheduling", "Check In Time"):            "CheckInTime",
    ("scheduling", "Check Out Date"):           "CheckOutDate",
    ("scheduling", "Check Out Time"):           "CheckOutTime",
    # optional
    ("scheduling", "Appt Provider Credentials"):        None,
    ("scheduling", "Appt Provider Primary Specialty"):  "ApptProvSpecialty",
    ("scheduling", "Referring Provider Full Name"):     "ReferProvFullNameOrig",
    ("scheduling", "Referring Provider First Name"):    "ReferProvFirstName",
    ("scheduling", "Referring Provider Middle Name"):   "ReferProvMidName",
    ("scheduling", "Referring Provider Last Name"):     "ReferProvLastName",
    ("scheduling", "Referring Provider Credentials"):   None,
    ("scheduling", "Referring Provider NPI"):           "ReferProvNPI",
    ("scheduling", "Referring Provider Primary Specialty"): "ReferProvSpecialty",

    # ── payroll → #staging_payroll ───────────────────────────────────────────
    ("payroll", "Employee ID"):             "EmployeeId",
    ("payroll", "Employee Full Name"):      "EmployeeFullName",
    ("payroll", "Job Code ID"):             "JobCode",
    ("payroll", "Job Code Description"):    "JobCodeDesc",
    ("payroll", "Department ID"):           "DepartmentId",
    ("payroll", "Pay Period Start Date"):   "PayPeriodStartDate",
    ("payroll", "Pay Period End Date"):     "PayPeriodEndDate",
    ("payroll", "Earnings Code"):           "EarningsCode",
    ("payroll", "Earnings Description"):    "EarningsCodeDesc",
    ("payroll", "Hours"):                   "Hours",
    ("payroll", "Amount"):                  "AmountOrig",
    # recommended
    ("payroll", "Provider ID"):             "EmployeeId",   # proxy — no separate ProvId in payroll
    ("payroll", "Employee First Name"):     "EmployeeFirstName",
    ("payroll", "Employee Middle Name"):    "EmployeeMiddleName",
    ("payroll", "Employee Last Name"):      "EmployeeLastName",
    ("payroll", "Employee NPI"):            "EmployeeNpi",
    ("payroll", "Department Name"):         "DepartmentName",
    # optional
    ("payroll", "Check/Pay Date"):          "CheckDate",

    # ── gl → #staging_gl ────────────────────────────────────────────────────
    ("gl", "Cost Center Number"):   "CostCenterNumberOrig",
    ("gl", "Cost Center Name"):     "CostCenterNameOrig",
    ("gl", "Report Date"):          "YearMonth",
    ("gl", "Account #"):            "AcctNumber",
    ("gl", "Account Description"):  "AcctDesc",
    ("gl", "Amount"):               "AmountOrig",
    # recommended
    ("gl", "Account Type"):         None,   # not in StagingTableStructure.xlsx
    # optional
    ("gl", "Sub-Account Number"):   "SubAcctNumber",
    ("gl", "Sub-Account Desc"):     "SubAcctDesc",

    # ── quality — no staging table; raw-column check ─────────────────────────
    ("quality", "Provider NPI"):                        "_raw_check",
    ("quality", "Measurement Period Start Date"):        "_raw_check",
    ("quality", "Measurement Period End Date"):          "_raw_check",
    ("quality", "Measure Number"):                       "_raw_check",
    ("quality", "Is_Inverse"):                           "_raw_check",
    ("quality", "Denominator"):                          "_raw_check",
    ("quality", "Exclusions/Exceptions"):                "_raw_check",
    ("quality", "Numerator"):                            "_raw_check",
    ("quality", "Performance Rate"):                     "_raw_check",
    ("quality", "Provider Name"):                        "_raw_check",
    ("quality", "Measure Description"):                  "_raw_check",
    ("quality", "Initial Population"):                   "_raw_check",
    ("quality", "Benchmark Target"):                     "_raw_check",

    # ── patient_satisfaction — no staging table; raw-column check ────────────
    ("patient_satisfaction", "Provider NPI"):                   "_raw_check",
    ("patient_satisfaction", "Survey Date Range Start"):         "_raw_check",
    ("patient_satisfaction", "Survey Date Range End"):           "_raw_check",
    ("patient_satisfaction", "Survey Question Full"):            "_raw_check",
    ("patient_satisfaction", "Question Order"):                  "_raw_check",
    ("patient_satisfaction", "Score"):                           "_raw_check",
    ("patient_satisfaction", "Provider Name"):                   "_raw_check",
    ("patient_satisfaction", "Survey Question Abbreviated"):     "_raw_check",
    ("patient_satisfaction", "Number of Respondents"):           "_raw_check",
    ("patient_satisfaction", "Standard Deviation"):              "_raw_check",
    ("patient_satisfaction", "Benchmarking Filter"):             "_raw_check",
    ("patient_satisfaction", "Benchmark 1"):                     "_raw_check",
    ("patient_satisfaction", "Benchmark 2"):                     "_raw_check",
}


# ---------------------------------------------------------------------------
# Domain-specific format patterns
# ---------------------------------------------------------------------------

DATA_FORMAT_PATTERNS: dict[str, dict] = {
    "npi": {
        "pattern": r"^\d{10}$",
        "description": "10-digit numeric",
    },
    "cpt_code": {
        "pattern": r"^\d{5}$|^[A-Za-z]\d{4}$",
        "description": "5-char: 5 digits (99213) or letter+4 digits (T1015)",
    },
    "icd10": {
        "pattern": r"^[A-TV-Z]\d{2,4}\.?\d{0,4}$",
        "description": "ICD-10 format: letter + digits, optional decimal",
    },
    "pos_code": {
        "pattern": r"^\d{1,2}$",
        "description": "1-2 digit numeric CMS place of service code",
    },
    "zip_code": {
        "pattern": r"^\d{5}(-\d{4})?$",
        "description": "5-digit or ZIP+4 format",
    },
    "yearmonth": {
        "pattern": r"^\d{6}$",
        "description": "YYYYMM integer (e.g. 202601)",
    },
    "modifier": {
        "pattern": r"^[A-Z0-9]{2}$|^$",
        "description": "2-character alphanumeric modifier code or blank",
    },
}


# ---------------------------------------------------------------------------
# Maps staging Source_Column → applicable DATA_FORMAT_PATTERNS key
# These are checked during datatype_checker.check() for mapped columns.
# ---------------------------------------------------------------------------

DOMAIN_FIELD_PATTERNS: dict[str, str] = {
    # NPI
    "RenderingProviderNpi":  "npi",
    "BillingProviderNpi":    "npi",
    "ReferringProviderNpi":  "npi",
    "EmployeeNpi":           "npi",
    "ApptProvNPI":           "npi",
    "ReferProvNPI":          "npi",
    # ICD-10
    "PrimaryIcdCode":        "icd10",
    "SecondaryIcdCodes":     "icd10",
    # Place of Service
    "PlaceOfServiceCode":    "pos_code",
    # ZIP
    "PatientZip":            "zip_code",
    "BillLocationZip":       "zip_code",
    # YearMonth
    "YearMonth":             "yearmonth",
    # Modifiers
    "Modifier1":             "modifier",
    "Modifier2":             "modifier",
    "Modifier3":             "modifier",
    "Modifier4":             "modifier",
}
