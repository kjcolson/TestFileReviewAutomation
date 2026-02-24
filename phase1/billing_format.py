"""
phase1/billing_format.py

Determines whether the submission uses Combined or Separate billing
by inspecting detected source assignments.  (spec §1.2)
"""

from __future__ import annotations

from typing import Any


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def detect_billing_format(
    source_assignments: dict[str, str]
) -> dict[str, Any]:
    """
    Determine billing format from the source assignment dict.

    Parameters
    ----------
    source_assignments : dict[filename, source_name]

    Returns
    -------
    {
        "format":        "combined" | "separate" | "none" | "unknown",
        "billing_files": [list of billing-related filenames],
        "notes":         str,
    }

    Format values:
        "combined"  — one file detected as billing_combined
        "separate"  — at least one billing_charges + one billing_transactions
        "none"      — no billing files detected at all
        "unknown"   — billing files present but pattern unrecognised
    """
    combined_files = [
        fn for fn, src in source_assignments.items()
        if src == "billing_combined"
    ]
    charges_files = [
        fn for fn, src in source_assignments.items()
        if src == "billing_charges"
    ]
    transactions_files = [
        fn for fn, src in source_assignments.items()
        if src == "billing_transactions"
    ]

    all_billing = combined_files + charges_files + transactions_files

    if not all_billing:
        return {
            "format": "none",
            "billing_files": [],
            "notes": "No billing files detected in submission.",
        }

    if combined_files:
        note = f"Combined billing detected: {combined_files}"
        if charges_files or transactions_files:
            note += (
                f"  ⚠ Also found charges/transactions files "
                f"({charges_files + transactions_files}) — verify intent."
            )
        return {
            "format": "combined",
            "billing_files": all_billing,
            "notes": note,
        }

    if charges_files and transactions_files:
        return {
            "format": "separate",
            "billing_files": all_billing,
            "notes": (
                f"Separate billing detected: "
                f"charges={charges_files}, transactions={transactions_files}"
            ),
        }

    # One side missing
    if charges_files and not transactions_files:
        return {
            "format": "unknown",
            "billing_files": all_billing,
            "notes": (
                f"Billing Charges file(s) found but no Transactions file: "
                f"{charges_files}.  Flag for manual review."
            ),
        }

    if transactions_files and not charges_files:
        return {
            "format": "unknown",
            "billing_files": all_billing,
            "notes": (
                f"Billing Transactions file(s) found but no Charges file: "
                f"{transactions_files}.  Flag for manual review."
            ),
        }

    return {
        "format": "unknown",
        "billing_files": all_billing,
        "notes": "Billing files present but format could not be determined.",
    }
