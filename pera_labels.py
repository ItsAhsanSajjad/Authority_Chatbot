"""
PERA AI — Canonical metric label map.

Single dictionary so every renderer (full dashboard, focused metric,
ranking, officer summary) labels a metric the same way.
"""
from __future__ import annotations

from typing import Dict


METRIC_LABELS: Dict[str, str] = {
    # Financial
    "fine_imposed":           "Fine Imposed",
    "fine_amount":            "Fine Imposed",
    "fine_recovered":         "Fine Recovered",
    "paid_amount":            "Paid Amount",
    "outstanding_amount":     "Outstanding Amount",
    "unpaid_amount":          "Outstanding Amount",
    "partially_paid_amount":  "Partially Paid Amount",
    # Counts
    "total_challans":         "Total Challans",
    "paid_challans":          "Paid Challans",
    "unpaid_challans":        "Unpaid Challans",
    "partially_paid_challans":"Partially Paid Challans",
    # Outcomes
    "firs":                   "FIRs Registered",
    "sealed":                 "Sealed Premises",
    "warnings":               "Warnings",
    "no_offense":             "No Offense",
    "no_offenses":            "No Offense",
    "epo":                    "EPO",
    "removal_order":          "Removal Orders",
    "removal_orders":         "Removal Orders",
    "arrest":                 "Arrest Cases",
    # Force / inspection
    "enforcer":               "Enforcers",
    "total_inspections":      "Total Inspections / Actions",
    "total_actions":          "Total Inspections / Actions",
    # Officer-level
    "officer_fine_imposed":   "Officer Fine Imposed",
    "officer_fine_recovered": "Officer Fine Recovered",
    "recovery_pct":           "Paid Recovery %",
}


# Tags so renderers know whether to format as money or count
MONEY_METRICS = {
    "fine_imposed", "fine_amount", "fine_recovered",
    "paid_amount", "outstanding_amount", "unpaid_amount",
    "partially_paid_amount",
    "officer_fine_imposed", "officer_fine_recovered",
}

COUNT_METRICS = {
    "total_challans", "paid_challans", "unpaid_challans",
    "partially_paid_challans",
    "firs", "sealed", "warnings", "no_offense", "no_offenses",
    "epo", "removal_order", "removal_orders",
    "arrest", "enforcer",
    "total_inspections", "total_actions",
}


def get_metric_label(key: str) -> str:
    """Return the canonical user-facing label for a metric key.
    Falls back to a humanised form of the raw key.
    """
    if not key:
        return ""
    if key in METRIC_LABELS:
        return METRIC_LABELS[key]
    return key.replace("_", " ").title()
