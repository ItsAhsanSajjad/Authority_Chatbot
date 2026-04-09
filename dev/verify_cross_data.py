"""Verify cross-reference counts match by date + officer + req type."""
import sys
sys.path.insert(0, r'D:\authority_chatbot\Pera_staff_chatbot')
from analytics_db import get_analytics_db
db = get_analytics_db()

print("=== Rabia Altaf challans by date + req type (Shalimar, Apr 1-6) ===")
rows = db.fetch_all("""
    WITH latest AS (
        SELECT DISTINCT ON (challan_id) *
        FROM challan_data
        WHERE tehsil_name = 'Shalimar'
          AND action_date >= '2026-04-01'
          AND action_date < '2026-04-07'
          AND action_date IS NOT NULL
          AND officer_name = 'Rabia Altaf'
        ORDER BY challan_id, snapshot_date DESC
    )
    SELECT action_date::date as dt, requisition_type_name, status,
           COUNT(*) as cnt, SUM(fine_amount) as fine
    FROM latest
    GROUP BY action_date::date, requisition_type_name, status
    ORDER BY action_date::date, requisition_type_name, status
""")

totals = {}
for r in rows:
    rtn = r['requisition_type_name']
    if rtn not in totals:
        totals[rtn] = {'challans': 0, 'fine': 0}
    totals[rtn]['challans'] += r['cnt']
    totals[rtn]['fine'] += float(r['fine'])
    print(f"  {r['dt']} | {rtn:20s} | {r['status']:8s} | count={r['cnt']} | fine=Rs.{r['fine']}")

print()
print("=== SUMMARY (Rabia Altaf only) ===")
grand = 0
grand_fine = 0
for rtn, t in sorted(totals.items()):
    print(f"  {rtn}: {t['challans']} challans, Rs. {t['fine']:,.0f}")
    grand += t['challans']
    grand_fine += t['fine']
print(f"  TOTAL: {grand} challans, Rs. {grand_fine:,.0f}")
