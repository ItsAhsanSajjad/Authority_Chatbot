"""Quick script to check OA + challan data for cross-reference debugging."""
import sys
sys.path.insert(0, r'D:\authority_chatbot\Pera_staff_chatbot')
from analytics_db import get_analytics_db
db = get_analytics_db()

print('=== OA DETAIL RECORDS (Shalimar, Apr 1-6) ===')
rows = db.fetch_all("""
    SELECT action_date::date as dt, assigned_to, requisition_name,
           requisition_type_id, status, created_date::date as created
    FROM operational_activity_detail
    WHERE tehsil_name = 'Shalimar'
      AND action_date >= '2026-04-01'
      AND action_date <= '2026-04-06T23:59:59'
      AND snapshot_date = (SELECT MAX(snapshot_date) FROM operational_activity_detail)
    ORDER BY action_date, requisition_name
""")
for r in rows:
    print(f"  {r['dt']} | {r['assigned_to']:20s} | {r['requisition_name']:20s} | type_id={r['requisition_type_id']} | {r['status']}")
print(f"Total OA records: {len(rows)}")

print()
print('=== CHALLAN RECORDS (Shalimar, Apr 1-6) ===')
challans = db.fetch_all("""
    WITH latest AS (
        SELECT DISTINCT ON (challan_id) *
        FROM challan_data
        WHERE tehsil_name = 'Shalimar'
          AND action_date >= '2026-04-01'
          AND action_date < '2026-04-07'
          AND action_date IS NOT NULL
        ORDER BY challan_id, snapshot_date DESC
    )
    SELECT action_date::date as dt, officer_name, requisition_type_name,
           status, fine_amount, challan_id
    FROM latest
    ORDER BY action_date, requisition_type_name, officer_name
""")
for c in challans:
    print(f"  {c['dt']} | {str(c['officer_name'] or ''):20s} | {str(c['requisition_type_name'] or ''):20s} | {c['status']:8s} | Rs.{c['fine_amount']:>10} | {c['challan_id']}")
print(f"Total challans: {len(challans)}")

print()
print('=== CHALLAN SUMMARY BY OFFICER + REQ TYPE ===')
summary = db.fetch_all("""
    WITH latest AS (
        SELECT DISTINCT ON (challan_id) *
        FROM challan_data
        WHERE tehsil_name = 'Shalimar'
          AND action_date >= '2026-04-01'
          AND action_date < '2026-04-07'
          AND action_date IS NOT NULL
        ORDER BY challan_id, snapshot_date DESC
    )
    SELECT officer_name, requisition_type_name, status,
           COUNT(*) as cnt, SUM(fine_amount) as total_fine
    FROM latest
    GROUP BY officer_name, requisition_type_name, status
    ORDER BY officer_name, requisition_type_name, status
""")
for s in summary:
    print(f"  {str(s['officer_name'] or ''):20s} | {str(s['requisition_type_name'] or ''):20s} | {s['status']:8s} | count={s['cnt']} | fine=Rs.{s['total_fine']}")

print()
print('=== OA OFFICERS vs CHALLAN OFFICERS ===')
oa_officers = set(r['assigned_to'] for r in rows)
ch_officers = set(s['officer_name'] for s in summary)
print(f"OA officers: {oa_officers}")
print(f"Challan officers: {ch_officers}")
print(f"Overlap: {oa_officers & ch_officers}")
