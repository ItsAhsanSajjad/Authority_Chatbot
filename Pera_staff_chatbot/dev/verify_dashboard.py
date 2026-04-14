"""Compare chatbot vs dashboard numbers for Shalimar Apr 1-5."""
import sys
sys.path.insert(0, r'D:\authority_chatbot\Pera_staff_chatbot')
from analytics_db import get_analytics_db
db = get_analytics_db()

# ALL challans in Shalimar Apr 1-5 (what dashboard shows)
all_ch = db.fetch_all("""
    WITH latest AS (
        SELECT DISTINCT ON (challan_id) *
        FROM challan_data
        WHERE tehsil_name = 'Shalimar'
          AND action_date >= '2026-04-01'
          AND action_date < '2026-04-06'
          AND action_date IS NOT NULL
        ORDER BY challan_id, snapshot_date DESC
    )
    SELECT requisition_type_name, status, COUNT(*) as cnt,
           SUM(fine_amount) as fine
    FROM latest
    GROUP BY requisition_type_name, status
    ORDER BY requisition_type_name, status
""")

print("=== ALL challans in Shalimar Apr 1-5 (dashboard view) ===")
total = 0
total_fine = 0
for r in all_ch:
    print(f"  {r['requisition_type_name']:20s} | {r['status']:8s} | {r['cnt']} | Rs.{r['fine']:,.0f}")
    total += r['cnt']
    total_fine += float(r['fine'] or 0)
print(f"  TOTAL: {total} challans, Rs. {total_fine:,.0f}")

# By officer
print("\n=== By officer ===")
by_off = db.fetch_all("""
    WITH latest AS (
        SELECT DISTINCT ON (challan_id) *
        FROM challan_data
        WHERE tehsil_name = 'Shalimar'
          AND action_date >= '2026-04-01'
          AND action_date < '2026-04-06'
          AND action_date IS NOT NULL
        ORDER BY challan_id, snapshot_date DESC
    )
    SELECT officer_name, COUNT(*) as cnt, SUM(fine_amount) as fine
    FROM latest GROUP BY officer_name ORDER BY cnt DESC
""")
for r in by_off:
    print(f"  {r['officer_name']:30s} | {r['cnt']} challans | Rs.{r['fine']:,.0f}")
