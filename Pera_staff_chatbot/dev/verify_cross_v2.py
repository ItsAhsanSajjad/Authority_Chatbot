"""Verify cross-reference counts with the new logic."""
import sys
sys.path.insert(0, r'D:\authority_chatbot\Pera_staff_chatbot')
from analytics_db import get_analytics_db
db = get_analytics_db()

# Price Control members (from req data)
pc_members = ['Rabia Altaf', 'Ghulam Abdul Basit SGT-0458', 'Muhammad Shehbaz',
              'Sharyar Butt SS-0003', 'Amir Hussain EO-0103', 'Arslan Ameen SGT-0508',
              'Muhammad Ali Khalil', 'Shakeel Ahmad EO-134', 'Waqar Arshad SGT-0457']

# Anti Encroachment members
ae_members = ['Ahmad Yaar EO-032', 'Ahmed Raza IO- 029', 'Muhammad Subhan Sarwar SGT-0572',
              'Ghulam Abdul Basit SGT-0458', 'Muhammad Shehbaz', 'Rabia Altaf',
              'Sharyar Butt SS-0003', 'Amir Hussain EO-0103', 'Muhammad Imran  SGT-0859']

for req_type, members in [("Price Control", pc_members), ("Anti Encroachment", ae_members), ("Public Nuisance", pc_members)]:
    ph = ','.join(['%s'] * len(members))
    rows = db.fetch_all(f"""
        WITH latest AS (
            SELECT DISTINCT ON (challan_id) *
            FROM challan_data
            WHERE tehsil_name = 'Shalimar'
              AND action_date >= '2026-03-20'
              AND action_date < '2026-04-07'
              AND action_date IS NOT NULL
              AND officer_name IN ({ph})
              AND requisition_type_name = %s
            ORDER BY challan_id, snapshot_date DESC
        )
        SELECT COUNT(*) as cnt, SUM(fine_amount) as fine FROM latest
    """, tuple(members + [req_type]))
    r = rows[0] if rows else {"cnt": 0, "fine": 0}
    print(f"{req_type}: {r['cnt']} challans, Rs. {r['fine'] or 0:,.0f}")
