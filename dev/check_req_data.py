"""Check real requisition data for Shalimar."""
import sys
sys.path.insert(0, r'D:\authority_chatbot\Pera_staff_chatbot')
from analytics_db import get_analytics_db
db = get_analytics_db()

rows = db.fetch_all("""
    SELECT requisition_id, requisition_type_name, created_at::date as dt,
           created_by_name, area_location, total_squad_members, arrived_members
    FROM requisition_detail
    WHERE tehsil_name = 'Shalimar'
      AND created_at >= '2026-03-20' AND created_at <= '2026-04-06T23:59:59'
      AND snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_detail)
    ORDER BY created_at
""")
print(f"Total requisitions: {len(rows)}")
for r in rows:
    print(f"  {r['dt']} | {(r['created_by_name'] or ''):15s} | {(r['requisition_type_name'] or ''):20s} | {(r['area_location'] or ''):25s} | squad={r['total_squad_members']} arrived={r['arrived_members']}")

# Check members
print()
req_ids = [r['requisition_id'] for r in rows]
if req_ids:
    ph = ','.join(['%s']*len(req_ids))
    members = db.fetch_all(f"""
        SELECT rm.requisition_id, rm.member_name, rm.is_completed
        FROM requisition_member rm
        WHERE rm.requisition_id IN ({ph})
          AND rm.snapshot_date = (SELECT MAX(snapshot_date) FROM requisition_member)
        ORDER BY rm.requisition_id, rm.member_name
    """, tuple(req_ids))
    print(f"Total members across all requisitions: {len(members)}")
    unique_members = set()
    for m in members:
        unique_members.add(m['member_name'])
        print(f"  req={m['requisition_id'][:8]}... | {(m['member_name'] or ''):30s} | completed={m['is_completed']}")
    print(f"\nUnique member names: {sorted(unique_members)}")
