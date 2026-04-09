"""Test requisition ingestion with a single tehsil (Shalimar = 410)."""
import sys
sys.path.insert(0, r'D:\authority_chatbot\Pera_staff_chatbot')

from requisition_ingest import ingest_requisitions

# Test with Shalimar (410), last 7 days
result = ingest_requisitions(
    start_date="2026-04-01",
    end_date="2026-04-07",
    tehsil_ids=[410],
    fetch_members=True,
)
print("\n=== RESULT ===")
for k, v in result.items():
    print(f"  {k}: {v}")

# Verify stored data
from analytics_db import get_analytics_db
db = get_analytics_db()

print("\n=== REQUISITION_DETAIL ===")
rows = db.fetch_all("""
    SELECT requisition_id, tehsil_name, requisition_type_name,
           created_at::date as dt, created_by_name, area_location,
           total_squad_members, arrived_members
    FROM requisition_detail
    WHERE tehsil_id = 410
    ORDER BY created_at
""")
for r in rows:
    print(f"  {r['dt']} | {r['requisition_type_name']:20s} | by: {r['created_by_name'] or 'N/A':15s} | area: {r['area_location'] or 'N/A'} | squad: {r['total_squad_members']}/{r['arrived_members']}")
print(f"Total: {len(rows)}")

print("\n=== REQUISITION_MEMBER (sample) ===")
if rows:
    sample_id = rows[0]['requisition_id']
    members = db.fetch_all("""
        SELECT member_name, is_completed, arrival_time, departure_time
        FROM requisition_member
        WHERE requisition_id = %s
        ORDER BY member_name
    """, (sample_id,))
    print(f"Members for requisition {sample_id}:")
    for m in members:
        status = "Completed" if m['is_completed'] else "Not completed"
        print(f"  {m['member_name']:30s} | {status} | arrived: {m['arrival_time']}")
