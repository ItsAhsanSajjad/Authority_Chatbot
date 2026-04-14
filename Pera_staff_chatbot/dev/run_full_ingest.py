"""Run full requisition ingestion for all tehsils, last 30 days."""
import sys
sys.path.insert(0, r'D:\authority_chatbot\Pera_staff_chatbot')

from requisition_ingest import ingest_requisitions

result = ingest_requisitions(
    start_date="2026-03-08",
    end_date="2026-04-07",
    fetch_members=True,
)
print("\n=== FINAL RESULT ===")
for k, v in result.items():
    print(f"  {k}: {v}")
