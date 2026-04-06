"""End-to-end test for the config-driven API lookup registry."""
import os
import sys
import time

import requests

os.chdir(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, ".")

BASE = "http://localhost:8000"
SID = f"e2e_test_{int(time.time())}"


def ask(q, mode="documents", label=""):
    r = requests.post(
        f"{BASE}/api/ask",
        json={"question": q, "source_mode": mode, "session_id": SID},
        timeout=60,
    )
    d = r.json()
    ans = d.get("answer", "")[:200]
    refs = d.get("references", [])
    ref_parts = []
    for ref in refs[:3]:
        ref_parts.append(f"{ref.get('document', '?')} p{ref.get('page_start', '?')}")
    ref_str = "; ".join(ref_parts) if ref_parts else "no refs"
    ok = d.get("decision", "") != "error"
    status = "PASS" if ok else "FAIL"
    print(f"  [{status}] {label}: {ans[:120]}...")
    print(f"         refs: {ref_str}")
    return d


print("=" * 70)
print("END-TO-END TESTS - Config-Driven API Lookup Registry")
print("=" * 70)

print("\n--- 1. DOCUMENT QUERIES (unchanged) ---")
ask("What are the responsibilities of the CTO?", "documents", "CTO role")
ask("What is the salary of Director General?", "documents", "DG salary")

print("\n--- 2. STORED API QUERIES (now config-driven) ---")
ask("List all PERA divisions", "stored_api", "Divisions")
ask("What is the workforce strength?", "stored_api", "Strength")
ask("Show me the monthly expenditure", "stored_api", "Finance")

print("\n--- 3. BOTH MODE (hybrid) ---")
ask("What divisions does PERA have?", "both", "Both-divisions")

print("\n--- 4. LIVE API ---")
ask("List divisions live data", "live_api", "Live-divisions")

print("\n--- 5. EDGE CASES ---")
ask("What is the recipe for biryani?", "documents", "Out-of-scope")
ask("PERA ki divisions kitni hain?", "stored_api", "Urdu-divisions")

print("\n" + "=" * 70)
print("ALL END-TO-END TESTS COMPLETED")
print("=" * 70)
