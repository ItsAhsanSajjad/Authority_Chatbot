
import sys
import os
from pprint import pprint

# Setup paths
sys.path.append(os.getcwd())

# Import retriever logic
try:
    from retriever import retrieve, _build_query_variants, _normalize_text
except ImportError:
    print("Could not import retriever. Run from project root.")
    sys.exit(1)

QUERY = "responsibilities of Manager Development"

print(f"\n--- 1. Testing Normalization & Variants for: '{QUERY}' ---")
qn = _normalize_text(QUERY)
print(f"Normalized: '{qn}'")

variants = _build_query_variants(QUERY)
print("Generated Variants:")
for v in variants:
    print(f"  - {v}")

print(f"\n--- 2. Running Full Retrieval ---")
results = retrieve(QUERY)

print("\n--- 3. Retrieval Result Summary ---")
if not results.get("has_evidence"):
    print("NO EVIDENCE FOUND.")
    if "debug" in results:
        print("DEBUG INFO:")
        pprint(results["debug"])
else:
    print(f"Primary Doc: {results.get('primary_doc')}")
    print(f"Total Reference Docs: {len(results.get('evidence', []))}")
    
    print("\n--- Evidence Hits ---")
    for doc in results.get("evidence", []):
        print(f"\nDoc: {doc['doc_name']} (Rank {doc['doc_rank']})")
        for hit in doc.get("hits", []):
            score = hit.get("score", 0)
            text = hit.get("text", "")[:200].replace("\n", " ")
            print(f"  - Score: {score:.4f} | Text: {text}...")
