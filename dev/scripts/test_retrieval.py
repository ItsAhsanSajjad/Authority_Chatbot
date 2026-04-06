import os
import sys
from dotenv import load_dotenv
from retriever import retrieve, rewrite_contextual_query

load_dotenv()

query = "CTO ki powers?"
print(f"Original Query: {query}")

# Test Rewriting
rewritten = rewrite_contextual_query(query, None, None)
print(f"Rewritten Query: {rewritten}")

with open("retrieval_log.txt", "w", encoding="utf-8") as f:
    f.write(f"Original Query: {query}\n")
    f.write(f"Rewritten Query: {rewritten}\n")
    f.write("Retrieving...\n")
    results = retrieve(rewritten)
    f.write(f"Has Evidence: {results['has_evidence']}\n")
    for doc in results['evidence']:
        f.write(f"\nDoc: {doc['doc_name']} (Max Score: {doc['max_score']})\n")
        for hit in doc['hits']:
            f.write(f"  - [{hit['score']:.4f}] {hit['text'][:100]}...\n")
            
print("Done. Wrote to retrieval_log.txt")
