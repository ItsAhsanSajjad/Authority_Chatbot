from retriever import retrieve, rewrite_contextual_query
from answerer import answer_question
import sys

# Force UTF-8
sys.stdout.reconfigure(encoding='utf-8')

q = "infra manager ki salary kia hai"
print(f"Query: {q}")

# 1. Test Rewrite
rewritten = rewrite_contextual_query(q, None, None)
print(f"Rewritten: '{rewritten}'")

# 2. Test Retrieve
print("Retrieving...")
res = retrieve(rewritten)
print(f"Has Evidence: {res.get('has_evidence')}")
print(f"Evidence Hits: {len(res.get('evidence', []))}")

if res.get('evidence'):
    # Print top hits
    for doc in res['evidence'][:3]:
        print(f"Doc: {doc['doc_name']}")
        for hit in doc['hits'][:1]:
            print(f"  Score: {hit['score']}")
            print(f"  Text: {hit['text'][:100]}...")

# 3. Test Answer
print("\nAnswering...")
ans = answer_question(q, res)
print(f"Answer: {ans['answer']}")
