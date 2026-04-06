"""
Verify Brain 2.0 Components
"""
import os
import sys

# Ensure we can import modules
sys.path.append(os.getcwd())
sys.stdout.reconfigure(encoding='utf-8')

from dotenv import load_dotenv
load_dotenv()

def test_retriever():
    print("\n--- Testing Retriever ---")
    try:
        from retriever import retrieve, rewrite_contextual_query
        
        # 1. Test Rewrite
        rewritten = rewrite_contextual_query("uski power kya hai?", "Who is the CTO?", "The CTO is the head of technology...")
        print(f"Rewrite: 'uski power kya hai?' -> '{rewritten}'")
        
        # Test Fresh Query Rewrite (Always On)
        q_fresh = "CTO powers"
        rw_fresh = rewrite_contextual_query(q_fresh, None, None)
        print(f"Rewrite (Fresh): '{q_fresh}' -> '{rw_fresh}'")
        
        # Use the rewritten fresh query for retrieval test if it looks good
        # But for now, let's stick to the script flow
        
        # 2. Test Retrieve (End-to-End Simulation)
        q = "Database Administrator salary"
        print(f"Original Query: '{q}'")
        
        # Simulate FastAPI logic: Rewrite first
        rewritten_q = rewrite_contextual_query(q, None, None)
        print(f"Rewritten Query: '{rewritten_q}'")
        
        print(f"Retrieving for: '{rewritten_q}'")
        res = retrieve(rewritten_q)
        print(f"Result Result Type: {type(res)}")
        if res:
            print(f"Has Evidence: {res.get('has_evidence')}")
            print(f"Evidence Count: {len(res.get('evidence', []))}")
            
            if res.get('evidence'):
                first = res['evidence'][0]
                print(f"First Doc: {first.get('doc_name')}")
                
                print("\n--- Top 3 Evidence Chunks ---")
                count = 0
                for doc in res['evidence']:
                    for hit in doc['hits']:
                        if count >= 3: break
                        print(f"Hit {count+1} (Score {hit['score']:.4f}):")
                        print(f"{hit['text'][:300]}...")
                        print("-----------------------------")
                        count += 1
        else:
            print("Retrieve returned None/Empty")
            
        return res
    except Exception as e:
        print(f"Retriever Error: {e}")
        import traceback
        traceback.print_exc()
        return None

def test_answerer(retrieval_result):
    print("\n--- Testing Answerer ---")
    if not retrieval_result or not retrieval_result.get('has_evidence'):
        print("Skipping answerer test (no evidence found)")
        return

    try:
        from answerer import answer_question
        
        q = "Database Administrator salary"
        print(f"Answering: '{q}'")
        
        # Mock history
        history = [{"role": "user", "content": "Who is CTO?"}, {"role": "assistant", "content": "The CTO is..."}]
        
        ans = answer_question(q, retrieval_result, history)
        
        print(f"Decision: {ans.get('decision')}")
        print(f"Answer: {ans.get('answer')}")
        print(f"References: {len(ans.get('references', []))}")
        
    except Exception as e:
        print(f"Answerer Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    r = test_retriever()
    test_answerer(r)
