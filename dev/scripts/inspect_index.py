"""
Inspect Index
"""
import sys
import os
sys.path.append(os.getcwd())
from dotenv import load_dotenv
load_dotenv()

from index_store import load_index_and_chunks, embed_texts

def search_index(query):
    print(f"\nSearching for: '{query}'")
    ptr = "assets/indexes/build_20260206_233624_hierarchical"
    idx, chunks = load_index_and_chunks(ptr)
    
    vec = embed_texts([query])[0]
    D, I = idx.search(vec.reshape(1, -1), 100)
    
    found = 0
    for i, idx_id in enumerate(I[0]):
        if idx_id < 0: continue
        ch = chunks[idx_id]
        txt = ch.get("text", "")
        doc = ch.get("doc_name", "")
        page = ch.get("loc_start", "")
        
        # Check if it looks like the CTO JD
        if "Chief Technology Officer" in txt:
            print(f"\n--- HIT {i} (Score {D[0][i]:.4f}) ---")
            print(f"Doc: {doc} (Page {page})")
            print(f"Text Preview:\n{txt[:300]}...")
            found += 1
            
    print(f"\nTotal hits mentioning 'Chief Technology Officer': {found}")

if __name__ == "__main__":
    search_index("Chief Technology Officer Position Title")
