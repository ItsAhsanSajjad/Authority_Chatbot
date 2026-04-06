"""
Debug Hang
"""
import os
import time
import sys
sys.path.append(os.getcwd())
from dotenv import load_dotenv
load_dotenv()

def test_embed():
    print("1. Testing embed_texts...")
    try:
        from index_store import embed_texts
        start = time.time()
        vecs = embed_texts(["Test query"])
        print(f"   Embed success. Shape: {vecs.shape}. Time: {time.time()-start:.2f}s")
    except Exception as e:
        print(f"   Embed failed: {e}")

def test_load_index():
    print("2. Testing load_index_and_chunks...")
    try:
        from index_store import load_index_and_chunks, ActiveIndexPointer
        
        ptr = ActiveIndexPointer().read_raw()
        print(f"   Active Pointer: {ptr}")
        
        if ptr:
            start = time.time()
            idx, chunks = load_index_and_chunks(ptr)
            print(f"   Load success. Chunks: {len(chunks)}. Time: {time.time()-start:.2f}s")
        else:
            print("   No active pointer found.")
            
    except Exception as e:
        print(f"   Load failed: {e}")

if __name__ == "__main__":
    test_embed()
    test_load_index()
