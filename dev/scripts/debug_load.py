"""
Debug Load Only
"""
import sys
import os
import time
sys.path.append(os.getcwd())

print(f"Starting debug_load.py")
try:
    from index_store import load_index_and_chunks, ActiveIndexPointer
    print("Imported successfully.")
    
    ptr = ActiveIndexPointer().read_raw()
    print(f"Active Pointer: {ptr}")
    
    if ptr:
        print("Calling load_index_and_chunks...")
        start = time.time()
        idx, chunks = load_index_and_chunks(ptr)
        print(f"Load success. Chunks: {len(chunks)}. Time: {time.time()-start:.2f}s")
    else:
        print("No active pointer found.")
        
except Exception as e:
    print(f"Load failed: {e}")
    import traceback
    traceback.print_exc()
