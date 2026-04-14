import os
import sys
from dotenv import load_dotenv

# Ensure we can import from local modules
sys.path.append(os.getcwd())

from index_manager import SafeAutoIndexer, IndexManagerConfig
from index_store import scan_and_ingest_if_needed

load_dotenv()

def force_rebuild():
    print("Initializing Rebuild (OpenAI)...")
    
    # Config
    cfg = IndexManagerConfig(
        data_dir="assets/data",
        indexes_root="assets/indexes",
        active_pointer_path="assets/indexes/ACTIVE.json"
    )
    
    indexer = SafeAutoIndexer(cfg)
    
    # Create new build dir
    build_dir = indexer._new_build_dir()
    print(f"Build Dir: {build_dir}")
    
    # Run Ingest
    print("Scanning and Ingesting...")
    res = scan_and_ingest_if_needed(
        data_dir=cfg.data_dir,
        index_dir=build_dir,
        chunk_max_chars=cfg.chunk_max_chars,
        chunk_overlap_chars=cfg.chunk_overlap_chars
    )
    
    print(f"Build Result: {res}")
    
    chunks_added = res.get("chunks_added", 0)
    if chunks_added == 0:
        print("Build failed (0 chunks added).")
        # return # actually, continue to debug
    
    print(f"Chunks Added: {chunks_added}")

    # Validate
    if not indexer._validate_index_dir(build_dir):
        print("Validation Failed.")
        return
        
    # Switch Pointer
    print("Switching Active Pointer...")
    indexer.pointer.write_atomic(build_dir)
    print("Active Pointer Updated.")
    
    # Cleanup
    indexer._cleanup_old_builds()
    print("Done.")

if __name__ == "__main__":
    force_rebuild()
