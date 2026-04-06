
import sys
import os
import re

# Mock environment if needed
sys.path.append(os.getcwd())

from retriever import _build_query_variants, _normalize_text

QUERY = "responsibilities of Manager Development"
print(f"Original: '{QUERY}'")

qn = _normalize_text(QUERY)
print(f"Normalized: '{qn}'")

variants = _build_query_variants(QUERY)
print(f"Variants count: {len(variants)}")
for i, v in enumerate(variants):
    print(f"{i+1}: {v}")
