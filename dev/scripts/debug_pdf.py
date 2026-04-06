import os
from pypdf import PdfReader

pdf_path = "assets/data/Compiled Working Paper_2nd Meeting PERA 03-07-2025.pdf"

try:
    reader = PdfReader(pdf_path)
    print(f"Reading {pdf_path} ({len(reader.pages)} pages)...")
    
    found_pages = []
    
    for i, page in enumerate(reader.pages):
        text = page.extract_text()
        if "Chief Technology Officer" in text:
            # Check context
            # We want to see if it's a "Position Title" or just a refernece
            found_pages.append((i+1, text))
            
    print(f"Found 'Chief Technology Officer' on {len(found_pages)} pages.")
    
    if found_pages:
        with open("cto_debug.txt", "w", encoding="utf-8") as f:
            for p, txt in found_pages:
                f.write(f"\n--- Page {p} ---\n")
                f.write(txt)
        print("Detailed text written to cto_debug.txt")
        
except Exception as e:
    print(f"Error: {e}")
