import os
import sys
from extractors import extract_pdf_units
from chunker import chunk_units

def debug_file():
    pdf_path = "assets/data/Compiled Working Paper_2nd Meeting PERA 03-07-2025.pdf"
    with open("extraction_log.txt", "w", encoding="utf-8") as f:
        f.write(f"Debug extraction for: {pdf_path}\n")
        
        if not os.path.exists(pdf_path):
            f.write(f"File not found: {pdf_path}\n")
            return

        f.write("Extracting units...\n")
        try:
            units = extract_pdf_units(pdf_path)
            f.write(f"Extracted {len(units)} units (pages).\n")
            
            if len(units) > 0:
                f.write(f"First unit text length: {len(units[0].text)}\n")
                f.write(f"First unit text preview: {units[0].text[:200]}\n")
        except Exception as e:
            f.write(f"Extraction failed: {e}\n")
            return

        f.write("Chunking units...\n")
        chunks = chunk_units(units)
        f.write(f"Generated {len(chunks)} chunks.\n")

        if len(chunks) > 0:
            f.write(f"First chunk text: {chunks[0].chunk_text[:200]}\n")

        # Search for "Chief Technology Officer"
        f.write("Searching for 'Chief Technology Officer' in chunks...\n")
        found_cto = False
        for i, c in enumerate(chunks):
            if "Chief Technology Officer" in c.chunk_text:
                f.write(f"Found CTO in chunk {i} (ID {c.id if hasattr(c, 'id') else 'N/A'}):\n")
                f.write(c.chunk_text + "\n")
                found_cto = True
                break
            if "CTO" in c.chunk_text:
                 f.write(f"Found 'CTO' in chunk {i}: {c.chunk_text[:100]}...\n")

        if not found_cto:
            f.write("Detailed CTO search failed.\n")


if __name__ == "__main__":
    debug_file()
