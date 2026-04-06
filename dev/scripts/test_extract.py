"""Test PDF extraction."""
import os

# List PDFs
pdfs = [f for f in os.listdir("assets/data") if f.endswith(".pdf")]
print(f"Found {len(pdfs)} PDFs")

if pdfs:
    pdf_path = f"assets/data/{pdfs[0]}"
    print(f"Testing: {pdf_path}")
    
    try:
        from pypdf import PdfReader
        reader = PdfReader(pdf_path)
        print(f"Pages: {len(reader.pages)}")
        
        text = reader.pages[0].extract_text() or ""
        print(f"Text length: {len(text)}")
        print(f"Sample: {text[:300]}...")
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()

    # Also test the extractor
    print("\n--- Testing extractors.extract_units_from_file ---")
    try:
        from extractors import extract_units_from_file
        units = extract_units_from_file(pdf_path)
        print(f"Units extracted: {len(units)}")
        if units:
            print(f"First unit text length: {len(units[0].text)}")
            print(f"First unit sample: {units[0].text[:200]}...")
    except Exception as e:
        print(f"ERROR in extractor: {e}")
        import traceback
        traceback.print_exc()
