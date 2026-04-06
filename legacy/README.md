# Legacy Streamlit Frontend (DEPRECATED)

This directory contains the legacy Streamlit-based frontend for PERA AI.

> **⚠️ DEPRECATED**: This application bypasses the FastAPI API layer and calls 
> retriever/answerer modules directly. It does NOT support:
> - Authentication / authorization
> - Session persistence (uses Streamlit session state only)
> - Audit trail logging
> - Rate limiting
> - Entity anchoring / context state
> - Smalltalk bypass
>
> **Use the FastAPI + Next.js stack instead** (`fastapi_app.py` + `frontend/`).

## Files
- `app.py` — Original Streamlit chat UI with PDF viewer
- `retriever_legacy.py` — Previous version of retriever module
- `abbreviations.py` — Previous standalone abbreviation maps (now consolidated in `pera_vocab.py`)
