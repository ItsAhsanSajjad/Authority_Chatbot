**PERA AI Assistant**

The PERA AI Assistant is a document-grounded, retrieval-augmented AI system designed to answer questions strictly from official PERA policies, Acts, and notifications. It ensures accurate, verifiable, and non-hallucinatory responses with clear document references.

**Key Features**

Document-Based Answers Only
Responds strictly using indexed PERA documents (PDF/DOCX), with page-level citations.

Automatic Document Ingestion
New or updated documents placed in the data directory are automatically detected and indexed.

Greeting & Small-Talk Handling
Deterministic intent handling for greetings (English, Urdu, Roman Urdu) without triggering document retrieval.

Voice & Text Queries
Supports both typed and voice-based questions via a Streamlit interface.

Hallucination Prevention
Built-in refusal logic when relevant evidence is not found.

**High-Level Architecture**

User submits a query (text or voice)

Greeting/small-talk intent is detected first

Relevant document chunks are retrieved using FAISS

AI generates an answer strictly from retrieved evidence

Verified answer is returned with references

**Technology Stack**

Frontend: Streamlit

Backend: Python

Search & Retrieval: FAISS

AI Models: OpenAI (RAG-based)

Document Parsing: PDF & DOCX extractors

**Usage**

Place PERA documents in assets/data/

Run the application:

streamlit run app.py


Ask questions related to PERA policies and regulations.


**Scope & Disclaimer**


This assistant is designed only for PERA-related queries and does not provide legal advice or information beyond the provided documents.

**API Data Ingestion (Phase 1)**

PERA AI is being extended to ingest data from external APIs in addition to
static PDF/DOCX documents. Phase 1 introduces the configuration, discovery,
and registry foundation:

- Place YAML config files in `assets/apis/` to register API data sources
- No live API fetching or query-time API integration exists yet
- See `docs/API_INGESTION_ARCHITECTURE.md` for the full design
- See `docs/API_SOURCE_CONFIG.md` for the YAML schema reference
- See `docs/API_OPERATIONS_RUNBOOK.md` for operational procedures

Set `API_INGESTION_ENABLED=1` in `.env` to enable the API subsystem when
later phases are implemented.


# ask.pera

## Analytics Database (Phase 1)

PERA AI now includes a PostgreSQL-backed structured data layer designed for
province-scale analytics, historical intelligence, and future hybrid DB+RAG
answers.

**Phase 1 provides:**
- PostgreSQL connection layer with graceful degradation
- Idempotent migration framework (12 migrations)
- Geography dimension tables (divisions, districts, tehsils)
- Date dimension (2020–2030)
- Workforce strength and finance overview fact tables
- Challan status summary scaffold
- Automatic write-through from API ingestion to PostgreSQL
- Curated mapping for 5 API sources

**Configuration:**
- Set `ANALYTICS_DB_ENABLED=1` and `ANALYTICS_WRITE_ENABLED=1` in `.env`
- Set `POSTGRES_URL` to your PostgreSQL connection string
- See `docs/ANALYTICS_DB_ARCHITECTURE.md` for architecture details
- See `docs/ANALYTICS_DATA_MODEL.md` for schema reference
