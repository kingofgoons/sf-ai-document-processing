# AI Document Processing with Snowflake Cortex

End-to-end demo of automated document intelligence using Snowflake Cortex AI functions. Parses, translates, extracts, and classifies commercial lease documents at scale — entirely within Snowflake.

## What This Demonstrates

| Cortex AI Function | Purpose |
|---|---|
| `AI_PARSE_DOCUMENT` | Extract text and structure from PDFs |
| `CORTEX.COMPLETE` | Detect document language; structured output extraction for unlimited fields (Phase 2) |
| `AI_TRANSLATE` | Translate non-English documents to English |
| `AI_EXTRACT` | Pull structured fields from unstructured text |
| `AI_CLASSIFY` | Categorize documents by risk level |

## Project Structure

```
00_setup.sql                             # One-time infra setup (ACCOUNTADMIN)
99_teardown.sql                          # Consolidated cleanup

phase1_doc_intelligence/                 # Document Intelligence
  00_generate_leases.py                  # Generate synthetic lease PDFs
  01_doc_intelligence.sql                # Full Cortex AI pipeline
  leases/                               # Generated PDFs (7 files)

phase2_high_field_extraction_at_scale/   # High-field extraction (planned)
phase3_search_analytics_intelligence/    # Search + Analytics (planned)
```

## Prerequisites

- Snowflake account with Cortex AI functions enabled
- `ACCOUNTADMIN` role access (one-time setup only)
- Python 3.8+ with `reportlab` (for PDF generation)
- [Snow CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (optional, for stage uploads)

## Quick Start

### 1. Generate Sample Leases

```bash
pip install reportlab
python phase1_doc_intelligence/00_generate_leases.py
```

Produces 7 synthetic industrial leases in `phase1_doc_intelligence/leases/`:
- 2 high-risk (unusual terms: unlimited liability, no security deposit)
- 2 medium-risk (non-standard: below-market escalation, early termination)
- 2 standard low-risk (NNN terms, 3% escalation)
- 1 Spanish-language lease (Miami market)

### 2. Run Setup

Execute `00_setup.sql` in Snowsight or via Snow CLI. This creates:

- **Database**: `AIML_DEMO_DB`
- **Schema**: `DOC_PROCESSING`
- **Role**: `CORTEX_AI_DEMO_ROLE` (least-privileged)
- Grants for tables, stages, views, search services, agents, semantic views

### 3. Upload Leases

Upload PDFs to the internal stage. In a Snowsight SQL worksheet:

```sql
USE ROLE CORTEX_AI_DEMO_ROLE;
USE DATABASE AIML_DEMO_DB;
USE SCHEMA DOC_PROCESSING;

CREATE STAGE IF NOT EXISTS LEASE_DOCUMENTS
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Upload via Snowsight UI or Snow CLI:
-- snow stage copy phase1_doc_intelligence/leases/*.pdf @LEASE_DOCUMENTS --overwrite

ALTER STAGE LEASE_DOCUMENTS REFRESH;
```

> **Important**: The stage must use `SNOWFLAKE_SSE` encryption. Client-side encryption breaks AI functions.

### 4. Run the Pipeline

Execute `phase1_doc_intelligence/01_doc_intelligence.sql` step by step. The pipeline:

1. **Parse** PDFs once into `PARSED_LEASES` table
2. **Detect language** via `CORTEX.COMPLETE` (llama3.1-8b)
3. **Translate** non-English documents to English
4. **Extract** ~15 structured fields (lease ID, tenant, rent, dates, etc.)
5. **Classify** each lease by risk level
6. **Create** final `EXTRACTED_LEASE_DATA` analytics table

### 5. Teardown

```sql
-- Section 1: Safe cleanup (drops tables and stage)
-- Section 2: Full infrastructure drop (commented out — uncomment to remove database and role)
```

## Pipeline Pattern

```
PDF files on stage
    |
    v
AI_PARSE_DOCUMENT  -->  PARSED_LEASES table (parse once, reuse)
    |
    v
CORTEX.COMPLETE    -->  Detect language (lang column)
    |
    v
AI_TRANSLATE       -->  English content (en_content column)
    |
    v
AI_EXTRACT         -->  Structured fields (lease_id, tenant, rent, ...)
    |
    v
AI_CLASSIFY        -->  Risk level (High / Medium / Low)
    |
    v
EXTRACTED_LEASE_DATA    (final analytics-ready table)
```

Key efficiency: each document is parsed exactly once. All downstream operations read from `PARSED_LEASES.en_content`.

## Roadmap

### Phase 2: High-Field Extraction at Scale

Real commercial leases have 300+ extractable fields. `AI_EXTRACT` caps at 100 questions per call. Phase 2 demonstrates two strategies:

- **Multi-pass `AI_EXTRACT`**: Split 351 fields into 4 batches, merge results
- **`AI_COMPLETE` structured output**: Single call with JSON schema via `response_format`

Includes a complex lease generator producing PDFs with 351 fields across 9 sections.

### Phase 3: Search, Analytics & Intelligence

- **Cortex Search**: Semantic search over raw lease content
- **Semantic View**: Natural-language analytics via Cortex Analyst
- **Snowflake Intelligence**: Unified interface connecting search + analyst

## RBAC Model

| Role | Used For |
|---|---|
| `ACCOUNTADMIN` | One-time setup and teardown only |
| `CORTEX_AI_DEMO_ROLE` | All demo execution (least-privileged) |

## License

This project contains synthetic data only. No real customer or tenant information is included.
