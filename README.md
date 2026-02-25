# AI Document Processing with Snowflake Cortex

End-to-end demo of automated document intelligence using Snowflake Cortex AI functions. Parses, translates, extracts, and classifies commercial lease documents at scale — entirely within Snowflake.

## What This Demonstrates

| Cortex AI Function | Purpose |
|---|---|
| `AI_PARSE_DOCUMENT` | Extract text and structure from PDFs |
| `AI_COMPLETE` | Structured JSON output extraction for 351 fields (Phase 2) |
| `CORTEX.COMPLETE` | Detect document language |
| `AI_TRANSLATE` | Translate non-English documents to English |
| `AI_EXTRACT` | Pull structured fields from unstructured text |
| `AI_CLASSIFY` | Categorize documents by risk level |
| `CORTEX SEARCH SERVICE` | Semantic search over lease document content (Phase 3) |
| `SEMANTIC VIEW` | Natural-language analytics via Cortex Analyst (Phase 3) |
| `CORTEX AGENT` | Unified intelligence combining search + analyst (Phase 3) |
| `SEARCH_PREVIEW` | SQL-based testing of Cortex Search services (Phase 3) |
| `DATA_AGENT_RUN` | Invoke Cortex Agent from SQL (Phase 3) |

## Project Structure

```
00_setup.sql                             # One-time infra setup (ACCOUNTADMIN)
99_teardown.sql                          # Consolidated cleanup

phase1_doc_intelligence/                 # Document Intelligence
  00_generate_leases.py                  # Generate synthetic lease PDFs
  01_doc_intelligence.sql                # Full Cortex AI pipeline
  leases/                               # Generated PDFs (7 files)

phase2_high_field_extraction_at_scale/   # High-field extraction (351 fields)
  00_generate_complex_leases.py          # Generate complex lease PDFs with 351 fields
  01_complex_extraction.sql              # Multi-pass AI_EXTRACT + AI_COMPLETE demo
  leases/                               # Generated PDFs (3 files)

phase3_search_analytics_intelligence/    # Search, Analytics & Intelligence
  01_search_analytics_intelligence.sql   # Cortex Search + Semantic View + Cortex Agent
```

## Prerequisites

- Snowflake account with Cortex AI functions enabled
- `ACCOUNTADMIN` role access (one-time setup only)
- Python 3.8+ with `reportlab` and `python-dateutil` (for PDF generation)
- [Snow CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index) (optional, for stage uploads)

## Quick Start

### 1. Generate Sample Leases

```bash
pip install reportlab python-dateutil
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
    |                          |
    v                          v
CORTEX.COMPLETE         LEASE_SEARCH_CORPUS (join with metadata)
    |                          |
    v                          v
AI_TRANSLATE            Cortex Search Service
    |                          |
    v                          |
AI_EXTRACT                     |
    |                          |
    v                          |
AI_CLASSIFY                    |
    |                          |
    v                          |
EXTRACTED_LEASE_DATA           |
    |                          |
    v                          |
Semantic View                  |
    |                          |
    +----------+---------------+
               |
               v
         Cortex Agent (Snowflake Intelligence)
```

Key efficiency: each document is parsed exactly once. All downstream operations read from `PARSED_LEASES.en_content`.

## Roadmap

### Phase 2: High-Field Extraction at Scale (Complete)

Real commercial leases have 300+ extractable fields. `AI_EXTRACT` caps at 100 questions per call. Phase 2 demonstrates two strategies to handle this:

**Generate complex leases:**

```bash
python phase2_high_field_extraction_at_scale/00_generate_complex_leases.py
```

Produces 3 complex lease PDFs with 351 extractable fields embedded in realistic legal prose across 11 articles (Parties, Premises, Term, Rent, Operating Expenses, Options, Insurance, Construction, Default, Environmental, Miscellaneous).

**Upload and run extraction:**

```bash
snow stage copy phase2_high_field_extraction_at_scale/leases/*.pdf @AIML_DEMO_DB.DOC_PROCESSING.COMPLEX_LEASE_DOCUMENTS --overwrite
```

Then execute `phase2_high_field_extraction_at_scale/01_complex_extraction.sql` step by step:

- **Approach A: Multi-pass AI_EXTRACT** — Splits 351 fields into 4 passes of ~88 fields each, then merges results via JOIN
- **Approach B: AI_COMPLETE with structured output** — Uses `response_format` with JSON schema for structured extraction, batched into 4 groups to stay under the 8192 token output limit

Both approaches read from `PARSED_COMPLEX_LEASES` (parse-once pattern) and produce a final table with all 351 fields as columns.

### Phase 3: Search, Analytics & Intelligence (Complete)

Builds three intelligence layers on top of Phase 1 extracted data:

1. **Cortex Search Service** — Semantic search over raw lease content with attribute filtering
2. **Semantic View** — Business-semantic layer enabling natural-language analytics via Cortex Analyst
3. **Cortex Agent** — Unified Snowflake Intelligence agent that routes questions to search or analyst

**Run Phase 3:**

Execute `phase3_search_analytics_intelligence/01_search_analytics_intelligence.sql` step by step:

- **Step 1**: Create `LEASE_SEARCH_CORPUS` (joins `PARSED_LEASES` content with `EXTRACTED_LEASE_DATA` metadata)
- **Step 2**: Create `LEASE_SEARCH_SERVICE` (Cortex Search over lease text, filterable by market, risk, tenant)
- **Step 3**: Test search with `SEARCH_PREVIEW` (basic queries + filtered queries)
- **Step 4**: Create `LEASE_ANALYTICS_VIEW` (Semantic View with 13 dimensions, 2 facts, 4 metrics)
- **Step 5**: Test semantic view with `SEMANTIC_VIEW()` table function
- **Step 6**: Create `LEASE_INTELLIGENCE_AGENT` (Cortex Agent with LeaseAnalytics + LeaseSearch tools)
- **Step 7**: Verify agent with `DESCRIBE AGENT` / `SHOW AGENTS`
- **Step 8**: Test agent with `DATA_AGENT_RUN` (analytics and search questions)

The agent is also accessible in the Snowflake Intelligence UI (AI & ML > Agents).

## RBAC Model

| Role | Used For |
|---|---|
| `ACCOUNTADMIN` | One-time setup and teardown only |
| `CORTEX_AI_DEMO_ROLE` | All demo execution (least-privileged) |

## License

This project contains synthetic data only. No real customer or tenant information is included.
