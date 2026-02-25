-- =============================================================================
-- SEARCH, ANALYTICS & INTELLIGENCE
-- =============================================================================
-- Builds three layers on top of Phase 1 and Phase 2 extracted data:
--
--   1. Cortex Search Service  - Semantic search over raw lease content
--   2. Semantic View           - Natural-language analytics via Cortex Analyst
--   3. Cortex Agent            - Unified interface connecting search + analyst
--
-- Database:   AIML_DEMO_DB
-- Schema:     DOC_PROCESSING
-- Role:       CORTEX_AI_DEMO_ROLE (least-privileged)
--
-- Prerequisites:
--   - Run 00_setup.sql first to create database and role
--   - Run Phase 1 pipeline (01_doc_intelligence.sql) to populate:
--       PARSED_LEASES (7 rows)
--       EXTRACTED_LEASE_DATA (7 rows)
-- =============================================================================


-- =============================================================================
-- STEP 0: SET CONTEXT
-- =============================================================================

USE ROLE CORTEX_AI_DEMO_ROLE;
USE DATABASE AIML_DEMO_DB;
USE SCHEMA DOC_PROCESSING;
USE WAREHOUSE DATA_ENGINEERING_WH;


-- =============================================================================
-- =============================================================================
-- COMPONENT 1: CORTEX SEARCH SERVICE
-- =============================================================================
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | PURPOSE: Enable semantic search over raw lease document content.         |
-- | Users can ask questions like "which lease has an early termination       |
-- | clause?" and get relevant document passages back.                        |
-- +-------------------------------------------------------------------------+
-- =============================================================================


-- =============================================================================
-- STEP 1: CREATE A SEARCH-READY TABLE
-- =============================================================================
-- Cortex Search needs a source table with searchable text and attributes.
-- We join PARSED_LEASES (raw content) with EXTRACTED_LEASE_DATA (metadata)
-- to create a rich searchable dataset.
-- =============================================================================

CREATE OR REPLACE TABLE LEASE_SEARCH_CORPUS AS
SELECT
    p.lease_file,
    e.lease_id,
    e.tenant_name,
    e.market,
    e.risk_level,
    e.lease_type,
    e.rentable_sqft,
    e.base_rent_annual,
    e.source_language,
    p.en_content AS lease_content
FROM PARSED_LEASES p
JOIN EXTRACTED_LEASE_DATA e
    ON p.lease_file = e.lease_file;

-- Verify
SELECT lease_file, lease_id, tenant_name, market, risk_level, LENGTH(lease_content) AS content_length
FROM LEASE_SEARCH_CORPUS
ORDER BY lease_file;


-- =============================================================================
-- STEP 2: CREATE CORTEX SEARCH SERVICE
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | ON = the column to search (full lease text)                             |
-- | ATTRIBUTES = columns available for filtering and return                 |
-- | TARGET_LAG = how often to refresh the index                             |
-- +-------------------------------------------------------------------------+
-- =============================================================================

CREATE OR REPLACE CORTEX SEARCH SERVICE LEASE_SEARCH_SERVICE
    ON lease_content
    ATTRIBUTES lease_file, lease_id, tenant_name, market, risk_level, lease_type
    WAREHOUSE = DATA_ENGINEERING_WH
    TARGET_LAG = '1 day'
    COMMENT = 'Semantic search over commercial lease documents - Phase 3 demo'
AS (
    SELECT
        lease_content,
        lease_file,
        lease_id,
        tenant_name,
        market,
        risk_level,
        lease_type,
        rentable_sqft,
        base_rent_annual,
        source_language
    FROM LEASE_SEARCH_CORPUS
);


-- =============================================================================
-- STEP 3: TEST CORTEX SEARCH
-- =============================================================================
-- SEARCH_PREVIEW is a SQL function for testing. Production apps use the
-- REST API or Python SDK for lower latency.
-- =============================================================================

-- Search for early termination clauses
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'LEASE_SEARCH_SERVICE',
        '{
            "query": "early termination clause",
            "columns": ["lease_file", "lease_id", "tenant_name", "risk_level"],
            "limit": 5
        }'
    )
)['results'] AS results;

-- Search for security deposit terms
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'LEASE_SEARCH_SERVICE',
        '{
            "query": "security deposit requirements",
            "columns": ["lease_file", "lease_id", "tenant_name", "risk_level"],
            "limit": 5
        }'
    )
)['results'] AS results;

-- Search with filter: only high-risk leases
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'LEASE_SEARCH_SERVICE',
        '{
            "query": "liability and indemnification",
            "columns": ["lease_file", "lease_id", "tenant_name", "risk_level"],
            "filter": {"@eq": {"risk_level": "High Risk"}},
            "limit": 5
        }'
    )
)['results'] AS results;

-- Search with filter: specific market
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'LEASE_SEARCH_SERVICE',
        '{
            "query": "rent escalation percentage",
            "columns": ["lease_file", "lease_id", "tenant_name", "market"],
            "filter": {"@eq": {"market": "Dallas"}},
            "limit": 5
        }'
    )
)['results'] AS results;


-- =============================================================================
-- =============================================================================
-- COMPONENT 2: SEMANTIC VIEW (Cortex Analyst)
-- =============================================================================
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | PURPOSE: Enable natural-language analytics over structured lease data.   |
-- | Users can ask "What is the average rent per sqft by market?" and get     |
-- | auto-generated SQL + results.                                            |
-- +-------------------------------------------------------------------------+
-- =============================================================================


-- =============================================================================
-- STEP 4: CREATE SEMANTIC VIEW
-- =============================================================================
-- The semantic view defines business meaning for columns, metrics, and
-- relationships so Cortex Analyst can generate accurate SQL from questions.
-- =============================================================================

CREATE OR REPLACE SEMANTIC VIEW LEASE_ANALYTICS_VIEW

    TABLES (
        leases AS AIML_DEMO_DB.DOC_PROCESSING.EXTRACTED_LEASE_DATA
            PRIMARY KEY (lease_file)
            COMMENT = 'Extracted commercial lease data with risk classification'
    )

    -- NOTE: FACTS must come before DIMENSIONS per Snowflake syntax rules
    FACTS (
        leases.rentable_sqft AS rentable_sqft
            WITH SYNONYMS = ('square footage', 'sqft', 'size', 'area', 'square feet')
            COMMENT = 'Total rentable square footage of the leased space',

        leases.lease_term_months AS lease_term_months
            WITH SYNONYMS = ('term', 'duration', 'lease length', 'months')
            COMMENT = 'Lease term duration in months'
    )

    DIMENSIONS (
        leases.lease_file AS lease_file
            WITH SYNONYMS = ('file', 'document', 'pdf')
            COMMENT = 'Source PDF file name',

        leases.lease_id AS lease_id
            WITH SYNONYMS = ('lease number', 'contract id', 'reference number')
            COMMENT = 'Unique lease identifier (e.g. LNK-2023-79255)',

        leases.tenant_name AS tenant_name
            WITH SYNONYMS = ('tenant', 'company', 'lessee', 'renter')
            COMMENT = 'Legal name of the tenant company',

        leases.property_address AS property_address
            WITH SYNONYMS = ('address', 'location', 'property')
            COMMENT = 'Full street address of the leased property',

        leases.market AS market
            WITH SYNONYMS = ('region', 'metro', 'area', 'geography')
            COMMENT = 'Geographic market (e.g. Dallas, Miami, Los Angeles)',

        leases.lease_type AS lease_type
            WITH SYNONYMS = ('type', 'structure', 'NNN', 'triple net', 'gross')
            COMMENT = 'Lease structure type (NNN, Gross, Modified Gross)',

        leases.risk_level AS risk_level
            WITH SYNONYMS = ('risk', 'risk category', 'risk rating', 'classification')
            COMMENT = 'AI-classified risk level: High Risk, Medium Risk, or Low Risk',

        leases.source_language AS source_language
            WITH SYNONYMS = ('language', 'original language')
            COMMENT = 'Original document language (en, es, etc.)',

        leases.was_translated AS was_translated
            COMMENT = 'Whether the document required translation to English',

        leases.lease_start_date AS lease_start_date
            WITH SYNONYMS = ('start date', 'commencement date', 'effective date')
            COMMENT = 'Lease commencement date',

        leases.lease_end_date AS lease_end_date
            WITH SYNONYMS = ('end date', 'expiration date', 'termination date')
            COMMENT = 'Lease expiration date',

        leases.rent_escalation AS rent_escalation
            WITH SYNONYMS = ('escalation', 'annual increase', 'rent increase')
            COMMENT = 'Annual rent escalation percentage (e.g. 3%)',

        leases.security_deposit AS security_deposit
            WITH SYNONYMS = ('deposit', 'security')
            COMMENT = 'Security deposit amount or terms'
    )

    METRICS (
        leases.lease_count AS COUNT(lease_file)
            COMMENT = 'Total number of leases',

        leases.total_sqft AS SUM(rentable_sqft)
            WITH SYNONYMS = ('total square footage', 'total area', 'portfolio size')
            COMMENT = 'Total rentable square footage across all matching leases',

        leases.avg_sqft AS AVG(rentable_sqft)
            WITH SYNONYMS = ('average size', 'average square footage')
            COMMENT = 'Average rentable square footage per lease',

        leases.avg_term_months AS AVG(lease_term_months)
            WITH SYNONYMS = ('average term', 'average duration', 'average lease length')
            COMMENT = 'Average lease term in months'
    )

    COMMENT = 'Semantic view for commercial lease portfolio analytics - enables natural language queries via Cortex Analyst'

    AI_SQL_GENERATION
        'When querying financial fields like base_rent_annual, rent_per_sqft, or security_deposit, '
        'note these are stored as strings with dollar signs and commas. '
        'Use TRY_CAST(REPLACE(REPLACE(field, ''$'', ''''), '','', '''') AS NUMBER) to convert them to numeric values. '
        'Round all numeric results to 2 decimal places. '
        'When asked about rent or cost, prefer base_rent_annual unless rent per square foot is specifically requested.'

    AI_QUESTION_CATEGORIZATION
        'This semantic view covers commercial lease portfolio data including tenant details, '
        'property locations, financial terms, and risk classifications. '
        'Reject questions that are not related to commercial real estate leases.';


-- =============================================================================
-- STEP 5: TEST SEMANTIC VIEW
-- =============================================================================
-- Query the semantic view directly to verify it works.
-- =============================================================================

-- Test: portfolio summary by market
SELECT * FROM SEMANTIC_VIEW(
    LEASE_ANALYTICS_VIEW
    DIMENSIONS leases.market
    METRICS leases.lease_count, leases.total_sqft, leases.avg_term_months
)
ORDER BY lease_count DESC;

-- Test: risk distribution
SELECT * FROM SEMANTIC_VIEW(
    LEASE_ANALYTICS_VIEW
    DIMENSIONS leases.risk_level
    METRICS leases.lease_count, leases.total_sqft
)
ORDER BY lease_count DESC;

-- Test: tenant portfolio
SELECT * FROM SEMANTIC_VIEW(
    LEASE_ANALYTICS_VIEW
    DIMENSIONS leases.tenant_name, leases.market
    METRICS leases.lease_count, leases.total_sqft
)
ORDER BY total_sqft DESC;


-- =============================================================================
-- =============================================================================
-- COMPONENT 3: CORTEX AGENT (Snowflake Intelligence)
-- =============================================================================
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | PURPOSE: Unified natural-language interface combining search + analyst.  |
-- | Users ask any lease question; the agent routes to the right tool:        |
-- |   - Structured data questions -> Cortex Analyst (semantic view)          |
-- |   - Clause/content questions  -> Cortex Search (raw lease text)          |
-- +-------------------------------------------------------------------------+
-- =============================================================================


-- =============================================================================
-- STEP 6: CREATE CORTEX AGENT
-- =============================================================================

CREATE OR REPLACE AGENT LEASE_INTELLIGENCE_AGENT
    COMMENT = 'Unified lease intelligence agent - combines search and analytics'
    PROFILE = '{"display_name": "Lease Intelligence", "color": "blue"}'
    FROM SPECIFICATION
    $$
    models:
      orchestration: claude-4-sonnet

    orchestration:
      budget:
        seconds: 60
        tokens: 32000

    instructions:
      system: >
        You are a commercial real estate lease intelligence assistant for Link Logistics.
        You help portfolio managers, asset managers, and legal teams understand their
        lease portfolio by answering questions about both structured lease data and
        raw lease document content.

      orchestration: >
        For questions about lease metrics, portfolio statistics, rent amounts, square footage,
        risk distribution, or any quantitative analysis, use the LeaseAnalytics tool.
        For questions about specific lease clauses, legal terms, provisions, or finding
        documents that mention certain topics, use the LeaseSearch tool.
        If a question involves both data and document content, use both tools.

      response: >
        Respond in a clear, professional tone suitable for commercial real estate professionals.
        When presenting numeric data, format dollar amounts with commas and two decimal places.
        When citing lease documents, reference the lease ID and tenant name.

      sample_questions:
        - question: "What is the total square footage by market?"
          answer: "I will query the lease portfolio data to show total square footage broken down by market."
        - question: "Which leases have early termination clauses?"
          answer: "I will search the lease documents for any mentions of early termination provisions."
        - question: "How many high-risk leases do we have?"
          answer: "I will query the lease data to count leases classified as high risk."
        - question: "What are the security deposit terms for Pacific Freight Solutions?"
          answer: "I will search the lease documents for security deposit provisions related to Pacific Freight Solutions."

    tools:
      - tool_spec:
          type: "cortex_analyst_text_to_sql"
          name: "LeaseAnalytics"
          description: >
            Queries structured commercial lease portfolio data including tenant names,
            property addresses, markets, square footage, rent amounts, lease terms,
            risk levels, and lease types. Use for any quantitative or analytical question
            about the lease portfolio.
      - tool_spec:
          type: "cortex_search"
          name: "LeaseSearch"
          description: >
            Searches the full text content of commercial lease documents. Use for finding
            specific clauses, legal provisions, terms and conditions, or any question
            that requires reading the actual lease language.

    tool_resources:
      LeaseAnalytics:
        semantic_view: "AIML_DEMO_DB.DOC_PROCESSING.LEASE_ANALYTICS_VIEW"
        execution_environment:
          type: "warehouse"
          warehouse: "DATA_ENGINEERING_WH"
      LeaseSearch:
        name: "AIML_DEMO_DB.DOC_PROCESSING.LEASE_SEARCH_SERVICE"
        max_results: "5"
        title_column: "lease_id"
        id_column: "lease_file"
    $$;


-- =============================================================================
-- STEP 7: VERIFY AGENT
-- =============================================================================

DESCRIBE AGENT LEASE_INTELLIGENCE_AGENT;
SHOW AGENTS IN SCHEMA DOC_PROCESSING;


-- =============================================================================
-- STEP 8: TEST AGENT
-- =============================================================================
-- Test the agent with various question types to verify routing.
-- =============================================================================

-- Test 1: Structured data question (should route to LeaseAnalytics)
SELECT TRY_PARSE_JSON(
    SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
        'AIML_DEMO_DB.DOC_PROCESSING.LEASE_INTELLIGENCE_AGENT',
        $${"messages": [{"role": "user", "content": [{"type": "text", "text": "How many leases do we have in each market?"}]}]}$$
    )
) AS agent_response;

-- Test 2: Document search question (should route to LeaseSearch)
SELECT TRY_PARSE_JSON(
    SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
        'AIML_DEMO_DB.DOC_PROCESSING.LEASE_INTELLIGENCE_AGENT',
        $${"messages": [{"role": "user", "content": [{"type": "text", "text": "Which leases mention early termination clauses?"}]}]}$$
    )
) AS agent_response;

-- Test 3: Risk analysis (structured data)
SELECT TRY_PARSE_JSON(
    SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
        'AIML_DEMO_DB.DOC_PROCESSING.LEASE_INTELLIGENCE_AGENT',
        $${"messages": [{"role": "user", "content": [{"type": "text", "text": "What is the risk level distribution across our portfolio?"}]}]}$$
    )
) AS agent_response;

-- Test 4: Specific clause search (document search)
SELECT TRY_PARSE_JSON(
    SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
        'AIML_DEMO_DB.DOC_PROCESSING.LEASE_INTELLIGENCE_AGENT',
        $${"messages": [{"role": "user", "content": [{"type": "text", "text": "What are the liability provisions in our high-risk leases?"}]}]}$$
    )
) AS agent_response;


-- =============================================================================
-- KEY TAKEAWAYS
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | COMPONENT 1: CORTEX SEARCH SERVICE                                      |
-- |   - Semantic search over full lease text                                |
-- |   - Filterable by risk_level, market, tenant, lease_type                |
-- |   - Auto-refreshes from base table (TARGET_LAG = 1 day)                |
-- |   - Use SEARCH_PREVIEW for SQL testing; REST/Python API for production  |
-- |                                                                         |
-- | COMPONENT 2: SEMANTIC VIEW                                              |
-- |   - Defines business meaning for columns and metrics                    |
-- |   - Enables Cortex Analyst to generate accurate SQL from questions      |
-- |   - Synonyms help map business language to technical columns            |
-- |   - AI_SQL_GENERATION instructions handle data type quirks              |
-- |                                                                         |
-- | COMPONENT 3: CORTEX AGENT                                               |
-- |   - Unified interface combining search + analyst tools                  |
-- |   - Routes questions to the right tool automatically                    |
-- |   - Powers Snowflake Intelligence UI                                    |
-- |   - Accessible via DATA_AGENT_RUN SQL function or REST API              |
-- |                                                                         |
-- | ARCHITECTURE:                                                           |
-- |                                                                         |
-- | Raw PDFs -> AI_PARSE_DOCUMENT -> PARSED_LEASES                          |
-- |               |                        |                                |
-- |               v                        v                                |
-- |         AI_EXTRACT              LEASE_SEARCH_CORPUS                     |
-- |         AI_CLASSIFY                    |                                |
-- |               |                        v                                |
-- |               v               Cortex Search Service                     |
-- |     EXTRACTED_LEASE_DATA               |                                |
-- |               |                        |                                |
-- |               v                        |                                |
-- |        Semantic View                   |                                |
-- |               |                        |                                |
-- |               +--------+-------+-------+                                |
-- |                        |                                                |
-- |                        v                                                |
-- |                  Cortex Agent                                           |
-- |                        |                                                |
-- |                        v                                                |
-- |             Snowflake Intelligence                                      |
-- +-------------------------------------------------------------------------+
-- =============================================================================
