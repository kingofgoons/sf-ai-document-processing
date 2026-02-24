-- =============================================================================
-- DOCUMENT INTELLIGENCE WITH CORTEX AI
-- =============================================================================
-- Demonstrates AI_PARSE_DOCUMENT, AI_TRANSLATE, AI_EXTRACT, and AI_CLASSIFY
-- for automated lease document processing at scale.
--
-- Database:   AIML_DEMO_DB
-- Schema:     DOC_PROCESSING
-- Role:       CORTEX_AI_DEMO_ROLE (least-privileged)
--
-- Prerequisites: Run 00_setup.sql first to create database and role
-- =============================================================================


-- =============================================================================
-- STEP 0: SET CONTEXT
-- =============================================================================
-- Uses CORTEX_AI_DEMO_ROLE - a least-privileged role with:
--   - USAGE on AIML_DEMO_DB and DOC_PROCESSING schema
--   - CREATE TABLE and CREATE STAGE on DOC_PROCESSING schema  
--   - USAGE on DATA_ENGINEERING_WH warehouse
-- =============================================================================

USE ROLE CORTEX_AI_DEMO_ROLE;
USE DATABASE AIML_DEMO_DB;
USE SCHEMA DOC_PROCESSING;
USE WAREHOUSE DATA_ENGINEERING_WH;


-- =============================================================================
-- STEP 1: CREATE STAGE FOR LEASE DOCUMENTS
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | IMPORTANT: AI functions require SNOWFLAKE_SSE encryption.               |
-- | Client-side encryption will cause "Client Side Encryption" errors.      |
-- +-------------------------------------------------------------------------+
-- =============================================================================

CREATE STAGE IF NOT EXISTS LEASE_DOCUMENTS
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for synthetic lease PDFs for AI_PARSE_DOCUMENT demo';

-- Upload PDFs using Snow CLI:
-- snow stage copy leases/*.pdf @LEASE_DOCUMENTS --overwrite

-- Refresh directory after upload
ALTER STAGE LEASE_DOCUMENTS REFRESH;

-- Verify files are uploaded (expect 7 files: 2 high-risk, 2 medium, 2 standard, 1 Spanish)
SELECT * FROM DIRECTORY(@LEASE_DOCUMENTS);


-- =============================================================================
-- STEP 2: AI_PARSE_DOCUMENT - SINGLE DOCUMENT DEMO
-- =============================================================================
-- AI_PARSE_DOCUMENT extracts text and structure from PDFs.
-- LAYOUT mode preserves document formatting and spatial relationships.
-- =============================================================================

-- Parse one lease to examine the extracted structure
SELECT 
    AI_PARSE_DOCUMENT(
        TO_FILE('@LEASE_DOCUMENTS', 'lease_high_risk_001.pdf'),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS parsed_content;


-- =============================================================================
-- STEP 3: AI_PARSE_DOCUMENT - PAGE SPLITTING DEMO
-- =============================================================================
-- page_split: TRUE returns content as an array, one element per page.
-- Useful for multi-page analysis or page-specific extraction.
-- =============================================================================

SELECT 
    AI_PARSE_DOCUMENT(
        TO_FILE('@LEASE_DOCUMENTS', 'lease_high_risk_001.pdf'),
        {'mode': 'LAYOUT', 'page_split': TRUE}
    ) AS parsed_pages;


-- =============================================================================
-- STEP 4: AI_TRANSLATE - SINGLE DOCUMENT DEMO
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | BUSINESS VALUE: Tenants may operate across multiple countries.          |
-- | Non-English leases can now be processed alongside English ones.        |
-- +-------------------------------------------------------------------------+
-- AI_TRANSLATE requires 3 params: text, source_language, target_language
-- Use empty string '' for source to auto-detect language
-- =============================================================================

-- First, view the Spanish lease content
SELECT 
    AI_PARSE_DOCUMENT(
        TO_FILE('@LEASE_DOCUMENTS', 'lease_spanish_miami_001.pdf'),
        {'mode': 'LAYOUT'}
    ):content::VARCHAR AS spanish_content;

-- Translate the Spanish lease to English
SELECT 
    AI_TRANSLATE(
        AI_PARSE_DOCUMENT(
            TO_FILE('@LEASE_DOCUMENTS', 'lease_spanish_miami_001.pdf'),
            {'mode': 'LAYOUT'}
        ):content::VARCHAR,
        '',   -- Source language: auto-detect
        'en'  -- Target language: English
    ) AS translated_to_english;


-- =============================================================================
-- STEP 5: BATCH PARSE - CREATE PARSED_LEASES TABLE
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | EFFICIENCY: Parse each document ONCE and store the results.             |
-- | All subsequent operations read from this table - no re-parsing needed.  |
-- +-------------------------------------------------------------------------+
-- =============================================================================

CREATE OR REPLACE TABLE PARSED_LEASES (
    lease_file      VARCHAR,
    file_size       NUMBER,
    raw_content     TEXT,        -- Original parsed content
    lang            VARCHAR(10), -- Detected language code (en, es, fr, etc.)
    en_content      TEXT         -- English content (translated if needed)
);

-- Batch parse all documents and insert into table
INSERT INTO PARSED_LEASES (lease_file, file_size, raw_content)
SELECT 
    RELATIVE_PATH AS lease_file,
    SIZE AS file_size,
    AI_PARSE_DOCUMENT(
        TO_FILE('@LEASE_DOCUMENTS', RELATIVE_PATH),
        {'mode': 'LAYOUT'}
    ):content::TEXT AS raw_content
FROM DIRECTORY(@LEASE_DOCUMENTS)
WHERE RELATIVE_PATH LIKE '%.pdf';

-- Verify parsed documents
SELECT lease_file, file_size, LEFT(raw_content, 100) || '...' AS content_preview
FROM PARSED_LEASES;


-- =============================================================================
-- STEP 6: DETECT LANGUAGE
-- =============================================================================
-- Use CORTEX.COMPLETE to detect language and store in lang column.
-- This allows us to handle any language dynamically without hardcoding.
-- =============================================================================

UPDATE PARSED_LEASES
SET lang = TRIM(SNOWFLAKE.CORTEX.COMPLETE(
    'llama3.1-8b',
    'What language is this text written in? Reply with ONLY the 2-letter ISO language code (en, de, fr, es, etc). No other text.

Text: ' || LEFT(raw_content, 500)
))
WHERE lang IS NULL;

-- Verify language detection
SELECT lease_file, lang, LEFT(raw_content, 80) || '...' AS content_preview
FROM PARSED_LEASES
ORDER BY lease_file;


-- =============================================================================
-- STEP 7: TRANSLATE NON-ENGLISH DOCUMENTS
-- =============================================================================
-- Use AI_TRANSLATE to populate en_content column.
-- English documents keep original content; others get translated.
-- =============================================================================

UPDATE PARSED_LEASES
SET en_content = CASE 
    WHEN lang != 'en' THEN AI_TRANSLATE(raw_content, lang, 'en')
    ELSE raw_content
END
WHERE en_content IS NULL;

-- Verify translations
SELECT 
    lease_file, 
    lang,
    CASE WHEN lang != 'en' THEN 'TRANSLATED' ELSE 'ORIGINAL' END AS status,
    LEFT(en_content, 100) || '...' AS english_preview
FROM PARSED_LEASES
ORDER BY lease_file;


-- =============================================================================
-- STEP 8: AI_EXTRACT - PULL STRUCTURED FIELDS
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | SYNTAX NOTE: AI_EXTRACT uses question-based format, NOT JSON schema.    |
-- | Each field maps to a natural language question about the document.      |
-- +-------------------------------------------------------------------------+
-- Response accessible via: result:response:field_name
-- Now reading from PARSED_LEASES.en_content - no re-parsing!
-- =============================================================================

SELECT 
    lease_file,
    lang,
    AI_EXTRACT(
        text => en_content,
        responseFormat => {
            'lease_id': 'What is the Lease ID number?',
            'tenant_name': 'What is the legal name of the tenant company?',
            'property_address': 'What is the full street address of the leased premises?',
            'market': 'What is the market name or region?',
            'rentable_sqft': 'What is the total rentable square footage as a number?',
            'lease_start_date': 'What is the lease commencement date?',
            'lease_end_date': 'What is the lease expiration date?',
            'lease_term_months': 'What is the lease term in months?',
            'base_rent_annual': 'What is the annual base rent in dollars?',
            'rent_per_sqft': 'What is the rent per square foot?',
            'rent_escalation': 'What is the annual rent escalation percentage?',
            'security_deposit': 'What is the security deposit amount or terms?',
            'free_rent_period': 'What is the free rent period if any?',
            'ti_allowance': 'What is the tenant improvement allowance?',
            'lease_type': 'What is the lease type (NNN, Gross, Modified Gross)?'
        }
    ) AS extracted_fields
FROM PARSED_LEASES;


-- =============================================================================
-- STEP 9: AI_CLASSIFY - CATEGORIZE BY RISK LEVEL
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | SYNTAX NOTE: AI_CLASSIFY uses category objects with label/description.  |
-- | Descriptions guide the model - keep under 25 words each.                |
-- +-------------------------------------------------------------------------+
-- Result accessible via: result:labels[0]
-- Reading from PARSED_LEASES.en_content - no re-parsing!
-- =============================================================================

SELECT 
    lease_file,
    lang,
    AI_CLASSIFY(
        en_content,
        [
            {'label': 'High Risk', 'description': 'Unusual terms: unlimited liability, no security deposit, termination without penalty, uncapped TI allowance, revenue-based rent'},
            {'label': 'Medium Risk', 'description': 'Non-standard clauses: below-market escalation, extended free rent, early termination options, CAM caps, co-tenancy'},
            {'label': 'Low Risk', 'description': 'Standard NNN terms: 3% escalation, security deposit, standard default provisions, market-rate TI'}
        ]
    ) AS risk_classification
FROM PARSED_LEASES;


-- =============================================================================
-- STEP 10: CREATE FINAL EXTRACTED_LEASE_DATA TABLE
-- =============================================================================
-- Combine extraction and classification into a single analytics-ready table.
-- All operations read from PARSED_LEASES - efficient batch processing!
-- =============================================================================

CREATE OR REPLACE TABLE EXTRACTED_LEASE_DATA AS
WITH extracted AS (
    SELECT 
        lease_file,
        lang,
        lang != 'en' AS was_translated,
        en_content,
        AI_EXTRACT(
            text => en_content,
            responseFormat => {
                'lease_id': 'What is the Lease ID number?',
                'tenant_name': 'What is the legal name of the tenant?',
                'property_address': 'What is the property address?',
                'market': 'What is the market name?',
                'rentable_sqft': 'What is the square footage as a number?',
                'lease_start_date': 'What is the start date?',
                'lease_end_date': 'What is the end date?',
                'lease_term_months': 'What is the term in months?',
                'base_rent_annual': 'What is the annual rent?',
                'rent_per_sqft': 'What is the rent per square foot?',
                'rent_escalation': 'What is the rent escalation percentage?',
                'security_deposit': 'What is the security deposit?',
                'lease_type': 'What is the lease type (NNN, Gross, etc.)?'
            }
        ) AS fields
    FROM PARSED_LEASES
)
SELECT 
    lease_file,
    lang AS source_language,
    was_translated,
    fields:response:lease_id::STRING AS lease_id,
    fields:response:tenant_name::STRING AS tenant_name,
    fields:response:property_address::STRING AS property_address,
    fields:response:market::STRING AS market,
    TRY_CAST(REGEXP_REPLACE(fields:response:rentable_sqft::STRING, '[^0-9]', '') AS NUMBER) AS rentable_sqft,
    fields:response:lease_start_date::STRING AS lease_start_date,
    fields:response:lease_end_date::STRING AS lease_end_date,
    TRY_CAST(fields:response:lease_term_months::STRING AS NUMBER) AS lease_term_months,
    fields:response:base_rent_annual::STRING AS base_rent_annual,
    fields:response:rent_per_sqft::STRING AS rent_per_sqft,
    fields:response:rent_escalation::STRING AS rent_escalation,
    fields:response:security_deposit::STRING AS security_deposit,
    fields:response:lease_type::STRING AS lease_type,
    AI_CLASSIFY(
        en_content,
        [
            {'label': 'High Risk', 'description': 'Unusual terms: unlimited liability, no security deposit, termination without penalty'},
            {'label': 'Medium Risk', 'description': 'Non-standard: below-market escalation, extended free rent, early termination'},
            {'label': 'Low Risk', 'description': 'Standard NNN terms: 3% escalation, security deposit, standard provisions'}
        ]
    ):labels[0]::STRING AS risk_level,
    CURRENT_TIMESTAMP() AS extracted_at
FROM extracted;

-- View the final extracted data
SELECT * FROM EXTRACTED_LEASE_DATA ORDER BY lease_file;


-- =============================================================================
-- KEY TAKEAWAYS
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | EFFICIENT PIPELINE PATTERN:                                             |
-- |                                                                         |
-- | 1. AI_PARSE_DOCUMENT - Parse ONCE, store in PARSED_LEASES table         |
-- |                                                                         |
-- | 2. CORTEX.COMPLETE  - Detect language, UPDATE lang column               |
-- |                                                                         |
-- | 3. AI_TRANSLATE     - Translate non-English, UPDATE en_content column   |
-- |                                                                         |
-- | 4. AI_EXTRACT       - Read from en_content (no re-parsing!)             |
-- |                                                                         |
-- | 5. AI_CLASSIFY      - Read from en_content (no re-parsing!)             |
-- |                                                                         |
-- | This pattern avoids calling AI_PARSE_DOCUMENT multiple times per file!  |
-- +-------------------------------------------------------------------------+
-- 
-- BUSINESS VALUE:
-- - Automate lease abstraction (currently manual process)
-- - Auto-flag high-risk leases for legal review
-- - Process leases in any language for global portfolio
-- - Feed extracted data into dbt models and Cortex Analyst
-- =============================================================================
