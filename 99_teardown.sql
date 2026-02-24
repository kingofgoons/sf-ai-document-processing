-- ============================================================
-- AI/ML Demo - Teardown Script
-- ============================================================
--
-- WARNING: This script removes ALL demo objects.
-- Section 1 cleans up demo data (safe, uses CORTEX_AI_DEMO_ROLE).
-- Section 2 drops the database and role (destructive, uses ACCOUNTADMIN).
--
-- ============================================================


-- ============================================================
-- Section 1: Clean up demo data (CORTEX_AI_DEMO_ROLE)
-- ============================================================

USE ROLE CORTEX_AI_DEMO_ROLE;
USE DATABASE AIML_DEMO_DB;
USE SCHEMA DOC_PROCESSING;
USE WAREHOUSE DATA_ENGINEERING_WH;

-- Phase 1 tables
DROP TABLE IF EXISTS EXTRACTED_LEASE_DATA;
DROP TABLE IF EXISTS PARSED_LEASES;

-- Phase 2 tables
DROP TABLE IF EXISTS COMPLEX_EXTRACTED_LEASE_DATA_MULTIPASS;
DROP TABLE IF EXISTS COMPLEX_EXTRACTED_LEASE_DATA_COMPLETE;
DROP TABLE IF EXISTS PARSED_COMPLEX_LEASES;

-- Phase 1 stage (also removes all uploaded PDFs)
DROP STAGE IF EXISTS LEASE_DOCUMENTS;

-- Phase 2 stage
DROP STAGE IF EXISTS COMPLEX_LEASE_DOCUMENTS;

-- Verify
SHOW STAGES IN SCHEMA DOC_PROCESSING;
SHOW TABLES LIKE '%LEASE%' IN SCHEMA DOC_PROCESSING;

-- Expected: Both queries return 0 rows


-- ============================================================
-- Section 2: Drop database and role (ACCOUNTADMIN)
-- Only run when completely done with ALL demos.
-- ============================================================

/*
USE ROLE ACCOUNTADMIN;

DROP DATABASE IF EXISTS AIML_DEMO_DB;
DROP ROLE IF EXISTS CORTEX_AI_DEMO_ROLE;

-- Verify
SHOW DATABASES LIKE 'AIML_DEMO_DB';
SHOW ROLES LIKE 'CORTEX_AI_DEMO_ROLE';

-- Expected: Both queries return 0 rows
*/
