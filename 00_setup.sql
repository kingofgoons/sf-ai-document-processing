-- ============================================================
-- AI Document Processing Demo - Setup Script
-- Run this ONCE before any demos to create shared infrastructure
-- ============================================================
-- 
-- This script creates:
--   1. AIML_DEMO_DB database and DOC_PROCESSING schema
--   2. CORTEX_AI_DEMO_ROLE with least-privileged access
--   3. Required grants for the role
--
-- Prerequisites:
--   - ACCOUNTADMIN role access (one-time setup only)
--   - A warehouse must exist (update DATA_ENGINEERING_WH below)
--
-- ============================================================

USE ROLE ACCOUNTADMIN;

-- ============================================================
-- Step 1: Create dedicated database for AI/ML demos
-- ============================================================

CREATE DATABASE IF NOT EXISTS AIML_DEMO_DB
    COMMENT = 'Database for AI/ML demos - document extraction, Cortex Agents, forecasting';

CREATE SCHEMA IF NOT EXISTS AIML_DEMO_DB.DOC_PROCESSING
    COMMENT = 'Schema for document processing demo objects';

-- ============================================================
-- Step 2: Create least-privileged role for demos
-- ============================================================

CREATE ROLE IF NOT EXISTS CORTEX_AI_DEMO_ROLE
    COMMENT = 'Least-privileged role for AI/ML demos';

-- ============================================================
-- Step 3: Grant database and schema privileges
-- ============================================================

GRANT USAGE ON DATABASE AIML_DEMO_DB TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT USAGE ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT CREATE TABLE ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT CREATE STAGE ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT CREATE VIEW ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;

-- Cortex Search and Agents
GRANT CREATE CORTEX SEARCH SERVICE ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT CREATE AGENT ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT CREATE SEMANTIC VIEW ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;

-- Feature Store and Model Registry
GRANT CREATE SCHEMA ON DATABASE AIML_DEMO_DB TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT CREATE DYNAMIC TABLE ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT CREATE MODEL ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE CORTEX_AI_DEMO_ROLE;

-- ============================================================
-- Step 4: Grant warehouse usage
-- ============================================================

GRANT USAGE ON WAREHOUSE DATA_ENGINEERING_WH TO ROLE CORTEX_AI_DEMO_ROLE;

-- ============================================================
-- Step 5: Add role to hierarchy and grant to demo user
-- ============================================================

GRANT ROLE CORTEX_AI_DEMO_ROLE TO ROLE SYSADMIN;

-- Grant to specific users who will run demos (add as needed)
-- GRANT ROLE CORTEX_AI_DEMO_ROLE TO USER <username>;

-- ============================================================
-- Step 6: Verify setup
-- ============================================================

SHOW GRANTS TO ROLE CORTEX_AI_DEMO_ROLE;

-- Test that the role can use AI functions
USE ROLE CORTEX_AI_DEMO_ROLE;
USE DATABASE AIML_DEMO_DB;
USE SCHEMA DOC_PROCESSING;
USE WAREHOUSE DATA_ENGINEERING_WH;

SELECT AI_CLASSIFY('test', ['A', 'B']) AS ai_function_test;

-- ============================================================
-- Setup complete! You can now run the individual demo scripts.
-- ============================================================
