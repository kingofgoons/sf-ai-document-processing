# Phase 3: Search, Analytics & Intelligence

Three intelligence layers built on top of Phase 1 extracted data. No re-parsing of PDFs — everything reads from `PARSED_LEASES` and `EXTRACTED_LEASE_DATA`.

## Components

| Component | Object | Purpose |
|---|---|---|
| Cortex Search | `LEASE_SEARCH_SERVICE` | Semantic search over raw lease text |
| Semantic View | `LEASE_ANALYTICS_VIEW` | Natural-language analytics via Cortex Analyst |
| Cortex Agent | `LEASE_INTELLIGENCE_AGENT` | Unified interface that routes to search or analyst |

## Sample Questions

### Cortex Search (via `SEARCH_PREVIEW`)

Find specific clauses, provisions, or language across lease documents.

| Question | What it tests |
|---|---|
| "early termination clause" | Basic semantic search across all leases |
| "security deposit requirements" | Financial provision search |
| "liability and indemnification" + filter `risk_level = "High Risk"` | Filtered search by attribute |
| "rent escalation percentage" + filter `market = "Dallas"` | Filtered search by market |
| "environmental contamination" | Unusual clause discovery |
| "subletting assignment rights" | Specific legal provision |
| "maintenance obligations HVAC" | Operational clause search |

**SQL example:**

```sql
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'LEASE_SEARCH_SERVICE',
        '{
            "query": "early termination clause",
            "columns": ["lease_id", "tenant_name", "market", "risk_level"],
            "limit": 5
        }'
    )
)['results'] AS results;

-- With attribute filter:
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'LEASE_SEARCH_SERVICE',
        '{
            "query": "liability and indemnification",
            "columns": ["lease_id", "tenant_name", "risk_level"],
            "filter": {"@eq": {"risk_level": "High Risk"}},
            "limit": 5
        }'
    )
)['results'] AS results;
```

### Cortex Analyst (via Semantic View)

Query structured lease data using the `SEMANTIC_VIEW()` table function.

| Question type | SQL example |
|---|---|
| Lease count by market | `METRICS leases.lease_count DIMENSIONS leases.market` |
| Average sqft by risk level | `METRICS leases.avg_sqft DIMENSIONS leases.risk_level` |
| Total sqft by tenant | `METRICS leases.total_sqft DIMENSIONS leases.tenant_name` |
| Average lease term by market | `METRICS leases.avg_term_months DIMENSIONS leases.market` |
| Portfolio overview | `METRICS leases.lease_count, leases.total_sqft, leases.avg_term_months DIMENSIONS leases.market` |

**SQL example:**

```sql
SELECT * FROM SEMANTIC_VIEW(
    AIML_DEMO_DB.DOC_PROCESSING.LEASE_ANALYTICS_VIEW
    METRICS leases.lease_count, leases.total_sqft
    DIMENSIONS leases.market
)
ORDER BY lease_count DESC;
```

**Available dimensions:** `lease_file`, `lease_id`, `tenant_name`, `property_address`, `market`, `lease_type`, `risk_level`, `source_language`, `was_translated`, `lease_start_date`, `lease_end_date`, `rent_escalation`, `security_deposit`

**Available facts:** `rentable_sqft`, `lease_term_months`

**Available metrics:** `lease_count`, `total_sqft`, `avg_sqft`, `avg_term_months`

### Snowflake Intelligence (via Cortex Agent)

Ask natural-language questions — the agent routes to the right tool automatically.

| Sample question | Expected tool | What it demonstrates |
|---|---|---|
| "How many leases do we have in each market?" | LeaseAnalytics | Portfolio breakdown by geography |
| "What is the average square footage by risk level?" | LeaseAnalytics | Metric aggregation with grouping |
| "Which leases mention early termination clauses?" | LeaseSearch | Document content search |
| "What are the liability provisions in our high-risk leases?" | LeaseSearch | Targeted clause search |
| "What are the security deposit terms for Pacific Freight Solutions?" | LeaseSearch | Tenant-specific document search |
| "How many high-risk leases do we have?" | LeaseAnalytics | Risk distribution count |
| "Show me leases expiring in 2031" | LeaseAnalytics | Date-based filtering |
| "What rent escalation language is in our Dallas leases?" | LeaseSearch | Market-specific clause search |
| "Compare average square footage across markets" | LeaseAnalytics | Cross-market comparison |
| "Find any co-tenancy clauses in the portfolio" | LeaseSearch | Uncommon clause discovery |

**SQL example (DATA_AGENT_RUN):**

```sql
SELECT TRY_PARSE_JSON(
    SNOWFLAKE.CORTEX.DATA_AGENT_RUN(
        'AIML_DEMO_DB.DOC_PROCESSING.LEASE_INTELLIGENCE_AGENT',
        $${"messages": [{"role": "user", "content": [{"type": "text", "text": "How many leases do we have in each market?"}]}]}$$
    )
) AS agent_response;
```

## Accessing Snowflake Intelligence UI

The agent is accessible in Snowsight at **AI & ML > Agents**. It appears as "Lease Intelligence" (blue theme).

### Required Grants

Users need the following privileges to interact with the agent:

```sql
-- Run as ACCOUNTADMIN or a role with MANAGE GRANTS

-- 1. Database role for Cortex Agent access (one of these):
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_USER TO ROLE <role_name>;
-- OR for agents-only access:
GRANT DATABASE ROLE SNOWFLAKE.CORTEX_AGENT_USER TO ROLE <role_name>;

-- 2. Object-level grants:
GRANT USAGE ON DATABASE AIML_DEMO_DB TO ROLE <role_name>;
GRANT USAGE ON SCHEMA AIML_DEMO_DB.DOC_PROCESSING TO ROLE <role_name>;
GRANT USAGE ON AGENT AIML_DEMO_DB.DOC_PROCESSING.LEASE_INTELLIGENCE_AGENT TO ROLE <role_name>;

-- 3. Tool-level grants (required for the agent's tools to work):
GRANT USAGE ON CORTEX SEARCH SERVICE AIML_DEMO_DB.DOC_PROCESSING.LEASE_SEARCH_SERVICE TO ROLE <role_name>;
GRANT SELECT ON SEMANTIC VIEW AIML_DEMO_DB.DOC_PROCESSING.LEASE_ANALYTICS_VIEW TO ROLE <role_name>;
GRANT SELECT ON TABLE AIML_DEMO_DB.DOC_PROCESSING.EXTRACTED_LEASE_DATA TO ROLE <role_name>;

-- 4. Warehouse for query execution:
GRANT USAGE ON WAREHOUSE DATA_ENGINEERING_WH TO ROLE <role_name>;
```

**Privilege summary:**

| Privilege | Object | Why |
|---|---|---|
| `SNOWFLAKE.CORTEX_USER` or `SNOWFLAKE.CORTEX_AGENT_USER` | Database role | Access to Cortex Agent API |
| `USAGE` | Database, schema | Navigate to the agent |
| `USAGE` | Agent | Query the agent |
| `USAGE` | Cortex Search Service | Agent's LeaseSearch tool |
| `SELECT` | Semantic View | Agent's LeaseAnalytics tool |
| `SELECT` | EXTRACTED_LEASE_DATA table | Underlying data for the semantic view |
| `USAGE` | Warehouse | Execute generated SQL queries |

### Quick setup for `CORTEX_AI_DEMO_ROLE`

The demo role created by `00_setup.sql` already has CREATE privileges. To also grant USAGE on the Phase 3 objects (if running as a different user):

```sql
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON AGENT AIML_DEMO_DB.DOC_PROCESSING.LEASE_INTELLIGENCE_AGENT TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT USAGE ON CORTEX SEARCH SERVICE AIML_DEMO_DB.DOC_PROCESSING.LEASE_SEARCH_SERVICE TO ROLE CORTEX_AI_DEMO_ROLE;
GRANT SELECT ON SEMANTIC VIEW AIML_DEMO_DB.DOC_PROCESSING.LEASE_ANALYTICS_VIEW TO ROLE CORTEX_AI_DEMO_ROLE;
```

> **Note**: The role that *creates* the objects automatically has OWNERSHIP, so `CORTEX_AI_DEMO_ROLE` (the creator) does not need these additional grants. These are only needed when granting access to *other* roles.
