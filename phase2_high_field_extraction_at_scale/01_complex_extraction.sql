-- =============================================================================
-- HIGH-FIELD EXTRACTION AT SCALE
-- =============================================================================
-- Demonstrates two strategies for extracting 351 fields from complex leases
-- when AI_EXTRACT's 100-question-per-call limit is exceeded.
--
-- Approach A: Multi-pass AI_EXTRACT with merge
-- Approach B: AI_COMPLETE with structured JSON output
--
-- Database:   AIML_DEMO_DB
-- Schema:     DOC_PROCESSING
-- Role:       CORTEX_AI_DEMO_ROLE (least-privileged)
--
-- Prerequisites:
--   - Run 00_setup.sql first to create database and role
--   - Run 00_generate_complex_leases.py to create PDFs
--   - Upload PDFs: snow stage copy leases/*.pdf @COMPLEX_LEASE_DOCUMENTS --overwrite
-- =============================================================================


-- =============================================================================
-- STEP 0: SET CONTEXT
-- =============================================================================

USE ROLE CORTEX_AI_DEMO_ROLE;
USE DATABASE AIML_DEMO_DB;
USE SCHEMA DOC_PROCESSING;
USE WAREHOUSE DATA_ENGINEERING_WH;


-- =============================================================================
-- STEP 1: CREATE STAGE AND UPLOAD COMPLEX LEASES
-- =============================================================================
-- Each complex lease contains 351 extractable fields embedded in realistic
-- legal prose across 11 articles. AI_EXTRACT allows max 100 questions per call,
-- so we need at least 4 passes to extract all fields.
-- =============================================================================

CREATE STAGE IF NOT EXISTS COMPLEX_LEASE_DOCUMENTS
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    COMMENT = 'Stage for complex lease PDFs - Phase 2 high-field extraction demo';

-- Upload PDFs using Snow CLI:
-- snow stage copy phase2_high_field_extraction_at_scale/leases/*.pdf @COMPLEX_LEASE_DOCUMENTS --overwrite

-- Refresh directory after upload
ALTER STAGE COMPLEX_LEASE_DOCUMENTS REFRESH;

-- Verify files (expect 3 complex lease PDFs)
SELECT * FROM DIRECTORY(@COMPLEX_LEASE_DOCUMENTS);


-- =============================================================================
-- STEP 2: PARSE DOCUMENTS ONCE (parse-once pattern)
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | EFFICIENCY: Parse each document ONCE and store the results.             |
-- | All downstream extraction reads from this table - no re-parsing.       |
-- +-------------------------------------------------------------------------+
-- =============================================================================

CREATE OR REPLACE TABLE PARSED_COMPLEX_LEASES (
    lease_file      VARCHAR,
    file_size       NUMBER,
    en_content      TEXT,
    parsed_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO PARSED_COMPLEX_LEASES (lease_file, file_size, en_content)
SELECT
    RELATIVE_PATH AS lease_file,
    SIZE AS file_size,
    AI_PARSE_DOCUMENT(
        TO_FILE('@COMPLEX_LEASE_DOCUMENTS', RELATIVE_PATH),
        {'mode': 'LAYOUT'}
    ):content::TEXT AS en_content
FROM DIRECTORY(@COMPLEX_LEASE_DOCUMENTS)
WHERE RELATIVE_PATH LIKE '%.pdf';

-- Verify parsed documents
SELECT lease_file, file_size, LENGTH(en_content) AS content_length
FROM PARSED_COMPLEX_LEASES
ORDER BY lease_file;


-- =============================================================================
-- =============================================================================
-- APPROACH A: MULTI-PASS AI_EXTRACT WITH MERGE
-- =============================================================================
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | PROBLEM: 351 fields > AI_EXTRACT's 100-question limit.                 |
-- | SOLUTION: Split into 4 passes of ~88 fields each, then merge results   |
-- |           using JOIN into one unified row per document.                 |
-- +-------------------------------------------------------------------------+
-- =============================================================================


-- =============================================================================
-- APPROACH A - Pass 1 of 4: Core Terms, Financial, and start of Operating Expenses (fields 1-88)
-- =============================================================================
-- 88 fields in this pass
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE EXTRACT_PASS_1 AS
SELECT
    lease_file,
    AI_EXTRACT(
        text => en_content,
        responseFormat => {
        'lease_id': 'What is the Lease ID or reference number?',
        'execution_date': 'What is the lease execution date?',
        'landlord_name': 'What is the full legal name of the Landlord?',
        'landlord_state': 'What state is the Landlord organized in?',
        'landlord_entity_type': 'What is the Landlord entity type (LLC, Corp, etc.)?',
        'landlord_address': 'What is the Landlord principal office address?',
        'tenant_name': 'What is the full legal name of the Tenant?',
        'tenant_state': 'What state is the Tenant organized in?',
        'tenant_entity_type': 'What is the Tenant entity type (LLC, Corp, etc.)?',
        'tenant_business_type': 'What is the Tenant primary business type?',
        'tenant_address': 'What is the Tenant principal office address?',
        'property_address': 'What is the street address of the Premises?',
        'property_city': 'What city is the Premises located in?',
        'property_state': 'What state is the Premises located in?',
        'property_zip': 'What is the ZIP code of the Premises?',
        'property_county': 'What county is the Premises in?',
        'market': 'What industrial market is referenced?',
        'tax_parcel_id': 'What is the Tax Parcel ID?',
        'total_rentable_sqft': 'What is the total rentable square footage?',
        'office_sqft': 'What is the office square footage?',
        'warehouse_sqft': 'What is the warehouse square footage?',
        'land_acres': 'How many acres is the land?',
        'clear_height_ft': 'What is the warehouse clear height in feet?',
        'dock_doors': 'How many dock-high loading doors?',
        'drive_in_doors': 'How many drive-in doors?',
        'trailer_parking_spaces': 'How many trailer parking spaces?',
        'auto_parking_spaces': 'How many automobile parking spaces?',
        'building_year_built': 'What year was the building constructed?',
        'lease_start_date': 'What is the Lease Commencement Date?',
        'lease_end_date': 'What is the Lease Expiration Date?',
        'lease_term_months': 'What is the Lease Term in months?',
        'rent_commencement_date': 'What is the Rent Commencement Date?',
        'lease_execution_city': 'What city was the lease executed in?',
        'base_rent_psf': 'What is the Base Rent per square foot?',
        'annual_base_rent': 'What is the annual Base Rent amount?',
        'monthly_base_rent': 'What is the monthly Base Rent amount?',
        'rent_escalation_pct': 'What is the annual rent escalation percentage?',
        'rent_escalation_type': 'What is the rent escalation method?',
        'yr1_annual_rent': 'What is the Year 1 annual rent?',
        'yr2_annual_rent': 'What is the Year 2 annual rent?',
        'yr3_annual_rent': 'What is the Year 3 annual rent?',
        'yr4_annual_rent': 'What is the Year 4 annual rent?',
        'yr5_annual_rent': 'What is the Year 5 annual rent?',
        'rent_payment_day': 'On what day of the month is rent due?',
        'rent_payment_method': 'What is the rent payment method?',
        'free_rent_months': 'How many months of free rent?',
        'free_rent_conditions': 'What are the free rent conditions?',
        'security_deposit_amount': 'What is the security deposit amount?',
        'security_deposit_months': 'How many months does the security deposit represent?',
        'security_deposit_form': 'What form is the security deposit (cash, LC)?',
        'security_deposit_return_days': 'How many days to return the security deposit?',
        'security_deposit_interest': 'Does the security deposit earn interest?',
        'letter_of_credit_amount': 'What is the letter of credit amount?',
        'letter_of_credit_issuer_rating': 'What is the required LC issuer rating?',
        'letter_of_credit_expiry_months': 'What is the LC term in months?',
        'letter_of_credit_burndown': 'Is the letter of credit subject to burndown?',
        'letter_of_credit_draw_conditions': 'What are the LC draw conditions?',
        'ti_allowance_psf': 'What is the TI allowance per square foot?',
        'ti_allowance_total': 'What is the total TI allowance amount?',
        'ti_deadline_months': 'How many months to complete tenant improvements?',
        'ti_unused_treatment': 'What happens to unused TI allowance?',
        'ti_approval_required': 'Is Landlord approval required for TI plans?',
        'ti_general_contractor': 'Who selects the TI general contractor?',
        'ti_change_order_cap_pct': 'What is the change order cap percentage?',
        'lease_type': 'What is the lease type (NNN, Gross, Modified Gross)?',
        'cam_psf_estimate': 'What is the estimated CAM cost per square foot?',
        'cam_annual_estimate': 'What is the estimated annual CAM cost?',
        'tax_psf_estimate': 'What is the estimated tax cost per square foot?',
        'tax_annual_estimate': 'What is the estimated annual tax cost?',
        'insurance_psf_estimate': 'What is the estimated insurance per square foot?',
        'insurance_annual_estimate': 'What is the estimated annual insurance cost?',
        'mgmt_fee_pct': 'What is the management fee percentage?',
        'base_year': 'What is the base year for expense calculations?',
        'proration_method': 'What is the proration method for expenses?',
        'late_payment_fee_pct': 'What is the late payment fee percentage?',
        'late_payment_grace_days': 'How many grace days for late payment?',
        'interest_on_late_payment_pct': 'What is the interest rate on late payments?',
        'holdover_rent_multiplier': 'What is the holdover rent multiplier?',
        'holdover_rent_type': 'How is holdover rent calculated?',
        'opex_cap_pct': 'What is the operating expense cap percentage?',
        'opex_cap_type': 'What type of operating expense cap applies?',
        'opex_base_year_amount': 'What is the base year operating expense amount?',
        'opex_reconciliation_deadline_months': 'How many months for expense reconciliation?',
        'opex_reconciliation_method': 'What is the expense reconciliation method?',
        'opex_audit_right': 'Does Tenant have operating expense audit rights?',
        'opex_audit_frequency': 'How often can Tenant audit expenses?',
        'opex_audit_notice_days': 'How many days notice for an expense audit?',
        'opex_audit_period_years': 'How many years can the audit cover?'
        }
    ) AS pass_1_result
FROM PARSED_COMPLEX_LEASES;


-- =============================================================================
-- APPROACH A - Pass 2 of 4: Operating Expenses (cont.), Options, and start of Insurance (fields 89-176)
-- =============================================================================
-- 88 fields in this pass
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE EXTRACT_PASS_2 AS
SELECT
    lease_file,
    AI_EXTRACT(
        text => en_content,
        responseFormat => {
        'opex_audit_cost_responsibility': 'Who bears the cost of an expense audit?',
        'opex_dispute_resolution': 'How are expense disputes resolved?',
        'opex_gross_up_provision': 'Is there a gross-up provision?',
        'opex_gross_up_method': 'What is the gross-up methodology?',
        'tax_protest_right': 'Who has the right to protest taxes?',
        'tax_protest_cost_sharing': 'How are tax protest costs shared?',
        'tax_abatement_sharing': 'How are tax abatement savings shared?',
        'controllable_expense_cap_pct': 'What is the controllable expense cap percentage?',
        'uncontrollable_expenses_list': 'What expenses are classified as uncontrollable?',
        'utility_responsibility': 'Who is responsible for utilities?',
        'utility_types_covered': 'What utility types are covered?',
        'hvac_maintenance_responsibility': 'Who is responsible for HVAC maintenance?',
        'hvac_contract_requirement': 'Is an HVAC service contract required?',
        'snow_removal_responsibility': 'Who is responsible for snow removal?',
        'landscaping_responsibility': 'Who is responsible for landscaping?',
        'janitorial_responsibility': 'Who is responsible for janitorial services?',
        'pest_control_responsibility': 'Who is responsible for pest control?',
        'trash_removal_responsibility': 'Who is responsible for trash removal?',
        'recycling_requirements': 'What are the recycling requirements?',
        'parking_lot_maintenance': 'Who maintains the parking lot?',
        'roof_maintenance_responsibility': 'Who is responsible for roof maintenance?',
        'capital_expenditure_treatment': 'How are capital expenditures treated?',
        'capital_expenditure_threshold': 'What is the capital expenditure threshold?',
        'reserve_fund_contribution_psf': 'What is the reserve fund contribution per sqft?',
        'insurance_requirements_for_cam': 'What are the CAM insurance requirements?',
        'property_management_company': 'What is the property management company name?',
        'property_management_fee_pct': 'What is the property management fee percentage?',
        'administrative_overhead_pct': 'What is the administrative overhead percentage?',
        'expense_exclusions': 'What items are excluded from operating expenses?',
        'tenant_proportionate_share_pct': 'What is the Tenant proportionate share percentage?',
        'common_area_definition': 'How are Common Areas defined?',
        'renewal_option_count': 'How many renewal options does Tenant have?',
        'renewal_option_term_months': 'What is the renewal option term in months?',
        'renewal_notice_months': 'How many months notice for renewal?',
        'renewal_rent_method': 'How is renewal rent determined?',
        'renewal_fmv_determination': 'How is Fair Market Value determined for renewal?',
        'renewal_fmv_dispute_resolution': 'How are FMV disputes resolved?',
        'renewal_ti_allowance': 'What is the renewal TI allowance?',
        'renewal_conditions': 'What conditions apply to renewal options?',
        'expansion_option': 'Is there an expansion option?',
        'expansion_space_sqft': 'What is the expansion space square footage?',
        'expansion_notice_months': 'How many months notice for expansion?',
        'expansion_rent_rate': 'What is the expansion space rent rate?',
        'expansion_deadline_month': 'By what month must expansion be exercised?',
        'expansion_ti_allowance': 'What is the expansion TI allowance?',
        'rofo_right': 'Is there a Right of First Offer (ROFO)?',
        'rofo_space_description': 'What space does the ROFO apply to?',
        'rofo_notice_days': 'How many days notice for ROFO?',
        'rofo_response_days': 'How many days to respond to ROFO?',
        'rofo_matching_terms': 'What are the ROFO matching terms?',
        'rofr_right': 'Is there a Right of First Refusal (ROFR)?',
        'rofr_space_description': 'What space does the ROFR apply to?',
        'rofr_notice_days': 'How many days notice for ROFR?',
        'purchase_option': 'Is there a purchase option?',
        'purchase_option_price_method': 'How is the purchase price determined?',
        'purchase_option_exercise_window': 'What is the purchase option exercise window?',
        'purchase_option_due_diligence_days': 'How many days for purchase due diligence?',
        'purchase_option_closing_days': 'How many days to close the purchase?',
        'termination_option': 'Is there an early termination option?',
        'termination_option_effective_month': 'When can termination be effective?',
        'termination_notice_months': 'How many months notice for termination?',
        'termination_fee_months_rent': 'How many months rent is the termination fee?',
        'termination_fee_includes_unamortized_ti': 'Does termination fee include unamortized TI?',
        'termination_fee_includes_commission': 'Does termination fee include unamortized commissions?',
        'contraction_option': 'Is there a contraction option?',
        'contraction_min_sqft_retained': 'What is the minimum sqft Tenant must retain?',
        'contraction_notice_months': 'How many months notice for contraction?',
        'contraction_fee_type': 'What is the contraction fee type?',
        'relocation_right_landlord': 'Does Landlord have a relocation right?',
        'relocation_comparable_space': 'What defines comparable relocation space?',
        'relocation_cost_responsibility': 'Who bears relocation costs?',
        'must_take_space': 'Is there a must-take obligation?',
        'must_take_space_sqft': 'What is the must-take space square footage?',
        'must_take_deadline_month': 'By what month must must-take space be leased?',
        'must_take_rent_rate': 'What is the must-take space rent rate?',
        'sublease_consent_required': 'Is consent required for sublease?',
        'sublease_profit_sharing_pct': 'What percentage of sublease profit goes to Landlord?',
        'gl_coverage_per_occurrence': 'What is the GL coverage per occurrence limit?',
        'gl_coverage_aggregate': 'What is the GL aggregate coverage limit?',
        'gl_deductible_max': 'What is the maximum GL deductible?',
        'property_insurance_coverage': 'What is the property insurance coverage basis?',
        'property_insurance_includes_ti': 'Does property insurance cover Tenant Improvements?',
        'property_insurance_business_personal_property': 'What is the business personal property coverage?',
        'umbrella_excess_liability': 'What is the umbrella/excess liability limit?',
        'workers_comp_coverage': 'What is the workers compensation coverage?',
        'auto_liability_coverage': 'What is the automobile liability coverage limit?',
        'business_interruption_coverage_months': 'How many months of business interruption coverage?',
        'professional_liability_required': 'Is professional liability insurance required?'
        }
    ) AS pass_2_result
FROM PARSED_COMPLEX_LEASES;


-- =============================================================================
-- APPROACH A - Pass 3 of 4: Insurance (cont.), Construction, and start of Default & Remedies (fields 177-264)
-- =============================================================================
-- 88 fields in this pass
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE EXTRACT_PASS_3 AS
SELECT
    lease_file,
    AI_EXTRACT(
        text => en_content,
        responseFormat => {
        'environmental_liability_coverage': 'What is the environmental liability coverage amount?',
        'tenant_insurance_carrier_rating': 'What is the minimum insurance carrier rating?',
        'landlord_additional_insured': 'Is Landlord named as additional insured?',
        'landlord_lender_additional_insured': 'Is Landlord lender named as additional insured?',
        'insurance_certificate_delivery_days': 'How many days to deliver insurance certificates?',
        'insurance_renewal_notice_days': 'How many days notice before insurance renewal?',
        'waiver_of_subrogation': 'What type of subrogation waiver applies?',
        'waiver_of_subrogation_scope': 'What is the scope of subrogation waiver?',
        'indemnification_by_tenant': 'What does Tenant indemnify Landlord for?',
        'indemnification_by_landlord': 'What does Landlord indemnify Tenant for?',
        'indemnification_survival_months': 'How many months does indemnification survive?',
        'indemnification_cap': 'What is the indemnification cap?',
        'mutual_waiver_of_consequential_damages': 'Is there a mutual waiver of consequential damages?',
        'landlord_liability_cap': 'What is the Landlord liability cap?',
        'tenant_liability_cap': 'What is the Tenant liability cap?',
        'hold_harmless_scope': 'What is the hold harmless scope?',
        'insurance_increase_due_to_tenant_use': 'What happens if Tenant causes insurance increase?',
        'landlord_property_insurance_type': 'What type of property insurance does Landlord carry?',
        'landlord_property_insurance_deductible': 'What is the Landlord property insurance deductible?',
        'earthquake_insurance': 'Is earthquake insurance required?',
        'flood_insurance': 'Is flood insurance required?',
        'terrorism_insurance': 'Is terrorism insurance included?',
        'cyber_liability_required': 'Is cyber liability insurance required?',
        'pollution_legal_liability': 'Is pollution legal liability required?',
        'builders_risk_during_ti': 'Who provides builders risk during TI construction?',
        'blanket_policy_acceptable': 'Are blanket insurance policies acceptable?',
        'self_insurance_permitted': 'Is self-insurance permitted?',
        'insurance_review_frequency': 'How often is insurance reviewed?',
        'insurance_adjustment_for_inflation': 'How are insurance limits adjusted for inflation?',
        'initial_buildout_responsibility': 'Who is responsible for initial build-out?',
        'initial_buildout_deadline_days': 'How many days to complete initial build-out?',
        'buildout_plan_approval_days': 'How many days to submit build-out plans?',
        'buildout_plan_resubmission_days': 'How many days to resubmit revised plans?',
        'construction_manager': 'Who is the construction manager?',
        'construction_oversight_fee_pct': 'What is the construction oversight fee percentage?',
        'prevailing_wage_required': 'Is prevailing wage required?',
        'construction_insurance_requirements': 'What are the construction insurance requirements?',
        'construction_lien_waiver_required': 'Are construction lien waivers required?',
        'construction_completion_guarantee': 'What is the construction completion guarantee?',
        'alterations_threshold_no_approval': 'What is the dollar threshold for alterations without approval?',
        'alterations_structural_consent': 'Is consent required for structural alterations?',
        'alterations_cosmetic_consent': 'Is consent required for cosmetic alterations?',
        'alterations_removal_at_expiration': 'Must alterations be removed at expiration?',
        'alterations_restoration_obligation': 'What is the restoration obligation?',
        'restoration_deposit_required': 'Is a restoration deposit required?',
        'restoration_cost_estimate': 'What is the estimated restoration cost?',
        'signage_right': 'What signage rights does Tenant have?',
        'signage_size_max_sqft': 'What is the maximum signage size in square feet?',
        'signage_approval_required': 'Is signage approval required?',
        'signage_cost_responsibility': 'Who pays for signage?',
        'signage_removal_at_expiration': 'What happens to signage at expiration?',
        'telecom_riser_access': 'What telecom riser access is provided?',
        'telecom_provider_choice': 'Who selects the telecom provider?',
        'telecom_equipment_rooftop': 'Is rooftop telecom equipment permitted?',
        'rooftop_license_fee_monthly': 'What is the monthly rooftop license fee?',
        'generator_permitted': 'Is an emergency generator permitted?',
        'generator_fuel_type': 'What fuel type for the generator?',
        'generator_noise_restrictions': 'What generator noise restrictions apply?',
        'solar_panel_permitted': 'Are solar panels permitted?',
        'ev_charging_stations_permitted': 'Are EV charging stations permitted?',
        'racking_system_approval': 'Is warehouse racking approval required?',
        'floor_load_capacity_psf': 'What is the floor load capacity in PSF?',
        'mezzanine_permitted': 'Is a mezzanine permitted?',
        'hazmat_storage_modifications': 'What hazmat storage modifications are allowed?',
        'monetary_default_cure_days': 'How many days to cure a monetary default?',
        'non_monetary_default_cure_days': 'How many days to cure a non-monetary default?',
        'non_monetary_extended_cure': 'What is the extended cure period for non-monetary default?',
        'notice_of_default_method': 'How are default notices delivered?',
        'notice_of_default_address_tenant': 'Where are default notices sent to Tenant?',
        'notice_of_default_address_landlord': 'Where are default notices sent to Landlord?',
        'late_fee_percentage': 'What is the late fee percentage in the default section?',
        'late_fee_grace_period_days': 'What is the late fee grace period in days?',
        'interest_on_past_due_rate': 'What is the interest rate on past due amounts?',
        'interest_calculation_method': 'How is interest on past due amounts calculated?',
        'landlord_lien_on_property': 'Does Landlord have a lien on Tenant property?',
        'landlord_lockout_right': 'Does Landlord have lockout rights?',
        'landlord_self_help_right': 'Does Landlord have self-help rights?',
        'cross_default_provision': 'Is there a cross-default provision?',
        'cross_default_cure_period_days': 'How many days to cure a cross-default?',
        'acceleration_of_rent': 'Can Landlord accelerate rent upon default?',
        'mitigation_of_damages': 'Must Landlord mitigate damages?',
        'consequential_damages_waiver': 'Is there a consequential damages waiver?',
        'attorneys_fees_prevailing_party': 'Are attorneys fees awarded to prevailing party?',
        'attorneys_fees_cap': 'What is the attorneys fees cap?',
        'guarantor_name': 'Who is the personal guarantor?',
        'guarantor_relationship': 'What is the guarantor relationship to Tenant?',
        'guarantee_type': 'What type of guarantee (full/limited recourse)?',
        'guarantee_amount_cap': 'What is the maximum guarantee amount?'
        }
    ) AS pass_3_result
FROM PARSED_COMPLEX_LEASES;


-- =============================================================================
-- APPROACH A - Pass 4 of 4: Default (cont.), Environmental, and Miscellaneous (fields 265-351)
-- =============================================================================
-- 87 fields in this pass
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE EXTRACT_PASS_4 AS
SELECT
    lease_file,
    AI_EXTRACT(
        text => en_content,
        responseFormat => {
        'guarantee_burndown_schedule': 'Is there a guarantee burndown schedule?',
        'guarantee_financial_reporting': 'How often must guarantor provide financials?',
        'bankruptcy_provision': 'What are the bankruptcy default provisions?',
        'bankruptcy_adequate_assurance_days': 'How many days for adequate assurance after bankruptcy?',
        'right_to_cure_by_lender': 'Does Landlord lender have cure rights?',
        'right_to_cure_by_guarantor': 'Does guarantor have cure rights?',
        'surrender_condition': 'In what condition must Premises be surrendered?',
        'surrender_inspection_days_before': 'How many days before expiration can Landlord inspect?',
        'holdover_provision': 'What is the holdover provision?',
        'holdover_notice_to_vacate_days': 'How many days notice to vacate after holdover?',
        'landlord_default_notice_days': 'How many days notice for Landlord default?',
        'landlord_default_cure_days': 'How many days for Landlord to cure default?',
        'rent_abatement_for_landlord_default': 'Is rent abatement available for Landlord default?',
        'tenant_offset_right': 'Does Tenant have an offset right?',
        'force_majeure_rent_abatement': 'Is rent abated during force majeure?',
        'dispute_resolution_method': 'What is the dispute resolution method?',
        'hazmat_permitted': 'Is Tenant use of hazardous materials permitted?',
        'hazmat_types_permitted': 'What types of hazardous materials are permitted?',
        'hazmat_storage_requirements': 'What are the hazmat storage requirements?',
        'hazmat_reporting_frequency': 'How often must Tenant report hazmat usage?',
        'hazmat_removal_at_expiration': 'Must Tenant remove hazmat at expiration?',
        'phase_i_esa_baseline': 'What is the Phase I ESA baseline status?',
        'phase_i_esa_date': 'When was the Phase I ESA completed?',
        'phase_ii_esa_required': 'Is a Phase II ESA required?',
        'environmental_indemnification_by_tenant': 'What does Tenant environmentally indemnify?',
        'environmental_indemnification_by_landlord': 'What does Landlord environmentally indemnify?',
        'environmental_indemnification_survival_years': 'How many years does environmental indemnification survive?',
        'environmental_remediation_responsibility': 'Who is responsible for environmental remediation?',
        'environmental_remediation_standard': 'What remediation standard applies?',
        'environmental_insurance_required': 'Is environmental insurance required?',
        'asbestos_survey_completed': 'Has an asbestos survey been completed?',
        'lead_paint_disclosure': 'What is the lead paint disclosure status?',
        'mold_prevention_responsibility': 'Who is responsible for mold prevention?',
        'indoor_air_quality_standards': 'What indoor air quality standards apply?',
        'stormwater_management_compliance': 'What stormwater compliance is required?',
        'spcc_plan_required': 'Is an SPCC plan required?',
        'ada_compliance_responsibility': 'Who is responsible for ADA compliance?',
        'ada_compliance_cost_sharing': 'How are ADA costs shared?',
        'fire_code_compliance': 'What fire code compliance is required?',
        'fire_sprinkler_system': 'What type of fire sprinkler system?',
        'fire_alarm_monitoring': 'What fire alarm monitoring is required?',
        'zoning_compliance_warranty_landlord': 'Does Landlord warrant zoning compliance?',
        'zoning_current_classification': 'What is the current zoning classification?',
        'zoning_special_use_permit': 'Is a special use permit required?',
        'building_code_compliance': 'What building code compliance is required?',
        'energy_code_compliance': 'What energy code applies?',
        'sustainability_requirements': 'What sustainability requirements apply?',
        'noise_restrictions': 'What noise restrictions apply?',
        'operating_hours_restrictions': 'What operating hours restrictions apply?',
        'truck_traffic_restrictions': 'What truck traffic restrictions apply?',
        'odor_emission_restrictions': 'What odor emission restrictions apply?',
        'governing_law_state': 'What state law governs this Lease?',
        'jurisdiction_venue': 'What is the jurisdiction and venue?',
        'force_majeure_definition': 'What events are covered by force majeure?',
        'force_majeure_max_days': 'What is the maximum force majeure extension in days?',
        'force_majeure_rent_obligation': 'Does rent continue during force majeure?',
        'subordination_required': 'Is the Lease subordinate to mortgages?',
        'subordination_non_disturbance': 'Is an SNDA required?',
        'snda_form': 'What form of SNDA?',
        'attornment_obligation': 'Is attornment to successor landlord required?',
        'estoppel_certificate_delivery_days': 'How many days to deliver estoppel certificates?',
        'estoppel_certificate_frequency': 'How often can estoppel certificates be requested?',
        'estoppel_certificate_content': 'What must the estoppel certificate confirm?',
        'recording_of_lease': 'Is the Lease recorded?',
        'recording_cost_responsibility': 'Who pays recording costs?',
        'broker_landlord': 'Who is the Landlord broker?',
        'broker_tenant': 'Who is the Tenant broker?',
        'broker_commission_responsibility': 'Who pays broker commissions?',
        'broker_commission_on_renewal': 'Is a commission paid on renewal?',
        'quiet_enjoyment_covenant': 'Is there a quiet enjoyment covenant?',
        'access_by_landlord': 'What notice is required for Landlord access?',
        'landlord_access_hours': 'When can Landlord access the Premises?',
        'signage_on_building_directory': 'Is Tenant listed on the building directory?',
        'parking_allocation': 'What is the parking allocation?',
        'confidentiality_of_lease_terms': 'Are lease terms confidential?',
        'entire_agreement_clause': 'Is there an entire agreement clause?',
        'amendment_requirements': 'What are the amendment requirements?',
        'severability_clause': 'Is there a severability clause?',
        'waiver_of_jury_trial': 'Is there a waiver of jury trial?',
        'notices_delivery_method': 'How must notices be delivered?',
        'notices_deemed_received': 'When are notices deemed received?',
        'assignment_consent_required': 'Is consent required for assignment?',
        'assignment_release_of_assignor': 'Is assignor released upon assignment?',
        'transfer_fee': 'What is the assignment/transfer fee?',
        'tenant_financial_reporting': 'What financial reporting must Tenant provide?',
        'landlord_lender_name': 'Who is the Landlord current mortgage lender?',
        'exhibit_list': 'What exhibits are attached to the Lease?'
        }
    ) AS pass_4_result
FROM PARSED_COMPLEX_LEASES;


-- =============================================================================
-- APPROACH A - MERGE: Combine all 4 passes into a single table
-- =============================================================================
-- Result: one row per lease with all 351 fields as columns.
-- =============================================================================

CREATE OR REPLACE TABLE COMPLEX_EXTRACTED_LEASE_DATA_MULTIPASS AS
SELECT
    p1.lease_file,
    p1.pass_1_result:response:lease_id::STRING AS lease_id,
    p1.pass_1_result:response:execution_date::STRING AS execution_date,
    p1.pass_1_result:response:landlord_name::STRING AS landlord_name,
    p1.pass_1_result:response:landlord_state::STRING AS landlord_state,
    p1.pass_1_result:response:landlord_entity_type::STRING AS landlord_entity_type,
    p1.pass_1_result:response:landlord_address::STRING AS landlord_address,
    p1.pass_1_result:response:tenant_name::STRING AS tenant_name,
    p1.pass_1_result:response:tenant_state::STRING AS tenant_state,
    p1.pass_1_result:response:tenant_entity_type::STRING AS tenant_entity_type,
    p1.pass_1_result:response:tenant_business_type::STRING AS tenant_business_type,
    p1.pass_1_result:response:tenant_address::STRING AS tenant_address,
    p1.pass_1_result:response:property_address::STRING AS property_address,
    p1.pass_1_result:response:property_city::STRING AS property_city,
    p1.pass_1_result:response:property_state::STRING AS property_state,
    p1.pass_1_result:response:property_zip::STRING AS property_zip,
    p1.pass_1_result:response:property_county::STRING AS property_county,
    p1.pass_1_result:response:market::STRING AS market,
    p1.pass_1_result:response:tax_parcel_id::STRING AS tax_parcel_id,
    p1.pass_1_result:response:total_rentable_sqft::STRING AS total_rentable_sqft,
    p1.pass_1_result:response:office_sqft::STRING AS office_sqft,
    p1.pass_1_result:response:warehouse_sqft::STRING AS warehouse_sqft,
    p1.pass_1_result:response:land_acres::STRING AS land_acres,
    p1.pass_1_result:response:clear_height_ft::STRING AS clear_height_ft,
    p1.pass_1_result:response:dock_doors::STRING AS dock_doors,
    p1.pass_1_result:response:drive_in_doors::STRING AS drive_in_doors,
    p1.pass_1_result:response:trailer_parking_spaces::STRING AS trailer_parking_spaces,
    p1.pass_1_result:response:auto_parking_spaces::STRING AS auto_parking_spaces,
    p1.pass_1_result:response:building_year_built::STRING AS building_year_built,
    p1.pass_1_result:response:lease_start_date::STRING AS lease_start_date,
    p1.pass_1_result:response:lease_end_date::STRING AS lease_end_date,
    p1.pass_1_result:response:lease_term_months::STRING AS lease_term_months,
    p1.pass_1_result:response:rent_commencement_date::STRING AS rent_commencement_date,
    p1.pass_1_result:response:lease_execution_city::STRING AS lease_execution_city,
    p1.pass_1_result:response:base_rent_psf::STRING AS base_rent_psf,
    p1.pass_1_result:response:annual_base_rent::STRING AS annual_base_rent,
    p1.pass_1_result:response:monthly_base_rent::STRING AS monthly_base_rent,
    p1.pass_1_result:response:rent_escalation_pct::STRING AS rent_escalation_pct,
    p1.pass_1_result:response:rent_escalation_type::STRING AS rent_escalation_type,
    p1.pass_1_result:response:yr1_annual_rent::STRING AS yr1_annual_rent,
    p1.pass_1_result:response:yr2_annual_rent::STRING AS yr2_annual_rent,
    p1.pass_1_result:response:yr3_annual_rent::STRING AS yr3_annual_rent,
    p1.pass_1_result:response:yr4_annual_rent::STRING AS yr4_annual_rent,
    p1.pass_1_result:response:yr5_annual_rent::STRING AS yr5_annual_rent,
    p1.pass_1_result:response:rent_payment_day::STRING AS rent_payment_day,
    p1.pass_1_result:response:rent_payment_method::STRING AS rent_payment_method,
    p1.pass_1_result:response:free_rent_months::STRING AS free_rent_months,
    p1.pass_1_result:response:free_rent_conditions::STRING AS free_rent_conditions,
    p1.pass_1_result:response:security_deposit_amount::STRING AS security_deposit_amount,
    p1.pass_1_result:response:security_deposit_months::STRING AS security_deposit_months,
    p1.pass_1_result:response:security_deposit_form::STRING AS security_deposit_form,
    p1.pass_1_result:response:security_deposit_return_days::STRING AS security_deposit_return_days,
    p1.pass_1_result:response:security_deposit_interest::STRING AS security_deposit_interest,
    p1.pass_1_result:response:letter_of_credit_amount::STRING AS letter_of_credit_amount,
    p1.pass_1_result:response:letter_of_credit_issuer_rating::STRING AS letter_of_credit_issuer_rating,
    p1.pass_1_result:response:letter_of_credit_expiry_months::STRING AS letter_of_credit_expiry_months,
    p1.pass_1_result:response:letter_of_credit_burndown::STRING AS letter_of_credit_burndown,
    p1.pass_1_result:response:letter_of_credit_draw_conditions::STRING AS letter_of_credit_draw_conditions,
    p1.pass_1_result:response:ti_allowance_psf::STRING AS ti_allowance_psf,
    p1.pass_1_result:response:ti_allowance_total::STRING AS ti_allowance_total,
    p1.pass_1_result:response:ti_deadline_months::STRING AS ti_deadline_months,
    p1.pass_1_result:response:ti_unused_treatment::STRING AS ti_unused_treatment,
    p1.pass_1_result:response:ti_approval_required::STRING AS ti_approval_required,
    p1.pass_1_result:response:ti_general_contractor::STRING AS ti_general_contractor,
    p1.pass_1_result:response:ti_change_order_cap_pct::STRING AS ti_change_order_cap_pct,
    p1.pass_1_result:response:lease_type::STRING AS lease_type,
    p1.pass_1_result:response:cam_psf_estimate::STRING AS cam_psf_estimate,
    p1.pass_1_result:response:cam_annual_estimate::STRING AS cam_annual_estimate,
    p1.pass_1_result:response:tax_psf_estimate::STRING AS tax_psf_estimate,
    p1.pass_1_result:response:tax_annual_estimate::STRING AS tax_annual_estimate,
    p1.pass_1_result:response:insurance_psf_estimate::STRING AS insurance_psf_estimate,
    p1.pass_1_result:response:insurance_annual_estimate::STRING AS insurance_annual_estimate,
    p1.pass_1_result:response:mgmt_fee_pct::STRING AS mgmt_fee_pct,
    p1.pass_1_result:response:base_year::STRING AS base_year,
    p1.pass_1_result:response:proration_method::STRING AS proration_method,
    p1.pass_1_result:response:late_payment_fee_pct::STRING AS late_payment_fee_pct,
    p1.pass_1_result:response:late_payment_grace_days::STRING AS late_payment_grace_days,
    p1.pass_1_result:response:interest_on_late_payment_pct::STRING AS interest_on_late_payment_pct,
    p1.pass_1_result:response:holdover_rent_multiplier::STRING AS holdover_rent_multiplier,
    p1.pass_1_result:response:holdover_rent_type::STRING AS holdover_rent_type,
    p1.pass_1_result:response:opex_cap_pct::STRING AS opex_cap_pct,
    p1.pass_1_result:response:opex_cap_type::STRING AS opex_cap_type,
    p1.pass_1_result:response:opex_base_year_amount::STRING AS opex_base_year_amount,
    p1.pass_1_result:response:opex_reconciliation_deadline_months::STRING AS opex_reconciliation_deadline_months,
    p1.pass_1_result:response:opex_reconciliation_method::STRING AS opex_reconciliation_method,
    p1.pass_1_result:response:opex_audit_right::STRING AS opex_audit_right,
    p1.pass_1_result:response:opex_audit_frequency::STRING AS opex_audit_frequency,
    p1.pass_1_result:response:opex_audit_notice_days::STRING AS opex_audit_notice_days,
    p1.pass_1_result:response:opex_audit_period_years::STRING AS opex_audit_period_years,
    p2.pass_2_result:response:opex_audit_cost_responsibility::STRING AS opex_audit_cost_responsibility,
    p2.pass_2_result:response:opex_dispute_resolution::STRING AS opex_dispute_resolution,
    p2.pass_2_result:response:opex_gross_up_provision::STRING AS opex_gross_up_provision,
    p2.pass_2_result:response:opex_gross_up_method::STRING AS opex_gross_up_method,
    p2.pass_2_result:response:tax_protest_right::STRING AS tax_protest_right,
    p2.pass_2_result:response:tax_protest_cost_sharing::STRING AS tax_protest_cost_sharing,
    p2.pass_2_result:response:tax_abatement_sharing::STRING AS tax_abatement_sharing,
    p2.pass_2_result:response:controllable_expense_cap_pct::STRING AS controllable_expense_cap_pct,
    p2.pass_2_result:response:uncontrollable_expenses_list::STRING AS uncontrollable_expenses_list,
    p2.pass_2_result:response:utility_responsibility::STRING AS utility_responsibility,
    p2.pass_2_result:response:utility_types_covered::STRING AS utility_types_covered,
    p2.pass_2_result:response:hvac_maintenance_responsibility::STRING AS hvac_maintenance_responsibility,
    p2.pass_2_result:response:hvac_contract_requirement::STRING AS hvac_contract_requirement,
    p2.pass_2_result:response:snow_removal_responsibility::STRING AS snow_removal_responsibility,
    p2.pass_2_result:response:landscaping_responsibility::STRING AS landscaping_responsibility,
    p2.pass_2_result:response:janitorial_responsibility::STRING AS janitorial_responsibility,
    p2.pass_2_result:response:pest_control_responsibility::STRING AS pest_control_responsibility,
    p2.pass_2_result:response:trash_removal_responsibility::STRING AS trash_removal_responsibility,
    p2.pass_2_result:response:recycling_requirements::STRING AS recycling_requirements,
    p2.pass_2_result:response:parking_lot_maintenance::STRING AS parking_lot_maintenance,
    p2.pass_2_result:response:roof_maintenance_responsibility::STRING AS roof_maintenance_responsibility,
    p2.pass_2_result:response:capital_expenditure_treatment::STRING AS capital_expenditure_treatment,
    p2.pass_2_result:response:capital_expenditure_threshold::STRING AS capital_expenditure_threshold,
    p2.pass_2_result:response:reserve_fund_contribution_psf::STRING AS reserve_fund_contribution_psf,
    p2.pass_2_result:response:insurance_requirements_for_cam::STRING AS insurance_requirements_for_cam,
    p2.pass_2_result:response:property_management_company::STRING AS property_management_company,
    p2.pass_2_result:response:property_management_fee_pct::STRING AS property_management_fee_pct,
    p2.pass_2_result:response:administrative_overhead_pct::STRING AS administrative_overhead_pct,
    p2.pass_2_result:response:expense_exclusions::STRING AS expense_exclusions,
    p2.pass_2_result:response:tenant_proportionate_share_pct::STRING AS tenant_proportionate_share_pct,
    p2.pass_2_result:response:common_area_definition::STRING AS common_area_definition,
    p2.pass_2_result:response:renewal_option_count::STRING AS renewal_option_count,
    p2.pass_2_result:response:renewal_option_term_months::STRING AS renewal_option_term_months,
    p2.pass_2_result:response:renewal_notice_months::STRING AS renewal_notice_months,
    p2.pass_2_result:response:renewal_rent_method::STRING AS renewal_rent_method,
    p2.pass_2_result:response:renewal_fmv_determination::STRING AS renewal_fmv_determination,
    p2.pass_2_result:response:renewal_fmv_dispute_resolution::STRING AS renewal_fmv_dispute_resolution,
    p2.pass_2_result:response:renewal_ti_allowance::STRING AS renewal_ti_allowance,
    p2.pass_2_result:response:renewal_conditions::STRING AS renewal_conditions,
    p2.pass_2_result:response:expansion_option::STRING AS expansion_option,
    p2.pass_2_result:response:expansion_space_sqft::STRING AS expansion_space_sqft,
    p2.pass_2_result:response:expansion_notice_months::STRING AS expansion_notice_months,
    p2.pass_2_result:response:expansion_rent_rate::STRING AS expansion_rent_rate,
    p2.pass_2_result:response:expansion_deadline_month::STRING AS expansion_deadline_month,
    p2.pass_2_result:response:expansion_ti_allowance::STRING AS expansion_ti_allowance,
    p2.pass_2_result:response:rofo_right::STRING AS rofo_right,
    p2.pass_2_result:response:rofo_space_description::STRING AS rofo_space_description,
    p2.pass_2_result:response:rofo_notice_days::STRING AS rofo_notice_days,
    p2.pass_2_result:response:rofo_response_days::STRING AS rofo_response_days,
    p2.pass_2_result:response:rofo_matching_terms::STRING AS rofo_matching_terms,
    p2.pass_2_result:response:rofr_right::STRING AS rofr_right,
    p2.pass_2_result:response:rofr_space_description::STRING AS rofr_space_description,
    p2.pass_2_result:response:rofr_notice_days::STRING AS rofr_notice_days,
    p2.pass_2_result:response:purchase_option::STRING AS purchase_option,
    p2.pass_2_result:response:purchase_option_price_method::STRING AS purchase_option_price_method,
    p2.pass_2_result:response:purchase_option_exercise_window::STRING AS purchase_option_exercise_window,
    p2.pass_2_result:response:purchase_option_due_diligence_days::STRING AS purchase_option_due_diligence_days,
    p2.pass_2_result:response:purchase_option_closing_days::STRING AS purchase_option_closing_days,
    p2.pass_2_result:response:termination_option::STRING AS termination_option,
    p2.pass_2_result:response:termination_option_effective_month::STRING AS termination_option_effective_month,
    p2.pass_2_result:response:termination_notice_months::STRING AS termination_notice_months,
    p2.pass_2_result:response:termination_fee_months_rent::STRING AS termination_fee_months_rent,
    p2.pass_2_result:response:termination_fee_includes_unamortized_ti::STRING AS termination_fee_includes_unamortized_ti,
    p2.pass_2_result:response:termination_fee_includes_commission::STRING AS termination_fee_includes_commission,
    p2.pass_2_result:response:contraction_option::STRING AS contraction_option,
    p2.pass_2_result:response:contraction_min_sqft_retained::STRING AS contraction_min_sqft_retained,
    p2.pass_2_result:response:contraction_notice_months::STRING AS contraction_notice_months,
    p2.pass_2_result:response:contraction_fee_type::STRING AS contraction_fee_type,
    p2.pass_2_result:response:relocation_right_landlord::STRING AS relocation_right_landlord,
    p2.pass_2_result:response:relocation_comparable_space::STRING AS relocation_comparable_space,
    p2.pass_2_result:response:relocation_cost_responsibility::STRING AS relocation_cost_responsibility,
    p2.pass_2_result:response:must_take_space::STRING AS must_take_space,
    p2.pass_2_result:response:must_take_space_sqft::STRING AS must_take_space_sqft,
    p2.pass_2_result:response:must_take_deadline_month::STRING AS must_take_deadline_month,
    p2.pass_2_result:response:must_take_rent_rate::STRING AS must_take_rent_rate,
    p2.pass_2_result:response:sublease_consent_required::STRING AS sublease_consent_required,
    p2.pass_2_result:response:sublease_profit_sharing_pct::STRING AS sublease_profit_sharing_pct,
    p2.pass_2_result:response:gl_coverage_per_occurrence::STRING AS gl_coverage_per_occurrence,
    p2.pass_2_result:response:gl_coverage_aggregate::STRING AS gl_coverage_aggregate,
    p2.pass_2_result:response:gl_deductible_max::STRING AS gl_deductible_max,
    p2.pass_2_result:response:property_insurance_coverage::STRING AS property_insurance_coverage,
    p2.pass_2_result:response:property_insurance_includes_ti::STRING AS property_insurance_includes_ti,
    p2.pass_2_result:response:property_insurance_business_personal_property::STRING AS property_insurance_business_personal_property,
    p2.pass_2_result:response:umbrella_excess_liability::STRING AS umbrella_excess_liability,
    p2.pass_2_result:response:workers_comp_coverage::STRING AS workers_comp_coverage,
    p2.pass_2_result:response:auto_liability_coverage::STRING AS auto_liability_coverage,
    p2.pass_2_result:response:business_interruption_coverage_months::STRING AS business_interruption_coverage_months,
    p2.pass_2_result:response:professional_liability_required::STRING AS professional_liability_required,
    p3.pass_3_result:response:environmental_liability_coverage::STRING AS environmental_liability_coverage,
    p3.pass_3_result:response:tenant_insurance_carrier_rating::STRING AS tenant_insurance_carrier_rating,
    p3.pass_3_result:response:landlord_additional_insured::STRING AS landlord_additional_insured,
    p3.pass_3_result:response:landlord_lender_additional_insured::STRING AS landlord_lender_additional_insured,
    p3.pass_3_result:response:insurance_certificate_delivery_days::STRING AS insurance_certificate_delivery_days,
    p3.pass_3_result:response:insurance_renewal_notice_days::STRING AS insurance_renewal_notice_days,
    p3.pass_3_result:response:waiver_of_subrogation::STRING AS waiver_of_subrogation,
    p3.pass_3_result:response:waiver_of_subrogation_scope::STRING AS waiver_of_subrogation_scope,
    p3.pass_3_result:response:indemnification_by_tenant::STRING AS indemnification_by_tenant,
    p3.pass_3_result:response:indemnification_by_landlord::STRING AS indemnification_by_landlord,
    p3.pass_3_result:response:indemnification_survival_months::STRING AS indemnification_survival_months,
    p3.pass_3_result:response:indemnification_cap::STRING AS indemnification_cap,
    p3.pass_3_result:response:mutual_waiver_of_consequential_damages::STRING AS mutual_waiver_of_consequential_damages,
    p3.pass_3_result:response:landlord_liability_cap::STRING AS landlord_liability_cap,
    p3.pass_3_result:response:tenant_liability_cap::STRING AS tenant_liability_cap,
    p3.pass_3_result:response:hold_harmless_scope::STRING AS hold_harmless_scope,
    p3.pass_3_result:response:insurance_increase_due_to_tenant_use::STRING AS insurance_increase_due_to_tenant_use,
    p3.pass_3_result:response:landlord_property_insurance_type::STRING AS landlord_property_insurance_type,
    p3.pass_3_result:response:landlord_property_insurance_deductible::STRING AS landlord_property_insurance_deductible,
    p3.pass_3_result:response:earthquake_insurance::STRING AS earthquake_insurance,
    p3.pass_3_result:response:flood_insurance::STRING AS flood_insurance,
    p3.pass_3_result:response:terrorism_insurance::STRING AS terrorism_insurance,
    p3.pass_3_result:response:cyber_liability_required::STRING AS cyber_liability_required,
    p3.pass_3_result:response:pollution_legal_liability::STRING AS pollution_legal_liability,
    p3.pass_3_result:response:builders_risk_during_ti::STRING AS builders_risk_during_ti,
    p3.pass_3_result:response:blanket_policy_acceptable::STRING AS blanket_policy_acceptable,
    p3.pass_3_result:response:self_insurance_permitted::STRING AS self_insurance_permitted,
    p3.pass_3_result:response:insurance_review_frequency::STRING AS insurance_review_frequency,
    p3.pass_3_result:response:insurance_adjustment_for_inflation::STRING AS insurance_adjustment_for_inflation,
    p3.pass_3_result:response:initial_buildout_responsibility::STRING AS initial_buildout_responsibility,
    p3.pass_3_result:response:initial_buildout_deadline_days::STRING AS initial_buildout_deadline_days,
    p3.pass_3_result:response:buildout_plan_approval_days::STRING AS buildout_plan_approval_days,
    p3.pass_3_result:response:buildout_plan_resubmission_days::STRING AS buildout_plan_resubmission_days,
    p3.pass_3_result:response:construction_manager::STRING AS construction_manager,
    p3.pass_3_result:response:construction_oversight_fee_pct::STRING AS construction_oversight_fee_pct,
    p3.pass_3_result:response:prevailing_wage_required::STRING AS prevailing_wage_required,
    p3.pass_3_result:response:construction_insurance_requirements::STRING AS construction_insurance_requirements,
    p3.pass_3_result:response:construction_lien_waiver_required::STRING AS construction_lien_waiver_required,
    p3.pass_3_result:response:construction_completion_guarantee::STRING AS construction_completion_guarantee,
    p3.pass_3_result:response:alterations_threshold_no_approval::STRING AS alterations_threshold_no_approval,
    p3.pass_3_result:response:alterations_structural_consent::STRING AS alterations_structural_consent,
    p3.pass_3_result:response:alterations_cosmetic_consent::STRING AS alterations_cosmetic_consent,
    p3.pass_3_result:response:alterations_removal_at_expiration::STRING AS alterations_removal_at_expiration,
    p3.pass_3_result:response:alterations_restoration_obligation::STRING AS alterations_restoration_obligation,
    p3.pass_3_result:response:restoration_deposit_required::STRING AS restoration_deposit_required,
    p3.pass_3_result:response:restoration_cost_estimate::STRING AS restoration_cost_estimate,
    p3.pass_3_result:response:signage_right::STRING AS signage_right,
    p3.pass_3_result:response:signage_size_max_sqft::STRING AS signage_size_max_sqft,
    p3.pass_3_result:response:signage_approval_required::STRING AS signage_approval_required,
    p3.pass_3_result:response:signage_cost_responsibility::STRING AS signage_cost_responsibility,
    p3.pass_3_result:response:signage_removal_at_expiration::STRING AS signage_removal_at_expiration,
    p3.pass_3_result:response:telecom_riser_access::STRING AS telecom_riser_access,
    p3.pass_3_result:response:telecom_provider_choice::STRING AS telecom_provider_choice,
    p3.pass_3_result:response:telecom_equipment_rooftop::STRING AS telecom_equipment_rooftop,
    p3.pass_3_result:response:rooftop_license_fee_monthly::STRING AS rooftop_license_fee_monthly,
    p3.pass_3_result:response:generator_permitted::STRING AS generator_permitted,
    p3.pass_3_result:response:generator_fuel_type::STRING AS generator_fuel_type,
    p3.pass_3_result:response:generator_noise_restrictions::STRING AS generator_noise_restrictions,
    p3.pass_3_result:response:solar_panel_permitted::STRING AS solar_panel_permitted,
    p3.pass_3_result:response:ev_charging_stations_permitted::STRING AS ev_charging_stations_permitted,
    p3.pass_3_result:response:racking_system_approval::STRING AS racking_system_approval,
    p3.pass_3_result:response:floor_load_capacity_psf::STRING AS floor_load_capacity_psf,
    p3.pass_3_result:response:mezzanine_permitted::STRING AS mezzanine_permitted,
    p3.pass_3_result:response:hazmat_storage_modifications::STRING AS hazmat_storage_modifications,
    p3.pass_3_result:response:monetary_default_cure_days::STRING AS monetary_default_cure_days,
    p3.pass_3_result:response:non_monetary_default_cure_days::STRING AS non_monetary_default_cure_days,
    p3.pass_3_result:response:non_monetary_extended_cure::STRING AS non_monetary_extended_cure,
    p3.pass_3_result:response:notice_of_default_method::STRING AS notice_of_default_method,
    p3.pass_3_result:response:notice_of_default_address_tenant::STRING AS notice_of_default_address_tenant,
    p3.pass_3_result:response:notice_of_default_address_landlord::STRING AS notice_of_default_address_landlord,
    p3.pass_3_result:response:late_fee_percentage::STRING AS late_fee_percentage,
    p3.pass_3_result:response:late_fee_grace_period_days::STRING AS late_fee_grace_period_days,
    p3.pass_3_result:response:interest_on_past_due_rate::STRING AS interest_on_past_due_rate,
    p3.pass_3_result:response:interest_calculation_method::STRING AS interest_calculation_method,
    p3.pass_3_result:response:landlord_lien_on_property::STRING AS landlord_lien_on_property,
    p3.pass_3_result:response:landlord_lockout_right::STRING AS landlord_lockout_right,
    p3.pass_3_result:response:landlord_self_help_right::STRING AS landlord_self_help_right,
    p3.pass_3_result:response:cross_default_provision::STRING AS cross_default_provision,
    p3.pass_3_result:response:cross_default_cure_period_days::STRING AS cross_default_cure_period_days,
    p3.pass_3_result:response:acceleration_of_rent::STRING AS acceleration_of_rent,
    p3.pass_3_result:response:mitigation_of_damages::STRING AS mitigation_of_damages,
    p3.pass_3_result:response:consequential_damages_waiver::STRING AS consequential_damages_waiver,
    p3.pass_3_result:response:attorneys_fees_prevailing_party::STRING AS attorneys_fees_prevailing_party,
    p3.pass_3_result:response:attorneys_fees_cap::STRING AS attorneys_fees_cap,
    p3.pass_3_result:response:guarantor_name::STRING AS guarantor_name,
    p3.pass_3_result:response:guarantor_relationship::STRING AS guarantor_relationship,
    p3.pass_3_result:response:guarantee_type::STRING AS guarantee_type,
    p3.pass_3_result:response:guarantee_amount_cap::STRING AS guarantee_amount_cap,
    p4.pass_4_result:response:guarantee_burndown_schedule::STRING AS guarantee_burndown_schedule,
    p4.pass_4_result:response:guarantee_financial_reporting::STRING AS guarantee_financial_reporting,
    p4.pass_4_result:response:bankruptcy_provision::STRING AS bankruptcy_provision,
    p4.pass_4_result:response:bankruptcy_adequate_assurance_days::STRING AS bankruptcy_adequate_assurance_days,
    p4.pass_4_result:response:right_to_cure_by_lender::STRING AS right_to_cure_by_lender,
    p4.pass_4_result:response:right_to_cure_by_guarantor::STRING AS right_to_cure_by_guarantor,
    p4.pass_4_result:response:surrender_condition::STRING AS surrender_condition,
    p4.pass_4_result:response:surrender_inspection_days_before::STRING AS surrender_inspection_days_before,
    p4.pass_4_result:response:holdover_provision::STRING AS holdover_provision,
    p4.pass_4_result:response:holdover_notice_to_vacate_days::STRING AS holdover_notice_to_vacate_days,
    p4.pass_4_result:response:landlord_default_notice_days::STRING AS landlord_default_notice_days,
    p4.pass_4_result:response:landlord_default_cure_days::STRING AS landlord_default_cure_days,
    p4.pass_4_result:response:rent_abatement_for_landlord_default::STRING AS rent_abatement_for_landlord_default,
    p4.pass_4_result:response:tenant_offset_right::STRING AS tenant_offset_right,
    p4.pass_4_result:response:force_majeure_rent_abatement::STRING AS force_majeure_rent_abatement,
    p4.pass_4_result:response:dispute_resolution_method::STRING AS dispute_resolution_method,
    p4.pass_4_result:response:hazmat_permitted::STRING AS hazmat_permitted,
    p4.pass_4_result:response:hazmat_types_permitted::STRING AS hazmat_types_permitted,
    p4.pass_4_result:response:hazmat_storage_requirements::STRING AS hazmat_storage_requirements,
    p4.pass_4_result:response:hazmat_reporting_frequency::STRING AS hazmat_reporting_frequency,
    p4.pass_4_result:response:hazmat_removal_at_expiration::STRING AS hazmat_removal_at_expiration,
    p4.pass_4_result:response:phase_i_esa_baseline::STRING AS phase_i_esa_baseline,
    p4.pass_4_result:response:phase_i_esa_date::STRING AS phase_i_esa_date,
    p4.pass_4_result:response:phase_ii_esa_required::STRING AS phase_ii_esa_required,
    p4.pass_4_result:response:environmental_indemnification_by_tenant::STRING AS environmental_indemnification_by_tenant,
    p4.pass_4_result:response:environmental_indemnification_by_landlord::STRING AS environmental_indemnification_by_landlord,
    p4.pass_4_result:response:environmental_indemnification_survival_years::STRING AS environmental_indemnification_survival_years,
    p4.pass_4_result:response:environmental_remediation_responsibility::STRING AS environmental_remediation_responsibility,
    p4.pass_4_result:response:environmental_remediation_standard::STRING AS environmental_remediation_standard,
    p4.pass_4_result:response:environmental_insurance_required::STRING AS environmental_insurance_required,
    p4.pass_4_result:response:asbestos_survey_completed::STRING AS asbestos_survey_completed,
    p4.pass_4_result:response:lead_paint_disclosure::STRING AS lead_paint_disclosure,
    p4.pass_4_result:response:mold_prevention_responsibility::STRING AS mold_prevention_responsibility,
    p4.pass_4_result:response:indoor_air_quality_standards::STRING AS indoor_air_quality_standards,
    p4.pass_4_result:response:stormwater_management_compliance::STRING AS stormwater_management_compliance,
    p4.pass_4_result:response:spcc_plan_required::STRING AS spcc_plan_required,
    p4.pass_4_result:response:ada_compliance_responsibility::STRING AS ada_compliance_responsibility,
    p4.pass_4_result:response:ada_compliance_cost_sharing::STRING AS ada_compliance_cost_sharing,
    p4.pass_4_result:response:fire_code_compliance::STRING AS fire_code_compliance,
    p4.pass_4_result:response:fire_sprinkler_system::STRING AS fire_sprinkler_system,
    p4.pass_4_result:response:fire_alarm_monitoring::STRING AS fire_alarm_monitoring,
    p4.pass_4_result:response:zoning_compliance_warranty_landlord::STRING AS zoning_compliance_warranty_landlord,
    p4.pass_4_result:response:zoning_current_classification::STRING AS zoning_current_classification,
    p4.pass_4_result:response:zoning_special_use_permit::STRING AS zoning_special_use_permit,
    p4.pass_4_result:response:building_code_compliance::STRING AS building_code_compliance,
    p4.pass_4_result:response:energy_code_compliance::STRING AS energy_code_compliance,
    p4.pass_4_result:response:sustainability_requirements::STRING AS sustainability_requirements,
    p4.pass_4_result:response:noise_restrictions::STRING AS noise_restrictions,
    p4.pass_4_result:response:operating_hours_restrictions::STRING AS operating_hours_restrictions,
    p4.pass_4_result:response:truck_traffic_restrictions::STRING AS truck_traffic_restrictions,
    p4.pass_4_result:response:odor_emission_restrictions::STRING AS odor_emission_restrictions,
    p4.pass_4_result:response:governing_law_state::STRING AS governing_law_state,
    p4.pass_4_result:response:jurisdiction_venue::STRING AS jurisdiction_venue,
    p4.pass_4_result:response:force_majeure_definition::STRING AS force_majeure_definition,
    p4.pass_4_result:response:force_majeure_max_days::STRING AS force_majeure_max_days,
    p4.pass_4_result:response:force_majeure_rent_obligation::STRING AS force_majeure_rent_obligation,
    p4.pass_4_result:response:subordination_required::STRING AS subordination_required,
    p4.pass_4_result:response:subordination_non_disturbance::STRING AS subordination_non_disturbance,
    p4.pass_4_result:response:snda_form::STRING AS snda_form,
    p4.pass_4_result:response:attornment_obligation::STRING AS attornment_obligation,
    p4.pass_4_result:response:estoppel_certificate_delivery_days::STRING AS estoppel_certificate_delivery_days,
    p4.pass_4_result:response:estoppel_certificate_frequency::STRING AS estoppel_certificate_frequency,
    p4.pass_4_result:response:estoppel_certificate_content::STRING AS estoppel_certificate_content,
    p4.pass_4_result:response:recording_of_lease::STRING AS recording_of_lease,
    p4.pass_4_result:response:recording_cost_responsibility::STRING AS recording_cost_responsibility,
    p4.pass_4_result:response:broker_landlord::STRING AS broker_landlord,
    p4.pass_4_result:response:broker_tenant::STRING AS broker_tenant,
    p4.pass_4_result:response:broker_commission_responsibility::STRING AS broker_commission_responsibility,
    p4.pass_4_result:response:broker_commission_on_renewal::STRING AS broker_commission_on_renewal,
    p4.pass_4_result:response:quiet_enjoyment_covenant::STRING AS quiet_enjoyment_covenant,
    p4.pass_4_result:response:access_by_landlord::STRING AS access_by_landlord,
    p4.pass_4_result:response:landlord_access_hours::STRING AS landlord_access_hours,
    p4.pass_4_result:response:signage_on_building_directory::STRING AS signage_on_building_directory,
    p4.pass_4_result:response:parking_allocation::STRING AS parking_allocation,
    p4.pass_4_result:response:confidentiality_of_lease_terms::STRING AS confidentiality_of_lease_terms,
    p4.pass_4_result:response:entire_agreement_clause::STRING AS entire_agreement_clause,
    p4.pass_4_result:response:amendment_requirements::STRING AS amendment_requirements,
    p4.pass_4_result:response:severability_clause::STRING AS severability_clause,
    p4.pass_4_result:response:waiver_of_jury_trial::STRING AS waiver_of_jury_trial,
    p4.pass_4_result:response:notices_delivery_method::STRING AS notices_delivery_method,
    p4.pass_4_result:response:notices_deemed_received::STRING AS notices_deemed_received,
    p4.pass_4_result:response:assignment_consent_required::STRING AS assignment_consent_required,
    p4.pass_4_result:response:assignment_release_of_assignor::STRING AS assignment_release_of_assignor,
    p4.pass_4_result:response:transfer_fee::STRING AS transfer_fee,
    p4.pass_4_result:response:tenant_financial_reporting::STRING AS tenant_financial_reporting,
    p4.pass_4_result:response:landlord_lender_name::STRING AS landlord_lender_name,
    p4.pass_4_result:response:exhibit_list::STRING AS exhibit_list,
    CURRENT_TIMESTAMP() AS extracted_at
FROM EXTRACT_PASS_1 p1
JOIN EXTRACT_PASS_2 p2 ON p1.lease_file = p2.lease_file
JOIN EXTRACT_PASS_3 p3 ON p1.lease_file = p3.lease_file
JOIN EXTRACT_PASS_4 p4 ON p1.lease_file = p4.lease_file;

-- Verify: count columns and check a sample
SELECT * FROM COMPLEX_EXTRACTED_LEASE_DATA_MULTIPASS LIMIT 1;

-- Verify field count (expect 351 data columns + lease_file + extracted_at = 353)
SELECT COUNT(*) AS column_count
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'COMPLEX_EXTRACTED_LEASE_DATA_MULTIPASS'
  AND TABLE_SCHEMA = 'DOC_PROCESSING';

-- Spot-check key fields
SELECT
    lease_file,
    lease_id,
    tenant_name,
    property_address,
    total_rentable_sqft,
    annual_base_rent,
    lease_type,
    lease_term_months
FROM COMPLEX_EXTRACTED_LEASE_DATA_MULTIPASS
ORDER BY lease_file;


-- =============================================================================
-- =============================================================================
-- APPROACH B: AI_COMPLETE WITH STRUCTURED JSON OUTPUT
-- =============================================================================
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | ALTERNATIVE: Use AI_COMPLETE with response_format for structured JSON.  |
-- | Advantages: more flexible prompting, model choice, JSON schema control. |
-- | Constraint: max 8192 output tokens, so we batch into 4 groups.         |
-- +-------------------------------------------------------------------------+
-- =============================================================================


-- =============================================================================
-- APPROACH B - Batch 1 of 4: Core Terms, Financial, and start of Operating Expenses (fields 1-88)
-- =============================================================================
-- 88 fields in this batch
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE COMPLETE_BATCH_1 AS
SELECT
    lease_file,
    TRY_PARSE_JSON(
        AI_COMPLETE(
            'claude-3-5-sonnet',
            'You are a commercial lease abstraction expert. Extract the following fields from this industrial lease document. Return ONLY the field values - use null if a field is not found.

Fields to extract: lease_id, execution_date, landlord_name, landlord_state, landlord_entity_type, landlord_address, tenant_name, tenant_state, tenant_entity_type, tenant_business_type, tenant_address, property_address, property_city, property_state, property_zip, property_county, market, tax_parcel_id, total_rentable_sqft, office_sqft, warehouse_sqft, land_acres, clear_height_ft, dock_doors, drive_in_doors, trailer_parking_spaces, auto_parking_spaces, building_year_built, lease_start_date, lease_end_date, lease_term_months, rent_commencement_date, lease_execution_city, base_rent_psf, annual_base_rent, monthly_base_rent, rent_escalation_pct, rent_escalation_type, yr1_annual_rent, yr2_annual_rent, yr3_annual_rent, yr4_annual_rent, yr5_annual_rent, rent_payment_day, rent_payment_method, free_rent_months, free_rent_conditions, security_deposit_amount, security_deposit_months, security_deposit_form, security_deposit_return_days, security_deposit_interest, letter_of_credit_amount, letter_of_credit_issuer_rating, letter_of_credit_expiry_months, letter_of_credit_burndown, letter_of_credit_draw_conditions, ti_allowance_psf, ti_allowance_total, ti_deadline_months, ti_unused_treatment, ti_approval_required, ti_general_contractor, ti_change_order_cap_pct, lease_type, cam_psf_estimate, cam_annual_estimate, tax_psf_estimate, tax_annual_estimate, insurance_psf_estimate, insurance_annual_estimate, mgmt_fee_pct, base_year, proration_method, late_payment_fee_pct, late_payment_grace_days, interest_on_late_payment_pct, holdover_rent_multiplier, holdover_rent_type, opex_cap_pct, opex_cap_type, opex_base_year_amount, opex_reconciliation_deadline_months, opex_reconciliation_method, opex_audit_right, opex_audit_frequency, opex_audit_notice_days, opex_audit_period_years

LEASE DOCUMENT:
' || en_content,
            {
                'temperature': 0,
                'max_tokens': 8192
            },
            {
                'type': 'json',
                'schema': {
                    'type': 'object',
                    'properties': {
                        "lease_id": {"type": "string"},
                        "execution_date": {"type": "string"},
                        "landlord_name": {"type": "string"},
                        "landlord_state": {"type": "string"},
                        "landlord_entity_type": {"type": "string"},
                        "landlord_address": {"type": "string"},
                        "tenant_name": {"type": "string"},
                        "tenant_state": {"type": "string"},
                        "tenant_entity_type": {"type": "string"},
                        "tenant_business_type": {"type": "string"},
                        "tenant_address": {"type": "string"},
                        "property_address": {"type": "string"},
                        "property_city": {"type": "string"},
                        "property_state": {"type": "string"},
                        "property_zip": {"type": "string"},
                        "property_county": {"type": "string"},
                        "market": {"type": "string"},
                        "tax_parcel_id": {"type": "string"},
                        "total_rentable_sqft": {"type": "string"},
                        "office_sqft": {"type": "string"},
                        "warehouse_sqft": {"type": "string"},
                        "land_acres": {"type": "string"},
                        "clear_height_ft": {"type": "string"},
                        "dock_doors": {"type": "string"},
                        "drive_in_doors": {"type": "string"},
                        "trailer_parking_spaces": {"type": "string"},
                        "auto_parking_spaces": {"type": "string"},
                        "building_year_built": {"type": "string"},
                        "lease_start_date": {"type": "string"},
                        "lease_end_date": {"type": "string"},
                        "lease_term_months": {"type": "string"},
                        "rent_commencement_date": {"type": "string"},
                        "lease_execution_city": {"type": "string"},
                        "base_rent_psf": {"type": "string"},
                        "annual_base_rent": {"type": "string"},
                        "monthly_base_rent": {"type": "string"},
                        "rent_escalation_pct": {"type": "string"},
                        "rent_escalation_type": {"type": "string"},
                        "yr1_annual_rent": {"type": "string"},
                        "yr2_annual_rent": {"type": "string"},
                        "yr3_annual_rent": {"type": "string"},
                        "yr4_annual_rent": {"type": "string"},
                        "yr5_annual_rent": {"type": "string"},
                        "rent_payment_day": {"type": "string"},
                        "rent_payment_method": {"type": "string"},
                        "free_rent_months": {"type": "string"},
                        "free_rent_conditions": {"type": "string"},
                        "security_deposit_amount": {"type": "string"},
                        "security_deposit_months": {"type": "string"},
                        "security_deposit_form": {"type": "string"},
                        "security_deposit_return_days": {"type": "string"},
                        "security_deposit_interest": {"type": "string"},
                        "letter_of_credit_amount": {"type": "string"},
                        "letter_of_credit_issuer_rating": {"type": "string"},
                        "letter_of_credit_expiry_months": {"type": "string"},
                        "letter_of_credit_burndown": {"type": "string"},
                        "letter_of_credit_draw_conditions": {"type": "string"},
                        "ti_allowance_psf": {"type": "string"},
                        "ti_allowance_total": {"type": "string"},
                        "ti_deadline_months": {"type": "string"},
                        "ti_unused_treatment": {"type": "string"},
                        "ti_approval_required": {"type": "string"},
                        "ti_general_contractor": {"type": "string"},
                        "ti_change_order_cap_pct": {"type": "string"},
                        "lease_type": {"type": "string"},
                        "cam_psf_estimate": {"type": "string"},
                        "cam_annual_estimate": {"type": "string"},
                        "tax_psf_estimate": {"type": "string"},
                        "tax_annual_estimate": {"type": "string"},
                        "insurance_psf_estimate": {"type": "string"},
                        "insurance_annual_estimate": {"type": "string"},
                        "mgmt_fee_pct": {"type": "string"},
                        "base_year": {"type": "string"},
                        "proration_method": {"type": "string"},
                        "late_payment_fee_pct": {"type": "string"},
                        "late_payment_grace_days": {"type": "string"},
                        "interest_on_late_payment_pct": {"type": "string"},
                        "holdover_rent_multiplier": {"type": "string"},
                        "holdover_rent_type": {"type": "string"},
                        "opex_cap_pct": {"type": "string"},
                        "opex_cap_type": {"type": "string"},
                        "opex_base_year_amount": {"type": "string"},
                        "opex_reconciliation_deadline_months": {"type": "string"},
                        "opex_reconciliation_method": {"type": "string"},
                        "opex_audit_right": {"type": "string"},
                        "opex_audit_frequency": {"type": "string"},
                        "opex_audit_notice_days": {"type": "string"},
                        "opex_audit_period_years": {"type": "string"}
                    }
                }
            }
        )
    ) AS batch_1_result
FROM PARSED_COMPLEX_LEASES;


-- =============================================================================
-- APPROACH B - Batch 2 of 4: Operating Expenses (cont.), Options, and start of Insurance (fields 89-176)
-- =============================================================================
-- 88 fields in this batch
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE COMPLETE_BATCH_2 AS
SELECT
    lease_file,
    TRY_PARSE_JSON(
        AI_COMPLETE(
            'claude-3-5-sonnet',
            'You are a commercial lease abstraction expert. Extract the following fields from this industrial lease document. Return ONLY the field values - use null if a field is not found.

Fields to extract: opex_audit_cost_responsibility, opex_dispute_resolution, opex_gross_up_provision, opex_gross_up_method, tax_protest_right, tax_protest_cost_sharing, tax_abatement_sharing, controllable_expense_cap_pct, uncontrollable_expenses_list, utility_responsibility, utility_types_covered, hvac_maintenance_responsibility, hvac_contract_requirement, snow_removal_responsibility, landscaping_responsibility, janitorial_responsibility, pest_control_responsibility, trash_removal_responsibility, recycling_requirements, parking_lot_maintenance, roof_maintenance_responsibility, capital_expenditure_treatment, capital_expenditure_threshold, reserve_fund_contribution_psf, insurance_requirements_for_cam, property_management_company, property_management_fee_pct, administrative_overhead_pct, expense_exclusions, tenant_proportionate_share_pct, common_area_definition, renewal_option_count, renewal_option_term_months, renewal_notice_months, renewal_rent_method, renewal_fmv_determination, renewal_fmv_dispute_resolution, renewal_ti_allowance, renewal_conditions, expansion_option, expansion_space_sqft, expansion_notice_months, expansion_rent_rate, expansion_deadline_month, expansion_ti_allowance, rofo_right, rofo_space_description, rofo_notice_days, rofo_response_days, rofo_matching_terms, rofr_right, rofr_space_description, rofr_notice_days, purchase_option, purchase_option_price_method, purchase_option_exercise_window, purchase_option_due_diligence_days, purchase_option_closing_days, termination_option, termination_option_effective_month, termination_notice_months, termination_fee_months_rent, termination_fee_includes_unamortized_ti, termination_fee_includes_commission, contraction_option, contraction_min_sqft_retained, contraction_notice_months, contraction_fee_type, relocation_right_landlord, relocation_comparable_space, relocation_cost_responsibility, must_take_space, must_take_space_sqft, must_take_deadline_month, must_take_rent_rate, sublease_consent_required, sublease_profit_sharing_pct, gl_coverage_per_occurrence, gl_coverage_aggregate, gl_deductible_max, property_insurance_coverage, property_insurance_includes_ti, property_insurance_business_personal_property, umbrella_excess_liability, workers_comp_coverage, auto_liability_coverage, business_interruption_coverage_months, professional_liability_required

LEASE DOCUMENT:
' || en_content,
            {
                'temperature': 0,
                'max_tokens': 8192
            },
            {
                'type': 'json',
                'schema': {
                    'type': 'object',
                    'properties': {
                        "opex_audit_cost_responsibility": {"type": "string"},
                        "opex_dispute_resolution": {"type": "string"},
                        "opex_gross_up_provision": {"type": "string"},
                        "opex_gross_up_method": {"type": "string"},
                        "tax_protest_right": {"type": "string"},
                        "tax_protest_cost_sharing": {"type": "string"},
                        "tax_abatement_sharing": {"type": "string"},
                        "controllable_expense_cap_pct": {"type": "string"},
                        "uncontrollable_expenses_list": {"type": "string"},
                        "utility_responsibility": {"type": "string"},
                        "utility_types_covered": {"type": "string"},
                        "hvac_maintenance_responsibility": {"type": "string"},
                        "hvac_contract_requirement": {"type": "string"},
                        "snow_removal_responsibility": {"type": "string"},
                        "landscaping_responsibility": {"type": "string"},
                        "janitorial_responsibility": {"type": "string"},
                        "pest_control_responsibility": {"type": "string"},
                        "trash_removal_responsibility": {"type": "string"},
                        "recycling_requirements": {"type": "string"},
                        "parking_lot_maintenance": {"type": "string"},
                        "roof_maintenance_responsibility": {"type": "string"},
                        "capital_expenditure_treatment": {"type": "string"},
                        "capital_expenditure_threshold": {"type": "string"},
                        "reserve_fund_contribution_psf": {"type": "string"},
                        "insurance_requirements_for_cam": {"type": "string"},
                        "property_management_company": {"type": "string"},
                        "property_management_fee_pct": {"type": "string"},
                        "administrative_overhead_pct": {"type": "string"},
                        "expense_exclusions": {"type": "string"},
                        "tenant_proportionate_share_pct": {"type": "string"},
                        "common_area_definition": {"type": "string"},
                        "renewal_option_count": {"type": "string"},
                        "renewal_option_term_months": {"type": "string"},
                        "renewal_notice_months": {"type": "string"},
                        "renewal_rent_method": {"type": "string"},
                        "renewal_fmv_determination": {"type": "string"},
                        "renewal_fmv_dispute_resolution": {"type": "string"},
                        "renewal_ti_allowance": {"type": "string"},
                        "renewal_conditions": {"type": "string"},
                        "expansion_option": {"type": "string"},
                        "expansion_space_sqft": {"type": "string"},
                        "expansion_notice_months": {"type": "string"},
                        "expansion_rent_rate": {"type": "string"},
                        "expansion_deadline_month": {"type": "string"},
                        "expansion_ti_allowance": {"type": "string"},
                        "rofo_right": {"type": "string"},
                        "rofo_space_description": {"type": "string"},
                        "rofo_notice_days": {"type": "string"},
                        "rofo_response_days": {"type": "string"},
                        "rofo_matching_terms": {"type": "string"},
                        "rofr_right": {"type": "string"},
                        "rofr_space_description": {"type": "string"},
                        "rofr_notice_days": {"type": "string"},
                        "purchase_option": {"type": "string"},
                        "purchase_option_price_method": {"type": "string"},
                        "purchase_option_exercise_window": {"type": "string"},
                        "purchase_option_due_diligence_days": {"type": "string"},
                        "purchase_option_closing_days": {"type": "string"},
                        "termination_option": {"type": "string"},
                        "termination_option_effective_month": {"type": "string"},
                        "termination_notice_months": {"type": "string"},
                        "termination_fee_months_rent": {"type": "string"},
                        "termination_fee_includes_unamortized_ti": {"type": "string"},
                        "termination_fee_includes_commission": {"type": "string"},
                        "contraction_option": {"type": "string"},
                        "contraction_min_sqft_retained": {"type": "string"},
                        "contraction_notice_months": {"type": "string"},
                        "contraction_fee_type": {"type": "string"},
                        "relocation_right_landlord": {"type": "string"},
                        "relocation_comparable_space": {"type": "string"},
                        "relocation_cost_responsibility": {"type": "string"},
                        "must_take_space": {"type": "string"},
                        "must_take_space_sqft": {"type": "string"},
                        "must_take_deadline_month": {"type": "string"},
                        "must_take_rent_rate": {"type": "string"},
                        "sublease_consent_required": {"type": "string"},
                        "sublease_profit_sharing_pct": {"type": "string"},
                        "gl_coverage_per_occurrence": {"type": "string"},
                        "gl_coverage_aggregate": {"type": "string"},
                        "gl_deductible_max": {"type": "string"},
                        "property_insurance_coverage": {"type": "string"},
                        "property_insurance_includes_ti": {"type": "string"},
                        "property_insurance_business_personal_property": {"type": "string"},
                        "umbrella_excess_liability": {"type": "string"},
                        "workers_comp_coverage": {"type": "string"},
                        "auto_liability_coverage": {"type": "string"},
                        "business_interruption_coverage_months": {"type": "string"},
                        "professional_liability_required": {"type": "string"}
                    }
                }
            }
        )
    ) AS batch_2_result
FROM PARSED_COMPLEX_LEASES;


-- =============================================================================
-- APPROACH B - Batch 3 of 4: Insurance (cont.), Construction, and start of Default & Remedies (fields 177-264)
-- =============================================================================
-- 88 fields in this batch
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE COMPLETE_BATCH_3 AS
SELECT
    lease_file,
    TRY_PARSE_JSON(
        AI_COMPLETE(
            'claude-3-5-sonnet',
            'You are a commercial lease abstraction expert. Extract the following fields from this industrial lease document. Return ONLY the field values - use null if a field is not found.

Fields to extract: environmental_liability_coverage, tenant_insurance_carrier_rating, landlord_additional_insured, landlord_lender_additional_insured, insurance_certificate_delivery_days, insurance_renewal_notice_days, waiver_of_subrogation, waiver_of_subrogation_scope, indemnification_by_tenant, indemnification_by_landlord, indemnification_survival_months, indemnification_cap, mutual_waiver_of_consequential_damages, landlord_liability_cap, tenant_liability_cap, hold_harmless_scope, insurance_increase_due_to_tenant_use, landlord_property_insurance_type, landlord_property_insurance_deductible, earthquake_insurance, flood_insurance, terrorism_insurance, cyber_liability_required, pollution_legal_liability, builders_risk_during_ti, blanket_policy_acceptable, self_insurance_permitted, insurance_review_frequency, insurance_adjustment_for_inflation, initial_buildout_responsibility, initial_buildout_deadline_days, buildout_plan_approval_days, buildout_plan_resubmission_days, construction_manager, construction_oversight_fee_pct, prevailing_wage_required, construction_insurance_requirements, construction_lien_waiver_required, construction_completion_guarantee, alterations_threshold_no_approval, alterations_structural_consent, alterations_cosmetic_consent, alterations_removal_at_expiration, alterations_restoration_obligation, restoration_deposit_required, restoration_cost_estimate, signage_right, signage_size_max_sqft, signage_approval_required, signage_cost_responsibility, signage_removal_at_expiration, telecom_riser_access, telecom_provider_choice, telecom_equipment_rooftop, rooftop_license_fee_monthly, generator_permitted, generator_fuel_type, generator_noise_restrictions, solar_panel_permitted, ev_charging_stations_permitted, racking_system_approval, floor_load_capacity_psf, mezzanine_permitted, hazmat_storage_modifications, monetary_default_cure_days, non_monetary_default_cure_days, non_monetary_extended_cure, notice_of_default_method, notice_of_default_address_tenant, notice_of_default_address_landlord, late_fee_percentage, late_fee_grace_period_days, interest_on_past_due_rate, interest_calculation_method, landlord_lien_on_property, landlord_lockout_right, landlord_self_help_right, cross_default_provision, cross_default_cure_period_days, acceleration_of_rent, mitigation_of_damages, consequential_damages_waiver, attorneys_fees_prevailing_party, attorneys_fees_cap, guarantor_name, guarantor_relationship, guarantee_type, guarantee_amount_cap

LEASE DOCUMENT:
' || en_content,
            {
                'temperature': 0,
                'max_tokens': 8192
            },
            {
                'type': 'json',
                'schema': {
                    'type': 'object',
                    'properties': {
                        "environmental_liability_coverage": {"type": "string"},
                        "tenant_insurance_carrier_rating": {"type": "string"},
                        "landlord_additional_insured": {"type": "string"},
                        "landlord_lender_additional_insured": {"type": "string"},
                        "insurance_certificate_delivery_days": {"type": "string"},
                        "insurance_renewal_notice_days": {"type": "string"},
                        "waiver_of_subrogation": {"type": "string"},
                        "waiver_of_subrogation_scope": {"type": "string"},
                        "indemnification_by_tenant": {"type": "string"},
                        "indemnification_by_landlord": {"type": "string"},
                        "indemnification_survival_months": {"type": "string"},
                        "indemnification_cap": {"type": "string"},
                        "mutual_waiver_of_consequential_damages": {"type": "string"},
                        "landlord_liability_cap": {"type": "string"},
                        "tenant_liability_cap": {"type": "string"},
                        "hold_harmless_scope": {"type": "string"},
                        "insurance_increase_due_to_tenant_use": {"type": "string"},
                        "landlord_property_insurance_type": {"type": "string"},
                        "landlord_property_insurance_deductible": {"type": "string"},
                        "earthquake_insurance": {"type": "string"},
                        "flood_insurance": {"type": "string"},
                        "terrorism_insurance": {"type": "string"},
                        "cyber_liability_required": {"type": "string"},
                        "pollution_legal_liability": {"type": "string"},
                        "builders_risk_during_ti": {"type": "string"},
                        "blanket_policy_acceptable": {"type": "string"},
                        "self_insurance_permitted": {"type": "string"},
                        "insurance_review_frequency": {"type": "string"},
                        "insurance_adjustment_for_inflation": {"type": "string"},
                        "initial_buildout_responsibility": {"type": "string"},
                        "initial_buildout_deadline_days": {"type": "string"},
                        "buildout_plan_approval_days": {"type": "string"},
                        "buildout_plan_resubmission_days": {"type": "string"},
                        "construction_manager": {"type": "string"},
                        "construction_oversight_fee_pct": {"type": "string"},
                        "prevailing_wage_required": {"type": "string"},
                        "construction_insurance_requirements": {"type": "string"},
                        "construction_lien_waiver_required": {"type": "string"},
                        "construction_completion_guarantee": {"type": "string"},
                        "alterations_threshold_no_approval": {"type": "string"},
                        "alterations_structural_consent": {"type": "string"},
                        "alterations_cosmetic_consent": {"type": "string"},
                        "alterations_removal_at_expiration": {"type": "string"},
                        "alterations_restoration_obligation": {"type": "string"},
                        "restoration_deposit_required": {"type": "string"},
                        "restoration_cost_estimate": {"type": "string"},
                        "signage_right": {"type": "string"},
                        "signage_size_max_sqft": {"type": "string"},
                        "signage_approval_required": {"type": "string"},
                        "signage_cost_responsibility": {"type": "string"},
                        "signage_removal_at_expiration": {"type": "string"},
                        "telecom_riser_access": {"type": "string"},
                        "telecom_provider_choice": {"type": "string"},
                        "telecom_equipment_rooftop": {"type": "string"},
                        "rooftop_license_fee_monthly": {"type": "string"},
                        "generator_permitted": {"type": "string"},
                        "generator_fuel_type": {"type": "string"},
                        "generator_noise_restrictions": {"type": "string"},
                        "solar_panel_permitted": {"type": "string"},
                        "ev_charging_stations_permitted": {"type": "string"},
                        "racking_system_approval": {"type": "string"},
                        "floor_load_capacity_psf": {"type": "string"},
                        "mezzanine_permitted": {"type": "string"},
                        "hazmat_storage_modifications": {"type": "string"},
                        "monetary_default_cure_days": {"type": "string"},
                        "non_monetary_default_cure_days": {"type": "string"},
                        "non_monetary_extended_cure": {"type": "string"},
                        "notice_of_default_method": {"type": "string"},
                        "notice_of_default_address_tenant": {"type": "string"},
                        "notice_of_default_address_landlord": {"type": "string"},
                        "late_fee_percentage": {"type": "string"},
                        "late_fee_grace_period_days": {"type": "string"},
                        "interest_on_past_due_rate": {"type": "string"},
                        "interest_calculation_method": {"type": "string"},
                        "landlord_lien_on_property": {"type": "string"},
                        "landlord_lockout_right": {"type": "string"},
                        "landlord_self_help_right": {"type": "string"},
                        "cross_default_provision": {"type": "string"},
                        "cross_default_cure_period_days": {"type": "string"},
                        "acceleration_of_rent": {"type": "string"},
                        "mitigation_of_damages": {"type": "string"},
                        "consequential_damages_waiver": {"type": "string"},
                        "attorneys_fees_prevailing_party": {"type": "string"},
                        "attorneys_fees_cap": {"type": "string"},
                        "guarantor_name": {"type": "string"},
                        "guarantor_relationship": {"type": "string"},
                        "guarantee_type": {"type": "string"},
                        "guarantee_amount_cap": {"type": "string"}
                    }
                }
            }
        )
    ) AS batch_3_result
FROM PARSED_COMPLEX_LEASES;


-- =============================================================================
-- APPROACH B - Batch 4 of 4: Default (cont.), Environmental, and Miscellaneous (fields 265-351)
-- =============================================================================
-- 87 fields in this batch
-- =============================================================================

CREATE OR REPLACE TEMPORARY TABLE COMPLETE_BATCH_4 AS
SELECT
    lease_file,
    TRY_PARSE_JSON(
        AI_COMPLETE(
            'claude-3-5-sonnet',
            'You are a commercial lease abstraction expert. Extract the following fields from this industrial lease document. Return ONLY the field values - use null if a field is not found.

Fields to extract: guarantee_burndown_schedule, guarantee_financial_reporting, bankruptcy_provision, bankruptcy_adequate_assurance_days, right_to_cure_by_lender, right_to_cure_by_guarantor, surrender_condition, surrender_inspection_days_before, holdover_provision, holdover_notice_to_vacate_days, landlord_default_notice_days, landlord_default_cure_days, rent_abatement_for_landlord_default, tenant_offset_right, force_majeure_rent_abatement, dispute_resolution_method, hazmat_permitted, hazmat_types_permitted, hazmat_storage_requirements, hazmat_reporting_frequency, hazmat_removal_at_expiration, phase_i_esa_baseline, phase_i_esa_date, phase_ii_esa_required, environmental_indemnification_by_tenant, environmental_indemnification_by_landlord, environmental_indemnification_survival_years, environmental_remediation_responsibility, environmental_remediation_standard, environmental_insurance_required, asbestos_survey_completed, lead_paint_disclosure, mold_prevention_responsibility, indoor_air_quality_standards, stormwater_management_compliance, spcc_plan_required, ada_compliance_responsibility, ada_compliance_cost_sharing, fire_code_compliance, fire_sprinkler_system, fire_alarm_monitoring, zoning_compliance_warranty_landlord, zoning_current_classification, zoning_special_use_permit, building_code_compliance, energy_code_compliance, sustainability_requirements, noise_restrictions, operating_hours_restrictions, truck_traffic_restrictions, odor_emission_restrictions, governing_law_state, jurisdiction_venue, force_majeure_definition, force_majeure_max_days, force_majeure_rent_obligation, subordination_required, subordination_non_disturbance, snda_form, attornment_obligation, estoppel_certificate_delivery_days, estoppel_certificate_frequency, estoppel_certificate_content, recording_of_lease, recording_cost_responsibility, broker_landlord, broker_tenant, broker_commission_responsibility, broker_commission_on_renewal, quiet_enjoyment_covenant, access_by_landlord, landlord_access_hours, signage_on_building_directory, parking_allocation, confidentiality_of_lease_terms, entire_agreement_clause, amendment_requirements, severability_clause, waiver_of_jury_trial, notices_delivery_method, notices_deemed_received, assignment_consent_required, assignment_release_of_assignor, transfer_fee, tenant_financial_reporting, landlord_lender_name, exhibit_list

LEASE DOCUMENT:
' || en_content,
            {
                'temperature': 0,
                'max_tokens': 8192
            },
            {
                'type': 'json',
                'schema': {
                    'type': 'object',
                    'properties': {
                        "guarantee_burndown_schedule": {"type": "string"},
                        "guarantee_financial_reporting": {"type": "string"},
                        "bankruptcy_provision": {"type": "string"},
                        "bankruptcy_adequate_assurance_days": {"type": "string"},
                        "right_to_cure_by_lender": {"type": "string"},
                        "right_to_cure_by_guarantor": {"type": "string"},
                        "surrender_condition": {"type": "string"},
                        "surrender_inspection_days_before": {"type": "string"},
                        "holdover_provision": {"type": "string"},
                        "holdover_notice_to_vacate_days": {"type": "string"},
                        "landlord_default_notice_days": {"type": "string"},
                        "landlord_default_cure_days": {"type": "string"},
                        "rent_abatement_for_landlord_default": {"type": "string"},
                        "tenant_offset_right": {"type": "string"},
                        "force_majeure_rent_abatement": {"type": "string"},
                        "dispute_resolution_method": {"type": "string"},
                        "hazmat_permitted": {"type": "string"},
                        "hazmat_types_permitted": {"type": "string"},
                        "hazmat_storage_requirements": {"type": "string"},
                        "hazmat_reporting_frequency": {"type": "string"},
                        "hazmat_removal_at_expiration": {"type": "string"},
                        "phase_i_esa_baseline": {"type": "string"},
                        "phase_i_esa_date": {"type": "string"},
                        "phase_ii_esa_required": {"type": "string"},
                        "environmental_indemnification_by_tenant": {"type": "string"},
                        "environmental_indemnification_by_landlord": {"type": "string"},
                        "environmental_indemnification_survival_years": {"type": "string"},
                        "environmental_remediation_responsibility": {"type": "string"},
                        "environmental_remediation_standard": {"type": "string"},
                        "environmental_insurance_required": {"type": "string"},
                        "asbestos_survey_completed": {"type": "string"},
                        "lead_paint_disclosure": {"type": "string"},
                        "mold_prevention_responsibility": {"type": "string"},
                        "indoor_air_quality_standards": {"type": "string"},
                        "stormwater_management_compliance": {"type": "string"},
                        "spcc_plan_required": {"type": "string"},
                        "ada_compliance_responsibility": {"type": "string"},
                        "ada_compliance_cost_sharing": {"type": "string"},
                        "fire_code_compliance": {"type": "string"},
                        "fire_sprinkler_system": {"type": "string"},
                        "fire_alarm_monitoring": {"type": "string"},
                        "zoning_compliance_warranty_landlord": {"type": "string"},
                        "zoning_current_classification": {"type": "string"},
                        "zoning_special_use_permit": {"type": "string"},
                        "building_code_compliance": {"type": "string"},
                        "energy_code_compliance": {"type": "string"},
                        "sustainability_requirements": {"type": "string"},
                        "noise_restrictions": {"type": "string"},
                        "operating_hours_restrictions": {"type": "string"},
                        "truck_traffic_restrictions": {"type": "string"},
                        "odor_emission_restrictions": {"type": "string"},
                        "governing_law_state": {"type": "string"},
                        "jurisdiction_venue": {"type": "string"},
                        "force_majeure_definition": {"type": "string"},
                        "force_majeure_max_days": {"type": "string"},
                        "force_majeure_rent_obligation": {"type": "string"},
                        "subordination_required": {"type": "string"},
                        "subordination_non_disturbance": {"type": "string"},
                        "snda_form": {"type": "string"},
                        "attornment_obligation": {"type": "string"},
                        "estoppel_certificate_delivery_days": {"type": "string"},
                        "estoppel_certificate_frequency": {"type": "string"},
                        "estoppel_certificate_content": {"type": "string"},
                        "recording_of_lease": {"type": "string"},
                        "recording_cost_responsibility": {"type": "string"},
                        "broker_landlord": {"type": "string"},
                        "broker_tenant": {"type": "string"},
                        "broker_commission_responsibility": {"type": "string"},
                        "broker_commission_on_renewal": {"type": "string"},
                        "quiet_enjoyment_covenant": {"type": "string"},
                        "access_by_landlord": {"type": "string"},
                        "landlord_access_hours": {"type": "string"},
                        "signage_on_building_directory": {"type": "string"},
                        "parking_allocation": {"type": "string"},
                        "confidentiality_of_lease_terms": {"type": "string"},
                        "entire_agreement_clause": {"type": "string"},
                        "amendment_requirements": {"type": "string"},
                        "severability_clause": {"type": "string"},
                        "waiver_of_jury_trial": {"type": "string"},
                        "notices_delivery_method": {"type": "string"},
                        "notices_deemed_received": {"type": "string"},
                        "assignment_consent_required": {"type": "string"},
                        "assignment_release_of_assignor": {"type": "string"},
                        "transfer_fee": {"type": "string"},
                        "tenant_financial_reporting": {"type": "string"},
                        "landlord_lender_name": {"type": "string"},
                        "exhibit_list": {"type": "string"}
                    }
                }
            }
        )
    ) AS batch_4_result
FROM PARSED_COMPLEX_LEASES;


-- =============================================================================
-- APPROACH B - MERGE: Combine all 4 batches
-- =============================================================================

CREATE OR REPLACE TABLE COMPLEX_EXTRACTED_LEASE_DATA_COMPLETE AS
SELECT
    b1.lease_file,
    b1.batch_1_result:lease_id::STRING AS lease_id,
    b1.batch_1_result:execution_date::STRING AS execution_date,
    b1.batch_1_result:landlord_name::STRING AS landlord_name,
    b1.batch_1_result:landlord_state::STRING AS landlord_state,
    b1.batch_1_result:landlord_entity_type::STRING AS landlord_entity_type,
    b1.batch_1_result:landlord_address::STRING AS landlord_address,
    b1.batch_1_result:tenant_name::STRING AS tenant_name,
    b1.batch_1_result:tenant_state::STRING AS tenant_state,
    b1.batch_1_result:tenant_entity_type::STRING AS tenant_entity_type,
    b1.batch_1_result:tenant_business_type::STRING AS tenant_business_type,
    b1.batch_1_result:tenant_address::STRING AS tenant_address,
    b1.batch_1_result:property_address::STRING AS property_address,
    b1.batch_1_result:property_city::STRING AS property_city,
    b1.batch_1_result:property_state::STRING AS property_state,
    b1.batch_1_result:property_zip::STRING AS property_zip,
    b1.batch_1_result:property_county::STRING AS property_county,
    b1.batch_1_result:market::STRING AS market,
    b1.batch_1_result:tax_parcel_id::STRING AS tax_parcel_id,
    b1.batch_1_result:total_rentable_sqft::STRING AS total_rentable_sqft,
    b1.batch_1_result:office_sqft::STRING AS office_sqft,
    b1.batch_1_result:warehouse_sqft::STRING AS warehouse_sqft,
    b1.batch_1_result:land_acres::STRING AS land_acres,
    b1.batch_1_result:clear_height_ft::STRING AS clear_height_ft,
    b1.batch_1_result:dock_doors::STRING AS dock_doors,
    b1.batch_1_result:drive_in_doors::STRING AS drive_in_doors,
    b1.batch_1_result:trailer_parking_spaces::STRING AS trailer_parking_spaces,
    b1.batch_1_result:auto_parking_spaces::STRING AS auto_parking_spaces,
    b1.batch_1_result:building_year_built::STRING AS building_year_built,
    b1.batch_1_result:lease_start_date::STRING AS lease_start_date,
    b1.batch_1_result:lease_end_date::STRING AS lease_end_date,
    b1.batch_1_result:lease_term_months::STRING AS lease_term_months,
    b1.batch_1_result:rent_commencement_date::STRING AS rent_commencement_date,
    b1.batch_1_result:lease_execution_city::STRING AS lease_execution_city,
    b1.batch_1_result:base_rent_psf::STRING AS base_rent_psf,
    b1.batch_1_result:annual_base_rent::STRING AS annual_base_rent,
    b1.batch_1_result:monthly_base_rent::STRING AS monthly_base_rent,
    b1.batch_1_result:rent_escalation_pct::STRING AS rent_escalation_pct,
    b1.batch_1_result:rent_escalation_type::STRING AS rent_escalation_type,
    b1.batch_1_result:yr1_annual_rent::STRING AS yr1_annual_rent,
    b1.batch_1_result:yr2_annual_rent::STRING AS yr2_annual_rent,
    b1.batch_1_result:yr3_annual_rent::STRING AS yr3_annual_rent,
    b1.batch_1_result:yr4_annual_rent::STRING AS yr4_annual_rent,
    b1.batch_1_result:yr5_annual_rent::STRING AS yr5_annual_rent,
    b1.batch_1_result:rent_payment_day::STRING AS rent_payment_day,
    b1.batch_1_result:rent_payment_method::STRING AS rent_payment_method,
    b1.batch_1_result:free_rent_months::STRING AS free_rent_months,
    b1.batch_1_result:free_rent_conditions::STRING AS free_rent_conditions,
    b1.batch_1_result:security_deposit_amount::STRING AS security_deposit_amount,
    b1.batch_1_result:security_deposit_months::STRING AS security_deposit_months,
    b1.batch_1_result:security_deposit_form::STRING AS security_deposit_form,
    b1.batch_1_result:security_deposit_return_days::STRING AS security_deposit_return_days,
    b1.batch_1_result:security_deposit_interest::STRING AS security_deposit_interest,
    b1.batch_1_result:letter_of_credit_amount::STRING AS letter_of_credit_amount,
    b1.batch_1_result:letter_of_credit_issuer_rating::STRING AS letter_of_credit_issuer_rating,
    b1.batch_1_result:letter_of_credit_expiry_months::STRING AS letter_of_credit_expiry_months,
    b1.batch_1_result:letter_of_credit_burndown::STRING AS letter_of_credit_burndown,
    b1.batch_1_result:letter_of_credit_draw_conditions::STRING AS letter_of_credit_draw_conditions,
    b1.batch_1_result:ti_allowance_psf::STRING AS ti_allowance_psf,
    b1.batch_1_result:ti_allowance_total::STRING AS ti_allowance_total,
    b1.batch_1_result:ti_deadline_months::STRING AS ti_deadline_months,
    b1.batch_1_result:ti_unused_treatment::STRING AS ti_unused_treatment,
    b1.batch_1_result:ti_approval_required::STRING AS ti_approval_required,
    b1.batch_1_result:ti_general_contractor::STRING AS ti_general_contractor,
    b1.batch_1_result:ti_change_order_cap_pct::STRING AS ti_change_order_cap_pct,
    b1.batch_1_result:lease_type::STRING AS lease_type,
    b1.batch_1_result:cam_psf_estimate::STRING AS cam_psf_estimate,
    b1.batch_1_result:cam_annual_estimate::STRING AS cam_annual_estimate,
    b1.batch_1_result:tax_psf_estimate::STRING AS tax_psf_estimate,
    b1.batch_1_result:tax_annual_estimate::STRING AS tax_annual_estimate,
    b1.batch_1_result:insurance_psf_estimate::STRING AS insurance_psf_estimate,
    b1.batch_1_result:insurance_annual_estimate::STRING AS insurance_annual_estimate,
    b1.batch_1_result:mgmt_fee_pct::STRING AS mgmt_fee_pct,
    b1.batch_1_result:base_year::STRING AS base_year,
    b1.batch_1_result:proration_method::STRING AS proration_method,
    b1.batch_1_result:late_payment_fee_pct::STRING AS late_payment_fee_pct,
    b1.batch_1_result:late_payment_grace_days::STRING AS late_payment_grace_days,
    b1.batch_1_result:interest_on_late_payment_pct::STRING AS interest_on_late_payment_pct,
    b1.batch_1_result:holdover_rent_multiplier::STRING AS holdover_rent_multiplier,
    b1.batch_1_result:holdover_rent_type::STRING AS holdover_rent_type,
    b1.batch_1_result:opex_cap_pct::STRING AS opex_cap_pct,
    b1.batch_1_result:opex_cap_type::STRING AS opex_cap_type,
    b1.batch_1_result:opex_base_year_amount::STRING AS opex_base_year_amount,
    b1.batch_1_result:opex_reconciliation_deadline_months::STRING AS opex_reconciliation_deadline_months,
    b1.batch_1_result:opex_reconciliation_method::STRING AS opex_reconciliation_method,
    b1.batch_1_result:opex_audit_right::STRING AS opex_audit_right,
    b1.batch_1_result:opex_audit_frequency::STRING AS opex_audit_frequency,
    b1.batch_1_result:opex_audit_notice_days::STRING AS opex_audit_notice_days,
    b1.batch_1_result:opex_audit_period_years::STRING AS opex_audit_period_years,
    b2.batch_2_result:opex_audit_cost_responsibility::STRING AS opex_audit_cost_responsibility,
    b2.batch_2_result:opex_dispute_resolution::STRING AS opex_dispute_resolution,
    b2.batch_2_result:opex_gross_up_provision::STRING AS opex_gross_up_provision,
    b2.batch_2_result:opex_gross_up_method::STRING AS opex_gross_up_method,
    b2.batch_2_result:tax_protest_right::STRING AS tax_protest_right,
    b2.batch_2_result:tax_protest_cost_sharing::STRING AS tax_protest_cost_sharing,
    b2.batch_2_result:tax_abatement_sharing::STRING AS tax_abatement_sharing,
    b2.batch_2_result:controllable_expense_cap_pct::STRING AS controllable_expense_cap_pct,
    b2.batch_2_result:uncontrollable_expenses_list::STRING AS uncontrollable_expenses_list,
    b2.batch_2_result:utility_responsibility::STRING AS utility_responsibility,
    b2.batch_2_result:utility_types_covered::STRING AS utility_types_covered,
    b2.batch_2_result:hvac_maintenance_responsibility::STRING AS hvac_maintenance_responsibility,
    b2.batch_2_result:hvac_contract_requirement::STRING AS hvac_contract_requirement,
    b2.batch_2_result:snow_removal_responsibility::STRING AS snow_removal_responsibility,
    b2.batch_2_result:landscaping_responsibility::STRING AS landscaping_responsibility,
    b2.batch_2_result:janitorial_responsibility::STRING AS janitorial_responsibility,
    b2.batch_2_result:pest_control_responsibility::STRING AS pest_control_responsibility,
    b2.batch_2_result:trash_removal_responsibility::STRING AS trash_removal_responsibility,
    b2.batch_2_result:recycling_requirements::STRING AS recycling_requirements,
    b2.batch_2_result:parking_lot_maintenance::STRING AS parking_lot_maintenance,
    b2.batch_2_result:roof_maintenance_responsibility::STRING AS roof_maintenance_responsibility,
    b2.batch_2_result:capital_expenditure_treatment::STRING AS capital_expenditure_treatment,
    b2.batch_2_result:capital_expenditure_threshold::STRING AS capital_expenditure_threshold,
    b2.batch_2_result:reserve_fund_contribution_psf::STRING AS reserve_fund_contribution_psf,
    b2.batch_2_result:insurance_requirements_for_cam::STRING AS insurance_requirements_for_cam,
    b2.batch_2_result:property_management_company::STRING AS property_management_company,
    b2.batch_2_result:property_management_fee_pct::STRING AS property_management_fee_pct,
    b2.batch_2_result:administrative_overhead_pct::STRING AS administrative_overhead_pct,
    b2.batch_2_result:expense_exclusions::STRING AS expense_exclusions,
    b2.batch_2_result:tenant_proportionate_share_pct::STRING AS tenant_proportionate_share_pct,
    b2.batch_2_result:common_area_definition::STRING AS common_area_definition,
    b2.batch_2_result:renewal_option_count::STRING AS renewal_option_count,
    b2.batch_2_result:renewal_option_term_months::STRING AS renewal_option_term_months,
    b2.batch_2_result:renewal_notice_months::STRING AS renewal_notice_months,
    b2.batch_2_result:renewal_rent_method::STRING AS renewal_rent_method,
    b2.batch_2_result:renewal_fmv_determination::STRING AS renewal_fmv_determination,
    b2.batch_2_result:renewal_fmv_dispute_resolution::STRING AS renewal_fmv_dispute_resolution,
    b2.batch_2_result:renewal_ti_allowance::STRING AS renewal_ti_allowance,
    b2.batch_2_result:renewal_conditions::STRING AS renewal_conditions,
    b2.batch_2_result:expansion_option::STRING AS expansion_option,
    b2.batch_2_result:expansion_space_sqft::STRING AS expansion_space_sqft,
    b2.batch_2_result:expansion_notice_months::STRING AS expansion_notice_months,
    b2.batch_2_result:expansion_rent_rate::STRING AS expansion_rent_rate,
    b2.batch_2_result:expansion_deadline_month::STRING AS expansion_deadline_month,
    b2.batch_2_result:expansion_ti_allowance::STRING AS expansion_ti_allowance,
    b2.batch_2_result:rofo_right::STRING AS rofo_right,
    b2.batch_2_result:rofo_space_description::STRING AS rofo_space_description,
    b2.batch_2_result:rofo_notice_days::STRING AS rofo_notice_days,
    b2.batch_2_result:rofo_response_days::STRING AS rofo_response_days,
    b2.batch_2_result:rofo_matching_terms::STRING AS rofo_matching_terms,
    b2.batch_2_result:rofr_right::STRING AS rofr_right,
    b2.batch_2_result:rofr_space_description::STRING AS rofr_space_description,
    b2.batch_2_result:rofr_notice_days::STRING AS rofr_notice_days,
    b2.batch_2_result:purchase_option::STRING AS purchase_option,
    b2.batch_2_result:purchase_option_price_method::STRING AS purchase_option_price_method,
    b2.batch_2_result:purchase_option_exercise_window::STRING AS purchase_option_exercise_window,
    b2.batch_2_result:purchase_option_due_diligence_days::STRING AS purchase_option_due_diligence_days,
    b2.batch_2_result:purchase_option_closing_days::STRING AS purchase_option_closing_days,
    b2.batch_2_result:termination_option::STRING AS termination_option,
    b2.batch_2_result:termination_option_effective_month::STRING AS termination_option_effective_month,
    b2.batch_2_result:termination_notice_months::STRING AS termination_notice_months,
    b2.batch_2_result:termination_fee_months_rent::STRING AS termination_fee_months_rent,
    b2.batch_2_result:termination_fee_includes_unamortized_ti::STRING AS termination_fee_includes_unamortized_ti,
    b2.batch_2_result:termination_fee_includes_commission::STRING AS termination_fee_includes_commission,
    b2.batch_2_result:contraction_option::STRING AS contraction_option,
    b2.batch_2_result:contraction_min_sqft_retained::STRING AS contraction_min_sqft_retained,
    b2.batch_2_result:contraction_notice_months::STRING AS contraction_notice_months,
    b2.batch_2_result:contraction_fee_type::STRING AS contraction_fee_type,
    b2.batch_2_result:relocation_right_landlord::STRING AS relocation_right_landlord,
    b2.batch_2_result:relocation_comparable_space::STRING AS relocation_comparable_space,
    b2.batch_2_result:relocation_cost_responsibility::STRING AS relocation_cost_responsibility,
    b2.batch_2_result:must_take_space::STRING AS must_take_space,
    b2.batch_2_result:must_take_space_sqft::STRING AS must_take_space_sqft,
    b2.batch_2_result:must_take_deadline_month::STRING AS must_take_deadline_month,
    b2.batch_2_result:must_take_rent_rate::STRING AS must_take_rent_rate,
    b2.batch_2_result:sublease_consent_required::STRING AS sublease_consent_required,
    b2.batch_2_result:sublease_profit_sharing_pct::STRING AS sublease_profit_sharing_pct,
    b2.batch_2_result:gl_coverage_per_occurrence::STRING AS gl_coverage_per_occurrence,
    b2.batch_2_result:gl_coverage_aggregate::STRING AS gl_coverage_aggregate,
    b2.batch_2_result:gl_deductible_max::STRING AS gl_deductible_max,
    b2.batch_2_result:property_insurance_coverage::STRING AS property_insurance_coverage,
    b2.batch_2_result:property_insurance_includes_ti::STRING AS property_insurance_includes_ti,
    b2.batch_2_result:property_insurance_business_personal_property::STRING AS property_insurance_business_personal_property,
    b2.batch_2_result:umbrella_excess_liability::STRING AS umbrella_excess_liability,
    b2.batch_2_result:workers_comp_coverage::STRING AS workers_comp_coverage,
    b2.batch_2_result:auto_liability_coverage::STRING AS auto_liability_coverage,
    b2.batch_2_result:business_interruption_coverage_months::STRING AS business_interruption_coverage_months,
    b2.batch_2_result:professional_liability_required::STRING AS professional_liability_required,
    b3.batch_3_result:environmental_liability_coverage::STRING AS environmental_liability_coverage,
    b3.batch_3_result:tenant_insurance_carrier_rating::STRING AS tenant_insurance_carrier_rating,
    b3.batch_3_result:landlord_additional_insured::STRING AS landlord_additional_insured,
    b3.batch_3_result:landlord_lender_additional_insured::STRING AS landlord_lender_additional_insured,
    b3.batch_3_result:insurance_certificate_delivery_days::STRING AS insurance_certificate_delivery_days,
    b3.batch_3_result:insurance_renewal_notice_days::STRING AS insurance_renewal_notice_days,
    b3.batch_3_result:waiver_of_subrogation::STRING AS waiver_of_subrogation,
    b3.batch_3_result:waiver_of_subrogation_scope::STRING AS waiver_of_subrogation_scope,
    b3.batch_3_result:indemnification_by_tenant::STRING AS indemnification_by_tenant,
    b3.batch_3_result:indemnification_by_landlord::STRING AS indemnification_by_landlord,
    b3.batch_3_result:indemnification_survival_months::STRING AS indemnification_survival_months,
    b3.batch_3_result:indemnification_cap::STRING AS indemnification_cap,
    b3.batch_3_result:mutual_waiver_of_consequential_damages::STRING AS mutual_waiver_of_consequential_damages,
    b3.batch_3_result:landlord_liability_cap::STRING AS landlord_liability_cap,
    b3.batch_3_result:tenant_liability_cap::STRING AS tenant_liability_cap,
    b3.batch_3_result:hold_harmless_scope::STRING AS hold_harmless_scope,
    b3.batch_3_result:insurance_increase_due_to_tenant_use::STRING AS insurance_increase_due_to_tenant_use,
    b3.batch_3_result:landlord_property_insurance_type::STRING AS landlord_property_insurance_type,
    b3.batch_3_result:landlord_property_insurance_deductible::STRING AS landlord_property_insurance_deductible,
    b3.batch_3_result:earthquake_insurance::STRING AS earthquake_insurance,
    b3.batch_3_result:flood_insurance::STRING AS flood_insurance,
    b3.batch_3_result:terrorism_insurance::STRING AS terrorism_insurance,
    b3.batch_3_result:cyber_liability_required::STRING AS cyber_liability_required,
    b3.batch_3_result:pollution_legal_liability::STRING AS pollution_legal_liability,
    b3.batch_3_result:builders_risk_during_ti::STRING AS builders_risk_during_ti,
    b3.batch_3_result:blanket_policy_acceptable::STRING AS blanket_policy_acceptable,
    b3.batch_3_result:self_insurance_permitted::STRING AS self_insurance_permitted,
    b3.batch_3_result:insurance_review_frequency::STRING AS insurance_review_frequency,
    b3.batch_3_result:insurance_adjustment_for_inflation::STRING AS insurance_adjustment_for_inflation,
    b3.batch_3_result:initial_buildout_responsibility::STRING AS initial_buildout_responsibility,
    b3.batch_3_result:initial_buildout_deadline_days::STRING AS initial_buildout_deadline_days,
    b3.batch_3_result:buildout_plan_approval_days::STRING AS buildout_plan_approval_days,
    b3.batch_3_result:buildout_plan_resubmission_days::STRING AS buildout_plan_resubmission_days,
    b3.batch_3_result:construction_manager::STRING AS construction_manager,
    b3.batch_3_result:construction_oversight_fee_pct::STRING AS construction_oversight_fee_pct,
    b3.batch_3_result:prevailing_wage_required::STRING AS prevailing_wage_required,
    b3.batch_3_result:construction_insurance_requirements::STRING AS construction_insurance_requirements,
    b3.batch_3_result:construction_lien_waiver_required::STRING AS construction_lien_waiver_required,
    b3.batch_3_result:construction_completion_guarantee::STRING AS construction_completion_guarantee,
    b3.batch_3_result:alterations_threshold_no_approval::STRING AS alterations_threshold_no_approval,
    b3.batch_3_result:alterations_structural_consent::STRING AS alterations_structural_consent,
    b3.batch_3_result:alterations_cosmetic_consent::STRING AS alterations_cosmetic_consent,
    b3.batch_3_result:alterations_removal_at_expiration::STRING AS alterations_removal_at_expiration,
    b3.batch_3_result:alterations_restoration_obligation::STRING AS alterations_restoration_obligation,
    b3.batch_3_result:restoration_deposit_required::STRING AS restoration_deposit_required,
    b3.batch_3_result:restoration_cost_estimate::STRING AS restoration_cost_estimate,
    b3.batch_3_result:signage_right::STRING AS signage_right,
    b3.batch_3_result:signage_size_max_sqft::STRING AS signage_size_max_sqft,
    b3.batch_3_result:signage_approval_required::STRING AS signage_approval_required,
    b3.batch_3_result:signage_cost_responsibility::STRING AS signage_cost_responsibility,
    b3.batch_3_result:signage_removal_at_expiration::STRING AS signage_removal_at_expiration,
    b3.batch_3_result:telecom_riser_access::STRING AS telecom_riser_access,
    b3.batch_3_result:telecom_provider_choice::STRING AS telecom_provider_choice,
    b3.batch_3_result:telecom_equipment_rooftop::STRING AS telecom_equipment_rooftop,
    b3.batch_3_result:rooftop_license_fee_monthly::STRING AS rooftop_license_fee_monthly,
    b3.batch_3_result:generator_permitted::STRING AS generator_permitted,
    b3.batch_3_result:generator_fuel_type::STRING AS generator_fuel_type,
    b3.batch_3_result:generator_noise_restrictions::STRING AS generator_noise_restrictions,
    b3.batch_3_result:solar_panel_permitted::STRING AS solar_panel_permitted,
    b3.batch_3_result:ev_charging_stations_permitted::STRING AS ev_charging_stations_permitted,
    b3.batch_3_result:racking_system_approval::STRING AS racking_system_approval,
    b3.batch_3_result:floor_load_capacity_psf::STRING AS floor_load_capacity_psf,
    b3.batch_3_result:mezzanine_permitted::STRING AS mezzanine_permitted,
    b3.batch_3_result:hazmat_storage_modifications::STRING AS hazmat_storage_modifications,
    b3.batch_3_result:monetary_default_cure_days::STRING AS monetary_default_cure_days,
    b3.batch_3_result:non_monetary_default_cure_days::STRING AS non_monetary_default_cure_days,
    b3.batch_3_result:non_monetary_extended_cure::STRING AS non_monetary_extended_cure,
    b3.batch_3_result:notice_of_default_method::STRING AS notice_of_default_method,
    b3.batch_3_result:notice_of_default_address_tenant::STRING AS notice_of_default_address_tenant,
    b3.batch_3_result:notice_of_default_address_landlord::STRING AS notice_of_default_address_landlord,
    b3.batch_3_result:late_fee_percentage::STRING AS late_fee_percentage,
    b3.batch_3_result:late_fee_grace_period_days::STRING AS late_fee_grace_period_days,
    b3.batch_3_result:interest_on_past_due_rate::STRING AS interest_on_past_due_rate,
    b3.batch_3_result:interest_calculation_method::STRING AS interest_calculation_method,
    b3.batch_3_result:landlord_lien_on_property::STRING AS landlord_lien_on_property,
    b3.batch_3_result:landlord_lockout_right::STRING AS landlord_lockout_right,
    b3.batch_3_result:landlord_self_help_right::STRING AS landlord_self_help_right,
    b3.batch_3_result:cross_default_provision::STRING AS cross_default_provision,
    b3.batch_3_result:cross_default_cure_period_days::STRING AS cross_default_cure_period_days,
    b3.batch_3_result:acceleration_of_rent::STRING AS acceleration_of_rent,
    b3.batch_3_result:mitigation_of_damages::STRING AS mitigation_of_damages,
    b3.batch_3_result:consequential_damages_waiver::STRING AS consequential_damages_waiver,
    b3.batch_3_result:attorneys_fees_prevailing_party::STRING AS attorneys_fees_prevailing_party,
    b3.batch_3_result:attorneys_fees_cap::STRING AS attorneys_fees_cap,
    b3.batch_3_result:guarantor_name::STRING AS guarantor_name,
    b3.batch_3_result:guarantor_relationship::STRING AS guarantor_relationship,
    b3.batch_3_result:guarantee_type::STRING AS guarantee_type,
    b3.batch_3_result:guarantee_amount_cap::STRING AS guarantee_amount_cap,
    b4.batch_4_result:guarantee_burndown_schedule::STRING AS guarantee_burndown_schedule,
    b4.batch_4_result:guarantee_financial_reporting::STRING AS guarantee_financial_reporting,
    b4.batch_4_result:bankruptcy_provision::STRING AS bankruptcy_provision,
    b4.batch_4_result:bankruptcy_adequate_assurance_days::STRING AS bankruptcy_adequate_assurance_days,
    b4.batch_4_result:right_to_cure_by_lender::STRING AS right_to_cure_by_lender,
    b4.batch_4_result:right_to_cure_by_guarantor::STRING AS right_to_cure_by_guarantor,
    b4.batch_4_result:surrender_condition::STRING AS surrender_condition,
    b4.batch_4_result:surrender_inspection_days_before::STRING AS surrender_inspection_days_before,
    b4.batch_4_result:holdover_provision::STRING AS holdover_provision,
    b4.batch_4_result:holdover_notice_to_vacate_days::STRING AS holdover_notice_to_vacate_days,
    b4.batch_4_result:landlord_default_notice_days::STRING AS landlord_default_notice_days,
    b4.batch_4_result:landlord_default_cure_days::STRING AS landlord_default_cure_days,
    b4.batch_4_result:rent_abatement_for_landlord_default::STRING AS rent_abatement_for_landlord_default,
    b4.batch_4_result:tenant_offset_right::STRING AS tenant_offset_right,
    b4.batch_4_result:force_majeure_rent_abatement::STRING AS force_majeure_rent_abatement,
    b4.batch_4_result:dispute_resolution_method::STRING AS dispute_resolution_method,
    b4.batch_4_result:hazmat_permitted::STRING AS hazmat_permitted,
    b4.batch_4_result:hazmat_types_permitted::STRING AS hazmat_types_permitted,
    b4.batch_4_result:hazmat_storage_requirements::STRING AS hazmat_storage_requirements,
    b4.batch_4_result:hazmat_reporting_frequency::STRING AS hazmat_reporting_frequency,
    b4.batch_4_result:hazmat_removal_at_expiration::STRING AS hazmat_removal_at_expiration,
    b4.batch_4_result:phase_i_esa_baseline::STRING AS phase_i_esa_baseline,
    b4.batch_4_result:phase_i_esa_date::STRING AS phase_i_esa_date,
    b4.batch_4_result:phase_ii_esa_required::STRING AS phase_ii_esa_required,
    b4.batch_4_result:environmental_indemnification_by_tenant::STRING AS environmental_indemnification_by_tenant,
    b4.batch_4_result:environmental_indemnification_by_landlord::STRING AS environmental_indemnification_by_landlord,
    b4.batch_4_result:environmental_indemnification_survival_years::STRING AS environmental_indemnification_survival_years,
    b4.batch_4_result:environmental_remediation_responsibility::STRING AS environmental_remediation_responsibility,
    b4.batch_4_result:environmental_remediation_standard::STRING AS environmental_remediation_standard,
    b4.batch_4_result:environmental_insurance_required::STRING AS environmental_insurance_required,
    b4.batch_4_result:asbestos_survey_completed::STRING AS asbestos_survey_completed,
    b4.batch_4_result:lead_paint_disclosure::STRING AS lead_paint_disclosure,
    b4.batch_4_result:mold_prevention_responsibility::STRING AS mold_prevention_responsibility,
    b4.batch_4_result:indoor_air_quality_standards::STRING AS indoor_air_quality_standards,
    b4.batch_4_result:stormwater_management_compliance::STRING AS stormwater_management_compliance,
    b4.batch_4_result:spcc_plan_required::STRING AS spcc_plan_required,
    b4.batch_4_result:ada_compliance_responsibility::STRING AS ada_compliance_responsibility,
    b4.batch_4_result:ada_compliance_cost_sharing::STRING AS ada_compliance_cost_sharing,
    b4.batch_4_result:fire_code_compliance::STRING AS fire_code_compliance,
    b4.batch_4_result:fire_sprinkler_system::STRING AS fire_sprinkler_system,
    b4.batch_4_result:fire_alarm_monitoring::STRING AS fire_alarm_monitoring,
    b4.batch_4_result:zoning_compliance_warranty_landlord::STRING AS zoning_compliance_warranty_landlord,
    b4.batch_4_result:zoning_current_classification::STRING AS zoning_current_classification,
    b4.batch_4_result:zoning_special_use_permit::STRING AS zoning_special_use_permit,
    b4.batch_4_result:building_code_compliance::STRING AS building_code_compliance,
    b4.batch_4_result:energy_code_compliance::STRING AS energy_code_compliance,
    b4.batch_4_result:sustainability_requirements::STRING AS sustainability_requirements,
    b4.batch_4_result:noise_restrictions::STRING AS noise_restrictions,
    b4.batch_4_result:operating_hours_restrictions::STRING AS operating_hours_restrictions,
    b4.batch_4_result:truck_traffic_restrictions::STRING AS truck_traffic_restrictions,
    b4.batch_4_result:odor_emission_restrictions::STRING AS odor_emission_restrictions,
    b4.batch_4_result:governing_law_state::STRING AS governing_law_state,
    b4.batch_4_result:jurisdiction_venue::STRING AS jurisdiction_venue,
    b4.batch_4_result:force_majeure_definition::STRING AS force_majeure_definition,
    b4.batch_4_result:force_majeure_max_days::STRING AS force_majeure_max_days,
    b4.batch_4_result:force_majeure_rent_obligation::STRING AS force_majeure_rent_obligation,
    b4.batch_4_result:subordination_required::STRING AS subordination_required,
    b4.batch_4_result:subordination_non_disturbance::STRING AS subordination_non_disturbance,
    b4.batch_4_result:snda_form::STRING AS snda_form,
    b4.batch_4_result:attornment_obligation::STRING AS attornment_obligation,
    b4.batch_4_result:estoppel_certificate_delivery_days::STRING AS estoppel_certificate_delivery_days,
    b4.batch_4_result:estoppel_certificate_frequency::STRING AS estoppel_certificate_frequency,
    b4.batch_4_result:estoppel_certificate_content::STRING AS estoppel_certificate_content,
    b4.batch_4_result:recording_of_lease::STRING AS recording_of_lease,
    b4.batch_4_result:recording_cost_responsibility::STRING AS recording_cost_responsibility,
    b4.batch_4_result:broker_landlord::STRING AS broker_landlord,
    b4.batch_4_result:broker_tenant::STRING AS broker_tenant,
    b4.batch_4_result:broker_commission_responsibility::STRING AS broker_commission_responsibility,
    b4.batch_4_result:broker_commission_on_renewal::STRING AS broker_commission_on_renewal,
    b4.batch_4_result:quiet_enjoyment_covenant::STRING AS quiet_enjoyment_covenant,
    b4.batch_4_result:access_by_landlord::STRING AS access_by_landlord,
    b4.batch_4_result:landlord_access_hours::STRING AS landlord_access_hours,
    b4.batch_4_result:signage_on_building_directory::STRING AS signage_on_building_directory,
    b4.batch_4_result:parking_allocation::STRING AS parking_allocation,
    b4.batch_4_result:confidentiality_of_lease_terms::STRING AS confidentiality_of_lease_terms,
    b4.batch_4_result:entire_agreement_clause::STRING AS entire_agreement_clause,
    b4.batch_4_result:amendment_requirements::STRING AS amendment_requirements,
    b4.batch_4_result:severability_clause::STRING AS severability_clause,
    b4.batch_4_result:waiver_of_jury_trial::STRING AS waiver_of_jury_trial,
    b4.batch_4_result:notices_delivery_method::STRING AS notices_delivery_method,
    b4.batch_4_result:notices_deemed_received::STRING AS notices_deemed_received,
    b4.batch_4_result:assignment_consent_required::STRING AS assignment_consent_required,
    b4.batch_4_result:assignment_release_of_assignor::STRING AS assignment_release_of_assignor,
    b4.batch_4_result:transfer_fee::STRING AS transfer_fee,
    b4.batch_4_result:tenant_financial_reporting::STRING AS tenant_financial_reporting,
    b4.batch_4_result:landlord_lender_name::STRING AS landlord_lender_name,
    b4.batch_4_result:exhibit_list::STRING AS exhibit_list,
    CURRENT_TIMESTAMP() AS extracted_at
FROM COMPLETE_BATCH_1 b1
JOIN COMPLETE_BATCH_2 b2 ON b1.lease_file = b2.lease_file
JOIN COMPLETE_BATCH_3 b3 ON b1.lease_file = b3.lease_file
JOIN COMPLETE_BATCH_4 b4 ON b1.lease_file = b4.lease_file;

-- Verify
SELECT * FROM COMPLEX_EXTRACTED_LEASE_DATA_COMPLETE LIMIT 1;

-- Spot-check key fields
SELECT
    lease_file,
    lease_id,
    tenant_name,
    property_address,
    total_rentable_sqft,
    annual_base_rent,
    lease_type,
    lease_term_months
FROM COMPLEX_EXTRACTED_LEASE_DATA_COMPLETE
ORDER BY lease_file;


-- =============================================================================
-- =============================================================================
-- COMPARE APPROACHES
-- =============================================================================
-- =============================================================================
-- Side-by-side comparison of key fields from both approaches.
-- This validates that both methods extract the same values.
-- =============================================================================

SELECT
    m.lease_file,
    '--- Lease ID ---' AS section,
    m.lease_id AS multipass_lease_id,
    c.lease_id AS complete_lease_id,
    m.lease_id = c.lease_id AS match_lease_id,
    '--- Tenant ---' AS section2,
    m.tenant_name AS multipass_tenant,
    c.tenant_name AS complete_tenant,
    '--- Rent ---' AS section3,
    m.annual_base_rent AS multipass_rent,
    c.annual_base_rent AS complete_rent,
    '--- Sqft ---' AS section4,
    m.total_rentable_sqft AS multipass_sqft,
    c.total_rentable_sqft AS complete_sqft
FROM COMPLEX_EXTRACTED_LEASE_DATA_MULTIPASS m
JOIN COMPLEX_EXTRACTED_LEASE_DATA_COMPLETE c
    ON m.lease_file = c.lease_file
ORDER BY m.lease_file;


-- =============================================================================
-- KEY TAKEAWAYS
-- =============================================================================
-- +-------------------------------------------------------------------------+
-- | APPROACH A: Multi-pass AI_EXTRACT                                       |
-- |   - Uses question-based extraction (natural language per field)          |
-- |   - 4 passes x 3 documents = 12 AI_EXTRACT calls                       |
-- |   - Best when: fields map well to simple questions                      |
-- |                                                                         |
-- | APPROACH B: AI_COMPLETE with structured output                          |
-- |   - Uses a prompt + JSON schema for structured responses                |
-- |   - Model choice flexibility (claude-3-5-sonnet, llama, GPT, etc.)     |
-- |   - 4 batches x 3 documents = 12 AI_COMPLETE calls                     |
-- |   - Best when: complex fields need richer prompting or instructions     |
-- |                                                                         |
-- | BOTH APPROACHES:                                                        |
-- |   - Read from PARSED_COMPLEX_LEASES (parse-once pattern)               |
-- |   - Handle 351 fields by splitting into groups of ~88                   |
-- |   - Merge results per document using JOIN on lease_file                 |
-- +-------------------------------------------------------------------------+
-- =============================================================================
